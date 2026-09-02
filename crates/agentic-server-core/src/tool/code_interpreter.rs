use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use agent_rt_control::execution_service_client::ExecutionServiceClient;
use agent_rt_control::workspace_service_client::WorkspaceServiceClient;
use agent_rt_control::{
    CancelExecutionRequest, Command, CreateWorkspaceRequest, Execution, ExecutionLimits, ExecutionState,
    GetExecutionRequest, StartExecutionRequest, WatchExecutionRequest, WorkspaceState,
};
use base64::Engine;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use tonic::metadata::MetadataValue;
use tonic::transport::{Channel, Endpoint};
use tonic::{Code, Request, Status};

use super::{GatewayExecutionContext, GatewayExecutor, ToolError, ToolHandler, ToolOutput, ToolType};
use crate::config::AgentRtExecutorConfig;
use crate::storage::{ClaimRemoteExecution, RemoteExecutionLedger, RemoteExecutionLink};
use crate::types::io::output::{
    CodeInterpreterCall, CodeInterpreterCallStatus, CodeInterpreterOutput, FunctionToolCall, GatewayCallStatus,
};
use crate::types::io::{FunctionTool, OutputItem};
use crate::types::tools::CodeInterpreterToolParam;

#[derive(Clone)]
pub struct RemoteAgentRtExecutor {
    channel: Channel,
    config: AgentRtExecutorConfig,
    ledger: RemoteExecutionLedger,
}

impl std::fmt::Debug for RemoteAgentRtExecutor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RemoteAgentRtExecutor")
            .field("endpoint", &self.config.endpoint)
            .field("workspace_class_id", &self.config.workspace_class_id)
            .field("route_id", &self.config.route_id)
            .finish_non_exhaustive()
    }
}

impl RemoteAgentRtExecutor {
    /// Builds the generated gRPC client over deployment-owned configuration.
    ///
    /// # Errors
    ///
    /// Returns [`ToolError::Config`] when the endpoint, workspace class, route,
    /// or signing key cannot satisfy the private execution contract.
    pub fn new(config: AgentRtExecutorConfig, ledger: RemoteExecutionLedger) -> Result<Self, ToolError> {
        if config.endpoint.trim().is_empty()
            || config.workspace_class_id.trim().is_empty()
            || config.route_id.trim().is_empty()
        {
            return Err(ToolError::Config(
                "agent-rt endpoint, workspace_class_id, and route_id must not be empty".to_owned(),
            ));
        }
        if config.subject_signing_key.expose().len() < 32 {
            return Err(ToolError::Config(
                "agent-rt subject signing key must contain at least 32 bytes".to_owned(),
            ));
        }
        let endpoint = Endpoint::from_shared(config.endpoint.trim_end_matches('/').to_owned())
            .map_err(|error| ToolError::Config(format!("invalid agent-rt endpoint: {error}")))?
            .connect_timeout(config.transport_timeout);
        Ok(Self {
            channel: endpoint.connect_lazy(),
            config,
            ledger,
        })
    }

    async fn execute_remote(&self, context: GatewayExecutionContext, arguments: &str) -> Result<ToolOutput, ToolError> {
        let (link, request, token) = self.prepare_execution(&context, arguments).await?;
        self.create_workspace(&link, &token, context.trace_context.traceparent.as_deref())
            .await
            .map_err(remote_error)?;
        let mut cancel_on_drop = RemoteCancelOnDrop::new(
            self.channel.clone(),
            self.config.transport_timeout,
            link.execution_id.clone(),
            token.clone(),
        );
        let record = self
            .start_or_recover(&link, request, &token, context.trace_context.traceparent.as_deref())
            .await?;
        let outcome = self.await_terminal(&context, &link, &token, record).await;
        cancel_on_drop.disarm();
        outcome
    }

    async fn prepare_execution(
        &self,
        context: &GatewayExecutionContext,
        arguments: &str,
    ) -> Result<(RemoteExecutionLink, StartExecutionRequest, String), ToolError> {
        let subject = context.subject.as_ref().ok_or_else(|| {
            ToolError::Config("agent-rt execution requires an authenticated tenant and principal".to_owned())
        })?;
        let args: CodeInterpreterArguments = serde_json::from_str(arguments)
            .map_err(|error| ToolError::Config(format!("invalid code_interpreter arguments: {error}")))?;
        if args.code.trim().is_empty() {
            return Err(ToolError::Config(
                "code_interpreter requires a non-empty code string".to_owned(),
            ));
        }

        let request_fingerprint = request_fingerprint(&self.config.route_id, &context.workspace_id, &args.code);
        let proposed_deadline = context
            .absolute_deadline
            .unwrap_or_else(|| SystemTime::now() + self.config.execution_timeout)
            .duration_since(UNIX_EPOCH)
            .map_err(|_| ToolError::Config("execution deadline precedes the Unix epoch".to_owned()))?
            .as_secs();
        let proposed_deadline = i64::try_from(proposed_deadline)
            .map_err(|_| ToolError::Config("execution deadline exceeds supported range".to_owned()))?;
        let link = self
            .ledger
            .claim(ClaimRemoteExecution {
                subject,
                response_id: &context.response_id,
                conversation_id: context.conversation_id.as_deref(),
                call_id: &context.call_id,
                route_id: &self.config.route_id,
                request_fingerprint: &request_fingerprint,
                absolute_deadline: proposed_deadline,
            })
            .await
            .map_err(|error| ToolError::Execution(format!("failed to claim remote execution: {error}")))?;
        if link.execution_id != context.execution_id || link.workspace_id != context.workspace_id {
            return Err(ToolError::Execution(
                "remote execution identity does not match the durable logical-call binding".to_owned(),
            ));
        }

        let absolute_deadline_unix_millis = u64::try_from(link.absolute_deadline)
            .unwrap_or_default()
            .saturating_mul(1_000);
        let request = StartExecutionRequest {
            execution_id: link.execution_id.clone(),
            workspace_id: link.workspace_id.clone(),
            route_id: link.route_id.clone(),
            command: Some(Command {
                argv: vec!["python".to_owned(), "-c".to_owned(), args.code],
                cwd: None,
                env: HashMap::new(),
                stdin: Vec::new(),
                artifact_paths: Vec::new(),
            }),
            absolute_deadline_unix_millis,
            limits: Some(ExecutionLimits::default()),
            client_metadata: [
                ("response_id".to_owned(), link.response_id.clone()),
                ("call_id".to_owned(), link.call_id.clone()),
            ]
            .into_iter()
            .chain(
                link.conversation_id
                    .clone()
                    .map(|value| ("conversation_id".to_owned(), value)),
            )
            .collect(),
        };
        let token = self.sign_subject(&link)?;
        Ok((link, request, token))
    }

    async fn create_workspace(
        &self,
        link: &RemoteExecutionLink,
        token: &str,
        traceparent: Option<&str>,
    ) -> Result<(), RemoteTransportError> {
        let message = CreateWorkspaceRequest {
            workspace_id: link.workspace_id.clone(),
            workspace_class_id: self.config.workspace_class_id.clone(),
        };
        let execute = || async {
            let request = grpc_request(message.clone(), token, self.config.transport_timeout, traceparent)?;
            WorkspaceServiceClient::new(self.channel.clone())
                .create_workspace(request)
                .await
                .map(tonic::Response::into_inner)
                .map_err(|status| classify_status(&status))
        };
        let workspace = match execute().await {
            Ok(workspace) => workspace,
            Err(RemoteTransportError::Ambiguous(_)) => execute().await?,
            Err(error) => return Err(error),
        };
        match WorkspaceState::try_from(workspace.state) {
            Ok(WorkspaceState::Ready) => Ok(()),
            Ok(state) => Err(RemoteTransportError::Rejected {
                code: Code::FailedPrecondition,
                message: format!("workspace is not ready: {}", state.as_str_name()),
            }),
            Err(_) => Err(RemoteTransportError::Rejected {
                code: Code::Internal,
                message: "workspace returned an unknown lifecycle state".to_owned(),
            }),
        }
    }

    async fn start_or_recover(
        &self,
        link: &RemoteExecutionLink,
        request: StartExecutionRequest,
        token: &str,
        traceparent: Option<&str>,
    ) -> Result<ExecutionOutcome, ToolError> {
        let record = match self.start(request.clone(), token, traceparent).await {
            Ok(record) => record,
            Err(RemoteTransportError::Ambiguous(_)) => match self.lookup(link, token).await {
                Ok(record) => record,
                Err(RemoteTransportError::NotFound) => match self.start(request, token, traceparent).await {
                    Ok(record) => record,
                    Err(RemoteTransportError::Ambiguous(_)) => {
                        return self
                            .finish_unknown(link, "start response and recovery lookup were both ambiguous")
                            .await;
                    }
                    Err(error) => return Err(remote_error(error)),
                },
                Err(RemoteTransportError::Ambiguous(_)) => {
                    return self
                        .finish_unknown(link, "start response was lost and recovery lookup was ambiguous")
                        .await;
                }
                Err(error) => return Err(remote_error(error)),
            },
            Err(error) => return Err(remote_error(error)),
        };
        ExecutionOutcome::try_from(record).map_err(remote_error)
    }

    async fn await_terminal(
        &self,
        context: &GatewayExecutionContext,
        link: &RemoteExecutionLink,
        token: &str,
        mut record: ExecutionOutcome,
    ) -> Result<ToolOutput, ToolError> {
        loop {
            if record.state.is_terminal() {
                return self.finish(&context.call_id, link, record).await;
            }
            if context.cancellation.is_cancelled() {
                record = self
                    .cancel(link, token)
                    .await
                    .and_then(ExecutionOutcome::try_from)
                    .map_err(remote_error)?;
                return self.finish(&context.call_id, link, record).await;
            }
            if unix_seconds_i64() >= link.absolute_deadline {
                record = match self.lookup(link, token).await.and_then(ExecutionOutcome::try_from) {
                    Ok(record) => record,
                    Err(_) => {
                        return self
                            .finish_unknown(link, "deadline recovery lookup did not return an authoritative outcome")
                            .await;
                    }
                };
                if record.state.is_terminal() {
                    return self.finish(&context.call_id, link, record).await;
                }
                return self
                    .finish_unknown(
                        link,
                        "execution deadline elapsed before an authoritative provider outcome",
                    )
                    .await;
            }

            record = tokio::select! {
                () = context.cancellation.cancelled() => continue,
                result = self.watch_once(link, token, record.revision) => {
                    match result.and_then(ExecutionOutcome::try_from) {
                        Ok(record) => record,
                        Err(RemoteTransportError::Ambiguous(_)) => {
                            tokio::time::sleep(Duration::from_millis(100)).await;
                            continue;
                        }
                        Err(_) => {
                            return self
                                .finish_unknown(link, "accepted execution could not be reconciled by watch")
                                .await;
                        }
                    }
                }
            };
        }
    }

    async fn finish(
        &self,
        call_id: &str,
        link: &RemoteExecutionLink,
        record: ExecutionOutcome,
    ) -> Result<ToolOutput, ToolError> {
        let outcome = serde_json::to_string(&record)
            .map_err(|error| ToolError::Execution(format!("failed to serialize agent-rt outcome: {error}")))?;
        self.ledger
            .record_outcome(link, record.state.as_str(), &outcome)
            .await
            .map_err(|error| ToolError::Execution(format!("failed to persist remote execution outcome: {error}")))?;
        if record.state == ExecutionOutcomeState::OutcomeUnknown {
            return Err(ToolError::OutcomeUnknown {
                execution_id: link.execution_id.clone(),
                message: "agent-rt cannot determine the provider outcome".to_owned(),
            });
        }
        if record.state != ExecutionOutcomeState::Completed {
            return Err(ToolError::Execution(format!(
                "agent-rt execution {} ended in state {}",
                link.execution_id,
                record.state.as_str()
            )));
        }
        Ok(ToolOutput {
            call_id: call_id.to_owned(),
            output: outcome,
        })
    }

    async fn finish_unknown<T>(&self, link: &RemoteExecutionLink, message: &str) -> Result<T, ToolError> {
        self.ledger
            .record_outcome(
                link,
                "outcome_unknown",
                &serde_json::json!({ "message": message }).to_string(),
            )
            .await
            .map_err(|error| ToolError::Execution(format!("failed to persist unknown execution outcome: {error}")))?;
        Err(ToolError::OutcomeUnknown {
            execution_id: link.execution_id.clone(),
            message: message.to_owned(),
        })
    }

    async fn start(
        &self,
        message: StartExecutionRequest,
        token: &str,
        traceparent: Option<&str>,
    ) -> Result<Execution, RemoteTransportError> {
        let request = grpc_request(message, token, self.config.transport_timeout, traceparent)?;
        ExecutionServiceClient::new(self.channel.clone())
            .start_execution(request)
            .await
            .map(tonic::Response::into_inner)
            .map_err(|status| classify_status(&status))
    }

    async fn lookup(&self, link: &RemoteExecutionLink, token: &str) -> Result<Execution, RemoteTransportError> {
        let request = grpc_request(
            GetExecutionRequest {
                execution_id: link.execution_id.clone(),
            },
            token,
            self.config.transport_timeout,
            None,
        )?;
        ExecutionServiceClient::new(self.channel.clone())
            .get_execution(request)
            .await
            .map(tonic::Response::into_inner)
            .map_err(|status| classify_status(&status))
    }

    async fn watch_once(
        &self,
        link: &RemoteExecutionLink,
        token: &str,
        after_revision: u64,
    ) -> Result<Execution, RemoteTransportError> {
        let request = grpc_request(
            WatchExecutionRequest {
                execution_id: link.execution_id.clone(),
                after_revision,
            },
            token,
            self.config.transport_timeout.saturating_add(self.config.lookup_wait),
            None,
        )?;
        let mut stream = ExecutionServiceClient::new(self.channel.clone())
            .watch_execution(request)
            .await
            .map_err(|status| classify_status(&status))?
            .into_inner();
        match tokio::time::timeout(self.config.lookup_wait, stream.message()).await {
            Ok(Ok(Some(record))) => Ok(record),
            Ok(Ok(None)) => self.lookup(link, token).await,
            Ok(Err(status)) => Err(classify_status(&status)),
            Err(_) => Err(RemoteTransportError::Ambiguous(
                "execution watch timed out before a revision".to_owned(),
            )),
        }
    }

    async fn cancel(&self, link: &RemoteExecutionLink, token: &str) -> Result<Execution, RemoteTransportError> {
        cancel_execution(
            self.channel.clone(),
            self.config.transport_timeout,
            link.execution_id.clone(),
            token,
        )
        .await
    }

    fn sign_subject(&self, link: &RemoteExecutionLink) -> Result<String, ToolError> {
        let now = u64::try_from(unix_seconds_i64()).unwrap_or_default();
        let claims = SubjectClaims {
            issuer: self.config.subject_issuer.clone(),
            audience: self.config.subject_audience.clone(),
            tenant_id: link.tenant_id.clone(),
            principal_id: link.principal_id.clone(),
            issued_at: now,
            expires_at: now.saturating_add(60),
            token_id: link.execution_id.clone(),
        };
        let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(
            serde_json::to_vec(&claims)
                .map_err(|error| ToolError::Execution(format!("failed to encode subject assertion: {error}")))?,
        );
        let mut mac = Hmac::<Sha256>::new_from_slice(self.config.subject_signing_key.expose().as_bytes())
            .map_err(|_| ToolError::Config("invalid agent-rt subject signing key".to_owned()))?;
        mac.update(payload.as_bytes());
        let signature = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
        Ok(format!("{payload}.{signature}"))
    }
}

struct RemoteCancelOnDrop {
    channel: Option<Channel>,
    transport_timeout: Duration,
    execution_id: String,
    token: String,
}

impl RemoteCancelOnDrop {
    fn new(channel: Channel, transport_timeout: Duration, execution_id: String, token: String) -> Self {
        Self {
            channel: Some(channel),
            transport_timeout,
            execution_id,
            token,
        }
    }

    fn disarm(&mut self) {
        self.channel = None;
    }
}

impl Drop for RemoteCancelOnDrop {
    fn drop(&mut self) {
        let Some(channel) = self.channel.take() else {
            return;
        };
        let execution_id = std::mem::take(&mut self.execution_id);
        let token = std::mem::take(&mut self.token);
        let transport_timeout = self.transport_timeout;
        if tokio::runtime::Handle::try_current().is_err() {
            return;
        }
        tokio::spawn(async move {
            if let Err(error) = cancel_execution(channel, transport_timeout, execution_id.clone(), &token).await {
                tracing::debug!(%execution_id, ?error, "best-effort agent-rt cancellation failed");
            }
        });
    }
}

impl ToolHandler for RemoteAgentRtExecutor {
    type ToolParams = CodeInterpreterToolParam;

    fn tool_type(&self) -> ToolType {
        ToolType::CodeInterpreter
    }

    fn validate(&self, _params: &Self::ToolParams) -> Result<(), ToolError> {
        Ok(())
    }

    fn normalize(&self, _params: &Self::ToolParams) -> Vec<FunctionTool> {
        vec![code_interpreter_function_tool()]
    }
}

impl GatewayExecutor for RemoteAgentRtExecutor {
    type ExecutionParams = CodeInterpreterToolParam;

    fn execute(
        &self,
        _call_id: &str,
        _tool_name: &str,
        _arguments: &str,
        _params: &Self::ExecutionParams,
    ) -> Pin<Box<dyn Future<Output = Result<ToolOutput, ToolError>> + Send + '_>> {
        Box::pin(async {
            Err(ToolError::Config(
                "remote code_interpreter requires request execution context".to_owned(),
            ))
        })
    }

    fn execute_with_context(
        &self,
        context: GatewayExecutionContext,
        _tool_name: &str,
        arguments: &str,
        _params: &Self::ExecutionParams,
    ) -> Pin<Box<dyn Future<Output = Result<ToolOutput, ToolError>> + Send + '_>> {
        let arguments = arguments.to_owned();
        Box::pin(async move { self.execute_remote(context, &arguments).await })
    }

    fn supports_parallel_execution(&self) -> bool {
        true
    }

    fn manages_execution_deadline(&self) -> bool {
        true
    }

    fn public_output(
        &self,
        call: &FunctionToolCall,
        output: &ToolOutput,
        status: GatewayCallStatus,
        _params: &Self::ExecutionParams,
    ) -> Option<OutputItem> {
        let arguments: CodeInterpreterArguments = serde_json::from_str(&call.arguments).ok()?;
        let record: ExecutionOutcome = serde_json::from_str(&output.output).ok()?;
        let result = record.result.as_ref();
        let mut logs = result.map_or_else(String::new, |result| result.stdout.clone());
        if let Some(stderr) = result
            .map(|result| result.stderr.as_str())
            .filter(|value| !value.is_empty())
        {
            if !logs.is_empty() && !logs.ends_with('\n') {
                logs.push('\n');
            }
            logs.push_str(stderr);
        }
        Some(OutputItem::CodeInterpreterCall(CodeInterpreterCall {
            id: call.id.clone(),
            code: arguments.code,
            container_id: record.workspace_id,
            outputs: (!logs.is_empty())
                .then_some(CodeInterpreterOutput::Logs { logs })
                .into_iter()
                .collect(),
            status: if status == GatewayCallStatus::Completed
                && record.state == ExecutionOutcomeState::Completed
                && result.is_some_and(|result| !result.is_error)
            {
                CodeInterpreterCallStatus::Completed
            } else {
                CodeInterpreterCallStatus::Failed
            },
        }))
    }
}

#[must_use]
pub(crate) fn code_interpreter_function_tool() -> FunctionTool {
    FunctionTool {
        type_: "function".to_owned(),
        name: "code_interpreter".to_owned(),
        description: Some("Execute Python code in an operator-managed sandbox.".to_owned()),
        parameters: Some(serde_json::json!({
            "type": "object",
            "properties": {
                "code": {"type": "string", "description": "Python source code to execute."}
            },
            "required": ["code"],
            "additionalProperties": false
        })),
        strict: Some(true),
    }
}

async fn cancel_execution(
    channel: Channel,
    timeout: Duration,
    execution_id: String,
    token: &str,
) -> Result<Execution, RemoteTransportError> {
    let request = grpc_request(CancelExecutionRequest { execution_id }, token, timeout, None)?;
    ExecutionServiceClient::new(channel)
        .cancel_execution(request)
        .await
        .map(tonic::Response::into_inner)
        .map_err(|status| classify_status(&status))
}

fn grpc_request<T>(
    message: T,
    token: &str,
    timeout: Duration,
    traceparent: Option<&str>,
) -> Result<Request<T>, RemoteTransportError> {
    let mut request = Request::new(message);
    request.set_timeout(timeout);
    let authorization =
        MetadataValue::try_from(format!("Bearer {token}")).map_err(|error| RemoteTransportError::Rejected {
            code: Code::InvalidArgument,
            message: format!("invalid authorization metadata: {error}"),
        })?;
    request.metadata_mut().insert("authorization", authorization);
    if let Some(traceparent) = traceparent {
        let traceparent = MetadataValue::try_from(traceparent).map_err(|error| RemoteTransportError::Rejected {
            code: Code::InvalidArgument,
            message: format!("invalid traceparent metadata: {error}"),
        })?;
        request.metadata_mut().insert("traceparent", traceparent);
    }
    Ok(request)
}

fn request_fingerprint(route_id: &str, workspace_id: &str, code: &str) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"agentic-api-agent-rt-request");
    for component in [route_id, workspace_id, "python", code] {
        hasher.update(&(component.len() as u64).to_le_bytes());
        hasher.update(component.as_bytes());
    }
    format!("blake3:{}", hasher.finalize().to_hex())
}

fn classify_status(status: &Status) -> RemoteTransportError {
    match status.code() {
        Code::NotFound => RemoteTransportError::NotFound,
        Code::AlreadyExists => RemoteTransportError::Conflict(status.message().to_owned()),
        Code::Cancelled | Code::DeadlineExceeded | Code::Unavailable | Code::Unknown => {
            RemoteTransportError::Ambiguous(status.to_string())
        }
        code => RemoteTransportError::Rejected {
            code,
            message: status.message().to_owned(),
        },
    }
}

fn remote_error(error: RemoteTransportError) -> ToolError {
    match error {
        RemoteTransportError::Conflict(message) => {
            ToolError::Execution(format!("agent-rt execution identity conflict: {message}"))
        }
        RemoteTransportError::NotFound => ToolError::Execution("agent-rt execution was not found".to_owned()),
        RemoteTransportError::Rejected { code, message } => {
            ToolError::Execution(format!("agent-rt rejected execution ({code:?}): {message}"))
        }
        RemoteTransportError::Ambiguous(message) => {
            ToolError::Execution(format!("agent-rt transport outcome is ambiguous: {message}"))
        }
    }
}

fn unix_seconds_i64() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_secs()).ok())
        .unwrap_or_default()
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CodeInterpreterArguments {
    code: String,
}

#[derive(Serialize)]
struct SubjectClaims {
    issuer: String,
    audience: String,
    tenant_id: String,
    principal_id: String,
    issued_at: u64,
    expires_at: u64,
    token_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum ExecutionOutcomeState {
    Accepted,
    Running,
    Completed,
    Failed,
    Cancelled,
    TimedOut,
    OutcomeUnknown,
}

impl ExecutionOutcomeState {
    const fn is_terminal(self) -> bool {
        !matches!(self, Self::Accepted | Self::Running)
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::TimedOut => "timed_out",
            Self::OutcomeUnknown => "outcome_unknown",
        }
    }
}

#[derive(Deserialize, Serialize)]
struct ExecutionOutcome {
    execution_id: String,
    workspace_id: String,
    route_id: String,
    revision: u64,
    state: ExecutionOutcomeState,
    result: Option<ExecutionResultProjection>,
    failure_code: Option<String>,
}

#[derive(Deserialize, Serialize)]
struct ExecutionResultProjection {
    exit_code: Option<i32>,
    stdout: String,
    stderr: String,
    output_truncated: bool,
    is_error: bool,
}

impl TryFrom<Execution> for ExecutionOutcome {
    type Error = RemoteTransportError;

    fn try_from(record: Execution) -> Result<Self, Self::Error> {
        let state = match ExecutionState::try_from(record.state) {
            Ok(ExecutionState::Accepted) => ExecutionOutcomeState::Accepted,
            Ok(ExecutionState::Running) => ExecutionOutcomeState::Running,
            Ok(ExecutionState::Succeeded) => ExecutionOutcomeState::Completed,
            Ok(ExecutionState::Failed) => ExecutionOutcomeState::Failed,
            Ok(ExecutionState::Cancelled) => ExecutionOutcomeState::Cancelled,
            Ok(ExecutionState::TimedOut) => ExecutionOutcomeState::TimedOut,
            Ok(ExecutionState::OutcomeUnknown) => ExecutionOutcomeState::OutcomeUnknown,
            Ok(ExecutionState::Unspecified) | Err(_) => {
                return Err(RemoteTransportError::Rejected {
                    code: Code::Internal,
                    message: "agent-rt returned an unknown execution state".to_owned(),
                });
            }
        };
        let result = record.result.map(|result| ExecutionResultProjection {
            exit_code: result.exit_code,
            stdout: String::from_utf8_lossy(&result.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&result.stderr).into_owned(),
            output_truncated: result.output_truncated,
            is_error: state != ExecutionOutcomeState::Completed
                || result.exit_code.is_some_and(|exit_code| exit_code != 0),
        });
        Ok(Self {
            execution_id: record.execution_id,
            workspace_id: record.workspace_id,
            route_id: record.route_id,
            revision: record.revision,
            state,
            result,
            failure_code: record.failure_code,
        })
    }
}

#[derive(Debug)]
enum RemoteTransportError {
    NotFound,
    Conflict(String),
    Rejected { code: Code, message: String },
    Ambiguous(String),
}

#[cfg(test)]
mod tests {
    use super::request_fingerprint;

    #[test]
    fn request_fingerprint_is_bound_to_route_workspace_and_code() {
        let base = request_fingerprint("route-a", "workspace-a", "print(1)");
        assert_ne!(base, request_fingerprint("route-b", "workspace-a", "print(1)"));
        assert_ne!(base, request_fingerprint("route-a", "workspace-b", "print(1)"));
        assert_ne!(base, request_fingerprint("route-a", "workspace-a", "print(2)"));
    }
}
