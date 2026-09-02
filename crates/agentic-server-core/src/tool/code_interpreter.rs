use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;

use super::{GatewayExecutionContext, GatewayExecutor, ToolError, ToolHandler, ToolOutput, ToolType};
use crate::config::AgentRtExecutorConfig;
use crate::storage::{ClaimRemoteExecution, RemoteExecutionLedger, RemoteExecutionLink};
use crate::types::io::output::{
    CodeInterpreterCall, CodeInterpreterCallStatus, CodeInterpreterOutput, FunctionToolCall, GatewayCallStatus,
};
use crate::types::io::{FunctionTool, OutputItem};
use crate::types::tools::CodeInterpreterToolParam;

const EXECUTION_API_VERSION: &str = "v1";
const COMMAND_SCHEMA_VERSION: &str = "sandbox-command-v1";

#[derive(Clone)]
pub struct RemoteAgentRtExecutor {
    client: Arc<reqwest::Client>,
    config: AgentRtExecutorConfig,
    ledger: RemoteExecutionLedger,
}

impl std::fmt::Debug for RemoteAgentRtExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteAgentRtExecutor")
            .field("endpoint", &self.config.endpoint)
            .field("route_id", &self.config.route_id)
            .finish_non_exhaustive()
    }
}

impl RemoteAgentRtExecutor {
    /// Builds a remote executor from deployment-owned configuration.
    ///
    /// # Errors
    ///
    /// Returns [`ToolError::Config`] when the endpoint, route, or signing key
    /// cannot satisfy the private execution contract.
    pub fn new(
        client: Arc<reqwest::Client>,
        config: AgentRtExecutorConfig,
        ledger: RemoteExecutionLedger,
    ) -> Result<Self, ToolError> {
        if config.endpoint.trim().is_empty() || config.route_id.trim().is_empty() {
            return Err(ToolError::Config(
                "agent-rt endpoint and route_id must not be empty".to_owned(),
            ));
        }
        if config.subject_signing_key.expose().len() < 32 {
            return Err(ToolError::Config(
                "agent-rt subject signing key must contain at least 32 bytes".to_owned(),
            ));
        }
        Ok(Self { client, config, ledger })
    }

    async fn execute_remote(&self, context: GatewayExecutionContext, arguments: &str) -> Result<ToolOutput, ToolError> {
        let (link, request, token) = self.prepare_execution(&context, arguments).await?;
        let mut cancel_on_drop = RemoteCancelOnDrop::new(
            Arc::clone(&self.client),
            self.endpoint().to_owned(),
            self.config.transport_timeout,
            link.execution_id.clone(),
            token.clone(),
        );
        let record = self
            .start_or_recover(&link, &request, &token, context.trace_context.traceparent.as_deref())
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

        let request = StartExecutionRequest {
            api_version: EXECUTION_API_VERSION.to_owned(),
            execution_id: link.execution_id.clone(),
            workspace_id: link.workspace_id.clone(),
            route_id: link.route_id.clone(),
            input: SandboxCommandInput {
                schema_version: COMMAND_SCHEMA_VERSION.to_owned(),
                argv: vec!["python".to_owned(), "-c".to_owned(), args.code],
                cwd: None,
                env: BTreeMap::new(),
                stdin_base64: String::new(),
                artifact_paths: Vec::new(),
            },
            absolute_deadline: chrono::DateTime::<chrono::Utc>::from(
                UNIX_EPOCH + Duration::from_secs(u64::try_from(link.absolute_deadline).unwrap_or_default()),
            )
            .to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            limits: RequestedExecutionLimits::default(),
            provenance: ExecutionProvenance {
                response: link.response_id.clone(),
                conversation: link.conversation_id.clone(),
                call: link.call_id.clone(),
            },
        };
        let token = self.sign_subject(&link)?;
        Ok((link, request, token))
    }

    async fn start_or_recover(
        &self,
        link: &RemoteExecutionLink,
        request: &StartExecutionRequest,
        token: &str,
        traceparent: Option<&str>,
    ) -> Result<ExecutionApiRecord, ToolError> {
        let record = match self.start(request, token, traceparent).await {
            Ok(record) => record,
            Err(RemoteTransportError::Ambiguous(_)) => match self.lookup(link, token, None, Duration::ZERO).await {
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
        Ok(record)
    }

    async fn await_terminal(
        &self,
        context: &GatewayExecutionContext,
        link: &RemoteExecutionLink,
        token: &str,
        mut record: ExecutionApiRecord,
    ) -> Result<ToolOutput, ToolError> {
        loop {
            if record.state.is_known_terminal() || record.state == ExecutionApiState::OutcomeUnknown {
                return self.finish(&context.call_id, link, record).await;
            }
            if context.cancellation.is_cancelled() {
                record = match self.cancel(link, token).await {
                    Ok(record) => record,
                    Err(_) => {
                        return self
                            .finish_unknown(link, "cancellation did not return an authoritative provider outcome")
                            .await;
                    }
                };
                if record.state.is_known_terminal() {
                    return self.finish(&context.call_id, link, record).await;
                }
                return self
                    .finish_unknown(link, "caller cancelled before an authoritative provider outcome")
                    .await;
            }
            if unix_seconds_i64() >= link.absolute_deadline {
                record = match self.lookup(link, token, Some(record.revision), Duration::ZERO).await {
                    Ok(record) => record,
                    Err(_) => {
                        return self
                            .finish_unknown(link, "deadline recovery lookup did not return an authoritative outcome")
                            .await;
                    }
                };
                if record.state.is_known_terminal() || record.state == ExecutionApiState::OutcomeUnknown {
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
                () = context.cancellation.cancelled() => {
                    continue;
                }
                result = self.lookup(link, token, Some(record.revision), self.config.lookup_wait) => {
                    match result {
                        Ok(record) => record,
                        Err(RemoteTransportError::Ambiguous(_)) => {
                            tokio::time::sleep(Duration::from_millis(100)).await;
                            continue;
                        }
                        Err(_) => {
                            return self
                                .finish_unknown(link, "accepted execution could not be reconciled by lookup")
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
        record: ExecutionApiRecord,
    ) -> Result<ToolOutput, ToolError> {
        let outcome = serde_json::to_string(&record)
            .map_err(|error| ToolError::Execution(format!("failed to serialize agent-rt outcome: {error}")))?;
        self.ledger
            .record_outcome(link, record.state.as_str(), &outcome)
            .await
            .map_err(|error| ToolError::Execution(format!("failed to persist remote execution outcome: {error}")))?;
        if record.state == ExecutionApiState::OutcomeUnknown {
            return Err(ToolError::OutcomeUnknown {
                execution_id: link.execution_id.clone(),
                message: "agent-rt cannot determine the provider outcome".to_owned(),
            });
        }
        if record.state != ExecutionApiState::Completed {
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
        request: &StartExecutionRequest,
        token: &str,
        traceparent: Option<&str>,
    ) -> Result<ExecutionApiRecord, RemoteTransportError> {
        let mut builder = self
            .client
            .post(format!("{}/internal/v1/executions", self.endpoint()))
            .bearer_auth(token)
            .timeout(self.config.transport_timeout)
            .json(request);
        if let Some(traceparent) = traceparent {
            builder = builder.header("traceparent", traceparent);
        }
        match builder.send().await {
            Ok(response) => decode_response(response).await,
            Err(error) => Err(RemoteTransportError::Ambiguous(error.to_string())),
        }
    }

    async fn lookup(
        &self,
        link: &RemoteExecutionLink,
        token: &str,
        after_revision: Option<u64>,
        wait: Duration,
    ) -> Result<ExecutionApiRecord, RemoteTransportError> {
        let mut request = self
            .client
            .get(format!(
                "{}/internal/v1/executions/{}",
                self.endpoint(),
                link.execution_id
            ))
            .bearer_auth(token)
            .timeout(self.config.transport_timeout + wait)
            .query(&[("wait_ms", u64::try_from(wait.as_millis()).unwrap_or(u64::MAX))]);
        if let Some(revision) = after_revision {
            request = request.query(&[("after_revision", revision)]);
        }
        match request.send().await {
            Ok(response) => decode_response(response).await,
            Err(error) => Err(RemoteTransportError::Ambiguous(error.to_string())),
        }
    }

    async fn cancel(
        &self,
        link: &RemoteExecutionLink,
        token: &str,
    ) -> Result<ExecutionApiRecord, RemoteTransportError> {
        let response = self
            .client
            .post(format!(
                "{}/internal/v1/executions/{}:cancel",
                self.endpoint(),
                link.execution_id
            ))
            .bearer_auth(token)
            .timeout(self.config.transport_timeout)
            .send()
            .await
            .map_err(|error| RemoteTransportError::Ambiguous(error.to_string()))?;
        decode_response(response).await
    }

    fn endpoint(&self) -> &str {
        self.config.endpoint.trim_end_matches('/')
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
        let signing_input = format!("v1.{payload}");
        let mut mac = Hmac::<Sha256>::new_from_slice(self.config.subject_signing_key.expose().as_bytes())
            .map_err(|_| ToolError::Config("invalid agent-rt subject signing key".to_owned()))?;
        mac.update(signing_input.as_bytes());
        let signature = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
        Ok(format!("{signing_input}.{signature}"))
    }
}

/// Sends best-effort cancellation when an in-flight remote executor future is
/// dropped before it classifies an authoritative outcome. This covers caller
/// disconnect, task abort, and runtime-local timeout without relying on the
/// dropped future being polled again.
struct RemoteCancelOnDrop {
    client: Option<Arc<reqwest::Client>>,
    endpoint: String,
    transport_timeout: Duration,
    execution_id: String,
    token: String,
}

impl RemoteCancelOnDrop {
    fn new(
        client: Arc<reqwest::Client>,
        endpoint: String,
        transport_timeout: Duration,
        execution_id: String,
        token: String,
    ) -> Self {
        Self {
            client: Some(client),
            endpoint,
            transport_timeout,
            execution_id,
            token,
        }
    }

    fn disarm(&mut self) {
        self.client = None;
    }
}

impl Drop for RemoteCancelOnDrop {
    fn drop(&mut self) {
        let Some(client) = self.client.take() else {
            return;
        };
        let endpoint = std::mem::take(&mut self.endpoint);
        let execution_id = std::mem::take(&mut self.execution_id);
        let token = std::mem::take(&mut self.token);
        let transport_timeout = self.transport_timeout;
        if tokio::runtime::Handle::try_current().is_err() {
            return;
        }
        tokio::spawn(async move {
            let result = client
                .post(format!("{endpoint}/internal/v1/executions/{execution_id}:cancel"))
                .bearer_auth(token)
                .timeout(transport_timeout)
                .send()
                .await;
            if let Err(error) = result {
                tracing::debug!(%execution_id, %error, "best-effort agent-rt cancellation failed");
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
        let record: ExecutionApiRecord = serde_json::from_str(&output.output).ok()?;
        let result = record.result.as_ref();
        let mut logs = result.map_or_else(String::new, |result| result.output.stdout.clone());
        if let Some(stderr) = result
            .map(|result| result.output.stderr.as_str())
            .filter(|stderr| !stderr.is_empty())
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
            status: if status == GatewayCallStatus::Completed && result.is_some_and(|result| !result.is_error) {
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

fn request_fingerprint(route_id: &str, workspace_id: &str, code: &str) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"agentic-api-agent-rt-request-v1");
    for component in [route_id, workspace_id, "python", code] {
        hasher.update(&(component.len() as u64).to_le_bytes());
        hasher.update(component.as_bytes());
    }
    format!("blake3:{}", hasher.finalize().to_hex())
}

fn remote_error(error: RemoteTransportError) -> ToolError {
    match error {
        RemoteTransportError::Conflict(message) => {
            ToolError::Execution(format!("agent-rt execution identity conflict: {message}"))
        }
        RemoteTransportError::NotFound => ToolError::Execution("agent-rt execution was not found".to_owned()),
        RemoteTransportError::Rejected { status, message } => {
            ToolError::Execution(format!("agent-rt rejected execution ({status}): {message}"))
        }
        RemoteTransportError::Ambiguous(message) => {
            ToolError::Execution(format!("agent-rt transport outcome is ambiguous: {message}"))
        }
    }
}

async fn decode_response(response: reqwest::Response) -> Result<ExecutionApiRecord, RemoteTransportError> {
    let status = response.status();
    if status == reqwest::StatusCode::NOT_FOUND {
        return Err(RemoteTransportError::NotFound);
    }
    let bytes = response
        .bytes()
        .await
        .map_err(|error| RemoteTransportError::Ambiguous(error.to_string()))?;
    if status == reqwest::StatusCode::CONFLICT {
        return Err(RemoteTransportError::Conflict(
            String::from_utf8_lossy(&bytes).into_owned(),
        ));
    }
    if !status.is_success() {
        return Err(RemoteTransportError::Rejected {
            status: status.as_u16(),
            message: String::from_utf8_lossy(&bytes).into_owned(),
        });
    }
    serde_json::from_slice(&bytes).map_err(|error| RemoteTransportError::Ambiguous(error.to_string()))
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

#[derive(Serialize)]
struct StartExecutionRequest {
    api_version: String,
    execution_id: String,
    workspace_id: String,
    route_id: String,
    input: SandboxCommandInput,
    absolute_deadline: String,
    limits: RequestedExecutionLimits,
    provenance: ExecutionProvenance,
}

#[derive(Serialize)]
struct SandboxCommandInput {
    schema_version: String,
    argv: Vec<String>,
    cwd: Option<String>,
    env: BTreeMap<String, String>,
    stdin_base64: String,
    artifact_paths: Vec<String>,
}

#[derive(Default, Serialize)]
struct RequestedExecutionLimits {
    max_output_bytes: Option<u64>,
    max_artifact_bytes: Option<u64>,
}

#[derive(Serialize)]
struct ExecutionProvenance {
    #[serde(rename = "response_id")]
    response: String,
    #[serde(rename = "conversation_id")]
    conversation: Option<String>,
    #[serde(rename = "call_id")]
    call: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum ExecutionApiState {
    Accepted,
    Running,
    Completed,
    Failed,
    Cancelled,
    TimedOut,
    OutcomeUnknown,
}

impl ExecutionApiState {
    const fn is_known_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled | Self::TimedOut)
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
struct ExecutionApiRecord {
    api_version: String,
    execution_id: String,
    workspace_id: String,
    route_id: String,
    route_version: String,
    request_fingerprint: String,
    revision: u64,
    state: ExecutionApiState,
    result: Option<SandboxCommandResult>,
    failure: Option<serde_json::Value>,
    #[serde(default)]
    artifacts: Vec<serde_json::Value>,
    accepted_at: String,
    completed_at: Option<String>,
}

#[derive(Deserialize, Serialize)]
struct SandboxCommandResult {
    schema_version: String,
    output: SandboxCommandOutput,
    is_error: bool,
}

#[derive(Deserialize, Serialize)]
struct SandboxCommandOutput {
    exit_code: Option<i32>,
    stdout: String,
    stderr: String,
}

enum RemoteTransportError {
    NotFound,
    Conflict(String),
    Rejected { status: u16, message: String },
    Ambiguous(String),
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use axum::extract::{Path, State};
    use axum::http::HeaderMap;
    use axum::routing::{get, post};
    use tokio::sync::Mutex;

    use super::*;

    #[test]
    fn request_fingerprint_is_bound_to_route_workspace_and_code() {
        let base = request_fingerprint("route-a", "workspace-a", "print(1)");
        assert_ne!(base, request_fingerprint("route-b", "workspace-a", "print(1)"));
        assert_ne!(base, request_fingerprint("route-a", "workspace-b", "print(1)"));
        assert_ne!(base, request_fingerprint("route-a", "workspace-a", "print(2)"));
    }

    #[derive(Clone, Default)]
    struct FakeAgentRtState {
        request: Arc<Mutex<Option<serde_json::Value>>>,
        post_count: Arc<AtomicUsize>,
        lookup_count: Arc<AtomicUsize>,
        cancel_count: Arc<AtomicUsize>,
        start_seen: Arc<tokio::sync::Notify>,
    }

    #[derive(Clone)]
    struct RestartAgentRtState {
        known_executions: Arc<Mutex<HashSet<String>>>,
        physical_starts: Arc<AtomicUsize>,
        post_attempts: Arc<AtomicUsize>,
    }

    async fn delayed_start(
        State(state): State<FakeAgentRtState>,
        headers: HeaderMap,
        axum::Json(request): axum::Json<serde_json::Value>,
    ) -> axum::Json<serde_json::Value> {
        assert!(
            headers
                .get(axum::http::header::AUTHORIZATION)
                .and_then(|value| value.to_str().ok())
                .is_some_and(|value| value.starts_with("Bearer v1."))
        );
        state.post_count.fetch_add(1, Ordering::SeqCst);
        *state.request.lock().await = Some(request.clone());
        tokio::time::sleep(Duration::from_millis(80)).await;
        axum::Json(completed_record(&request))
    }

    async fn lookup_execution(
        State(state): State<FakeAgentRtState>,
        Path(execution_id): Path<String>,
    ) -> Result<axum::Json<serde_json::Value>, axum::http::StatusCode> {
        state.lookup_count.fetch_add(1, Ordering::SeqCst);
        let request = state.request.lock().await.clone();
        let Some(request) = request else {
            return Err(axum::http::StatusCode::NOT_FOUND);
        };
        assert_eq!(request["execution_id"], execution_id);
        Ok(axum::Json(completed_record(&request)))
    }

    async fn accepted_start(
        State(state): State<FakeAgentRtState>,
        axum::Json(request): axum::Json<serde_json::Value>,
    ) -> axum::Json<serde_json::Value> {
        state.post_count.fetch_add(1, Ordering::SeqCst);
        *state.request.lock().await = Some(request.clone());
        state.start_seen.notify_one();
        axum::Json(nonterminal_record(&request, "accepted", 1))
    }

    async fn running_lookup(
        State(state): State<FakeAgentRtState>,
        Path(execution_id): Path<String>,
    ) -> Result<axum::Json<serde_json::Value>, axum::http::StatusCode> {
        state.lookup_count.fetch_add(1, Ordering::SeqCst);
        let Some(request) = state.request.lock().await.clone() else {
            return Err(axum::http::StatusCode::NOT_FOUND);
        };
        assert_eq!(request["execution_id"], execution_id);
        tokio::time::sleep(Duration::from_secs(5)).await;
        Ok(axum::Json(nonterminal_record(&request, "running", 2)))
    }

    async fn cancel_execution(
        State(state): State<FakeAgentRtState>,
        Path(execution_action): Path<String>,
    ) -> Result<axum::Json<serde_json::Value>, axum::http::StatusCode> {
        let execution_id = execution_action
            .strip_suffix(":cancel")
            .ok_or(axum::http::StatusCode::NOT_FOUND)?;
        state.cancel_count.fetch_add(1, Ordering::SeqCst);
        let Some(request) = state.request.lock().await.clone() else {
            return Err(axum::http::StatusCode::NOT_FOUND);
        };
        assert_eq!(request["execution_id"], execution_id);
        Ok(axum::Json(nonterminal_record(&request, "cancelled", 3)))
    }

    fn nonterminal_record(request: &serde_json::Value, state: &str, revision: u64) -> serde_json::Value {
        serde_json::json!({
            "api_version": "v1",
            "execution_id": request["execution_id"],
            "workspace_id": request["workspace_id"],
            "route_id": request["route_id"],
            "route_version": "blake3:route-v1",
            "request_fingerprint": "blake3:agent-rt-fingerprint",
            "revision": revision,
            "state": state,
            "result": null,
            "failure": null,
            "artifacts": [],
            "accepted_at": "2026-09-01T00:00:00Z",
            "completed_at": null
        })
    }

    fn completed_record(request: &serde_json::Value) -> serde_json::Value {
        serde_json::json!({
            "api_version": "v1",
            "execution_id": request["execution_id"],
            "workspace_id": request["workspace_id"],
            "route_id": request["route_id"],
            "route_version": "blake3:route-v1",
            "request_fingerprint": "blake3:agent-rt-fingerprint",
            "revision": 2,
            "state": "completed",
            "result": {
                "schema_version": "sandbox-command-result-v1",
                "output": {"exit_code": 0, "stdout": "42\n", "stderr": ""},
                "is_error": false
            },
            "failure": null,
            "artifacts": [],
            "accepted_at": "2026-09-01T00:00:00Z",
            "completed_at": "2026-09-01T00:00:01Z"
        })
    }

    async fn create_or_get_after_restart(
        State(state): State<RestartAgentRtState>,
        axum::Json(request): axum::Json<serde_json::Value>,
    ) -> axum::Json<serde_json::Value> {
        state.post_attempts.fetch_add(1, Ordering::SeqCst);
        let execution_id = request["execution_id"].as_str().expect("execution ID").to_owned();
        if state.known_executions.lock().await.insert(execution_id) {
            state.physical_starts.fetch_add(1, Ordering::SeqCst);
        }
        axum::Json(completed_record(&request))
    }

    #[tokio::test]
    async fn ambiguous_start_is_reconciled_by_lookup_without_redispatch() {
        let fake = FakeAgentRtState::default();
        let app = axum::Router::new()
            .route("/internal/v1/executions", post(delayed_start))
            .route("/internal/v1/executions/{execution_id}", get(lookup_execution))
            .with_state(fake.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake agent-rt");
        let address = listener.local_addr().expect("fake agent-rt address");
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });

        let pool = crate::storage::create_pool_with_schema(Some("sqlite::memory:"))
            .await
            .expect("create execution ledger");
        let executor = RemoteAgentRtExecutor::new(
            Arc::new(reqwest::Client::new()),
            AgentRtExecutorConfig {
                endpoint: format!("http://{address}"),
                route_id: "sandbox.python.default".to_owned(),
                subject_signing_key: crate::config::SubjectSigningKey::new(
                    "0123456789abcdef0123456789abcdef".to_owned(),
                ),
                subject_issuer: "agentic-api".to_owned(),
                subject_audience: "agent-rt".to_owned(),
                tenant_id: "tenant-a".to_owned(),
                default_principal_id: "principal-a".to_owned(),
                execution_timeout: Duration::from_secs(2),
                transport_timeout: Duration::from_millis(20),
                lookup_wait: Duration::from_millis(10),
            },
            RemoteExecutionLedger::new(pool.clone()),
        )
        .expect("remote executor");
        let subject = crate::tool::AuthenticatedSubject {
            tenant_id: "tenant-a".to_owned(),
            principal_id: "principal-a".to_owned(),
        };
        let (execution_id, workspace_id) = crate::storage::remote_execution::stable_execution_identity(
            Some(&subject),
            "resp-a",
            Some("conv-a"),
            "call-a",
        );
        let context = GatewayExecutionContext {
            response_id: "resp-a".to_owned(),
            conversation_id: Some("conv-a".to_owned()),
            call_id: "call-a".to_owned(),
            execution_id,
            workspace_id,
            subject: Some(subject),
            absolute_deadline: Some(SystemTime::now() + Duration::from_secs(2)),
            cancellation: tokio_util::sync::CancellationToken::new(),
            trace_context: crate::tool::TraceContext::default(),
        };

        let output = executor
            .execute_remote(context, r#"{"code":"print(40 + 2)"}"#)
            .await
            .expect("lookup recovers completed result");
        assert_eq!(fake.post_count.load(Ordering::SeqCst), 1);
        assert_eq!(fake.lookup_count.load(Ordering::SeqCst), 1);
        let request = fake.request.lock().await.clone().expect("captured request");
        assert_eq!(request["route_id"], "sandbox.python.default");
        assert_eq!(
            request["input"]["argv"],
            serde_json::json!(["python", "-c", "print(40 + 2)"])
        );
        assert!(request.get("provider").is_none());
        assert!(request.get("credentials").is_none());

        let state: String = sqlx::query_scalar("SELECT state FROM remote_executions WHERE call_id = $1")
            .bind("call-a")
            .fetch_one(pool.as_ref())
            .await
            .expect("load persisted terminal state");
        assert_eq!(state, "completed");

        let public = executor
            .public_output(
                &FunctionToolCall {
                    id: "ci-a".to_owned(),
                    call_id: "call-a".to_owned(),
                    name: "code_interpreter".to_owned(),
                    namespace: None,
                    arguments: r#"{"code":"print(40 + 2)"}"#.to_owned(),
                    status: crate::types::event::MessageStatus::Completed,
                },
                &output,
                GatewayCallStatus::Completed,
                &CodeInterpreterToolParam::default(),
            )
            .expect("native public item");
        let OutputItem::CodeInterpreterCall(public) = public else {
            panic!("expected native code interpreter output");
        };
        assert_eq!(public.status, CodeInterpreterCallStatus::Completed);
        assert!(matches!(
            public.outputs.as_slice(),
            [CodeInterpreterOutput::Logs { logs }] if logs == "42\n"
        ));
        server.abort();
    }

    #[tokio::test]
    async fn dropping_in_flight_remote_execution_sends_best_effort_cancel() {
        let fake = FakeAgentRtState::default();
        let app = axum::Router::new()
            .route("/internal/v1/executions", post(accepted_start))
            .route(
                "/internal/v1/executions/{execution_id}",
                get(running_lookup).post(cancel_execution),
            )
            .with_state(fake.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake agent-rt");
        let address = listener.local_addr().expect("fake agent-rt address");
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });
        let pool = crate::storage::create_pool_with_schema(Some("sqlite::memory:"))
            .await
            .expect("create execution ledger");
        let executor = RemoteAgentRtExecutor::new(
            Arc::new(reqwest::Client::new()),
            AgentRtExecutorConfig {
                endpoint: format!("http://{address}"),
                route_id: "sandbox.python.default".to_owned(),
                subject_signing_key: crate::config::SubjectSigningKey::new(
                    "0123456789abcdef0123456789abcdef".to_owned(),
                ),
                subject_issuer: "agentic-api".to_owned(),
                subject_audience: "agent-rt".to_owned(),
                tenant_id: "tenant-a".to_owned(),
                default_principal_id: "principal-a".to_owned(),
                execution_timeout: Duration::from_secs(30),
                transport_timeout: Duration::from_millis(250),
                lookup_wait: Duration::from_secs(5),
            },
            RemoteExecutionLedger::new(pool),
        )
        .expect("remote executor");
        let subject = crate::tool::AuthenticatedSubject {
            tenant_id: "tenant-a".to_owned(),
            principal_id: "principal-a".to_owned(),
        };
        let (execution_id, workspace_id) = crate::storage::remote_execution::stable_execution_identity(
            Some(&subject),
            "resp-cancel",
            Some("conv-cancel"),
            "call-cancel",
        );
        let context = GatewayExecutionContext {
            response_id: "resp-cancel".to_owned(),
            conversation_id: Some("conv-cancel".to_owned()),
            call_id: "call-cancel".to_owned(),
            execution_id,
            workspace_id,
            subject: Some(subject),
            absolute_deadline: Some(SystemTime::now() + Duration::from_secs(30)),
            cancellation: tokio_util::sync::CancellationToken::new(),
            trace_context: crate::tool::TraceContext::default(),
        };

        let execution =
            tokio::spawn(async move { executor.execute_remote(context, r#"{"code":"while True: pass"}"#).await });
        tokio::time::timeout(Duration::from_secs(2), fake.start_seen.notified())
            .await
            .expect("remote start was not accepted");
        execution.abort();
        let _ = execution.await;
        tokio::time::timeout(Duration::from_secs(2), async {
            while fake.cancel_count.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("best-effort cancel was not sent");
        assert_eq!(fake.cancel_count.load(Ordering::SeqCst), 1);
        assert_eq!(fake.post_count.load(Ordering::SeqCst), 1);
        server.abort();
    }

    fn remove_sqlite_test_database(database_path: &std::path::Path) {
        std::fs::remove_file(database_path).expect("remove restart test database");
        for suffix in ["-shm", "-wal"] {
            let sidecar = format!("{}{suffix}", database_path.display());
            if std::path::Path::new(&sidecar).exists() {
                std::fs::remove_file(sidecar).expect("remove SQLite restart sidecar");
            }
        }
    }

    #[tokio::test]
    async fn process_restart_reuses_durable_ledger_and_existing_remote_execution() {
        let subject = crate::tool::AuthenticatedSubject {
            tenant_id: "tenant-a".to_owned(),
            principal_id: "principal-a".to_owned(),
        };
        let (execution_id, workspace_id) = crate::storage::remote_execution::stable_execution_identity(
            Some(&subject),
            "resp-restart",
            Some("conv-restart"),
            "call-restart",
        );
        let fingerprint = request_fingerprint("sandbox.python.default", &workspace_id, "print(42)");
        let database_path = std::env::temp_dir().join(format!("agentic-api-restart-{}.db", uuid::Uuid::now_v7()));
        let database_url = format!("sqlite://{}?mode=rwc", database_path.display());
        let first_pool = crate::storage::create_pool_with_schema(Some(&database_url))
            .await
            .expect("create durable execution ledger");
        let first_ledger = RemoteExecutionLedger::new(first_pool.clone());
        let first_link = first_ledger
            .claim(crate::storage::ClaimRemoteExecution {
                subject: &subject,
                response_id: "resp-restart",
                conversation_id: Some("conv-restart"),
                call_id: "call-restart",
                route_id: "sandbox.python.default",
                request_fingerprint: &fingerprint,
                absolute_deadline: unix_seconds_i64() + 30,
            })
            .await
            .expect("first process durably claimed call before dispatch");
        assert_eq!(first_link.execution_id, execution_id);
        drop(first_ledger);
        first_pool.close().await;

        let state = RestartAgentRtState {
            known_executions: Arc::new(Mutex::new(HashSet::from([execution_id.clone()]))),
            physical_starts: Arc::new(AtomicUsize::new(1)),
            post_attempts: Arc::new(AtomicUsize::new(0)),
        };
        let app = axum::Router::new()
            .route("/internal/v1/executions", post(create_or_get_after_restart))
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake agent-rt");
        let address = listener.local_addr().expect("fake agent-rt address");
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });
        let recovered_pool = crate::storage::create_pool_with_schema(Some(&database_url))
            .await
            .expect("reopen execution ledger after process restart");
        let executor = RemoteAgentRtExecutor::new(
            Arc::new(reqwest::Client::new()),
            AgentRtExecutorConfig {
                endpoint: format!("http://{address}"),
                route_id: "sandbox.python.default".to_owned(),
                subject_signing_key: crate::config::SubjectSigningKey::new(
                    "0123456789abcdef0123456789abcdef".to_owned(),
                ),
                subject_issuer: "agentic-api".to_owned(),
                subject_audience: "agent-rt".to_owned(),
                tenant_id: "tenant-a".to_owned(),
                default_principal_id: "principal-a".to_owned(),
                execution_timeout: Duration::from_secs(30),
                transport_timeout: Duration::from_millis(250),
                lookup_wait: Duration::from_millis(10),
            },
            RemoteExecutionLedger::new(recovered_pool.clone()),
        )
        .expect("recovered remote executor");
        let context = GatewayExecutionContext {
            response_id: "resp-restart".to_owned(),
            conversation_id: Some("conv-restart".to_owned()),
            call_id: "call-restart".to_owned(),
            execution_id,
            workspace_id,
            subject: Some(subject),
            absolute_deadline: Some(SystemTime::now() + Duration::from_secs(30)),
            cancellation: tokio_util::sync::CancellationToken::new(),
            trace_context: crate::tool::TraceContext::default(),
        };

        executor
            .execute_remote(context, r#"{"code":"print(42)"}"#)
            .await
            .expect("restarted process reconciles existing physical execution");
        assert_eq!(state.post_attempts.load(Ordering::SeqCst), 1);
        assert_eq!(state.physical_starts.load(Ordering::SeqCst), 1);
        let ledger_state: String = sqlx::query_scalar("SELECT state FROM remote_executions")
            .fetch_one(recovered_pool.as_ref())
            .await
            .expect("recovered process persisted terminal state");
        assert_eq!(ledger_state, "completed");
        recovered_pool.close().await;
        remove_sqlite_test_database(&database_path);
        server.abort();
    }
}
