use std::collections::HashMap;
use std::sync::{Arc, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use agent_rt_control::execution_service_client::ExecutionServiceClient;
use agent_rt_control::workspace_file_service_client::WorkspaceFileServiceClient;
use agent_rt_control::workspace_service_client::WorkspaceServiceClient;
use agent_rt_control::{
    CancelExecutionRequest, Command, CreateWorkspaceRequest, DeleteWorkspaceRequest, Execution, ExecutionLimits,
    ExecutionState, FileChunk, FileMetadata, GetExecutionRequest, GetWorkspaceRequest, ReadFileRequest,
    RemoveFileRequest, StartExecutionRequest, StatFileRequest, WatchExecutionRequest, Workspace, WorkspaceState,
    WriteFileRequest,
};
use base64::Engine;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};
use tonic::metadata::MetadataValue;
use tonic::transport::{Channel, Endpoint};
use tonic::{Code, Request, Status};

use super::{AuthenticatedSubject, GatewayExecutionContext, ToolError, ToolOutput};
use crate::config::AgentRtExecutorConfig;
use crate::storage::{ClaimRemoteExecution, RemoteExecutionLedger, RemoteExecutionLink};

#[derive(Clone)]
pub(crate) struct AgentRtExecutor {
    client: AgentRtClient,
    ledger: RemoteExecutionLedger,
    workspace_locks: Arc<Mutex<HashMap<String, Weak<Semaphore>>>>,
}

#[derive(Clone)]
pub(crate) struct AgentRtClient {
    channel: Channel,
    config: AgentRtExecutorConfig,
}

pub(crate) struct RemoteFileWrite {
    pub offset: u64,
    pub data: Vec<u8>,
    pub truncate: bool,
}

pub(crate) struct RemoteCommand {
    pub command: Command,
    pub timeout: Option<Duration>,
    pub max_output_bytes: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WorkspaceResolution {
    CreateOrGet,
    Existing,
}

impl std::fmt::Debug for AgentRtExecutor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AgentRtExecutor")
            .field("endpoint", &self.client.config.endpoint)
            .field("workspace_class_id", &self.client.config.workspace_class_id)
            .field("route_id", &self.client.config.route_id)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for AgentRtClient {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AgentRtClient")
            .field("endpoint", &self.config.endpoint)
            .field("workspace_class_id", &self.config.workspace_class_id)
            .field("route_id", &self.config.route_id)
            .finish_non_exhaustive()
    }
}

impl AgentRtExecutor {
    /// Builds the generated gRPC client over deployment-owned configuration.
    ///
    /// # Errors
    ///
    /// Returns [`ToolError::Config`] when the endpoint, workspace class, route,
    /// or signing key cannot satisfy the private execution contract.
    pub(crate) fn new(config: AgentRtExecutorConfig, ledger: RemoteExecutionLedger) -> Result<Self, ToolError> {
        Ok(Self::from_client(AgentRtClient::new(config)?, ledger))
    }

    pub(crate) fn from_client(client: AgentRtClient, ledger: RemoteExecutionLedger) -> Self {
        Self {
            client,
            ledger,
            workspace_locks: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl AgentRtClient {
    pub(crate) fn new(config: AgentRtExecutorConfig) -> Result<Self, ToolError> {
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
        })
    }

    pub(crate) fn config(&self) -> &AgentRtExecutorConfig {
        &self.config
    }

    pub(crate) async fn create_workspace(
        &self,
        workspace_id: &str,
        workspace_class_id: &str,
        token: &str,
        traceparent: Option<&str>,
    ) -> Result<Workspace, RemoteTransportError> {
        let message = CreateWorkspaceRequest {
            workspace_id: workspace_id.to_owned(),
            workspace_class_id: workspace_class_id.to_owned(),
        };
        let execute = || async {
            let request = grpc_request(message.clone(), token, self.config.transport_timeout, traceparent)?;
            WorkspaceServiceClient::new(self.channel.clone())
                .create_workspace(request)
                .await
                .map(tonic::Response::into_inner)
                .map_err(|status| classify_status(&status))
        };
        match execute().await {
            Ok(workspace) => Ok(workspace),
            Err(RemoteTransportError::Ambiguous(_)) => execute().await,
            Err(error) => Err(error),
        }
    }

    pub(crate) async fn get_workspace(
        &self,
        workspace_id: &str,
        token: &str,
        traceparent: Option<&str>,
    ) -> Result<Workspace, RemoteTransportError> {
        let request = grpc_request(
            GetWorkspaceRequest {
                workspace_id: workspace_id.to_owned(),
            },
            token,
            self.config.transport_timeout,
            traceparent,
        )?;
        WorkspaceServiceClient::new(self.channel.clone())
            .get_workspace(request)
            .await
            .map(tonic::Response::into_inner)
            .map_err(|status| classify_status(&status))
    }

    pub(crate) async fn delete_workspace(
        &self,
        workspace_id: &str,
        token: &str,
        traceparent: Option<&str>,
    ) -> Result<Workspace, RemoteTransportError> {
        let request = grpc_request(
            DeleteWorkspaceRequest {
                workspace_id: workspace_id.to_owned(),
            },
            token,
            self.config.transport_timeout,
            traceparent,
        )?;
        WorkspaceServiceClient::new(self.channel.clone())
            .delete_workspace(request)
            .await
            .map(tonic::Response::into_inner)
            .map_err(|status| classify_status(&status))
    }

    pub(crate) async fn stat_file(
        &self,
        workspace_id: &str,
        path: &str,
        token: &str,
        traceparent: Option<&str>,
    ) -> Result<FileMetadata, RemoteTransportError> {
        let request = grpc_request(
            StatFileRequest {
                workspace_id: workspace_id.to_owned(),
                path: path.to_owned(),
                user: None,
            },
            token,
            self.config.transport_timeout,
            traceparent,
        )?;
        WorkspaceFileServiceClient::new(self.channel.clone())
            .stat_file(request)
            .await
            .map(tonic::Response::into_inner)
            .map_err(|status| classify_status(&status))
    }

    pub(crate) async fn read_file(
        &self,
        workspace_id: &str,
        path: &str,
        offset: u64,
        max_bytes: u64,
        token: &str,
        traceparent: Option<&str>,
    ) -> Result<FileChunk, RemoteTransportError> {
        let request = grpc_request(
            ReadFileRequest {
                workspace_id: workspace_id.to_owned(),
                path: path.to_owned(),
                offset,
                max_bytes,
                user: None,
            },
            token,
            self.config.transport_timeout,
            traceparent,
        )?;
        WorkspaceFileServiceClient::new(self.channel.clone())
            .read_file(request)
            .await
            .map(tonic::Response::into_inner)
            .map_err(|status| classify_status(&status))
    }

    pub(crate) async fn write_file(
        &self,
        workspace_id: &str,
        path: &str,
        write: RemoteFileWrite,
        token: &str,
        traceparent: Option<&str>,
    ) -> Result<FileMetadata, RemoteTransportError> {
        let message = WriteFileRequest {
            workspace_id: workspace_id.to_owned(),
            path: path.to_owned(),
            offset: write.offset,
            data: write.data,
            truncate: write.truncate,
            user: None,
        };
        let execute = || async {
            let request = grpc_request(message.clone(), token, self.config.transport_timeout, traceparent)?;
            WorkspaceFileServiceClient::new(self.channel.clone())
                .write_file(request)
                .await
                .map(tonic::Response::into_inner)
                .map_err(|status| classify_status(&status))
        };
        match execute().await {
            Ok(metadata) => Ok(metadata),
            Err(RemoteTransportError::Ambiguous(_)) => execute().await,
            Err(error) => Err(error),
        }
    }

    pub(crate) async fn remove_file(
        &self,
        workspace_id: &str,
        path: &str,
        token: &str,
        traceparent: Option<&str>,
    ) -> Result<(), RemoteTransportError> {
        let request = grpc_request(
            RemoveFileRequest {
                workspace_id: workspace_id.to_owned(),
                path: path.to_owned(),
                recursive: false,
                user: None,
            },
            token,
            self.config.transport_timeout,
            traceparent,
        )?;
        WorkspaceFileServiceClient::new(self.channel.clone())
            .remove_file(request)
            .await
            .map(|_| ())
            .map_err(|status| classify_status(&status))
    }

    pub(crate) fn sign_subject(&self, subject: &AuthenticatedSubject, token_id: &str) -> Result<String, ToolError> {
        let now = u64::try_from(unix_seconds_i64()).unwrap_or_default();
        let claims = SubjectClaims {
            issuer: self.config.subject_issuer.clone(),
            audience: self.config.subject_audience.clone(),
            tenant_id: subject.tenant_id.clone(),
            principal_id: subject.principal_id.clone(),
            issued_at: now,
            expires_at: now.saturating_add(60),
            token_id: token_id.to_owned(),
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

impl AgentRtExecutor {
    pub(crate) async fn execute_commands_in_workspace(
        &self,
        context: GatewayExecutionContext,
        commands: Vec<RemoteCommand>,
        workspace_resolution: WorkspaceResolution,
    ) -> Result<ToolOutput, ToolError> {
        if commands.is_empty() {
            return Err(ToolError::Config("agent-rt requires at least one command".to_owned()));
        }
        let subject = context.subject.as_ref().ok_or_else(|| {
            ToolError::Config("agent-rt execution requires an authenticated tenant and principal".to_owned())
        })?;
        let _workspace_permit = self.workspace_permit(&context.workspace_id).await?;

        let request_fingerprint = request_fingerprint(&self.client.config.route_id, &context.workspace_id, &commands);
        let proposed_deadline = context
            .absolute_deadline
            .unwrap_or_else(|| SystemTime::now() + self.client.config.execution_timeout)
            .duration_since(UNIX_EPOCH)
            .map_err(|_| ToolError::Config("execution deadline precedes the Unix epoch".to_owned()))?;
        let proposed_deadline = proposed_deadline
            .as_secs()
            .saturating_add(u64::from(proposed_deadline.subsec_nanos() != 0));
        let proposed_deadline = i64::try_from(proposed_deadline)
            .map_err(|_| ToolError::Config("execution deadline exceeds supported range".to_owned()))?;
        let link = self
            .ledger
            .claim(ClaimRemoteExecution {
                subject,
                response_id: &context.response_id,
                conversation_id: context.conversation_id.as_deref(),
                call_id: &context.call_id,
                workspace_id: &context.workspace_id,
                route_id: &self.client.config.route_id,
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
        let token = self.client.sign_subject(subject, &link.execution_id)?;
        match workspace_resolution {
            WorkspaceResolution::CreateOrGet => {
                self.client
                    .create_workspace(
                        &link.workspace_id,
                        &self.client.config.workspace_class_id,
                        &token,
                        context.trace_context.traceparent.as_deref(),
                    )
                    .await
                    .and_then(|workspace| validate_ready_workspace(&workspace, &self.client.config.workspace_class_id))
                    .map_err(remote_error)?;
            }
            WorkspaceResolution::Existing => {
                self.client
                    .get_workspace(&link.workspace_id, &token, context.trace_context.traceparent.as_deref())
                    .await
                    .and_then(|workspace| validate_ready_workspace(&workspace, &self.client.config.workspace_class_id))
                    .map_err(remote_error)?;
            }
        }

        let command_count = commands.len();
        let mut outcomes = Vec::with_capacity(command_count);
        let mut cancelled = false;
        for (command_index, command) in commands.into_iter().enumerate() {
            if context.cancellation.is_cancelled() {
                cancelled = true;
                break;
            }
            let record = self
                .execute_command(&context, &link, &token, command_index, command_count, command)
                .await?;
            cancelled = context.cancellation.is_cancelled() || record.state == ExecutionOutcomeState::Cancelled;
            outcomes.push(record);
            if cancelled {
                break;
            }
        }

        let outcome = serde_json::to_string(&outcomes)
            .map_err(|error| ToolError::Execution(format!("failed to serialize agent-rt outcome: {error}")))?;
        self.ledger
            .record_outcome(&link, if cancelled { "cancelled" } else { "completed" }, &outcome)
            .await
            .map_err(|error| ToolError::Execution(format!("failed to persist remote execution outcome: {error}")))?;
        Ok(ToolOutput {
            call_id: context.call_id,
            output: outcome,
        })
    }

    async fn execute_command(
        &self,
        context: &GatewayExecutionContext,
        link: &RemoteExecutionLink,
        token: &str,
        command_index: usize,
        command_count: usize,
        command: RemoteCommand,
    ) -> Result<ExecutionOutcome, ToolError> {
        let execution_id = if command_count == 1 {
            link.execution_id.clone()
        } else {
            child_execution_id(&link.execution_id, command_index)
        };
        let root_deadline_millis = u64::try_from(link.absolute_deadline)
            .unwrap_or_default()
            .saturating_mul(1_000);
        let command_deadline_millis = command
            .timeout
            .and_then(|timeout| SystemTime::now().checked_add(timeout))
            .and_then(|deadline| deadline.duration_since(UNIX_EPOCH).ok())
            .and_then(|duration| u64::try_from(duration.as_millis()).ok())
            .map_or(root_deadline_millis, |deadline| deadline.min(root_deadline_millis));
        let request = StartExecutionRequest {
            execution_id: execution_id.clone(),
            workspace_id: link.workspace_id.clone(),
            route_id: link.route_id.clone(),
            command: Some(command.command),
            absolute_deadline_unix_millis: command_deadline_millis,
            limits: Some(ExecutionLimits {
                max_output_bytes: command.max_output_bytes,
                max_artifact_bytes: None,
            }),
            client_metadata: [
                ("response_id".to_owned(), link.response_id.clone()),
                ("call_id".to_owned(), link.call_id.clone()),
                ("command_index".to_owned(), command_index.to_string()),
            ]
            .into_iter()
            .chain(
                link.conversation_id
                    .clone()
                    .map(|value| ("conversation_id".to_owned(), value)),
            )
            .collect(),
        };
        let mut cancel_on_drop = RemoteCancelOnDrop::new(
            self.client.channel.clone(),
            self.client.config.transport_timeout,
            execution_id.clone(),
            token.to_owned(),
        );
        let record = self
            .start_or_recover(
                link,
                &execution_id,
                request,
                token,
                context.trace_context.traceparent.as_deref(),
            )
            .await?;
        let outcome = self
            .await_terminal(context, link, &execution_id, command_deadline_millis, token, record)
            .await?;
        cancel_on_drop.disarm();
        Ok(outcome)
    }

    async fn workspace_permit(&self, workspace_id: &str) -> Result<OwnedSemaphorePermit, ToolError> {
        let semaphore = {
            let mut locks = self.workspace_locks.lock().await;
            locks.retain(|_, semaphore| semaphore.strong_count() > 0);
            locks.get(workspace_id).and_then(Weak::upgrade).unwrap_or_else(|| {
                let semaphore = Arc::new(Semaphore::new(1));
                locks.insert(workspace_id.to_owned(), Arc::downgrade(&semaphore));
                semaphore
            })
        };
        semaphore
            .acquire_owned()
            .await
            .map_err(|_| ToolError::Execution("agent-rt workspace serializer closed unexpectedly".to_owned()))
    }

    async fn start_or_recover(
        &self,
        link: &RemoteExecutionLink,
        execution_id: &str,
        request: StartExecutionRequest,
        token: &str,
        traceparent: Option<&str>,
    ) -> Result<ExecutionOutcome, ToolError> {
        let record = match self.start(request.clone(), token, traceparent).await {
            Ok(record) => record,
            Err(RemoteTransportError::Ambiguous(_)) => match self.lookup(execution_id, token).await {
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
        execution_id: &str,
        deadline_unix_millis: u64,
        token: &str,
        mut record: ExecutionOutcome,
    ) -> Result<ExecutionOutcome, ToolError> {
        loop {
            if record.state.is_terminal() {
                return Ok(record);
            }
            if context.cancellation.is_cancelled() {
                record = self
                    .cancel(execution_id, token)
                    .await
                    .and_then(ExecutionOutcome::try_from)
                    .map_err(remote_error)?;
                return Ok(record);
            }
            if unix_millis_u64() >= deadline_unix_millis {
                record = match self
                    .lookup(execution_id, token)
                    .await
                    .and_then(ExecutionOutcome::try_from)
                {
                    Ok(record) => record,
                    Err(_) => {
                        return self
                            .finish_unknown(link, "deadline recovery lookup did not return an authoritative outcome")
                            .await;
                    }
                };
                if record.state.is_terminal() {
                    return Ok(record);
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
                result = self.watch_once(execution_id, token, record.revision) => {
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
        let request = grpc_request(message, token, self.client.config.transport_timeout, traceparent)?;
        ExecutionServiceClient::new(self.client.channel.clone())
            .start_execution(request)
            .await
            .map(tonic::Response::into_inner)
            .map_err(|status| classify_status(&status))
    }

    async fn lookup(&self, execution_id: &str, token: &str) -> Result<Execution, RemoteTransportError> {
        let request = grpc_request(
            GetExecutionRequest {
                execution_id: execution_id.to_owned(),
            },
            token,
            self.client.config.transport_timeout,
            None,
        )?;
        ExecutionServiceClient::new(self.client.channel.clone())
            .get_execution(request)
            .await
            .map(tonic::Response::into_inner)
            .map_err(|status| classify_status(&status))
    }

    async fn watch_once(
        &self,
        execution_id: &str,
        token: &str,
        after_revision: u64,
    ) -> Result<Execution, RemoteTransportError> {
        let request = grpc_request(
            WatchExecutionRequest {
                execution_id: execution_id.to_owned(),
                after_revision,
            },
            token,
            self.client
                .config
                .transport_timeout
                .saturating_add(self.client.config.lookup_wait),
            None,
        )?;
        let mut stream = ExecutionServiceClient::new(self.client.channel.clone())
            .watch_execution(request)
            .await
            .map_err(|status| classify_status(&status))?
            .into_inner();
        match tokio::time::timeout(self.client.config.lookup_wait, stream.message()).await {
            Ok(Ok(Some(record))) => Ok(record),
            Ok(Ok(None)) => self.lookup(execution_id, token).await,
            Ok(Err(status)) => Err(classify_status(&status)),
            Err(_) => Err(RemoteTransportError::Ambiguous(
                "execution watch timed out before a revision".to_owned(),
            )),
        }
    }

    async fn cancel(&self, execution_id: &str, token: &str) -> Result<Execution, RemoteTransportError> {
        cancel_execution(
            self.client.channel.clone(),
            self.client.config.transport_timeout,
            execution_id.to_owned(),
            token,
        )
        .await
    }
}

fn validate_ready_workspace(workspace: &Workspace, expected_class_id: &str) -> Result<(), RemoteTransportError> {
    if workspace.workspace_class_id != expected_class_id {
        return Err(RemoteTransportError::Rejected {
            code: Code::FailedPrecondition,
            message: format!(
                "workspace class mismatch: expected {expected_class_id}, got {}",
                workspace.workspace_class_id
            ),
        });
    }
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

fn request_fingerprint(route_id: &str, workspace_id: &str, commands: &[RemoteCommand]) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"agentic-api-agent-rt-request");
    hash_bytes(&mut hasher, route_id.as_bytes());
    hash_bytes(&mut hasher, workspace_id.as_bytes());
    hash_len(&mut hasher, commands.len());
    for remote in commands {
        hash_len(&mut hasher, remote.command.argv.len());
        for argument in &remote.command.argv {
            hash_bytes(&mut hasher, argument.as_bytes());
        }
        hash_option_bytes(&mut hasher, remote.command.cwd.as_deref().map(str::as_bytes));
        let mut environment = remote.command.env.iter().collect::<Vec<_>>();
        environment.sort_unstable_by(|left, right| left.0.cmp(right.0));
        hash_len(&mut hasher, environment.len());
        for (name, value) in environment {
            hash_bytes(&mut hasher, name.as_bytes());
            hash_bytes(&mut hasher, value.as_bytes());
        }
        hash_bytes(&mut hasher, &remote.command.stdin);
        hash_len(&mut hasher, remote.command.artifact_paths.len());
        for path in &remote.command.artifact_paths {
            hash_bytes(&mut hasher, path.as_bytes());
        }
        hash_option_u128(&mut hasher, remote.timeout.map(|timeout| timeout.as_nanos()));
        hash_option_u64(&mut hasher, remote.max_output_bytes);
    }
    format!("blake3:{}", hasher.finalize().to_hex())
}

fn hash_bytes(hasher: &mut blake3::Hasher, value: &[u8]) {
    hasher.update(&(value.len() as u64).to_le_bytes());
    hasher.update(value);
}

fn hash_len(hasher: &mut blake3::Hasher, value: usize) {
    hasher.update(&u64::try_from(value).unwrap_or(u64::MAX).to_le_bytes());
}

fn hash_option_bytes(hasher: &mut blake3::Hasher, value: Option<&[u8]>) {
    match value {
        Some(value) => {
            hasher.update(&[1]);
            hash_bytes(hasher, value);
        }
        None => {
            hasher.update(&[0]);
        }
    }
}

fn hash_option_u64(hasher: &mut blake3::Hasher, value: Option<u64>) {
    match value {
        Some(value) => {
            hasher.update(&[1]);
            hash_bytes(hasher, &value.to_le_bytes());
        }
        None => {
            hasher.update(&[0]);
        }
    }
}

fn hash_option_u128(hasher: &mut blake3::Hasher, value: Option<u128>) {
    match value {
        Some(value) => {
            hasher.update(&[1]);
            hash_bytes(hasher, &value.to_le_bytes());
        }
        None => {
            hasher.update(&[0]);
        }
    }
}

fn child_execution_id(root_execution_id: &str, command_index: usize) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"agentic-api-agent-rt-child-execution");
    hash_bytes(&mut hasher, root_execution_id.as_bytes());
    hasher.update(&u64::try_from(command_index).unwrap_or(u64::MAX).to_le_bytes());
    format!("exec_{}", hasher.finalize().to_hex())
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

fn unix_millis_u64() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| u64::try_from(duration.as_millis()).ok())
        .unwrap_or_default()
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
pub(crate) enum ExecutionOutcomeState {
    Accepted,
    Running,
    Completed,
    Failed,
    Cancelled,
    TimedOut,
    OutcomeUnknown,
}

impl ExecutionOutcomeState {
    pub(crate) const fn is_terminal(self) -> bool {
        !matches!(self, Self::Accepted | Self::Running)
    }
}

#[derive(Deserialize, Serialize)]
pub(crate) struct ExecutionOutcome {
    pub execution_id: String,
    pub workspace_id: String,
    pub route_id: String,
    pub revision: u64,
    pub state: ExecutionOutcomeState,
    pub result: Option<ExecutionResultProjection>,
    pub failure_code: Option<String>,
}

#[derive(Deserialize, Serialize)]
pub(crate) struct ExecutionResultProjection {
    pub exit_code: Option<i32>,
    pub stdout: String,
    pub stderr: String,
    pub output_truncated: bool,
    pub is_error: bool,
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

#[derive(Debug, thiserror::Error)]
pub(crate) enum RemoteTransportError {
    #[error("not found")]
    NotFound,
    #[error("identity conflict: {0}")]
    Conflict(String),
    #[error("request rejected ({code:?}): {message}")]
    Rejected { code: Code, message: String },
    #[error("ambiguous transport outcome: {0}")]
    Ambiguous(String),
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use agent_rt_control::Command;

    use super::{RemoteCommand, request_fingerprint};

    fn command(source: &str) -> RemoteCommand {
        RemoteCommand {
            command: Command {
                argv: vec!["python".to_owned(), "-c".to_owned(), source.to_owned()],
                user: None,
                cwd: None,
                env: HashMap::new(),
                stdin: Vec::new(),
                artifact_paths: Vec::new(),
            },
            timeout: None,
            max_output_bytes: None,
        }
    }

    #[test]
    fn request_fingerprint_is_bound_to_route_workspace_and_code() {
        let base = request_fingerprint("route-a", "workspace-a", &[command("print(1)")]);
        assert_ne!(
            base,
            request_fingerprint("route-b", "workspace-a", &[command("print(1)")])
        );
        assert_ne!(
            base,
            request_fingerprint("route-a", "workspace-b", &[command("print(1)")])
        );
        assert_ne!(
            base,
            request_fingerprint("route-a", "workspace-a", &[command("print(2)")])
        );
    }
}
