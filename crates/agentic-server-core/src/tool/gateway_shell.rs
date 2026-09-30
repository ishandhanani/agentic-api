//! Gateway-executed `shell` calls backed by a deployment-provided service.
//!
//! Ordinary `shell` declarations stay client-executed. A call reaches this
//! module only when the embedding application registers a [`ShellBackend`]
//! and installs a trusted [`GatewayExecutionPolicy`] that grants shell
//! execution for the request. The backend owns workspaces, isolation, and
//! process lifecycle; this module owns the model-facing contract, limits,
//! public `shell_call`/`shell_call_output` items, and their continuation.

use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use base64::Engine as _;
use ring::hmac;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::handler::{GatewayExecutor, GatewayToolEventPlan, ToolError, ToolHandler, ToolOutput};
use super::registry::ToolType;
use super::shell::{SHELL_FUNCTION_NAME, ShellHandler, public_item_id};
use crate::types::io::output::{FunctionToolCall, GatewayCallStatus, OutputItem, ReasoningOutput};
use crate::types::io::{
    FunctionTool, InputItem, ShellCall, ShellCallAction, ShellCallLimit, ShellCallOutcome, ShellCallOutputContent,
    ShellCallOutputMessage, ShellCallStatus, ShellItemOrigin,
};
use crate::types::tools::{ShellEnvironment, ShellToolParam};

/// Boxed future returned by [`ShellBackend`] methods.
pub type ShellBackendFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Authenticated owner of gateway-executed work, established by the embedding
/// application. It is never read from a request body or model output.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ExecutionSubject {
    pub tenant_id: String,
    pub principal_id: String,
}

/// Trusted per-request execution policy installed by the embedding
/// application, for example by its authentication middleware as a request
/// extension. Registering a backend alone never authorizes execution.
#[derive(Debug, Clone)]
pub struct GatewayExecutionPolicy {
    pub subject: ExecutionSubject,
    /// `None` keeps every `shell` declaration client-executed.
    pub shell: Option<GatewayShellGrant>,
    /// Opaque correlation (session, thread, turn) forwarded to backends for
    /// tracing and workspace naming. It is never authorization.
    pub correlation: BTreeMap<String, String>,
}

/// Grant allowing gateway execution of hosted `shell` environments.
#[derive(Debug, Clone)]
pub struct GatewayShellGrant {
    /// Environment the gateway declares for callers that cannot declare hosted
    /// shell themselves. `None` requires an explicit `shell` declaration.
    pub declare: Option<ShellEnvironmentSelection>,
    /// How gateway shell history is presented to this caller.
    pub client_view: ShellClientView,
}

/// A hosted shell environment request, before backend authorization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShellEnvironmentSelection {
    /// The deployment chooses or creates the environment.
    Auto,
    /// Reuse an existing environment the subject must be authorized for.
    Reference { container_id: String },
}

/// Public projection of gateway shell history.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShellClientView {
    /// Native `shell_call` and `shell_call_output` items.
    Native,
    /// One signed `reasoning` item per call whose `encrypted_content` carries
    /// the canonical items. For stateless clients that discard unknown item
    /// types but replay reasoning verbatim; the gateway restores the carried
    /// items when the client sends them back.
    SealedReasoning,
}

/// Environment resolved and authorized by the backend before inference.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedShellEnvironment {
    pub container_id: String,
}

/// Trusted context for one gateway shell call.
#[derive(Debug, Clone)]
pub struct ShellBackendContext {
    pub subject: ExecutionSubject,
    pub container_id: String,
    pub response_id: String,
    pub call_id: String,
    /// Stable for the logical call; backends derive idempotent command IDs
    /// from it so a retried call never starts a command twice.
    pub execution_id: String,
    pub deadline: Instant,
    pub correlation: BTreeMap<String, String>,
}

/// One command with limits already clamped to operator policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShellCommandRequest {
    pub command: String,
    pub timeout: Duration,
    pub max_output_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShellCommandOutcome {
    Exit(i32),
    Timeout,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShellCommandResult {
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub outcome: ShellCommandOutcome,
    pub output_truncated: bool,
    /// Backend identity of the execution, for traces and audit.
    pub execution_id: String,
}

#[derive(Debug, thiserror::Error)]
pub enum ShellBackendError {
    #[error("shell environment is not authorized for this caller")]
    Unauthorized,
    #[error("shell environment was not found")]
    NotFound,
    #[error("shell environment is no longer available: {0}")]
    Lost(String),
    #[error("shell backend is unavailable: {0}")]
    Unavailable(String),
    #[error("shell command was cancelled")]
    Cancelled,
    /// The command may or may not have run. It stays tracked by the backend
    /// and is never resubmitted automatically.
    #[error("shell command outcome is unknown: {0}")]
    OutcomeUnknown(String),
    #[error("invalid shell request: {0}")]
    Invalid(String),
}

/// Deployment-provided execution service for hosted `shell` environments.
pub trait ShellBackend: Send + Sync + 'static {
    /// Authorizes and resolves an environment before any inference or side
    /// effect. Called once per request that exposes gateway shell.
    fn prepare<'a>(
        &'a self,
        subject: &'a ExecutionSubject,
        environment: &'a ShellEnvironmentSelection,
        correlation: &'a BTreeMap<String, String>,
    ) -> ShellBackendFuture<'a, Result<PreparedShellEnvironment, ShellBackendError>>;

    /// Runs command `index` of a call to completion. Dropping the returned
    /// future must request cancellation of the command; the backend keeps its
    /// capacity charged until termination is confirmed.
    fn run_command<'a>(
        &'a self,
        context: &'a ShellBackendContext,
        index: usize,
        command: &'a ShellCommandRequest,
    ) -> ShellBackendFuture<'a, Result<ShellCommandResult, ShellBackendError>>;
}

/// Operator ceilings applied to model-requested shell arguments.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShellExecutionLimits {
    pub max_commands: usize,
    pub max_command_bytes: usize,
    pub default_timeout: Duration,
    pub max_timeout: Duration,
    pub default_output_bytes: u64,
    pub max_output_bytes: u64,
}

impl Default for ShellExecutionLimits {
    fn default() -> Self {
        Self {
            max_commands: 8,
            max_command_bytes: 64 * 1024,
            default_timeout: Duration::from_secs(120),
            max_timeout: Duration::from_secs(600),
            default_output_bytes: 16 * 1024,
            max_output_bytes: 64 * 1024,
        }
    }
}

/// Registration for gateway shell execution.
#[derive(Clone)]
pub struct GatewayShellConfig {
    pub backend: Arc<dyn ShellBackend>,
    pub limits: ShellExecutionLimits,
    /// HMAC key for sealed client views; at least 32 bytes.
    pub sealing_key: Vec<u8>,
}

impl std::fmt::Debug for GatewayShellConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("GatewayShellConfig")
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}

impl GatewayShellConfig {
    /// # Errors
    /// Rejects weak sealing keys and limits whose aggregate output could
    /// exceed the per-call gateway output bound.
    pub fn validate(&self) -> Result<(), ToolError> {
        if self.sealing_key.len() < 32 {
            return Err(ToolError::Config(
                "gateway shell sealing key must contain at least 32 bytes".to_owned(),
            ));
        }
        let limits = &self.limits;
        let worst_case = u64::try_from(limits.max_commands)
            .unwrap_or(u64::MAX)
            .saturating_mul(limits.max_output_bytes.saturating_mul(2));
        if limits.max_commands == 0
            || limits.max_command_bytes == 0
            || limits.default_timeout.is_zero()
            || limits.default_timeout > limits.max_timeout
            || limits.default_output_bytes == 0
            || limits.default_output_bytes > limits.max_output_bytes
            // Two bytes per output byte leaves room for JSON escaping.
            || worst_case > super::handler::MAX_GATEWAY_TOOL_OUTPUT_BYTES as u64
        {
            return Err(ToolError::Config("invalid gateway shell limits".to_owned()));
        }
        Ok(())
    }
}

/// Shared gateway shell executor, registered once per server.
pub struct GatewayShellExecutor {
    config: GatewayShellConfig,
    sealing_key: hmac::Key,
}

impl std::fmt::Debug for GatewayShellExecutor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("GatewayShellExecutor")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

/// Request-scoped binding for one gateway `shell` declaration.
#[derive(Debug, Clone)]
pub struct GatewayShellBinding {
    pub subject: ExecutionSubject,
    pub container_id: String,
    pub response_id: String,
    pub client_view: ShellClientView,
    pub correlation: BTreeMap<String, String>,
}

impl GatewayShellExecutor {
    /// # Errors
    /// See [`GatewayShellConfig::validate`].
    pub fn new(config: GatewayShellConfig) -> Result<Self, ToolError> {
        config.validate()?;
        let sealing_key = hmac::Key::new(hmac::HMAC_SHA256, &config.sealing_key);
        Ok(Self { config, sealing_key })
    }

    /// Authorizes the hosted environment for this request before inference.
    ///
    /// # Errors
    /// Returns [`ToolError::Config`] when the environment is not hosted or the
    /// backend refuses it for this subject.
    pub async fn bind(
        &self,
        policy: &GatewayExecutionPolicy,
        grant: &GatewayShellGrant,
        environment: &ShellEnvironment,
        response_id: &str,
    ) -> Result<GatewayShellBinding, ToolError> {
        let selection = match environment {
            ShellEnvironment::ContainerAuto(_) => ShellEnvironmentSelection::Auto,
            ShellEnvironment::ContainerReference(reference) => ShellEnvironmentSelection::Reference {
                container_id: reference.container_id.clone(),
            },
            _ => {
                return Err(ToolError::Config(
                    "only container_auto and container_reference shell environments are gateway-executed".to_owned(),
                ));
            }
        };
        let prepared = self
            .config
            .backend
            .prepare(&policy.subject, &selection, &policy.correlation)
            .await
            .map_err(|error| ToolError::Config(format!("shell environment was rejected: {error}")))?;
        Ok(GatewayShellBinding {
            subject: policy.subject.clone(),
            container_id: prepared.container_id,
            response_id: response_id.to_owned(),
            client_view: grant.client_view,
            correlation: policy.correlation.clone(),
        })
    }

    fn parse(&self, arguments: &str) -> Result<(ShellCallAction, Vec<ShellCommandRequest>, u64), ToolError> {
        let action: ShellCallAction = serde_json::from_str(arguments)
            .map_err(|error| ToolError::Execution(format!("invalid shell arguments: {error}")))?;
        let limits = &self.config.limits;
        if action.commands.is_empty() || action.commands.len() > limits.max_commands {
            return Err(ToolError::Execution(format!(
                "shell calls must contain between 1 and {} commands",
                limits.max_commands
            )));
        }
        if action.commands.iter().any(|command| {
            command.trim().is_empty() || command.len() > limits.max_command_bytes || command.contains('\0')
        }) {
            return Err(ToolError::Execution(format!(
                "each shell command must be non-empty and at most {} bytes",
                limits.max_command_bytes
            )));
        }
        let timeout = match action.timeout_ms {
            Some(ShellCallLimit::Value(millis)) if millis > 0 => Duration::from_millis(millis),
            _ => limits.default_timeout,
        }
        .min(limits.max_timeout);
        let max_output_bytes = match action.max_output_length {
            Some(ShellCallLimit::Value(bytes)) if bytes > 0 => bytes,
            _ => limits.default_output_bytes,
        }
        .min(limits.max_output_bytes);
        let commands = action
            .commands
            .iter()
            .map(|command| ShellCommandRequest {
                command: command.clone(),
                timeout,
                max_output_bytes,
            })
            .collect();
        Ok((action, commands, max_output_bytes))
    }

    fn seal(&self, subject: &ExecutionSubject, items: &[InputItem]) -> Result<String, ToolError> {
        let payload = SealedShellHistory {
            version: 1,
            subject: subject_digest(subject),
            items: items.to_vec(),
        };
        let payload = serde_json::to_vec(&payload).map_err(|error| ToolError::Execution(error.to_string()))?;
        let tag = hmac::sign(&self.sealing_key, &payload);
        let encode = |bytes: &[u8]| base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes);
        Ok(format!("{SEAL_PREFIX}{}.{}", encode(&payload), encode(tag.as_ref())))
    }

    /// Restores sealed gateway shell history in client-supplied input.
    ///
    /// # Errors
    /// Rejects carriers that were not issued by this gateway for `subject`.
    pub fn unseal(&self, subject: &ExecutionSubject, items: &mut Vec<InputItem>) -> Result<usize, String> {
        let mut restored = 0;
        let mut index = 0;
        while index < items.len() {
            let Some(token) = sealed_token(&items[index]) else {
                index += 1;
                continue;
            };
            let history = self.open(subject, token)?;
            let count = history.items.len();
            items.splice(index..=index, history.items);
            index += count;
            restored += 1;
        }
        Ok(restored)
    }

    fn open(&self, subject: &ExecutionSubject, token: &str) -> Result<SealedShellHistory, String> {
        let body = token.strip_prefix(SEAL_PREFIX).ok_or("unknown carrier")?;
        let (payload, tag) = body.split_once('.').ok_or("malformed gateway shell history")?;
        let decode = |value: &str| {
            base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(value)
                .map_err(|_| "malformed gateway shell history".to_owned())
        };
        let payload = decode(payload)?;
        hmac::verify(&self.sealing_key, &payload, &decode(tag)?)
            .map_err(|_| "gateway shell history was not issued by this gateway".to_owned())?;
        let history: SealedShellHistory =
            serde_json::from_slice(&payload).map_err(|_| "malformed gateway shell history".to_owned())?;
        if history.version != 1 || history.subject != subject_digest(subject) {
            return Err("gateway shell history belongs to a different caller".to_owned());
        }
        if !history
            .items
            .iter()
            .all(|item| matches!(item, InputItem::ShellCall(_) | InputItem::ShellCallOutput(_)))
        {
            return Err("gateway shell history contains unexpected items".to_owned());
        }
        Ok(history)
    }

    fn public_items(
        call: &FunctionToolCall,
        output: &ToolOutput,
        status: GatewayCallStatus,
        params: &GatewayShellBinding,
    ) -> (ShellCall, ShellCallOutputMessage) {
        let (contents, error) = match serde_json::from_str::<GatewayShellOutput>(&output.output) {
            Ok(GatewayShellOutput::Completed(contents)) => (contents, None),
            Ok(GatewayShellOutput::Failed { error, output }) => (output, Some(error)),
            Err(_) => (Vec::new(), Some("shell output could not be recorded".to_owned())),
        };
        let complete = status == GatewayCallStatus::Completed && error.is_none();
        let item_status = Some(if complete {
            ShellCallStatus::Completed
        } else {
            ShellCallStatus::Incomplete
        });
        let action = serde_json::from_str::<ShellCallAction>(&call.arguments).unwrap_or(ShellCallAction {
            commands: Vec::new(),
            timeout_ms: None,
            max_output_length: None,
            extra: HashMap::new(),
        });
        let max_output_length = match action.max_output_length {
            Some(ShellCallLimit::Value(limit)) => Some(limit),
            _ => None,
        };
        let environment = serde_json::json!({
            "type": "container_reference",
            "container_id": params.container_id,
        });
        let shell_call = ShellCall {
            agent: call.agent.clone(),
            id: Some(public_item_id(&call.id)),
            call_id: call.call_id.clone(),
            action,
            status: item_status,
            extra: HashMap::from([("environment".to_owned(), environment)]),
            origin: ShellItemOrigin::Gateway,
        };
        let mut extra = HashMap::new();
        if let Some(error) = error {
            extra.insert("error".to_owned(), Value::String(error));
        }
        let shell_output = ShellCallOutputMessage {
            id: Some(output_item_id(&call.id)),
            call_id: call.call_id.clone(),
            max_output_length,
            output: contents,
            status: item_status,
            extra,
            origin: ShellItemOrigin::Gateway,
        };
        (shell_call, shell_output)
    }

    fn summary(call: &ShellCall, output: &ShellCallOutputMessage, container_id: &str) -> String {
        let mut summary = format!("Gateway shell in {container_id}:");
        for (index, command) in call.action.commands.iter().enumerate() {
            let outcome = match output.output.get(index).map(|content| &content.outcome) {
                Some(ShellCallOutcome::Exit { exit_code }) => format!("exit {exit_code}"),
                Some(ShellCallOutcome::Timeout) => "timed out".to_owned(),
                _ => "did not complete".to_owned(),
            };
            let command = command.lines().next().unwrap_or_default();
            let _ = write!(summary, "\n$ {} -> {outcome}", truncate_chars(command, 160));
        }
        summary
    }
}

impl ToolHandler for GatewayShellExecutor {
    type ToolParams = ShellToolParam;

    fn tool_type(&self) -> ToolType {
        ToolType::Shell
    }

    fn validate(&self, params: &ShellToolParam) -> Result<(), ToolError> {
        if params.environment.is_hosted() {
            Ok(())
        } else {
            Err(ToolError::Config(
                "gateway shell requires a hosted environment".to_owned(),
            ))
        }
    }

    fn normalize(&self, params: &ShellToolParam) -> Vec<FunctionTool> {
        let mut tools = ShellHandler.normalize(params);
        for tool in &mut tools {
            tool.description = Some(
                "Run one or more shell commands, in order, in a persistent remote workspace rooted at /workspace. \
                 Files persist between calls. Each command runs with `sh -lc` and returns its stdout, stderr, and \
                 exit status."
                    .to_owned(),
            );
        }
        tools
    }
}

impl GatewayExecutor for GatewayShellExecutor {
    type ExecutionParams = GatewayShellBinding;

    fn execute(
        &self,
        call_id: &str,
        _tool_name: &str,
        arguments: &str,
        params: &GatewayShellBinding,
    ) -> Pin<Box<dyn Future<Output = Result<ToolOutput, ToolError>> + Send + '_>> {
        let call_id = call_id.to_owned();
        let arguments = arguments.to_owned();
        let params = params.clone();
        Box::pin(async move {
            let (_, commands, _) = self.parse(&arguments)?;
            let budget = commands.iter().fold(Duration::from_secs(30), |total, command| {
                total.saturating_add(command.timeout)
            });
            let context = ShellBackendContext {
                execution_id: execution_id(&params.subject, &params.response_id, &call_id),
                subject: params.subject,
                container_id: params.container_id,
                response_id: params.response_id,
                call_id: call_id.clone(),
                deadline: Instant::now() + budget,
                correlation: params.correlation,
            };
            let mut contents = Vec::with_capacity(commands.len());
            for (index, command) in commands.iter().enumerate() {
                let started = Instant::now();
                let result = self.config.backend.run_command(&context, index, command).await;
                let elapsed_ms = started.elapsed().as_millis();
                match result {
                    Ok(result) => {
                        tracing::info!(
                            target: "agentic::gateway_shell",
                            call_id = %call_id,
                            command_index = index,
                            backend_execution_id = %result.execution_id,
                            container_id = %context.container_id,
                            elapsed_ms,
                            outcome = ?result.outcome,
                            "gateway shell command finished"
                        );
                        contents.push(output_content(&result));
                    }
                    Err(error) => {
                        tracing::warn!(
                            target: "agentic::gateway_shell",
                            call_id = %call_id,
                            command_index = index,
                            elapsed_ms,
                            %error,
                            "gateway shell command did not complete"
                        );
                        if matches!(error, ShellBackendError::Cancelled) {
                            return Err(ToolError::Execution(error.to_string()));
                        }
                        let failed = GatewayShellOutput::Failed {
                            error: format!("command {} did not complete: {error}", index + 1),
                            output: contents,
                        };
                        return Ok(ToolOutput {
                            call_id,
                            output: serde_json::to_string(&failed)
                                .map_err(|error| ToolError::Execution(error.to_string()))?,
                        });
                    }
                }
            }
            Ok(ToolOutput {
                call_id,
                output: serde_json::to_string(&GatewayShellOutput::Completed(contents))
                    .map_err(|error| ToolError::Execution(error.to_string()))?,
            })
        })
    }

    fn supports_parallel_execution(&self) -> bool {
        // The backend serializes conflicting work per workspace across replicas.
        true
    }

    fn manages_own_deadline(&self) -> bool {
        true
    }

    fn plan_gateway_events(&self, call: &FunctionToolCall, params: &GatewayShellBinding) -> GatewayToolEventPlan {
        let started = match params.client_view {
            ShellClientView::Native => {
                ShellHandler::output_item_with_status(call, ShellCallStatus::InProgress).map(|item| match item {
                    OutputItem::ShellCall(mut shell_call) => {
                        shell_call.origin = ShellItemOrigin::Gateway;
                        OutputItem::ShellCall(shell_call)
                    }
                    other => other,
                })
            }
            ShellClientView::SealedReasoning => {
                let mut placeholder = ReasoningOutput::new(reasoning_item_id(&call.id));
                placeholder.status = Some("in_progress".to_owned());
                Some(OutputItem::Reasoning(placeholder))
            }
        };
        GatewayToolEventPlan::new(started)
    }

    fn public_output(
        &self,
        call: &FunctionToolCall,
        output: &ToolOutput,
        status: GatewayCallStatus,
        params: &GatewayShellBinding,
    ) -> Option<OutputItem> {
        let (shell_call, shell_output) = Self::public_items(call, output, status, params);
        match params.client_view {
            ShellClientView::Native => Some(OutputItem::ShellCall(shell_call)),
            ShellClientView::SealedReasoning => {
                let summary = Self::summary(&shell_call, &shell_output, &params.container_id);
                let token = self
                    .seal(
                        &params.subject,
                        &[
                            InputItem::ShellCall(ShellCall {
                                origin: ShellItemOrigin::Client,
                                ..shell_call
                            }),
                            InputItem::ShellCallOutput(ShellCallOutputMessage {
                                origin: ShellItemOrigin::Client,
                                ..shell_output
                            }),
                        ],
                    )
                    .ok()?;
                let mut carrier = ReasoningOutput::new(reasoning_item_id(&call.id));
                carrier.summary = vec![serde_json::json!({"type": "summary_text", "text": summary})];
                carrier.encrypted_content = Some(Value::String(token));
                carrier.status = Some("completed".to_owned());
                Some(OutputItem::Reasoning(carrier))
            }
        }
    }

    fn trailing_public_outputs(
        &self,
        call: &FunctionToolCall,
        output: &ToolOutput,
        status: GatewayCallStatus,
        params: &GatewayShellBinding,
    ) -> Vec<OutputItem> {
        match params.client_view {
            ShellClientView::Native => {
                let (_, shell_output) = Self::public_items(call, output, status, params);
                vec![OutputItem::ShellCallOutput(shell_output)]
            }
            ShellClientView::SealedReasoning => Vec::new(),
        }
    }
}

const SEAL_PREFIX: &str = "agentic-shell.v1.";

#[derive(Serialize, Deserialize)]
struct SealedShellHistory {
    #[serde(rename = "v")]
    version: u32,
    #[serde(rename = "sub")]
    subject: String,
    items: Vec<InputItem>,
}

/// Model-visible output of one gateway shell call.
#[derive(Serialize, Deserialize)]
#[serde(untagged)]
enum GatewayShellOutput {
    Completed(Vec<ShellCallOutputContent>),
    Failed {
        error: String,
        output: Vec<ShellCallOutputContent>,
    },
}

/// Whether an input item is a sealed gateway shell carrier.
#[must_use]
pub fn is_sealed_shell_carrier(item: &InputItem) -> bool {
    sealed_token(item).is_some()
}

fn sealed_token(item: &InputItem) -> Option<&str> {
    let InputItem::Reasoning(reasoning) = item else {
        return None;
    };
    sealed_reasoning_token(reasoning)
}

fn sealed_reasoning_token(reasoning: &ReasoningOutput) -> Option<&str> {
    reasoning
        .encrypted_content
        .as_ref()
        .and_then(Value::as_str)
        .filter(|token| token.starts_with(SEAL_PREFIX))
}

/// Whether a reasoning item is a gateway-issued shell history carrier. Stored
/// carriers are projections of canonical history recorded separately.
#[must_use]
pub(crate) fn is_sealed_reasoning(reasoning: &ReasoningOutput) -> bool {
    sealed_reasoning_token(reasoning).is_some()
}

fn output_content(result: &ShellCommandResult) -> ShellCallOutputContent {
    let mut extra = HashMap::new();
    if result.output_truncated {
        extra.insert("output_truncated".to_owned(), Value::Bool(true));
    }
    ShellCallOutputContent {
        stdout: String::from_utf8_lossy(&result.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&result.stderr).into_owned(),
        outcome: match result.outcome {
            ShellCommandOutcome::Exit(exit_code) => ShellCallOutcome::Exit { exit_code },
            ShellCommandOutcome::Timeout => ShellCallOutcome::Timeout,
        },
        extra,
    }
}

fn subject_digest(subject: &ExecutionSubject) -> String {
    let digest = ring::digest::digest(
        &ring::digest::SHA256,
        format!("{}\0{}", subject.tenant_id, subject.principal_id).as_bytes(),
    );
    hex(digest.as_ref())
}

/// Stable identity for one logical call, shared by retries of that call.
fn execution_id(subject: &ExecutionSubject, response_id: &str, call_id: &str) -> String {
    let digest = ring::digest::digest(
        &ring::digest::SHA256,
        format!(
            "{}\0{}\0{response_id}\0{call_id}",
            subject.tenant_id, subject.principal_id
        )
        .as_bytes(),
    );
    format!("gsh_{}", &hex(digest.as_ref())[..40])
}

fn output_item_id(item_id: &str) -> String {
    let base = public_item_id(item_id);
    format!("sho_{}", base.trim_start_matches("sh_"))
}

fn reasoning_item_id(item_id: &str) -> String {
    let base = public_item_id(item_id);
    format!("rs_{}", base.trim_start_matches("sh_"))
}

fn hex(bytes: &[u8]) -> String {
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut out, byte| {
            let _ = write!(out, "{byte:02x}");
            out
        })
}

fn truncate_chars(value: &str, limit: usize) -> String {
    if value.chars().count() <= limit {
        value.to_owned()
    } else {
        format!("{}...", value.chars().take(limit).collect::<String>())
    }
}

/// Name the model uses for gateway shell calls.
#[must_use]
pub const fn function_name() -> &'static str {
    SHELL_FUNCTION_NAME
}
