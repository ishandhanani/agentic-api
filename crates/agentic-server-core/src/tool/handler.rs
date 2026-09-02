use std::future::Future;
use std::pin::Pin;
use std::time::SystemTime;

use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use crate::types::io::FunctionTool;
use crate::types::io::output::{FunctionToolCall, GatewayCallStatus, OutputItem};

#[derive(Debug, Clone)]
pub struct ToolOutput {
    pub call_id: String,
    pub output: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthenticatedSubject {
    pub tenant_id: String,
    pub principal_id: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TraceContext {
    pub traceparent: Option<String>,
    pub tracestate: Option<String>,
}

/// Request identity and control signals supplied to one gateway-executed
/// built-in tool call. Local executors may ignore fields they do not need;
/// side-effecting remote executors must consume the stable execution identity.
#[derive(Debug, Clone)]
pub struct GatewayExecutionContext {
    pub response_id: String,
    pub conversation_id: Option<String>,
    pub call_id: String,
    pub execution_id: String,
    pub workspace_id: String,
    pub subject: Option<AuthenticatedSubject>,
    pub absolute_deadline: Option<SystemTime>,
    pub cancellation: CancellationToken,
    pub trace_context: TraceContext,
}

impl GatewayExecutionContext {
    pub(crate) fn compatibility(call_id: &str) -> Self {
        let (execution_id, workspace_id) =
            crate::storage::remote_execution::stable_execution_identity(None, "compatibility", None, call_id);
        Self {
            response_id: "compatibility".to_owned(),
            conversation_id: None,
            call_id: call_id.to_owned(),
            execution_id,
            workspace_id,
            subject: None,
            absolute_deadline: None,
            cancellation: CancellationToken::new(),
            trace_context: TraceContext::default(),
        }
    }
}

/// Tool-owned public lifecycle projection for one scheduled gateway call.
///
/// Handlers provide typed Responses output items while the gateway scheduler
/// remains responsible for output indexes, SSE framing, and event ordering.
#[derive(Debug, Clone, Default)]
pub struct GatewayToolEventPlan {
    started_outputs: Vec<Option<OutputItem>>,
}

impl GatewayToolEventPlan {
    #[must_use]
    pub fn one(started_output: Option<OutputItem>) -> Self {
        Self {
            started_outputs: vec![started_output],
        }
    }

    #[must_use]
    pub fn with_slots(started_outputs: Vec<Option<OutputItem>>) -> Self {
        Self { started_outputs }
    }

    #[must_use]
    pub(crate) fn into_started_outputs(self) -> Vec<Option<OutputItem>> {
        self.started_outputs
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ToolError {
    #[error("execution failed: {0}")]
    Execution(String),
    #[error("invalid tool config: {0}")]
    Config(String),
    #[error("remote execution outcome is unknown for {execution_id}: {message}")]
    OutcomeUnknown { execution_id: String, message: String },
    /// A continuation request omitted the output for a pending function call
    /// from the prior turn.
    #[error("No tool output found for function call {call_id}.")]
    MissingOutput { call_id: String },
}

/// Trait implemented by every tool type — client-owned and gateway-owned alike.
///
/// Covers typed validation and normalization: the steps that apply to all
/// tools regardless of who executes them.
pub trait ToolHandler: Send + Sync {
    /// The public declaration parameters handled by this implementation.
    type ToolParams: Send + Sync;

    #[must_use]
    fn tool_type(&self) -> super::registry::ToolType;

    /// Validate the typed tool declaration parameters.
    ///
    /// # Errors
    ///
    /// Returns [`ToolError::Config`] for obviously invalid configurations.
    fn validate(&self, params: &Self::ToolParams) -> Result<(), ToolError>;

    /// Normalise this tool declaration into vLLM-compatible `FunctionTool` entries.
    #[must_use]
    fn normalize(&self, params: &Self::ToolParams) -> Vec<FunctionTool>;
}

/// Extension of [`ToolHandler`] for tool types that are executed by the gateway.
///
/// Only executable gateway handlers implement this trait. Client-owned tools
/// (`Function`, `Custom`, `CodexNamespace`) do not implement it, so they cannot
/// be dispatched through this interface.
///
/// ## Note on `async fn` in traits
///
/// Native `async fn` in traits (Rust 1.75+) is not yet `dyn`-compatible, so this
/// trait uses explicit `Pin<Box<dyn Future>>` return types. Concrete executors
/// are paired with their typed parameters before the pair is erased for storage
/// in the heterogeneous tool registry.
pub trait GatewayExecutor: ToolHandler + 'static {
    /// Request-scoped parameters for one model-visible executable tool.
    ///
    /// These may differ from [`ToolHandler::ToolParams`]. MCP, for example,
    /// normalizes an [`McpToolParam`](crate::types::tools::McpToolParam) server
    /// declaration but executes one
    /// [`McpDiscoveredToolParam`](crate::types::tools::McpDiscoveredToolParam).
    type ExecutionParams: Clone + Send + Sync + 'static;

    /// Execute a tool call and return the result.
    ///
    /// # Errors
    ///
    /// Returns [`ToolError::Execution`] if the tool call fails.
    fn execute(
        &self,
        call_id: &str,
        tool_name: &str,
        arguments: &str,
        params: &Self::ExecutionParams,
    ) -> Pin<Box<dyn Future<Output = Result<ToolOutput, ToolError>> + Send + '_>>;

    /// Execute with request identity. Existing in-process executors use the
    /// compatibility default; remote executors override this method.
    fn execute_with_context(
        &self,
        context: GatewayExecutionContext,
        tool_name: &str,
        arguments: &str,
        params: &Self::ExecutionParams,
    ) -> Pin<Box<dyn Future<Output = Result<ToolOutput, ToolError>> + Send + '_>> {
        let call_id = context.call_id;
        let tool_name = tool_name.to_owned();
        let arguments = arguments.to_owned();
        let params = params.clone();
        Box::pin(async move { self.execute(&call_id, &tool_name, &arguments, &params).await })
    }

    /// Whether multiple calls to this same model-visible tool name may overlap.
    /// Defaults to `false`, which serializes only same-name calls; calls to
    /// different tools may still execute concurrently in the same round.
    #[must_use]
    fn supports_parallel_execution(&self) -> bool {
        false
    }

    /// Whether this executor owns its authoritative deadline and recovery.
    ///
    /// The gateway must not drop such an executor behind a generic local
    /// timeout: doing so could abandon a remote side effect without lookup or
    /// cancellation. Executors returning `true` must bound their own work and
    /// reconcile ambiguous transport outcomes before returning.
    #[must_use]
    fn manages_execution_deadline(&self) -> bool {
        false
    }

    /// Plans the typed public lifecycle for one gateway call.
    ///
    /// The returned plan must not assign protocol indexes or construct SSE
    /// frames. Defaults to an empty lifecycle for tools that have no public
    /// gateway-specific call item.
    #[must_use]
    fn plan_gateway_events(&self, call: &FunctionToolCall, params: &Self::ExecutionParams) -> GatewayToolEventPlan {
        GatewayToolEventPlan::one(self.started_output(call, params))
    }

    /// The placeholder output item shown while this call is in progress.
    ///
    /// Kept as the compatibility hook for existing gateway executors;
    /// implementations that need richer planning should override
    /// [`Self::plan_gateway_events`] instead.
    #[must_use]
    fn started_output(&self, call: &FunctionToolCall, params: &Self::ExecutionParams) -> Option<OutputItem> {
        let _ = (call, params);
        None
    }

    /// The public output items for a completed or failed call.
    /// Defaults to an empty projection.
    #[must_use]
    fn public_outputs(
        &self,
        call: &FunctionToolCall,
        output: &ToolOutput,
        status: GatewayCallStatus,
        params: &Self::ExecutionParams,
    ) -> Vec<OutputItem> {
        let _ = (call, output, status, params);
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    // Compile-time check: a GatewayExecutor with fixed associated parameter
    // types remains dyn-compatible for typed executor slots.
    fn _assert_gateway_executor_dyn_compatible(_: Arc<dyn GatewayExecutor<ToolParams = (), ExecutionParams = ()>>) {}
}
