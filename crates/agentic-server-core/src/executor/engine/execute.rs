//! The request façade: [`ExecuteRequest`] prepares one stateful turn,
//! opens its `agentic.execute` span, and hands off to the blocking or
//! streaming driver.

use std::sync::Arc;

use either::Either;
use tracing::{Instrument as _, debug};

use super::run_blocking;
use super::streaming::run_stream;
use crate::executor::error::ExecutorError;
use crate::executor::error::ExecutorResult;
use crate::executor::inference::BoxStream;
use crate::executor::prepare::prepare_request_tools;
use crate::executor::rehydrate::validate_reasoning_for_vllm;
use crate::executor::request::ExecutionContext;
use crate::executor::telemetry::{Api, ExecutionSpan, Route};
use crate::tool::gateway_shell::is_sealed_shell_carrier;
use crate::tool::{GatewayExecutionPolicy, ShellEnvironmentSelection};
use crate::types::io::ResponsesInput;
use crate::types::request_response::{RequestPayload, ResponsePayload};
use crate::types::tools::{
    ContainerAutoShellEnvironment, ContainerReferenceShellEnvironment, ResponsesTool, ShellEnvironment, ShellToolParam,
};

/// Builder for a stateful conversation turn.
///
/// ```ignore
/// ExecuteRequest::new(payload, exec_ctx).with_auth(token).run().await
/// ```
pub struct ExecuteRequest {
    payload: RequestPayload,
    exec_ctx: Arc<ExecutionContext>,
    client_auth: Option<String>,
    continuation: Option<crate::executor::session::ResponseContinuation>,
    max_stream_event_bytes: Option<usize>,
    execution: Option<ExecutionSpan>,
}

impl ExecuteRequest {
    #[must_use]
    pub fn new(payload: RequestPayload, exec_ctx: Arc<ExecutionContext>) -> Self {
        Self {
            payload,
            exec_ctx,
            client_auth: None,
            continuation: None,
            max_stream_event_bytes: None,
            execution: None,
        }
    }

    /// Bound every serialized client event, including the terminal
    /// `response.completed`, to what the delivering transport can carry after
    /// its own routing metadata. The configured `max_stream_event_bytes` still
    /// applies; a larger transport limit does not raise it.
    #[must_use]
    pub fn with_max_stream_event_bytes(mut self, max_bytes: usize) -> Self {
        self.max_stream_event_bytes = Some(max_bytes);
        self
    }

    fn effective_max_stream_event_bytes(&self) -> usize {
        let configured = self.exec_ctx.responses_config.max_stream_event_bytes;
        self.max_stream_event_bytes
            .map_or(configured, |transport| transport.min(configured))
    }

    /// Attach the trusted execution policy established by the embedding
    /// application for this request (for example by its authentication layer).
    /// Without a policy every `shell` declaration stays client-executed.
    #[must_use]
    pub fn with_execution_policy(mut self, policy: Option<Arc<GatewayExecutionPolicy>>) -> Self {
        self.payload.execution_policy = policy;
        self
    }

    /// Override the bearer token for this request only; does not touch the shared [`ExecutionContext`].
    #[must_use]
    pub fn with_auth(mut self, token: Option<String>) -> Self {
        self.client_auth = token;
        self
    }

    /// Continue the execution span opened by a transport when it admitted the request.
    /// The executor takes responsibility for finalizing its outcomes.
    #[must_use]
    pub fn with_execution_span(mut self, execution: ExecutionSpan) -> Self {
        self.execution = Some(execution);
        self
    }

    /// Retain this turn's continuation state in the supplied serial session.
    ///
    /// # Errors
    /// Returns an error when the session is busy or closed.
    pub fn with_session(mut self, session: &crate::executor::session::ResponseSession) -> ExecutorResult<Self> {
        self.continuation = Some(
            session
                .begin(self.payload.previous_response_id.as_deref())
                .inspect_err(|error| {
                    if let Some(execution) = &mut self.execution {
                        execution.failed(error);
                        execution.not_delivered();
                    }
                })?,
        );
        Ok(self)
    }

    /// Execute one stateful conversation turn.
    ///
    /// Returns `Either::Left(ResponsePayload)` for non-streaming requests, or
    /// `Either::Right(BoxStream)` for streaming, where each yielded `String` is
    /// a complete SSE frame ready to forward to the client.
    ///
    /// # Errors
    /// Returns [`crate::executor::error::ExecutorError`] if rehydration or (non-streaming) LLM inference fails.
    pub async fn run(mut self) -> ExecutorResult<Either<ResponsePayload, BoxStream>> {
        let execution = self
            .execution
            .take()
            .unwrap_or_else(|| ExecutionSpan::start(Api::Responses, Route::Executor, self.payload.stream));
        let span = execution.span().clone();
        self.run_traced(execution).instrument(span).await
    }

    /// The body of [`Self::run`], executed inside the `agentic.execute` span.
    ///
    /// Takes the span guard by value so the streaming path can move it into
    /// the stream, where it lives until the last frame is yielded or the
    /// stream is dropped. Every other path finalizes it here.
    async fn run_traced(self, mut execution: ExecutionSpan) -> ExecutorResult<Either<ResponsePayload, BoxStream>> {
        debug!(
            model = %self.payload.model,
            store = self.payload.store,
            stream = self.payload.stream,
            has_previous_response_id = self.payload.previous_response_id.is_some(),
            has_conversation_id = self.payload.conversation_id.is_some(),
            tools = self.payload.tools.as_ref().map_or(0, Vec::len),
            "executor received responses request"
        );
        let max_stream_event_bytes = self.effective_max_stream_event_bytes();
        let mut payload = self.payload;
        if let Err(error) = apply_execution_policy(&mut payload, &self.exec_ctx).await {
            execution.failed(&error);
            execution.not_delivered();
            return Err(error);
        }
        let prepared = async {
            let ctx =
                crate::executor::rehydrate::rehydrate_with_continuation(payload, &self.exec_ctx, self.continuation)
                    .await?;
            if !ctx.enriched_request.input.has_compaction_trigger() {
                validate_reasoning_for_vllm(&ctx.enriched_request.input)?;
            }
            prepare_request_tools(ctx, &self.exec_ctx.conv_handler, &self.exec_ctx.resp_handler).await
        }
        .await;
        let (ctx, tool_search_state) = match prepared {
            Ok(prepared) => prepared,
            Err(error) => {
                execution.failed(&error);
                execution.not_delivered();
                return Err(error);
            }
        };
        if ctx.original_request.stream {
            return Ok(Either::Right(run_stream(
                ctx,
                tool_search_state,
                self.exec_ctx,
                self.client_auth,
                max_stream_event_bytes,
                execution,
            )));
        }
        let result = Box::pin(run_blocking(
            ctx,
            tool_search_state,
            &self.exec_ctx,
            self.client_auth.as_deref(),
            max_stream_event_bytes,
        ))
        .await;
        match result {
            Ok(payload) => {
                execution.completed_with_status(&payload.status);
                execution.delivered();
                Ok(Either::Left(payload))
            }
            Err(error) => {
                execution.failed(&error);
                execution.not_delivered();
                Err(error)
            }
        }
    }
}

/// Applies the trusted execution policy before any storage or inference work:
/// declares granted gateway shell for callers that cannot declare it, and
/// restores sealed gateway shell history the caller replayed.
async fn apply_execution_policy(payload: &mut RequestPayload, exec_ctx: &ExecutionContext) -> ExecutorResult<()> {
    let has_carriers = match &payload.input {
        ResponsesInput::Items(items) => items.iter().any(is_sealed_shell_carrier),
        ResponsesInput::Text(_) => false,
    };
    let Some(policy) = payload.execution_policy.clone() else {
        if has_carriers {
            return Err(ExecutorError::InvalidRequest(
                "gateway shell history can only be replayed by an authenticated caller".to_owned(),
            ));
        }
        return Ok(());
    };
    if let Some(selection) = policy.shell.as_ref().and_then(|grant| grant.declare.as_ref()) {
        let tools = payload.tools.get_or_insert_with(Vec::new);
        if !tools.iter().any(|tool| matches!(tool, ResponsesTool::Shell(_))) {
            let environment = match selection {
                ShellEnvironmentSelection::Auto => {
                    ShellEnvironment::ContainerAuto(ContainerAutoShellEnvironment::default())
                }
                ShellEnvironmentSelection::Reference { container_id } => {
                    ShellEnvironment::ContainerReference(ContainerReferenceShellEnvironment {
                        container_id: container_id.clone(),
                        extra: std::collections::HashMap::new(),
                    })
                }
            };
            tools.push(ResponsesTool::Shell(ShellToolParam {
                environment,
                allowed_callers: None,
                extra: std::collections::HashMap::new(),
            }));
        }
    }
    authorize_hosted_shell(payload, exec_ctx, &policy).await?;
    if has_carriers {
        let executor = exec_ctx.gateway_executors.shell_executor().ok_or_else(|| {
            ExecutorError::InvalidRequest("gateway shell history requires a configured shell backend".to_owned())
        })?;
        if let ResponsesInput::Items(items) = &mut payload.input {
            let restored = executor
                .unseal(&policy.subject, items)
                .map_err(ExecutorError::InvalidRequest)?;
            debug!(restored, "restored sealed gateway shell history");
        }
    }
    Ok(())
}

/// Authorizes every declared hosted shell environment before any response
/// bytes are sent, so an unauthorized caller receives an explicit request
/// error rather than a failed stream. The registry authorizes again, which
/// also covers declarations inherited from stored state.
async fn authorize_hosted_shell(
    payload: &RequestPayload,
    exec_ctx: &ExecutionContext,
    policy: &GatewayExecutionPolicy,
) -> ExecutorResult<()> {
    let hosted = payload.tools.iter().flatten().filter_map(|tool| match tool {
        ResponsesTool::Shell(param) if param.environment.is_hosted() => Some(&param.environment),
        _ => None,
    });
    for environment in hosted {
        let executor = exec_ctx.gateway_executors.shell_executor().ok_or_else(|| {
            ExecutorError::InvalidRequest(
                "hosted shell environments require a configured gateway shell backend".to_owned(),
            )
        })?;
        let grant = policy.shell.as_ref().ok_or_else(|| {
            ExecutorError::InvalidRequest("this caller is not granted gateway shell execution".to_owned())
        })?;
        executor
            .bind(policy, grant, environment, "preflight")
            .await
            .map_err(|error| ExecutorError::InvalidRequest(error.to_string()))?;
    }
    Ok(())
}

/// Execute one stateful conversation turn.
///
/// Thin shim over [`ExecuteRequest`] for callers that don't need per-request auth override.
///
/// # Errors
/// Returns [`crate::executor::error::ExecutorError`] if rehydration or (non-streaming) LLM inference fails.
pub async fn execute(
    request: RequestPayload,
    exec_ctx: Arc<ExecutionContext>,
) -> ExecutorResult<Either<ResponsePayload, BoxStream>> {
    ExecuteRequest::new(request, exec_ctx).run().await
}
