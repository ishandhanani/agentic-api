use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use agent_rt_control::Command;
use serde::Deserialize;

use super::code_interpreter::{ExecutionOutcome, ExecutionOutcomeState, RemoteAgentRtExecutor, RemoteCommand};
use super::{
    GatewayExecutionContext, GatewayExecutor, GatewayToolEventPlan, ToolError, ToolHandler, ToolOutput, ToolType,
};
use crate::types::io::{
    FunctionTool, FunctionToolCall, GatewayCallStatus, OutputItem, ShellCall, ShellCallAction, ShellCallEnvironment,
    ShellCallOutcome, ShellCallOutput, ShellCallOutputContent, ShellCallStatus,
};
use crate::types::tools::{ShellAllowedCallerParam, ShellEnvironmentParam, ShellToolParam};

const DEFAULT_MAX_OUTPUT_LENGTH: u64 = 4_096;
const MAX_COMMANDS_PER_CALL: usize = 64;

#[derive(Clone, Debug)]
pub struct RemoteAgentRtShellExecutor {
    inner: RemoteAgentRtExecutor,
}

impl RemoteAgentRtShellExecutor {
    #[must_use]
    pub const fn new(inner: RemoteAgentRtExecutor) -> Self {
        Self { inner }
    }

    async fn execute_remote(&self, context: GatewayExecutionContext, arguments: &str) -> Result<ToolOutput, ToolError> {
        let action = parse_action(arguments)?;
        let timeout = action.timeout_ms.map(Duration::from_millis);
        let max_output_bytes = Some(action.max_output_length.unwrap_or(DEFAULT_MAX_OUTPUT_LENGTH));
        let commands = action
            .commands
            .into_iter()
            .map(|command| RemoteCommand {
                command: Command {
                    argv: vec!["sh".to_owned(), "-lc".to_owned(), command],
                    cwd: None,
                    env: HashMap::new(),
                    stdin: Vec::new(),
                    artifact_paths: Vec::new(),
                },
                timeout,
                max_output_bytes,
            })
            .collect();
        self.inner.execute_commands(context, commands).await
    }
}

impl ToolHandler for RemoteAgentRtShellExecutor {
    type ToolParams = ShellToolParam;

    fn tool_type(&self) -> ToolType {
        ToolType::Shell
    }

    fn validate(&self, params: &Self::ToolParams) -> Result<(), ToolError> {
        if params
            .allowed_callers
            .as_ref()
            .is_some_and(|callers| callers.iter().any(|caller| *caller != ShellAllowedCallerParam::Direct))
        {
            return Err(ToolError::Config(
                "agent-rt shell does not support programmatic callers".to_owned(),
            ));
        }
        match &params.environment {
            None => Ok(()),
            Some(ShellEnvironmentParam::ContainerAuto {
                file_ids,
                memory_limit,
                network_policy,
                skills,
            }) if file_ids.is_empty() && memory_limit.is_none() && network_policy.is_none() && skills.is_empty() => {
                Ok(())
            }
            Some(ShellEnvironmentParam::ContainerAuto { .. }) => Err(ToolError::Config(
                "agent-rt workspace classes own files, memory, and network policy; omit container_auto overrides"
                    .to_owned(),
            )),
            Some(ShellEnvironmentParam::Local { .. }) => Err(ToolError::Config(
                "environment.local is client-executed and cannot be handled by agent-rt".to_owned(),
            )),
            Some(ShellEnvironmentParam::ContainerReference { .. }) => Err(ToolError::Config(
                "container_reference requires the agent-rt-backed Containers API".to_owned(),
            )),
        }
    }

    fn normalize(&self, _params: &Self::ToolParams) -> Vec<FunctionTool> {
        vec![shell_function_tool()]
    }
}

impl GatewayExecutor for RemoteAgentRtShellExecutor {
    type ExecutionParams = ShellToolParam;

    fn execute(
        &self,
        _call_id: &str,
        _tool_name: &str,
        _arguments: &str,
        _params: &Self::ExecutionParams,
    ) -> Pin<Box<dyn Future<Output = Result<ToolOutput, ToolError>> + Send + '_>> {
        Box::pin(async {
            Err(ToolError::Config(
                "remote shell requires request execution context".to_owned(),
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

    fn manages_execution_deadline(&self) -> bool {
        true
    }

    fn supports_parallel_execution(&self) -> bool {
        true
    }

    fn plan_gateway_events(&self, _call: &FunctionToolCall, _params: &Self::ExecutionParams) -> GatewayToolEventPlan {
        GatewayToolEventPlan::with_slots(vec![None, None])
    }

    fn public_outputs(
        &self,
        call: &FunctionToolCall,
        output: &ToolOutput,
        status: GatewayCallStatus,
        _params: &Self::ExecutionParams,
    ) -> Vec<OutputItem> {
        let Ok(action) = parse_action(&call.arguments) else {
            return Vec::new();
        };
        let outcomes = serde_json::from_str::<Vec<ExecutionOutcome>>(&output.output).unwrap_or_default();
        let environment = outcomes
            .first()
            .map(|outcome| ShellCallEnvironment::ContainerReference {
                container_id: outcome.workspace_id.clone(),
            });
        let completed = status == GatewayCallStatus::Completed
            && !outcomes.is_empty()
            && outcomes.iter().all(|outcome| {
                !matches!(
                    outcome.state,
                    ExecutionOutcomeState::Cancelled | ExecutionOutcomeState::OutcomeUnknown
                )
            });
        let call_status = if completed {
            ShellCallStatus::Completed
        } else {
            ShellCallStatus::Incomplete
        };
        let max_output_length = action.max_output_length;
        let output_items = if outcomes.is_empty() {
            vec![ShellCallOutputContent {
                stdout: String::new(),
                stderr: shell_error_message(&output.output),
                outcome: ShellCallOutcome::Exit { exit_code: 1 },
            }]
        } else {
            outcomes.into_iter().map(shell_output_content).collect()
        };
        vec![
            shell_call(call, &action, environment, call_status),
            OutputItem::ShellCallOutput(ShellCallOutput {
                id: format!("{}_output", call.id),
                call_id: call.call_id.clone(),
                max_output_length,
                output: output_items,
                status: call_status,
            }),
        ]
    }
}

fn shell_call(
    call: &FunctionToolCall,
    action: &ShellArguments,
    environment: Option<ShellCallEnvironment>,
    status: ShellCallStatus,
) -> OutputItem {
    OutputItem::ShellCall(ShellCall {
        id: call.id.clone(),
        call_id: call.call_id.clone(),
        action: ShellCallAction {
            commands: action.commands.clone(),
            timeout_ms: action.timeout_ms,
            max_output_length: action.max_output_length,
        },
        environment,
        status,
    })
}

fn shell_error_message(output: &str) -> String {
    #[derive(Deserialize)]
    struct ExecutionError {
        error: String,
    }

    serde_json::from_str::<ExecutionError>(output).map_or_else(
        |_| "shell execution produced no command outcomes".to_owned(),
        |error| error.error,
    )
}

fn shell_output_content(outcome: ExecutionOutcome) -> ShellCallOutputContent {
    let result = outcome.result;
    let stdout = result.as_ref().map_or_else(String::new, |result| result.stdout.clone());
    let mut stderr = result.as_ref().map_or_else(String::new, |result| result.stderr.clone());
    if let Some(failure_code) = outcome.failure_code.filter(|_| stderr.is_empty()) {
        stderr = failure_code;
    }
    let outcome = match outcome.state {
        ExecutionOutcomeState::TimedOut => ShellCallOutcome::Timeout,
        ExecutionOutcomeState::Completed => ShellCallOutcome::Exit {
            exit_code: result.and_then(|result| result.exit_code).unwrap_or(0),
        },
        ExecutionOutcomeState::Accepted
        | ExecutionOutcomeState::Running
        | ExecutionOutcomeState::Failed
        | ExecutionOutcomeState::Cancelled
        | ExecutionOutcomeState::OutcomeUnknown => ShellCallOutcome::Exit {
            exit_code: result
                .and_then(|result| result.exit_code)
                .filter(|code| *code != 0)
                .unwrap_or(1),
        },
    };
    ShellCallOutputContent {
        stdout,
        stderr,
        outcome,
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ShellArguments {
    commands: Vec<String>,
    timeout_ms: Option<u64>,
    max_output_length: Option<u64>,
}

fn parse_action(arguments: &str) -> Result<ShellArguments, ToolError> {
    let action = serde_json::from_str::<ShellArguments>(arguments)
        .map_err(|error| ToolError::Config(format!("invalid shell arguments: {error}")))?;
    if action.commands.is_empty() || action.commands.iter().any(|command| command.trim().is_empty()) {
        return Err(ToolError::Config(
            "shell requires at least one non-empty command".to_owned(),
        ));
    }
    if action.commands.len() > MAX_COMMANDS_PER_CALL {
        return Err(ToolError::Config(format!(
            "shell accepts at most {MAX_COMMANDS_PER_CALL} commands per call"
        )));
    }
    if action.max_output_length == Some(0) {
        return Err(ToolError::Config(
            "shell max_output_length must be greater than zero".to_owned(),
        ));
    }
    Ok(action)
}

#[must_use]
pub(crate) fn shell_function_tool() -> FunctionTool {
    FunctionTool {
        type_: "function".to_owned(),
        name: "shell".to_owned(),
        description: Some("Execute ordered shell commands in an operator-managed sandbox.".to_owned()),
        parameters: Some(serde_json::json!({
            "type": "object",
            "properties": {
                "commands": {
                    "type": "array",
                    "items": {"type": "string"},
                    "minItems": 1,
                    "maxItems": MAX_COMMANDS_PER_CALL
                },
                "timeout_ms": {"type": "integer", "minimum": 1},
                "max_output_length": {"type": "integer", "minimum": 1}
            },
            "required": ["commands"],
            "additionalProperties": false
        })),
        strict: Some(true),
    }
}
