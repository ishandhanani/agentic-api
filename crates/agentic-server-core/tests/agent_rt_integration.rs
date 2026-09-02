use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use agent_rt_control::execution_service_server::{ExecutionService, ExecutionServiceServer};
use agent_rt_control::workspace_service_server::{WorkspaceService, WorkspaceServiceServer};
use agent_rt_control::{
    CancelExecutionRequest, Capability, CreateWorkspaceRequest, DeleteWorkspaceRequest, Execution, ExecutionResult,
    ExecutionState, GetExecutionRequest, GetWorkspaceRequest, StartExecutionRequest, WatchExecutionRequest, Workspace,
    WorkspaceState,
};
use agentic_core::config::{
    AgentRtExecutorConfig, Config, PostgresConfig, SqliteConfig, SubjectSigningKey, ToolRuntimeConfig,
};
use agentic_core::executor::{ExecuteRequest, ExecutionContext};
use agentic_core::types::io::OutputItem;
use axum::extract::State;
use axum::routing::post;
use either::Either;
use futures::Stream;
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};

#[derive(Clone, Default)]
struct InferenceState {
    calls: Arc<AtomicUsize>,
    requests: Arc<Mutex<Vec<serde_json::Value>>>,
}

async fn inference(
    State(state): State<InferenceState>,
    axum::Json(request): axum::Json<serde_json::Value>,
) -> axum::Json<serde_json::Value> {
    let call = state.calls.fetch_add(1, Ordering::SeqCst);
    state.requests.lock().await.push(request);
    let output = if call == 0 {
        serde_json::json!([{
            "id": "fc_code_1",
            "type": "function_call",
            "call_id": "call_code_1",
            "name": "code_interpreter",
            "arguments": "{\"code\":\"print(40 + 2)\"}",
            "status": "completed"
        }])
    } else {
        serde_json::json!([{
            "id": "msg_final",
            "type": "message",
            "role": "assistant",
            "status": "completed",
            "content": [{"type": "output_text", "text": "The result is 42.", "annotations": []}]
        }])
    };
    axum::Json(serde_json::json!({
        "id": format!("upstream_{call}"),
        "object": "response",
        "created_at": 0,
        "model": "test-model",
        "status": "completed",
        "output": output,
        "usage": {"input_tokens": 1, "output_tokens": 1, "total_tokens": 2},
        "incomplete_details": null,
        "error": null,
        "previous_response_id": null,
        "conversation_id": null,
        "instructions": null
    }))
}

#[derive(Clone, Default)]
struct AgentRtState {
    workspace_requests: Arc<Mutex<Vec<CreateWorkspaceRequest>>>,
    execution_requests: Arc<Mutex<Vec<StartExecutionRequest>>>,
}

fn assert_subject<T>(request: &Request<T>) {
    let authorization = request
        .metadata()
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .expect("authorization metadata");
    assert!(authorization.starts_with("Bearer "));
    assert_eq!(authorization.matches('.').count(), 1);
}

#[tonic::async_trait]
impl WorkspaceService for AgentRtState {
    async fn create_workspace(&self, request: Request<CreateWorkspaceRequest>) -> Result<Response<Workspace>, Status> {
        assert_subject(&request);
        let request = request.into_inner();
        self.workspace_requests.lock().await.push(request.clone());
        Ok(Response::new(Workspace {
            workspace_id: request.workspace_id,
            workspace_class_id: request.workspace_class_id,
            workspace_class_revision: "python.default@sha256:test".to_owned(),
            state: WorkspaceState::Ready as i32,
            revision: 1,
            created_at_unix_millis: 1,
            last_active_at_unix_millis: 1,
            expires_at_unix_millis: None,
            capabilities: vec![Capability {
                name: "command.execute".to_owned(),
                version: 1,
            }],
            failure_code: None,
        }))
    }

    async fn get_workspace(&self, _request: Request<GetWorkspaceRequest>) -> Result<Response<Workspace>, Status> {
        Err(Status::unimplemented("not needed by this integration test"))
    }

    async fn delete_workspace(&self, _request: Request<DeleteWorkspaceRequest>) -> Result<Response<Workspace>, Status> {
        Err(Status::unimplemented("not needed by this integration test"))
    }
}

#[tonic::async_trait]
impl ExecutionService for AgentRtState {
    type WatchExecutionStream = Pin<Box<dyn Stream<Item = Result<Execution, Status>> + Send + 'static>>;

    async fn start_execution(&self, request: Request<StartExecutionRequest>) -> Result<Response<Execution>, Status> {
        assert_subject(&request);
        let request = request.into_inner();
        self.execution_requests.lock().await.push(request.clone());
        Ok(Response::new(Execution {
            execution_id: request.execution_id,
            workspace_id: request.workspace_id,
            route_id: request.route_id,
            revision: 2,
            state: ExecutionState::Succeeded as i32,
            result: Some(ExecutionResult {
                exit_code: Some(0),
                stdout: b"42\n".to_vec(),
                stderr: Vec::new(),
                output_truncated: false,
            }),
            failure_code: None,
            artifacts: Vec::new(),
            accepted_at_unix_millis: 1,
            completed_at_unix_millis: Some(2),
        }))
    }

    async fn get_execution(&self, _request: Request<GetExecutionRequest>) -> Result<Response<Execution>, Status> {
        Err(Status::unimplemented("not needed by this integration test"))
    }

    async fn cancel_execution(&self, _request: Request<CancelExecutionRequest>) -> Result<Response<Execution>, Status> {
        Err(Status::unimplemented("not needed by this integration test"))
    }

    async fn watch_execution(
        &self,
        _request: Request<WatchExecutionRequest>,
    ) -> Result<Response<Self::WatchExecutionStream>, Status> {
        Err(Status::unimplemented("not needed by this integration test"))
    }
}

async fn serve_http(app: axum::Router) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind HTTP test server");
    let address = listener.local_addr().expect("HTTP test server address");
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.ok();
    });
    (format!("http://{address}"), server)
}

async fn serve_agent_rt(state: AgentRtState) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind gRPC test server");
    let address = listener.local_addr().expect("gRPC test server address");
    let incoming = async_stream::stream! {
        loop {
            yield listener.accept().await.map(|(socket, _)| socket);
        }
    };
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(WorkspaceServiceServer::new(state.clone()))
            .add_service(ExecutionServiceServer::new(state))
            .serve_with_incoming(incoming)
            .await
            .ok();
    });
    (format!("http://{address}"), server)
}

fn test_config(inference_url: String, agent_rt_url: String) -> Config {
    Config {
        llm_api_base: inference_url,
        openai_api_key: None,
        llm_ready_timeout_s: 1.0,
        llm_ready_interval_s: 0.01,
        skip_llm_ready_check: true,
        db_url: Some("sqlite::memory:".to_owned()),
        postgres: PostgresConfig::default(),
        sqlite: SqliteConfig::default(),
        tools: ToolRuntimeConfig {
            agent_rt: Some(AgentRtExecutorConfig {
                endpoint: agent_rt_url,
                workspace_class_id: "python.default".to_owned(),
                route_id: "sandbox.python.default".to_owned(),
                subject_signing_key: SubjectSigningKey::new("0123456789abcdef0123456789abcdef".to_owned()),
                subject_issuer: "agentic-api".to_owned(),
                subject_audience: "agent-rt".to_owned(),
                tenant_id: "tenant-a".to_owned(),
                default_principal_id: "principal-a".to_owned(),
                execution_timeout: Duration::from_secs(2),
                transport_timeout: Duration::from_millis(200),
                lookup_wait: Duration::from_millis(20),
            }),
            max_concurrent_gateway_calls: NonZeroUsize::new(2).expect("nonzero"),
            ..ToolRuntimeConfig::default()
        },
    }
}

#[tokio::test]
async fn code_interpreter_uses_agent_rt_grpc_and_reinjects_only_into_dynamo() {
    let inference_state = InferenceState::default();
    let (inference_url, inference_server) = serve_http(
        axum::Router::new()
            .route("/v1/responses", post(inference))
            .with_state(inference_state.clone()),
    )
    .await;
    let agent_rt_state = AgentRtState::default();
    let (agent_rt_url, agent_rt_server) = serve_agent_rt(agent_rt_state.clone()).await;
    let exec_ctx = Arc::new(
        ExecutionContext::from_config(&test_config(inference_url, agent_rt_url))
            .await
            .expect("execution context"),
    );
    let payload = serde_json::from_value(serde_json::json!({
        "model": "test-model",
        "input": "Calculate 40 + 2 with Python.",
        "store": false,
        "stream": false,
        "tools": [{"type": "code_interpreter"}]
    }))
    .expect("request payload");

    let Either::Left(response) = ExecuteRequest::new(payload, Arc::clone(&exec_ctx))
        .run()
        .await
        .expect("agentic tool round")
    else {
        panic!("expected blocking response");
    };

    assert_eq!(inference_state.calls.load(Ordering::SeqCst), 2);
    assert!(
        response
            .output
            .iter()
            .any(|item| matches!(item, OutputItem::CodeInterpreterCall(call) if call.container_id.starts_with("ws_")))
    );
    let inference_requests = inference_state.requests.lock().await;
    assert_eq!(inference_requests.len(), 2);
    let reinjected = inference_requests[1].to_string();
    assert!(reinjected.contains("call_code_1"));
    assert!(reinjected.contains("function_call_output"));
    assert!(!reinjected.contains("previous_response_id"));

    let workspace_requests = agent_rt_state.workspace_requests.lock().await;
    assert_eq!(workspace_requests.len(), 1);
    assert_eq!(workspace_requests[0].workspace_class_id, "python.default");
    let execution_requests = agent_rt_state.execution_requests.lock().await;
    assert_eq!(execution_requests.len(), 1);
    assert_eq!(execution_requests[0].route_id, "sandbox.python.default");
    assert_eq!(
        execution_requests[0].command.as_ref().expect("command").argv,
        ["python", "-c", "print(40 + 2)"]
    );
    assert_eq!(execution_requests[0].client_metadata["call_id"], "call_code_1");

    let ledger_state: String = sqlx::query_scalar("SELECT state FROM remote_executions")
        .fetch_one(exec_ctx.storage_pool().expect("configured ledger"))
        .await
        .expect("ledger row");
    assert_eq!(ledger_state, "completed");

    inference_server.abort();
    agent_rt_server.abort();
}
