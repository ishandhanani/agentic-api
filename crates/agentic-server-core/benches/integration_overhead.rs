//! Integration-seam microbenchmarks for Dynamo and `shed`.
//!
//! - `direct_dynamo_http` is the raw inference-hop baseline.
//! - `agentic_to_dynamo` adds Agentic request normalization and response mapping.
//! - `agentic_to_shed_control_plane` adds the durable execution claim,
//!   subject signing, remote execution gRPC exchange, and outcome persistence.
//!
//! The fake `shed` completes immediately, so provider runtime is excluded
//! from the control-plane number. The provider seam is benchmarked separately
//! in `dynamo-shed`'s `execution_overhead` benchmark.

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime};

use agentic_core::config::{ShedExecutorConfig, SubjectSigningKey};
use agentic_core::executor::{ConversationHandler, ExecutionContext, ResponseHandler, execute};
use agentic_core::storage::{ConversationStore, RemoteExecutionLedger, ResponseStore, create_pool_with_schema};
use agentic_core::tool::{
    AuthenticatedSubject, GatewayExecutionContext, GatewayExecutor, ShedShellExecutor, TraceContext,
};
use agentic_core::types::io::{ResponsesInput, ToolChoice};
use agentic_core::types::request_response::RequestPayload;
use agentic_core::{ShellEnvironmentParam, ShellToolParam};
use axum::Router;
use axum::routing::post;
use criterion::{Criterion, black_box, criterion_group};
use futures::Stream;
use shed_control::execution_service_server::{ExecutionService, ExecutionServiceServer};
use shed_control::workspace_service_server::{WorkspaceService, WorkspaceServiceServer};
use shed_control::{
    CancelExecutionRequest, Capability, CreateWorkspaceRequest, DeleteWorkspaceRequest, Execution, ExecutionResult,
    ExecutionState, GetExecutionRequest, GetWorkspaceRequest, StartExecutionRequest, WatchExecutionRequest, Workspace,
    WorkspaceState,
};
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status};

const DYNAMO_RESPONSE: &str = r#"{
  "id": "resp_bench_upstream",
  "object": "response",
  "created_at": 1700000000,
  "status": "completed",
  "model": "test-model",
  "output": [{
    "type": "message",
    "id": "msg_bench",
    "role": "assistant",
    "status": "completed",
    "content": [{"type": "output_text", "text": "OK", "annotations": []}]
  }],
  "usage": {
    "input_tokens": 5,
    "output_tokens": 1,
    "total_tokens": 6,
    "input_tokens_details": {"cached_tokens": 0},
    "output_tokens_details": {"reasoning_tokens": 0}
  }
}"#;

fn start_dynamo_server(runtime: &tokio::runtime::Runtime) -> String {
    let listener = runtime.block_on(async { tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap() });
    let address = listener.local_addr().unwrap();
    runtime.spawn(async move {
        let app = Router::new().route(
            "/v1/responses",
            post(|| async {
                (
                    [(axum::http::header::CONTENT_TYPE, "application/json")],
                    DYNAMO_RESPONSE,
                )
            }),
        );
        axum::serve(listener, app).await.ok();
    });
    format!("http://{address}")
}

#[derive(Clone, Copy)]
struct BenchShed;

#[tonic::async_trait]
impl WorkspaceService for BenchShed {
    async fn create_workspace(&self, request: Request<CreateWorkspaceRequest>) -> Result<Response<Workspace>, Status> {
        let request = request.into_inner();
        Ok(Response::new(Workspace {
            workspace_id: request.workspace_id,
            profile_id: request.profile_id,
            profile_revision: "python.default@bench".to_owned(),
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
        Err(Status::unimplemented("benchmark does not look up workspaces"))
    }

    async fn delete_workspace(&self, _request: Request<DeleteWorkspaceRequest>) -> Result<Response<Workspace>, Status> {
        Err(Status::unimplemented("benchmark does not delete workspaces"))
    }
}

#[tonic::async_trait]
impl ExecutionService for BenchShed {
    type WatchExecutionStream = Pin<Box<dyn Stream<Item = Result<Execution, Status>> + Send + 'static>>;

    async fn start_execution(&self, request: Request<StartExecutionRequest>) -> Result<Response<Execution>, Status> {
        let request = request.into_inner();
        Ok(Response::new(Execution {
            execution_id: request.execution_id,
            workspace_id: request.workspace_id,
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
        Err(Status::unimplemented("benchmark executions complete synchronously"))
    }

    async fn cancel_execution(&self, _request: Request<CancelExecutionRequest>) -> Result<Response<Execution>, Status> {
        Err(Status::unimplemented("benchmark executions complete synchronously"))
    }

    async fn watch_execution(
        &self,
        _request: Request<WatchExecutionRequest>,
    ) -> Result<Response<Self::WatchExecutionStream>, Status> {
        Err(Status::unimplemented("benchmark executions complete synchronously"))
    }
}

fn start_shed_server(runtime: &tokio::runtime::Runtime) -> String {
    let listener = runtime.block_on(async { tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap() });
    let address = listener.local_addr().unwrap();
    let incoming = async_stream::stream! {
        loop {
            yield listener.accept().await.map(|(socket, _)| socket);
        }
    };
    runtime.spawn(async move {
        tonic::transport::Server::builder()
            .add_service(WorkspaceServiceServer::new(BenchShed))
            .add_service(ExecutionServiceServer::new(BenchShed))
            .serve_with_incoming(incoming)
            .await
            .ok();
    });
    format!("http://{address}")
}

fn request_payload() -> RequestPayload {
    RequestPayload {
        model: "test-model".to_owned(),
        input: ResponsesInput::Text("Say OK.".to_owned()),
        instructions: None,
        previous_response_id: None,
        conversation_id: None,
        tools: None,
        tool_choice: Some(ToolChoice::Auto),
        stream: false,
        store: false,
        include: None,
        reasoning: None,
        temperature: None,
        top_p: None,
        max_output_tokens: None,
        truncation: None,
        metadata: None,
        parallel_tool_calls: None,
        cache_salt: None,
        context_management: None,
    }
}

fn stable_id(prefix: &str, components: &[&str]) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"agentic-api-remote-execution");
    for component in components {
        hasher.update(&(component.len() as u64).to_le_bytes());
        hasher.update(component.as_bytes());
    }
    format!("{prefix}{}", hasher.finalize().to_hex())
}

fn remote_context(sequence: u64, subject: AuthenticatedSubject) -> GatewayExecutionContext {
    let response_id = format!("resp_bench_{sequence}");
    let call_id = format!("call_bench_{sequence}");
    let execution_id = stable_id(
        "exec_",
        &[&subject.tenant_id, &subject.principal_id, &response_id, &call_id],
    );
    let workspace_id = stable_id("ws_", &[&subject.tenant_id, &subject.principal_id, &response_id]);
    GatewayExecutionContext {
        response_id,
        conversation_id: None,
        call_id,
        execution_id,
        workspace_id,
        subject: Some(subject),
        absolute_deadline: Some(SystemTime::now() + Duration::from_secs(30)),
        cancellation: CancellationToken::new(),
        trace_context: TraceContext::default(),
    }
}

fn shell_tool() -> ShellToolParam {
    ShellToolParam {
        environment: Some(ShellEnvironmentParam::ContainerAuto {
            file_ids: Vec::new(),
            memory_limit: None,
            network_policy: None,
            skills: Vec::new(),
        }),
        allowed_callers: None,
    }
}

fn integration_overhead(c: &mut Criterion) {
    let server_runtime = tokio::runtime::Runtime::new().unwrap();
    let dynamo_endpoint = start_dynamo_server(&server_runtime);
    let shed_endpoint = start_shed_server(&server_runtime);
    let setup_runtime = tokio::runtime::Runtime::new().unwrap();
    let pool = setup_runtime.block_on(async { create_pool_with_schema(Some("sqlite::memory:")).await.unwrap() });
    let client = Arc::new(reqwest::Client::new());
    let execution_context = Arc::new(ExecutionContext::new(
        ConversationHandler::new(ConversationStore::new(Arc::clone(&pool))),
        ResponseHandler::new(ResponseStore::new(Arc::clone(&pool))),
        Arc::clone(&client),
        dynamo_endpoint.clone(),
    ));
    let remote_executor = {
        let _runtime_guard = setup_runtime.enter();
        Arc::new(
            ShedShellExecutor::new(
                ShedExecutorConfig {
                    endpoint: shed_endpoint,
                    profile_id: "python.default".to_owned(),
                    subject_signing_key: SubjectSigningKey::new("0123456789abcdef0123456789abcdef".to_owned()),
                    subject_issuer: "agentic-api".to_owned(),
                    subject_audience: "shed".to_owned(),
                    tenant_id: "tenant-bench".to_owned(),
                    default_principal_id: "principal-bench".to_owned(),
                    execution_timeout: Duration::from_secs(30),
                    transport_timeout: Duration::from_secs(2),
                    lookup_wait: Duration::from_millis(10),
                },
                RemoteExecutionLedger::new(pool),
            )
            .unwrap(),
        )
    };
    let direct_body = serde_json::json!({
        "model": "test-model",
        "input": "Say OK.",
        "stream": false,
        "store": false
    });
    let dynamo_url = format!("{dynamo_endpoint}/v1/responses");
    let subject = AuthenticatedSubject {
        tenant_id: "tenant-bench".to_owned(),
        principal_id: "principal-bench".to_owned(),
    };
    let sequence = Arc::new(AtomicU64::new(0));

    let mut group = c.benchmark_group("integration_overhead");
    group.bench_function("direct_dynamo_http", |b| {
        let client = Arc::clone(&client);
        let url = dynamo_url.clone();
        let body = direct_body.clone();
        b.to_async(tokio::runtime::Runtime::new().unwrap()).iter(|| {
            let client = Arc::clone(&client);
            let url = url.clone();
            let body = body.clone();
            async move {
                let response = client.post(url).json(&body).send().await.unwrap();
                black_box(response.json::<serde_json::Value>().await.unwrap())
            }
        });
    });
    group.bench_function("agentic_to_dynamo", |b| {
        let execution_context = Arc::clone(&execution_context);
        b.to_async(tokio::runtime::Runtime::new().unwrap()).iter(|| {
            let execution_context = Arc::clone(&execution_context);
            async move { black_box(execute(request_payload(), execution_context).await.unwrap()) }
        });
    });
    group.bench_function("agentic_to_shed_control_plane", |b| {
        let remote_executor = Arc::clone(&remote_executor);
        let sequence = Arc::clone(&sequence);
        let subject = subject.clone();
        b.to_async(tokio::runtime::Runtime::new().unwrap()).iter(|| {
            let remote_executor = Arc::clone(&remote_executor);
            let sequence = Arc::clone(&sequence);
            let subject = subject.clone();
            async move {
                let context = remote_context(sequence.fetch_add(1, Ordering::Relaxed), subject);
                black_box(
                    remote_executor
                        .execute_with_context(context, "shell", r#"{"commands":["printf 42"]}"#, &shell_tool())
                        .await
                        .unwrap(),
                )
            }
        });
    });
    group.finish();
}

criterion_group!(integration_overhead_benches, integration_overhead);
