use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use agentic_core::config::{
    Config, PostgresConfig, ShedExecutorConfig, SqliteConfig, SubjectSigningKey, ToolRuntimeConfig,
};
use agentic_core::executor::{ExecuteRequest, ExecutionContext};
use agentic_core::types::io::{OutputItem, ShellCallEnvironment, ShellCallOutcome, ShellCallStatus};
use agentic_core::{AuthenticatedSubject, CreateContainerRequest, ListContainerFilesRequest, ListContainersRequest};
use axum::extract::State;
use axum::routing::post;
use either::Either;
use futures::{Stream, TryStreamExt};
use shed_control::execution_service_server::{ExecutionService, ExecutionServiceServer};
use shed_control::workspace_file_service_server::{WorkspaceFileService, WorkspaceFileServiceServer};
use shed_control::workspace_service_server::{WorkspaceService, WorkspaceServiceServer};
use shed_control::{
    CancelExecutionRequest, Capability, CreateWorkspaceRequest, DeleteWorkspaceRequest, Execution, ExecutionResult,
    ExecutionState, FileChunk, FileMetadata, GetExecutionRequest, GetWorkspaceRequest, ListFilesRequest,
    ListFilesResponse, ReadFileRequest, RemoveFileRequest, RemoveFileResponse, StartExecutionRequest, StatFileRequest,
    WatchExecutionRequest, Workspace, WorkspaceState, WriteFileRequest,
};
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};

type WorkspaceFiles = Arc<Mutex<HashMap<(String, String), Vec<u8>>>>;

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
            "id": "fc_shell_1",
            "type": "function_call",
            "call_id": "call_shell_1",
            "name": "shell",
            "arguments": "{\"commands\":[\"printf first\",\"printf second\"],\"max_output_length\":1024}",
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
struct ShedState {
    workspace_requests: Arc<Mutex<Vec<CreateWorkspaceRequest>>>,
    execution_requests: Arc<Mutex<Vec<StartExecutionRequest>>>,
    file_write_requests: Arc<Mutex<Vec<WriteFileRequest>>>,
    files: WorkspaceFiles,
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
impl WorkspaceService for ShedState {
    async fn create_workspace(&self, request: Request<CreateWorkspaceRequest>) -> Result<Response<Workspace>, Status> {
        assert_subject(&request);
        let request = request.into_inner();
        self.workspace_requests.lock().await.push(request.clone());
        Ok(Response::new(Workspace {
            workspace_id: request.workspace_id,
            profile_id: request.profile_id,
            profile_revision: "python.default@sha256:test".to_owned(),
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

    async fn get_workspace(&self, request: Request<GetWorkspaceRequest>) -> Result<Response<Workspace>, Status> {
        assert_subject(&request);
        let request = request.into_inner();
        Ok(Response::new(Workspace {
            workspace_id: request.workspace_id,
            profile_id: "python.default".to_owned(),
            profile_revision: "python.default@sha256:test".to_owned(),
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

    async fn delete_workspace(&self, request: Request<DeleteWorkspaceRequest>) -> Result<Response<Workspace>, Status> {
        assert_subject(&request);
        let request = request.into_inner();
        Ok(Response::new(Workspace {
            workspace_id: request.workspace_id,
            profile_id: "python.default".to_owned(),
            profile_revision: "python.default@sha256:test".to_owned(),
            state: WorkspaceState::Deleted as i32,
            revision: 2,
            created_at_unix_millis: 1,
            last_active_at_unix_millis: 2,
            expires_at_unix_millis: None,
            capabilities: Vec::new(),
            failure_code: None,
        }))
    }
}

#[tonic::async_trait]
impl WorkspaceFileService for ShedState {
    async fn stat_file(&self, request: Request<StatFileRequest>) -> Result<Response<FileMetadata>, Status> {
        assert_subject(&request);
        let request = request.into_inner();
        let files = self.files.lock().await;
        let content = files
            .get(&(request.workspace_id, request.path.clone()))
            .ok_or_else(|| Status::not_found("file_not_found"))?;
        Ok(Response::new(file_metadata(request.path, content.len())))
    }

    async fn list_files(&self, _request: Request<ListFilesRequest>) -> Result<Response<ListFilesResponse>, Status> {
        Err(Status::unimplemented(
            "public listing uses the durable container catalog",
        ))
    }

    async fn read_file(&self, request: Request<ReadFileRequest>) -> Result<Response<FileChunk>, Status> {
        assert_subject(&request);
        let request = request.into_inner();
        let files = self.files.lock().await;
        let content = files
            .get(&(request.workspace_id, request.path))
            .ok_or_else(|| Status::not_found("file_not_found"))?;
        let start = usize::try_from(request.offset).map_err(|_| Status::invalid_argument("invalid_offset"))?;
        let max_bytes =
            usize::try_from(request.max_bytes).map_err(|_| Status::invalid_argument("invalid_max_bytes"))?;
        if start > content.len() {
            return Err(Status::invalid_argument("invalid_offset"));
        }
        let end = start.saturating_add(max_bytes).min(content.len());
        Ok(Response::new(FileChunk {
            data: content[start..end].to_vec(),
            next_offset: u64::try_from(end).expect("test file offset fits u64"),
            eof: end == content.len(),
        }))
    }

    async fn write_file(&self, request: Request<WriteFileRequest>) -> Result<Response<FileMetadata>, Status> {
        assert_subject(&request);
        let request = request.into_inner();
        self.file_write_requests.lock().await.push(request.clone());
        let key = (request.workspace_id, request.path.clone());
        let mut files = self.files.lock().await;
        let content = files.entry(key).or_default();
        if request.truncate {
            content.clear();
        }
        let start = usize::try_from(request.offset).map_err(|_| Status::invalid_argument("invalid_offset"))?;
        if start > content.len() {
            return Err(Status::invalid_argument("invalid_offset"));
        }
        let end = start
            .checked_add(request.data.len())
            .ok_or_else(|| Status::invalid_argument("file_too_large"))?;
        content.resize(end, 0);
        content[start..end].copy_from_slice(&request.data);
        Ok(Response::new(file_metadata(request.path, content.len())))
    }

    async fn remove_file(&self, request: Request<RemoveFileRequest>) -> Result<Response<RemoveFileResponse>, Status> {
        assert_subject(&request);
        let request = request.into_inner();
        let removed = self.files.lock().await.remove(&(request.workspace_id, request.path));
        if removed.is_none() {
            return Err(Status::not_found("file_not_found"));
        }
        Ok(Response::new(RemoveFileResponse {}))
    }
}

fn file_metadata(path: String, size: usize) -> FileMetadata {
    FileMetadata {
        path,
        size_bytes: u64::try_from(size).expect("test file size fits u64"),
        is_directory: false,
        modified_at_unix_millis: Some(2),
    }
}

#[tonic::async_trait]
impl ExecutionService for ShedState {
    type WatchExecutionStream = Pin<Box<dyn Stream<Item = Result<Execution, Status>> + Send + 'static>>;

    async fn start_execution(&self, request: Request<StartExecutionRequest>) -> Result<Response<Execution>, Status> {
        assert_subject(&request);
        let request = request.into_inner();
        self.execution_requests.lock().await.push(request.clone());
        let stdout = request
            .command
            .as_ref()
            .and_then(|command| command.argv.last())
            .map_or_else(Vec::new, |command| {
                if command == "printf first" {
                    b"first".to_vec()
                } else if command == "printf second" {
                    b"second".to_vec()
                } else {
                    b"unexpected command".to_vec()
                }
            });
        Ok(Response::new(Execution {
            execution_id: request.execution_id,
            workspace_id: request.workspace_id,
            revision: 2,
            state: ExecutionState::Succeeded as i32,
            result: Some(ExecutionResult {
                exit_code: Some(0),
                stdout,
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

async fn serve_shed(state: ShedState) -> (String, tokio::task::JoinHandle<()>) {
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
            .add_service(ExecutionServiceServer::new(state.clone()))
            .add_service(WorkspaceFileServiceServer::new(state))
            .serve_with_incoming(incoming)
            .await
            .ok();
    });
    (format!("http://{address}"), server)
}

fn test_config(inference_url: String, shed_url: String) -> Config {
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
            shed: Some(ShedExecutorConfig {
                endpoint: shed_url,
                profile_id: "python.default".to_owned(),
                subject_signing_key: SubjectSigningKey::new("0123456789abcdef0123456789abcdef".to_owned()),
                subject_issuer: "agentic-api".to_owned(),
                subject_audience: "shed".to_owned(),
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
async fn shell_reuses_referenced_shed_workspace_and_projects_native_outputs() {
    let inference_state = InferenceState::default();
    let (inference_url, inference_server) = serve_http(
        axum::Router::new()
            .route("/v1/responses", post(inference))
            .with_state(inference_state.clone()),
    )
    .await;
    let shed_state = ShedState::default();
    let (shed_url, shed_server) = serve_shed(shed_state.clone()).await;
    let exec_ctx = Arc::new(
        ExecutionContext::from_config(&test_config(inference_url, shed_url))
            .await
            .expect("execution context"),
    );
    let payload = serde_json::from_value(serde_json::json!({
        "model": "test-model",
        "input": "Run two shell commands.",
        "store": false,
        "stream": false,
        "tools": [{"type": "shell", "environment": {
            "type": "container_reference",
            "container_id": "cntr_existing"
        }}]
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
    let [
        OutputItem::ShellCall(call),
        OutputItem::ShellCallOutput(output),
        OutputItem::Message(_),
    ] = response.output.as_slice()
    else {
        panic!("expected native shell call, shell output, then assistant message");
    };
    assert_eq!(call.call_id, "call_shell_1");
    assert_eq!(call.action.commands, ["printf first", "printf second"]);
    assert_eq!(call.status, ShellCallStatus::Completed);
    let Some(ShellCallEnvironment::ContainerReference { container_id }) = &call.environment else {
        panic!("shell must project the referenced shed workspace");
    };
    assert_eq!(container_id, "cntr_existing");
    assert_eq!(output.call_id, "call_shell_1");
    assert_eq!(output.status, ShellCallStatus::Completed);
    assert_eq!(output.output.len(), 2);
    assert_eq!(output.output[0].stdout, "first");
    assert_eq!(output.output[1].stdout, "second");
    assert!(matches!(
        output.output[0].outcome,
        ShellCallOutcome::Exit { exit_code: 0 }
    ));
    assert!(matches!(
        output.output[1].outcome,
        ShellCallOutcome::Exit { exit_code: 0 }
    ));
    let inference_requests = inference_state.requests.lock().await;
    assert_eq!(inference_requests.len(), 2);
    let reinjected = inference_requests[1].to_string();
    assert!(reinjected.contains("call_shell_1"));
    assert!(reinjected.contains("function_call_output"));
    assert!(reinjected.contains("first"));
    assert!(reinjected.contains("second"));
    assert!(!reinjected.contains("shell_call_output"));
    assert!(!reinjected.contains("previous_response_id"));

    let workspace_requests = shed_state.workspace_requests.lock().await;
    assert!(workspace_requests.is_empty());
    let execution_requests = shed_state.execution_requests.lock().await;
    assert_eq!(execution_requests.len(), 2);
    assert_eq!(execution_requests[0].workspace_id, execution_requests[1].workspace_id);
    assert_eq!(execution_requests[0].workspace_id, "cntr_existing");
    assert_ne!(execution_requests[0].execution_id, execution_requests[1].execution_id);
    assert_eq!(
        execution_requests[0].command.as_ref().expect("command").argv,
        ["sh", "-lc", "printf first"]
    );
    assert_eq!(
        execution_requests[1].command.as_ref().expect("command").argv,
        ["sh", "-lc", "printf second"]
    );
    assert_eq!(execution_requests[0].client_metadata["call_id"], "call_shell_1");
    assert_eq!(execution_requests[0].client_metadata["command_index"], "0");
    assert_eq!(execution_requests[1].client_metadata["command_index"], "1");

    let ledger_state: String = sqlx::query_scalar("SELECT state FROM remote_executions")
        .fetch_one(exec_ctx.storage_pool().expect("configured ledger"))
        .await
        .expect("ledger row");
    assert_eq!(ledger_state, "completed");

    inference_server.abort();
    shed_server.abort();
}

#[tokio::test]
async fn local_shell_round_trips_through_the_client_without_shed_dispatch() {
    let inference_state = InferenceState::default();
    let (inference_url, inference_server) = serve_http(
        axum::Router::new()
            .route("/v1/responses", post(inference))
            .with_state(inference_state.clone()),
    )
    .await;
    let mut config = test_config(inference_url, "http://127.0.0.1:1".to_owned());
    config.tools.shed = None;
    let exec_ctx = Arc::new(ExecutionContext::from_config(&config).await.expect("execution context"));
    let first_request = serde_json::from_value(serde_json::json!({
        "model": "test-model",
        "input": "Run two shell commands locally.",
        "store": true,
        "stream": false,
        "tools": [{"type": "shell", "environment": {"type": "local"}}]
    }))
    .expect("local shell request");

    let Either::Left(first_response) = ExecuteRequest::new(first_request, Arc::clone(&exec_ctx))
        .run()
        .await
        .expect("client-owned shell round")
    else {
        panic!("expected blocking response");
    };
    let [OutputItem::ShellCall(call)] = first_response.output.as_slice() else {
        panic!("expected one client-owned native shell call");
    };
    assert_eq!(call.call_id, "call_shell_1");
    assert!(matches!(call.environment, Some(ShellCallEnvironment::Local)));
    assert_eq!(call.status, ShellCallStatus::InProgress);

    let previous_response_id = first_response.id.clone();
    let second_request = serde_json::from_value(serde_json::json!({
        "model": "test-model",
        "previous_response_id": previous_response_id,
        "input": [
            {
                "type": "shell_call_output",
                "call_id": call.call_id,
                "max_output_length": call.action.max_output_length,
                "output": [
                    {"stdout": "first", "stderr": "", "outcome": {"type": "exit", "exit_code": 0}},
                    {"stdout": "second", "stderr": "", "outcome": {"type": "exit", "exit_code": 0}}
                ]
            }
        ],
        "store": true,
        "stream": false
    }))
    .expect("local shell continuation");

    let Either::Left(second_response) = ExecuteRequest::new(second_request, Arc::clone(&exec_ctx))
        .run()
        .await
        .expect("local shell continuation round")
    else {
        panic!("expected blocking response");
    };
    assert!(matches!(second_response.output.as_slice(), [OutputItem::Message(_)]));

    let inference_requests = inference_state.requests.lock().await;
    assert_eq!(inference_requests.len(), 2);
    let continuation = inference_requests[1].to_string();
    assert!(continuation.contains("function_call"));
    assert!(continuation.contains("function_call_output"));
    assert!(continuation.contains("call_shell_1"));
    assert!(continuation.contains("first"));
    assert!(!continuation.contains("shell_call_output"));
    inference_server.abort();
}

#[tokio::test]
async fn containers_catalog_maps_lifecycle_and_files_to_shed() {
    let shed_state = ShedState::default();
    let (shed_url, shed_server) = serve_shed(shed_state.clone()).await;
    let exec_ctx = ExecutionContext::from_config(&test_config("http://127.0.0.1:1".to_owned(), shed_url))
        .await
        .expect("execution context");
    let service = exec_ctx.container_service().expect("container service");
    let subject = AuthenticatedSubject {
        tenant_id: "tenant-a".to_owned(),
        principal_id: "principal-a".to_owned(),
    };
    let traceparent = Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01");
    let create: CreateContainerRequest =
        serde_json::from_value(serde_json::json!({"name": "test-container"})).expect("create container request");

    let container = service
        .create(&subject, create, traceparent)
        .await
        .expect("create container");
    assert!(container.id.starts_with("cntr_"));
    assert_eq!(container.name, "test-container");
    assert_eq!(container.status, "running");

    let mut expected = vec![b'a'; 1024 * 1024];
    expected.extend_from_slice(b"tail");
    let file = service
        .create_file(
            &subject,
            &container.id,
            "../results?.txt",
            bytes::Bytes::from(expected.clone()),
            traceparent,
        )
        .await
        .expect("create container file");
    assert!(file.id.starts_with("cfile_"));
    assert_eq!(file.container_id, container.id);
    assert!(file.path.ends_with("-results_.txt"));
    assert_eq!(file.bytes, u64::try_from(expected.len()).expect("test size fits u64"));

    let writes = shed_state.file_write_requests.lock().await;
    assert_eq!(writes.len(), 2);
    assert_eq!(writes[0].offset, 0);
    assert!(writes[0].truncate);
    assert_eq!(writes[1].offset, 1024 * 1024);
    assert!(!writes[1].truncate);
    drop(writes);

    let retrieved = service
        .retrieve_file(&subject, &container.id, &file.id, traceparent)
        .await
        .expect("retrieve container file");
    assert_eq!(retrieved.bytes, file.bytes);
    let files = service
        .list_files(
            &subject,
            &container.id,
            serde_json::from_value::<ListContainerFilesRequest>(serde_json::json!({})).expect("list files request"),
        )
        .await
        .expect("list container files");
    assert_eq!(files.data.len(), 1);
    assert_eq!(files.first_id.as_deref(), Some(file.id.as_str()));
    assert!(!files.has_more);

    let chunks = service
        .read_file_content(&subject, &container.id, &file.id, traceparent)
        .await
        .expect("container file content")
        .try_collect::<Vec<_>>()
        .await
        .expect("read container file content");
    assert_eq!(chunks.concat(), expected);

    let deleted_file = service
        .delete_file(&subject, &container.id, &file.id, traceparent)
        .await
        .expect("delete container file");
    assert!(deleted_file.deleted);
    let files = service
        .list_files(
            &subject,
            &container.id,
            serde_json::from_value::<ListContainerFilesRequest>(serde_json::json!({})).expect("list files request"),
        )
        .await
        .expect("list container files after deletion");
    assert!(files.data.is_empty());

    let deleted = service
        .delete(&subject, &container.id, traceparent)
        .await
        .expect("delete container");
    assert!(deleted.deleted);
    let containers = service
        .list(
            &subject,
            serde_json::from_value::<ListContainersRequest>(serde_json::json!({})).expect("list containers request"),
        )
        .await
        .expect("list containers after deletion");
    assert!(containers.data.is_empty());

    shed_server.abort();
}
