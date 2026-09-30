//! Gateway-executed `shell` with a deterministic backend and scripted model.
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use agentic_core::executor::modes::{ConversationHandler, ResponseHandler};
use agentic_core::executor::{ExecuteRequest, ExecutionContext};
use agentic_core::storage::{ConversationStore, ResponseStore};
use agentic_core::tool::{
    ExecutionSubject, GatewayExecutionPolicy, GatewayExecutorRegistration, GatewayShellConfig, GatewayShellGrant,
    PreparedShellEnvironment, ShellBackend, ShellBackendContext, ShellBackendError, ShellBackendFuture,
    ShellClientView, ShellCommandOutcome, ShellCommandRequest, ShellCommandResult, ShellEnvironmentSelection,
    ShellExecutionLimits,
};
use agentic_core::types::request_response::RequestPayload;
use either::Either;
use futures::StreamExt;
use serde_json::{Value, json};
use tokio::sync::Mutex;

mod support;
use support::{MockResponse, MockServer, function_call_response, setup_pool, text_response};

#[derive(Default)]
struct FakeShell {
    prepared: Mutex<Vec<(ExecutionSubject, ShellEnvironmentSelection)>>,
    commands: Mutex<Vec<(String, String, usize, ShellCommandRequest)>>,
    runs: AtomicUsize,
    dropped: AtomicUsize,
    block: bool,
    fail: bool,
}

struct DropProbe<'a>(&'a AtomicUsize, bool);

impl Drop for DropProbe<'_> {
    fn drop(&mut self) {
        if !self.1 {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
}

impl ShellBackend for FakeShell {
    fn prepare<'a>(
        &'a self,
        subject: &'a ExecutionSubject,
        environment: &'a ShellEnvironmentSelection,
        _correlation: &'a BTreeMap<String, String>,
    ) -> ShellBackendFuture<'a, Result<PreparedShellEnvironment, ShellBackendError>> {
        Box::pin(async move {
            self.prepared.lock().await.push((subject.clone(), environment.clone()));
            match environment {
                ShellEnvironmentSelection::Reference { container_id } if container_id == "cntr_forbidden" => {
                    Err(ShellBackendError::Unauthorized)
                }
                ShellEnvironmentSelection::Reference { container_id } => Ok(PreparedShellEnvironment {
                    container_id: container_id.clone(),
                }),
                ShellEnvironmentSelection::Auto => Ok(PreparedShellEnvironment {
                    container_id: "cntr_auto".to_owned(),
                }),
            }
        })
    }

    fn run_command<'a>(
        &'a self,
        context: &'a ShellBackendContext,
        index: usize,
        command: &'a ShellCommandRequest,
    ) -> ShellBackendFuture<'a, Result<ShellCommandResult, ShellBackendError>> {
        Box::pin(async move {
            self.runs.fetch_add(1, Ordering::SeqCst);
            self.commands.lock().await.push((
                context.container_id.clone(),
                context.execution_id.clone(),
                index,
                command.clone(),
            ));
            let mut probe = DropProbe(&self.dropped, false);
            if self.block {
                tokio::time::sleep(Duration::from_secs(60)).await;
            }
            probe.1 = true;
            if self.fail {
                return Err(ShellBackendError::Lost("workspace expired".to_owned()));
            }
            Ok(ShellCommandResult {
                stdout: format!("ran:{}\n", command.command).into_bytes(),
                stderr: Vec::new(),
                outcome: ShellCommandOutcome::Exit(i32::try_from(index).unwrap()),
                output_truncated: false,
                execution_id: format!("{}-{index}", context.execution_id),
            })
        })
    }
}

const KEY: &[u8] = b"gateway-shell-test-sealing-key-0123456789";

fn subject(principal: &str) -> ExecutionSubject {
    ExecutionSubject {
        tenant_id: "tenant-a".to_owned(),
        principal_id: principal.to_owned(),
    }
}

fn policy(principal: &str, view: ShellClientView, declare: Option<&str>) -> Arc<GatewayExecutionPolicy> {
    Arc::new(GatewayExecutionPolicy {
        subject: subject(principal),
        shell: Some(GatewayShellGrant {
            declare: declare.map(|container_id| ShellEnvironmentSelection::Reference {
                container_id: container_id.to_owned(),
            }),
            client_view: view,
        }),
        correlation: BTreeMap::from([("session_id".to_owned(), "thread-1".to_owned())]),
    })
}

async fn context(server: &MockServer, backend: Option<Arc<FakeShell>>) -> Arc<ExecutionContext> {
    let pool = setup_pool().await;
    let mut context = ExecutionContext::new(
        ConversationHandler::new(ConversationStore::new(Arc::clone(&pool))),
        ResponseHandler::new(ResponseStore::new(Arc::clone(&pool))),
        Arc::new(reqwest::Client::new()),
        server.url().to_owned(),
    );
    if let Some(backend) = backend {
        context = context.with_gateway_executor(
            GatewayExecutorRegistration::shell(GatewayShellConfig {
                backend,
                limits: ShellExecutionLimits {
                    max_commands: 4,
                    default_timeout: Duration::from_secs(10),
                    max_timeout: Duration::from_secs(30),
                    ..ShellExecutionLimits::default()
                },
                sealing_key: KEY.to_vec(),
            })
            .expect("valid shell registration"),
        );
    }
    Arc::new(context)
}

fn request(value: Value) -> RequestPayload {
    serde_json::from_value(value).expect("valid request")
}

fn shell_call(arguments: &Value) -> MockResponse {
    function_call_response("fc_1", "call_1", "shell", &arguments.to_string())
}

/// Streams one completed item the way a Responses upstream does.
fn sse_item(item: &Value) -> MockResponse {
    let mut added = item.clone();
    added["status"] = json!("in_progress");
    if added["type"] == "function_call" {
        added["arguments"] = json!("");
    }
    if added["type"] == "message" {
        added["content"] = json!([]);
    }
    let events = [
        json!({"type": "response.created", "response": {"id": "resp_up", "status": "in_progress", "output": []}}),
        json!({"type": "response.in_progress", "response": {"id": "resp_up", "status": "in_progress", "output": []}}),
        json!({"type": "response.output_item.added", "output_index": 0, "item": added}),
        json!({"type": "response.output_item.done", "output_index": 0, "item": item}),
        json!({"type": "response.completed", "response": {"id": "resp_up", "status": "completed", "output": [item]}}),
    ];
    let mut body = String::new();
    for event in events {
        write!(&mut body, "data: {event}\n\n").unwrap();
    }
    body.push_str("data: [DONE]\n\n");
    MockResponse::Sse(body)
}

fn sse_shell_call(arguments: &Value) -> MockResponse {
    sse_item(
        &json!({"type": "function_call", "id": "fc_1", "call_id": "call_1", "name": "shell",
        "arguments": arguments.to_string(), "status": "completed"}),
    )
}

fn sse_text(text: &str) -> MockResponse {
    sse_item(
        &json!({"type": "message", "id": "msg_1", "role": "assistant", "status": "completed",
        "content": [{"type": "output_text", "text": text, "annotations": []}]}),
    )
}

async fn blocking(
    exec_ctx: &Arc<ExecutionContext>,
    payload: RequestPayload,
    policy: Option<Arc<GatewayExecutionPolicy>>,
) -> Result<Value, String> {
    match ExecuteRequest::new(payload, Arc::clone(exec_ctx))
        .with_execution_policy(policy)
        .run()
        .await
    {
        Ok(Either::Left(response)) => Ok(serde_json::to_value(response).unwrap()),
        Ok(Either::Right(_)) => panic!("expected a blocking response"),
        Err(error) => Err(error.to_string()),
    }
}

fn types(output: &Value) -> Vec<&str> {
    output["output"]
        .as_array()
        .unwrap()
        .iter()
        .map(|item| item["type"].as_str().unwrap())
        .collect()
}

#[tokio::test]
async fn hosted_shell_fails_closed_before_inference() {
    let server = MockServer::start_deque(vec![text_response("unused")]).await;
    let hosted = json!({"model": "m", "input": "hi", "store": false,
        "tools": [{"type": "shell", "environment": {"type": "container_auto"}}]});

    // No backend registered.
    let error = blocking(&context(&server, None).await, request(hosted.clone()), None)
        .await
        .unwrap_err();
    assert!(error.contains("gateway shell backend"), "{error}");

    // Backend registered, but the caller has no trusted grant.
    let backend = Arc::new(FakeShell::default());
    let exec_ctx = context(&server, Some(Arc::clone(&backend))).await;
    let error = blocking(&exec_ctx, request(hosted.clone()), None).await.unwrap_err();
    assert!(error.contains("execution policy"), "{error}");
    let mut ungranted = (*policy("p1", ShellClientView::Native, None)).clone();
    ungranted.shell = None;
    let error = blocking(&exec_ctx, request(hosted), Some(Arc::new(ungranted)))
        .await
        .unwrap_err();
    assert!(error.contains("not granted"), "{error}");

    // The backend refuses an environment the caller does not own.
    let forbidden = json!({"model": "m", "input": "hi", "store": false,
        "tools": [{"type": "shell", "environment": {"type": "container_reference", "container_id": "cntr_forbidden"}}]});
    let error = blocking(
        &exec_ctx,
        request(forbidden),
        Some(policy("p1", ShellClientView::Native, None)),
    )
    .await
    .unwrap_err();
    assert!(error.contains("rejected"), "{error}");

    assert!(server.request_bodies().await.is_empty(), "no inference may run");
    assert_eq!(backend.runs.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn local_shell_stays_client_executed_under_a_grant() {
    let server = MockServer::start_deque(vec![shell_call(&json!({"commands": ["pwd"]}))]).await;
    let backend = Arc::new(FakeShell::default());
    let exec_ctx = context(&server, Some(Arc::clone(&backend))).await;
    let response = blocking(
        &exec_ctx,
        request(json!({"model": "m", "input": "hi", "store": false,
            "tools": [{"type": "shell", "environment": {"type": "local"}}]})),
        Some(policy("p1", ShellClientView::Native, None)),
    )
    .await
    .unwrap();
    assert_eq!(types(&response), ["shell_call"]);
    assert_eq!(backend.runs.load(Ordering::SeqCst), 0);
    assert!(backend.prepared.lock().await.is_empty());
}

#[tokio::test]
async fn native_view_pairs_calls_with_outputs_and_reinjects_them() {
    let server = MockServer::start_deque(vec![
        shell_call(&json!({"commands": ["pwd", "ls"], "timeout_ms": 999_999_999, "max_output_length": 64})),
        text_response("done"),
    ])
    .await;
    let backend = Arc::new(FakeShell::default());
    let exec_ctx = context(&server, Some(Arc::clone(&backend))).await;
    let response = blocking(
        &exec_ctx,
        request(json!({"model": "m", "input": "inspect", "store": false})),
        Some(policy("p1", ShellClientView::Native, Some("cntr_1"))),
    )
    .await
    .unwrap();

    assert_eq!(types(&response), ["shell_call", "shell_call_output", "message"]);
    let call = &response["output"][0];
    let output = &response["output"][1];
    assert_eq!(call["call_id"], output["call_id"]);
    assert_eq!(call["status"], "completed");
    assert_eq!(
        call["environment"],
        json!({"type": "container_reference", "container_id": "cntr_1"})
    );
    assert_eq!(output["output"][0]["stdout"], "ran:pwd\n");
    assert_eq!(output["output"][1]["outcome"], json!({"type": "exit", "exit_code": 1}));

    let commands = backend.commands.lock().await.clone();
    assert_eq!(commands.len(), 2);
    assert!(commands.iter().all(|(container, _, _, _)| container == "cntr_1"));
    assert_eq!(commands[0].1, commands[1].1, "one stable execution identity per call");
    assert_eq!(
        commands[0].3.timeout,
        Duration::from_secs(30),
        "timeout is clamped to policy"
    );
    assert_eq!(commands[0].3.max_output_bytes, 64);

    let bodies = server.request_bodies().await;
    assert_eq!(
        bodies[0]["tools"][0]["name"], "shell",
        "the gateway declared the granted shell"
    );
    let second = bodies[1]["input"].as_array().unwrap();
    let call_index = second.iter().position(|item| item["type"] == "function_call").unwrap();
    assert_eq!(second[call_index + 1]["type"], "function_call_output");
    let reinjected: Value = serde_json::from_str(second[call_index + 1]["output"].as_str().unwrap()).unwrap();
    assert_eq!(reinjected[0]["stdout"], "ran:pwd\n");
}

#[tokio::test]
async fn native_view_streams_one_lifecycle_per_item() {
    let server = MockServer::start_deque(vec![sse_shell_call(&json!({"commands": ["pwd"]})), sse_text("done")]).await;
    let backend = Arc::new(FakeShell::default());
    let exec_ctx = context(&server, Some(backend)).await;
    let result = ExecuteRequest::new(
        request(json!({"model": "m", "input": "inspect", "store": false, "stream": true})),
        Arc::clone(&exec_ctx),
    )
    .with_execution_policy(Some(policy("p1", ShellClientView::Native, Some("cntr_1"))))
    .run()
    .await
    .unwrap();
    let Either::Right(mut stream) = result else {
        panic!("expected a stream");
    };
    let mut events = Vec::new();
    while let Some(chunk) = stream.next().await {
        if let Some(event) = support::streamed_sse_event(&chunk) {
            events.push(event);
        }
    }
    let sequence: Vec<u64> = events
        .iter()
        .filter_map(|event| event["sequence_number"].as_u64())
        .collect();
    assert!(sequence.windows(2).all(|pair| pair[0] + 1 == pair[1]), "{sequence:?}");
    let lifecycle: Vec<(String, u64, String)> = events
        .iter()
        .filter(|event| {
            matches!(
                event["type"].as_str(),
                Some("response.output_item.added" | "response.output_item.done")
            )
        })
        .map(|event| {
            (
                event["type"]
                    .as_str()
                    .unwrap()
                    .trim_start_matches("response.output_item.")
                    .to_owned(),
                event["output_index"].as_u64().unwrap(),
                event["item"]["type"].as_str().unwrap().to_owned(),
            )
        })
        .collect();
    let expected = [
        ("added", 0, "shell_call"),
        ("done", 0, "shell_call"),
        ("added", 1, "shell_call_output"),
        ("done", 1, "shell_call_output"),
    ];
    for (expected, actual) in expected.iter().zip(&lifecycle) {
        assert_eq!(
            (expected.0, expected.1, expected.2),
            (actual.0.as_str(), actual.1, actual.2.as_str())
        );
    }
    assert!(
        lifecycle
            .iter()
            .any(|(_, index, kind)| *index == 2 && kind == "message"),
        "{lifecycle:?}"
    );
    let completed = events.last().unwrap();
    assert_eq!(completed["type"], "response.completed");
    let output = &completed["response"]["output"];
    assert_eq!(output[0]["call_id"], output[1]["call_id"]);
}

#[tokio::test]
async fn sealed_view_round_trips_without_reexecution() {
    let server = MockServer::start_deque(vec![
        shell_call(&json!({"commands": ["echo hi > f.txt"]})),
        text_response("wrote it"),
        text_response("still there"),
    ])
    .await;
    let backend = Arc::new(FakeShell::default());
    let exec_ctx = context(&server, Some(Arc::clone(&backend))).await;
    let grant = policy("p1", ShellClientView::SealedReasoning, Some("cntr_1"));
    let first = blocking(
        &exec_ctx,
        request(json!({"model": "m", "input": [{"role": "user", "content": "write"}], "store": false})),
        Some(Arc::clone(&grant)),
    )
    .await
    .unwrap();
    assert_eq!(types(&first), ["reasoning", "message"]);
    let carrier = first["output"][0].clone();
    assert!(
        carrier["encrypted_content"]
            .as_str()
            .unwrap()
            .starts_with("agentic-shell.v1.")
    );
    assert!(
        carrier["summary"][0]["text"]
            .as_str()
            .unwrap()
            .contains("$ echo hi > f.txt -> exit 0")
    );

    // A stateless client replays the full history, as stock Codex does.
    let replay = json!({"model": "m", "store": false, "input": [
        {"role": "user", "content": "write"},
        carrier,
        first["output"][1],
        {"role": "user", "content": "check"},
    ]});
    let second = blocking(&exec_ctx, request(replay.clone()), Some(Arc::clone(&grant)))
        .await
        .unwrap();
    assert_eq!(types(&second), ["message"]);
    assert_eq!(
        backend.runs.load(Ordering::SeqCst),
        1,
        "completed commands are never re-run"
    );

    let bodies = server.request_bodies().await;
    let restored = bodies[2]["input"].as_array().unwrap();
    let kinds: Vec<&str> = restored.iter().filter_map(|item| item["type"].as_str()).collect();
    assert!(
        kinds.contains(&"function_call") && kinds.contains(&"function_call_output"),
        "{kinds:?}"
    );
    assert!(
        !kinds.contains(&"reasoning"),
        "carriers never reach the model: {kinds:?}"
    );

    // Another principal, a forged carrier, or an unauthenticated caller fail closed.
    let error = blocking(
        &exec_ctx,
        request(replay.clone()),
        Some(policy("p2", ShellClientView::SealedReasoning, Some("cntr_1"))),
    )
    .await
    .unwrap_err();
    assert!(error.contains("different caller"), "{error}");
    let mut forged = replay.clone();
    let token = forged["input"][1]["encrypted_content"].as_str().unwrap().to_owned();
    forged["input"][1]["encrypted_content"] = json!(format!("{}A", token));
    let error = blocking(&exec_ctx, request(forged), Some(Arc::clone(&grant)))
        .await
        .unwrap_err();
    assert!(error.contains("not issued"), "{error}");
    let error = blocking(&exec_ctx, request(replay), None).await.unwrap_err();
    assert!(error.contains("authenticated caller"), "{error}");
}

#[tokio::test]
async fn stored_continuation_keeps_one_canonical_call() {
    let server = MockServer::start_deque(vec![
        shell_call(&json!({"commands": ["pwd"]})),
        text_response("first"),
        text_response("second"),
    ])
    .await;
    let backend = Arc::new(FakeShell::default());
    let exec_ctx = context(&server, Some(Arc::clone(&backend))).await;
    let grant = policy("p1", ShellClientView::Native, Some("cntr_1"));
    let first = blocking(
        &exec_ctx,
        request(json!({"model": "m", "input": "go", "store": true})),
        Some(Arc::clone(&grant)),
    )
    .await
    .unwrap();
    let follow_up = json!({"model": "m", "input": "again", "store": true,
        "previous_response_id": first["id"]});
    blocking(&exec_ctx, request(follow_up), Some(grant)).await.unwrap();
    assert_eq!(backend.runs.load(Ordering::SeqCst), 1);
    let bodies = server.request_bodies().await;
    let history = bodies[2]["input"].as_array().unwrap();
    let calls = history.iter().filter(|item| item["call_id"] == "call_1").count();
    assert_eq!(calls, 2, "exactly one function_call and one output: {history:#?}");
    assert!(
        history
            .iter()
            .all(|item| item["type"] != "shell_call" && item["type"] != "shell_call_output")
    );
}

#[tokio::test]
async fn backend_failures_are_reported_to_the_model_and_marked_incomplete() {
    let server = MockServer::start_deque(vec![shell_call(&json!({"commands": ["pwd"]})), text_response("sorry")]).await;
    let backend = Arc::new(FakeShell {
        fail: true,
        ..FakeShell::default()
    });
    let exec_ctx = context(&server, Some(backend)).await;
    let response = blocking(
        &exec_ctx,
        request(json!({"model": "m", "input": "go", "store": false})),
        Some(policy("p1", ShellClientView::Native, Some("cntr_1"))),
    )
    .await
    .unwrap();
    assert_eq!(response["output"][0]["status"], "incomplete");
    assert!(
        response["output"][1]["error"]
            .as_str()
            .unwrap()
            .contains("workspace expired")
    );
    let bodies = server.request_bodies().await;
    let second = bodies[1]["input"].as_array().unwrap();
    let output = second
        .iter()
        .find(|item| item["type"] == "function_call_output")
        .unwrap();
    assert!(output["output"].as_str().unwrap().contains("did not complete"));
}

#[tokio::test]
async fn dropping_the_stream_cancels_backend_work() {
    let server = MockServer::start_deque(vec![sse_shell_call(&json!({"commands": ["sleep 60"]}))]).await;
    let backend = Arc::new(FakeShell {
        block: true,
        ..FakeShell::default()
    });
    let exec_ctx = context(&server, Some(Arc::clone(&backend))).await;
    let result = ExecuteRequest::new(
        request(json!({"model": "m", "input": "go", "store": false, "stream": true})),
        exec_ctx,
    )
    .with_execution_policy(Some(policy("p1", ShellClientView::Native, Some("cntr_1"))))
    .run()
    .await
    .unwrap();
    let Either::Right(mut stream) = result else {
        panic!("expected a stream");
    };
    while backend.runs.load(Ordering::SeqCst) == 0 {
        let _ = tokio::time::timeout(Duration::from_millis(50), stream.next()).await;
    }
    drop(stream);
    for _ in 0..100 {
        if backend.dropped.load(Ordering::SeqCst) == 1 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("backend command future was not dropped after client disconnect");
}

#[test]
fn weak_sealing_keys_are_rejected() {
    let registration = GatewayExecutorRegistration::shell(GatewayShellConfig {
        backend: Arc::new(FakeShell::default()),
        limits: ShellExecutionLimits::default(),
        sealing_key: b"short".to_vec(),
    });
    assert!(registration.is_err());
}
