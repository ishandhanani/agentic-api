use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use agentic_core::config::{
    AgentRtExecutorConfig, Config, PostgresConfig, SqliteConfig, SubjectSigningKey, ToolRuntimeConfig,
};
use agentic_core::executor::{ExecuteRequest, ExecutionContext};
use agentic_core::types::io::{CodeInterpreterOutput, OutputItem};
use axum::extract::State;
use axum::http::HeaderMap;
use axum::routing::post;
use either::Either;
use futures::StreamExt;
use tokio::sync::Mutex;

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

async fn inference_stream(
    State(state): State<InferenceState>,
    axum::Json(request): axum::Json<serde_json::Value>,
) -> impl axum::response::IntoResponse {
    let call = state.calls.fetch_add(1, Ordering::SeqCst);
    state.requests.lock().await.push(request);
    let events = if call == 0 {
        vec![
            serde_json::json!({
                "type": "response.created",
                "response": {"id": "upstream_stream_tool", "status": "in_progress", "usage": null}
            }),
            serde_json::json!({
                "type": "response.output_item.added",
                "output_index": 0,
                "item": {
                    "id": "fc_code_stream",
                    "type": "function_call",
                    "call_id": "call_code_stream",
                    "name": "code_interpreter",
                    "arguments": "",
                    "status": "in_progress"
                }
            }),
            serde_json::json!({
                "type": "response.function_call_arguments.done",
                "item_id": "fc_code_stream",
                "output_index": 0,
                "call_id": "call_code_stream",
                "name": "code_interpreter",
                "arguments": "{\"code\":\"print(40 + 2)\"}"
            }),
            serde_json::json!({
                "type": "response.completed",
                "response": {"id": "upstream_stream_tool", "status": "completed", "usage": null}
            }),
        ]
    } else {
        vec![
            serde_json::json!({
                "type": "response.created",
                "response": {"id": "upstream_stream_final", "status": "in_progress", "usage": null}
            }),
            serde_json::json!({
                "type": "response.output_item.added",
                "output_index": 0,
                "item": {
                    "id": "msg_stream_final",
                    "type": "message",
                    "role": "assistant",
                    "status": "in_progress",
                    "content": []
                }
            }),
            serde_json::json!({
                "type": "response.output_text.delta",
                "item_id": "msg_stream_final",
                "output_index": 0,
                "content_index": 0,
                "delta": "The result is 42."
            }),
            serde_json::json!({
                "type": "response.completed",
                "response": {"id": "upstream_stream_final", "status": "completed", "usage": null}
            }),
        ]
    };
    let mut body = String::new();
    for event in events {
        body.push_str("data: ");
        body.push_str(&serde_json::to_string(&event).expect("serialize SSE event"));
        body.push_str("\n\n");
    }
    body.push_str("data: [DONE]\n\n");
    ([(axum::http::header::CONTENT_TYPE, "text/event-stream")], body)
}

#[derive(Clone, Default)]
struct AgentRtState {
    calls: Arc<AtomicUsize>,
    requests: Arc<Mutex<Vec<serde_json::Value>>>,
}

#[derive(Clone, Default)]
struct ParallelAgentRtState {
    calls: Arc<AtomicUsize>,
    in_flight: Arc<AtomicUsize>,
    max_in_flight: Arc<AtomicUsize>,
}

#[derive(Clone, Default)]
struct DisconnectAgentRtState {
    request: Arc<Mutex<Option<serde_json::Value>>>,
    start_seen: Arc<tokio::sync::Notify>,
    cancel_count: Arc<AtomicUsize>,
    cancelled: Arc<AtomicBool>,
}

async fn agent_rt(
    State(state): State<AgentRtState>,
    headers: HeaderMap,
    axum::Json(request): axum::Json<serde_json::Value>,
) -> axum::Json<serde_json::Value> {
    assert!(
        headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.starts_with("Bearer v1."))
    );
    state.calls.fetch_add(1, Ordering::SeqCst);
    state.requests.lock().await.push(request.clone());
    axum::Json(serde_json::json!({
        "api_version": "v1",
        "execution_id": request["execution_id"],
        "workspace_id": request["workspace_id"],
        "route_id": request["route_id"],
        "route_version": "blake3:route-v1",
        "request_fingerprint": "blake3:request-v1",
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
    }))
}

async fn parallel_inference(
    State(state): State<InferenceState>,
    axum::Json(request): axum::Json<serde_json::Value>,
) -> axum::Json<serde_json::Value> {
    let call = state.calls.fetch_add(1, Ordering::SeqCst);
    state.requests.lock().await.push(request);
    let output = if call == 0 {
        serde_json::json!([
            {
                "id": "fc_slow",
                "type": "function_call",
                "call_id": "call_slow",
                "name": "code_interpreter",
                "arguments": "{\"code\":\"print('slow-first')\"}",
                "status": "completed"
            },
            {
                "id": "fc_fast",
                "type": "function_call",
                "call_id": "call_fast",
                "name": "code_interpreter",
                "arguments": "{\"code\":\"print('fast-second')\"}",
                "status": "completed"
            }
        ])
    } else {
        serde_json::json!([{
            "id": "msg_parallel_final",
            "type": "message",
            "role": "assistant",
            "status": "completed",
            "content": [{"type": "output_text", "text": "Both finished.", "annotations": []}]
        }])
    };
    axum::Json(serde_json::json!({
        "id": format!("upstream_parallel_{call}"),
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

async fn parallel_agent_rt(
    State(state): State<ParallelAgentRtState>,
    axum::Json(request): axum::Json<serde_json::Value>,
) -> axum::Json<serde_json::Value> {
    state.calls.fetch_add(1, Ordering::SeqCst);
    let in_flight = state.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
    state.max_in_flight.fetch_max(in_flight, Ordering::SeqCst);
    let code = request["input"]["argv"][2].as_str().expect("Python source in argv");
    let (delay, stdout) = if code.contains("slow-first") {
        (Duration::from_millis(80), "slow-first\n")
    } else {
        (Duration::from_millis(10), "fast-second\n")
    };
    tokio::time::sleep(delay).await;
    state.in_flight.fetch_sub(1, Ordering::SeqCst);
    axum::Json(serde_json::json!({
        "api_version": "v1",
        "execution_id": request["execution_id"],
        "workspace_id": request["workspace_id"],
        "route_id": request["route_id"],
        "route_version": "blake3:route-v1",
        "request_fingerprint": "blake3:request-v1",
        "revision": 2,
        "state": "completed",
        "result": {
            "schema_version": "sandbox-command-result-v1",
            "output": {"exit_code": 0, "stdout": stdout, "stderr": ""},
            "is_error": false
        },
        "failure": null,
        "artifacts": [],
        "accepted_at": "2026-09-01T00:00:00Z",
        "completed_at": "2026-09-01T00:00:01Z"
    }))
}

fn disconnect_record(request: &serde_json::Value, state: &str, revision: u64) -> serde_json::Value {
    serde_json::json!({
        "api_version": "v1",
        "execution_id": request["execution_id"],
        "workspace_id": request["workspace_id"],
        "route_id": request["route_id"],
        "route_version": "blake3:route-v1",
        "request_fingerprint": "blake3:request-v1",
        "revision": revision,
        "state": state,
        "result": null,
        "failure": null,
        "artifacts": [],
        "accepted_at": "2026-09-01T00:00:00Z",
        "completed_at": null
    })
}

async fn disconnect_start(
    State(state): State<DisconnectAgentRtState>,
    axum::Json(request): axum::Json<serde_json::Value>,
) -> axum::Json<serde_json::Value> {
    *state.request.lock().await = Some(request.clone());
    state.start_seen.notify_one();
    axum::Json(disconnect_record(&request, "accepted", 1))
}

async fn disconnect_lookup(
    State(state): State<DisconnectAgentRtState>,
    axum::extract::Path(execution_action): axum::extract::Path<String>,
) -> Result<axum::Json<serde_json::Value>, axum::http::StatusCode> {
    let Some(request) = state.request.lock().await.clone() else {
        return Err(axum::http::StatusCode::NOT_FOUND);
    };
    assert_eq!(request["execution_id"], execution_action);
    if state.cancelled.load(Ordering::SeqCst) {
        return Ok(axum::Json(disconnect_record(&request, "cancelled", 3)));
    }
    tokio::time::sleep(Duration::from_secs(5)).await;
    Ok(axum::Json(disconnect_record(&request, "running", 2)))
}

async fn disconnect_cancel(
    State(state): State<DisconnectAgentRtState>,
    axum::extract::Path(execution_action): axum::extract::Path<String>,
) -> Result<axum::Json<serde_json::Value>, axum::http::StatusCode> {
    let execution_id = execution_action
        .strip_suffix(":cancel")
        .ok_or(axum::http::StatusCode::NOT_FOUND)?;
    let Some(request) = state.request.lock().await.clone() else {
        return Err(axum::http::StatusCode::NOT_FOUND);
    };
    assert_eq!(request["execution_id"], execution_id);
    state.cancel_count.fetch_add(1, Ordering::SeqCst);
    state.cancelled.store(true, Ordering::SeqCst);
    Ok(axum::Json(disconnect_record(&request, "cancelled", 3)))
}

async fn serve(app: axum::Router) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test server");
    let address = listener.local_addr().expect("test server address");
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.ok();
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

fn chunk_events(chunk: &str) -> impl Iterator<Item = serde_json::Value> + '_ {
    chunk
        .lines()
        .filter_map(|line| line.strip_prefix("data: "))
        .filter(|data| *data != "[DONE]")
        .filter_map(|data| serde_json::from_str(data).ok())
}

#[tokio::test]
async fn code_interpreter_round_uses_agent_rt_and_reinjects_only_into_dynamo_inference() {
    let inference_state = InferenceState::default();
    let (inference_url, inference_server) = serve(
        axum::Router::new()
            .route("/v1/responses", post(inference))
            .with_state(inference_state.clone()),
    )
    .await;
    let agent_rt_state = AgentRtState::default();
    let (agent_rt_url, agent_rt_server) = serve(
        axum::Router::new()
            .route("/internal/v1/executions", post(agent_rt))
            .with_state(agent_rt_state.clone()),
    )
    .await;

    let config = test_config(inference_url, agent_rt_url);
    let exec_ctx = Arc::new(ExecutionContext::from_config(&config).await.expect("execution context"));
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
    assert_eq!(agent_rt_state.calls.load(Ordering::SeqCst), 1);
    assert!(
        response
            .output
            .iter()
            .any(|item| matches!(item, OutputItem::CodeInterpreterCall(call) if call.container_id.starts_with("ws_")))
    );
    let inference_requests = inference_state.requests.lock().await;
    assert_eq!(inference_requests.len(), 2);
    assert!(inference_requests[0]["tools"].as_array().is_some_and(|tools| {
        tools
            .iter()
            .any(|tool| tool["function"]["name"] == "code_interpreter" || tool["name"] == "code_interpreter")
    }));
    let reinjected = inference_requests[1]["input"].to_string();
    assert!(reinjected.contains("call_code_1"));
    assert!(reinjected.contains("function_call_output"));
    assert!(!inference_requests[1].to_string().contains("previous_response_id"));

    let execution_requests = agent_rt_state.requests.lock().await;
    assert_eq!(execution_requests.len(), 1);
    assert_eq!(execution_requests[0]["route_id"], "sandbox.python.default");
    assert!(execution_requests[0].get("provider").is_none());
    assert!(execution_requests[0].get("credentials").is_none());

    let ledger_state: String = sqlx::query_scalar("SELECT state FROM remote_executions")
        .fetch_one(exec_ctx.storage_pool().expect("configured ledger"))
        .await
        .expect("ledger row");
    assert_eq!(ledger_state, "completed");

    inference_server.abort();
    agent_rt_server.abort();
}

#[tokio::test]
async fn previous_response_continuation_hydrates_remote_tool_history_for_dynamo() {
    let inference_state = InferenceState::default();
    let (inference_url, inference_server) = serve(
        axum::Router::new()
            .route("/v1/responses", post(inference))
            .with_state(inference_state.clone()),
    )
    .await;
    let agent_rt_state = AgentRtState::default();
    let (agent_rt_url, agent_rt_server) = serve(
        axum::Router::new()
            .route("/internal/v1/executions", post(agent_rt))
            .with_state(agent_rt_state.clone()),
    )
    .await;
    let exec_ctx = Arc::new(
        ExecutionContext::from_config(&test_config(inference_url, agent_rt_url))
            .await
            .expect("execution context"),
    );
    let first_payload = serde_json::from_value(serde_json::json!({
        "model": "test-model",
        "input": "Calculate 40 + 2 with Python.",
        "store": true,
        "stream": false,
        "tools": [{"type": "code_interpreter"}]
    }))
    .expect("first request payload");
    let Either::Left(first) = ExecuteRequest::new(first_payload, Arc::clone(&exec_ctx))
        .run()
        .await
        .expect("stored remote tool turn")
    else {
        panic!("expected blocking response");
    };
    let second_payload = serde_json::from_value(serde_json::json!({
        "model": "test-model",
        "input": "Confirm the prior result.",
        "previous_response_id": first.id,
        "store": true,
        "stream": false,
        "tools": [{"type": "code_interpreter"}]
    }))
    .expect("continuation payload");
    let Either::Left(_) = ExecuteRequest::new(second_payload, Arc::clone(&exec_ctx))
        .run()
        .await
        .expect("hydrated continuation")
    else {
        panic!("expected blocking continuation");
    };

    assert_eq!(agent_rt_state.calls.load(Ordering::SeqCst), 1);
    let requests = inference_state.requests.lock().await;
    assert_eq!(requests.len(), 3);
    let continuation = requests[2].to_string();
    for expected in [
        "Calculate 40 + 2 with Python.",
        "call_code_1",
        "function_call_output",
        "42",
        "The result is 42.",
        "Confirm the prior result.",
    ] {
        assert!(
            continuation.contains(expected),
            "hydrated continuation omitted {expected}: {continuation}"
        );
    }
    assert!(!continuation.contains("previous_response_id"));
    inference_server.abort();
    agent_rt_server.abort();
}

#[tokio::test]
async fn parallel_agent_rt_calls_overlap_and_retain_model_call_order() {
    let inference_state = InferenceState::default();
    let (inference_url, inference_server) = serve(
        axum::Router::new()
            .route("/v1/responses", post(parallel_inference))
            .with_state(inference_state.clone()),
    )
    .await;
    let agent_rt_state = ParallelAgentRtState::default();
    let (agent_rt_url, agent_rt_server) = serve(
        axum::Router::new()
            .route("/internal/v1/executions", post(parallel_agent_rt))
            .with_state(agent_rt_state.clone()),
    )
    .await;
    let exec_ctx = Arc::new(
        ExecutionContext::from_config(&test_config(inference_url, agent_rt_url))
            .await
            .expect("execution context"),
    );
    let payload = serde_json::from_value(serde_json::json!({
        "model": "test-model",
        "input": "Run both snippets.",
        "store": false,
        "stream": false,
        "parallel_tool_calls": true,
        "tools": [{"type": "code_interpreter"}]
    }))
    .expect("request payload");

    let Either::Left(response) = ExecuteRequest::new(payload, Arc::clone(&exec_ctx))
        .run()
        .await
        .expect("parallel agentic tool round")
    else {
        panic!("expected blocking response");
    };

    assert_eq!(agent_rt_state.calls.load(Ordering::SeqCst), 2);
    assert_eq!(agent_rt_state.max_in_flight.load(Ordering::SeqCst), 2);
    let calls = response
        .output
        .iter()
        .filter_map(|item| match item {
            OutputItem::CodeInterpreterCall(call) => Some((call.id.as_str(), call.code.as_str(), &call.outputs)),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[0].0, "fc_slow");
    assert_eq!(calls[0].1, "print('slow-first')");
    assert!(matches!(calls[0].2.as_slice(), [CodeInterpreterOutput::Logs { logs }] if logs == "slow-first\n"));
    assert_eq!(calls[1].0, "fc_fast");
    assert_eq!(calls[1].1, "print('fast-second')");
    assert!(matches!(calls[1].2.as_slice(), [CodeInterpreterOutput::Logs { logs }] if logs == "fast-second\n"));

    let ledger_rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM remote_executions")
        .fetch_one(exec_ctx.storage_pool().expect("configured ledger"))
        .await
        .expect("ledger rows");
    assert_eq!(ledger_rows, 2);
    inference_server.abort();
    agent_rt_server.abort();
}

#[tokio::test]
async fn streaming_code_interpreter_persists_remote_outcome_before_public_completion() {
    let inference_state = InferenceState::default();
    let (inference_url, inference_server) = serve(
        axum::Router::new()
            .route("/v1/responses", post(inference_stream))
            .with_state(inference_state.clone()),
    )
    .await;
    let agent_rt_state = AgentRtState::default();
    let (agent_rt_url, agent_rt_server) = serve(
        axum::Router::new()
            .route("/internal/v1/executions", post(agent_rt))
            .with_state(agent_rt_state.clone()),
    )
    .await;
    let exec_ctx = Arc::new(
        ExecutionContext::from_config(&test_config(inference_url, agent_rt_url))
            .await
            .expect("execution context"),
    );
    let payload = serde_json::from_value(serde_json::json!({
        "model": "test-model",
        "input": "Calculate 40 + 2 with Python.",
        "store": false,
        "stream": true,
        "tools": [{"type": "code_interpreter"}]
    }))
    .expect("request payload");

    let Either::Right(stream) = ExecuteRequest::new(payload, Arc::clone(&exec_ctx))
        .run()
        .await
        .expect("streaming agentic tool round")
    else {
        panic!("expected streaming response");
    };
    let mut stream = Box::pin(stream);
    let mut event_types = Vec::new();
    while let Some(chunk) = stream.next().await {
        for event in chunk_events(&chunk) {
            let Some(event_type) = event["type"].as_str() else {
                continue;
            };
            if event_type == "response.code_interpreter_call.completed" {
                let ledger_state: String = sqlx::query_scalar("SELECT state FROM remote_executions")
                    .fetch_one(exec_ctx.storage_pool().expect("configured ledger"))
                    .await
                    .expect("ledger row must exist before public completion");
                assert_eq!(ledger_state, "completed");
            }
            event_types.push(event_type.to_owned());
        }
    }

    let tool_completed = event_types
        .iter()
        .position(|event| event == "response.code_interpreter_call.completed")
        .unwrap_or_else(|| panic!("native code interpreter completion event missing: {event_types:?}"));
    let response_completed = event_types
        .iter()
        .rposition(|event| event == "response.completed")
        .expect("terminal response event");
    assert!(tool_completed < response_completed);
    assert_eq!(inference_state.calls.load(Ordering::SeqCst), 2);
    assert_eq!(agent_rt_state.calls.load(Ordering::SeqCst), 1);
    let inference_requests = inference_state.requests.lock().await;
    assert!(inference_requests.iter().all(|request| request["stream"] == true));
    assert!(!inference_requests[1].to_string().contains("previous_response_id"));

    inference_server.abort();
    agent_rt_server.abort();
}

#[tokio::test]
async fn dropping_public_stream_cancels_remote_execution_and_keeps_it_queryable() {
    let inference_state = InferenceState::default();
    let (inference_url, inference_server) = serve(
        axum::Router::new()
            .route("/v1/responses", post(inference_stream))
            .with_state(inference_state),
    )
    .await;
    let agent_rt_state = DisconnectAgentRtState::default();
    let (agent_rt_url, agent_rt_server) = serve(
        axum::Router::new()
            .route("/internal/v1/executions", post(disconnect_start))
            .route(
                "/internal/v1/executions/{execution_id}",
                axum::routing::get(disconnect_lookup).post(disconnect_cancel),
            )
            .with_state(agent_rt_state.clone()),
    )
    .await;
    let exec_ctx = Arc::new(
        ExecutionContext::from_config(&test_config(inference_url, agent_rt_url.clone()))
            .await
            .expect("execution context"),
    );
    let payload = serde_json::from_value(serde_json::json!({
        "model": "test-model",
        "input": "Run until disconnected.",
        "store": false,
        "stream": true,
        "tools": [{"type": "code_interpreter"}]
    }))
    .expect("request payload");
    let Either::Right(mut stream) = ExecuteRequest::new(payload, Arc::clone(&exec_ctx))
        .run()
        .await
        .expect("streaming agentic tool round")
    else {
        panic!("expected streaming response");
    };
    let consumer = tokio::spawn(async move { while stream.next().await.is_some() {} });
    tokio::time::timeout(Duration::from_secs(2), agent_rt_state.start_seen.notified())
        .await
        .expect("remote execution was not accepted");
    consumer.abort();
    let _ = consumer.await;
    tokio::time::timeout(Duration::from_secs(2), async {
        while agent_rt_state.cancel_count.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("disconnect did not send best-effort cancellation");
    assert_eq!(agent_rt_state.cancel_count.load(Ordering::SeqCst), 1);

    let request = agent_rt_state
        .request
        .lock()
        .await
        .clone()
        .expect("accepted execution request");
    let retained = reqwest::Client::new()
        .get(format!(
            "{agent_rt_url}/internal/v1/executions/{}",
            request["execution_id"].as_str().expect("execution ID")
        ))
        .send()
        .await
        .expect("lookup retained execution");
    assert_eq!(retained.status(), reqwest::StatusCode::OK);
    assert_eq!(
        retained.json::<serde_json::Value>().await.unwrap()["state"],
        "cancelled"
    );
    let ledger_rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM remote_executions")
        .fetch_one(exec_ctx.storage_pool().expect("configured ledger"))
        .await
        .expect("ledger row survives disconnect");
    assert_eq!(ledger_rows, 1);
    inference_server.abort();
    agent_rt_server.abort();
}
