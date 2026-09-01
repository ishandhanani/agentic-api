use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use agentic_core::config::{
    AgentRtExecutorConfig, Config, PostgresConfig, SqliteConfig, SubjectSigningKey, ToolRuntimeConfig,
};
use agentic_core::executor::{ExecuteRequest, ExecutionContext};
use agentic_core::types::io::OutputItem;
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
