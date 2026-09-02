use axum::body::Body;
use axum::extract::{Extension, FromRequest, Multipart, Path, Query, Request, State};
use axum::http::{HeaderMap, header};
use axum::response::{IntoResponse, Response};

use agentic_core::executor::ExecutorError;
use agentic_core::{
    AuthenticatedSubject, ContainerService, CreateContainerFileRequest, CreateContainerRequest,
    ListContainerFilesRequest, ListContainersRequest,
};

use super::super::common::{executor_error_response, read_json};
use crate::app::AppState;
use crate::auth::AuthenticatedPrincipal;

pub async fn create_container(
    State(state): State<AppState>,
    principal: Option<Extension<AuthenticatedPrincipal>>,
    request: Request,
) -> Response {
    let (parts, body) = request.into_parts();
    let payload = match read_json::<CreateContainerRequest>(body).await {
        Ok(payload) => payload,
        Err(response) => return response,
    };
    let (service, subject) = match service_and_subject(&state, principal.as_deref()) {
        Ok(values) => values,
        Err(error) => return invalid_request(error),
    };
    match service.create(&subject, payload, traceparent(&parts.headers)).await {
        Ok(container) => axum::Json(container).into_response(),
        Err(error) => executor_error_response(error.into()),
    }
}

pub async fn retrieve_container(
    State(state): State<AppState>,
    Path(container_id): Path<String>,
    principal: Option<Extension<AuthenticatedPrincipal>>,
    headers: HeaderMap,
) -> Response {
    let (service, subject) = match service_and_subject(&state, principal.as_deref()) {
        Ok(values) => values,
        Err(error) => return invalid_request(error),
    };
    match service.retrieve(&subject, &container_id, traceparent(&headers)).await {
        Ok(container) => axum::Json(container).into_response(),
        Err(error) => executor_error_response(error.into()),
    }
}

pub async fn list_containers(
    State(state): State<AppState>,
    Query(request): Query<ListContainersRequest>,
    principal: Option<Extension<AuthenticatedPrincipal>>,
) -> Response {
    let (service, subject) = match service_and_subject(&state, principal.as_deref()) {
        Ok(values) => values,
        Err(error) => return invalid_request(error),
    };
    match service.list(&subject, request).await {
        Ok(containers) => axum::Json(containers).into_response(),
        Err(error) => executor_error_response(error.into()),
    }
}

pub async fn delete_container(
    State(state): State<AppState>,
    Path(container_id): Path<String>,
    principal: Option<Extension<AuthenticatedPrincipal>>,
    headers: HeaderMap,
) -> Response {
    let (service, subject) = match service_and_subject(&state, principal.as_deref()) {
        Ok(values) => values,
        Err(error) => return invalid_request(error),
    };
    match service.delete(&subject, &container_id, traceparent(&headers)).await {
        Ok(deleted) => axum::Json(deleted).into_response(),
        Err(error) => executor_error_response(error.into()),
    }
}

pub async fn create_container_file(
    State(state): State<AppState>,
    Path(container_id): Path<String>,
    principal: Option<Extension<AuthenticatedPrincipal>>,
    request: Request,
) -> Response {
    let (service, subject) = match service_and_subject(&state, principal.as_deref()) {
        Ok(values) => values,
        Err(error) => return invalid_request(error),
    };
    let traceparent = traceparent(request.headers()).map(str::to_owned);
    let content_type = request
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    if content_type.starts_with("application/json") {
        let (_, body) = request.into_parts();
        let payload = match read_json::<CreateContainerFileRequest>(body).await {
            Ok(payload) => payload,
            Err(response) => return response,
        };
        return invalid_request(format!(
            "file_id '{}' requires an OpenAI Files binding that is not configured",
            payload.file_id
        ));
    }
    if !content_type.starts_with("multipart/form-data") {
        return invalid_request("container file upload requires multipart/form-data or a JSON file_id".to_owned());
    }
    let mut multipart = match Multipart::from_request(request, &state).await {
        Ok(multipart) => multipart,
        Err(error) => return invalid_request(format!("invalid container file upload: {error}")),
    };
    let mut upload = None;
    while let Some(field) = match multipart.next_field().await {
        Ok(field) => field,
        Err(error) => return invalid_request(format!("invalid container file upload: {error}")),
    } {
        match field.name() {
            Some("file") => {
                if upload.is_some() {
                    return invalid_request("container file upload must contain exactly one file".to_owned());
                }
                let filename = match field.file_name() {
                    Some(filename) => filename.to_owned(),
                    None => return invalid_request("container file upload is missing a filename".to_owned()),
                };
                let content = match field.bytes().await {
                    Ok(content) => content,
                    Err(error) => return invalid_request(format!("invalid container file upload: {error}")),
                };
                upload = Some((filename, content));
            }
            Some("file_id") => {
                return invalid_request("file_id requires an OpenAI Files binding that is not configured".to_owned());
            }
            _ => {}
        }
    }
    let Some((filename, content)) = upload else {
        return invalid_request("container file upload must contain a file field".to_owned());
    };
    match service
        .create_file(&subject, &container_id, &filename, content, traceparent.as_deref())
        .await
    {
        Ok(file) => axum::Json(file).into_response(),
        Err(error) => executor_error_response(error.into()),
    }
}

pub async fn list_container_files(
    State(state): State<AppState>,
    Path(container_id): Path<String>,
    Query(request): Query<ListContainerFilesRequest>,
    principal: Option<Extension<AuthenticatedPrincipal>>,
) -> Response {
    let (service, subject) = match service_and_subject(&state, principal.as_deref()) {
        Ok(values) => values,
        Err(error) => return invalid_request(error),
    };
    match service.list_files(&subject, &container_id, request).await {
        Ok(files) => axum::Json(files).into_response(),
        Err(error) => executor_error_response(error.into()),
    }
}

pub async fn retrieve_container_file(
    State(state): State<AppState>,
    Path((container_id, file_id)): Path<(String, String)>,
    principal: Option<Extension<AuthenticatedPrincipal>>,
    headers: HeaderMap,
) -> Response {
    let (service, subject) = match service_and_subject(&state, principal.as_deref()) {
        Ok(values) => values,
        Err(error) => return invalid_request(error),
    };
    match service
        .retrieve_file(&subject, &container_id, &file_id, traceparent(&headers))
        .await
    {
        Ok(file) => axum::Json(file).into_response(),
        Err(error) => executor_error_response(error.into()),
    }
}

pub async fn retrieve_container_file_content(
    State(state): State<AppState>,
    Path((container_id, file_id)): Path<(String, String)>,
    principal: Option<Extension<AuthenticatedPrincipal>>,
    headers: HeaderMap,
) -> Response {
    let (service, subject) = match service_and_subject(&state, principal.as_deref()) {
        Ok(values) => values,
        Err(error) => return invalid_request(error),
    };
    match service
        .read_file_content(&subject, &container_id, &file_id, traceparent(&headers))
        .await
    {
        Ok(content) => {
            let mut response = Response::new(Body::from_stream(content));
            response.headers_mut().insert(
                header::CONTENT_TYPE,
                header::HeaderValue::from_static("application/octet-stream"),
            );
            response
        }
        Err(error) => executor_error_response(error.into()),
    }
}

pub async fn delete_container_file(
    State(state): State<AppState>,
    Path((container_id, file_id)): Path<(String, String)>,
    principal: Option<Extension<AuthenticatedPrincipal>>,
    headers: HeaderMap,
) -> Response {
    let (service, subject) = match service_and_subject(&state, principal.as_deref()) {
        Ok(values) => values,
        Err(error) => return invalid_request(error),
    };
    match service
        .delete_file(&subject, &container_id, &file_id, traceparent(&headers))
        .await
    {
        Ok(deleted) => axum::Json(deleted).into_response(),
        Err(error) => executor_error_response(error.into()),
    }
}

fn service_and_subject<'a>(
    state: &'a AppState,
    principal: Option<&AuthenticatedPrincipal>,
) -> Result<(&'a ContainerService, AuthenticatedSubject), String> {
    let service = state
        .exec_ctx
        .container_service()
        .ok_or_else(|| "containers require an SHED_ENDPOINT configuration".to_owned())?;
    let subject = state
        .exec_ctx
        .remote_execution_subject(principal.map(AuthenticatedPrincipal::subject))
        .ok_or_else(|| "containers require an authenticated tenant and principal".to_owned())?;
    Ok((service, subject))
}

fn traceparent(headers: &HeaderMap) -> Option<&str> {
    headers.get("traceparent").and_then(|value| value.to_str().ok())
}

fn invalid_request(message: String) -> Response {
    executor_error_response(ExecutorError::InvalidRequest(message))
}
