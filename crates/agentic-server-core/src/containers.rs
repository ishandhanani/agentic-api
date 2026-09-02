use std::pin::Pin;
use std::time::{SystemTime, UNIX_EPOCH};

use agent_rt_control::{Workspace, WorkspaceState};
use bytes::Bytes;
use futures::Stream;
use tonic::Code;
use uuid::Uuid;

use crate::executor::ExecutorError;
use crate::storage::{
    ClaimContainer, ClaimContainerFile, ContainerFileRecord, ContainerFileStore, ContainerOrder, ContainerRecord,
    ContainerStore, StorageError,
};
use crate::tool::agent_rt::{AgentRtClient, RemoteFileWrite, RemoteTransportError};
use crate::tool::{AuthenticatedSubject, ToolError};
use crate::types::{
    Container, ContainerExpiration, ContainerExpirationAnchor, ContainerFile, ContainerFileList, ContainerList,
    CreateContainerRequest, DeletedContainer, DeletedContainerFile, ListContainerFilesRequest, ListContainersRequest,
    ListOrder, ShellMemoryLimitParam,
};

const DEFAULT_LIST_LIMIT: u32 = 20;
const MAX_LIST_LIMIT: u32 = 100;
const MAX_CONTAINER_NAME_BYTES: usize = 256;
const FILE_CHUNK_BYTES: usize = 1024 * 1024;
const FILE_CHUNK_BYTES_U64: u64 = 1024 * 1024;

/// Raw container file bytes streamed from agent-rt.
pub type ContainerFileContent = Pin<Box<dyn Stream<Item = Result<Bytes, ContainerError>> + Send>>;

#[derive(Clone, Debug)]
pub struct ContainerService {
    client: AgentRtClient,
    store: ContainerStore,
    file_store: ContainerFileStore,
}

#[derive(Debug, thiserror::Error)]
pub enum ContainerError {
    #[error("invalid container request: {0}")]
    Invalid(String),
    #[error("{entity} not found: {id}")]
    NotFound { entity: String, id: String },
    #[error("container persistence failed")]
    Storage(#[source] StorageError),
    #[error("container control plane failed: {0}")]
    Remote(String),
    #[error("container control plane is not configured correctly: {0}")]
    Config(String),
}

impl ContainerService {
    pub(crate) fn new(client: AgentRtClient, store: ContainerStore, file_store: ContainerFileStore) -> Self {
        Self {
            client,
            store,
            file_store,
        }
    }

    /// Creates a public container backed by an agent-rt workspace.
    ///
    /// # Errors
    ///
    /// Returns an error when the request is invalid, persistence fails, or agent-rt rejects workspace creation.
    pub async fn create(
        &self,
        subject: &AuthenticatedSubject,
        request: CreateContainerRequest,
        traceparent: Option<&str>,
    ) -> Result<Container, ContainerError> {
        validate_create_request(&request)?;
        let name = request.name.trim();
        let id = format!("cntr_{}", Uuid::now_v7().simple());
        let workspace_class_id = self.client.config().workspace_class_id.as_str();
        let memory_limit = memory_limit_str(request.memory_limit.unwrap_or(ShellMemoryLimitParam::OneGiB));
        self.store
            .claim(ClaimContainer {
                subject,
                id: &id,
                name,
                workspace_class_id,
                memory_limit,
                created_at_millis: unix_millis(),
            })
            .await
            .map_err(ContainerError::from_storage)?;
        let token = self
            .client
            .sign_subject(subject, &id)
            .map_err(ContainerError::from_tool)?;
        let workspace = self
            .client
            .create_workspace(&id, workspace_class_id, &token, traceparent)
            .await
            .map_err(|error| ContainerError::from_remote("container", &id, error))?;
        self.sync_workspace(subject, &id, workspace).await
    }

    /// Retrieves and refreshes a public container from its agent-rt workspace.
    ///
    /// # Errors
    ///
    /// Returns an error when the container does not exist or agent-rt cannot resolve its workspace.
    pub async fn retrieve(
        &self,
        subject: &AuthenticatedSubject,
        id: &str,
        traceparent: Option<&str>,
    ) -> Result<Container, ContainerError> {
        self.store
            .get(subject, id)
            .await
            .map_err(ContainerError::from_storage)?;
        let token = self
            .client
            .sign_subject(subject, id)
            .map_err(ContainerError::from_tool)?;
        let workspace = self
            .client
            .get_workspace(id, &token, traceparent)
            .await
            .map_err(|error| ContainerError::from_remote("container", id, error))?;
        self.sync_workspace(subject, id, workspace).await
    }

    /// Lists containers visible to one authenticated subject.
    ///
    /// # Errors
    ///
    /// Returns an error when pagination is invalid or persistence fails.
    pub async fn list(
        &self,
        subject: &AuthenticatedSubject,
        request: ListContainersRequest,
    ) -> Result<ContainerList, ContainerError> {
        let limit = request.limit.unwrap_or(DEFAULT_LIST_LIMIT);
        if !(1..=MAX_LIST_LIMIT).contains(&limit) {
            return Err(ContainerError::Invalid(format!(
                "limit must be between 1 and {MAX_LIST_LIMIT}"
            )));
        }
        let order = match request.order {
            ListOrder::Asc => ContainerOrder::Asc,
            ListOrder::Desc => ContainerOrder::Desc,
        };
        let (records, has_more) = self
            .store
            .list(subject, request.after.as_deref(), limit, request.name.as_deref(), order)
            .await
            .map_err(ContainerError::from_storage)?;
        let data = records
            .into_iter()
            .map(container_from_record)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(ContainerList {
            object: "list",
            first_id: data.first().map(|container| container.id.clone()),
            last_id: data.last().map(|container| container.id.clone()),
            data,
            has_more,
        })
    }

    /// Deletes a public container and its agent-rt workspace.
    ///
    /// # Errors
    ///
    /// Returns an error when the container does not exist or either lifecycle operation fails.
    pub async fn delete(
        &self,
        subject: &AuthenticatedSubject,
        id: &str,
        traceparent: Option<&str>,
    ) -> Result<DeletedContainer, ContainerError> {
        self.store
            .get(subject, id)
            .await
            .map_err(ContainerError::from_storage)?;
        let token = self
            .client
            .sign_subject(subject, id)
            .map_err(ContainerError::from_tool)?;
        self.client
            .delete_workspace(id, &token, traceparent)
            .await
            .map_err(|error| ContainerError::from_remote("container", id, error))?;
        self.store
            .mark_deleted(subject, id, unix_millis())
            .await
            .map_err(ContainerError::from_storage)?;
        Ok(DeletedContainer {
            id: id.to_owned(),
            object: "container.deleted",
            deleted: true,
        })
    }

    /// Uploads a public container file through the agent-rt workspace file service.
    ///
    /// # Errors
    ///
    /// Returns an error when the filename is invalid, the container is absent, or file persistence or transport fails.
    pub async fn create_file(
        &self,
        subject: &AuthenticatedSubject,
        container_id: &str,
        filename: &str,
        content: Bytes,
        traceparent: Option<&str>,
    ) -> Result<ContainerFile, ContainerError> {
        self.store
            .get(subject, container_id)
            .await
            .map_err(ContainerError::from_storage)?;
        let filename = sanitize_filename(filename)?;
        let id = format!("cfile_{}", Uuid::now_v7().simple());
        let path = format!("/mnt/data/{id}-{filename}");
        self.file_store
            .claim(ClaimContainerFile {
                subject,
                id: &id,
                container_id,
                path: &path,
                source: "user",
                created_at_millis: unix_millis(),
            })
            .await
            .map_err(ContainerError::from_storage)?;
        let token = self
            .client
            .sign_subject(subject, &id)
            .map_err(ContainerError::from_tool)?;
        let mut final_metadata = None;
        if content.is_empty() {
            final_metadata = Some(
                self.client
                    .write_file(
                        container_id,
                        &path,
                        RemoteFileWrite {
                            offset: 0,
                            data: Vec::new(),
                            truncate: true,
                        },
                        &token,
                        traceparent,
                    )
                    .await
                    .map_err(|error| ContainerError::from_remote("container file", &id, error))?,
            );
        } else {
            for (index, chunk) in content.chunks(FILE_CHUNK_BYTES).enumerate() {
                let offset = index
                    .checked_mul(FILE_CHUNK_BYTES)
                    .and_then(|value| u64::try_from(value).ok())
                    .ok_or_else(|| ContainerError::Invalid("container file is too large".to_owned()))?;
                final_metadata = Some(
                    self.client
                        .write_file(
                            container_id,
                            &path,
                            RemoteFileWrite {
                                offset,
                                data: chunk.to_vec(),
                                truncate: index == 0,
                            },
                            &token,
                            traceparent,
                        )
                        .await
                        .map_err(|error| ContainerError::from_remote("container file", &id, error))?,
                );
            }
        }
        let metadata = final_metadata.ok_or_else(|| {
            ContainerError::Remote("agent-rt did not return metadata for a container file write".to_owned())
        })?;
        let expected_size = u64::try_from(content.len())
            .map_err(|_| ContainerError::Invalid("container file is too large".to_owned()))?;
        validate_file_metadata(
            &id,
            &path,
            &metadata.path,
            metadata.size_bytes,
            metadata.is_directory,
            Some(expected_size),
        )?;
        let record = self
            .file_store
            .finalize(subject, container_id, &id, metadata.size_bytes)
            .await
            .map_err(ContainerError::from_storage)?;
        Ok(container_file_from_record(record))
    }

    /// Retrieves current metadata for a public container file.
    ///
    /// # Errors
    ///
    /// Returns an error when the container or file is absent or agent-rt returns an invalid file binding.
    pub async fn retrieve_file(
        &self,
        subject: &AuthenticatedSubject,
        container_id: &str,
        id: &str,
        traceparent: Option<&str>,
    ) -> Result<ContainerFile, ContainerError> {
        self.store
            .get(subject, container_id)
            .await
            .map_err(ContainerError::from_storage)?;
        let record = self
            .file_store
            .get(subject, container_id, id)
            .await
            .map_err(ContainerError::from_storage)?;
        let token = self
            .client
            .sign_subject(subject, id)
            .map_err(ContainerError::from_tool)?;
        let metadata = self
            .client
            .stat_file(container_id, &record.path, &token, traceparent)
            .await
            .map_err(|error| ContainerError::from_remote("container file", id, error))?;
        validate_file_metadata(
            id,
            &record.path,
            &metadata.path,
            metadata.size_bytes,
            metadata.is_directory,
            None,
        )?;
        let record = self
            .file_store
            .refresh_size(subject, container_id, id, metadata.size_bytes)
            .await
            .map_err(ContainerError::from_storage)?;
        Ok(container_file_from_record(record))
    }

    /// Lists files in a public container catalog.
    ///
    /// # Errors
    ///
    /// Returns an error when the container is absent, pagination is invalid, or persistence fails.
    pub async fn list_files(
        &self,
        subject: &AuthenticatedSubject,
        container_id: &str,
        request: ListContainerFilesRequest,
    ) -> Result<ContainerFileList, ContainerError> {
        self.store
            .get(subject, container_id)
            .await
            .map_err(ContainerError::from_storage)?;
        let limit = request.limit.unwrap_or(DEFAULT_LIST_LIMIT);
        if !(1..=MAX_LIST_LIMIT).contains(&limit) {
            return Err(ContainerError::Invalid(format!(
                "limit must be between 1 and {MAX_LIST_LIMIT}"
            )));
        }
        let (records, has_more) = self
            .file_store
            .list(
                subject,
                container_id,
                request.after.as_deref(),
                limit,
                matches!(request.order, ListOrder::Asc),
            )
            .await
            .map_err(ContainerError::from_storage)?;
        let data = records.into_iter().map(container_file_from_record).collect::<Vec<_>>();
        Ok(ContainerFileList {
            object: "list",
            first_id: data.first().map(|file| file.id.clone()),
            last_id: data.last().map(|file| file.id.clone()),
            data,
            has_more,
        })
    }

    /// Streams raw public container file content from agent-rt.
    ///
    /// # Errors
    ///
    /// Returns an error before streaming when the catalog binding is absent or invalid. Transport failures may be
    /// emitted by the returned stream.
    pub async fn read_file_content(
        &self,
        subject: &AuthenticatedSubject,
        container_id: &str,
        id: &str,
        traceparent: Option<&str>,
    ) -> Result<ContainerFileContent, ContainerError> {
        self.store
            .get(subject, container_id)
            .await
            .map_err(ContainerError::from_storage)?;
        let record = self
            .file_store
            .get(subject, container_id, id)
            .await
            .map_err(ContainerError::from_storage)?;
        let token = self
            .client
            .sign_subject(subject, id)
            .map_err(ContainerError::from_tool)?;
        let client = self.client.clone();
        let container_id = container_id.to_owned();
        let id = id.to_owned();
        let traceparent = traceparent.map(str::to_owned);
        Ok(Box::pin(async_stream::try_stream! {
            let mut offset = 0_u64;
            loop {
                let chunk = client
                    .read_file(
                        &container_id,
                        &record.path,
                        offset,
                        FILE_CHUNK_BYTES_U64,
                        &token,
                        traceparent.as_deref(),
                    )
                    .await
                    .map_err(|error| ContainerError::from_remote("container file", &id, error))?;
                let data_len = u64::try_from(chunk.data.len())
                    .map_err(|_| ContainerError::Remote("agent-rt returned an oversized file chunk".to_owned()))?;
                let expected_offset = offset.checked_add(data_len).ok_or_else(|| {
                    ContainerError::Remote("agent-rt returned an overflowing file offset".to_owned())
                })?;
                if chunk.next_offset != expected_offset || (!chunk.eof && data_len == 0) {
                    Err(ContainerError::Remote(
                        "agent-rt returned a non-monotonic container file chunk".to_owned(),
                    ))?;
                }
                if !chunk.data.is_empty() {
                    yield Bytes::from(chunk.data);
                }
                if chunk.eof {
                    break;
                }
                offset = chunk.next_offset;
            }
        }))
    }

    /// Deletes a public container file from agent-rt and the catalog.
    ///
    /// # Errors
    ///
    /// Returns an error when the container or file is absent or either deletion operation fails.
    pub async fn delete_file(
        &self,
        subject: &AuthenticatedSubject,
        container_id: &str,
        id: &str,
        traceparent: Option<&str>,
    ) -> Result<DeletedContainerFile, ContainerError> {
        self.store
            .get(subject, container_id)
            .await
            .map_err(ContainerError::from_storage)?;
        let record = self
            .file_store
            .get(subject, container_id, id)
            .await
            .map_err(ContainerError::from_storage)?;
        let token = self
            .client
            .sign_subject(subject, id)
            .map_err(ContainerError::from_tool)?;
        self.client
            .remove_file(container_id, &record.path, &token, traceparent)
            .await
            .map_err(|error| ContainerError::from_remote("container file", id, error))?;
        self.file_store
            .mark_deleted(subject, container_id, id, unix_millis())
            .await
            .map_err(ContainerError::from_storage)?;
        Ok(DeletedContainerFile {
            id: id.to_owned(),
            object: "container.file.deleted",
            deleted: true,
        })
    }

    async fn sync_workspace(
        &self,
        subject: &AuthenticatedSubject,
        id: &str,
        workspace: Workspace,
    ) -> Result<Container, ContainerError> {
        if workspace.workspace_id != id || workspace.workspace_class_id != self.client.config().workspace_class_id {
            return Err(ContainerError::Remote(
                "agent-rt returned a workspace outside the requested logical binding".to_owned(),
            ));
        }
        let status = workspace_status(workspace.state)?;
        let record = self
            .store
            .update_workspace(
                subject,
                id,
                workspace.created_at_unix_millis,
                workspace.last_active_at_unix_millis,
                workspace.expires_at_unix_millis,
                status,
            )
            .await
            .map_err(ContainerError::from_storage)?;
        container_from_record(record)
    }
}

impl ContainerError {
    fn from_storage(error: StorageError) -> Self {
        match error {
            StorageError::NotFound { resource_type, id } => Self::NotFound {
                entity: resource_type,
                id,
            },
            other => Self::Storage(other),
        }
    }

    fn from_tool(error: ToolError) -> Self {
        match error {
            ToolError::Config(message) => Self::Config(message),
            other => Self::Remote(other.to_string()),
        }
    }

    fn from_remote(entity: &str, id: &str, error: RemoteTransportError) -> Self {
        match error {
            RemoteTransportError::NotFound => Self::NotFound {
                entity: entity.to_owned(),
                id: id.to_owned(),
            },
            RemoteTransportError::Conflict(message) => Self::Invalid(format!("container identity conflict: {message}")),
            RemoteTransportError::Rejected {
                code: Code::InvalidArgument | Code::FailedPrecondition | Code::Unimplemented,
                message,
            } => Self::Invalid(message),
            other => Self::Remote(other.to_string()),
        }
    }
}

impl From<ContainerError> for ExecutorError {
    fn from(error: ContainerError) -> Self {
        match error {
            ContainerError::Invalid(message) => Self::InvalidRequest(message),
            ContainerError::NotFound { entity, id } => Self::NotFound { entity, id },
            ContainerError::Storage(source) => Self::Storage(source),
            ContainerError::Remote(message) | ContainerError::Config(message) => {
                Self::Tool(ToolError::Execution(message))
            }
        }
    }
}

fn validate_create_request(request: &CreateContainerRequest) -> Result<(), ContainerError> {
    let name = request.name.trim();
    if name.is_empty() || name.len() > MAX_CONTAINER_NAME_BYTES {
        return Err(ContainerError::Invalid(format!(
            "name must contain between 1 and {MAX_CONTAINER_NAME_BYTES} bytes"
        )));
    }
    if request.expires_after.is_some() {
        return Err(ContainerError::Invalid(
            "custom expires_after is not supported; the operator workspace class owns retention".to_owned(),
        ));
    }
    if !request.file_ids.is_empty() {
        return Err(ContainerError::Invalid(
            "file_ids require an OpenAI Files binding that is not configured".to_owned(),
        ));
    }
    if request
        .memory_limit
        .is_some_and(|limit| limit != ShellMemoryLimitParam::OneGiB)
    {
        return Err(ContainerError::Invalid(
            "this deployment exposes only the operator-configured 1g container class".to_owned(),
        ));
    }
    if request.network_policy.is_some() {
        return Err(ContainerError::Invalid(
            "network_policy requires an operator policy binding that is not configured".to_owned(),
        ));
    }
    if !request.skills.is_empty() {
        return Err(ContainerError::Invalid(
            "skills require an operator skill binding that is not configured".to_owned(),
        ));
    }
    Ok(())
}

fn workspace_status(state: i32) -> Result<&'static str, ContainerError> {
    match WorkspaceState::try_from(state) {
        Ok(WorkspaceState::Creating) => Ok("creating"),
        Ok(WorkspaceState::Ready) => Ok("running"),
        Ok(WorkspaceState::Suspended) => Ok("suspended"),
        Ok(WorkspaceState::Deleting) => Ok("deleting"),
        Ok(WorkspaceState::Deleted) => Ok("deleted"),
        Ok(WorkspaceState::Failed) => Ok("failed"),
        Ok(WorkspaceState::Unspecified) | Err(_) => Err(ContainerError::Remote(
            "agent-rt returned an unknown workspace state".to_owned(),
        )),
    }
}

fn container_from_record(record: ContainerRecord) -> Result<Container, ContainerError> {
    let memory_limit = match record.memory_limit.as_str() {
        "1g" => ShellMemoryLimitParam::OneGiB,
        "4g" => ShellMemoryLimitParam::FourGiB,
        "16g" => ShellMemoryLimitParam::SixteenGiB,
        "64g" => ShellMemoryLimitParam::SixtyFourGiB,
        value => {
            return Err(ContainerError::Config(format!(
                "unknown persisted memory limit '{value}'"
            )));
        }
    };
    Ok(Container {
        id: record.id,
        object: "container".to_owned(),
        created_at: record.created_at_millis / 1_000,
        status: record.status,
        expires_after: record.expires_after_minutes.map(|minutes| ContainerExpiration {
            anchor: ContainerExpirationAnchor::LastActiveAt,
            minutes,
        }),
        last_active_at: Some(record.last_active_at_millis / 1_000),
        memory_limit: Some(memory_limit),
        network_policy: None,
        name: record.name,
    })
}

const fn memory_limit_str(limit: ShellMemoryLimitParam) -> &'static str {
    match limit {
        ShellMemoryLimitParam::OneGiB => "1g",
        ShellMemoryLimitParam::FourGiB => "4g",
        ShellMemoryLimitParam::SixteenGiB => "16g",
        ShellMemoryLimitParam::SixtyFourGiB => "64g",
    }
}

fn container_file_from_record(record: ContainerFileRecord) -> ContainerFile {
    ContainerFile {
        id: record.id,
        object: "container.file".to_owned(),
        created_at: record.created_at_millis / 1_000,
        bytes: record.size_bytes,
        container_id: record.container_id,
        path: record.path,
        source: record.source,
    }
}

fn sanitize_filename(filename: &str) -> Result<String, ContainerError> {
    let basename = filename.rsplit(['/', '\\']).next().unwrap_or_default().trim();
    if basename.is_empty() {
        return Err(ContainerError::Invalid(
            "container file must have a filename".to_owned(),
        ));
    }
    let sanitized = basename
        .chars()
        .take(128)
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '.' | '-' | '_') {
                character
            } else {
                '_'
            }
        })
        .collect::<String>();
    if matches!(sanitized.as_str(), "." | "..") {
        return Err(ContainerError::Invalid(
            "container file must have a valid filename".to_owned(),
        ));
    }
    Ok(sanitized)
}

fn validate_file_metadata(
    id: &str,
    expected_path: &str,
    actual_path: &str,
    actual_size: u64,
    is_directory: bool,
    expected_size: Option<u64>,
) -> Result<(), ContainerError> {
    if actual_path != expected_path || is_directory || expected_size.is_some_and(|size| size != actual_size) {
        return Err(ContainerError::Remote(format!(
            "agent-rt returned metadata outside container file binding '{id}'"
        )));
    }
    Ok(())
}

fn unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| u64::try_from(duration.as_millis()).ok())
        .unwrap_or_default()
}
