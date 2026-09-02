use serde::{Deserialize, Serialize};

use super::{ShellMemoryLimitParam, ShellNetworkPolicyParam, ShellSkillParam};

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateContainerRequest {
    pub name: String,
    #[serde(default)]
    pub expires_after: Option<ContainerExpiration>,
    #[serde(default)]
    pub file_ids: Vec<String>,
    #[serde(default)]
    pub memory_limit: Option<ShellMemoryLimitParam>,
    #[serde(default)]
    pub network_policy: Option<ShellNetworkPolicyParam>,
    #[serde(default)]
    pub skills: Vec<ShellSkillParam>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContainerExpiration {
    pub anchor: ContainerExpirationAnchor,
    pub minutes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContainerExpirationAnchor {
    LastActiveAt,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ContainerNetworkPolicy {
    Disabled,
    Allowlist { allowed_domains: Vec<String> },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Container {
    pub id: String,
    pub object: String,
    pub created_at: u64,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_after: Option<ContainerExpiration>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_active_at: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memory_limit: Option<ShellMemoryLimitParam>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub network_policy: Option<ContainerNetworkPolicy>,
    pub name: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ContainerList {
    pub object: &'static str,
    pub data: Vec<Container>,
    pub first_id: Option<String>,
    pub last_id: Option<String>,
    pub has_more: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct DeletedContainer {
    pub id: String,
    pub object: &'static str,
    pub deleted: bool,
}

#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ListOrder {
    Asc,
    #[default]
    Desc,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ListContainersRequest {
    pub after: Option<String>,
    pub limit: Option<u32>,
    pub name: Option<String>,
    #[serde(default)]
    pub order: ListOrder,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateContainerFileRequest {
    pub file_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContainerFile {
    pub id: String,
    pub object: String,
    pub created_at: u64,
    pub bytes: u64,
    pub container_id: String,
    pub path: String,
    pub source: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ContainerFileList {
    pub object: &'static str,
    pub data: Vec<ContainerFile>,
    pub first_id: Option<String>,
    pub last_id: Option<String>,
    pub has_more: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct DeletedContainerFile {
    pub id: String,
    pub object: &'static str,
    pub deleted: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ListContainerFilesRequest {
    pub after: Option<String>,
    pub limit: Option<u32>,
    #[serde(default)]
    pub order: ListOrder,
}
