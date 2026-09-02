use std::sync::Arc;

use super::{DbPool, StorageError};
use crate::tool::AuthenticatedSubject;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ContainerRecord {
    pub(crate) id: String,
    pub(crate) name: String,
    pub(crate) profile_id: String,
    pub(crate) memory_limit: String,
    pub(crate) status: String,
    pub(crate) expires_after_minutes: Option<u64>,
    pub(crate) created_at_millis: u64,
    pub(crate) last_active_at_millis: u64,
    pub(crate) expires_at_millis: Option<u64>,
}

pub(crate) struct ClaimContainer<'a> {
    pub(crate) subject: &'a AuthenticatedSubject,
    pub(crate) id: &'a str,
    pub(crate) name: &'a str,
    pub(crate) profile_id: &'a str,
    pub(crate) memory_limit: &'a str,
    pub(crate) created_at_millis: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ContainerOrder {
    Asc,
    Desc,
}

#[derive(Clone)]
pub(crate) struct ContainerStore {
    pool: Arc<DbPool>,
}

impl std::fmt::Debug for ContainerStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("ContainerStore").finish_non_exhaustive()
    }
}

impl ContainerStore {
    #[must_use]
    pub(crate) fn new(pool: Arc<DbPool>) -> Self {
        Self { pool }
    }

    pub(crate) async fn claim(&self, claim: ClaimContainer<'_>) -> Result<ContainerRecord, StorageError> {
        let created_at = millis_to_i64(claim.created_at_millis)?;
        sqlx::query(
            "INSERT INTO containers (
                 tenant_id, principal_id, id, name, profile_id, memory_limit, status,
                 created_at, last_active_at
             ) VALUES ($1, $2, $3, $4, $5, $6, 'creating', $7, $7)",
        )
        .bind(&claim.subject.tenant_id)
        .bind(&claim.subject.principal_id)
        .bind(claim.id)
        .bind(claim.name)
        .bind(claim.profile_id)
        .bind(claim.memory_limit)
        .bind(created_at)
        .execute(self.pool.as_ref())
        .await?;
        self.get(claim.subject, claim.id).await
    }

    pub(crate) async fn update_workspace(
        &self,
        subject: &AuthenticatedSubject,
        id: &str,
        created_at_millis: u64,
        last_active_at_millis: u64,
        expires_at_millis: Option<u64>,
        status: &str,
    ) -> Result<ContainerRecord, StorageError> {
        let expires_after_minutes = expires_at_millis
            .map(|expires_at| expires_at.saturating_sub(last_active_at_millis).saturating_add(59_999) / 60_000);
        let updated = sqlx::query(
            "UPDATE containers
             SET created_at = $1, last_active_at = $2, expires_at = $3, expires_after_minutes = $4, status = $5
             WHERE tenant_id = $6 AND principal_id = $7 AND id = $8 AND deleted_at IS NULL",
        )
        .bind(millis_to_i64(created_at_millis)?)
        .bind(millis_to_i64(last_active_at_millis)?)
        .bind(optional_millis_to_i64(expires_at_millis)?)
        .bind(expires_after_minutes.map(millis_to_i64).transpose()?)
        .bind(status)
        .bind(&subject.tenant_id)
        .bind(&subject.principal_id)
        .bind(id)
        .execute(self.pool.as_ref())
        .await?;
        if updated.rows_affected() != 1 {
            return Err(StorageError::not_found("container", id));
        }
        self.get(subject, id).await
    }

    pub(crate) async fn get(&self, subject: &AuthenticatedSubject, id: &str) -> Result<ContainerRecord, StorageError> {
        sqlx::query_as::<_, ContainerRow>(
            "SELECT id, name, profile_id, memory_limit, status, expires_after_minutes,
                    created_at, last_active_at, expires_at
             FROM containers
             WHERE tenant_id = $1 AND principal_id = $2 AND id = $3 AND deleted_at IS NULL",
        )
        .bind(&subject.tenant_id)
        .bind(&subject.principal_id)
        .bind(id)
        .fetch_optional(self.pool.as_ref())
        .await?
        .map(ContainerRecord::try_from)
        .transpose()?
        .ok_or_else(|| StorageError::not_found("container", id))
    }

    pub(crate) async fn list(
        &self,
        subject: &AuthenticatedSubject,
        after: Option<&str>,
        limit: u32,
        name: Option<&str>,
        order: ContainerOrder,
    ) -> Result<(Vec<ContainerRecord>, bool), StorageError> {
        let cursor = match after {
            Some(id) => Some(self.get(subject, id).await?),
            None => None,
        };
        let cursor_created_at = cursor
            .as_ref()
            .map(|record| record.created_at_millis)
            .unwrap_or_default();
        let cursor_id = cursor.as_ref().map_or("", |record| record.id.as_str());
        let fetch_limit = i64::from(limit.saturating_add(1));
        let rows = match (order, name) {
            (ContainerOrder::Asc, Some(name)) => {
                sqlx::query_as::<_, ContainerRow>(LIST_ASC_WITH_NAME)
                    .bind(&subject.tenant_id)
                    .bind(&subject.principal_id)
                    .bind(cursor.is_none())
                    .bind(millis_to_i64(cursor_created_at)?)
                    .bind(cursor_id)
                    .bind(name)
                    .bind(fetch_limit)
                    .fetch_all(self.pool.as_ref())
                    .await?
            }
            (ContainerOrder::Asc, None) => {
                sqlx::query_as::<_, ContainerRow>(LIST_ASC)
                    .bind(&subject.tenant_id)
                    .bind(&subject.principal_id)
                    .bind(cursor.is_none())
                    .bind(millis_to_i64(cursor_created_at)?)
                    .bind(cursor_id)
                    .bind(fetch_limit)
                    .fetch_all(self.pool.as_ref())
                    .await?
            }
            (ContainerOrder::Desc, Some(name)) => {
                sqlx::query_as::<_, ContainerRow>(LIST_DESC_WITH_NAME)
                    .bind(&subject.tenant_id)
                    .bind(&subject.principal_id)
                    .bind(cursor.is_none())
                    .bind(millis_to_i64(cursor_created_at)?)
                    .bind(cursor_id)
                    .bind(name)
                    .bind(fetch_limit)
                    .fetch_all(self.pool.as_ref())
                    .await?
            }
            (ContainerOrder::Desc, None) => {
                sqlx::query_as::<_, ContainerRow>(LIST_DESC)
                    .bind(&subject.tenant_id)
                    .bind(&subject.principal_id)
                    .bind(cursor.is_none())
                    .bind(millis_to_i64(cursor_created_at)?)
                    .bind(cursor_id)
                    .bind(fetch_limit)
                    .fetch_all(self.pool.as_ref())
                    .await?
            }
        };
        let has_more = rows.len() > limit as usize;
        let records = rows
            .into_iter()
            .take(limit as usize)
            .map(ContainerRecord::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        Ok((records, has_more))
    }

    pub(crate) async fn mark_deleted(
        &self,
        subject: &AuthenticatedSubject,
        id: &str,
        deleted_at_millis: u64,
    ) -> Result<(), StorageError> {
        let updated = sqlx::query(
            "UPDATE containers SET deleted_at = $1
             WHERE tenant_id = $2 AND principal_id = $3 AND id = $4 AND deleted_at IS NULL",
        )
        .bind(millis_to_i64(deleted_at_millis)?)
        .bind(&subject.tenant_id)
        .bind(&subject.principal_id)
        .bind(id)
        .execute(self.pool.as_ref())
        .await?;
        if updated.rows_affected() != 1 {
            return Err(StorageError::not_found("container", id));
        }
        Ok(())
    }
}

const LIST_ASC: &str = "SELECT id, name, profile_id, memory_limit, status, expires_after_minutes, created_at, last_active_at, expires_at FROM containers WHERE tenant_id = $1 AND principal_id = $2 AND deleted_at IS NULL AND ($3 OR created_at > $4 OR (created_at = $4 AND id > $5)) ORDER BY created_at ASC, id ASC LIMIT $6";
const LIST_DESC: &str = "SELECT id, name, profile_id, memory_limit, status, expires_after_minutes, created_at, last_active_at, expires_at FROM containers WHERE tenant_id = $1 AND principal_id = $2 AND deleted_at IS NULL AND ($3 OR created_at < $4 OR (created_at = $4 AND id < $5)) ORDER BY created_at DESC, id DESC LIMIT $6";
const LIST_ASC_WITH_NAME: &str = "SELECT id, name, profile_id, memory_limit, status, expires_after_minutes, created_at, last_active_at, expires_at FROM containers WHERE tenant_id = $1 AND principal_id = $2 AND deleted_at IS NULL AND ($3 OR created_at > $4 OR (created_at = $4 AND id > $5)) AND name = $6 ORDER BY created_at ASC, id ASC LIMIT $7";
const LIST_DESC_WITH_NAME: &str = "SELECT id, name, profile_id, memory_limit, status, expires_after_minutes, created_at, last_active_at, expires_at FROM containers WHERE tenant_id = $1 AND principal_id = $2 AND deleted_at IS NULL AND ($3 OR created_at < $4 OR (created_at = $4 AND id < $5)) AND name = $6 ORDER BY created_at DESC, id DESC LIMIT $7";

#[derive(sqlx::FromRow)]
struct ContainerRow {
    id: String,
    name: String,
    profile_id: String,
    memory_limit: String,
    status: String,
    expires_after_minutes: Option<i64>,
    created_at: i64,
    last_active_at: i64,
    expires_at: Option<i64>,
}

impl TryFrom<ContainerRow> for ContainerRecord {
    type Error = StorageError;

    fn try_from(row: ContainerRow) -> Result<Self, Self::Error> {
        Ok(Self {
            id: row.id,
            name: row.name,
            profile_id: row.profile_id,
            memory_limit: row.memory_limit,
            status: row.status,
            expires_after_minutes: row.expires_after_minutes.map(i64_to_u64).transpose()?,
            created_at_millis: i64_to_u64(row.created_at)?,
            last_active_at_millis: i64_to_u64(row.last_active_at)?,
            expires_at_millis: row.expires_at.map(i64_to_u64).transpose()?,
        })
    }
}

fn millis_to_i64(value: u64) -> Result<i64, StorageError> {
    i64::try_from(value).map_err(|_| sqlx::Error::Protocol("container timestamp exceeds BIGINT".to_owned()).into())
}

fn optional_millis_to_i64(value: Option<u64>) -> Result<Option<i64>, StorageError> {
    value.map(millis_to_i64).transpose()
}

fn i64_to_u64(value: i64) -> Result<u64, StorageError> {
    u64::try_from(value).map_err(|_| sqlx::Error::Protocol("container timestamp is negative".to_owned()).into())
}
