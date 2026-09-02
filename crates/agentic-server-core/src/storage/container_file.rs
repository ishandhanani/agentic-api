use std::sync::Arc;

use super::{DbPool, StorageError};
use crate::tool::AuthenticatedSubject;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ContainerFileRecord {
    pub(crate) id: String,
    pub(crate) container_id: String,
    pub(crate) path: String,
    pub(crate) source: String,
    pub(crate) size_bytes: u64,
    pub(crate) created_at_millis: u64,
}

pub(crate) struct ClaimContainerFile<'a> {
    pub(crate) subject: &'a AuthenticatedSubject,
    pub(crate) id: &'a str,
    pub(crate) container_id: &'a str,
    pub(crate) path: &'a str,
    pub(crate) source: &'a str,
    pub(crate) created_at_millis: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct ContainerFileStore {
    pool: Arc<DbPool>,
}

impl ContainerFileStore {
    #[must_use]
    pub(crate) fn new(pool: Arc<DbPool>) -> Self {
        Self { pool }
    }

    pub(crate) async fn claim(&self, claim: ClaimContainerFile<'_>) -> Result<(), StorageError> {
        sqlx::query(
            "INSERT INTO container_files (
                 tenant_id, principal_id, id, container_id, path, source, status, size_bytes, created_at
             ) VALUES ($1, $2, $3, $4, $5, $6, 'creating', 0, $7)",
        )
        .bind(&claim.subject.tenant_id)
        .bind(&claim.subject.principal_id)
        .bind(claim.id)
        .bind(claim.container_id)
        .bind(claim.path)
        .bind(claim.source)
        .bind(u64_to_i64(claim.created_at_millis)?)
        .execute(self.pool.as_ref())
        .await?;
        Ok(())
    }

    pub(crate) async fn finalize(
        &self,
        subject: &AuthenticatedSubject,
        container_id: &str,
        id: &str,
        size_bytes: u64,
    ) -> Result<ContainerFileRecord, StorageError> {
        let updated = sqlx::query(
            "UPDATE container_files SET status = 'ready', size_bytes = $1
             WHERE tenant_id = $2 AND principal_id = $3 AND container_id = $4 AND id = $5 AND deleted_at IS NULL",
        )
        .bind(u64_to_i64(size_bytes)?)
        .bind(&subject.tenant_id)
        .bind(&subject.principal_id)
        .bind(container_id)
        .bind(id)
        .execute(self.pool.as_ref())
        .await?;
        if updated.rows_affected() != 1 {
            return Err(StorageError::not_found("container file", id));
        }
        self.get(subject, container_id, id).await
    }

    pub(crate) async fn refresh_size(
        &self,
        subject: &AuthenticatedSubject,
        container_id: &str,
        id: &str,
        size_bytes: u64,
    ) -> Result<ContainerFileRecord, StorageError> {
        let updated = sqlx::query(
            "UPDATE container_files SET size_bytes = $1
             WHERE tenant_id = $2 AND principal_id = $3 AND container_id = $4 AND id = $5
               AND status = 'ready' AND deleted_at IS NULL",
        )
        .bind(u64_to_i64(size_bytes)?)
        .bind(&subject.tenant_id)
        .bind(&subject.principal_id)
        .bind(container_id)
        .bind(id)
        .execute(self.pool.as_ref())
        .await?;
        if updated.rows_affected() != 1 {
            return Err(StorageError::not_found("container file", id));
        }
        self.get(subject, container_id, id).await
    }

    pub(crate) async fn get(
        &self,
        subject: &AuthenticatedSubject,
        container_id: &str,
        id: &str,
    ) -> Result<ContainerFileRecord, StorageError> {
        sqlx::query_as::<_, ContainerFileRow>(
            "SELECT id, container_id, path, source, size_bytes, created_at
             FROM container_files
             WHERE tenant_id = $1 AND principal_id = $2 AND container_id = $3 AND id = $4
               AND status = 'ready' AND deleted_at IS NULL",
        )
        .bind(&subject.tenant_id)
        .bind(&subject.principal_id)
        .bind(container_id)
        .bind(id)
        .fetch_optional(self.pool.as_ref())
        .await?
        .map(ContainerFileRecord::try_from)
        .transpose()?
        .ok_or_else(|| StorageError::not_found("container file", id))
    }

    pub(crate) async fn list(
        &self,
        subject: &AuthenticatedSubject,
        container_id: &str,
        after: Option<&str>,
        limit: u32,
        ascending: bool,
    ) -> Result<(Vec<ContainerFileRecord>, bool), StorageError> {
        let cursor = match after {
            Some(id) => Some(self.get(subject, container_id, id).await?),
            None => None,
        };
        let created_at = cursor.as_ref().map_or(0, |record| record.created_at_millis);
        let cursor_id = cursor.as_ref().map_or("", |record| record.id.as_str());
        let query = if ascending { LIST_ASC } else { LIST_DESC };
        let rows = sqlx::query_as::<_, ContainerFileRow>(query)
            .bind(&subject.tenant_id)
            .bind(&subject.principal_id)
            .bind(container_id)
            .bind(cursor.is_none())
            .bind(u64_to_i64(created_at)?)
            .bind(cursor_id)
            .bind(i64::from(limit.saturating_add(1)))
            .fetch_all(self.pool.as_ref())
            .await?;
        let has_more = rows.len() > limit as usize;
        let records = rows
            .into_iter()
            .take(limit as usize)
            .map(ContainerFileRecord::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        Ok((records, has_more))
    }

    pub(crate) async fn mark_deleted(
        &self,
        subject: &AuthenticatedSubject,
        container_id: &str,
        id: &str,
        deleted_at_millis: u64,
    ) -> Result<(), StorageError> {
        let updated = sqlx::query(
            "UPDATE container_files SET deleted_at = $1, status = 'deleted'
             WHERE tenant_id = $2 AND principal_id = $3 AND container_id = $4 AND id = $5
               AND status = 'ready' AND deleted_at IS NULL",
        )
        .bind(u64_to_i64(deleted_at_millis)?)
        .bind(&subject.tenant_id)
        .bind(&subject.principal_id)
        .bind(container_id)
        .bind(id)
        .execute(self.pool.as_ref())
        .await?;
        if updated.rows_affected() != 1 {
            return Err(StorageError::not_found("container file", id));
        }
        Ok(())
    }
}

const LIST_ASC: &str = "SELECT id, container_id, path, source, size_bytes, created_at FROM container_files WHERE tenant_id = $1 AND principal_id = $2 AND container_id = $3 AND status = 'ready' AND deleted_at IS NULL AND ($4 OR created_at > $5 OR (created_at = $5 AND id > $6)) ORDER BY created_at ASC, id ASC LIMIT $7";
const LIST_DESC: &str = "SELECT id, container_id, path, source, size_bytes, created_at FROM container_files WHERE tenant_id = $1 AND principal_id = $2 AND container_id = $3 AND status = 'ready' AND deleted_at IS NULL AND ($4 OR created_at < $5 OR (created_at = $5 AND id < $6)) ORDER BY created_at DESC, id DESC LIMIT $7";

#[derive(sqlx::FromRow)]
struct ContainerFileRow {
    id: String,
    container_id: String,
    path: String,
    source: String,
    size_bytes: i64,
    created_at: i64,
}

impl TryFrom<ContainerFileRow> for ContainerFileRecord {
    type Error = StorageError;

    fn try_from(row: ContainerFileRow) -> Result<Self, Self::Error> {
        Ok(Self {
            id: row.id,
            container_id: row.container_id,
            path: row.path,
            source: row.source,
            size_bytes: i64_to_u64(row.size_bytes)?,
            created_at_millis: i64_to_u64(row.created_at)?,
        })
    }
}

fn u64_to_i64(value: u64) -> Result<i64, StorageError> {
    i64::try_from(value).map_err(|_| sqlx::Error::Protocol("container file value exceeds BIGINT".to_owned()).into())
}

fn i64_to_u64(value: i64) -> Result<u64, StorageError> {
    u64::try_from(value).map_err(|_| sqlx::Error::Protocol("container file value is negative".to_owned()).into())
}
