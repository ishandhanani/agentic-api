use std::sync::Arc;

use serde::{Deserialize, Serialize};

use super::DbPool;
use crate::tool::AuthenticatedSubject;
use crate::utils::common::utcnow_str;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteExecutionLink {
    pub tenant_id: String,
    pub principal_id: String,
    pub response_id: String,
    pub conversation_id: Option<String>,
    pub call_id: String,
    pub execution_id: String,
    pub workspace_id: String,
    pub route_id: String,
    pub request_fingerprint: String,
    pub absolute_deadline: i64,
    pub state: String,
    pub terminal_outcome: Option<String>,
}

pub struct ClaimRemoteExecution<'a> {
    pub subject: &'a AuthenticatedSubject,
    pub response_id: &'a str,
    pub conversation_id: Option<&'a str>,
    pub call_id: &'a str,
    pub route_id: &'a str,
    pub request_fingerprint: &'a str,
    pub absolute_deadline: i64,
}

#[derive(Debug, thiserror::Error)]
pub enum RemoteExecutionLedgerError {
    #[error("remote execution ledger database failed: {0}")]
    Database(#[from] sqlx::Error),
    #[error("logical tool call is already bound to a different remote request")]
    Conflict,
    #[error("remote execution link does not exist")]
    NotFound,
}

#[derive(Clone)]
pub struct RemoteExecutionLedger {
    pool: Arc<DbPool>,
}

impl std::fmt::Debug for RemoteExecutionLedger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteExecutionLedger").finish_non_exhaustive()
    }
}

impl RemoteExecutionLedger {
    #[must_use]
    pub fn new(pool: Arc<DbPool>) -> Self {
        Self { pool }
    }

    /// Claims or loads the durable execution identity for one logical tool call.
    ///
    /// # Errors
    ///
    /// Returns a storage error when the claim cannot be committed, or a
    /// conflict when the logical call was previously bound to different work.
    pub async fn claim(
        &self,
        claim: ClaimRemoteExecution<'_>,
    ) -> Result<RemoteExecutionLink, RemoteExecutionLedgerError> {
        let (execution_id, workspace_id) = stable_execution_identity(
            Some(claim.subject),
            claim.response_id,
            claim.conversation_id,
            claim.call_id,
        );
        let now = utcnow_str();
        let mut transaction = self.pool.begin().await?;
        sqlx::query(
            "INSERT INTO remote_executions (
                 tenant_id, principal_id, response_id, conversation_id, call_id,
                 execution_id, workspace_id, route_id, request_fingerprint,
                 absolute_deadline, state, created_at, updated_at
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 'claimed', $11, $11)
             ON CONFLICT (tenant_id, principal_id, response_id, call_id) DO NOTHING",
        )
        .bind(&claim.subject.tenant_id)
        .bind(&claim.subject.principal_id)
        .bind(claim.response_id)
        .bind(claim.conversation_id)
        .bind(claim.call_id)
        .bind(&execution_id)
        .bind(&workspace_id)
        .bind(claim.route_id)
        .bind(claim.request_fingerprint)
        .bind(claim.absolute_deadline)
        .bind(now)
        .execute(&mut *transaction)
        .await?;
        let link = load_in_transaction(&mut transaction, claim.subject, claim.response_id, claim.call_id)
            .await?
            .ok_or(RemoteExecutionLedgerError::NotFound)?;
        if link.route_id != claim.route_id
            || link.request_fingerprint != claim.request_fingerprint
            || link.conversation_id.as_deref() != claim.conversation_id
        {
            return Err(RemoteExecutionLedgerError::Conflict);
        }
        transaction.commit().await?;
        Ok(link)
    }

    /// Persists the authoritative remote outcome before public reinjection.
    ///
    /// # Errors
    ///
    /// Returns a storage error when the update fails or the scoped execution
    /// link no longer exists.
    pub async fn record_outcome(
        &self,
        link: &RemoteExecutionLink,
        state: &str,
        outcome: &str,
    ) -> Result<(), RemoteExecutionLedgerError> {
        let updated = sqlx::query(
            "UPDATE remote_executions
             SET state = $1, terminal_outcome = $2, updated_at = $3
             WHERE tenant_id = $4 AND principal_id = $5 AND execution_id = $6",
        )
        .bind(state)
        .bind(outcome)
        .bind(utcnow_str())
        .bind(&link.tenant_id)
        .bind(&link.principal_id)
        .bind(&link.execution_id)
        .execute(self.pool.as_ref())
        .await?;
        if updated.rows_affected() != 1 {
            return Err(RemoteExecutionLedgerError::NotFound);
        }
        Ok(())
    }
}

async fn load_in_transaction(
    transaction: &mut sqlx::Transaction<'_, sqlx::Any>,
    subject: &AuthenticatedSubject,
    response_id: &str,
    call_id: &str,
) -> Result<Option<RemoteExecutionLink>, sqlx::Error> {
    let row = sqlx::query_as::<_, RemoteExecutionRow>(
        "SELECT tenant_id, principal_id, response_id, conversation_id, call_id,
                execution_id, workspace_id, route_id, request_fingerprint,
                absolute_deadline, state, terminal_outcome
         FROM remote_executions
         WHERE tenant_id = $1 AND principal_id = $2 AND response_id = $3 AND call_id = $4",
    )
    .bind(&subject.tenant_id)
    .bind(&subject.principal_id)
    .bind(response_id)
    .bind(call_id)
    .fetch_optional(&mut **transaction)
    .await?;
    Ok(row.map(Into::into))
}

#[derive(sqlx::FromRow)]
struct RemoteExecutionRow {
    tenant_id: String,
    principal_id: String,
    response_id: String,
    conversation_id: Option<String>,
    call_id: String,
    execution_id: String,
    workspace_id: String,
    route_id: String,
    request_fingerprint: String,
    absolute_deadline: i64,
    state: String,
    terminal_outcome: Option<String>,
}

impl From<RemoteExecutionRow> for RemoteExecutionLink {
    fn from(row: RemoteExecutionRow) -> Self {
        Self {
            tenant_id: row.tenant_id,
            principal_id: row.principal_id,
            response_id: row.response_id,
            conversation_id: row.conversation_id,
            call_id: row.call_id,
            execution_id: row.execution_id,
            workspace_id: row.workspace_id,
            route_id: row.route_id,
            request_fingerprint: row.request_fingerprint,
            absolute_deadline: row.absolute_deadline,
            state: row.state,
            terminal_outcome: row.terminal_outcome,
        }
    }
}

pub(crate) fn stable_execution_identity(
    subject: Option<&AuthenticatedSubject>,
    response_id: &str,
    conversation_id: Option<&str>,
    call_id: &str,
) -> (String, String) {
    let tenant_id = subject.map_or("", |subject| subject.tenant_id.as_str());
    let principal_id = subject.map_or("", |subject| subject.principal_id.as_str());
    let execution_id = stable_id("exec_", &[tenant_id, principal_id, response_id, call_id]);
    let workspace_basis = conversation_id.unwrap_or(response_id);
    let workspace_id = stable_id("ws_", &[tenant_id, principal_id, workspace_basis]);
    (execution_id, workspace_id)
}

fn stable_id(prefix: &str, components: &[&str]) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"agentic-api-remote-execution-v1");
    for component in components {
        hasher.update(&(component.len() as u64).to_le_bytes());
        hasher.update(component.as_bytes());
    }
    format!("{prefix}{}", hasher.finalize().to_hex())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stable_identity_is_length_delimited() {
        assert_ne!(stable_id("exec_", &["a", "bc"]), stable_id("exec_", &["ab", "c"]));
    }

    #[tokio::test]
    async fn logical_call_claim_is_durable_idempotent_and_conflict_checked() {
        let pool = crate::storage::create_pool_with_schema(Some("sqlite::memory:"))
            .await
            .expect("create ledger database");
        let ledger = RemoteExecutionLedger::new(pool);
        let subject = AuthenticatedSubject {
            tenant_id: "tenant-a".to_owned(),
            principal_id: "principal-a".to_owned(),
        };
        let claim = || ClaimRemoteExecution {
            subject: &subject,
            response_id: "resp-a",
            conversation_id: Some("conv-a"),
            call_id: "call-a",
            route_id: "sandbox.python.default",
            request_fingerprint: "blake3:request-a",
            absolute_deadline: 2_000_000_000,
        };

        let first = ledger.claim(claim()).await.expect("first claim");
        let second = ledger.claim(claim()).await.expect("idempotent claim");
        assert_eq!(first.execution_id, second.execution_id);
        assert_eq!(first.workspace_id, second.workspace_id);
        assert_eq!(second.absolute_deadline, 2_000_000_000);

        let conflict = ledger
            .claim(ClaimRemoteExecution {
                request_fingerprint: "blake3:different",
                ..claim()
            })
            .await
            .expect_err("different request must conflict");
        assert!(matches!(conflict, RemoteExecutionLedgerError::Conflict));

        ledger
            .record_outcome(&first, "completed", r#"{"stdout":"42"}"#)
            .await
            .expect("persist outcome");
        let persisted = ledger.claim(claim()).await.expect("load persisted claim");
        assert_eq!(persisted.state, "completed");
        assert_eq!(persisted.terminal_outcome.as_deref(), Some(r#"{"stdout":"42"}"#));
    }
}
