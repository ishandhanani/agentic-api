CREATE TABLE IF NOT EXISTS remote_executions (
    tenant_id           TEXT NOT NULL,
    principal_id        TEXT NOT NULL,
    response_id         TEXT NOT NULL,
    conversation_id     TEXT,
    call_id             TEXT NOT NULL,
    execution_id        TEXT NOT NULL,
    workspace_id        TEXT NOT NULL,
    request_fingerprint TEXT NOT NULL,
    absolute_deadline   BIGINT NOT NULL,
    state               TEXT NOT NULL,
    terminal_outcome    TEXT,
    created_at          BIGINT NOT NULL,
    updated_at          BIGINT NOT NULL,
    PRIMARY KEY (tenant_id, principal_id, response_id, call_id),
    UNIQUE (tenant_id, principal_id, execution_id)
);

CREATE INDEX IF NOT EXISTS idx_remote_executions_conversation
    ON remote_executions (tenant_id, principal_id, conversation_id);
