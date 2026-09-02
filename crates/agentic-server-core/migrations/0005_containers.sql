CREATE TABLE IF NOT EXISTS containers (
    tenant_id             TEXT NOT NULL,
    principal_id          TEXT NOT NULL,
    id                    TEXT NOT NULL,
    name                  TEXT NOT NULL,
    workspace_class_id    TEXT NOT NULL,
    memory_limit          TEXT NOT NULL,
    status                TEXT NOT NULL,
    expires_after_minutes BIGINT,
    created_at            BIGINT NOT NULL,
    last_active_at        BIGINT NOT NULL,
    expires_at            BIGINT,
    deleted_at            BIGINT,
    PRIMARY KEY (tenant_id, principal_id, id)
);

CREATE INDEX IF NOT EXISTS idx_containers_subject_created
    ON containers (tenant_id, principal_id, created_at, id);
