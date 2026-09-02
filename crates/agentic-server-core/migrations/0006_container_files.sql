CREATE TABLE IF NOT EXISTS container_files (
    tenant_id    TEXT NOT NULL,
    principal_id TEXT NOT NULL,
    id           TEXT NOT NULL,
    container_id TEXT NOT NULL,
    path         TEXT NOT NULL,
    source       TEXT NOT NULL,
    status       TEXT NOT NULL,
    size_bytes   BIGINT NOT NULL,
    created_at   BIGINT NOT NULL,
    deleted_at   BIGINT,
    PRIMARY KEY (tenant_id, principal_id, container_id, id),
    UNIQUE (tenant_id, principal_id, container_id, path)
);

CREATE INDEX IF NOT EXISTS idx_container_files_subject_created
    ON container_files (tenant_id, principal_id, container_id, created_at, id);
