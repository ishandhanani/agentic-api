//! Storage layer for persistence operations.

// Database URL classification and sanitization
pub mod backend;

// Public container metadata and pagination; physical lifecycle remains in shed.
pub(crate) mod container;
pub(crate) mod container_file;

// Strong types for storage operations (newtype pattern)
pub mod types;

// Database connection pooling and initialization
pub mod pool;

// Database schema management and migrations
pub mod schema;

// Database schema models (sqlx FromRow types)
pub mod models;

// Response storage operations
pub mod response;

// Durable logical-call to remote-execution mapping.
pub mod remote_execution;

// Conversation storage operations
pub mod conversation;

// Re-export commonly used types for convenience
pub use backend::DatabaseBackend;
pub(crate) use container::{ClaimContainer, ContainerOrder, ContainerRecord, ContainerStore};
pub(crate) use container_file::{ClaimContainerFile, ContainerFileRecord, ContainerFileStore};
pub use conversation::ConversationStore;
pub use models::Conversation as DbConversation;
pub use models::Item;
pub use models::Response as DbResponse;
pub use pool::{
    DbPool, DbResult, DbTransaction, create_pool, create_pool_with_configs, create_pool_with_schema,
    create_pool_with_schema_and_configs, create_pool_with_schema_and_sqlite_config, create_pool_with_sqlite_config,
};
pub use remote_execution::{
    ClaimRemoteExecution, RemoteExecutionLedger, RemoteExecutionLedgerError, RemoteExecutionLink,
};
pub use response::ResponseStore;
pub use schema::{PoolWithSchema, SchemaManager};
pub use types::{
    ConversationData, ConversationSnapshot, ConversationVersion, InOutItem, ItemKind, ResponseData, ResponseMetadata,
    StorageError, StoreResult,
};
