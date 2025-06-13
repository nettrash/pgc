use serde::{Deserialize, Serialize};
use sqlx::postgres::types::Oid;

// This is an information about a PostgreSQL table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableTrigger {
    pub oid: Oid, // Object identifier of the trigger
    pub name: String, // Name of the trigger
    pub definition: String, // Definition of the trigger
}
