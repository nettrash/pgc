use serde::{Deserialize, Serialize};
use sqlx::postgres::types::Oid;
use sha2::{Digest, Sha256};

// This is an information about a PostgreSQL table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableTrigger {
    pub oid: Oid, // Object identifier of the trigger
    pub name: String, // Name of the trigger
    pub definition: String, // Definition of the trigger
}

impl TableTrigger {
    /// Hash
    pub fn add_to_hasher(&self, hasher: &mut Sha256) {
        hasher.update(self.oid.0.to_string().as_bytes());
        hasher.update(self.name.as_bytes());
        hasher.update(self.definition.as_bytes());
    }
}