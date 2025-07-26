use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::postgres::types::Oid;

// This is an information about a PostgreSQL table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableTrigger {
    pub oid: Oid,           // Object identifier of the trigger
    pub name: String,       // Name of the trigger
    pub definition: String, // Definition of the trigger
}

impl TableTrigger {
    /// Hash
    pub fn add_to_hasher(&self, hasher: &mut Sha256) {
        hasher.update(self.oid.0.to_string().as_bytes());
        hasher.update(self.name.as_bytes());
        hasher.update(self.definition.as_bytes());
    }

    /// Returns a string representation of the trigger
    pub fn get_script(&self) -> String {
        let mut script = String::new();
        script.push_str(&format!("create trigger {} ", self.name));
        script.push_str(&self.definition);
        script.push(';');
        script
    }
}
