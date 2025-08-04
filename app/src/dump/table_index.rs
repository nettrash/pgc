use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

// This is an information about a PostgreSQL table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableIndex {
    pub schema: String,          // Schema name
    pub table: String,           // Table name
    pub name: String,            // Index name
    pub catalog: Option<String>, // Catalog name
    pub indexdef: String,        // Index definition
}

impl TableIndex {
    /// Hash
    pub fn add_to_hasher(&self, hasher: &mut Sha256) {
        hasher.update(self.schema.as_bytes());
        hasher.update(self.table.as_bytes());
        hasher.update(self.name.as_bytes());
        if let Some(catalog) = &self.catalog {
            hasher.update(catalog.as_bytes());
        }
        hasher.update(self.indexdef.as_bytes());
    }

    /// Returns a string representation of the index
    pub fn get_script(&self) -> String {
        let mut script = String::new();
        script.push_str(&self.indexdef.to_lowercase());
        script.push_str(";\n");
        script
    }
}

impl PartialEq for TableIndex {
    fn eq(&self, other: &Self) -> bool {
        self.schema == other.schema
            && self.table == other.table
            && self.name == other.name
            && self.catalog == other.catalog
            && self.indexdef == other.indexdef
    }
}
