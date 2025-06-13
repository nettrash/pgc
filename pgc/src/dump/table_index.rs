use serde::{Deserialize, Serialize};

// This is an information about a PostgreSQL table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableIndex {
    pub schema: String,          // Schema name
    pub table: String,           // Table name
    pub name: String,            // Index name
    pub catalog: Option<String>, // Catalog name
    pub indexdef: String,        // Index definition
}
