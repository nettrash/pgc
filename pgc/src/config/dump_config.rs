use serde::{Deserialize, Serialize};

// This is a database dump configuration structure.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DumpConfig {
    // Database host
    pub host: String,
    // Database name
    pub database: String,
    // Schema name. Mask allowed. For example: sche*
    pub scheme: String,
    // Flag of SSL usage
    pub ssl: bool,
    // Dump file name
    pub file: String,
}

impl Default for DumpConfig {
    fn default() -> Self {
        DumpConfig {
            host: "localhost".to_string(),
            database: "postgres".to_string(),
            scheme: "public".to_string(),
            ssl: false,
            file: "dump.io".to_string(),
        }
    }
}