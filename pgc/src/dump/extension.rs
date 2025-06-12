use serde::{Deserialize, Serialize};

// This is an information about a PostgreSQL extension.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Extension {
    /// Name of the extension
    pub name: String,
    /// Version of the extension
    pub version: String,
    /// Schema where the extension is installed
    pub schema: String,
}