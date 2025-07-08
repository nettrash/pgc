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

impl Extension {
    /// Creates a new Extension with the given name, version, and schema
    pub fn new(name: String, version: String, schema: String) -> Self {
        Self {
            name,
            version,
            schema,
        }
    }


    /// Hash
    pub fn hash(&self) -> String {
        format!("{:x}", md5::compute(format!(
            "{}.{}",
            self.schema,
            self.name
        )))
    }

    /// Returns a string to create the extension.
    pub fn get_script(&self) -> String {
        let script = format!(
            "create extension {} with schema {};\n",
            self.name,
            self.schema
        );

        script
    }

    /// Returns a string to drop the extension.
    pub fn get_drop_script(&self) -> String {
        format!(
            "drop extension if exists {};\n",
            self.name
        )
    }
}