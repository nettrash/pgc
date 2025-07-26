use serde::{Deserialize, Serialize};

// This is a database dump configuration structure.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DumpConfig {
    // Database host
    pub host: String,
    // Database port
    pub port: String,
    // Database user name
    pub user: String,
    // Database user password
    pub password: String,
    // Database name
    pub database: String,
    // Schema name. Mask allowed. For example: sche*
    pub scheme: String,
    // Flag of SSL usage
    pub ssl: bool,
    // Dump file name
    pub file: String,
}

impl DumpConfig {
    // Returns the connection string for the database.
    pub fn get_connection_string(&self) -> String {
        format!(
            "postgres://{}:{}@{}:{}/{}?sslmode={}",
            self.user,
            self.password,
            self.host,
            self.port,
            self.database,
            if self.ssl { "require" } else { "disable" }
        )
    }

    // Returns a masked connection string for the database.
    pub fn get_masked_connection_string(&self) -> String {
        format!(
            "postgres://*:*@{}:{}/{}?sslmode={}",
            self.host,
            self.port,
            self.database,
            if self.ssl { "require" } else { "disable" }
        )
    }
}
impl Default for DumpConfig {
    fn default() -> Self {
        DumpConfig {
            host: "localhost".to_string(),
            port: "5432".to_string(),
            user: "postgres".to_string(),
            password: "postgres".to_string(),
            database: "postgres".to_string(),
            scheme: "public".to_string(),
            ssl: false,
            file: "dump.io".to_string(),
        }
    }
}
