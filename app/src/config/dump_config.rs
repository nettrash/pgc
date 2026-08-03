//! Connection details and output path for one side of a comparison.
//!
//! Note that [`DumpConfig`] is `skip_serializing` on
//! [`Dump`](crate::dump::core::Dump): the password must never reach a dump file.

use serde::{Deserialize, Serialize};

/// This is a database dump configuration structure.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DumpConfig {
    /// Database host
    pub host: String,
    /// Database port
    pub port: String,
    /// Database user name
    pub user: String,
    /// Database user password
    pub password: String,
    /// Database name
    pub database: String,
    /// Schema name. Mask allowed. For example: sche*
    pub scheme: String,
    /// Flag of SSL usage
    pub ssl: bool,
    /// Dump file name
    pub file: String,
}

impl DumpConfig {
    /// Returns the connection string for the database.
    /// # Examples
    ///
    /// ```
    /// use pgc::config::dump_config::DumpConfig;
    ///
    /// let config = DumpConfig {
    ///     host: "db.example".to_string(),
    ///     port: "5432".to_string(),
    ///     user: "alice".to_string(),
    ///     password: "s3cret".to_string(),
    ///     database: "shop".to_string(),
    ///     scheme: "public".to_string(),
    ///     ssl: true,
    ///     file: "dump.from".to_string(),
    /// };
    ///
    /// assert_eq!(
    ///     config.get_connection_string(),
    ///     "postgres://alice:s3cret@db.example:5432/shop?sslmode=require"
    /// );
    /// ```
    ///
    /// This string contains the password — use
    /// [`get_masked_connection_string`](Self::get_masked_connection_string)
    /// for anything that gets printed or logged.
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

    /// Returns a masked connection string for the database.
    /// # Examples
    ///
    /// ```
    /// use pgc::config::dump_config::DumpConfig;
    ///
    /// let config = DumpConfig {
    ///     host: "db.example".to_string(),
    ///     port: "5432".to_string(),
    ///     user: "alice".to_string(),
    ///     password: "s3cret".to_string(),
    ///     database: "shop".to_string(),
    ///     scheme: "public".to_string(),
    ///     ssl: false,
    ///     file: "dump.from".to_string(),
    /// };
    ///
    /// let masked = config.get_masked_connection_string();
    /// assert_eq!(masked, "postgres://*:*@db.example:5432/shop?sslmode=disable");
    /// assert!(!masked.contains("s3cret"));
    /// ```
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

#[cfg(test)]
#[path = "tests/dump_config.rs"]
mod tests;
