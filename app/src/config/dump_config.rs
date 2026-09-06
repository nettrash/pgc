//! Connection details and output path for one side of a comparison.
//!
//! Note that [`DumpConfig`] is `skip_serializing` on
//! [`Dump`](crate::dump::core::Dump): the password must never reach a dump file.

use serde::{Deserialize, Serialize};
use sqlx::postgres::{PgConnectOptions, PgSslMode};

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
    /// Connection options for the database, ready to hand to
    /// `PgPoolOptions::connect_with`.
    ///
    /// Deliberately **not** a URL. The credentials go into the fields as they
    /// were typed, and sqlx passes them to the PostgreSQL wire protocol
    /// unaltered; nothing ever parses them as URL syntax. Building a
    /// `postgres://user:password@host:port/db` string and letting sqlx parse it
    /// back means every RFC 3986 reserved character in a password is read as
    /// syntax rather than data, and that failed three different ways
    /// (issue #244, all verified live):
    ///
    /// - `#`, `/` and `?` end the authority section early, so the parser
    ///   eventually reads the port out of something that is not a number and
    ///   reports `invalid port number` — naming the one part of the URL that
    ///   was never wrong;
    /// - `%` followed by two hex digits is silently percent-**decoded**, so
    ///   `pass%41word` authenticates as `passAword`. A password that really
    ///   contains `%41` cannot be used at all, and the failure blames the
    ///   credentials rather than pgc;
    /// - `@` and a bare `%` happened to survive, which is worse than if they
    ///   had not: the class of bug looked narrower than it was.
    ///
    /// Built on `new_without_pgpass`, not `new`, because that is what sqlx's
    /// own URL parser uses (`PgConnectOptions::parse_from_url`). Starting from
    /// `new` instead would newly consult `~/.pgpass` and could silently
    /// substitute a password the configuration did not ask for.
    ///
    /// # Examples
    ///
    /// ```
    /// use pgc::config::dump_config::DumpConfig;
    ///
    /// let config = DumpConfig {
    ///     host: "db.example".to_string(),
    ///     port: "5432".to_string(),
    ///     user: "alice".to_string(),
    ///     // Reserved in a URL, ordinary in a password.
    ///     password: "p#ss/w?rd%41".to_string(),
    ///     database: "shop".to_string(),
    ///     scheme: "public".to_string(),
    ///     ssl: true,
    ///     file: "dump.from".to_string(),
    /// };
    ///
    /// let options = config.get_connect_options().unwrap();
    /// assert_eq!(options.get_host(), "db.example");
    /// assert_eq!(options.get_port(), 5432);
    /// ```
    ///
    /// # Errors
    ///
    /// [`sqlx::Error::Configuration`] when [`port`](Self::port) is not a `u16`.
    /// The field is a `String` because it arrives from a config file or a CLI
    /// flag, so the parse has to happen somewhere; doing it here means a bad
    /// port is reported as a bad port.
    pub fn get_connect_options(&self) -> Result<PgConnectOptions, sqlx::Error> {
        let port: u16 = self.port.trim().parse().map_err(sqlx::Error::config)?;

        Ok(PgConnectOptions::new_without_pgpass()
            .host(&self.host)
            .port(port)
            .username(&self.user)
            .password(&self.password)
            .database(&self.database)
            .ssl_mode(if self.ssl {
                PgSslMode::Require
            } else {
                PgSslMode::Disable
            }))
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
