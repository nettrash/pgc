use crate::config::dump_config::DumpConfig;
use serde::{Deserialize, Serialize};
use sqlx::Connection;
use std::io::Error;
use sqlx_postgres::PgConnection;

// This file defines the Dump struct and its serialization/deserialization logic.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Dump {
    // Configuration of the dump.
    #[serde(skip_serializing, skip_deserializing)]
    configuration: DumpConfig,
}

impl Dump {
    // Create a new Dump instance.
    pub fn new(config: DumpConfig) -> Self {
        Dump {
            configuration: config,
        }
    }

    // Retrieve the dump from the configuration.
    pub async fn process(&mut self) -> Result<(), Error> {
        let result = PgConnection::connect(self.configuration.get_connection_string().as_str()).await;
        if result.is_err() {
            return Err(Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to connect to database ({}): {}.", self.configuration.get_masked_connection_string(), result.err().unwrap()),
            ));
        }
        let mut conn = result.unwrap();
/*         sqlx::query(&self.configuration.dump_query)
            .execute(&mut conn)
            .await?;*/
        let result = conn.close().await;
        if result.is_err() {
            return Err(Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to close connection: {}.", result.err().unwrap()),
            ));
        }
        Ok(())
    }
}
