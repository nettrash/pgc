use crate::{config::dump_config::DumpConfig, dump::extension::Extension};
use serde::{Deserialize, Serialize};
use sqlx::Connection;
use sqlx::PgConnection;
use sqlx::Row;
use zip::ZipWriter;
use std::fs::File;
use std::io::Error;
use std::io::Write;
use zip::write::SimpleFileOptions;

// This file defines the Dump struct and its serialization/deserialization logic.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Dump {
    // Configuration of the dump.
    #[serde(skip_serializing, skip_deserializing)]
    configuration: DumpConfig,

    extensions: Vec<Extension>,
}

impl Dump {
    // Create a new Dump instance.
    pub fn new(config: DumpConfig) -> Self {
        Dump {
            configuration: config,
            extensions: Vec::new(),
        }
    }

    // Retrieve the dump from the configuration.
    pub async fn process(&mut self) -> Result<(), Error> {
        let mut conn = PgConnection::connect(self.configuration.get_connection_string().as_str())
            .await
            .map_err(|e| {
                Error::new(
                    std::io::ErrorKind::Other,
                    format!(
                        "Failed to connect to database ({}): {}.",
                        self.configuration.get_masked_connection_string(),
                        e
                    ),
                )
            })?;

        // Fill the dump.
        self.fill(conn.as_mut()).await?;

        let result = conn.close().await;
        if result.is_err() {
            return Err(Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to close connection: {}.", result.err().unwrap()),
            ));
        }
        // Serialize the dump to a file.
        let serialized = serde_json::to_string(&self);
        if serialized.is_err() {
            return Err(Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to serialize dump: {}.", serialized.err().unwrap()),
            ));
        }
        let serialized_data = serialized.unwrap();
        let serialized_bytes = serialized_data.as_bytes();

        let file = File::create(&self.configuration.file)?;
        let mut zip = ZipWriter::new(file);
        let options =
            SimpleFileOptions::default().compression_method(zip::CompressionMethod::Deflated).unix_permissions(0o644);
        zip.start_file("dump.io", options)?;
        zip.write_all(serialized_bytes)?;
        zip.finish()?;

        // Successfully created the dump file.
        println!("Dump created successfully: {}", self.configuration.file);
        Ok(())
    }

    async fn fill(&mut self, conn: &mut PgConnection) -> Result<(), Error> {
        // Fetch extensions from the database.
        self.get_extensions(conn).await?;
        Ok(())
    }

    // Fetch extensions from the database and populate the dump.
    async fn get_extensions(&mut self, conn: &mut PgConnection) -> Result<(), Error> {
        let result = sqlx::query(format!("select n.nspname, e.* from pg_extension e join pg_namespace n on e.extnamespace = n.oid and n.nspname like '{}'", self.configuration.scheme).as_str())
            .fetch_all(conn)
            .await;
        if result.is_err() {
            return Err(Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to fetch extensions: {}.", result.err().unwrap()),
            ));
        }
        let rows = result.unwrap();

        if rows.is_empty() {
            println!("No extensions found.");
        } else {
            println!("Extensions found:");
            for row in rows {
                let ext = Extension {
                    name: row.get("extname"),
                    version: row.get("extversion"),
                    schema: row.get("nspname"),
                };
                self.extensions.push(ext.clone());
                println!(
                    " - {} (version: {}, namespace: {})",
                    ext.name, ext.version, ext.schema
                );
            }
        }
        Ok(())
    }
}
