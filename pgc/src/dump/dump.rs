use crate::dump::pg_enum::PgEnum;
use crate::dump::pg_type::PgType;
use crate::dump::routine::Routine;
use crate::{config::dump_config::DumpConfig, dump::extension::Extension};
use serde::{Deserialize, Serialize};
use sqlx::Connection;
use sqlx::PgConnection;
use sqlx::Row;
use sqlx::postgres::types::Oid;
use std::fs::File;
use std::io::Error;
use std::io::Write;
use zip::ZipWriter;
use zip::write::SimpleFileOptions;

// This file defines the Dump struct and its serialization/deserialization logic.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Dump {
    // Configuration of the dump.
    #[serde(skip_serializing, skip_deserializing)]
    configuration: DumpConfig,

    // List of extensions in the dump.
    extensions: Vec<Extension>,

    // List of PostgreSQL types in the dump.
    types: Vec<PgType>,

    // List of PostgreSQL enums in the dump.
    enums: Vec<PgEnum>,

    // List of routines in the dump.
    routines: Vec<Routine>,
}

impl Dump {
    // Create a new Dump instance.
    pub fn new(config: DumpConfig) -> Self {
        Dump {
            configuration: config,
            extensions: Vec::new(),
            types: Vec::new(),
            enums: Vec::new(),
            routines: Vec::new(),
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
        let options = SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Deflated)
            .unix_permissions(0o644);
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
        self.get_types(conn).await?;
        self.get_enums(conn).await?;
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

    // Fetch types from the database and populate the dump.
    async fn get_types(&mut self, conn: &mut PgConnection) -> Result<(), Error> {
        let result = sqlx::query(
            format!(
                "select 
                n.nspname, 
                t.typname,
                t.typnamespace,
                t.typowner,
                t.typlen,
                t.typbyval,
                t.typtype,
                t.typcategory,
                t.typispreferred,
                t.typisdefined,
                t.typdelim,
                t.typrelid,
                t.typsubscript::text as typsubscript,
                t.typelem,
                t.typarray,
                t.typinput::text as typinput,
                t.typoutput::text as typoutput,
                t.typreceive::text as typreceive,
                t.typsend::text as typsend,
                t.typmodin::text as typmodin,
                t.typmodout::text as typmodout,
                t.typanalyze::text as typanalyze,
                t.typalign,
                t.typstorage,
                t.typnotnull,
                t.typbasetype,
                t.typtypmod,
                t.typndims,
                t.typcollation,
                t.typdefault
            from 
                pg_type t 
                join pg_namespace n on t.typnamespace = n.oid 
            where 
                n.nspname like '{}' 
                and t.typtype in ('d', 'e', 'r', 'm') 
                and t.typcategory = 'U'
                and t.typisdefined = true",
                self.configuration.scheme
            )
            .as_str(),
        )
        .fetch_all(conn)
        .await;
        if result.is_err() {
            return Err(Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to fetch types: {}.", result.err().unwrap()),
            ));
        }
        let rows = result.unwrap();

        if rows.is_empty() {
            println!("No types found.");
        } else {
            println!("Types found:");
            for row in rows {
                let pgtype = PgType {
                    schema: row.get("nspname"),
                    typname: row.get("typname"),
                    typnamespace: row.get("typnamespace"),
                    typowner: row.get("typowner"),
                    typlen: row.get("typlen"),
                    typbyval: row.get("typbyval"),
                    typtype: row.get("typtype"),
                    typcategory: row.get("typcategory"),
                    typispreferred: row.get("typispreferred"),
                    typisdefined: row.get("typisdefined"),
                    typdelim: row.get("typdelim"),
                    typrelid: row.get::<Option<Oid>, _>("typrelid"),
                    typsubscript: row.get::<Option<String>, _>("typsubscript"),
                    typelem: row.get::<Option<Oid>, _>("typelem"),
                    typarray: row.get::<Option<Oid>, _>("typarray"),
                    typinput: row.get("typinput"),
                    typoutput: row.get("typoutput"),
                    typreceive: row.get::<Option<String>, _>("typreceive"),
                    typsend: row.get::<Option<String>, _>("typsend"),
                    typmodin: row.get::<Option<String>, _>("typmodin"),
                    typmodout: row.get::<Option<String>, _>("typmodout"),
                    typanalyze: row.get::<Option<String>, _>("typanalyze"),
                    typalign: row.get("typalign"),
                    typstorage: row.get("typstorage"),
                    typnotnull: row.get("typnotnull"),
                    typbasetype: row.get::<Option<Oid>, _>("typbasetype"),
                    typtypmod: row.get::<Option<i32>, _>("typtypmod"),
                    typndims: row.get("typndims"),
                    typcollation: row.get::<Option<Oid>, _>("typcollation"),
                    typdefault: row.get::<Option<String>, _>("typdefault"),
                };
                self.types.push(pgtype.clone());
                println!(" - {} (namespace: {})", pgtype.typname, pgtype.schema);
            }
        }
        Ok(())
    }
    
    // Fetch enums from the database and populate the dump.
    async fn get_enums(&mut self, conn: &mut PgConnection) -> Result<(), Error> {
        let result = sqlx::query("select * from pg_enum")
        .fetch_all(conn)
        .await;
        if result.is_err() {
            return Err(Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to fetch enums: {}.", result.err().unwrap()),
            ));
        }
        let rows = result.unwrap();

        if rows.is_empty() {
            println!("No enums found.");
        } else {
            println!("Enums found:");
            for row in rows {
                let pgenum = PgEnum {
                    oid: row.get("oid"),
                    enumtypid: row.get("enumtypid"),
                    enumsortorder: row.get("enumsortorder"),
                    enumlabel: row.get("enumlabel")
                };
                self.enums.push(pgenum.clone());
                println!(" - enumtypid {} (label: {})", pgenum.enumtypid.0, pgenum.enumlabel);
            }
        }
        Ok(())
    }
}
