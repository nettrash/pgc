use crate::dump::pg_enum::PgEnum;
use crate::dump::pg_type::PgType;
use crate::dump::routine::Routine;
use crate::dump::table::Table;
use crate::{config::dump_config::DumpConfig, dump::extension::Extension};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
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

    // List of tables in the dump.
    tables: Vec<Table>,
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
            tables: Vec::new(),
        }
    }

    // Retrieve the dump from the configuration.
    pub async fn process(&mut self) -> Result<(), Error> {
        let pool = PgPool::connect(self.configuration.get_connection_string().as_str())
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
        self.fill(&pool).await?;

        pool.close().await;

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

    // Fill the Dump with data from the database.
    async fn fill(&mut self, pool: &PgPool) -> Result<(), Error> {
        // Fetch extensions from the database.
        self.get_extensions(pool).await?;
        self.get_types(pool).await?;
        self.get_enums(pool).await?;
        self.get_routines(pool).await?;
        self.get_tables(pool).await?;
        Ok(())
    }

    // Fetch extensions from the database and populate the dump.
    async fn get_extensions(&mut self, pool: &PgPool) -> Result<(), Error> {
        let result = sqlx::query(format!("SELECT n.nspname, e.* from pg_extension e JOIN pg_namespace n ON e.extnamespace = n.oid AND n.nspname LIKE '{}'", self.configuration.scheme).as_str())
            .fetch_all(pool)
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
    async fn get_types(&mut self, pool: &PgPool) -> Result<(), Error> {
        let result = sqlx::query(
            format!(
                "SELECT 
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
                t.typsubscript::text AS typsubscript,
                t.typelem,
                t.typarray,
                t.typinput::text AS typinput,
                t.typoutput::text AS typoutput,
                t.typreceive::text AS typreceive,
                t.typsend::text AS typsend,
                t.typmodin::text AS typmodin,
                t.typmodout::text AS typmodout,
                t.typanalyze::text AS typanalyze,
                t.typalign,
                t.typstorage,
                t.typnotnull,
                t.typbasetype,
                t.typtypmod,
                t.typndims,
                t.typcollation,
                t.typdefault
            FROM 
                pg_type t 
                JOIN pg_namespace n ON t.typnamespace = n.oid 
            WHERE 
                n.nspname LIKE '{}' 
                AND t.typtype IN ('d', 'e', 'r', 'm') 
                AND t.typcategory = 'U'
                AND t.typisdefined = true",
                self.configuration.scheme
            )
            .as_str(),
        )
        .fetch_all(pool)
        .await;
        if result.is_err() {
            return Err(Error::new(
                std::io::ErrorKind::Other,
                format!(
                    "Failed to fetch user-defined types: {}.",
                    result.err().unwrap()
                ),
            ));
        }
        let rows = result.unwrap();

        if rows.is_empty() {
            println!("No user-defined types found.");
        } else {
            println!("User-defined types found:");
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
    async fn get_enums(&mut self, pool: &PgPool) -> Result<(), Error> {
        let result = sqlx::query("SELECT * FROM pg_enum").fetch_all(pool).await;
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
                    enumlabel: row.get("enumlabel"),
                };
                self.enums.push(pgenum.clone());
                println!(
                    " - enumtypid {} (label: {})",
                    pgenum.enumtypid.0, pgenum.enumlabel
                );
            }
        }
        Ok(())
    }

    // Fetch routines from the database and populate the dump.
    async fn get_routines(&mut self, pool: &PgPool) -> Result<(), Error> {
        let result = sqlx::query(
            format!(
                "select
                    n.nspname,
                    r.oid,
                    r.proname,
                    l.lanname as prolang,
                    case when r.prokind = 'f' then 'function' else 'procedure' end as prokind,
                    t.typname as prorettype,
                    pg_get_function_identity_arguments(r.oid) as proarguments,
                    pg_get_expr(r.proargdefaults, 0) as proargdefaults,
                    r.prosrc
                from
                    pg_proc r
                    join pg_namespace n on r.pronamespace = n.oid
                    join pg_language l on r.prolang = l.oid
                    join pg_type t on r.prorettype = t.oid
                where
                    n.nspname like '{}'
                    and n.nspname not in ('pg_catalog', 'information_schema')
                    and l.lanname not in ('c', 'internal')
                    and r.prokind in ('f', 'p');
                ",
                self.configuration.scheme
            )
            .as_str(),
        )
        .fetch_all(pool)
        .await;
        if result.is_err() {
            return Err(Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to fetch routines: {}.", result.err().unwrap()),
            ));
        }
        let rows = result.unwrap();

        if rows.is_empty() {
            println!("No routines found.");
        } else {
            println!("Routines found:");
            for row in rows {
                let routine = Routine {
                    schema: row.get("nspname"),
                    oid: row.get("oid"),
                    name: row.get("proname"),
                    lang: row.get("prolang"),
                    kind: row.get("prokind"),
                    return_type: row.get("prorettype"),
                    arguments: row.get("proarguments"),
                    arguments_defaults: row.get::<Option<String>, _>("proargdefaults"),
                    source_code: row.get("prosrc"),
                };
                self.routines.push(routine.clone());
                println!(
                    " - {} {}.{} (lang: {}, arguments: {})",
                    routine.kind, routine.schema, routine.name, routine.lang, routine.arguments
                );
            }
        }
        Ok(())
    }

    // Fetch tables from the database and populate the dump.
    async fn get_tables(&mut self, pool: &PgPool) -> Result<(), Error> {
        let result = sqlx::query(
            format!(
                "
                    SELECT * 
                    FROM 
                        pg_tables 
                    WHERE 
                        schemaname NOT IN ('pg_catalog', 'information_schema') 
                        AND schemaname LIKE '{}' 
                        AND tablename NOT LIKE 'pg_%' 
                    ORDER BY 
                        schemaname, 
                        tablename;",
                self.configuration.scheme
            )
            .as_str(),
        )
        .fetch_all(pool)
        .await;
        if result.is_err() {
            return Err(Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to fetch tables: {}.", result.err().unwrap()),
            ));
        }
        let rows = result.unwrap();

        if rows.is_empty() {
            println!("No tables found.");
        } else {
            println!("Tables found:");
            for row in rows {
                let mut table = Table {
                    schema: row.get("schemaname"),
                    name: row.get("tablename"),
                    owner: row.get("tableowner"),
                    space: row.get("tablespace"),
                    has_indexes: row.get("hasindexes"),
                    has_triggers: row.get("hastriggers"),
                    has_rules: row.get("hasrules"),
                    has_rowsecurity: row.get("rowsecurity"),
                    columns: Vec::new(),
                    constraints: Vec::new(),
                    indexes: Vec::new(),
                    triggers: Vec::new(),
                };
                table.fill(pool).await.map_err(|e| {
                    Error::new(
                        std::io::ErrorKind::Other,
                        format!("Failed to fill table {}: {}.", table.name, e),
                    )
                })?;

                self.tables.push(table.clone());

                println!(" - {}.{}", table.schema, table.name);
            }
        }
        Ok(())
    }
}
