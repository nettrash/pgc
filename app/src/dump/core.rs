use crate::dump::pg_enum::PgEnum;
use crate::dump::pg_type::PgType;
use crate::dump::routine::Routine;
use crate::dump::schema::Schema;
use crate::dump::sequence::Sequence;
use crate::dump::table::Table;
use crate::{config::dump_config::DumpConfig, dump::extension::Extension};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use sqlx::Row;
use sqlx::postgres::types::Oid;
use std::fs::File;
use std::io::Write;
use std::io::{Error, Read};
use zip::ZipWriter;
use zip::write::SimpleFileOptions;

// This file defines the Dump struct and its serialization/deserialization logic.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Dump {
    // Configuration of the dump.
    #[serde(skip_serializing, skip_deserializing)]
    pub configuration: DumpConfig,

    // List of schemas in the dump.
    pub schemas: Vec<Schema>,

    // List of extensions in the dump.
    pub extensions: Vec<Extension>,

    // List of PostgreSQL types in the dump.
    pub types: Vec<PgType>,

    // List of PostgreSQL enums in the dump.
    pub enums: Vec<PgEnum>,

    // List of sequences in the dump.
    pub sequences: Vec<Sequence>,

    // List of routines in the dump.
    pub routines: Vec<Routine>,

    // List of tables in the dump.
    pub tables: Vec<Table>,
}

impl Dump {
    // Create a new Dump instance.
    pub fn new(config: DumpConfig) -> Self {
        Dump {
            configuration: config,
            schemas: Vec::new(),
            extensions: Vec::new(),
            types: Vec::new(),
            enums: Vec::new(),
            sequences: Vec::new(),
            routines: Vec::new(),
            tables: Vec::new(),
        }
    }

    // Retrieve the dump from the configuration.
    pub async fn process(&mut self) -> Result<(), Error> {
        let pool = PgPool::connect(self.configuration.get_connection_string().as_str())
            .await
            .map_err(|e| {
                Error::other(format!(
                    "Failed to connect to database ({}): {}.",
                    self.configuration.get_masked_connection_string(),
                    e
                ))
            })?;

        // Fill the dump.
        self.fill(&pool).await?;

        pool.close().await;

        // Serialize the dump to a file.
        let serialized = serde_json::to_string(&self);
        if serialized.is_err() {
            return Err(Error::other(format!(
                "Failed to serialize dump: {}.",
                serialized.err().unwrap()
            )));
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
        self.get_schemas(pool).await?;
        self.get_extensions(pool).await?;
        self.get_types(pool).await?;
        self.get_enums(pool).await?;
        self.get_sequences(pool).await?;
        self.get_routines(pool).await?;
        self.get_tables(pool).await?;
        Ok(())
    }

    async fn get_schemas(&mut self, pool: &PgPool) -> Result<(), Error> {
        let result = sqlx::query(
            format!("SELECT schema_name FROM information_schema.schemata WHERE schema_name LIKE '{}' AND schema_name NOT IN ('pg_catalog', 'information_schema')", self.configuration.scheme).as_str(),
        )
        .fetch_all(pool)
        .await;
        if result.is_err() {
            return Err(Error::other(format!(
                "Failed to fetch schemas: {}.",
                result.err().unwrap()
            )));
        }
        let rows = result.unwrap();

        if rows.is_empty() {
            println!("No schemas found.");
        } else {
            println!("Schemas found:");
            for row in rows {
                let schema = row.get("schema_name");
                let sch = Schema::new(schema);
                self.schemas.push(sch.clone());
                println!(" - {}", sch.name);
            }
        }
        Ok(())
    }

    // Fetch extensions from the database and populate the dump.
    async fn get_extensions(&mut self, pool: &PgPool) -> Result<(), Error> {
        let result = sqlx::query(format!("SELECT n.nspname, e.* from pg_extension e JOIN pg_namespace n ON e.extnamespace = n.oid AND (n.nspname LIKE '{}' OR n.nspname = 'public')", self.configuration.scheme).as_str())
            .fetch_all(pool)
            .await;
        if result.is_err() {
            return Err(Error::other(format!(
                "Failed to fetch extensions: {}.",
                result.err().unwrap()
            )));
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
            return Err(Error::other(format!(
                "Failed to fetch user-defined types: {}.",
                result.err().unwrap()
            )));
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
            return Err(Error::other(format!(
                "Failed to fetch enums: {}.",
                result.err().unwrap()
            )));
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

    // Fetch sequences from the database and populate the dump.
    async fn get_sequences(&mut self, pool: &PgPool) -> Result<(), Error> {
        let result = sqlx::query(
            format!(
                "
        SELECT 
            schemaname, 
            sequencename, 
            sequenceowner, 
            data_type::varchar as sequencedatatype, 
            start_value, 
            min_value, 
            max_value, 
            increment_by, 
            cycle, 
            cache_size, 
            last_value 
        FROM 
            pg_sequences 
        WHERE 
            schemaname like '%{}%'",
                self.configuration.scheme
            )
            .as_str(),
        )
        .fetch_all(pool)
        .await;

        if result.is_err() {
            return Err(Error::other(format!(
                "Failed to fetch sequences: {}.",
                result.err().unwrap()
            )));
        }
        let rows = result.unwrap();

        if rows.is_empty() {
            println!("No sequences found.");
        } else {
            println!("Sequences found:");
            for row in rows {
                let seq = Sequence {
                    schema: row.get("schemaname"),
                    name: row.get("sequencename"),
                    owner: row.get("sequenceowner"),
                    data_type: row.get("sequencedatatype"),
                    start_value: row.get::<Option<i64>, _>("start_value"),
                    min_value: row.get::<Option<i64>, _>("min_value"),
                    max_value: row.get::<Option<i64>, _>("max_value"),
                    increment_by: row.get::<Option<i64>, _>("increment_by"),
                    cycle: row.get("cycle"),
                    cache_size: row.get::<Option<i64>, _>("cache_size"),
                    last_value: row.get::<Option<i64>, _>("last_value"),
                };
                self.sequences.push(seq.clone());
                println!(" - name {} (type: {})", seq.name, seq.data_type);
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
            return Err(Error::other(format!(
                "Failed to fetch routines: {}.",
                result.err().unwrap()
            )));
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
            return Err(Error::other(format!(
                "Failed to fetch tables: {}.",
                result.err().unwrap()
            )));
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
                    definition: None,
                };
                table.fill(pool).await.map_err(|e| {
                    Error::other(format!("Failed to fill table {}: {}.", table.name, e))
                })?;

                self.tables.push(table.clone());

                println!(" - {}.{}", table.schema, table.name);
            }
        }
        Ok(())
    }

    // Read a dump from a file and deserialize it.
    pub async fn read_from_file(file: &str) -> Result<Self, Error> {
        let file = File::open(file)?;
        let mut zip = zip::ZipArchive::new(file)?;
        let mut dump_file = zip.by_name("dump.io")?;
        let mut serialized_data = String::new();
        dump_file.read_to_string(&mut serialized_data)?;

        let dump: Dump = serde_json::from_str(&serialized_data)
            .map_err(|e| Error::other(format!("Failed to deserialize dump: {e}.")))?;

        Ok(dump)
    }

    pub fn get_info(&self) -> String {
        format!(
            "\tDump Info:\n\t\t- Schemas: {}\n\t\t- Extensions: {}\n\t\t- Types: {}\n\t\t- Enums: {}\n\t\t- Routines: {}\n\t\t- Tables: {}",
            self.schemas.len(),
            self.extensions.len(),
            self.types.len(),
            self.enums.len(),
            self.routines.len(),
            self.tables.len()
        )
    }
}
