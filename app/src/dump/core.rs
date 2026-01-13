use crate::dump::pg_enum::PgEnum;
use crate::dump::pg_type::{DomainConstraint, PgType};
use crate::dump::routine::Routine;
use crate::dump::schema::Schema;
use crate::dump::sequence::Sequence;
use crate::dump::table::Table;
use crate::dump::view::View;
use crate::{config::dump_config::DumpConfig, dump::extension::Extension};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use sqlx::Row;
use sqlx::postgres::types::Oid;
use std::cmp::Ordering;
use std::collections::HashMap;
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

    // List of views in the dump.
    pub views: Vec<View>,
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
            views: Vec::new(),
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
        Ok(())
    }

    async fn fill(&mut self, pool: &PgPool) -> Result<(), Error> {
        self.get_schemas(pool).await?;
        self.get_extensions(pool).await?;
        self.get_types(pool).await?;
        self.get_domain_constraints(pool).await?;
        self.get_enums(pool).await?;
        self.get_sequences(pool).await?;
        self.get_routines(pool).await?;
        self.get_tables(pool).await?;
        self.get_views(pool).await?;
        Ok(())
    }

    async fn get_schemas(&mut self, pool: &PgPool) -> Result<(), Error> {
        let query = format!(
            "select n.nspname as schema_name, d.description as schema_comment
             from pg_namespace n
             left join pg_description d on d.objoid = n.oid
                 and d.classoid = 'pg_namespace'::regclass
                 and d.objsubid = 0
             where n.nspname like '{}'
               and n.nspname not in ('pg_catalog', 'information_schema')",
            self.configuration.scheme
        );

        let result = sqlx::query(query.as_str()).fetch_all(pool).await;
        if let Err(e) = &result {
            return Err(Error::other(format!("Failed to fetch schemas: {}.", e)));
        }
        let rows = result.unwrap();

        if rows.is_empty() {
            println!("No schemas found.");
        } else {
            println!("Schemas found:");
            for row in rows {
                let schema = row.get("schema_name");
                let comment: Option<String> = row.get("schema_comment");
                let sch = Schema::new(schema, comment);
                self.schemas.push(sch.clone());
                println!(" - {}", sch.name);
            }
        }
        Ok(())
    }

    // Fetch extensions from the database and populate the dump.
    async fn get_extensions(&mut self, pool: &PgPool) -> Result<(), Error> {
        let result = sqlx::query(format!("select n.nspname, e.* from pg_extension e join pg_namespace n on e.extnamespace = n.oid and (n.nspname like '{}' or n.nspname = 'public')", self.configuration.scheme).as_str())
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
                "select 
                n.nspname, 
                t.oid as type_oid,
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
                t.typdefault,
                pg_catalog.format_type(t.typbasetype, t.typtypmod) as formatted_basetype,
                d.description as comment
            from 
                pg_type t 
                join pg_namespace n on t.typnamespace = n.oid 
                left join pg_description d on d.objoid = t.oid
                    and d.classoid = 'pg_type'::regclass
                    and d.objsubid = 0
            where 
                n.nspname like '{}' 
                and t.typtype in ('d', 'e', 'r', 'm') 
                and t.typisdefined = true",
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
                let mut pgtype = PgType {
                    oid: row.get("type_oid"),
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
                    formatted_basetype: row.get::<Option<String>, _>("formatted_basetype"),
                    comment: row.get::<Option<String>, _>("comment"),
                    enum_labels: Vec::new(),
                    domain_constraints: Vec::new(),
                    hash: None,
                };
                pgtype.hash();
                self.types.push(pgtype.clone());
                println!(
                    " - {} (namespace: {}, hash: {})",
                    pgtype.typname,
                    pgtype.schema,
                    pgtype.hash.as_deref().unwrap_or("None")
                );
            }
        }
        Ok(())
    }

    // Fetch enums from the database and populate the dump.
    async fn get_enums(&mut self, pool: &PgPool) -> Result<(), Error> {
        let result = sqlx::query("select * from pg_enum").fetch_all(pool).await;
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

            let mut labels_by_type: HashMap<u32, Vec<(f32, String)>> = HashMap::new();
            for enum_value in &self.enums {
                labels_by_type
                    .entry(enum_value.enumtypid.0)
                    .or_default()
                    .push((enum_value.enumsortorder, enum_value.enumlabel.clone()));
            }

            for (type_oid, mut labels) in labels_by_type {
                labels.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(Ordering::Equal));

                if let Some(pg_type) = self.types.iter_mut().find(|t| t.oid.0 == type_oid) {
                    pg_type.enum_labels = labels.into_iter().map(|(_, label)| label).collect();
                }
            }
        }
        Ok(())
    }

    async fn get_domain_constraints(&mut self, pool: &PgPool) -> Result<(), Error> {
        let query = format!(
            "select
                c.contypid as domain_oid,
                c.conname,
                pg_get_constraintdef(c.oid) as definition
            from pg_constraint c
            join pg_type t on t.oid = c.contypid
            join pg_namespace n on n.oid = t.typnamespace
            where c.contype = 'c'
              and c.contypid <> 0
              and n.nspname like '{}'
            order by c.contypid, c.conname",
            self.configuration.scheme
        );

        let result = sqlx::query(query.as_str()).fetch_all(pool).await;
        if result.is_err() {
            return Err(Error::other(format!(
                "Failed to fetch domain constraints: {}.",
                result.err().unwrap()
            )));
        }
        let rows = result.unwrap();

        if rows.is_empty() {
            println!("No domain constraints found.");
        } else {
            println!("Domain constraints found:");
        }

        let mut constraints_by_type: HashMap<u32, Vec<DomainConstraint>> = HashMap::new();
        for row in rows {
            let type_oid: Oid = row.get("domain_oid");
            let constraint = DomainConstraint {
                name: row.get("conname"),
                definition: row.get("definition"),
            };
            constraints_by_type
                .entry(type_oid.0)
                .or_default()
                .push(constraint.clone());
            println!(" - {} on type {}", constraint.name, type_oid.0);
        }

        for pg_type in &mut self.types {
            if let Some(mut constraints) = constraints_by_type.remove(&pg_type.oid.0) {
                constraints.sort_by(|a, b| a.name.cmp(&b.name));
                pg_type.domain_constraints = constraints;
            }
        }

        Ok(())
    }

    // Fetch sequences from the database and populate the dump.
    async fn get_sequences(&mut self, pool: &PgPool) -> Result<(), Error> {
        let result = sqlx::query(
            format!(
                "
                select
                    seq.schemaname,
                    seq.sequencename,
                    seq.sequenceowner,
                    seq.data_type::varchar as sequencedatatype,
                    seq.start_value,
                    seq.min_value,
                    seq.max_value,
                    seq.increment_by,
                    seq.cycle,
                    seq.cache_size,
                    seq.last_value,
                    owner_ns.nspname as owned_by_schema,
                    owner_table.relname as owned_by_table,
                    owner_attr.attname as owned_by_column,
                    dep.deptype::text as dependency_type,
                    seq_desc.description as seq_comment
                from
                    pg_sequences seq
                    left join pg_namespace seq_ns on seq_ns.nspname = seq.schemaname
                    left join pg_class seq_class on seq_class.relname = seq.sequencename
                        and seq_class.relnamespace = seq_ns.oid
                    left join pg_description seq_desc on seq_desc.objoid = seq_class.oid and seq_desc.objsubid = 0
                    left join pg_depend dep on dep.objid = seq_class.oid
                        and dep.deptype in ('a', 'i')
                    left join pg_class owner_table on owner_table.oid = dep.refobjid
                    left join pg_namespace owner_ns on owner_ns.oid = owner_table.relnamespace
                    left join pg_attribute owner_attr on owner_attr.attrelid = dep.refobjid
                        and owner_attr.attnum = dep.refobjsubid
                where
                    seq.schemaname like '%{}%'",
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
                let mut seq = Sequence {
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
                    owned_by_schema: row.get::<Option<String>, _>("owned_by_schema"),
                    owned_by_table: row.get::<Option<String>, _>("owned_by_table"),
                    owned_by_column: row.get::<Option<String>, _>("owned_by_column"),
                    is_identity: false,
                    comment: row.get("seq_comment"),
                    hash: None,
                };
                if let Some(deptype) = row.get::<Option<String>, _>("dependency_type")
                    && deptype == "i"
                {
                    seq.is_identity = true;
                }
                seq.hash();
                self.sequences.push(seq.clone());
                println!(
                    " - name {} (type: {}, hash: {})",
                    seq.name,
                    seq.data_type,
                    seq.hash.as_deref().unwrap_or("None")
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
                    pg_get_function_result(r.oid) as prorettype,
                    pg_get_function_identity_arguments(r.oid) as proarguments,
                    pg_get_expr(r.proargdefaults, 0) as proargdefaults,
                    r.prosrc,
                    d.description as routine_comment
                from
                    pg_proc r
                    join pg_namespace n on r.pronamespace = n.oid
                    join pg_language l on r.prolang = l.oid
                    left join pg_description d on d.objoid = r.oid and d.classoid = 'pg_proc'::regclass and d.objsubid = 0
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
                let mut routine = Routine {
                    schema: row.get("nspname"),
                    oid: row.get("oid"),
                    name: row.get("proname"),
                    lang: row.get("prolang"),
                    kind: row.get("prokind"),
                    return_type: row
                        .get::<Option<String>, _>("prorettype")
                        .unwrap_or_else(|| "void".to_string()),
                    arguments: row.get("proarguments"),
                    arguments_defaults: row.get::<Option<String>, _>("proargdefaults"),
                    comment: row.get("routine_comment"),
                    source_code: row.get("prosrc"),
                    hash: None,
                };
                routine.hash();
                self.routines.push(routine.clone());
                println!(
                    " - {} {}.{} (lang: {}, arguments: {}, hash: {})",
                    routine.kind,
                    routine.schema,
                    routine.name,
                    routine.lang,
                    routine.arguments,
                    routine.hash.as_deref().unwrap_or("None")
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
                    select t.*, d.description as table_comment
                    from pg_tables t
                    left join pg_class c on c.relname = t.tablename
                        and c.relkind in ('r','p')
                        and c.relnamespace = (select oid from pg_namespace where nspname = t.schemaname)
                    left join pg_description d on d.objoid = c.oid and d.objsubid = 0
                    where 
                        t.schemaname not in ('pg_catalog', 'information_schema') 
                        and t.schemaname like '{}' 
                        and t.tablename not like 'pg_%';",
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
                    partition_key: None,
                    partition_of: None,
                    partition_bound: None,
                    comment: row.get("table_comment"),
                    hash: None,
                };
                table.fill(pool).await.map_err(|e| {
                    Error::other(format!("Failed to fill table {}: {}.", table.name, e))
                })?;

                table.hash();
                self.tables.push(table.clone());

                println!(
                    " - {}.{} (hash: {})",
                    table.schema,
                    table.name,
                    table.hash.as_deref().unwrap_or("None")
                );
            }
        }
        Ok(())
    }

    async fn get_views(&mut self, pool: &PgPool) -> Result<(), Error> {
        let result = sqlx::query(
            format!(
                "select v.table_schema, v.table_name, v.view_definition, array_agg(distinct vtu.table_schema || '.' || vtu.table_name) as table_relation, d.description as view_comment
                from information_schema.views v
                join information_schema.view_table_usage vtu on v.table_name = vtu.view_name and v.table_schema = vtu.view_schema
                left join pg_class c on c.relname = v.table_name and c.relnamespace = (select oid from pg_namespace where nspname = v.table_schema)
                left join pg_description d on d.objoid = c.oid and d.objsubid = 0
                where
                    v.table_schema not in ('pg_catalog', 'information_schema')
                    and v.table_schema like '{}'
                group by v.table_schema, v.table_name, v.view_definition, d.description;",
                self.configuration.scheme
            )
            .as_str(),
        )
        .fetch_all(pool)
        .await;
        if result.is_err() {
            return Err(Error::other(format!(
                "Failed to fetch views: {}.",
                result.err().unwrap()
            )));
        }
        let rows = result.unwrap();

        if rows.is_empty() {
            println!("No views found.");
        } else {
            println!("Views found:");
            for row in rows {
                let mut view = View {
                    schema: row.get("table_schema"),
                    name: row.get("table_name"),
                    definition: row.get("view_definition"),
                    table_relation: row.get("table_relation"),
                    comment: row.get("view_comment"),
                    hash: None,
                };
                view.hash();
                self.views.push(view.clone());

                println!(
                    " - {}.{} (hash: {})",
                    view.schema,
                    view.name,
                    view.hash.as_deref().unwrap_or("None")
                );
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
            "\tDump Info:\n\t\t- Schemas: {}\n\t\t- Extensions: {}\n\t\t- Types: {}\n\t\t- Enums: {}\n\t\t- Routines: {}\n\t\t- Tables: {}\n\t\t- Views: {}",
            self.schemas.len(),
            self.extensions.len(),
            self.types.len(),
            self.enums.len(),
            self.routines.len(),
            self.tables.len(),
            self.views.len()
        )
    }
}
