use crate::dump::pg_enum::PgEnum;
use crate::dump::pg_type::{CompositeAttribute, DomainConstraint, PgType};
use crate::dump::routine::Routine;
use crate::dump::schema::Schema;
use crate::dump::sequence::Sequence;
use crate::dump::table::Table;
use crate::dump::view::View;
use crate::{config::dump_config::DumpConfig, dump::extension::Extension};
use futures::stream::{self, StreamExt};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use sqlx::Row;
use sqlx::postgres::PgPoolOptions;
use sqlx::postgres::types::Oid;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::io::{Error, Read};
use zip::ZipWriter;
use zip::write::SimpleFileOptions;

/// Number of sibling branches in `fill()` that run concurrently with the
/// table-fetching stream via `tokio::try_join!`: types/enums, extensions,
/// sequences, routines, and views.  We reserve this many pool connections so
/// the `buffer_unordered` table stream doesn't starve them.
///
/// **Keep in sync** with the number of non-table futures passed to
/// `tokio::try_join!` in `Dump::fill()`.
const FILL_SIBLING_BRANCH_COUNT: u32 = 5;

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
    pub async fn process(&mut self, max_connections: u32) -> Result<(), Error> {
        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .connect(self.configuration.get_connection_string().as_str())
            .await
            .map_err(|e| {
                Error::other(format!(
                    "Failed to connect to database ({}): {}.",
                    self.configuration.get_masked_connection_string(),
                    e
                ))
            })?;

        // Fill the dump.
        self.fill(&pool, max_connections).await?;

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

    /// Returns a SQL IN-clause for accessible schemas, e.g. `('public', 'data')`.
    /// Falls back to a no-match condition when there are no accessible schemas.
    fn accessible_schema_filter(&self) -> String {
        if self.schemas.is_empty() {
            return "(NULL)".to_string();
        }
        let names: Vec<String> = self
            .schemas
            .iter()
            .map(|s| format!("'{}'", s.raw_name.replace('\'', "''")))
            .collect();
        format!("({})", names.join(", "))
    }

    async fn fill(&mut self, pool: &PgPool, max_connections: u32) -> Result<(), Error> {
        self.get_schemas(pool).await?;
        if self.schemas.is_empty() {
            println!("No accessible schemas to dump.");
            return Ok(());
        }

        // Types, domain constraints and enums must run sequentially (they
        // depend on each other), but the rest of the queries are independent.
        // We run them all in parallel to reduce total wall-clock time on
        // high-latency (remote) connections.
        let schema_filter = self.accessible_schema_filter();

        let types_enums_fut = async {
            let mut types = Vec::new();
            let mut enums = Vec::new();
            // get_types logic (inlined to avoid &mut self borrow conflicts)
            Self::fetch_types_standalone(pool, &schema_filter, &mut types).await?;
            Self::fetch_domain_constraints_standalone(pool, &schema_filter, &mut types).await?;
            Self::fetch_enums_standalone(pool, &mut types, &mut enums).await?;
            Ok::<(Vec<PgType>, Vec<PgEnum>), Error>((types, enums))
        };

        let extensions_fut = Self::fetch_extensions_standalone(pool, &schema_filter);
        let sequences_fut = Self::fetch_sequences_standalone(pool, &schema_filter);
        let routines_fut = Self::fetch_routines_standalone(pool, &schema_filter);
        let tables_fut = Self::fetch_tables_standalone(pool, &schema_filter, max_connections);
        let views_fut = Self::fetch_views_standalone(pool, &schema_filter);

        let (types_enums, extensions, sequences, routines, tables, views) = tokio::try_join!(
            types_enums_fut,
            extensions_fut,
            sequences_fut,
            routines_fut,
            tables_fut,
            views_fut,
        )?;

        let (types, enums) = types_enums;
        self.types = types;
        self.enums = enums;
        self.extensions = extensions;
        self.sequences = sequences;
        self.routines = routines;
        self.tables = tables;
        self.views = views;

        Ok(())
    }

    async fn get_schemas(&mut self, pool: &PgPool) -> Result<(), Error> {
        let result = sqlx::query(
            "select
                    quote_ident(n.nspname) as schema_name,
                    n.nspname as raw_schema_name,
                    quote_ident(r.rolname) as schema_owner,
                    d.description as schema_comment,
                    has_schema_privilege(n.nspname, 'USAGE') as has_usage,
                    n.nspacl::text[] as schema_acl
             from pg_namespace n
             left join pg_roles r on r.oid = n.nspowner
             left join pg_description d on d.objoid = n.oid
                 and d.classoid = 'pg_namespace'::regclass
                 and d.objsubid = 0
             where n.nspname similar to $1
               and n.nspname not in ('pg_catalog', 'information_schema')",
        )
        .bind(&self.configuration.scheme)
        .fetch_all(pool)
        .await;
        if let Err(e) = &result {
            return Err(Error::other(format!("Failed to fetch schemas: {}.", e)));
        }
        let rows = result.unwrap();

        if rows.is_empty() {
            println!("No schemas found.");
        } else {
            println!("Schemas found:");
            for row in rows {
                let schema_name: String = row.get("schema_name");
                let raw_schema_name: String = row.get("raw_schema_name");
                let has_usage: bool = row.get("has_usage");

                if !has_usage {
                    println!(" - {} (skipped: no USAGE privilege)", schema_name);
                    continue;
                }

                let owner: Option<String> = row.get("schema_owner");
                let comment: Option<String> = row.get("schema_comment");
                let acl: Option<Vec<String>> = row.get("schema_acl");
                let mut sch = Schema::new(schema_name, raw_schema_name, comment);
                sch.owner = owner.unwrap_or_default();
                sch.acl = acl.unwrap_or_default();
                sch.hash();
                println!(" - {}", sch.name);
                self.schemas.push(sch);
            }
        }
        Ok(())
    }

    // ---------------------------------------------------------------
    // Standalone (static) fetch helpers used by the parallelised fill
    // ---------------------------------------------------------------

    async fn fetch_extensions_standalone(
        pool: &PgPool,
        schema_filter: &str,
    ) -> Result<Vec<Extension>, Error> {
        let query = format!(
            "
            select
                quote_ident(n.nspname) as nspname,
                quote_ident(e.extname) as extname,
                e.extversion,
                quote_ident(r.rolname) as extowner
            from
                pg_extension e
                join pg_namespace n on e.extnamespace = n.oid
                left join pg_roles r on r.oid = e.extowner
            where
                (n.nspname in {} or n.nspname = 'public')",
            schema_filter
        );

        let rows = sqlx::query(query.as_str())
            .fetch_all(pool)
            .await
            .map_err(|e| Error::other(format!("Failed to fetch extensions: {e}.")))?;

        let mut extensions = Vec::new();
        if rows.is_empty() {
            println!("No extensions found.");
        } else {
            println!("Extensions found:");
            for row in rows {
                let ext = Extension {
                    name: row.get("extname"),
                    version: row.get("extversion"),
                    schema: row.get("nspname"),
                    owner: row.get::<Option<String>, _>("extowner").unwrap_or_default(),
                };
                println!(
                    " - {} (version: {}, namespace: {})",
                    ext.name, ext.version, ext.schema
                );
                extensions.push(ext);
            }
        }
        Ok(extensions)
    }

    async fn fetch_types_standalone(
        pool: &PgPool,
        schema_filter: &str,
        types: &mut Vec<PgType>,
    ) -> Result<(), Error> {
        let composite_attributes_rows = sqlx::query(
            format!(
                "select
                    t.oid as type_oid,
                    a.attname,
                    pg_catalog.format_type(a.atttypid, a.atttypmod) as data_type
                 from pg_type t
                 join pg_namespace n on t.typnamespace = n.oid
                 join pg_class c on c.oid = t.typrelid
                 join pg_attribute a on a.attrelid = c.oid
                 where
                    n.nspname in {}
                    and t.typtype = 'c'
                    and c.relkind = 'c'
                    and t.typisdefined = true
                    and a.attnum > 0
                    and a.attisdropped = false
                    and not exists (
                        select 1 from pg_depend ext_dep
                        where ext_dep.objid = t.oid
                        and ext_dep.deptype = 'e'
                    )
                 order by t.oid, a.attnum",
                schema_filter
            )
            .as_str(),
        )
        .fetch_all(pool)
        .await
        .map_err(|e| Error::other(format!("Failed to fetch composite type attributes: {e}.")))?;

        let mut composite_attributes_map: HashMap<Oid, Vec<CompositeAttribute>> = HashMap::new();
        for row in composite_attributes_rows {
            let type_oid: Oid = row.get("type_oid");
            let attribute = CompositeAttribute {
                name: row.get("attname"),
                data_type: row.get("data_type"),
            };
            composite_attributes_map
                .entry(type_oid)
                .or_default()
                .push(attribute);
        }

        let rows = sqlx::query(
            format!(
                "select 
                quote_ident(n.nspname) as nspname,
                t.oid as type_oid,
                quote_ident(t.typname) as typname,
                t.typnamespace,
                t.typowner,
                quote_ident(owner_role.rolname) as typowner_name,
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
                left join pg_class c on c.oid = t.typrelid
                left join pg_roles owner_role on owner_role.oid = t.typowner
                left join pg_description d on d.objoid = t.oid
                    and d.classoid = 'pg_type'::regclass
                    and d.objsubid = 0
            where 
                n.nspname in {} 
                and (
                    t.typtype in ('d', 'e', 'r', 'm')
                    or (t.typtype = 'c' and c.relkind = 'c')
                )
                and t.typisdefined = true
                and not exists (
                    select 1 from pg_depend ext_dep
                    where ext_dep.objid = t.oid
                    and ext_dep.deptype = 'e'
                )",
                schema_filter
            )
            .as_str(),
        )
        .fetch_all(pool)
        .await
        .map_err(|e| Error::other(format!("Failed to fetch user-defined types: {e}.")))?;

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
                    owner: row
                        .get::<Option<String>, _>("typowner_name")
                        .unwrap_or_default(),
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
                    composite_attributes: composite_attributes_map
                        .remove(&row.get::<Oid, _>("type_oid"))
                        .unwrap_or_default(),
                    hash: None,
                };
                pgtype.hash();
                println!(
                    " - {} (namespace: {}, hash: {})",
                    pgtype.typname,
                    pgtype.schema,
                    pgtype.hash.as_deref().unwrap_or("None")
                );
                types.push(pgtype);
            }
        }
        Ok(())
    }

    async fn fetch_domain_constraints_standalone(
        pool: &PgPool,
        schema_filter: &str,
        types: &mut [PgType],
    ) -> Result<(), Error> {
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
              and n.nspname in {}
              and not exists (
                  select 1 from pg_depend ext_dep
                  where ext_dep.objid = c.contypid
                  and ext_dep.deptype = 'e'
              )
            order by c.contypid, c.conname",
            schema_filter
        );

        let rows = sqlx::query(query.as_str())
            .fetch_all(pool)
            .await
            .map_err(|e| Error::other(format!("Failed to fetch domain constraints: {e}.")))?;

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
            println!(" - {} on type {}", constraint.name, type_oid.0);
            constraints_by_type
                .entry(type_oid.0)
                .or_default()
                .push(constraint);
        }

        for pg_type in types.iter_mut() {
            if let Some(mut constraints) = constraints_by_type.remove(&pg_type.oid.0) {
                constraints.sort_by(|a, b| a.name.cmp(&b.name));
                pg_type.domain_constraints = constraints;
            }
        }

        Ok(())
    }

    async fn fetch_enums_standalone(
        pool: &PgPool,
        types: &mut [PgType],
        enums: &mut Vec<PgEnum>,
    ) -> Result<(), Error> {
        let rows = sqlx::query("select e.* from pg_enum e where not exists (select 1 from pg_depend ext_dep where ext_dep.objid = e.enumtypid and ext_dep.deptype = 'e')")
            .fetch_all(pool)
            .await
            .map_err(|e| Error::other(format!("Failed to fetch enums: {e}.")))?;

        if rows.is_empty() {
            println!("No enums found.");
        } else {
            println!("Enums found:");
            for row in &rows {
                let pgenum = PgEnum {
                    oid: row.get("oid"),
                    enumtypid: row.get("enumtypid"),
                    enumsortorder: row.get("enumsortorder"),
                    enumlabel: row.get("enumlabel"),
                };
                println!(
                    " - enumtypid {} (label: {})",
                    pgenum.enumtypid.0, pgenum.enumlabel
                );
                enums.push(pgenum);
            }

            let mut labels_by_type: HashMap<u32, Vec<(f32, String)>> = HashMap::new();
            for enum_value in enums.iter() {
                labels_by_type
                    .entry(enum_value.enumtypid.0)
                    .or_default()
                    .push((enum_value.enumsortorder, enum_value.enumlabel.clone()));
            }

            for (type_oid, mut labels) in labels_by_type {
                labels.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(Ordering::Equal));

                if let Some(pg_type) = types.iter_mut().find(|t| t.oid.0 == type_oid) {
                    pg_type.enum_labels = labels.into_iter().map(|(_, label)| label).collect();
                }
            }
        }
        Ok(())
    }

    async fn fetch_sequences_standalone(
        pool: &PgPool,
        schema_filter: &str,
    ) -> Result<Vec<Sequence>, Error> {
        let query = format!(
            "
            select
                quote_ident(seq.schemaname) as schemaname,
                quote_ident(seq.sequencename) as sequencename,
                quote_ident(seq.sequenceowner) as sequenceowner,
                seq.data_type::varchar as sequencedatatype,
                seq.start_value,
                seq.min_value,
                seq.max_value,
                seq.increment_by,
                seq.cycle,
                seq.cache_size,
                seq.last_value,
                quote_ident(owner_ns.nspname) as owned_by_schema,
                quote_ident(owner_table.relname) as owned_by_table,
                quote_ident(owner_attr.attname) as owned_by_column,
                dep.deptype::text as dependency_type,
                seq_desc.description as seq_comment,
                seq_class.relacl::text[] as seq_acl
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
                seq.schemaname in {}
                and not exists (
                    select 1 from pg_depend ext_dep
                    where ext_dep.objid = seq_class.oid
                    and ext_dep.deptype = 'e'
                )",
            schema_filter
        );

        let rows = sqlx::query(query.as_str())
            .fetch_all(pool)
            .await
            .map_err(|e| Error::other(format!("Failed to fetch sequences: {e}.")))?;

        let mut sequences = Vec::new();
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
                    acl: row
                        .get::<Option<Vec<String>>, _>("seq_acl")
                        .unwrap_or_default(),
                };
                if let Some(deptype) = row.get::<Option<String>, _>("dependency_type")
                    && deptype == "i"
                {
                    seq.is_identity = true;
                }
                seq.hash();
                println!(
                    " - name {} (type: {}, hash: {})",
                    seq.name,
                    seq.data_type,
                    seq.hash.as_deref().unwrap_or("None")
                );
                sequences.push(seq);
            }
        }
        Ok(sequences)
    }

    async fn fetch_routines_standalone(
        pool: &PgPool,
        schema_filter: &str,
    ) -> Result<Vec<Routine>, Error> {
        let query = format!(
            "select
                quote_ident(n.nspname) as nspname,
                r.oid,
                quote_ident(r.proname) as proname,
                l.lanname as prolang,
                case r.prokind
                    when 'f' then 'function'
                    when 'p' then 'procedure'
                    when 'a' then 'aggregate'
                    when 'w' then 'window'
                end as prokind,
                pg_get_function_result(r.oid) as prorettype,
                pg_get_function_identity_arguments(r.oid) as proarguments,
                pg_get_expr(r.proargdefaults, 0) as proargdefaults,
                quote_ident(owner_role.rolname) as owner_name,
                r.prosrc,
                d.description as routine_comment,
                r.provolatile::text as provolatile,
                r.proisstrict,
                r.proleakproof,
                r.proparallel::text as proparallel,
                r.prosecdef,
                r.proacl::text[] as routine_acl,
                agg.aggtransfn::regproc::text as agg_sfunc,
                format_type(agg.aggtranstype, null) as agg_stype,
                agg.aggtransspace as agg_sspace,
                case when agg.aggfinalfn != 0 then agg.aggfinalfn::regproc::text end as agg_finalfunc,
                agg.aggfinalextra as agg_finalfunc_extra,
                agg.aggfinalmodify::text as agg_finalfunc_modify,
                case when agg.aggcombinefn != 0 then agg.aggcombinefn::regproc::text end as agg_combinefunc,
                case when agg.aggserialfn != 0 then agg.aggserialfn::regproc::text end as agg_serialfunc,
                case when agg.aggdeserialfn != 0 then agg.aggdeserialfn::regproc::text end as agg_deserialfunc,
                agg.agginitval as agg_initcond,
                case when agg.aggmtransfn != 0 then agg.aggmtransfn::regproc::text end as agg_msfunc,
                case when agg.aggminvtransfn != 0 then agg.aggminvtransfn::regproc::text end as agg_minvfunc,
                case when agg.aggmtransfn != 0 then format_type(agg.aggmtranstype, null) end as agg_mstype,
                agg.aggmtransspace as agg_msspace,
                case when agg.aggmfinalfn != 0 then agg.aggmfinalfn::regproc::text end as agg_mfinalfunc,
                agg.aggmfinalextra as agg_mfinalfunc_extra,
                agg.aggmfinalmodify::text as agg_mfinalfunc_modify,
                agg.aggminitval as agg_minitcond,
                case when agg.aggsortop != 0 then agg.aggsortop::regoper::text end as agg_sortop,
                agg.aggkind::text as agg_kind,
                agg.aggnumdirectargs as agg_numdirectargs
            from
                pg_proc r
                join pg_namespace n on r.pronamespace = n.oid
                join pg_language l on r.prolang = l.oid
                left join pg_roles owner_role on owner_role.oid = r.proowner
                left join pg_description d on d.objoid = r.oid and d.classoid = 'pg_proc'::regclass and d.objsubid = 0
                left join pg_aggregate agg on agg.aggfnoid = r.oid
            where
                n.nspname in {}
                and n.nspname not in ('pg_catalog', 'information_schema')
                and (l.lanname not in ('c', 'internal') or r.prokind in ('a', 'w'))
                and r.prokind in ('f', 'p', 'a', 'w')
                and not exists (
                    select 1 from pg_depend ext_dep
                    where ext_dep.objid = r.oid
                    and ext_dep.deptype = 'e'
                );",
            schema_filter
        );

        let rows = sqlx::query(query.as_str())
            .fetch_all(pool)
            .await
            .map_err(|e| Error::other(format!("Failed to fetch routines: {e}.")))?;

        let mut routines = Vec::new();
        if rows.is_empty() {
            println!("No routines found.");
        } else {
            println!("Routines found:");
            for row in rows {
                let prokind: String = row.get("prokind");
                let is_aggregate = prokind == "aggregate";

                let volatility = match row.get::<String, _>("provolatile").as_str() {
                    "i" => "immutable".to_string(),
                    "s" => "stable".to_string(),
                    _ => "volatile".to_string(),
                };
                let parallel = match row.get::<String, _>("proparallel").as_str() {
                    "s" => "safe".to_string(),
                    "r" => "restricted".to_string(),
                    _ => "unsafe".to_string(),
                };

                let aggregate_info = if is_aggregate {
                    let agg_kind_str: Option<String> = row.get("agg_kind");
                    let agg_kind = agg_kind_str
                        .as_deref()
                        .and_then(|s| s.chars().next())
                        .unwrap_or('n');
                    let finalfunc_modify: Option<String> = row.get("agg_finalfunc_modify");
                    let mfinalfunc_modify: Option<String> = row.get("agg_mfinalfunc_modify");

                    Some(crate::dump::routine::AggregateInfo {
                        sfunc: row
                            .get::<Option<String>, _>("agg_sfunc")
                            .unwrap_or_default(),
                        stype: row
                            .get::<Option<String>, _>("agg_stype")
                            .unwrap_or_default(),
                        sspace: row.get::<Option<i32>, _>("agg_sspace"),
                        finalfunc: row.get("agg_finalfunc"),
                        finalfunc_extra: row
                            .get::<Option<bool>, _>("agg_finalfunc_extra")
                            .unwrap_or(false),
                        finalfunc_modify: finalfunc_modify.filter(|v| v != "r"),
                        combinefunc: row.get("agg_combinefunc"),
                        serialfunc: row.get("agg_serialfunc"),
                        deserialfunc: row.get("agg_deserialfunc"),
                        initcond: row.get("agg_initcond"),
                        msfunc: row.get("agg_msfunc"),
                        minvfunc: row.get("agg_minvfunc"),
                        mstype: row.get("agg_mstype"),
                        msspace: row.get::<Option<i32>, _>("agg_msspace"),
                        mfinalfunc: row.get("agg_mfinalfunc"),
                        mfinalfunc_extra: row
                            .get::<Option<bool>, _>("agg_mfinalfunc_extra")
                            .unwrap_or(false),
                        mfinalfunc_modify: mfinalfunc_modify.filter(|v| v != "r"),
                        minitcond: row.get("agg_minitcond"),
                        sortop: row.get("agg_sortop"),
                        kind: agg_kind,
                        num_direct_args: row
                            .get::<Option<i16>, _>("agg_numdirectargs")
                            .unwrap_or(0),
                    })
                } else {
                    None
                };

                let mut routine = Routine {
                    schema: row.get("nspname"),
                    oid: row.get("oid"),
                    name: row.get("proname"),
                    lang: row.get("prolang"),
                    kind: prokind,
                    return_type: row
                        .get::<Option<String>, _>("prorettype")
                        .unwrap_or_else(|| "void".to_string()),
                    arguments: row.get("proarguments"),
                    arguments_defaults: row.get::<Option<String>, _>("proargdefaults"),
                    owner: row
                        .get::<Option<String>, _>("owner_name")
                        .unwrap_or_default(),
                    comment: row.get("routine_comment"),
                    source_code: crate::utils::string_extensions::normalize_line_endings(
                        row.get::<String, _>("prosrc"),
                    ),
                    volatility,
                    is_strict: row.get("proisstrict"),
                    is_leakproof: row.get("proleakproof"),
                    parallel,
                    security_definer: row.get("prosecdef"),
                    aggregate_info,
                    hash: None,
                    acl: row
                        .get::<Option<Vec<String>>, _>("routine_acl")
                        .unwrap_or_default(),
                };
                routine.hash();
                println!(
                    " - {} {}.{} (lang: {}, arguments: {}, hash: {})",
                    routine.kind,
                    routine.schema,
                    routine.name,
                    routine.lang,
                    routine.arguments,
                    routine.hash.as_deref().unwrap_or("None")
                );
                routines.push(routine);
            }
        }
        Ok(routines)
    }

    /// Fetch all tables with bounded-parallel fills.
    ///
    /// Tables are filled concurrently so that per-table sub-queries
    /// overlap, drastically reducing wall-clock time on remote connections.
    async fn fetch_tables_standalone(
        pool: &PgPool,
        schema_filter: &str,
        max_connections: u32,
    ) -> Result<Vec<Table>, Error> {
        // Check once whether the pg_get_tabledef extension function exists.
        let has_tabledef_fn =
            sqlx::query("select proname from pg_proc where proname = 'pg_get_tabledef';")
                .fetch_optional(pool)
                .await
                .unwrap_or(None)
                .is_some();

        let query = format!(
            "
                select
                    quote_ident(t.schemaname) as schemaname,
                    quote_ident(t.tablename) as tablename,
                    quote_ident(t.tableowner) as tableowner,
                    t.schemaname as raw_schema_name,
                    t.tablename as raw_table_name,
                    t.tablespace,
                    t.hasindexes,
                    t.hastriggers,
                    t.hasrules,
                    t.rowsecurity,
                    d.description as table_comment,
                    c.relacl::text[] as table_acl
                from pg_tables t
                left join pg_class c on c.relname = t.tablename
                    and c.relkind in ('r','p')
                    and c.relnamespace = (select oid from pg_namespace where nspname = t.schemaname)
                left join pg_description d on d.objoid = c.oid and d.objsubid = 0
                where 
                    t.schemaname not in ('pg_catalog', 'information_schema') 
                    and t.schemaname in {} 
                    and t.tablename not like 'pg_%'
                    and not exists (
                        select 1 from pg_depend ext_dep
                        where ext_dep.objid = c.oid
                        and ext_dep.deptype = 'e'
                    );",
            schema_filter
        );

        let rows = sqlx::query(query.as_str())
            .fetch_all(pool)
            .await
            .map_err(|e| Error::other(format!("Failed to fetch tables: {e}.")))?;

        if rows.is_empty() {
            println!("No tables found.");
            return Ok(Vec::new());
        }

        println!("Tables found:");

        // Build lightweight table structs from the catalog rows.
        let mut shell_tables: Vec<Table> = Vec::with_capacity(rows.len());
        for row in rows {
            shell_tables.push(Table {
                schema: row.get("schemaname"),
                name: row.get("tablename"),
                raw_schema: row.get("raw_schema_name"),
                raw_name: row.get("raw_table_name"),
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
                policies: Vec::new(),
                definition: None,
                partition_key: None,
                partition_of: None,
                partition_bound: None,
                comment: row.get("table_comment"),
                hash: None,
                acl: row
                    .get::<Option<Vec<String>>, _>("table_acl")
                    .unwrap_or_default(),
            });
        }

        // Fill all tables concurrently, reserving FILL_SIBLING_BRANCH_COUNT
        // connections for the sibling branches (extensions, sequences, routines,
        // types/enums, views) that run in parallel via tokio::try_join! in fill().
        let pool_ref = pool;
        let tables: Vec<Result<Table, Error>> = stream::iter(shell_tables)
            .map(|mut table| async move {
                table.fill(pool_ref, has_tabledef_fn).await.map_err(|e| {
                    Error::other(format!("Failed to fill table {}: {}.", table.name, e))
                })?;
                table.hash();
                println!(
                    " - {}.{} (hash: {})",
                    table.schema,
                    table.name,
                    table.hash.as_deref().unwrap_or("None")
                );
                Ok(table)
            })
            .buffer_unordered(
                max_connections
                    .saturating_sub(FILL_SIBLING_BRANCH_COUNT)
                    .max(1) as usize,
            )
            .collect()
            .await;

        let mut result = Vec::with_capacity(tables.len());
        for t in tables {
            result.push(t?);
        }
        // Re-sort to ensure deterministic output regardless of completion order.
        result.sort_by(|a, b| (&a.schema, &a.name).cmp(&(&b.schema, &b.name)));
        Ok(result)
    }

    async fn fetch_views_standalone(
        pool: &PgPool,
        schema_filter: &str,
    ) -> Result<Vec<View>, Error> {
        // Fetch regular and materialized views concurrently.
        let regular_query = format!(
            "select 
                    quote_ident(v.table_schema) as table_schema,
                    quote_ident(v.table_name) as table_name,
                    v.view_definition,
                    quote_ident(pv.viewowner) as view_owner,
                    array_agg(distinct vtu.table_schema || '.' || vtu.table_name) as table_relation,
                    d.description as view_comment,
                    (select cc.relacl::text[] from pg_class cc where cc.oid = c.oid) as view_acl
            from information_schema.views v
            join information_schema.view_table_usage vtu on v.table_name = vtu.view_name and v.table_schema = vtu.view_schema
            left join pg_views pv on pv.schemaname = v.table_schema and pv.viewname = v.table_name
            left join pg_class c on c.relname = v.table_name and c.relnamespace = (select oid from pg_namespace where nspname = v.table_schema)
            left join pg_description d on d.objoid = c.oid and d.objsubid = 0
            where
                v.table_schema not in ('pg_catalog', 'information_schema')
                and v.table_schema in {}
                and not exists (
                    select 1 from pg_depend ext_dep
                    where ext_dep.objid = c.oid
                    and ext_dep.deptype = 'e'
                )
            group by v.table_schema, v.table_name, v.view_definition, pv.viewowner, d.description, c.oid;",
            schema_filter
        );

        let mat_query = format!(
            "select
                    mv.schemaname as table_schema,
                    mv.matviewname as table_name,
                    mv.definition as view_definition,
                    mv.matviewowner as view_owner,
                    array(
                        select distinct n.nspname || '.' || dc.relname
                        from pg_depend dep
                        join pg_class dc on dc.oid = dep.refobjid
                        join pg_namespace n on n.oid = dc.relnamespace
                        where dep.objid = c.oid
                          and dep.deptype = 'n'
                          and dc.relkind in ('r', 'v', 'm')
                    ) as table_relation,
                    d.description as view_comment,
                    c.relacl::text[] as view_acl
            from pg_matviews mv
            join pg_class c on c.relname = mv.matviewname
                and c.relnamespace = (select oid from pg_namespace where nspname = mv.schemaname)
            left join pg_description d on d.objoid = c.oid and d.objsubid = 0
            where mv.schemaname not in ('pg_catalog', 'information_schema')
                and mv.schemaname in {}
                and not exists (
                    select 1 from pg_depend ext_dep
                    where ext_dep.objid = c.oid
                    and ext_dep.deptype = 'e'
                );",
            schema_filter
        );

        let (regular_rows, mat_rows) = tokio::try_join!(
            async {
                sqlx::query(regular_query.as_str())
                    .fetch_all(pool)
                    .await
                    .map_err(|e| Error::other(format!("Failed to fetch views: {e}.")))
            },
            async {
                sqlx::query(mat_query.as_str())
                    .fetch_all(pool)
                    .await
                    .map_err(|e| Error::other(format!("Failed to fetch materialized views: {e}.")))
            },
        )?;

        let mut views = Vec::new();

        if regular_rows.is_empty() {
            println!("No views found.");
        } else {
            println!("Views found:");
            for row in regular_rows {
                let mut view = View {
                    schema: row.get("table_schema"),
                    name: row.get("table_name"),
                    definition: row.get("view_definition"),
                    table_relation: row.get("table_relation"),
                    owner: row
                        .get::<Option<String>, _>("view_owner")
                        .unwrap_or_default(),
                    comment: row.get("view_comment"),
                    is_materialized: false,
                    hash: None,
                    acl: row
                        .get::<Option<Vec<String>>, _>("view_acl")
                        .unwrap_or_default(),
                };
                view.hash();
                println!(
                    " - {}.{} (hash: {})",
                    view.schema,
                    view.name,
                    view.hash.as_deref().unwrap_or("None")
                );
                views.push(view);
            }
        }

        if mat_rows.is_empty() {
            println!("No materialized views found.");
        } else {
            println!("Materialized views found:");
            for row in mat_rows {
                let mut view = View {
                    schema: row.get("table_schema"),
                    name: row.get("table_name"),
                    definition: row.get("view_definition"),
                    table_relation: row.get("table_relation"),
                    owner: row
                        .get::<Option<String>, _>("view_owner")
                        .unwrap_or_default(),
                    comment: row.get("view_comment"),
                    is_materialized: true,
                    hash: None,
                    acl: row
                        .get::<Option<Vec<String>>, _>("view_acl")
                        .unwrap_or_default(),
                };
                view.hash();
                println!(
                    " - {}.{} (materialized, hash: {})",
                    view.schema,
                    view.name,
                    view.hash.as_deref().unwrap_or("None")
                );
                views.push(view);
            }
        }

        Ok(views)
    }

    // Read a dump from a file and deserialize it.
    pub async fn read_from_file(file: &str) -> Result<Self, Error> {
        let file = File::open(file)?;
        let mut zip = zip::ZipArchive::new(file)?;
        let mut dump_file = zip.by_name("dump.io")?;
        let mut serialized_data = String::new();
        dump_file.read_to_string(&mut serialized_data)?;

        let mut dump: Dump = serde_json::from_str(&serialized_data)
            .map_err(|e| Error::other(format!("Failed to deserialize dump: {e}.")))?;

        // Normalize CRLF -> LF in routine source code so that hashes are
        // consistent regardless of the line-ending style stored in the dump.
        for routine in &mut dump.routines {
            if routine.source_code.contains("\r\n") {
                routine.source_code = crate::utils::string_extensions::normalize_line_endings(
                    std::mem::take(&mut routine.source_code),
                );
                routine.hash();
            }
        }

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

    /// Connect to the database and fill the dump without saving to a file.
    pub async fn inspect(&mut self, max_connections: u32) -> Result<(), Error> {
        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .connect(self.configuration.get_connection_string().as_str())
            .await
            .map_err(|e| {
                Error::other(format!(
                    "Failed to connect to database ({}): {}.",
                    self.configuration.get_masked_connection_string(),
                    e
                ))
            })?;

        self.fill(&pool, max_connections).await?;

        pool.close().await;
        Ok(())
    }

    /// Generate a SQL script that drops all objects found in this dump.
    /// The drop order respects dependencies: views (topologically sorted by
    /// table_relation), tables (with foreign keys dropped first), routines,
    /// sequences, types/enums, extensions, schemas.
    pub fn generate_clear_script(
        &self,
        use_single_transaction: bool,
        use_comments: bool,
        use_cascade: bool,
    ) -> String {
        use crate::utils::string_extensions::StringExt;

        let cascade_suffix = if use_cascade { " cascade" } else { "" };
        let mut script = String::new();

        script.push_str("/*\n");
        script.append_block(" Script generated by PostgreSQL Comparer (clear command)");
        script.push_str(&format!(" Database: {}\n", self.configuration.database));
        script.push_str(&format!(" Schema(s): {}\n", self.configuration.scheme));
        script.push_str(&format!("\n{}\n", self.get_info()));
        script.append_block("*/");

        if use_single_transaction {
            script.append_block("begin;");
        }

        // 1. Drop views in dependency-safe order (topological sort on table_relation).
        //    Views that depend on other views are dropped first.  Tie-breaker:
        //    materialized views before regular views, then alphabetical by schema.name.
        if !self.views.is_empty() {
            use std::collections::HashSet;

            let n = self.views.len();

            // Qualified name for each view.
            let view_keys: Vec<String> = self
                .views
                .iter()
                .map(|v| format!("{}.{}", v.schema, v.name))
                .collect();

            // Map qualified name → index (only views, not tables).
            let key_to_idx: HashMap<&str, usize> = view_keys
                .iter()
                .enumerate()
                .map(|(i, k)| (k.as_str(), i))
                .collect();

            // Build the drop-order graph.
            // Edge i → j means view i depends on view j, so i must be dropped first.
            // in_degree[j] = number of views that must be dropped before j.
            let mut in_degree = vec![0usize; n];
            let mut edges: Vec<Vec<usize>> = vec![Vec::new(); n];

            for (i, view) in self.views.iter().enumerate() {
                for rel in &view.table_relation {
                    if let Some(&j) = key_to_idx.get(rel.as_str())
                        && j != i
                    {
                        edges[i].push(j);
                        in_degree[j] += 1;
                    }
                }
            }

            // Kahn's algorithm with a deterministic tie-breaker.
            let sort_key = |idx: &usize| {
                let v = &self.views[*idx];
                // materialized first (false < true, so negate), then alphabetical
                (!v.is_materialized, view_keys[*idx].clone())
            };

            let mut ready: Vec<usize> = (0..n).filter(|i| in_degree[*i] == 0).collect();
            ready.sort_by_key(sort_key);

            let mut drop_order: Vec<usize> = Vec::with_capacity(n);
            while let Some(idx) = ready.first().copied() {
                ready.remove(0);
                drop_order.push(idx);

                for &j in &edges[idx] {
                    in_degree[j] -= 1;
                    if in_degree[j] == 0 {
                        ready.push(j);
                        ready.sort_by_key(sort_key);
                    }
                }
            }

            // If cycles exist, append remaining views in stable order.
            if drop_order.len() < n {
                let processed: HashSet<usize> = drop_order.iter().copied().collect();
                let mut remaining: Vec<usize> = (0..n).filter(|i| !processed.contains(i)).collect();
                remaining.sort_by_key(sort_key);
                drop_order.extend(remaining);
            }

            if use_comments {
                script.append_block("\n/* ---> Drop Views --------------- */");
            }
            for &idx in &drop_order {
                let view = &self.views[idx];
                if use_comments {
                    if view.is_materialized {
                        script.push_str(&format!(
                            "/* Drop materialized view: {}.{} */\n",
                            view.schema, view.name
                        ));
                    } else {
                        script
                            .push_str(&format!("/* Drop view: {}.{} */\n", view.schema, view.name));
                    }
                }
                script.push_str(
                    &format!(
                        "drop {} if exists {}.{}{cascade_suffix};",
                        view.view_keyword(),
                        view.schema,
                        view.name
                    )
                    .with_empty_lines(),
                );
            }
        }

        // 2. Drop tables (foreign keys first, then tables themselves)
        if !self.tables.is_empty() {
            if use_comments {
                script.append_block("\n/* ---> Drop Tables --------------- */");
            }

            // Drop foreign key constraints first to avoid dependency issues
            for table in &self.tables {
                for constraint in &table.constraints {
                    if constraint.constraint_type.to_lowercase() == "foreign key" {
                        if use_comments {
                            script.push_str(&format!(
                                "/* Drop foreign key: {}.{}.{} */\n",
                                constraint.schema, constraint.table_name, constraint.name
                            ));
                        }
                        script.push_str(
                            &format!(
                                "alter table {}.{} drop constraint if exists {};",
                                constraint.schema, constraint.table_name, constraint.name
                            )
                            .with_empty_lines(),
                        );
                    }
                }
            }

            // Now drop the tables
            for table in &self.tables {
                if use_comments {
                    script.push_str(&format!(
                        "/* Drop table: {}.{} */\n",
                        table.schema, table.name
                    ));
                }
                script.push_str(
                    &format!(
                        "drop table if exists {}.{}{cascade_suffix};",
                        table.schema, table.name
                    )
                    .with_empty_lines(),
                );
            }
        }

        // 3. Drop routines (functions, procedures, aggregates)
        if !self.routines.is_empty() {
            if use_comments {
                script.append_block("\n/* ---> Drop Routines --------------- */");
            }
            for routine in &self.routines {
                if use_comments {
                    script.push_str(&format!(
                        "/* Drop {}: {}.{} */\n",
                        routine.kind, routine.schema, routine.name
                    ));
                }
                let drop_kind = match routine.kind.to_lowercase().as_str() {
                    "window" => "function",
                    "procedure" => "procedure",
                    "aggregate" => "aggregate",
                    _ => "function",
                };
                let args =
                    if routine.kind.to_lowercase() == "aggregate" && routine.arguments.is_empty() {
                        "*".to_string()
                    } else {
                        routine.arguments.clone()
                    };
                script.push_str(
                    &format!(
                        "drop {} if exists {}.{} ({}){cascade_suffix};",
                        drop_kind, routine.schema, routine.name, args
                    )
                    .with_empty_lines(),
                );
            }
        }

        // 4. Drop sequences
        if !self.sequences.is_empty() {
            if use_comments {
                script.append_block("\n/* ---> Drop Sequences --------------- */");
            }
            for sequence in &self.sequences {
                if use_comments {
                    script.push_str(&format!(
                        "/* Drop sequence: {}.{} */\n",
                        sequence.schema, sequence.name
                    ));
                }
                script.push_str(
                    &format!(
                        "drop sequence if exists {}.{}{cascade_suffix};",
                        sequence.schema, sequence.name
                    )
                    .with_empty_lines(),
                );
            }
        }

        // 5. Drop types (includes enums, composites, domains, etc.)
        if !self.types.is_empty() {
            if use_comments {
                script.append_block("\n/* ---> Drop Types --------------- */");
            }
            for pg_type in &self.types {
                if use_comments {
                    script.push_str(&format!(
                        "/* Drop type: {}.{} */\n",
                        pg_type.schema, pg_type.typname
                    ));
                }
                script.push_str(
                    &format!(
                        "drop type if exists {}.{}{cascade_suffix};",
                        pg_type.schema, pg_type.typname
                    )
                    .with_empty_lines(),
                );
            }
        }

        // 6. Drop extensions
        if !self.extensions.is_empty() {
            if use_comments {
                script.append_block("\n/* ---> Drop Extensions --------------- */");
            }
            for ext in &self.extensions {
                if use_comments {
                    script.push_str(&format!("/* Drop extension: {} */\n", ext.name));
                }
                script.push_str(
                    &format!("drop extension if exists {}{cascade_suffix};", ext.name)
                        .with_empty_lines(),
                );
            }
        }

        // 7. Drop schemas (last, since everything else lives inside them)
        if !self.schemas.is_empty() {
            if use_comments {
                script.append_block("\n/* ---> Drop Schemas --------------- */");
            }
            for schema in &self.schemas {
                if use_comments {
                    script.push_str(&format!("/* Drop schema: {} */\n", schema.name));
                }
                script.push_str(
                    &format!("drop schema if exists {}{cascade_suffix};", schema.name)
                        .with_empty_lines(),
                );
            }
        }

        if use_single_transaction {
            script.append_block("\ncommit;");
        }

        script
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dump::extension::Extension;
    use crate::dump::routine::Routine;
    use crate::dump::schema::Schema;
    use crate::dump::sequence::Sequence;
    use crate::dump::table::Table;
    use crate::dump::table_constraint::TableConstraint;
    use crate::dump::view::View;
    use sqlx::postgres::types::Oid;

    fn empty_dump() -> Dump {
        Dump::new(DumpConfig {
            host: "localhost".to_string(),
            port: "5432".to_string(),
            user: "test".to_string(),
            password: "test".to_string(),
            database: "testdb".to_string(),
            scheme: "public".to_string(),
            ssl: false,
            file: String::new(),
        })
    }

    fn make_schema(name: &str) -> Schema {
        Schema::new(name.to_string(), name.to_string(), None)
    }

    fn make_extension(name: &str) -> Extension {
        Extension::new(name.to_string(), "1.0".to_string(), "public".to_string())
    }

    fn make_table(schema: &str, name: &str) -> Table {
        Table::new(
            schema.to_string(),
            name.to_string(),
            schema.to_string(),
            name.to_string(),
            String::new(),
            None,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            None,
        )
    }

    fn make_table_with_fk(schema: &str, name: &str, fk_name: &str) -> Table {
        let fk = TableConstraint {
            catalog: "postgres".to_string(),
            schema: schema.to_string(),
            name: fk_name.to_string(),
            table_name: name.to_string(),
            constraint_type: "FOREIGN KEY".to_string(),
            is_deferrable: false,
            initially_deferred: false,
            definition: None,
            coninhcount: 0,
        };
        Table::new(
            schema.to_string(),
            name.to_string(),
            schema.to_string(),
            name.to_string(),
            String::new(),
            None,
            Vec::new(),
            vec![fk],
            Vec::new(),
            Vec::new(),
            None,
        )
    }

    fn make_view(schema: &str, name: &str) -> View {
        View::new(
            name.to_string(),
            "select 1".to_string(),
            schema.to_string(),
            Vec::new(),
        )
    }

    fn make_view_with_deps(schema: &str, name: &str, deps: Vec<&str>) -> View {
        View::new(
            name.to_string(),
            "select 1".to_string(),
            schema.to_string(),
            deps.into_iter().map(String::from).collect(),
        )
    }

    fn make_materialized_view(schema: &str, name: &str) -> View {
        let mut view = make_view(schema, name);
        view.is_materialized = true;
        view.hash();
        view
    }

    fn make_sequence(schema: &str, name: &str) -> Sequence {
        Sequence::new(
            schema.to_string(),
            name.to_string(),
            String::new(),
            "bigint".to_string(),
            Some(1),
            Some(1),
            Some(i64::MAX),
            Some(1),
            false,
            Some(1),
            None,
            None,
            None,
            None,
        )
    }

    fn make_routine(schema: &str, name: &str) -> Routine {
        Routine::new(
            schema.to_string(),
            Oid(1),
            name.to_string(),
            "plpgsql".to_string(),
            "function".to_string(),
            "void".to_string(),
            String::new(),
            None,
            None,
            "BEGIN END".to_string(),
        )
    }

    fn make_pg_type(schema: &str, name: &str) -> PgType {
        PgType {
            oid: Oid(10000),
            schema: schema.to_string(),
            typname: name.to_string(),
            typnamespace: Oid(2200),
            typowner: Oid(10),
            owner: String::new(),
            typlen: -1,
            typbyval: false,
            typtype: b'c' as i8,
            typcategory: b'C' as i8,
            typispreferred: false,
            typisdefined: true,
            typdelim: b',' as i8,
            typrelid: None,
            typsubscript: None,
            typelem: None,
            typarray: None,
            typinput: "record_in".to_string(),
            typoutput: "record_out".to_string(),
            typreceive: None,
            typsend: None,
            typmodin: None,
            typmodout: None,
            typanalyze: None,
            typalign: b'i' as i8,
            typstorage: b'x' as i8,
            typnotnull: false,
            typbasetype: None,
            typtypmod: None,
            typndims: 0,
            typcollation: None,
            typdefault: None,
            formatted_basetype: None,
            enum_labels: Vec::new(),
            domain_constraints: Vec::new(),
            composite_attributes: Vec::new(),
            comment: None,
            hash: None,
        }
    }

    #[test]
    fn test_clear_script_empty_dump() {
        let dump = empty_dump();
        let script = dump.generate_clear_script(false, false, false);
        // Should only contain the header comment
        assert!(script.contains("clear command"));
        assert!(script.contains("testdb"));
        assert!(!script.contains("drop"));
    }

    #[test]
    fn test_clear_script_single_transaction() {
        let mut dump = empty_dump();
        dump.schemas.push(make_schema("public"));
        let script = dump.generate_clear_script(true, false, false);
        assert!(script.contains("begin;\n"));
        assert!(script.contains("commit;\n"));
    }

    #[test]
    fn test_clear_script_no_transaction() {
        let mut dump = empty_dump();
        dump.schemas.push(make_schema("public"));
        let script = dump.generate_clear_script(false, false, false);
        assert!(!script.contains("begin;"));
        assert!(!script.contains("commit;"));
    }

    #[test]
    fn test_clear_script_with_comments() {
        let mut dump = empty_dump();
        dump.schemas.push(make_schema("public"));
        dump.tables.push(make_table("public", "users"));
        let script = dump.generate_clear_script(false, true, false);
        assert!(script.contains("/* Drop schema: public */"));
        assert!(script.contains("/* Drop table: public.users */"));
        assert!(script.contains("/* ---> Drop Tables --------------- */"));
        assert!(script.contains("/* ---> Drop Schemas --------------- */"));
    }

    #[test]
    fn test_clear_script_without_comments() {
        let mut dump = empty_dump();
        dump.schemas.push(make_schema("public"));
        dump.tables.push(make_table("public", "users"));
        let script = dump.generate_clear_script(false, false, false);
        assert!(!script.contains("/* Drop schema:"));
        assert!(!script.contains("/* Drop table:"));
        // Drop statements should still be present (without cascade)
        assert!(script.contains("drop schema if exists public;"));
        assert!(script.contains("drop table if exists public.users;"));
        assert!(!script.contains("cascade"));
    }

    #[test]
    fn test_clear_script_drops_schemas() {
        let mut dump = empty_dump();
        dump.schemas.push(make_schema("public"));
        dump.schemas.push(make_schema("analytics"));
        let script = dump.generate_clear_script(false, false, false);
        assert!(script.contains("drop schema if exists public;"));
        assert!(script.contains("drop schema if exists analytics;"));
    }

    #[test]
    fn test_clear_script_drops_extensions() {
        let mut dump = empty_dump();
        dump.extensions.push(make_extension("\"uuid-ossp\""));
        dump.extensions.push(make_extension("pg_trgm"));
        let script = dump.generate_clear_script(false, false, false);
        assert!(script.contains("drop extension if exists \"uuid-ossp\";"));
        assert!(script.contains("drop extension if exists pg_trgm;"));
    }

    #[test]
    fn test_clear_script_drops_tables() {
        let mut dump = empty_dump();
        dump.tables.push(make_table("public", "users"));
        dump.tables.push(make_table("public", "orders"));
        let script = dump.generate_clear_script(false, false, false);
        assert!(script.contains("drop table if exists public.users;"));
        assert!(script.contains("drop table if exists public.orders;"));
    }

    #[test]
    fn test_clear_script_drops_foreign_keys_before_tables() {
        let mut dump = empty_dump();
        dump.tables
            .push(make_table_with_fk("public", "orders", "fk_orders_users"));
        let script = dump.generate_clear_script(false, false, false);
        let fk_pos = script
            .find("alter table public.orders drop constraint if exists fk_orders_users;")
            .expect("FK drop missing");
        let table_pos = script
            .find("drop table if exists public.orders;")
            .expect("table drop missing");
        assert!(fk_pos < table_pos, "FK should be dropped before the table");
    }

    #[test]
    fn test_clear_script_drops_views() {
        let mut dump = empty_dump();
        dump.views.push(make_view("public", "active_users"));
        let script = dump.generate_clear_script(false, false, false);
        assert!(script.contains("drop view if exists public.active_users;"));
    }

    #[test]
    fn test_clear_script_drops_materialized_views() {
        let mut dump = empty_dump();
        dump.views
            .push(make_materialized_view("public", "daily_stats"));
        let script = dump.generate_clear_script(false, false, false);
        assert!(script.contains("drop materialized view if exists public.daily_stats;"));
    }

    #[test]
    fn test_clear_script_drops_materialized_before_regular_views() {
        let mut dump = empty_dump();
        dump.views.push(make_view("public", "regular_view"));
        dump.views
            .push(make_materialized_view("public", "mat_view"));
        let script = dump.generate_clear_script(false, false, false);
        let mat_pos = script
            .find("drop materialized view if exists public.mat_view;")
            .expect("materialized view drop missing");
        let reg_pos = script
            .find("drop view if exists public.regular_view;")
            .expect("regular view drop missing");
        assert!(
            mat_pos < reg_pos,
            "Materialized views should be dropped before regular views"
        );
    }

    #[test]
    fn test_clear_script_drops_sequences() {
        let mut dump = empty_dump();
        dump.sequences.push(make_sequence("public", "users_id_seq"));
        let script = dump.generate_clear_script(false, false, false);
        assert!(script.contains("drop sequence if exists public.users_id_seq;"));
    }

    #[test]
    fn test_clear_script_drops_routines() {
        let mut dump = empty_dump();
        dump.routines.push(make_routine("public", "my_func"));
        let script = dump.generate_clear_script(false, false, false);
        assert!(script.contains("drop function if exists public.my_func ();"));
    }

    #[test]
    fn test_clear_script_drops_types() {
        let mut dump = empty_dump();
        dump.types.push(make_pg_type("public", "my_composite"));
        let script = dump.generate_clear_script(false, false, false);
        assert!(script.contains("drop type if exists public.my_composite;"));
    }

    #[test]
    fn test_clear_script_dependency_order() {
        let mut dump = empty_dump();
        dump.schemas.push(make_schema("public"));
        dump.extensions.push(make_extension("pg_trgm"));
        dump.types.push(make_pg_type("public", "my_type"));
        dump.sequences.push(make_sequence("public", "seq1"));
        dump.routines.push(make_routine("public", "fn1"));
        dump.tables.push(make_table("public", "tbl1"));
        dump.views.push(make_view("public", "v1"));

        let script = dump.generate_clear_script(false, false, false);

        let view_pos = script.find("drop view if exists public.v1;").unwrap();
        let table_pos = script.find("drop table if exists public.tbl1;").unwrap();
        let routine_pos = script
            .find("drop function if exists public.fn1 ();")
            .unwrap();
        let seq_pos = script.find("drop sequence if exists public.seq1;").unwrap();
        let type_pos = script.find("drop type if exists public.my_type;").unwrap();
        let ext_pos = script.find("drop extension if exists pg_trgm;").unwrap();
        let schema_pos = script.find("drop schema if exists public;").unwrap();

        assert!(view_pos < table_pos, "views before tables");
        assert!(table_pos < routine_pos, "tables before routines");
        assert!(routine_pos < seq_pos, "routines before sequences");
        assert!(seq_pos < type_pos, "sequences before types");
        assert!(type_pos < ext_pos, "types before extensions");
        assert!(ext_pos < schema_pos, "extensions before schemas");
    }

    #[test]
    fn test_clear_script_header_contains_db_info() {
        let dump = empty_dump();
        let script = dump.generate_clear_script(false, false, false);
        assert!(script.contains("Database: testdb"));
        assert!(script.contains("Schema(s): public"));
        assert!(script.contains("Dump Info:"));
    }

    #[test]
    fn test_clear_script_no_cascade_by_default() {
        let mut dump = empty_dump();
        dump.schemas.push(make_schema("public"));
        dump.extensions.push(make_extension("pg_trgm"));
        dump.types.push(make_pg_type("public", "my_type"));
        dump.sequences.push(make_sequence("public", "seq1"));
        dump.tables.push(make_table("public", "tbl1"));
        dump.views.push(make_view("public", "v1"));

        let script = dump.generate_clear_script(false, false, false);

        assert!(
            !script.contains("cascade"),
            "default should not use CASCADE"
        );
        assert!(script.contains("drop view if exists public.v1;"));
        assert!(script.contains("drop table if exists public.tbl1;"));
        assert!(script.contains("drop sequence if exists public.seq1;"));
        assert!(script.contains("drop type if exists public.my_type;"));
        assert!(script.contains("drop extension if exists pg_trgm;"));
        assert!(script.contains("drop schema if exists public;"));
    }

    #[test]
    fn test_clear_script_cascade_when_enabled() {
        let mut dump = empty_dump();
        dump.schemas.push(make_schema("public"));
        dump.extensions.push(make_extension("pg_trgm"));
        dump.types.push(make_pg_type("public", "my_type"));
        dump.sequences.push(make_sequence("public", "seq1"));
        dump.routines.push(make_routine("public", "fn1"));
        dump.tables.push(make_table("public", "tbl1"));
        dump.views.push(make_view("public", "v1"));

        let script = dump.generate_clear_script(false, false, true);

        assert!(script.contains("drop view if exists public.v1 cascade;"));
        assert!(script.contains("drop table if exists public.tbl1 cascade;"));
        assert!(script.contains("drop function if exists public.fn1 () cascade;"));
        assert!(script.contains("drop sequence if exists public.seq1 cascade;"));
        assert!(script.contains("drop type if exists public.my_type cascade;"));
        assert!(script.contains("drop extension if exists pg_trgm cascade;"));
        assert!(script.contains("drop schema if exists public cascade;"));
    }

    #[test]
    fn test_clear_script_full_integration() {
        let mut dump = empty_dump();
        dump.schemas.push(make_schema("app"));
        dump.extensions.push(make_extension("\"uuid-ossp\""));
        dump.tables
            .push(make_table_with_fk("app", "orders", "fk_user"));
        dump.tables.push(make_table("app", "users"));
        dump.views.push(make_view("app", "order_summary"));
        dump.views
            .push(make_materialized_view("app", "daily_report"));
        dump.sequences.push(make_sequence("app", "orders_id_seq"));
        dump.routines.push(make_routine("app", "calc_total"));
        dump.types.push(make_pg_type("app", "order_status"));

        let script = dump.generate_clear_script(true, true, false);

        // Transaction wrapping
        assert!(script.contains("begin;"));
        assert!(script.contains("commit;"));

        // All section headers
        assert!(script.contains("/* ---> Drop Views --------------- */"));
        assert!(script.contains("/* ---> Drop Tables --------------- */"));
        assert!(script.contains("/* ---> Drop Routines --------------- */"));
        assert!(script.contains("/* ---> Drop Sequences --------------- */"));
        assert!(script.contains("/* ---> Drop Types --------------- */"));
        assert!(script.contains("/* ---> Drop Extensions --------------- */"));
        assert!(script.contains("/* ---> Drop Schemas --------------- */"));

        // All objects are dropped (without cascade by default)
        assert!(script.contains("drop materialized view if exists app.daily_report;"));
        assert!(script.contains("drop view if exists app.order_summary;"));
        assert!(script.contains("alter table app.orders drop constraint if exists fk_user;"));
        assert!(script.contains("drop table if exists app.orders;"));
        assert!(script.contains("drop table if exists app.users;"));
        assert!(script.contains("drop function if exists app.calc_total ();"));
        assert!(script.contains("drop sequence if exists app.orders_id_seq;"));
        assert!(script.contains("drop type if exists app.order_status;"));
        assert!(script.contains("drop extension if exists \"uuid-ossp\";"));
        assert!(script.contains("drop schema if exists app;"));
        assert!(!script.contains("cascade"));
    }

    #[test]
    fn test_clear_script_view_on_view_dependency_order() {
        // v_top_customers depends on v_customer_summary — must be dropped first
        let mut dump = empty_dump();
        dump.views.push(make_view_with_deps(
            "app",
            "v_customer_summary",
            vec!["app.customers"],
        ));
        dump.views.push(make_view_with_deps(
            "app",
            "v_top_customers",
            vec!["app.v_customer_summary"],
        ));

        let script = dump.generate_clear_script(false, false, false);
        let top_pos = script
            .find("drop view if exists app.v_top_customers;")
            .expect("v_top_customers drop missing");
        let summary_pos = script
            .find("drop view if exists app.v_customer_summary;")
            .expect("v_customer_summary drop missing");
        assert!(
            top_pos < summary_pos,
            "dependent view must be dropped before its dependency"
        );
    }

    #[test]
    fn test_clear_script_regular_view_depends_on_materialized_view() {
        // regular view depends on a materialized view
        let mut dump = empty_dump();
        let mut mv = make_view_with_deps("app", "base_stats", vec!["app.orders"]);
        mv.is_materialized = true;
        mv.hash();
        dump.views.push(mv);
        dump.views.push(make_view_with_deps(
            "app",
            "top_stats",
            vec!["app.base_stats"],
        ));

        let script = dump.generate_clear_script(false, false, false);
        let top_pos = script
            .find("drop view if exists app.top_stats;")
            .expect("top_stats drop missing");
        let base_pos = script
            .find("drop materialized view if exists app.base_stats;")
            .expect("base_stats drop missing");
        assert!(
            top_pos < base_pos,
            "regular view that depends on materialized view must be dropped first"
        );
    }

    #[test]
    fn test_clear_script_three_level_view_chain() {
        // c depends on b, b depends on a — must drop c, b, a
        let mut dump = empty_dump();
        dump.views
            .push(make_view_with_deps("s", "a", vec!["s.tbl"]));
        dump.views.push(make_view_with_deps("s", "b", vec!["s.a"]));
        dump.views.push(make_view_with_deps("s", "c", vec!["s.b"]));

        let script = dump.generate_clear_script(false, false, false);
        let pos_c = script.find("drop view if exists s.c;").unwrap();
        let pos_b = script.find("drop view if exists s.b;").unwrap();
        let pos_a = script.find("drop view if exists s.a;").unwrap();
        assert!(pos_c < pos_b, "c before b");
        assert!(pos_b < pos_a, "b before a");
    }

    #[test]
    fn test_clear_script_views_stable_alphabetical_tie_break() {
        // Independent views with no deps — must appear in alphabetical order
        let mut dump = empty_dump();
        dump.views.push(make_view("s", "zeta"));
        dump.views.push(make_view("s", "alpha"));
        dump.views.push(make_view("s", "mu"));

        let script = dump.generate_clear_script(false, false, false);
        let pos_a = script.find("drop view if exists s.alpha;").unwrap();
        let pos_m = script.find("drop view if exists s.mu;").unwrap();
        let pos_z = script.find("drop view if exists s.zeta;").unwrap();
        assert!(pos_a < pos_m, "alpha before mu");
        assert!(pos_m < pos_z, "mu before zeta");
    }

    #[test]
    fn test_clear_script_views_materialized_tie_break_before_regular() {
        // At the same dependency level, materialized views come first
        let mut dump = empty_dump();
        dump.views.push(make_view("s", "regular_b"));
        dump.views.push(make_materialized_view("s", "mat_a"));
        dump.views.push(make_view("s", "regular_a"));

        let script = dump.generate_clear_script(false, false, false);
        let mat_pos = script
            .find("drop materialized view if exists s.mat_a;")
            .unwrap();
        let reg_a_pos = script.find("drop view if exists s.regular_a;").unwrap();
        let reg_b_pos = script.find("drop view if exists s.regular_b;").unwrap();
        assert!(mat_pos < reg_a_pos, "materialized before regular_a");
        assert!(mat_pos < reg_b_pos, "materialized before regular_b");
        assert!(
            reg_a_pos < reg_b_pos,
            "regular_a before regular_b alphabetically"
        );
    }
}
