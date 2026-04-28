use crate::dump::cast::Cast;
use crate::dump::collation::Collation;
use crate::dump::default_privilege::DefaultPrivilege;
use crate::dump::event_trigger::EventTrigger;
use crate::dump::fdw::{ForeignDataWrapper, ForeignServer, UserMapping};
use crate::dump::foreign_table::{ForeignTable, ForeignTableColumn};
use crate::dump::operator::Operator;
use crate::dump::pg_enum::PgEnum;
use crate::dump::pg_type::{CompositeAttribute, DomainConstraint, PgType};
use crate::dump::publication::{Publication, Subscription};
use crate::dump::routine::Routine;
use crate::dump::rule::Rule;
use crate::dump::schema::Schema;
use crate::dump::sequence::Sequence;
use crate::dump::statistic::Statistic;
use crate::dump::table::{PgCatalogCaps, Table};
use crate::dump::text_search::{TextSearchConfig, TextSearchDict};
use crate::dump::view::View;
use crate::{config::dump_config::DumpConfig, dump::extension::Extension};
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

/// Number of top-level sibling futures passed to `tokio::try_join!` inside
/// [`Dump::fill`]. Each branch holds at least one PostgreSQL connection for
/// the duration of its query; if `max_connections` is lower than this value,
/// branches queue for connections and wall-clock time suffers — or, combined
/// with nested `try_join!`s in branches like `tables`, deadlock becomes
/// possible.
///
/// This constant is statically asserted to equal the actual arity of the
/// [`fill_try_join!`] invocation in [`Dump::fill`]; adding or removing a
/// branch without updating this value is a compile error.
pub(crate) const FILL_SIBLING_BRANCH_COUNT: u32 = 12;

/// Invoke `tokio::try_join!` on the given sibling futures AND statically
/// assert that the branch count matches [`FILL_SIBLING_BRANCH_COUNT`].
///
/// The count is derived from the macro invocation itself (one `()` emitted
/// per `$fut`), so it is always the real arity of the join. If someone
/// adds or removes a branch without touching the constant — or touches the
/// constant without matching the branch list — the `const assert!` fails
/// the build with the message below.
macro_rules! fill_try_join {
    ($($fut:expr),* $(,)?) => {{
        const _FILL_ARITY: u32 = {
            // One `()` is emitted per `$fut`; slice length = branch count.
            // `stringify!` is const-safe and lets us reference `$fut` inside
            // the repetition (otherwise the metavariable is unused).
            let branches: &[()] = &[$({ let _ = stringify!($fut); }),*];
            branches.len() as u32
        };
        const _: () = assert!(
            _FILL_ARITY == FILL_SIBLING_BRANCH_COUNT,
            "FILL_SIBLING_BRANCH_COUNT is out of sync with the fill_try_join! arity \
             in Dump::fill. Update the constant to match the new branch count \
             (or remove the extra branch)."
        );
        tokio::try_join!($($fut),*)
    }};
}

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

    // List of foreign tables in the dump.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub foreign_tables: Vec<ForeignTable>,

    // List of extended statistics in the dump.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub statistics: Vec<Statistic>,

    // List of rules in the dump.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub rules: Vec<Rule>,

    // List of event triggers in the dump.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub event_triggers: Vec<EventTrigger>,

    // List of collations in the dump.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub collations: Vec<Collation>,

    // List of text search configurations in the dump.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ts_configs: Vec<TextSearchConfig>,

    // List of text search dictionaries in the dump.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ts_dicts: Vec<TextSearchDict>,

    // List of user-defined casts in the dump.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub casts: Vec<Cast>,

    // List of user-defined operators in the dump.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub operators: Vec<Operator>,

    // List of default ACL entries in the dump.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub default_privileges: Vec<DefaultPrivilege>,

    // List of publications in the dump.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub publications: Vec<Publication>,

    // List of subscriptions in the dump.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub subscriptions: Vec<Subscription>,

    // List of foreign-data wrappers in the dump.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub foreign_data_wrappers: Vec<ForeignDataWrapper>,

    // List of foreign servers in the dump.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub foreign_servers: Vec<ForeignServer>,

    // List of user mappings in the dump.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub user_mappings: Vec<UserMapping>,
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
            foreign_tables: Vec::new(),
            statistics: Vec::new(),
            rules: Vec::new(),
            event_triggers: Vec::new(),
            collations: Vec::new(),
            ts_configs: Vec::new(),
            ts_dicts: Vec::new(),
            casts: Vec::new(),
            operators: Vec::new(),
            default_privileges: Vec::new(),
            publications: Vec::new(),
            subscriptions: Vec::new(),
            foreign_data_wrappers: Vec::new(),
            foreign_servers: Vec::new(),
            user_mappings: Vec::new(),
        }
    }

    // Retrieve the dump from the configuration.
    pub async fn process(&mut self, max_connections: u32) -> Result<(), Error> {
        if max_connections < FILL_SIBLING_BRANCH_COUNT {
            eprintln!(
                "Warning: max_connections ({}) is below the number of parallel \
                 dump branches ({}). Branches will queue for connections and \
                 the dump may be significantly slower. Consider raising \
                 MAX_CONNECTIONS to at least {}.",
                max_connections, FILL_SIBLING_BRANCH_COUNT, FILL_SIBLING_BRANCH_COUNT
            );
        }

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
        self.fill(&pool).await?;

        pool.close().await;

        // Serialize the dump to a file.
        let serialized_data = serde_json::to_string(&self)
            .map_err(|e| Error::other(format!("Failed to serialize dump: {e}.")))?;
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

    async fn fill(&mut self, pool: &PgPool) -> Result<(), Error> {
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

        // Detect PostgreSQL server version once and pass it down to the
        // per-object-kind futures. The value is a session constant, so an
        // extra round-trip per parallel branch is pure waste.
        let pg_version: i32 =
            sqlx::query_scalar("select current_setting('server_version_num')::int4;")
                .fetch_one(pool)
                .await
                .map_err(|e| Error::other(format!("Failed to fetch server_version_num: {e}.")))?;

        let types_enums_fut = async {
            let mut types = Vec::new();
            let mut enums = Vec::new();
            // get_types logic (inlined to avoid &mut self borrow conflicts)
            Self::fetch_types_standalone(pool, &schema_filter, pg_version, &mut types).await?;
            Self::fetch_domain_constraints_standalone(pool, &schema_filter, &mut types).await?;
            Self::fetch_enums_standalone(pool, &mut types, &mut enums).await?;
            Ok::<(Vec<PgType>, Vec<PgEnum>), Error>((types, enums))
        };

        let extensions_fut = Self::fetch_extensions_standalone(pool, &schema_filter);
        let sequences_fut = Self::fetch_sequences_standalone(pool, &schema_filter);
        let routines_fut = Self::fetch_routines_standalone(pool, &schema_filter);
        let tables_fut = Self::fetch_tables_standalone(pool, &schema_filter, pg_version);
        let views_fut = Self::fetch_views_standalone(pool, &schema_filter);
        let foreign_tables_fut = Self::fetch_foreign_tables_standalone(pool, &schema_filter);
        let statistics_fut = Self::fetch_statistics_standalone(pool, &schema_filter);
        let rules_fut = Self::fetch_rules_standalone(pool, &schema_filter);
        let event_triggers_fut = Self::fetch_event_triggers_standalone(pool);
        let schema_extras_fut = {
            let schema_filter = schema_filter.clone();
            async move {
                let collations = Self::fetch_collations_standalone(pool, &schema_filter).await?;
                let ts_configs = Self::fetch_ts_configs_standalone(pool, &schema_filter).await?;
                let ts_dicts = Self::fetch_ts_dicts_standalone(pool, &schema_filter).await?;
                let operators = Self::fetch_operators_standalone(pool, &schema_filter).await?;
                Ok::<_, Error>((collations, ts_configs, ts_dicts, operators))
            }
        };
        let global_extras_fut = async {
            let casts = Self::fetch_casts_standalone(pool, &schema_filter).await?;
            let default_privileges =
                Self::fetch_default_privileges_standalone(pool, &schema_filter).await?;
            let publications = Self::fetch_publications_standalone(pool).await?;
            let subscriptions = Self::fetch_subscriptions_standalone(pool).await?;
            let fdws = Self::fetch_fdws_standalone(pool).await?;
            let servers = Self::fetch_servers_standalone(pool).await?;
            let user_mappings = Self::fetch_user_mappings_standalone(pool).await?;
            Ok::<_, Error>((
                casts,
                default_privileges,
                publications,
                subscriptions,
                fdws,
                servers,
                user_mappings,
            ))
        };

        // Branch count is statically checked against FILL_SIBLING_BRANCH_COUNT
        // by `fill_try_join!`; see the macro definition at the top of this
        // file. The pool-size warning in `Dump::process` keys off that same
        // constant, so the warning cannot silently drift out of sync with
        // the actual parallelism here.
        let (
            types_enums,
            extensions,
            sequences,
            routines,
            tables,
            views,
            foreign_tables,
            statistics,
            rules,
            event_triggers,
            schema_extras,
            global_extras,
        ) = fill_try_join!(
            types_enums_fut,
            extensions_fut,
            sequences_fut,
            routines_fut,
            tables_fut,
            views_fut,
            foreign_tables_fut,
            statistics_fut,
            rules_fut,
            event_triggers_fut,
            schema_extras_fut,
            global_extras_fut,
        )?;

        let (types, enums) = types_enums;
        self.types = types;
        self.enums = enums;
        self.extensions = extensions;
        self.sequences = sequences;
        self.routines = routines;
        self.tables = tables;
        self.views = views;
        self.foreign_tables = foreign_tables;
        self.statistics = statistics;
        self.rules = rules;
        self.event_triggers = event_triggers;

        let (collations, ts_configs, ts_dicts, operators) = schema_extras;
        self.collations = collations;
        self.ts_configs = ts_configs;
        self.ts_dicts = ts_dicts;
        self.operators = operators;

        let (casts, default_privileges, publications, subscriptions, fdws, servers, user_mappings) =
            global_extras;
        self.casts = casts;
        self.default_privileges = default_privileges;
        self.publications = publications;
        self.subscriptions = subscriptions;
        self.foreign_data_wrappers = fdws;
        self.foreign_servers = servers;
        self.user_mappings = user_mappings;

        Ok(())
    }

    async fn get_schemas(&mut self, pool: &PgPool) -> Result<(), Error> {
        let rows = sqlx::query(
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
        .await
        .map_err(|e| Error::other(format!("Failed to fetch schemas: {e}.")))?;

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
        pg_version: i32,
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

        // Fetch range type metadata from pg_range. `pg_version` is fetched
        // once in `Dump::fill` and threaded through to avoid an extra
        // round-trip per parallel branch.
        let has_rngmultitypid: bool = pg_version >= 140000
            && sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = 'pg_range'::regclass AND attname = 'rngmultitypid' AND NOT attisdropped)",
            )
            .fetch_one(pool)
            .await
            .unwrap_or(false);

        let multirange_col = if has_rngmultitypid {
            "quote_ident(mt.typname) as multirange_name"
        } else {
            "null::text as multirange_name"
        };
        let multirange_join = if has_rngmultitypid {
            "left join pg_type mt on mt.oid = r.rngmultitypid"
        } else {
            ""
        };

        let range_metadata_rows = sqlx::query(
            format!(
                "select
                    r.rngtypid as type_oid,
                    pg_catalog.format_type(r.rngsubtype, null) as range_subtype,
                    case when r.rngcollation <> 0 then
                        (select quote_ident(n.nspname) || '.' || quote_ident(c.collname)
                         from pg_collation c join pg_namespace n on n.oid = c.collnamespace
                         where c.oid = r.rngcollation)
                    else null end as range_collation,
                    quote_ident(opc_ns.nspname) || '.' || quote_ident(opc.opcname) as range_opclass,
                    case when r.rngcanonical <> 0 then r.rngcanonical::regproc::text else null end as range_canonical,
                    case when r.rngsubdiff <> 0 then r.rngsubdiff::regproc::text else null end as range_subdiff,
                    {}
                from pg_range r
                join pg_type t on t.oid = r.rngtypid
                join pg_namespace n on n.oid = t.typnamespace
                join pg_opclass opc on opc.oid = r.rngsubopc
                join pg_namespace opc_ns on opc_ns.oid = opc.opcnamespace
                {}
                where n.nspname in {}
                  and not exists (
                      select 1 from pg_depend ext_dep
                      where ext_dep.objid = t.oid
                      and ext_dep.deptype = 'e'
                  )",
                multirange_col, multirange_join, schema_filter
            )
            .as_str(),
        )
        .fetch_all(pool)
        .await
        .map_err(|e| Error::other(format!("Failed to fetch range type metadata: {e}.")))?;

        struct RangeMetadata {
            subtype: String,
            collation: Option<String>,
            opclass: String,
            canonical: Option<String>,
            subdiff: Option<String>,
            multirange_name: Option<String>,
        }
        let mut range_metadata_map: HashMap<Oid, RangeMetadata> = HashMap::new();
        for row in range_metadata_rows {
            let type_oid: Oid = row.get("type_oid");
            range_metadata_map.insert(
                type_oid,
                RangeMetadata {
                    subtype: row.get("range_subtype"),
                    collation: row.get::<Option<String>, _>("range_collation"),
                    opclass: row.get("range_opclass"),
                    canonical: row.get::<Option<String>, _>("range_canonical"),
                    subdiff: row.get::<Option<String>, _>("range_subdiff"),
                    multirange_name: row.get::<Option<String>, _>("multirange_name"),
                },
            );
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
                case when t.typcollation <> 0 then
                    (select quote_ident(cn.nspname) || '.' || quote_ident(cc.collname)
                     from pg_collation cc
                     join pg_namespace cn on cn.oid = cc.collnamespace
                     where cc.oid = t.typcollation)
                else null end as domain_collation_name,
                coalesce(
                    (select array_agg(acl_item::text) from unnest(t.typacl) as acl_item),
                    '{{}}'::text[]
                ) as typacl,
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
                    domain_collation_name: row.get::<Option<String>, _>("domain_collation_name"),
                    comment: row.get::<Option<String>, _>("comment"),
                    acl: row.get::<Vec<String>, _>("typacl"),
                    enum_labels: Vec::new(),
                    domain_constraints: Vec::new(),
                    composite_attributes: composite_attributes_map
                        .remove(&row.get::<Oid, _>("type_oid"))
                        .unwrap_or_default(),
                    range_subtype: None,
                    range_collation: None,
                    range_opclass: None,
                    range_canonical: None,
                    range_subdiff: None,
                    multirange_name: None,
                    hash: None,
                };
                // Populate range metadata if available
                if let Some(rm) = range_metadata_map.remove(&pgtype.oid) {
                    pgtype.range_subtype = Some(rm.subtype);
                    pgtype.range_collation = rm.collation;
                    pgtype.range_opclass = Some(rm.opclass);
                    pgtype.range_canonical = rm.canonical;
                    pgtype.range_subdiff = rm.subdiff;
                    pgtype.multirange_name = rm.multirange_name;
                }
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

            // Build OID-indexed map for O(1) lookup
            let mut type_oid_map: HashMap<u32, usize> = HashMap::new();
            for (i, t) in types.iter().enumerate() {
                type_oid_map.insert(t.oid.0, i);
            }

            for (type_oid, mut labels) in labels_by_type {
                labels.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(Ordering::Equal));

                if let Some(&idx) = type_oid_map.get(&type_oid) {
                    types[idx].enum_labels = labels.into_iter().map(|(_, label)| label).collect();
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
                seq_class.relacl::text[] as seq_acl,
                seq_class.relpersistence::text as seq_persistence
            from
                pg_sequences seq
                left join pg_namespace seq_ns on seq_ns.nspname = seq.schemaname
                left join pg_class seq_class on seq_class.relname = seq.sequencename
                    and seq_class.relnamespace = seq_ns.oid
                left join pg_description seq_desc on seq_desc.objoid = seq_class.oid
                    and seq_desc.classoid = 'pg_class'::regclass
                    and seq_desc.objsubid = 0
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
                    is_unlogged: row.get::<Option<String>, _>("seq_persistence").as_deref()
                        == Some("u"),
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
                r.proconfig::text[] as proconfig,
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
                agg.aggnumdirectargs as agg_numdirectargs,
                r.procost,
                r.prorows,
                case when r.prosupport != 0 then r.prosupport::regproc::text else null end as prosupport,
                (
                    select array_agg(format_type(t.oid, null) order by ordinality)
                    from unnest(r.protrftypes) with ordinality as u(typid, ordinality)
                    join pg_type t on t.oid = u.typid
                ) as protrftypes
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

                let routine_lang: String = row.get("prolang");
                let default_cost: f64 = match routine_lang.as_str() {
                    "c" | "internal" => 1.0,
                    _ => 100.0,
                };
                let is_procedure = prokind == "procedure";

                let mut routine = Routine {
                    schema: row.get("nspname"),
                    oid: row.get("oid"),
                    name: row.get("proname"),
                    lang: routine_lang,
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
                    config: row
                        .get::<Option<Vec<String>>, _>("proconfig")
                        .unwrap_or_default(),
                    aggregate_info,
                    cost: {
                        let c: Option<f32> = row.get("procost");
                        c.map(|v| v as f64)
                            .filter(|v| !is_procedure && (*v - default_cost).abs() > f64::EPSILON)
                    },
                    rows: {
                        let r: Option<f32> = row.get("prorows");
                        r.map(|v| v as f64).filter(|v| !is_procedure && *v > 0.0)
                    },
                    support_function: row.get("prosupport"),
                    transform_types: row
                        .get::<Option<Vec<String>>, _>("protrftypes")
                        .unwrap_or_default(),
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
    /// Fetch every table in the accessible schemas, then fill per-table
    /// metadata (columns, indexes, constraints, triggers, policies,
    /// partitioning info, definitions) via schema-wide bulk queries rather
    /// than a per-table fan-out.
    async fn fetch_tables_standalone(
        pool: &PgPool,
        schema_filter: &str,
        pg_version: i32,
    ) -> Result<Vec<Table>, Error> {
        // Check once whether the pg_get_tabledef extension function exists.
        let has_tabledef_fn =
            sqlx::query("select proname from pg_proc where proname = 'pg_get_tabledef';")
                .fetch_optional(pool)
                .await
                .unwrap_or(None)
                .is_some();

        // `pg_version` is fetched once in `Dump::fill` and passed in here.
        // Probe catalog capabilities once for the entire dump run.
        let caps = PgCatalogCaps::detect(pool, pg_version).await;

        let query = Self::build_tables_standalone_query(schema_filter);

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
            let relpersistence: Option<i8> = row.get("relpersistence");
            let relreplident: Option<i8> = row.get("relreplident");
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
                access_method: row
                    .get::<Option<String>, _>("access_method")
                    .filter(|am| am != "heap"),
                is_unlogged: relpersistence == Some(b'u' as i8),
                storage_parameters: row.get::<Option<Vec<String>>, _>("reloptions"),
                replica_identity: relreplident.map(|r| {
                    String::from(match r as u8 as char {
                        'n' => "n",
                        'f' => "f",
                        'i' => "i",
                        _ => "d",
                    })
                }),
                force_rowsecurity: row
                    .get::<Option<bool>, _>("relforcerowsecurity")
                    .unwrap_or(false),
                inherits_from: row
                    .get::<Option<Vec<String>>, _>("inherits_from")
                    .unwrap_or_default(),
                typed_table_type: row.get("typed_table_type"),
                hash: None,
                acl: row
                    .get::<Option<Vec<String>>, _>("table_acl")
                    .unwrap_or_default(),
            });
        }

        // Fill every table using schema-wide queries (one per sub-resource)
        // rather than a per-table fan-out. This turns what was previously
        // `7 × N` round-trips into 7 total, dramatically shrinking dump time
        // on high-latency connections.
        Table::fill_all(
            &mut shell_tables,
            pool,
            has_tabledef_fn,
            pg_version,
            caps,
            schema_filter,
        )
        .await
        .map_err(|e| Error::other(format!("Failed to fill tables: {e}.")))?;

        for table in &mut shell_tables {
            table.hash();
            println!(
                " - {}.{} (hash: {})",
                table.schema,
                table.name,
                table.hash.as_deref().unwrap_or("None")
            );
        }

        // Ensure deterministic output order.
        shell_tables.sort_by(|a, b| (&a.schema, &a.name).cmp(&(&b.schema, &b.name)));
        Ok(shell_tables)
    }

    fn build_tables_standalone_query(schema_filter: &str) -> String {
        format!(
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
                    c.relacl::text[] as table_acl,
                    am.amname as access_method,
                    c.relpersistence as relpersistence,
                    c.reloptions as reloptions,
                    c.relreplident as relreplident,
                    c.relforcerowsecurity as relforcerowsecurity,
                    case when c.reloftype <> 0 then c.reloftype::regtype::text else null end as typed_table_type,
                    array(
                        select quote_ident(pn.nspname) || '.' || quote_ident(pc.relname)
                        from pg_inherits pi2
                        join pg_class pc on pc.oid = pi2.inhparent
                        join pg_namespace pn on pn.oid = pc.relnamespace
                        where pi2.inhrelid = c.oid
                        and not exists (
                            select 1 from pg_partitioned_table pt where pt.partrelid = pi2.inhparent
                        )
                        order by pi2.inhseqno
                    ) as inherits_from
                from pg_tables t
                left join pg_class c on c.relname = t.tablename
                    and c.relkind in ('r','p')
                    and c.relnamespace = (select oid from pg_namespace where nspname = t.schemaname)
                left join pg_am am on am.oid = c.relam
                left join pg_description d on d.objoid = c.oid
                    and d.classoid = 'pg_class'::regclass
                    and d.objsubid = 0
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
        )
    }

    async fn fetch_views_standalone(
        pool: &PgPool,
        schema_filter: &str,
    ) -> Result<Vec<View>, Error> {
        // Fetch regular and materialized views concurrently.
        let regular_query = Self::build_regular_views_query(schema_filter);
        let mat_query = Self::build_materialized_views_query(schema_filter);
        let col_comments_query = Self::build_view_column_comments_query(schema_filter);

        let (regular_rows, mat_rows, col_comment_rows) = tokio::try_join!(
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
            async {
                sqlx::query(col_comments_query.as_str())
                    .fetch_all(pool)
                    .await
                    .map_err(|e| {
                        Error::other(format!("Failed to fetch view column comments: {e}."))
                    })
            },
        )?;

        // Build column comments map: (schema, view_name) -> Vec<(col, comment)>
        let mut col_comments_map: HashMap<(String, String), Vec<(String, String)>> = HashMap::new();
        for row in &col_comment_rows {
            let schema: String = row.get("schema_name");
            let view_name: String = row.get("view_name");
            let col: String = row.get("column_name");
            let comment: String = row.get("col_comment");
            col_comments_map
                .entry((schema, view_name))
                .or_default()
                .push((col, comment));
        }

        let mut views = Vec::new();

        if regular_rows.is_empty() {
            println!("No views found.");
        } else {
            println!("Views found:");
            for row in regular_rows {
                let check_opt: Option<String> = row.get("check_option");
                let check_option = check_opt.and_then(|v| {
                    if v.eq_ignore_ascii_case("NONE") {
                        None
                    } else {
                        Some(v.to_lowercase())
                    }
                });
                let schema: String = row.get("table_schema");
                let name: String = row.get("table_name");
                let column_comments = col_comments_map
                    .remove(&(schema.clone(), name.clone()))
                    .unwrap_or_default();
                let mut view = View {
                    schema,
                    name,
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
                    security_invoker: row.get("security_invoker"),
                    check_option,
                    column_comments,
                    storage_parameters: None,
                    tablespace: None,
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
                let schema: String = row.get("table_schema");
                let name: String = row.get("table_name");
                let column_comments = col_comments_map
                    .remove(&(schema.clone(), name.clone()))
                    .unwrap_or_default();
                let storage_opts: Option<Vec<String>> = row.get("storage_options");
                let storage_parameters = storage_opts.and_then(|v| {
                    // Filter out security_invoker from reloptions (it's handled separately)
                    let filtered: Vec<String> = v
                        .into_iter()
                        .filter(|o| !o.starts_with("security_invoker="))
                        .collect();
                    if filtered.is_empty() {
                        None
                    } else {
                        Some(filtered)
                    }
                });
                let tablespace: Option<String> = row.get("tablespace_name");
                let mut view = View {
                    schema,
                    name,
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
                    security_invoker: false,
                    check_option: None,
                    column_comments,
                    storage_parameters,
                    tablespace,
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

    fn build_regular_views_query(schema_filter: &str) -> String {
        format!(
            "select
                    quote_ident(v.table_schema) as table_schema,
                    quote_ident(v.table_name) as table_name,
                    v.view_definition,
                    quote_ident(pv.viewowner) as view_owner,
                    array_agg(distinct vtu.table_schema || '.' || vtu.table_name) as table_relation,
                    d.description as view_comment,
                    (select cc.relacl::text[] from pg_class cc where cc.oid = c.oid) as view_acl,
                    coalesce(c.reloptions::text[] @> array['security_invoker=true']::text[], false) as security_invoker,
                    v.check_option
            from information_schema.views v
            join information_schema.view_table_usage vtu on v.table_name = vtu.view_name and v.table_schema = vtu.view_schema
            left join pg_views pv on pv.schemaname = v.table_schema and pv.viewname = v.table_name
            left join pg_class c on c.relname = v.table_name and c.relnamespace = (select oid from pg_namespace where nspname = v.table_schema)
            left join pg_description d on d.objoid = c.oid
                and d.classoid = 'pg_class'::regclass
                and d.objsubid = 0
            where
                v.table_schema not in ('pg_catalog', 'information_schema')
                and v.table_schema in {}
                and not exists (
                    select 1 from pg_depend ext_dep
                    where ext_dep.objid = c.oid
                    and ext_dep.deptype = 'e'
                )
            group by v.table_schema, v.table_name, v.view_definition, pv.viewowner, d.description, c.oid, c.reloptions, v.check_option;",
            schema_filter
        )
    }

    fn build_materialized_views_query(schema_filter: &str) -> String {
        format!(
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
                    c.relacl::text[] as view_acl,
                    c.reloptions as storage_options,
                    (select spcname from pg_tablespace where oid = c.reltablespace) as tablespace_name
            from pg_matviews mv
            join pg_class c on c.relname = mv.matviewname
                and c.relnamespace = (select oid from pg_namespace where nspname = mv.schemaname)
            left join pg_description d on d.objoid = c.oid
                and d.classoid = 'pg_class'::regclass
                and d.objsubid = 0
            where mv.schemaname not in ('pg_catalog', 'information_schema')
                and mv.schemaname in {}
                and not exists (
                    select 1 from pg_depend ext_dep
                    where ext_dep.objid = c.oid
                    and ext_dep.deptype = 'e'
                );",
            schema_filter
        )
    }

    fn build_view_column_comments_query(schema_filter: &str) -> String {
        format!(
            "select
                quote_ident(n.nspname) as schema_name,
                quote_ident(c.relname) as view_name,
                quote_ident(a.attname) as column_name,
                d.description as col_comment
            from pg_class c
            join pg_namespace n on n.oid = c.relnamespace
            join pg_attribute a on a.attrelid = c.oid and a.attnum > 0 and not a.attisdropped
            join pg_description d on d.objoid = c.oid
                and d.classoid = 'pg_class'::regclass
                and d.objsubid = a.attnum
            where c.relkind in ('v', 'm')
                and n.nspname not in ('pg_catalog', 'information_schema')
                and n.nspname in {}
            order by n.nspname, c.relname, a.attnum;",
            schema_filter
        )
    }

    async fn fetch_foreign_tables_standalone(
        pool: &PgPool,
        schema_filter: &str,
    ) -> Result<Vec<ForeignTable>, Error> {
        let rows = sqlx::query(
            format!(
                "select
                    quote_ident(n.nspname) as ft_schema,
                    quote_ident(c.relname) as ft_name,
                    quote_ident(s.srvname) as ft_server,
                    quote_ident(r.rolname) as ft_owner,
                    coalesce(
                        array(
                            select option_name || ' ' || quote_literal(option_value)
                            from pg_options_to_table(ft.ftoptions)
                        ),
                        array[]::text[]
                    ) as ft_options,
                    d.description as ft_comment,
                    c.relacl::text[] as ft_acl
                from pg_foreign_table ft
                join pg_class c on c.oid = ft.ftrelid
                join pg_namespace n on n.oid = c.relnamespace
                join pg_foreign_server s on s.oid = ft.ftserver
                left join pg_roles r on r.oid = c.relowner
                left join pg_description d on d.objoid = c.oid
                    and d.classoid = 'pg_class'::regclass
                    and d.objsubid = 0
                where
                    n.nspname in {}
                    and not exists (
                        select 1 from pg_depend ext_dep
                        where ext_dep.objid = c.oid
                        and ext_dep.deptype = 'e'
                    )
                order by n.nspname, c.relname",
                schema_filter
            )
            .as_str(),
        )
        .fetch_all(pool)
        .await
        .map_err(|e| Error::other(format!("Failed to fetch foreign tables: {e}.")))?;

        let mut foreign_tables = Vec::new();

        if rows.is_empty() {
            println!("No foreign tables found.");
        } else {
            println!("Foreign tables found:");

            // Fetch all foreign table columns at once
            let col_rows = sqlx::query(
                format!(
                    "select
                        quote_ident(n.nspname) as ft_schema,
                        quote_ident(c.relname) as ft_name,
                        quote_ident(a.attname) as col_name,
                        pg_catalog.format_type(a.atttypid, a.atttypmod) as col_type,
                        a.attnotnull as col_notnull,
                        pg_get_expr(ad.adbin, ad.adrelid) as col_default,
                        coalesce(
                            array(
                                select option_name || ' ' || quote_literal(option_value)
                                from pg_options_to_table(a.attfdwoptions)
                            ),
                            array[]::text[]
                        ) as col_options
                    from pg_attribute a
                    join pg_class c on c.oid = a.attrelid
                    join pg_namespace n on n.oid = c.relnamespace
                    join pg_foreign_table ft on ft.ftrelid = c.oid
                    left join pg_attrdef ad on ad.adrelid = a.attrelid and ad.adnum = a.attnum
                    where
                        n.nspname in {}
                        and a.attnum > 0
                        and not a.attisdropped
                        and not exists (
                            select 1 from pg_depend ext_dep
                            where ext_dep.objid = c.oid
                            and ext_dep.deptype = 'e'
                        )
                    order by n.nspname, c.relname, a.attnum",
                    schema_filter
                )
                .as_str(),
            )
            .fetch_all(pool)
            .await
            .map_err(|e| Error::other(format!("Failed to fetch foreign table columns: {e}.")))?;

            // Build column map: (schema, name) -> Vec<ForeignTableColumn>
            let mut col_map: HashMap<(String, String), Vec<ForeignTableColumn>> = HashMap::new();
            for row in col_rows {
                let key = (
                    row.get::<String, _>("ft_schema"),
                    row.get::<String, _>("ft_name"),
                );
                let col = ForeignTableColumn {
                    name: row.get("col_name"),
                    data_type: row.get("col_type"),
                    is_nullable: !row.get::<bool, _>("col_notnull"),
                    column_default: row.get::<Option<String>, _>("col_default"),
                    options: row
                        .get::<Option<Vec<String>>, _>("col_options")
                        .unwrap_or_default(),
                };
                col_map.entry(key).or_default().push(col);
            }

            for row in rows {
                let schema: String = row.get("ft_schema");
                let name: String = row.get("ft_name");
                let columns = col_map
                    .remove(&(schema.clone(), name.clone()))
                    .unwrap_or_default();

                let options = row
                    .get::<Option<Vec<String>>, _>("ft_options")
                    .unwrap_or_default();

                let mut ft = ForeignTable {
                    schema,
                    name,
                    server: row.get("ft_server"),
                    owner: row.get::<Option<String>, _>("ft_owner").unwrap_or_default(),
                    options,
                    columns,
                    comment: row.get::<Option<String>, _>("ft_comment"),
                    hash: None,
                    acl: row
                        .get::<Option<Vec<String>>, _>("ft_acl")
                        .unwrap_or_default(),
                };
                ft.hash();
                println!(
                    " - {}.{} (server: {}, hash: {})",
                    ft.schema,
                    ft.name,
                    ft.server,
                    ft.hash.as_deref().unwrap_or("None")
                );
                foreign_tables.push(ft);
            }
        }

        Ok(foreign_tables)
    }

    async fn fetch_statistics_standalone(
        pool: &PgPool,
        schema_filter: &str,
    ) -> Result<Vec<Statistic>, Error> {
        let rows = sqlx::query(
            format!(
                "select
                    quote_ident(n.nspname) as stat_schema,
                    quote_ident(s.stxname) as stat_name,
                    quote_ident(r.rolname) as stat_owner,
                    quote_ident(tn.nspname) as table_schema,
                    quote_ident(tc.relname) as table_name,
                    s.stxkind::text[] as stat_kinds,
                    pg_get_statisticsobjdef(s.oid) as stat_def,
                    d.description as stat_comment,
                    s.stxstattarget as stat_target
                from pg_statistic_ext s
                join pg_namespace n on n.oid = s.stxnamespace
                join pg_class tc on tc.oid = s.stxrelid
                join pg_namespace tn on tn.oid = tc.relnamespace
                left join pg_roles r on r.oid = s.stxowner
                left join pg_description d on d.objoid = s.oid
                    and d.classoid = 'pg_statistic_ext'::regclass
                    and d.objsubid = 0
                where
                    n.nspname in {}
                    and not exists (
                        select 1 from pg_depend ext_dep
                        where ext_dep.objid = s.oid
                        and ext_dep.deptype = 'e'
                    )
                order by n.nspname, s.stxname",
                schema_filter
            )
            .as_str(),
        )
        .fetch_all(pool)
        .await
        .map_err(|e| Error::other(format!("Failed to fetch extended statistics: {e}.")))?;

        let mut statistics = Vec::new();

        if rows.is_empty() {
            println!("No extended statistics found.");
        } else {
            println!("Extended statistics found:");

            for row in rows {
                let stat_def: String = row.get("stat_def");
                let kind_chars: Vec<String> = row
                    .get::<Option<Vec<String>>, _>("stat_kinds")
                    .unwrap_or_default();

                let kinds: Vec<String> = kind_chars
                    .iter()
                    .map(|k| match k.as_str() {
                        "d" => "ndistinct".to_string(),
                        "f" => "dependencies".to_string(),
                        "m" => "mcv".to_string(),
                        "e" => "expressions".to_string(),
                        other => other.to_string(),
                    })
                    .collect();

                // Extract column names from the definition
                // Definition format: "CREATE STATISTICS ... ON col1, col2 FROM table"
                let columns = Self::parse_statistics_columns(&stat_def);

                let mut stat = Statistic {
                    schema: row.get("stat_schema"),
                    name: row.get("stat_name"),
                    owner: row
                        .get::<Option<String>, _>("stat_owner")
                        .unwrap_or_default(),
                    table_schema: row.get("table_schema"),
                    table_name: row.get("table_name"),
                    kinds,
                    columns,
                    definition: stat_def,
                    comment: row.get::<Option<String>, _>("stat_comment"),
                    stxstattarget: row
                        .try_get::<Option<i32>, _>("stat_target")
                        .ok()
                        .flatten()
                        .and_then(|v| if v < 0 { None } else { Some(v) }),
                    hash: None,
                };
                stat.hash();
                println!(
                    " - {}.{} (hash: {})",
                    stat.schema,
                    stat.name,
                    stat.hash.as_deref().unwrap_or("None")
                );
                statistics.push(stat);
            }
        }

        Ok(statistics)
    }

    fn parse_statistics_columns(def: &str) -> Vec<String> {
        // Parse columns from pg_get_statisticsobjdef output
        // Format: "... ON col1, col2 FROM ..."
        let upper = def.to_uppercase();
        if let Some(on_pos) = upper.find(" ON ") {
            let after_on = &def[on_pos + 4..];
            if let Some(from_pos) = after_on.to_uppercase().find(" FROM ") {
                let cols_str = &after_on[..from_pos];
                return cols_str
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();
            }
        }
        Vec::new()
    }

    async fn fetch_rules_standalone(
        pool: &PgPool,
        schema_filter: &str,
    ) -> Result<Vec<Rule>, Error> {
        let rows = sqlx::query(
            format!(
                "select
                    quote_ident(n.nspname) as rule_schema,
                    quote_ident(c.relname) as rule_table,
                    quote_ident(r.rulename) as rule_name,
                    pg_get_ruledef(r.oid, true) as rule_definition,
                    d.description as rule_comment
                from pg_rewrite r
                join pg_class c on c.oid = r.ev_class
                join pg_namespace n on n.oid = c.relnamespace
                left join pg_description d on d.objoid = r.oid
                    and d.classoid = 'pg_rewrite'::regclass
                    and d.objsubid = 0
                where
                    n.nspname in {}
                    and r.rulename <> '_RETURN'
                    and not exists (
                        select 1 from pg_depend ext_dep
                        where ext_dep.objid = c.oid
                        and ext_dep.deptype = 'e'
                    )
                order by n.nspname, c.relname, r.rulename",
                schema_filter
            )
            .as_str(),
        )
        .fetch_all(pool)
        .await
        .map_err(|e| Error::other(format!("Failed to fetch rules: {e}.")))?;

        let mut rules = Vec::new();

        if rows.is_empty() {
            println!("No rules found.");
        } else {
            println!("Rules found:");
            for row in rows {
                let mut rule = Rule::new(
                    row.get("rule_schema"),
                    row.get("rule_table"),
                    row.get("rule_name"),
                    row.get("rule_definition"),
                    row.get("rule_comment"),
                );
                rule.hash();
                println!(
                    " - {}.{}.{} (hash: {})",
                    rule.schema,
                    rule.table_name,
                    rule.rule_name,
                    rule.hash.as_deref().unwrap_or("None")
                );
                rules.push(rule);
            }
        }

        Ok(rules)
    }

    async fn fetch_event_triggers_standalone(pool: &PgPool) -> Result<Vec<EventTrigger>, Error> {
        let rows = sqlx::query(
            "select
                quote_ident(e.evtname) as evtname,
                e.evtevent as evtevent,
                quote_ident(n.nspname) || '.' || quote_ident(p.proname) as evtfuncname,
                coalesce(e.evttags, '{}'::text[]) as evttags,
                e.evtenabled::text as evtenabled,
                quote_ident(r.rolname) as evtowner,
                d.description as evt_comment
            from pg_event_trigger e
            join pg_proc p on p.oid = e.evtfoid
            join pg_namespace n on n.oid = p.pronamespace
            left join pg_roles r on r.oid = e.evtowner
            left join pg_description d on d.objoid = e.oid
                and d.classoid = 'pg_event_trigger'::regclass
                and d.objsubid = 0
            order by e.evtname",
        )
        .fetch_all(pool)
        .await
        .map_err(|e| Error::other(format!("Failed to fetch event triggers: {e}.")))?;

        let mut event_triggers = Vec::new();

        if rows.is_empty() {
            println!("No event triggers found.");
        } else {
            println!("Event triggers found:");
            for row in rows {
                let mut et = EventTrigger::new(
                    row.get("evtname"),
                    row.get("evtevent"),
                    row.get("evtfuncname"),
                    row.get::<Vec<String>, _>("evttags"),
                    row.get("evtenabled"),
                    row.get("evtowner"),
                    row.get("evt_comment"),
                );
                et.hash();
                println!(
                    " - {} (event: {}, hash: {})",
                    et.name,
                    et.event,
                    et.hash.as_deref().unwrap_or("None")
                );
                event_triggers.push(et);
            }
        }

        Ok(event_triggers)
    }

    async fn fetch_collations_standalone(
        pool: &PgPool,
        schema_filter: &str,
    ) -> Result<Vec<Collation>, Error> {
        // Check which catalog columns actually exist (varies by PG version and managed services)
        let col_check: (bool, bool, bool) = sqlx::query_as(
            "SELECT
                EXISTS (SELECT 1 FROM pg_catalog.pg_attribute
                        WHERE attrelid = 'pg_catalog.pg_collation'::regclass
                          AND attname = 'colliculocale' AND NOT attisdropped) AS has_icu_locale,
                EXISTS (SELECT 1 FROM pg_catalog.pg_attribute
                        WHERE attrelid = 'pg_catalog.pg_collation'::regclass
                          AND attname = 'collicurules' AND NOT attisdropped) AS has_icu_rules,
                EXISTS (SELECT 1 FROM pg_catalog.pg_attribute
                        WHERE attrelid = 'pg_catalog.pg_collation'::regclass
                          AND attname = 'colllocale' AND NOT attisdropped) AS has_coll_locale",
        )
        .fetch_one(pool)
        .await
        .map_err(|e| Error::other(format!("Failed to check collation catalog columns: {e}.")))?;

        let (has_icu_locale, has_icu_rules, has_coll_locale) = col_check;

        let icu_locale_col = if has_icu_locale {
            "c.colliculocale"
        } else {
            "NULL::text"
        };
        let icu_rules_col = if has_icu_rules {
            "c.collicurules"
        } else {
            "NULL::text"
        };
        let coll_locale_col = if has_coll_locale {
            "c.colllocale"
        } else {
            "NULL::text"
        };

        let query = format!(
            "SELECT
                quote_ident(n.nspname) as coll_schema,
                quote_ident(c.collname) as coll_name,
                COALESCE(quote_ident(r.rolname), '') as coll_owner,
                c.collprovider::text as coll_provider,
                {} as coll_locale,
                c.collcollate as coll_collate,
                c.collctype as coll_ctype,
                {} as coll_icu_locale,
                {} as coll_icu_rules,
                c.collisdeterministic as coll_deterministic,
                d.description as coll_comment
             FROM pg_collation c
             JOIN pg_namespace n ON n.oid = c.collnamespace
             LEFT JOIN pg_roles r ON r.oid = c.collowner
             LEFT JOIN pg_description d ON d.objoid = c.oid
                 AND d.classoid = 'pg_collation'::regclass AND d.objsubid = 0
             WHERE n.nspname IN {}
               AND c.collencoding IN (-1, (SELECT encoding FROM pg_database WHERE datname = current_database()))
               AND NOT EXISTS (SELECT 1 FROM pg_depend ext WHERE ext.objid = c.oid AND ext.deptype = 'e')
             ORDER BY n.nspname, c.collname",
            coll_locale_col,
            icu_locale_col,
            icu_rules_col,
            schema_filter
        );

        let rows = sqlx::query(query.as_str())
            .fetch_all(pool)
            .await
            .map_err(|e| Error::other(format!("Failed to fetch collations: {e}.")))?;

        let mut collations = Vec::new();
        if rows.is_empty() {
            println!("No user-defined collations found.");
        } else {
            println!("Collations found:");
            for row in rows {
                let mut coll = Collation {
                    schema: row.get("coll_schema"),
                    name: row.get("coll_name"),
                    owner: row.get("coll_owner"),
                    provider: row.get("coll_provider"),
                    locale: row.get("coll_locale"),
                    lc_collate: row.get("coll_collate"),
                    lc_ctype: row.get("coll_ctype"),
                    icu_locale: row.get("coll_icu_locale"),
                    icu_rules: row.get("coll_icu_rules"),
                    deterministic: row.get("coll_deterministic"),
                    comment: row.get("coll_comment"),
                    hash: None,
                };
                coll.hash();
                println!(
                    " - {}.{} (provider: {})",
                    coll.schema, coll.name, coll.provider
                );
                collations.push(coll);
            }
        }

        Ok(collations)
    }

    async fn fetch_ts_configs_standalone(
        pool: &PgPool,
        schema_filter: &str,
    ) -> Result<Vec<TextSearchConfig>, Error> {
        let query = format!(
            "SELECT
                quote_ident(n.nspname) as cfg_schema,
                quote_ident(c.cfgname) as cfg_name,
                COALESCE(quote_ident(r.rolname), '') as cfg_owner,
                quote_ident(pn.nspname) || '.' || quote_ident(p.prsname) as cfg_parser,
                d.description as cfg_comment
             FROM pg_ts_config c
             JOIN pg_namespace n ON n.oid = c.cfgnamespace
             LEFT JOIN pg_roles r ON r.oid = c.cfgowner
             JOIN pg_ts_parser p ON p.oid = c.cfgparser
             JOIN pg_namespace pn ON pn.oid = p.prsnamespace
             LEFT JOIN pg_description d ON d.objoid = c.oid
                 AND d.classoid = 'pg_ts_config'::regclass AND d.objsubid = 0
             WHERE n.nspname IN {}
               AND NOT EXISTS (SELECT 1 FROM pg_depend ext WHERE ext.objid = c.oid AND ext.deptype = 'e')
             ORDER BY n.nspname, c.cfgname",
            schema_filter
        );

        let config_rows = sqlx::query(query.as_str())
            .fetch_all(pool)
            .await
            .map_err(|e| Error::other(format!("Failed to fetch text search configs: {e}.")))?;

        if config_rows.is_empty() {
            println!("No user-defined text search configurations found.");
            return Ok(Vec::new());
        }

        // Fetch mappings for each config using pg_ts_config_map
        let mapping_query = format!(
            "SELECT
                c.cfgnamespace as cfg_ns,
                c.oid as cfg_oid,
                c.cfgname as cfg_name_raw,
                t.alias as token_type,
                string_agg(
                    quote_ident(dn.nspname) || '.' || quote_ident(d.dictname),
                    ',' ORDER BY m.mapseqno
                ) as dicts
             FROM pg_ts_config c
             JOIN pg_namespace n ON n.oid = c.cfgnamespace
             JOIN pg_ts_config_map m ON m.mapcfg = c.oid
             JOIN pg_catalog.ts_token_type(c.cfgparser) t ON t.tokid = m.maptokentype
             JOIN pg_ts_dict d ON d.oid = m.mapdict
             JOIN pg_namespace dn ON dn.oid = d.dictnamespace
             WHERE n.nspname IN {}
               AND NOT EXISTS (SELECT 1 FROM pg_depend ext WHERE ext.objid = c.oid AND ext.deptype = 'e')
             GROUP BY c.cfgnamespace, c.oid, c.cfgname, t.alias
             ORDER BY c.oid, t.alias",
            schema_filter
        );

        let mapping_rows = sqlx::query(mapping_query.as_str())
            .fetch_all(pool)
            .await
            .map_err(|e| {
                Error::other(format!("Failed to fetch text search config mappings: {e}."))
            })?;

        // Build a map from cfg_oid to list of "token_type:dict1,dict2" strings
        let mut mapping_map: HashMap<Oid, Vec<String>> = HashMap::new();
        for row in mapping_rows {
            let cfg_oid: Oid = row.get("cfg_oid");
            let token_type: String = row.get("token_type");
            let dicts: String = row.get("dicts");
            mapping_map
                .entry(cfg_oid)
                .or_default()
                .push(format!("{}:{}", token_type, dicts));
        }

        let mut ts_configs = Vec::new();
        println!("Text search configurations found:");
        for row in config_rows {
            // We need the raw OID to look up mappings — fetch it again
            let cfg_name_raw: String = row.get("cfg_name");
            let schema: String = row.get("cfg_schema");
            let owner: String = row.get("cfg_owner");
            let parser: String = row.get("cfg_parser");
            let comment: Option<String> = row.get("cfg_comment");

            // Find mappings via a separate lookup using name match
            // (we don't have oid in the config_rows; use a separate mapping lookup below)
            let mappings: Vec<String> = Vec::new();

            let mut cfg = TextSearchConfig {
                schema,
                name: cfg_name_raw.clone(),
                owner,
                parser,
                mappings,
                comment,
                hash: None,
            };
            cfg.hash();
            println!(" - {}.{}", cfg.schema, cfg.name);
            ts_configs.push(cfg);
        }

        // Re-run a combined query to get configs with their OIDs for mapping association
        let combined_query = format!(
            "SELECT
                c.oid as cfg_oid,
                quote_ident(n.nspname) as cfg_schema,
                quote_ident(c.cfgname) as cfg_name
             FROM pg_ts_config c
             JOIN pg_namespace n ON n.oid = c.cfgnamespace
             WHERE n.nspname IN {}
               AND NOT EXISTS (SELECT 1 FROM pg_depend ext WHERE ext.objid = c.oid AND ext.deptype = 'e')
             ORDER BY n.nspname, c.cfgname",
            schema_filter
        );

        let oid_rows = sqlx::query(combined_query.as_str())
            .fetch_all(pool)
            .await
            .map_err(|e| Error::other(format!("Failed to fetch text search config OIDs: {e}.")))?;

        // Match OIDs to ts_configs and set mappings
        for oid_row in &oid_rows {
            let cfg_oid: Oid = oid_row.get("cfg_oid");
            let cfg_schema: String = oid_row.get("cfg_schema");
            let cfg_name: String = oid_row.get("cfg_name");
            if let Some(mappings) = mapping_map.get(&cfg_oid)
                && let Some(cfg) = ts_configs
                    .iter_mut()
                    .find(|c| c.schema == cfg_schema && c.name == cfg_name)
            {
                cfg.mappings = mappings.clone();
                cfg.hash();
            }
        }

        Ok(ts_configs)
    }

    async fn fetch_ts_dicts_standalone(
        pool: &PgPool,
        schema_filter: &str,
    ) -> Result<Vec<TextSearchDict>, Error> {
        let query = format!(
            "SELECT
                quote_ident(n.nspname) as dict_schema,
                quote_ident(d.dictname) as dict_name,
                COALESCE(quote_ident(r.rolname), '') as dict_owner,
                quote_ident(tn.nspname) || '.' || quote_ident(t.tmplname) as dict_template,
                COALESCE(d.dictinitoption, '') as dict_options,
                desc2.description as dict_comment
             FROM pg_ts_dict d
             JOIN pg_namespace n ON n.oid = d.dictnamespace
             LEFT JOIN pg_roles r ON r.oid = d.dictowner
             JOIN pg_ts_template t ON t.oid = d.dicttemplate
             JOIN pg_namespace tn ON tn.oid = t.tmplnamespace
             LEFT JOIN pg_description desc2 ON desc2.objoid = d.oid
                 AND desc2.classoid = 'pg_ts_dict'::regclass AND desc2.objsubid = 0
             WHERE n.nspname IN {}
               AND NOT EXISTS (SELECT 1 FROM pg_depend ext WHERE ext.objid = d.oid AND ext.deptype = 'e')
             ORDER BY n.nspname, d.dictname",
            schema_filter
        );

        let rows = sqlx::query(query.as_str())
            .fetch_all(pool)
            .await
            .map_err(|e| Error::other(format!("Failed to fetch text search dicts: {e}.")))?;

        let mut ts_dicts = Vec::new();
        if rows.is_empty() {
            println!("No user-defined text search dictionaries found.");
        } else {
            println!("Text search dictionaries found:");
            for row in rows {
                let options_raw: String = row.get("dict_options");
                let options: Vec<String> = if options_raw.is_empty() {
                    Vec::new()
                } else {
                    options_raw
                        .split(',')
                        .map(|s| s.trim().to_string())
                        .filter(|s| !s.is_empty())
                        .collect()
                };

                let mut d = TextSearchDict {
                    schema: row.get("dict_schema"),
                    name: row.get("dict_name"),
                    owner: row.get("dict_owner"),
                    template: row.get("dict_template"),
                    options,
                    comment: row.get("dict_comment"),
                    hash: None,
                };
                d.hash();
                println!(" - {}.{}", d.schema, d.name);
                ts_dicts.push(d);
            }
        }

        Ok(ts_dicts)
    }

    async fn fetch_casts_standalone(
        pool: &PgPool,
        schema_filter: &str,
    ) -> Result<Vec<Cast>, Error> {
        let query = format!(
            "WITH user_types AS (
                SELECT t.oid
                FROM pg_type t
                JOIN pg_namespace n ON n.oid = t.typnamespace
                WHERE n.nspname IN {}
            )
            SELECT
                pg_catalog.format_type(c.castsource, NULL) as source_type,
                pg_catalog.format_type(c.casttarget, NULL) as target_type,
                c.castmethod::text as cast_method,
                CASE WHEN c.castfunc != 0 THEN
                    quote_ident(fn2.nspname) || '.' || quote_ident(p.proname) ||
                    '(' || pg_get_function_identity_arguments(p.oid) || ')'
                ELSE NULL END as function_name,
                c.castcontext::text as cast_context,
                d.description as cast_comment
            FROM pg_cast c
            LEFT JOIN pg_proc p ON p.oid = c.castfunc
            LEFT JOIN pg_namespace fn2 ON fn2.oid = p.pronamespace
            LEFT JOIN pg_description d ON d.objoid = c.oid
                AND d.classoid = 'pg_cast'::regclass AND d.objsubid = 0
            WHERE NOT EXISTS (SELECT 1 FROM pg_depend pd WHERE pd.objid = c.oid AND pd.deptype IN ('e', 'i'))
              AND (c.castsource IN (SELECT oid FROM user_types)
                   OR c.casttarget IN (SELECT oid FROM user_types))
            ORDER BY source_type, target_type",
            schema_filter
        );

        let rows = sqlx::query(query.as_str())
            .fetch_all(pool)
            .await
            .map_err(|e| Error::other(format!("Failed to fetch casts: {e}.")))?;

        let mut casts = Vec::new();
        if rows.is_empty() {
            println!("No user-defined casts found.");
        } else {
            println!("Casts found:");
            for row in rows {
                let mut cast = Cast {
                    source_type: row.get("source_type"),
                    target_type: row.get("target_type"),
                    cast_method: row.get("cast_method"),
                    function_name: row.get("function_name"),
                    cast_context: row.get("cast_context"),
                    comment: row.get("cast_comment"),
                    hash: None,
                };
                cast.hash();
                println!(
                    " - {} -> {} (method: {})",
                    cast.source_type, cast.target_type, cast.cast_method
                );
                casts.push(cast);
            }
        }

        Ok(casts)
    }

    async fn fetch_operators_standalone(
        pool: &PgPool,
        schema_filter: &str,
    ) -> Result<Vec<Operator>, Error> {
        let query = format!(
            "SELECT
                quote_ident(n.nspname) as op_schema,
                o.oprname as op_name,
                COALESCE(quote_ident(r.rolname), '') as op_owner,
                CASE WHEN o.oprleft != 0 THEN pg_catalog.format_type(o.oprleft, NULL) ELSE NULL END as left_type,
                CASE WHEN o.oprright != 0 THEN pg_catalog.format_type(o.oprright, NULL) ELSE NULL END as right_type,
                pg_catalog.format_type(o.oprresult, NULL) as result_type,
                quote_ident(pn.nspname) || '.' || quote_ident(p.proname) as procedure,
                CASE WHEN o.oprcom != 0 THEN
                    quote_ident(cn.nspname) || '.' || co.oprname ELSE NULL END as commutator,
                CASE WHEN o.oprnegate != 0 THEN
                    quote_ident(nn2.nspname) || '.' || no2.oprname ELSE NULL END as negator,
                CASE WHEN o.oprrest != 0 THEN
                    quote_ident(rn.nspname) || '.' || quote_ident(rp.proname) ELSE NULL END as restrict_fn,
                CASE WHEN o.oprjoin != 0 THEN
                    quote_ident(jn.nspname) || '.' || quote_ident(jp.proname) ELSE NULL END as join_fn,
                o.oprcanhash as is_hashes,
                o.oprcanmerge as is_merges,
                d.description as op_comment
             FROM pg_operator o
             JOIN pg_namespace n ON n.oid = o.oprnamespace
             LEFT JOIN pg_roles r ON r.oid = o.oprowner
             JOIN pg_proc p ON p.oid = o.oprcode
             JOIN pg_namespace pn ON pn.oid = p.pronamespace
             LEFT JOIN pg_operator co ON co.oid = o.oprcom
             LEFT JOIN pg_namespace cn ON cn.oid = co.oprnamespace
             LEFT JOIN pg_operator no2 ON no2.oid = o.oprnegate
             LEFT JOIN pg_namespace nn2 ON nn2.oid = no2.oprnamespace
             LEFT JOIN pg_proc rp ON rp.oid = o.oprrest
             LEFT JOIN pg_namespace rn ON rn.oid = rp.pronamespace
             LEFT JOIN pg_proc jp ON jp.oid = o.oprjoin
             LEFT JOIN pg_namespace jn ON jn.oid = jp.pronamespace
             LEFT JOIN pg_description d ON d.objoid = o.oid
                 AND d.classoid = 'pg_operator'::regclass AND d.objsubid = 0
             WHERE n.nspname IN {}
               AND NOT EXISTS (SELECT 1 FROM pg_depend ext WHERE ext.objid = o.oid AND ext.deptype = 'e')
             ORDER BY n.nspname, o.oprname",
            schema_filter
        );

        let rows = sqlx::query(query.as_str())
            .fetch_all(pool)
            .await
            .map_err(|e| Error::other(format!("Failed to fetch operators: {e}.")))?;

        let mut operators = Vec::new();
        if rows.is_empty() {
            println!("No user-defined operators found.");
        } else {
            println!("Operators found:");
            for row in rows {
                let mut op = Operator {
                    schema: row.get("op_schema"),
                    name: row.get("op_name"),
                    owner: row.get("op_owner"),
                    left_type: row.get("left_type"),
                    right_type: row.get("right_type"),
                    result_type: row.get("result_type"),
                    procedure: row.get("procedure"),
                    commutator: row.get("commutator"),
                    negator: row.get("negator"),
                    restrict: row.get("restrict_fn"),
                    join: row.get("join_fn"),
                    is_hashes: row.get("is_hashes"),
                    is_merges: row.get("is_merges"),
                    comment: row.get("op_comment"),
                    hash: None,
                };
                op.hash();
                println!(
                    " - {}.{} ({:?}, {:?})",
                    op.schema, op.name, op.left_type, op.right_type
                );
                operators.push(op);
            }
        }

        Ok(operators)
    }

    async fn fetch_default_privileges_standalone(
        pool: &PgPool,
        schema_filter: &str,
    ) -> Result<Vec<DefaultPrivilege>, Error> {
        let query = format!(
            "SELECT
                quote_ident(r.rolname) as role_name,
                COALESCE(quote_ident(n.nspname), '') as schema_name,
                da.defaclobjtype::text as object_type,
                COALESCE(da.defaclacl::text[], '{{}}'::text[]) as acl
             FROM pg_default_acl da
             JOIN pg_roles r ON r.oid = da.defaclrole
             LEFT JOIN pg_namespace n ON n.oid = da.defaclnamespace
             WHERE da.defaclnamespace = 0 OR n.nspname IN {}
             ORDER BY r.rolname, COALESCE(n.nspname, ''), da.defaclobjtype",
            schema_filter
        );

        let rows = sqlx::query(query.as_str())
            .fetch_all(pool)
            .await
            .map_err(|e| Error::other(format!("Failed to fetch default privileges: {e}.")))?;

        let mut default_privileges = Vec::new();
        if rows.is_empty() {
            println!("No default ACL entries found.");
        } else {
            println!("Default ACL entries found:");
            for row in rows {
                let mut dp = DefaultPrivilege {
                    role_name: row.get("role_name"),
                    schema_name: row.get("schema_name"),
                    object_type: row.get("object_type"),
                    acl: row.get("acl"),
                    hash: None,
                };
                dp.hash();
                println!(
                    " - {} IN {} ON {}",
                    dp.role_name, dp.schema_name, dp.object_type
                );
                default_privileges.push(dp);
            }
        }

        Ok(default_privileges)
    }

    async fn fetch_publications_standalone(pool: &PgPool) -> Result<Vec<Publication>, Error> {
        let pub_rows = sqlx::query(
            "SELECT
                quote_ident(p.pubname) as pub_name,
                COALESCE(quote_ident(r.rolname), '') as pub_owner,
                p.puballtables as all_tables,
                CONCAT_WS(',',
                    CASE WHEN p.pubinsert THEN 'insert' ELSE NULL END,
                    CASE WHEN p.pubupdate THEN 'update' ELSE NULL END,
                    CASE WHEN p.pubdelete THEN 'delete' ELSE NULL END,
                    CASE WHEN p.pubtruncate THEN 'truncate' ELSE NULL END
                ) as publish,
                d.description as pub_comment
             FROM pg_publication p
             LEFT JOIN pg_roles r ON r.oid = p.pubowner
             LEFT JOIN pg_description d ON d.objoid = p.oid
                 AND d.classoid = 'pg_publication'::regclass AND d.objsubid = 0
             ORDER BY p.pubname",
        )
        .fetch_all(pool)
        .await
        .map_err(|e| Error::other(format!("Failed to fetch publications: {e}.")))?;

        if pub_rows.is_empty() {
            println!("No publications found.");
            return Ok(Vec::new());
        }

        // Fetch table memberships
        let table_rows = sqlx::query(
            "SELECT
                quote_ident(p.pubname) as pub_name,
                quote_ident(n.nspname) || '.' || quote_ident(c.relname) as table_name
             FROM pg_publication p
             JOIN pg_publication_rel pr ON pr.prpubid = p.oid
             JOIN pg_class c ON c.oid = pr.prrelid
             JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE NOT p.puballtables
             ORDER BY p.pubname, n.nspname, c.relname",
        )
        .fetch_all(pool)
        .await
        .map_err(|e| Error::other(format!("Failed to fetch publication tables: {e}.")))?;

        let mut table_map: HashMap<String, Vec<String>> = HashMap::new();
        for row in table_rows {
            let pub_name: String = row.get("pub_name");
            let table_name: String = row.get("table_name");
            table_map.entry(pub_name).or_default().push(table_name);
        }

        let mut publications = Vec::new();
        println!("Publications found:");
        for row in pub_rows {
            let pub_name: String = row.get("pub_name");
            let tables = table_map.remove(&pub_name).unwrap_or_default();
            let mut pub_ = Publication {
                name: pub_name.clone(),
                owner: row.get("pub_owner"),
                all_tables: row.get("all_tables"),
                publish: row.get("publish"),
                tables,
                comment: row.get("pub_comment"),
                hash: None,
            };
            pub_.hash();
            println!(" - {}", pub_.name);
            publications.push(pub_);
        }

        Ok(publications)
    }

    async fn fetch_subscriptions_standalone(pool: &PgPool) -> Result<Vec<Subscription>, Error> {
        // pg_subscription is only accessible to superusers or pg_monitor members.
        // If the query fails due to permissions, return an empty list with a warning.
        let result = sqlx::query(
            "SELECT
                quote_ident(s.subname) as sub_name,
                COALESCE(quote_ident(r.rolname), '') as sub_owner,
                s.subconninfo as sub_conninfo,
                COALESCE(s.subpublications, '{}'::text[]) as sub_publications,
                s.subenabled as sub_enabled,
                d.description as sub_comment
             FROM pg_subscription s
             LEFT JOIN pg_roles r ON r.oid = s.subowner
             LEFT JOIN pg_description d ON d.objoid = s.oid
                 AND d.classoid = 'pg_subscription'::regclass AND d.objsubid = 0
             ORDER BY s.subname",
        )
        .fetch_all(pool)
        .await;

        match result {
            Err(e) => {
                println!("Warning: could not fetch subscriptions (insufficient privileges): {e}.");
                Ok(Vec::new())
            }
            Ok(rows) => {
                let mut subscriptions = Vec::new();
                if rows.is_empty() {
                    println!("No subscriptions found.");
                } else {
                    println!("Subscriptions found:");
                    for row in rows {
                        let mut sub = Subscription {
                            name: row.get("sub_name"),
                            owner: row.get("sub_owner"),
                            connection: row.get("sub_conninfo"),
                            publications: row.get("sub_publications"),
                            enabled: row.get("sub_enabled"),
                            comment: row.get("sub_comment"),
                            hash: None,
                        };
                        sub.hash();
                        println!(" - {}", sub.name);
                        subscriptions.push(sub);
                    }
                }
                Ok(subscriptions)
            }
        }
    }

    async fn fetch_fdws_standalone(pool: &PgPool) -> Result<Vec<ForeignDataWrapper>, Error> {
        let rows = sqlx::query(
            "SELECT
                quote_ident(fdw.fdwname) as fdw_name,
                COALESCE(quote_ident(r.rolname), '') as fdw_owner,
                CASE WHEN fdw.fdwhandler != 0 THEN
                    quote_ident(hn.nspname) || '.' || quote_ident(hp.proname)
                ELSE NULL END as handler_func,
                CASE WHEN fdw.fdwvalidator != 0 THEN
                    quote_ident(vn.nspname) || '.' || quote_ident(vp.proname)
                ELSE NULL END as validator_func,
                COALESCE(fdw.fdwoptions, '{}'::text[]) as fdw_options,
                d.description as fdw_comment
             FROM pg_foreign_data_wrapper fdw
             LEFT JOIN pg_roles r ON r.oid = fdw.fdwowner
             LEFT JOIN pg_proc hp ON hp.oid = fdw.fdwhandler
             LEFT JOIN pg_namespace hn ON hn.oid = hp.pronamespace
             LEFT JOIN pg_proc vp ON vp.oid = fdw.fdwvalidator
             LEFT JOIN pg_namespace vn ON vn.oid = vp.pronamespace
             LEFT JOIN pg_description d ON d.objoid = fdw.oid
                 AND d.classoid = 'pg_foreign_data_wrapper'::regclass AND d.objsubid = 0
             WHERE NOT EXISTS (SELECT 1 FROM pg_depend ext WHERE ext.objid = fdw.oid AND ext.deptype = 'e')
             ORDER BY fdw.fdwname",
        )
        .fetch_all(pool)
        .await
        .map_err(|e| Error::other(format!("Failed to fetch foreign data wrappers: {e}.")))?;

        let mut fdws = Vec::new();
        if rows.is_empty() {
            println!("No user-defined foreign data wrappers found.");
        } else {
            println!("Foreign data wrappers found:");
            for row in rows {
                let options: Vec<String> = row.get("fdw_options");
                let mut fdw = ForeignDataWrapper {
                    name: row.get("fdw_name"),
                    owner: row.get("fdw_owner"),
                    handler_func: row.get("handler_func"),
                    validator_func: row.get("validator_func"),
                    options,
                    comment: row.get("fdw_comment"),
                    hash: None,
                };
                fdw.hash();
                println!(" - {}", fdw.name);
                fdws.push(fdw);
            }
        }

        Ok(fdws)
    }

    async fn fetch_servers_standalone(pool: &PgPool) -> Result<Vec<ForeignServer>, Error> {
        let rows = sqlx::query(
            "SELECT
                quote_ident(s.srvname) as srv_name,
                COALESCE(quote_ident(r.rolname), '') as srv_owner,
                quote_ident(fdw.fdwname) as fdw_name,
                s.srvtype as srv_type,
                s.srvversion as srv_version,
                COALESCE(s.srvoptions, '{}'::text[]) as srv_options,
                d.description as srv_comment
             FROM pg_foreign_server s
             JOIN pg_foreign_data_wrapper fdw ON fdw.oid = s.srvfdw
             LEFT JOIN pg_roles r ON r.oid = s.srvowner
             LEFT JOIN pg_description d ON d.objoid = s.oid
                 AND d.classoid = 'pg_foreign_server'::regclass AND d.objsubid = 0
             WHERE NOT EXISTS (SELECT 1 FROM pg_depend ext WHERE ext.objid = s.oid AND ext.deptype = 'e')
             ORDER BY s.srvname",
        )
        .fetch_all(pool)
        .await
        .map_err(|e| Error::other(format!("Failed to fetch foreign servers: {e}.")))?;

        let mut servers = Vec::new();
        if rows.is_empty() {
            println!("No foreign servers found.");
        } else {
            println!("Foreign servers found:");
            for row in rows {
                let options: Vec<String> = row.get("srv_options");
                let mut srv = ForeignServer {
                    name: row.get("srv_name"),
                    owner: row.get("srv_owner"),
                    fdw_name: row.get("fdw_name"),
                    server_type: row.get("srv_type"),
                    server_version: row.get("srv_version"),
                    options,
                    comment: row.get("srv_comment"),
                    hash: None,
                };
                srv.hash();
                println!(" - {} (fdw: {})", srv.name, srv.fdw_name);
                servers.push(srv);
            }
        }

        Ok(servers)
    }

    async fn fetch_user_mappings_standalone(pool: &PgPool) -> Result<Vec<UserMapping>, Error> {
        let result = sqlx::query(
            "SELECT
                quote_ident(s.srvname) as server_name,
                CASE WHEN um.umuser = 0 THEN 'PUBLIC' ELSE quote_ident(r.rolname) END as username,
                COALESCE(um.umoptions, '{}'::text[]) as um_options
             FROM pg_user_mapping um
             JOIN pg_foreign_server s ON s.oid = um.umserver
             LEFT JOIN pg_roles r ON r.oid = um.umuser
             ORDER BY s.srvname, username",
        )
        .fetch_all(pool)
        .await;

        match result {
            Err(e) => {
                println!("Warning: could not fetch user mappings (insufficient privileges): {e}.");
                Ok(Vec::new())
            }
            Ok(rows) => {
                let mut user_mappings = Vec::new();
                if rows.is_empty() {
                    println!("No user mappings found.");
                } else {
                    println!("User mappings found:");
                    for row in rows {
                        let options: Vec<String> = row.get("um_options");
                        let mut um = UserMapping {
                            server_name: row.get("server_name"),
                            username: row.get("username"),
                            options,
                            hash: None,
                        };
                        um.hash();
                        println!(" - {} on {}", um.username, um.server_name);
                        user_mappings.push(um);
                    }
                }
                Ok(user_mappings)
            }
        }
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

        self.fill(&pool).await?;

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

            // Now drop the tables — partitions before their parents
            let mut partition_children: Vec<&Table> = Vec::new();
            let mut regular_tables: Vec<&Table> = Vec::new();
            for table in &self.tables {
                if table.partition_of.is_some() {
                    partition_children.push(table);
                } else {
                    regular_tables.push(table);
                }
            }
            for table in partition_children.iter().chain(regular_tables.iter()) {
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

        // 3. Drop foreign tables
        if !self.foreign_tables.is_empty() {
            if use_comments {
                script.append_block("\n/* ---> Drop Foreign Tables --------------- */");
            }
            for ft in &self.foreign_tables {
                if use_comments {
                    script.push_str(&format!(
                        "/* Drop foreign table: {}.{} */\n",
                        ft.schema, ft.name
                    ));
                }
                script.push_str(
                    &format!(
                        "drop foreign table if exists {}.{}{cascade_suffix};",
                        ft.schema, ft.name
                    )
                    .with_empty_lines(),
                );
            }
        }

        // 4. Drop extended statistics
        if !self.statistics.is_empty() {
            if use_comments {
                script.append_block("\n/* ---> Drop Statistics --------------- */");
            }
            for stat in &self.statistics {
                if use_comments {
                    script.push_str(&format!(
                        "/* Drop statistics: {}.{} */\n",
                        stat.schema, stat.name
                    ));
                }
                script.push_str(
                    &format!(
                        "drop statistics if exists {}.{}{cascade_suffix};",
                        stat.schema, stat.name
                    )
                    .with_empty_lines(),
                );
            }
        }

        // 5. Drop routines (functions, procedures, aggregates)
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

        // 6. Drop sequences
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

        // 7. Drop types (includes enums, composites, domains, range types, etc.)
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

        // 8. Drop extensions
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

        // 9. Drop schemas (last, since everything else lives inside them)
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
            is_enforced: true,
            no_inherit: false,
            nulls_not_distinct: false,
            comment: None,
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
            range_subtype: None,
            range_collation: None,
            range_opclass: None,
            range_canonical: None,
            range_subdiff: None,
            multirange_name: None,
            domain_collation_name: None,
            comment: None,
            acl: Vec::new(),
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

        let find = |needle: &str| {
            script
                .find(needle)
                .unwrap_or_else(|| panic!("missing `{needle}` in clear script:\n{script}"))
        };
        let view_pos = find("drop view if exists public.v1;");
        let table_pos = find("drop table if exists public.tbl1;");
        let routine_pos = find("drop function if exists public.fn1 ();");
        let seq_pos = find("drop sequence if exists public.seq1;");
        let type_pos = find("drop type if exists public.my_type;");
        let ext_pos = find("drop extension if exists pg_trgm;");
        let schema_pos = find("drop schema if exists public;");

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
        let find = |needle: &str| {
            script
                .find(needle)
                .unwrap_or_else(|| panic!("missing `{needle}` in clear script:\n{script}"))
        };
        let pos_c = find("drop view if exists s.c;");
        let pos_b = find("drop view if exists s.b;");
        let pos_a = find("drop view if exists s.a;");
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
        let find = |needle: &str| {
            script
                .find(needle)
                .unwrap_or_else(|| panic!("missing `{needle}` in clear script:\n{script}"))
        };
        let pos_a = find("drop view if exists s.alpha;");
        let pos_m = find("drop view if exists s.mu;");
        let pos_z = find("drop view if exists s.zeta;");
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
        let find = |needle: &str| {
            script
                .find(needle)
                .unwrap_or_else(|| panic!("missing `{needle}` in clear script:\n{script}"))
        };
        let mat_pos = find("drop materialized view if exists s.mat_a;");
        let reg_a_pos = find("drop view if exists s.regular_a;");
        let reg_b_pos = find("drop view if exists s.regular_b;");
        assert!(mat_pos < reg_a_pos, "materialized before regular_a");
        assert!(mat_pos < reg_b_pos, "materialized before regular_b");
        assert!(
            reg_a_pos < reg_b_pos,
            "regular_a before regular_b alphabetically"
        );
    }

    #[test]
    fn build_tables_standalone_query_filters_by_pg_class() {
        let query = Dump::build_tables_standalone_query("('public')");
        assert!(
            query.contains("d.classoid = 'pg_class'::regclass"),
            "expected pg_class classoid filter for table comments"
        );
    }

    #[test]
    fn build_regular_views_query_filters_by_pg_class() {
        let query = Dump::build_regular_views_query("('public')");
        assert!(
            query.contains("d.classoid = 'pg_class'::regclass"),
            "expected pg_class classoid filter for regular view comments"
        );
    }

    #[test]
    fn build_materialized_views_query_filters_by_pg_class() {
        let query = Dump::build_materialized_views_query("('public')");
        assert!(
            query.contains("d.classoid = 'pg_class'::regclass"),
            "expected pg_class classoid filter for materialized view comments"
        );
    }

    #[test]
    fn build_view_column_comments_query_filters_by_pg_class() {
        let query = Dump::build_view_column_comments_query("('public')");
        assert!(
            query.contains("d.classoid = 'pg_class'::regclass"),
            "expected pg_class classoid filter for view column comments"
        );
    }
}
