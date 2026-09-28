//! The [`Dump`] snapshot: every object `pgc` knows how to compare, for one
//! database, plus the code that fills it and moves it to and from disk.
//!
//! The private `Dump::fill` runs the per-object-kind queries in parallel over a single
//! [`PgPool`]. Connection budgeting there is a hard correctness concern — see
//! `FILL_SIBLING_BRANCH_COUNT` and the `fill_try_join!` macro at the top of this
//! file.
//!
//! On disk a dump is a zip archive holding one `dump.io` entry with the
//! JSON-serialized `Dump`. Fields added after the initial release carry
//! `#[serde(default, skip_serializing_if = "Vec::is_empty")]` so dumps written by
//! older `pgc` versions stay readable; preserve that when adding object kinds.

use crate::config::dump_config::DumpConfig;
use crate::dump::cast::Cast;
use crate::dump::collation::Collation;
use crate::dump::column_dependent::{ColumnDependent, ColumnDependentKind};
use crate::dump::default_privilege::DefaultPrivilege;
use crate::dump::drop_order::DropDependency;
use crate::dump::event_trigger::EventTrigger;
use crate::dump::extension::{AvailableExtension, Extension};
use crate::dump::fdw::{ForeignDataWrapper, ForeignServer, UserMapping};
use crate::dump::foreign_table::{ForeignTable, ForeignTableColumn};
use crate::dump::operator::Operator;
use crate::dump::pg_enum::PgEnum;
use crate::dump::pg_type::{CompositeAttribute, DomainConstraint, PgType};
use crate::dump::publication::{Publication, PublicationTableSpec, Subscription, SubscriptionSlot};
use crate::dump::routine::{Routine, SqlBodyDependencies};
use crate::dump::rule::{Rule, RuleDependencies};
use crate::dump::schema::Schema;
use crate::dump::sequence::Sequence;
use crate::dump::statistic::Statistic;
use crate::dump::table::{PgCatalogCaps, Table};
use crate::dump::table_index::TableIndex;
use crate::dump::text_search::{TextSearchConfig, TextSearchDict};
use crate::dump::view::View;
use serde::{Deserialize, Serialize};
use sqlx::Executor;
use sqlx::PgPool;
use sqlx::Row;
use sqlx::postgres::PgPoolOptions;
use sqlx::postgres::types::Oid;
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::{BufWriter, Error, Read, Write};
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
/// `fill_try_join!` invocation in [`Dump::fill`]; adding or removing a
/// branch without updating this value is a compile error.
pub(crate) const FILL_SIBLING_BRANCH_COUNT: u32 = 13;

/// What `Dump::fill` reads when the `--scheme` pattern matches no accessible
/// schema: none matches, or every schema it matches lacks `USAGE`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WithoutSchemas {
    /// The whole fill, as for any other dump (`pgc --command dump`, and so
    /// the `--config` chain). Its schema-scoped queries return nothing, but
    /// the database-wide objects — event triggers, publications,
    /// subscriptions, foreign-data wrappers, foreign servers, user mappings,
    /// the extensions installed in `public` and the default privileges not
    /// bound to a schema — are read exactly as a dump of any schema of the
    /// same database reads them. A compare only sees such an object as
    /// missing when its database really lacks it: an empty TO dump whose
    /// database shares them with FROM drops none of them, and an empty FROM
    /// dump (a first deployment into a database that already holds some)
    /// re-creates none of them.
    DatabaseWide,
    /// Only the server facts (`Dump::fill_server_facts`), no object
    /// (`Dump::inspect`, the clear command): a pattern matching nothing
    /// clears nothing, where the whole fill would hand the clear script every
    /// extension installed in `public`.
    ServerFactsOnly,
}

/// Session setup run on every connection pgc opens, before any catalog query.
///
/// The server deparses defaults, CHECK constraints, index predicates, view
/// bodies and routine bodies according to the session's
/// `standard_conforming_strings`. On PostgreSQL 14–18 a server-, database- or
/// role-level `off` makes every backslash in a literal come back doubled
/// (`'C:\\temp'::text`), which is a different definition — and, applied to a
/// server running with the setting on (always the case on PostgreSQL 19,
/// which removed `off`), a silently different value. A session `SET`
/// overrides those levels, exactly as `pg_dump` does on its own connection.
///
/// It is a `SET` statement rather than a startup-packet option so that
/// connection poolers, which reject unknown startup parameters, still work.
/// `escape_string_warning` must never be sent: PostgreSQL 19 removed it and
/// refuses the connection.
pub(crate) const SESSION_SETUP_SQL: &str = "set standard_conforming_strings = on";

/// First `server_version_num` with SQL-standard routine bodies
/// (`BEGIN ATOMIC … END` / `RETURN expr`, `pg_proc.prosqlbody`).
pub(crate) const SQL_BODY_MIN_VERSION: i32 = 140_000;

/// Empties `search_path` until the end of the current transaction, the way
/// `pg_dump` sets it (`set_config`, which, unlike `SET search_path = ''`,
/// leaves no empty-named schema on the path). `is_local` is `true`, so a
/// pooled connection gets its own `search_path` back when the transaction
/// ends.
pub(crate) const EMPTY_SEARCH_PATH_SQL: &str =
    "select pg_catalog.set_config('search_path', '', true)";

/// Pool options shared by every pgc pool: `max_connections` plus
/// [`SESSION_SETUP_SQL`] on each new connection.
fn pool_options(max_connections: u32) -> PgPoolOptions {
    PgPoolOptions::new()
        .max_connections(max_connections)
        .after_connect(|conn, _meta| {
            Box::pin(async move {
                conn.execute(SESSION_SETUP_SQL).await?;
                Ok(())
            })
        })
}

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

/// This file defines the Dump struct and its serialization/deserialization logic.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Dump {
    /// Configuration of the dump.
    #[serde(skip_serializing, skip_deserializing)]
    pub configuration: DumpConfig,

    /// List of schemas in the dump.
    pub schemas: Vec<Schema>,

    /// List of extensions in the dump.
    pub extensions: Vec<Extension>,

    /// List of PostgreSQL types in the dump.
    pub types: Vec<PgType>,

    /// List of PostgreSQL enums in the dump.
    pub enums: Vec<PgEnum>,

    /// List of sequences in the dump.
    pub sequences: Vec<Sequence>,

    /// List of routines in the dump.
    pub routines: Vec<Routine>,

    /// List of tables in the dump.
    pub tables: Vec<Table>,

    /// List of views in the dump.
    pub views: Vec<View>,

    /// List of foreign tables in the dump.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub foreign_tables: Vec<ForeignTable>,

    /// List of extended statistics in the dump.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub statistics: Vec<Statistic>,

    /// List of rules in the dump.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub rules: Vec<Rule>,

    /// List of event triggers in the dump.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub event_triggers: Vec<EventTrigger>,

    /// List of collations in the dump.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub collations: Vec<Collation>,

    /// List of text search configurations in the dump.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ts_configs: Vec<TextSearchConfig>,

    /// List of text search dictionaries in the dump.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ts_dicts: Vec<TextSearchDict>,

    /// List of user-defined casts in the dump.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub casts: Vec<Cast>,

    /// List of user-defined operators in the dump.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub operators: Vec<Operator>,

    /// List of default ACL entries in the dump.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub default_privileges: Vec<DefaultPrivilege>,

    /// List of publications in the dump.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub publications: Vec<Publication>,

    /// List of subscriptions in the dump.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub subscriptions: Vec<Subscription>,

    /// List of foreign-data wrappers in the dump.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub foreign_data_wrappers: Vec<ForeignDataWrapper>,

    /// List of foreign servers in the dump.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub foreign_servers: Vec<ForeignServer>,

    /// List of user mappings in the dump.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub user_mappings: Vec<UserMapping>,

    /// Column → dependent-object edges from pg_depend. Powers Phase 7's
    /// restoration of secondary dependents (indexes/constraints/policies)
    /// that PostgreSQL silently CASCADE-drops along with a generated
    /// column. Empty in pre-issue-#188 dumps; the comparer degrades to the
    /// previous behaviour (the documented "run pgc compare twice"
    /// workaround) when the field is absent.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub column_dependents: Vec<ColumnDependent>,

    /// `server_version_num` of the server the dump was taken from (e.g.
    /// `190000` for PostgreSQL 19). `None` in dumps written by pgc 1.0.x and
    /// earlier. It is not part of any object hash.
    ///
    /// A migration is applied to the FROM database, so the comparer reads this
    /// on the FROM dump to decide whether version-specific DDL (for example
    /// PostgreSQL 19's `ALTER PUBLICATION ... SET ALL TABLES`) will run there.
    /// An unknown version is treated as the oldest supported server.
    ///
    /// Its presence is also how a dump written by pgc 1.1+ is told from one
    /// written by pgc 1.0.x: `pgc` refuses to compare a FROM dump that has it
    /// with a TO dump that does not
    /// ([`Comparer::check_dump_versions`](crate::comparer::core::Comparer::check_dump_versions)).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server_version_num: Option<i32>,

    /// Extension versions the dumped server can install
    /// (`pg_available_extension_versions`). Not compared as objects: the
    /// comparer reads it on the FROM dump, the server the migration runs on,
    /// so it never pins an extension `VERSION` that server cannot install.
    /// Empty in dumps written by pgc 1.0.x, and when the server could not
    /// list them; the comparer then emits versions exactly as before.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub available_extensions: Vec<AvailableExtension>,

    /// `true` when a client of the dumped database gets
    /// `standard_conforming_strings = off` by default: a server-, database-
    /// or role-level setting, possible only on PostgreSQL 14–18. Read from
    /// `pg_settings.reset_val`, which pgc's own session `SET` (see
    /// `SESSION_SETUP_SQL`) does not change. Not part of any hash.
    ///
    /// The dump itself always holds literals in standard form. The comparer
    /// reads this on the FROM dump, the server the migration is applied to:
    /// run there with the setting `off`, a standard literal such as
    /// `'C:\temp'` would be parsed as an escape string and store a tab. So
    /// the migration then begins with `set standard_conforming_strings = on;`,
    /// as a `pg_dump` script does. `false` in dumps written by pgc 1.0.x,
    /// which do not record the setting: against a TO dump written by pgc
    /// 1.1+, such a FROM dump gets the `SET` as well.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub standard_conforming_strings_off: bool,

    /// What has to be dropped before what, from `pg_depend`
    /// ([`DropDependency`], keyed as the [`drop_order`](super::drop_order)
    /// module describes). Read only by [`inspect`](Self::inspect), for the
    /// clear command: `pgc --command dump` never fills it, so dump files do
    /// not carry it. When it is empty, [`generate_clear_script`](Self::generate_clear_script)
    /// orders the drops by what it can infer from the dumped objects alone.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub drop_dependencies: Vec<DropDependency>,
}

impl Dump {
    /// Create a new Dump instance.
    /// An empty dump bound to `config`. Nothing is read from the database until
    /// [`process`](Self::process) or [`inspect`](Self::inspect) is called.
    ///
    /// # Examples
    ///
    /// ```
    /// use pgc::config::dump_config::DumpConfig;
    /// use pgc::dump::core::Dump;
    ///
    /// let dump = Dump::new(DumpConfig {
    ///     host: "localhost".to_string(),
    ///     port: "5432".to_string(),
    ///     user: "postgres".to_string(),
    ///     password: String::new(),
    ///     database: "shop".to_string(),
    ///     scheme: "public".to_string(),
    ///     ssl: false,
    ///     file: "shop.dump".to_string(),
    /// });
    ///
    /// assert!(dump.schemas.is_empty());
    /// assert!(dump.tables.is_empty());
    /// ```
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
            column_dependents: Vec::new(),
            server_version_num: None,
            available_extensions: Vec::new(),
            standard_conforming_strings_off: false,
            drop_dependencies: Vec::new(),
        }
    }

    /// Retrieve the dump from the configuration.
    /// Connect, introspect every object kind, and write the dump to
    /// `config.file`. This is the `pgc --command dump` path.
    ///
    /// `max_connections` sizes the pool the parallel fill runs over.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use pgc::config::dump_config::DumpConfig;
    /// # use pgc::dump::core::Dump;
    /// # fn main() -> Result<(), std::io::Error> {
    /// # tokio::runtime::Runtime::new()?.block_on(async {
    /// # let config = DumpConfig {
    /// #     host: "localhost".to_string(), port: "5432".to_string(),
    /// #     user: "postgres".to_string(), password: String::new(),
    /// #     database: "shop".to_string(), scheme: "public".to_string(),
    /// #     ssl: false, file: "shop.dump".to_string(),
    /// # };
    /// let mut dump = Dump::new(config);
    /// dump.process(16).await?;
    /// println!("{}", dump.get_info());
    /// # Ok::<(), std::io::Error>(())
    /// # })?;
    /// # Ok(())
    /// # }
    /// ```
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

        // A bad port is a configuration error, not a connection failure, and
        // saying so is the whole point of issue #244 — the old URL path
        // reported every unparseable credential as `invalid port number`.
        let connect_options = self.configuration.get_connect_options().map_err(|e| {
            Error::other(format!(
                "Invalid connection configuration ({}): {}.",
                self.configuration.get_masked_connection_string(),
                e
            ))
        })?;

        let pool = pool_options(max_connections)
            .connect_with(connect_options)
            .await
            .map_err(|e| {
                Error::other(format!(
                    "Failed to connect to database ({}): {}.",
                    self.configuration.get_masked_connection_string(),
                    e
                ))
            })?;

        // Fill the dump.
        self.fill(&pool, WithoutSchemas::DatabaseWide).await?;

        pool.close().await;

        self.write_to_file(&self.configuration.file)
    }

    /// Persist this dump to `path` as a zip-compressed JSON archive.
    ///
    /// The JSON payload is streamed into the zip writer through a 256 KiB
    /// `BufWriter`, which is required for performance: `serde_json::to_writer`
    /// emits many small writes (one per JSON token), and feeding them
    /// straight into the deflate stream pays a per-call cost on every one.
    /// Buffering keeps peak memory bounded (no full intermediate `String`
    /// copy of the payload) while still amortising the compressor overhead.
    /// Pairs with [`Dump::read_from_file`].
    /// The connection configuration is deliberately *not* written, so a dump
    /// file never carries a password.
    ///
    /// # Examples
    ///
    /// ```
    /// use pgc::config::dump_config::DumpConfig;
    /// use pgc::dump::core::Dump;
    /// use pgc::dump::schema::Schema;
    ///
    /// # fn main() -> Result<(), std::io::Error> {
    /// # tokio::runtime::Runtime::new()?.block_on(async {
    /// # let config = DumpConfig {
    /// #     host: "localhost".to_string(), port: "5432".to_string(),
    /// #     user: "postgres".to_string(), password: "hunter2".to_string(),
    /// #     database: "shop".to_string(), scheme: "public".to_string(),
    /// #     ssl: false, file: String::new(),
    /// # };
    /// let path = std::env::temp_dir().join("pgc-doctest-write.dump");
    /// let path = path.to_str().unwrap();
    ///
    /// let mut dump = Dump::new(config);
    /// dump.schemas.push(Schema::new("app".to_string(), "app".to_string(), None));
    /// dump.write_to_file(path)?;
    ///
    /// let reloaded = Dump::read_from_file(path).await?;
    /// assert_eq!(reloaded.schemas.len(), 1);
    ///
    /// // The credentials did not survive the round-trip, because they were
    /// // never written: the reloaded dump carries `DumpConfig::default()`.
    /// assert_ne!(reloaded.configuration.password, "hunter2");
    /// assert_eq!(reloaded.configuration.password, DumpConfig::default().password);
    ///
    /// std::fs::remove_file(path)?;
    /// # Ok::<(), std::io::Error>(())
    /// # })?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn write_to_file(&self, path: &str) -> Result<(), Error> {
        const WRITE_BUF_BYTES: usize = 256 * 1024;
        let file = File::create(path)?;
        let mut zip = ZipWriter::new(file);
        let options = SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Deflated)
            .unix_permissions(0o644);
        zip.start_file("dump.io", options)?;
        let mut buf = BufWriter::with_capacity(WRITE_BUF_BYTES, &mut zip);
        serde_json::to_writer(&mut buf, self)
            .map_err(|e| Error::other(format!("Failed to serialize dump: {e}.")))?;
        buf.flush()?;
        drop(buf);
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

    /// Reads the schemas the `--scheme` pattern selects, then every object
    /// kind, in parallel (see `FILL_SIBLING_BRANCH_COUNT`).
    ///
    /// `without_schemas` says what a dump whose pattern matches no accessible
    /// schema reads; see [`WithoutSchemas`].
    async fn fill(&mut self, pool: &PgPool, without_schemas: WithoutSchemas) -> Result<(), Error> {
        self.get_schemas(pool).await?;

        // Detect PostgreSQL server version once and pass it down to the
        // per-object-kind futures. The value is a session constant, so an
        // extra round-trip per parallel branch is pure waste. Read before the
        // empty-schema return below: see `fill_server_facts`.
        let pg_version: i32 =
            sqlx::query_scalar("select current_setting('server_version_num')::int4;")
                .fetch_one(pool)
                .await
                .map_err(|e| Error::other(format!("Failed to fetch server_version_num: {e}.")))?;

        if self.schemas.is_empty() {
            match without_schemas {
                WithoutSchemas::ServerFactsOnly => {
                    println!("No accessible schemas to dump.");
                    self.fill_server_facts(pool, pg_version).await;
                    return Ok(());
                }
                // The schema filter below is then the no-match `(NULL)`, so
                // every schema-scoped query returns nothing, and the
                // database-wide ones return what they always do.
                WithoutSchemas::DatabaseWide => {
                    println!("No accessible schemas to dump: dumping database-wide objects only.");
                }
            }
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
            Self::fetch_types_standalone(pool, &schema_filter, pg_version, &mut types).await?;
            Self::fetch_domain_constraints_standalone(pool, &schema_filter, &mut types).await?;
            Self::fetch_enums_standalone(pool, &mut types, &mut enums).await?;
            // Last: the hash covers the constraints and labels filled in above.
            Self::hash_types(&mut types);
            Ok::<(Vec<PgType>, Vec<PgEnum>), Error>((types, enums))
        };

        // Sequential inside one branch (FILL_SIBLING_BRANCH_COUNT unchanged):
        // the availability and session-default queries are a few
        // milliseconds each and need no extra connection of their own.
        let extensions_fut = async {
            let extensions = Self::fetch_extensions_standalone(pool, &schema_filter).await?;
            let available = Self::fetch_available_extensions_standalone(pool).await;
            let scs_off = Self::fetch_standard_conforming_strings_off_standalone(pool).await;
            Ok::<_, Error>((extensions, available, scs_off))
        };
        let sequences_fut = Self::fetch_sequences_standalone(pool, &schema_filter);
        let routines_fut = Self::fetch_routines_standalone(pool, &schema_filter, pg_version);
        let tables_fut = Self::fetch_tables_standalone(pool, &schema_filter, pg_version);
        let views_fut = Self::fetch_views_standalone(pool, &schema_filter);
        let foreign_tables_fut = Self::fetch_foreign_tables_standalone(pool, &schema_filter);
        let statistics_fut = Self::fetch_statistics_standalone(pool, &schema_filter, pg_version);
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
        let column_dependents_fut =
            Self::fetch_column_dependents_standalone(pool, &schema_filter, pg_version);
        let global_extras_fut = async {
            let casts = Self::fetch_casts_standalone(pool, &schema_filter).await?;
            let default_privileges =
                Self::fetch_default_privileges_standalone(pool, &schema_filter).await?;
            let publications = Self::fetch_publications_standalone(pool, pg_version).await?;
            let subscriptions = Self::fetch_subscriptions_standalone(pool, pg_version).await?;
            let fdws = Self::fetch_fdws_standalone(pool, pg_version).await?;
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
            column_dependents,
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
            column_dependents_fut,
        )?;

        let (types, enums) = types_enums;
        self.types = types;
        self.enums = enums;
        let (extensions, available_extensions, standard_conforming_strings_off) = extensions;
        self.extensions = extensions;
        self.available_extensions = available_extensions;
        self.standard_conforming_strings_off = standard_conforming_strings_off;
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

        self.column_dependents = column_dependents;
        self.server_version_num = Some(pg_version);

        Ok(())
    }

    /// Records what a dump says about its server rather than its schemas —
    /// [`server_version_num`](Self::server_version_num),
    /// [`available_extensions`](Self::available_extensions) and
    /// [`standard_conforming_strings_off`](Self::standard_conforming_strings_off)
    /// — for a dump with no accessible schema that `fill` returns from before
    /// its parallel branches run ([`WithoutSchemas::ServerFactsOnly`]). Every
    /// other dump reads the last two in its extensions branch.
    ///
    /// Without them such a dump would read as written by pgc 1.0.x: as TO,
    /// [`Comparer::check_dump_versions`](crate::comparer::core::Comparer::check_dump_versions)
    /// would refuse the compare, and as FROM it would get the pgc 1.0.x
    /// warning, the `standard_conforming_strings` guard and pre-PostgreSQL 19
    /// fallbacks.
    async fn fill_server_facts(&mut self, pool: &PgPool, pg_version: i32) {
        self.available_extensions = Self::fetch_available_extensions_standalone(pool).await;
        self.standard_conforming_strings_off =
            Self::fetch_standard_conforming_strings_off_standalone(pool).await;
        self.server_version_num = Some(pg_version);
    }

    /// The schema-resolution query: expands the `--scheme` SIMILAR TO pattern into
    /// the concrete schema list every other dump query filters by.
    ///
    /// All `pg_`-prefixed schemas are excluded, not just `pg_catalog`: a broad
    /// pattern such as `%` would otherwise pick up `pg_toast` and — whenever any
    /// session holds temporary objects at dump time — the per-session
    /// `pg_temp_N` / `pg_toast_temp_N` schemas, and the generated
    /// `create schema pg_temp_N` fails with `unacceptable schema name` (issue
    /// #229). PostgreSQL reserves the `pg_` prefix for system schemas ("The
    /// prefix \"pg_\" is reserved"), so no user schema can ever match the filter
    /// and nothing dumpable is lost.
    fn build_schemas_query() -> &'static str {
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
           and n.nspname not like 'pg\\_%'
           and n.nspname <> 'information_schema'"
    }

    async fn get_schemas(&mut self, pool: &PgPool) -> Result<(), Error> {
        let rows = sqlx::query(Self::build_schemas_query())
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

    /// Every extension the server can install, with its default version and
    /// the versions `CREATE EXTENSION ... VERSION` accepts.
    ///
    /// Both views exist on every supported server, so nothing is gated. The
    /// columns are named rather than `*`: PostgreSQL 19 added `location` to
    /// both views.
    fn build_available_extensions_query() -> &'static str {
        "select
                quote_ident(a.name) as name,
                coalesce(a.default_version, '') as default_version,
                coalesce(
                    array_agg(v.version order by v.version) filter (where v.version is not null),
                    '{}'::text[]
                ) as versions
         from pg_available_extensions a
         left join pg_available_extension_versions v on v.name = a.name
         group by a.name, a.default_version
         order by a.name"
    }

    /// Fetch [`AvailableExtension`]s. Never fails the dump: the views read
    /// every extension control file, so one broken third-party control file
    /// makes them error. The result is then empty, with a warning, and the
    /// comparer falls back to emitting extension versions as pgc always has.
    async fn fetch_available_extensions_standalone(pool: &PgPool) -> Vec<AvailableExtension> {
        let rows = match sqlx::query(Self::build_available_extensions_query())
            .fetch_all(pool)
            .await
        {
            Ok(rows) => rows,
            Err(e) => {
                eprintln!(
                    "Warning: could not list installable extension versions ({e}); \
                     extension versions will be emitted without checking them."
                );
                return Vec::new();
            }
        };

        let available: Vec<AvailableExtension> = rows
            .iter()
            .map(|row| AvailableExtension {
                name: row.get("name"),
                default_version: row.get("default_version"),
                versions: row.get("versions"),
            })
            .collect();
        println!(
            "Installable extension versions recorded for {} extensions.",
            available.len()
        );
        available
    }

    /// Whether the session default of `standard_conforming_strings` is `off`.
    ///
    /// `reset_val`, not `current_setting()`: every pgc connection has already
    /// run `SESSION_SETUP_SQL`, which changes the current value but not the
    /// value the session started with. That start value is what `psql` gets
    /// when it applies the migration as the same role to the same database.
    /// `pg_settings` and `reset_val` exist on every supported server, so
    /// nothing is gated; PostgreSQL 19 always reports `on`.
    fn build_standard_conforming_strings_off_query() -> &'static str {
        "select coalesce(
                (select reset_val = 'off' from pg_catalog.pg_settings
                 where name = 'standard_conforming_strings'),
                false
            ) as scs_off"
    }

    /// Fetch [`Dump::standard_conforming_strings_off`]. Never fails the dump:
    /// on an error it warns and reports `on`, which leaves the migration
    /// exactly as pgc 1.0.x would have written it.
    async fn fetch_standard_conforming_strings_off_standalone(pool: &PgPool) -> bool {
        match sqlx::query_scalar::<_, bool>(Self::build_standard_conforming_strings_off_query())
            .fetch_one(pool)
            .await
        {
            Ok(off) => {
                if off {
                    println!(
                        "Server default standard_conforming_strings = off; literals are dumped \
                         in standard form, and a migration applied here starts with \
                         set standard_conforming_strings = on."
                    );
                }
                off
            }
            Err(e) => {
                eprintln!(
                    "Warning: could not read the session default of \
                     standard_conforming_strings ({e}); assuming on."
                );
                false
            }
        }
    }

    /// The attributes of every composite type in `schema_filter`, in `attnum`
    /// order, each with its own collation when that is not its data type's
    /// default (see [`CompositeAttribute::collation`]). Every catalog column
    /// it reads exists on PostgreSQL 14.
    fn build_composite_type_attributes_query(schema_filter: &str) -> String {
        format!(
            "select
                t.oid as type_oid,
                a.attname,
                pg_catalog.format_type(a.atttypid, a.atttypmod) as data_type,
                case when a.attcollation <> 0
                    and a.attcollation <> (select at.typcollation from pg_type at where at.oid = a.atttypid)
                then
                    (select quote_ident(cn.nspname) || '.' || quote_ident(cc.collname)
                     from pg_collation cc
                     join pg_namespace cn on cn.oid = cc.collnamespace
                     where cc.oid = a.attcollation)
                else null end as collation
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
                    where ext_dep.classid = 'pg_type'::regclass
                    and ext_dep.objid = t.oid
                    and ext_dep.objsubid = 0
                    and ext_dep.deptype = 'e'
                )
             order by t.oid, a.attnum",
            schema_filter
        )
    }

    /// For every composite type in `schema_filter`: the table,
    /// partitioned-table and materialized-view columns, in any schema, that
    /// store values of it (`stored_in`), and its typed tables
    /// (`typed_tables`). See [`PgType::composite_stored_in`].
    ///
    /// `stored_in` follows what PostgreSQL's `find_composite_type_dependencies`
    /// follows before it refuses `ALTER TYPE … ALTER ATTRIBUTE … TYPE`: a
    /// column of the type, of an array of it, of a domain over it, or of the
    /// row type of a composite type, view or foreign table that has a column
    /// of any of those (checked live on 14 and 19; a foreign table's own
    /// column does not count). Every catalog column it reads exists on 14.
    ///
    /// Set-wise, so its cost does not grow with the number of composite types
    /// times the size of the catalog: `contained_in` lists every "type A is
    /// held in type B" edge once, the recursion joins each level of the
    /// closure to it, and the stored columns and typed tables are joined and
    /// grouped once each. (Correlated per-type subqueries seq-scanned
    /// `pg_attribute` once per composite type: 2000 composite types took
    /// 26 s on PostgreSQL 18, and 0.3 s this way.)
    fn build_composite_usage_query(schema_filter: &str) -> String {
        format!(
            "with recursive composite as (
                select t.oid as type_oid
                from pg_type t
                join pg_namespace n on n.oid = t.typnamespace
                join pg_class c on c.oid = t.typrelid
                where
                    n.nspname in {}
                    and t.typtype = 'c'
                    and c.relkind = 'c'
                    and t.typisdefined = true
            ),
            contained_in(used_oid, container_oid) as (
                select x.typelem, x.oid
                from pg_type x
                where x.typcategory = 'A' and x.typelem <> 0
                union all
                select x.typbasetype, x.oid
                from pg_type x
                where x.typtype = 'd'
                union all
                select a.atttypid, r.reltype
                from pg_attribute a
                join pg_class r on r.oid = a.attrelid
                where a.attnum > 0
                    and not a.attisdropped
                    and r.relkind in ('c', 'v', 'f')
                    and r.reltype <> 0
            ),
            usage(type_oid, used_oid) as (
                select type_oid, type_oid from composite
                union
                select u.type_oid, ci.container_oid
                from usage u
                join contained_in ci on ci.used_oid = u.used_oid
            ),
            stored as (
                select distinct u.type_oid,
                    quote_ident(rn.nspname) || '.' || quote_ident(r.relname)
                        || '.' || quote_ident(a.attname) as stored_column
                from usage u
                join pg_attribute a on a.atttypid = u.used_oid
                    and a.attnum > 0
                    and not a.attisdropped
                join pg_class r on r.oid = a.attrelid and r.relkind in ('r', 'p', 'm')
                join pg_namespace rn on rn.oid = r.relnamespace
            ),
            typed as (
                select tt.reloftype as type_oid,
                    quote_ident(tn.nspname) || '.' || quote_ident(tt.relname) as typed_table
                from pg_class tt
                join pg_namespace tn on tn.oid = tt.relnamespace
                where tt.reloftype <> 0
            )
            select
                c.type_oid,
                coalesce(s.stored_in, '{{}}'::text[]) as stored_in,
                coalesce(tt.typed_tables, '{{}}'::text[]) as typed_tables
            from composite c
            left join (
                select type_oid, array_agg(stored_column order by stored_column) as stored_in
                from stored
                group by type_oid
            ) s on s.type_oid = c.type_oid
            left join (
                select type_oid, array_agg(typed_table order by typed_table) as typed_tables
                from typed
                group by type_oid
            ) tt on tt.type_oid = c.type_oid",
            schema_filter
        )
    }

    /// The select-list columns and the join `build_range_types_query` reads a
    /// range type's multirange with: its name and, separately, its schema
    /// ([`PgType::multirange_schema`]), or NULLs where `pg_range` has no
    /// `rngmultitypid` (before PostgreSQL 14).
    fn multirange_columns(has_rngmultitypid: bool) -> (&'static str, &'static str) {
        if has_rngmultitypid {
            (
                "quote_ident(mt.typname) as multirange_name,
                quote_ident(mtn.nspname) as multirange_schema",
                "left join pg_type mt on mt.oid = r.rngmultitypid
            left join pg_namespace mtn on mtn.oid = mt.typnamespace",
            )
        } else {
            (
                "null::text as multirange_name,
                null::text as multirange_schema",
                "",
            )
        }
    }

    fn build_range_types_query(
        schema_filter: &str,
        multirange_col: &str,
        multirange_join: &str,
    ) -> String {
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
                  where ext_dep.classid = 'pg_type'::regclass
                  and ext_dep.objid = t.oid
                  and ext_dep.objsubid = 0
                  and ext_dep.deptype = 'e'
              )",
            multirange_col, multirange_join, schema_filter
        )
    }

    fn build_types_query(schema_filter: &str) -> String {
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
            coalesce(cardinality(t.typacl) = 0, false) as typacl_explicitly_empty,
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
                where ext_dep.classid = 'pg_type'::regclass
                and ext_dep.objid = t.oid
                and ext_dep.objsubid = 0
                and ext_dep.deptype = 'e'
            )",
            schema_filter
        )
    }

    async fn fetch_types_standalone(
        pool: &PgPool,
        schema_filter: &str,
        pg_version: i32,
        types: &mut Vec<PgType>,
    ) -> Result<(), Error> {
        let composite_attributes_rows =
            sqlx::query(Self::build_composite_type_attributes_query(schema_filter).as_str())
                .fetch_all(pool)
                .await
                .map_err(|e| {
                    Error::other(format!("Failed to fetch composite type attributes: {e}."))
                })?;

        let mut composite_attributes_map: HashMap<Oid, Vec<CompositeAttribute>> = HashMap::new();
        for row in composite_attributes_rows {
            let type_oid: Oid = row.get("type_oid");
            let attribute = CompositeAttribute {
                name: row.get("attname"),
                data_type: row.get("data_type"),
                collation: row.get::<Option<String>, _>("collation"),
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

        let (multirange_col, multirange_join) = Self::multirange_columns(has_rngmultitypid);

        let range_metadata_rows = sqlx::query(
            Self::build_range_types_query(schema_filter, multirange_col, multirange_join).as_str(),
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
            multirange_schema: Option<String>,
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
                    multirange_schema: row.get::<Option<String>, _>("multirange_schema"),
                },
            );
        }

        // Run here, after the queries above, not as a branch of its own:
        // `FILL_SIBLING_BRANCH_COUNT` counts the branches of `Dump::fill`.
        let composite_usage_rows =
            sqlx::query(Self::build_composite_usage_query(schema_filter).as_str())
                .fetch_all(pool)
                .await
                .map_err(|e| Error::other(format!("Failed to fetch composite type usage: {e}.")))?;
        let mut composite_usage_map: HashMap<Oid, (Vec<String>, Vec<String>)> = HashMap::new();
        for row in composite_usage_rows {
            composite_usage_map.insert(
                row.get("type_oid"),
                (
                    row.get::<Vec<String>, _>("stored_in"),
                    row.get::<Vec<String>, _>("typed_tables"),
                ),
            );
        }

        let rows = sqlx::query(Self::build_types_query(schema_filter).as_str())
            .fetch_all(pool)
            .await
            .map_err(|e| Error::other(format!("Failed to fetch user-defined types: {e}.")))?;

        if rows.is_empty() {
            println!("No user-defined types found.");
        } else {
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
                    acl_explicitly_empty: row.get::<bool, _>("typacl_explicitly_empty"),
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
                    multirange_schema: None,
                    typed_tables: None,
                    composite_stored_in: None,
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
                    pgtype.multirange_schema = rm.multirange_schema;
                }
                if let Some((stored_in, typed_tables)) = composite_usage_map.remove(&pgtype.oid) {
                    pgtype.composite_stored_in = Some(stored_in);
                    pgtype.typed_tables = Some(typed_tables);
                }
                // Not hashed here: the domain constraints and enum labels the
                // hash covers are filled in later, and `hash_types` hashes
                // (and lists) every type once they are.
                types.push(pgtype);
            }
        }
        Ok(())
    }

    /// Hashes every fetched type and lists it on stdout.
    ///
    /// `Dump::fill` calls this after `fetch_domain_constraints_standalone`
    /// and `fetch_enums_standalone`, never inside `fetch_types_standalone`:
    /// [`PgType::hash`] covers the domain constraints and enum labels those
    /// two fill in after the type rows, so a hash taken before them — which
    /// is what pgc 1.0.x stored — misses every domain-CHECK-only and
    /// enum-label-only change, and the comparer never looks at such a type.
    fn hash_types(types: &mut [PgType]) {
        if !types.is_empty() {
            println!("User-defined types found:");
        }
        for pgtype in types.iter_mut() {
            pgtype.hash();
            println!(
                " - {} (namespace: {}, hash: {})",
                pgtype.typname,
                pgtype.schema,
                pgtype.hash.as_deref().unwrap_or("None")
            );
        }
    }

    fn build_domain_constraints_query(schema_filter: &str) -> String {
        format!(
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
                  where ext_dep.classid = 'pg_type'::regclass
                  and ext_dep.objid = c.contypid
                  and ext_dep.objsubid = 0
                  and ext_dep.deptype = 'e'
              )
            order by c.contypid, c.conname",
            schema_filter
        )
    }

    async fn fetch_domain_constraints_standalone(
        pool: &PgPool,
        schema_filter: &str,
        types: &mut [PgType],
    ) -> Result<(), Error> {
        let rows = sqlx::query(Self::build_domain_constraints_query(schema_filter).as_str())
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

    fn build_enums_query() -> &'static str {
        "select
            e.*
        from
            pg_enum e
        where
            not exists
            (
                select 1
                from
                    pg_depend ext_dep
                where
                    ext_dep.classid = 'pg_type'::regclass and
                    ext_dep.objid = e.enumtypid and
                    ext_dep.objsubid = 0 and
                    ext_dep.deptype = 'e'
            )"
    }

    async fn fetch_enums_standalone(
        pool: &PgPool,
        types: &mut [PgType],
        enums: &mut Vec<PgEnum>,
    ) -> Result<(), Error> {
        let rows = sqlx::query(Self::build_enums_query())
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
        let query = Self::build_sequences_standalone_query(schema_filter);

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

    fn build_sequences_standalone_query(schema_filter: &str) -> String {
        format!(
            "select
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
                left join pg_depend dep on dep.classid = 'pg_class'::regclass
                    and dep.objid = seq_class.oid
                    and dep.refclassid = 'pg_class'::regclass
                    and dep.deptype in ('a', 'i')
                left join pg_class owner_table on owner_table.oid = dep.refobjid
                left join pg_namespace owner_ns on owner_ns.oid = owner_table.relnamespace
                left join pg_attribute owner_attr on owner_attr.attrelid = dep.refobjid
                    and owner_attr.attnum = dep.refobjsubid
            where
                seq.schemaname in {}
                and not exists (
                    select 1 from pg_depend ext_dep
                    where ext_dep.classid = 'pg_class'::regclass
                    and ext_dep.objid = seq_class.oid
                    and ext_dep.objsubid = 0
                    and ext_dep.deptype = 'e'
                )",
            schema_filter
        )
    }

    /// The routines query.
    ///
    /// `pg_version` (`server_version_num`) gates the `has_sql_body` column:
    /// whether the routine has the SQL-standard body (`BEGIN ATOMIC … END` /
    /// `RETURN expr`, `pg_proc.prosqlbody`) PostgreSQL 14 added. 14 is also
    /// pgc's floor; the gate keeps an older server to a constant `false`
    /// rather than an unknown-column error, the same cut-off `pg_dump` uses.
    ///
    /// The body itself is not read here: it has to be deparsed with an empty
    /// `search_path`, which would also change how this query renders argument
    /// and return types. `Dump::fetch_sql_body_details` reads it, for the
    /// routines this column flags, with `Dump::build_routine_sql_bodies_query`.
    fn build_routines_standalone_query(schema_filter: &str, pg_version: i32) -> String {
        let sql_body_column = if pg_version >= SQL_BODY_MIN_VERSION {
            "(r.prosqlbody is not null) as has_sql_body"
        } else {
            "false as has_sql_body"
        };
        format!(
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
                {sql_body_column},
                d.description as routine_comment,
                r.provolatile::text as provolatile,
                r.proisstrict,
                r.proleakproof,
                r.proparallel::text as proparallel,
                r.prosecdef,
                r.proacl::text[] as routine_acl,
                r.proconfig::text[] as proconfig,
                agg.aggtransfn::regproc::text as agg_sfunc,
                pg_catalog.format_type(agg.aggtranstype, null) as agg_stype,
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
                case when agg.aggmtransfn != 0 then pg_catalog.format_type(agg.aggmtranstype, null) end as agg_mstype,
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
                    select array_agg(pg_catalog.format_type(t.oid, null) order by ordinality)
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
                    where ext_dep.classid = 'pg_proc'::regclass
                    and ext_dep.objid = r.oid
                    and ext_dep.objsubid = 0
                    and ext_dep.deptype = 'e'
                );",
            schema_filter
        )
    }

    /// The body a routine is dumped with, and whether it is a SQL-standard one
    /// (`Routine::is_sql_body`), from its `prosrc` and `prosqlbody` columns.
    ///
    /// A SQL-standard body leaves `prosrc` empty, so the deparsed `prosqlbody`
    /// wins whenever it is present, and is kept byte for byte: a carriage
    /// return in it can only sit inside a literal. A string body is `prosrc`
    /// with CRLF normalised to LF, as pgc has always dumped it.
    fn routine_body(prosrc: String, prosqlbody: Option<String>) -> (String, bool) {
        match prosqlbody {
            Some(body) => (body, true),
            None => (
                crate::utils::string_extensions::normalize_line_endings(prosrc),
                false,
            ),
        }
    }

    /// The SQL-standard bodies of the routines in `schema_filter`, by oid.
    ///
    /// Run with an empty `search_path` (`EMPTY_SEARCH_PATH_SQL`), as `pg_dump`
    /// reads them: the deparser drops the schema of every name the session's
    /// `search_path` can see — including one the author qualified — so under
    /// the default `"$user", public` a call to `public.f` came back as `f(…)`.
    /// That lost the text the comparer's dependency scans match on, and it
    /// made the body depend on the search path of whoever applies the
    /// migration, or of the database it was dumped from (`ALTER DATABASE …
    /// SET search_path`). With nothing on the path only `pg_catalog` stays
    /// unqualified, and it is always searched.
    fn build_routine_sql_bodies_query(schema_filter: &str) -> String {
        format!(
            "select
                r.oid,
                pg_catalog.pg_get_function_sqlbody(r.oid) as prosqlbody
            from pg_catalog.pg_proc r
            join pg_catalog.pg_namespace n on n.oid = r.pronamespace
            where r.prosqlbody is not null
                and n.nspname in {schema_filter}
                and n.nspname not in ('pg_catalog', 'information_schema');"
        )
    }

    /// What the SQL-standard body of each routine in `schema_filter` uses, from
    /// its `pg_depend` rows (`Routine::sql_body_dependencies`).
    ///
    /// Run with the session's default `search_path`, like the routines query:
    /// the routine and operator keys it builds (`pg_get_function_identity_arguments`,
    /// `format_type`) have to come out exactly as that query and the operators
    /// query render them. Relations are only those whose lifecycle the
    /// comparer drives (tables, partitioned tables, views, materialized views,
    /// foreign tables, sequences); a column row makes the relation count too,
    /// because PostgreSQL replaces a whole-relation row by the column rows when
    /// both exist. Every array is sorted, so the dump is deterministic.
    fn build_routine_sql_body_dependencies_query(schema_filter: &str) -> String {
        format!(
            "select
                r.oid,
                array(
                    select distinct dn.nspname || '.' || dc.relname
                    from pg_depend dep
                    join pg_class dc on dc.oid = dep.refobjid
                    join pg_namespace dn on dn.oid = dc.relnamespace
                    where dep.classid = 'pg_proc'::regclass
                      and dep.objid = r.oid
                      and dep.refclassid = 'pg_class'::regclass
                      and dep.deptype = 'n'
                      and dc.relkind in ('r', 'p', 'v', 'm', 'f', 'S')
                    order by 1
                ) as relations,
                array(
                    select distinct dn.nspname || '.' || dc.relname || '.' || da.attname
                    from pg_depend dep
                    join pg_class dc on dc.oid = dep.refobjid
                    join pg_namespace dn on dn.oid = dc.relnamespace
                    join pg_attribute da on da.attrelid = dc.oid and da.attnum = dep.refobjsubid
                    where dep.classid = 'pg_proc'::regclass
                      and dep.objid = r.oid
                      and dep.refclassid = 'pg_class'::regclass
                      and dep.deptype = 'n'
                      and dep.refobjsubid > 0
                      and dc.relkind in ('r', 'p', 'v', 'm', 'f', 'S')
                    order by 1
                ) as columns,
                array(
                    select distinct quote_ident(pn.nspname) || '.' || quote_ident(p.proname)
                        || '(' || pg_get_function_identity_arguments(p.oid) || ')'
                    from pg_depend dep
                    join pg_proc p on p.oid = dep.refobjid
                    join pg_namespace pn on pn.oid = p.pronamespace
                    where dep.classid = 'pg_proc'::regclass
                      and dep.objid = r.oid
                      and dep.refclassid = 'pg_proc'::regclass
                      and dep.deptype = 'n'
                      and p.oid <> r.oid
                    order by 1
                ) as routines,
                array(
                    select distinct quote_ident(opn.nspname) || '.' || o.oprname || '('
                        || coalesce(case when o.oprleft <> 0 then pg_catalog.format_type(o.oprleft, null) end, 'NONE')
                        || ', '
                        || coalesce(case when o.oprright <> 0 then pg_catalog.format_type(o.oprright, null) end, 'NONE')
                        || ')'
                    from pg_depend dep
                    join pg_operator o on o.oid = dep.refobjid
                    join pg_namespace opn on opn.oid = o.oprnamespace
                    where dep.classid = 'pg_proc'::regclass
                      and dep.objid = r.oid
                      and dep.refclassid = 'pg_operator'::regclass
                      and dep.deptype = 'n'
                    order by 1
                ) as operators
            from pg_proc r
            join pg_namespace n on n.oid = r.pronamespace
            where r.prosqlbody is not null
                and n.nspname in {schema_filter}
                and n.nspname not in ('pg_catalog', 'information_schema');"
        )
    }

    /// The deparsed SQL-standard bodies and their dependencies, by routine oid
    /// (see `Dump::build_routine_sql_bodies_query` and
    /// `Dump::build_routine_sql_body_dependencies_query`).
    ///
    /// Both run on one connection inside one transaction, sequentially inside
    /// the routines branch of `Dump::fill`, so `FILL_SIBLING_BRANCH_COUNT` is
    /// unchanged: the dependencies first, with the session's `search_path`,
    /// then `EMPTY_SEARCH_PATH_SQL` — local to the transaction, so the pooled
    /// connection gets its `search_path` back at commit — and the bodies.
    async fn fetch_sql_body_details(
        pool: &PgPool,
        schema_filter: &str,
    ) -> Result<(HashMap<Oid, String>, HashMap<Oid, SqlBodyDependencies>), Error> {
        let fail = |e: sqlx::Error| {
            Error::other(format!("Failed to fetch SQL-standard routine bodies: {e}."))
        };
        let mut tx = pool.begin().await.map_err(fail)?;

        let dependency_rows =
            sqlx::query(Self::build_routine_sql_body_dependencies_query(schema_filter).as_str())
                .fetch_all(&mut *tx)
                .await
                .map_err(fail)?;
        let dependencies = dependency_rows
            .into_iter()
            .map(|row| {
                (
                    row.get::<Oid, _>("oid"),
                    SqlBodyDependencies {
                        relations: row.get("relations"),
                        columns: row.get("columns"),
                        routines: row.get("routines"),
                        operators: row.get("operators"),
                    },
                )
            })
            .collect();

        sqlx::query(EMPTY_SEARCH_PATH_SQL)
            .execute(&mut *tx)
            .await
            .map_err(fail)?;
        let body_rows = sqlx::query(Self::build_routine_sql_bodies_query(schema_filter).as_str())
            .fetch_all(&mut *tx)
            .await
            .map_err(fail)?;
        let bodies = body_rows
            .into_iter()
            .filter_map(|row| {
                let body: Option<String> = row.get("prosqlbody");
                body.map(|body| (row.get::<Oid, _>("oid"), body))
            })
            .collect();

        tx.commit().await.map_err(fail)?;
        Ok((bodies, dependencies))
    }

    async fn fetch_routines_standalone(
        pool: &PgPool,
        schema_filter: &str,
        pg_version: i32,
    ) -> Result<Vec<Routine>, Error> {
        let query = Self::build_routines_standalone_query(schema_filter, pg_version);

        let rows = sqlx::query(query.as_str())
            .fetch_all(pool)
            .await
            .map_err(|e| Error::other(format!("Failed to fetch routines: {e}.")))?;

        // Only when a routine has one: a server without SQL-standard bodies
        // (or a schema without any) pays no extra round trip.
        let (mut sql_bodies, mut sql_body_dependencies) =
            if rows.iter().any(|row| row.get::<bool, _>("has_sql_body")) {
                Self::fetch_sql_body_details(pool, schema_filter).await?
            } else {
                (HashMap::new(), HashMap::new())
            };

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

                let oid: Oid = row.get("oid");
                let prosqlbody = if row.get::<bool, _>("has_sql_body") {
                    sql_bodies.remove(&oid)
                } else {
                    None
                };
                let (source_code, is_sql_body) = Self::routine_body(row.get("prosrc"), prosqlbody);
                let dependencies = if is_sql_body {
                    sql_body_dependencies.remove(&oid).unwrap_or_default()
                } else {
                    SqlBodyDependencies::default()
                };

                let routine_acl = row.get::<Option<Vec<String>>, _>("routine_acl");
                // NULL is the default ACL; `{}` has everything revoked.
                let acl_explicitly_empty = routine_acl.as_ref().is_some_and(Vec::is_empty);
                let mut routine = Routine {
                    schema: row.get("nspname"),
                    oid,
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
                    source_code,
                    is_sql_body,
                    sql_body_dependencies: dependencies,
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
                    acl: routine_acl.unwrap_or_default(),
                    acl_explicitly_empty,
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
                enforced_inherited_checks: Vec::new(),
                indexes: Vec::new(),
                triggers: Vec::new(),
                trigger_clones: Vec::new(),
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
                replica_identity_index: row.get("replica_identity_index"),
                replica_identity_columns: row
                    .get::<Option<Vec<String>>, _>("replica_identity_columns")
                    .unwrap_or_default(),
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
                    case
                        when c.relreplident = 'i' and not exists (
                            select 1 from pg_index ri
                            where ri.indrelid = c.oid and ri.indisreplident
                        ) then 'n'
                        else c.relreplident
                    end as relreplident,
                    case when c.relreplident = 'i' then (
                        select quote_ident(ric.relname)
                        from pg_index ri
                        join pg_class ric on ric.oid = ri.indexrelid
                        where ri.indrelid = c.oid and ri.indisreplident
                        limit 1
                    ) end as replica_identity_index,
                    case when c.relreplident = 'i' then array(
                        select quote_ident(a.attname)
                        from pg_index ri
                        cross join lateral unnest(ri.indkey::int2[]) with ordinality as k(attnum, ord)
                        join pg_attribute a on a.attrelid = ri.indrelid and a.attnum = k.attnum
                        where ri.indrelid = c.oid and ri.indisreplident
                        order by k.ord
                    ) end as replica_identity_columns,
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
                    and t.tablename not like 'pg\\_%'
                    and not exists (
                        select 1 from pg_depend ext_dep
                        where ext_dep.classid = 'pg_class'::regclass
                        and ext_dep.objid = c.oid
                        and ext_dep.objsubid = 0
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

        // Output columns per regular view, ordinal order (fetched sequentially so
        // the pool budget of this branch is unchanged). Rows arrive ordered by
        // attnum, so pushing preserves position.
        let view_columns_query = Self::build_view_columns_query(schema_filter);
        let view_column_rows = sqlx::query(view_columns_query.as_str())
            .fetch_all(pool)
            .await
            .map_err(|e| Error::other(format!("Failed to fetch view columns: {e}.")))?;
        let mut view_columns_map: HashMap<(String, String), Vec<crate::dump::view::ViewColumn>> =
            HashMap::new();
        for row in &view_column_rows {
            let schema: String = row.get("schema_name");
            let view_name: String = row.get("view_name");
            view_columns_map
                .entry((schema, view_name))
                .or_default()
                .push(crate::dump::view::ViewColumn {
                    name: row.get("column_name"),
                    data_type: row.get("data_type"),
                    collation: row.get("collation"),
                });
        }

        // Indexes per materialized view (fetched sequentially for the same
        // pool-budget reason as the view columns above). Keyed by the
        // `quote_ident` schema/name, which is what `build_materialized_views_query`
        // stores on the view itself.
        let matview_indexes_query = Self::build_materialized_view_indexes_query(schema_filter);
        let matview_index_rows = sqlx::query(matview_indexes_query.as_str())
            .fetch_all(pool)
            .await
            .map_err(|e| {
                Error::other(format!("Failed to fetch materialized view indexes: {e}."))
            })?;
        let mut matview_indexes_map: HashMap<(String, String), Vec<TableIndex>> = HashMap::new();
        for row in &matview_index_rows {
            let schema: String = row.get("schemaname");
            let name: String = row.get("matviewname");
            matview_indexes_map
                .entry((schema.clone(), name.clone()))
                .or_default()
                .push(TableIndex {
                    schema,
                    table: name,
                    name: row.get("indexname"),
                    catalog: row.get("tablespace"),
                    indexdef: row.get("indexdef"),
                    // A materialized view cannot be partitioned, so none of its
                    // indexes is inherited from a partitioned parent.
                    is_partition_index: false,
                    comment: row.get("index_comment"),
                });
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
                let columns = view_columns_map
                    .remove(&(schema.clone(), name.clone()))
                    .unwrap_or_default();
                let definition = Self::require_view_definition(
                    row.get("view_definition"),
                    &schema,
                    &name,
                    false,
                )?;
                let mut view = View {
                    schema,
                    name,
                    definition,
                    table_relation: row.get("table_relation"),
                    column_relation: Some(row.get("column_relation")),
                    routine_dependencies: Some(row.get("routine_dependencies")),
                    owner: row
                        .get::<Option<String>, _>("view_owner")
                        .unwrap_or_default(),
                    comment: row.get("view_comment"),
                    is_materialized: false,
                    // Only materialized views can be unpopulated.
                    is_populated: true,
                    hash: None,
                    acl: row
                        .get::<Option<Vec<String>>, _>("view_acl")
                        .unwrap_or_default(),
                    security_invoker: row.get("security_invoker"),
                    check_option,
                    column_comments,
                    storage_parameters: None,
                    tablespace: None,
                    columns,
                    // Only materialized views can be indexed.
                    indexes: Vec::new(),
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
                let mut indexes = matview_indexes_map
                    .remove(&(schema.clone(), name.clone()))
                    .unwrap_or_default();
                indexes.sort_by_key(|i| i.name.to_lowercase());
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
                let definition = Self::require_view_definition(
                    row.get("view_definition"),
                    &schema,
                    &name,
                    true,
                )?;
                let mut view = View {
                    schema,
                    name,
                    definition,
                    table_relation: row.get("table_relation"),
                    column_relation: Some(row.get("column_relation")),
                    routine_dependencies: Some(row.get("routine_dependencies")),
                    owner: row
                        .get::<Option<String>, _>("view_owner")
                        .unwrap_or_default(),
                    comment: row.get("view_comment"),
                    is_materialized: true,
                    is_populated: row.get::<Option<bool>, _>("is_populated").unwrap_or(true),
                    hash: None,
                    acl: row
                        .get::<Option<Vec<String>>, _>("view_acl")
                        .unwrap_or_default(),
                    security_invoker: false,
                    check_option: None,
                    column_comments,
                    storage_parameters,
                    tablespace,
                    // Materialized views never use CREATE OR REPLACE, so the
                    // OR-REPLACE compatibility column list is not collected for them.
                    columns: Vec::new(),
                    indexes,
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

    /// `information_schema.views` and `pg_matviews` are privilege-filtered:
    /// a NULL definition means the role can't read the body.
    fn require_view_definition(
        definition: Option<String>,
        schema: &str,
        name: &str,
        is_materialized: bool,
    ) -> Result<String, Error> {
        match definition {
            Some(d) => Ok(d),
            None => {
                let kind = if is_materialized {
                    "materialized view"
                } else {
                    "view"
                };
                Err(Error::other(format!(
                    "Failed to read {kind} definition for {schema}.{name}: \
                     view_definition is NULL. The connecting role likely \
                     lacks SELECT privileges on this {kind}. Grant the role \
                     access or run the dump under a more privileged role."
                )))
            }
        }
    }

    /// Relations referenced by the view whose `pg_class` row is aliased `c`.
    ///
    /// A view's dependencies are recorded against its `_RETURN` rewrite rule rather
    /// than the view relation, so the walk goes through `pg_rewrite`. Views that only
    /// call functions reference no relations and correctly yield an empty array.
    ///
    /// Only `_RETURN` is followed: it holds the view definition. A view can carry
    /// user-defined `DO INSTEAD` rules whose bodies touch unrelated tables, and those
    /// are dumped separately as rules, so counting their targets here would attribute
    /// relations to the view that its definition never reads.
    fn view_table_relation_subquery() -> &'static str {
        "array(
                        select distinct dn.nspname || '.' || dc.relname
                        from pg_rewrite r
                        join pg_depend dep on dep.classid = 'pg_rewrite'::regclass and dep.objid = r.oid
                        join pg_class dc on dc.oid = dep.refobjid
                        join pg_namespace dn on dn.oid = dc.relnamespace
                        where r.ev_class = c.oid
                          and r.rulename = '_RETURN'
                          and dep.refclassid = 'pg_class'::regclass
                          and dep.deptype = 'n'
                          and dc.oid <> c.oid
                          and dc.relkind in ('r', 'v', 'm', 'f', 'p')
                    )"
    }

    /// The column-level twin of [`Dump::view_table_relation_subquery`]: the
    /// individual columns the view's `_RETURN` rule depends on, which is exactly
    /// the set PostgreSQL refuses to drop or retype while the view exists.
    ///
    /// Same filters as the table-level query, plus `refobjsubid > 0` — a
    /// `pg_depend` row with subid 0 is the whole relation (what
    /// `table_relation` already collects), and a row with a positive subid is
    /// one column of it.
    fn view_column_relation_subquery() -> &'static str {
        "array(
                        select distinct dn.nspname || '.' || dc.relname || '.' || da.attname
                        from pg_rewrite r
                        join pg_depend dep on dep.classid = 'pg_rewrite'::regclass and dep.objid = r.oid
                        join pg_class dc on dc.oid = dep.refobjid
                        join pg_namespace dn on dn.oid = dc.relnamespace
                        join pg_attribute da on da.attrelid = dc.oid and da.attnum = dep.refobjsubid
                        where r.ev_class = c.oid
                          and r.rulename = '_RETURN'
                          and dep.refclassid = 'pg_class'::regclass
                          and dep.deptype = 'n'
                          and dep.refobjsubid > 0
                          and dc.oid <> c.oid
                          and dc.relkind in ('r', 'v', 'm', 'f', 'p')
                    )"
    }

    /// The routines the `_RETURN` rule of the view whose `pg_class` row is
    /// aliased `c` depends on (`View::routine_dependencies`), keyed like
    /// `Routine::dependency_key`: `quote_ident`'d schema and name, and
    /// `pg_get_function_identity_arguments`, which the routines query also
    /// runs under the session's default `search_path`. `pg_catalog` and
    /// `information_schema` routines are left out, and the array is sorted, so
    /// the dump is deterministic.
    fn view_routine_dependencies_subquery() -> &'static str {
        "array(
                        select distinct quote_ident(pn.nspname) || '.' || quote_ident(p.proname)
                            || '(' || pg_get_function_identity_arguments(p.oid) || ')'
                        from pg_rewrite r
                        join pg_depend dep on dep.classid = 'pg_rewrite'::regclass and dep.objid = r.oid
                        join pg_proc p on p.oid = dep.refobjid
                        join pg_namespace pn on pn.oid = p.pronamespace
                        where r.ev_class = c.oid
                          and r.rulename = '_RETURN'
                          and dep.refclassid = 'pg_proc'::regclass
                          and dep.deptype = 'n'
                          and pn.nspname not in ('pg_catalog', 'information_schema')
                        order by 1
                    )"
    }

    fn build_regular_views_query(schema_filter: &str) -> String {
        format!(
            "select
                    quote_ident(v.table_schema) as table_schema,
                    quote_ident(v.table_name) as table_name,
                    v.view_definition,
                    quote_ident(pv.viewowner) as view_owner,
                    {} as table_relation,
                    {} as column_relation,
                    {} as routine_dependencies,
                    d.description as view_comment,
                    (select cc.relacl::text[] from pg_class cc where cc.oid = c.oid) as view_acl,
                    coalesce(c.reloptions::text[] @> array['security_invoker=true']::text[], false) as security_invoker,
                    v.check_option
            from information_schema.views v
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
                    where ext_dep.classid = 'pg_class'::regclass
                    and ext_dep.objid = c.oid
                    and ext_dep.objsubid = 0
                    and ext_dep.deptype = 'e'
                );",
            Self::view_table_relation_subquery(),
            Self::view_column_relation_subquery(),
            Self::view_routine_dependencies_subquery(),
            schema_filter
        )
    }

    /// Materialized views, from `pg_matviews`.
    ///
    /// Schema, name and owner are read through `quote_ident`, as the regular
    /// views query reads them: they go into every statement pgc writes for the
    /// view (`CREATE`, `ALTER ... OWNER TO`, `DROP`, the clear script). pgc
    /// 1.0.x read them raw, so a mixed-case name never converged and a name
    /// holding SQL ran it; [`Dump::read_from_file`] quotes the names in such a
    /// dump on load.
    fn build_materialized_views_query(schema_filter: &str) -> String {
        format!(
            "select
                    quote_ident(mv.schemaname) as table_schema,
                    quote_ident(mv.matviewname) as table_name,
                    mv.definition as view_definition,
                    quote_ident(mv.matviewowner) as view_owner,
                    {} as table_relation,
                    {} as column_relation,
                    {} as routine_dependencies,
                    d.description as view_comment,
                    c.relacl::text[] as view_acl,
                    c.reloptions as storage_options,
                    mv.ispopulated as is_populated,
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
                    where ext_dep.classid = 'pg_class'::regclass
                    and ext_dep.objid = c.oid
                    and ext_dep.objsubid = 0
                    and ext_dep.deptype = 'e'
                );",
            Self::view_table_relation_subquery(),
            Self::view_column_relation_subquery(),
            Self::view_routine_dependencies_subquery(),
            schema_filter
        )
    }

    /// Indexes defined on materialized views.
    ///
    /// `Table::build_indexes_bulk_query` also sees these rows (`pg_indexes`
    /// spans relkind `r`/`m`/`p`) but distributes them by `(schema, table)` into
    /// the dump's table list, which only holds relkind `r`/`p` — so a
    /// materialized view's indexes matched nothing and were silently discarded,
    /// and a recreated view came back without them (issue #235). They are
    /// captured here instead, alongside the view they belong to.
    ///
    /// Unlike a table, a materialized view can carry no constraints at all, so
    /// there is no primary-key/unique-constraint-backed index to filter out:
    /// every index on one is a standalone `CREATE [UNIQUE] INDEX`.
    fn build_materialized_view_indexes_query(schema_filter: &str) -> String {
        format!(
            "select
                quote_ident(n.nspname) as schemaname,
                quote_ident(mv.relname) as matviewname,
                quote_ident(ic.relname) as indexname,
                (select spcname from pg_catalog.pg_tablespace ts where ts.oid = ic.reltablespace) as tablespace,
                pg_catalog.pg_get_indexdef(ic.oid) as indexdef,
                d.description as index_comment
            from pg_catalog.pg_index idx
            join pg_catalog.pg_class ic on ic.oid = idx.indexrelid
            join pg_catalog.pg_class mv on mv.oid = idx.indrelid
            join pg_catalog.pg_namespace n on n.oid = mv.relnamespace
            left join pg_catalog.pg_description d
                on d.objoid = ic.oid
                and d.classoid = 'pg_class'::regclass
                and d.objsubid = 0
            where mv.relkind = 'm'
                and n.nspname not in ('pg_catalog', 'information_schema')
                and n.nspname in {schema_filter}
                and not exists (
                    select 1 from pg_catalog.pg_depend ext_dep
                    where ext_dep.classid = 'pg_class'::regclass
                    and ext_dep.objid = ic.oid
                    and ext_dep.objsubid = 0
                    and ext_dep.deptype = 'e'
                )
            order by n.nspname, mv.relname, ic.relname;"
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

    /// Output columns of every regular view, in ordinal order. Captured so the
    /// comparer can tell whether a changed view can be updated with
    /// `CREATE OR REPLACE VIEW` (old columns an exact prefix of new: same name,
    /// type and collation per position) or must be dropped and recreated
    /// (issue #227). Materialized views are excluded — they never use OR REPLACE.
    fn build_view_columns_query(schema_filter: &str) -> String {
        format!(
            "select
                quote_ident(n.nspname) as schema_name,
                quote_ident(c.relname) as view_name,
                a.attname as column_name,
                pg_catalog.format_type(a.atttypid, a.atttypmod) as data_type,
                (select case
                            when nco.nspname = 'pg_catalog' then co.collname
                            else nco.nspname || '.' || co.collname
                        end
                 from pg_catalog.pg_collation co
                 join pg_catalog.pg_namespace nco on nco.oid = co.collnamespace
                 where co.oid = a.attcollation) as collation
            from pg_class c
            join pg_namespace n on n.oid = c.relnamespace
            join pg_attribute a on a.attrelid = c.oid and a.attnum > 0 and not a.attisdropped
            where c.relkind = 'v'
                and n.nspname not in ('pg_catalog', 'information_schema')
                and n.nspname in {}
                and not exists (
                    select 1 from pg_depend ext_dep
                    where ext_dep.classid = 'pg_class'::regclass
                    and ext_dep.objid = c.oid
                    and ext_dep.objsubid = 0
                    and ext_dep.deptype = 'e'
                )
            order by n.nspname, c.relname, a.attnum;",
            schema_filter
        )
    }

    fn build_foreign_tables_query(schema_filter: &str) -> String {
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
                c.relacl::text[] as ft_acl,
                case when c.relispartition then (
                    select quote_ident(pn.nspname) || '.' || quote_ident(pc.relname)
                    from pg_inherits pi
                    join pg_class pc on pc.oid = pi.inhparent
                    join pg_namespace pn on pn.oid = pc.relnamespace
                    where pi.inhrelid = c.oid
                ) end as ft_partition_of,
                case when c.relispartition
                    then pg_get_expr(c.relpartbound, c.oid)
                end as ft_partition_bound
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
                    where ext_dep.classid = 'pg_class'::regclass
                    and ext_dep.objid = c.oid
                    and ext_dep.objsubid = 0
                    and ext_dep.deptype = 'e'
                )
            order by n.nspname, c.relname",
            schema_filter
        )
    }

    fn build_foreign_table_columns_query(schema_filter: &str) -> String {
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
                ) as col_options,
                coalesce(a.attacl::text[], array[]::text[]) as col_acl
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
                    where ext_dep.classid = 'pg_class'::regclass
                    and ext_dep.objid = c.oid
                    and ext_dep.objsubid = 0
                    and ext_dep.deptype = 'e'
                )
            order by n.nspname, c.relname, a.attnum",
            schema_filter
        )
    }

    async fn fetch_foreign_tables_standalone(
        pool: &PgPool,
        schema_filter: &str,
    ) -> Result<Vec<ForeignTable>, Error> {
        let rows = sqlx::query(Self::build_foreign_tables_query(schema_filter).as_str())
            .fetch_all(pool)
            .await
            .map_err(|e| Error::other(format!("Failed to fetch foreign tables: {e}.")))?;

        let mut foreign_tables = Vec::new();

        if rows.is_empty() {
            println!("No foreign tables found.");
        } else {
            println!("Foreign tables found:");

            // Fetch all foreign table columns at once
            let col_rows =
                sqlx::query(Self::build_foreign_table_columns_query(schema_filter).as_str())
                    .fetch_all(pool)
                    .await
                    .map_err(|e| {
                        Error::other(format!("Failed to fetch foreign table columns: {e}."))
                    })?;

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
                    acl: row
                        .get::<Option<Vec<String>>, _>("col_acl")
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
                    partition_of: row.get("ft_partition_of"),
                    partition_bound: row.get("ft_partition_bound"),
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

    /// The extended-statistics query.
    ///
    /// `stat_columns` is `pg_get_statisticsobjdef_columns(oid)`, the `ON` list
    /// as the server deparses it — the form `Statistic::get_script` emits. The
    /// function arrived in PostgreSQL 14 together with expression statistics;
    /// older servers get `NULL` there and emission falls back to the list
    /// cut out of `pg_get_statisticsobjdef`.
    ///
    /// `stxstattarget` is cast to `int4` because its type changed: `integer`
    /// with `-1` for the default up to PostgreSQL 16, nullable `smallint` from
    /// 17 on. Read uncast, the `smallint` failed to decode into the `i32`
    /// field, so every explicit statistics target on 17+ was silently lost.
    fn build_statistics_query(schema_filter: &str, pg_version: i32) -> String {
        let columns_def = if pg_version >= 140000 {
            "pg_get_statisticsobjdef_columns(s.oid)"
        } else {
            "null::text"
        };
        format!(
            "select
                quote_ident(n.nspname) as stat_schema,
                quote_ident(s.stxname) as stat_name,
                quote_ident(r.rolname) as stat_owner,
                quote_ident(tn.nspname) as table_schema,
                quote_ident(tc.relname) as table_name,
                s.stxkind::text[] as stat_kinds,
                pg_get_statisticsobjdef(s.oid) as stat_def,
                {columns_def} as stat_columns,
                d.description as stat_comment,
                s.stxstattarget::int4 as stat_target
            from pg_statistic_ext s
            join pg_namespace n on n.oid = s.stxnamespace
            join pg_class tc on tc.oid = s.stxrelid
            join pg_namespace tn on tn.oid = tc.relnamespace
            left join pg_roles r on r.oid = s.stxowner
            left join pg_description d on d.objoid = s.oid
                and d.classoid = 'pg_statistic_ext'::regclass
                and d.objsubid = 0
            where
                n.nspname in {schema_filter}
                and not exists (
                    select 1 from pg_depend ext_dep
                    where ext_dep.classid = 'pg_statistic_ext'::regclass
                    and ext_dep.objid = s.oid
                    and ext_dep.objsubid = 0
                    and ext_dep.deptype = 'e'
                )
            order by n.nspname, s.stxname"
        )
    }

    async fn fetch_statistics_standalone(
        pool: &PgPool,
        schema_filter: &str,
        pg_version: i32,
    ) -> Result<Vec<Statistic>, Error> {
        let rows = sqlx::query(Self::build_statistics_query(schema_filter, pg_version).as_str())
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

                // The split ON list is hashed, so it keeps the shape every
                // earlier pgc produced; emission uses `columns_def` instead.
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
                    columns_def: row.get::<Option<String>, _>("stat_columns"),
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

    /// Split the `ON` list out of a `pg_get_statisticsobjdef` definition
    /// (`... ON col1, col2 FROM ...`) on `,`.
    ///
    /// Lossy by construction: it stops at the first ` FROM `, so
    /// `EXTRACT(year FROM d)` is cut short, and it splits inside literals and
    /// function arguments. The result is kept only because `Statistic::hash`
    /// has always hashed it; `Statistic::get_script` emits the server-deparsed
    /// `columns_def` instead, and falls back to this only as a last resort.
    fn parse_statistics_columns(def: &str) -> Vec<String> {
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

    /// Walk `pg_catalog.pg_depend` to harvest column → dependent edges
    /// that PostgreSQL would CASCADE-drop when a column is dropped.
    /// Powers Phase 7's restoration of secondary dependents (issue #188).
    ///
    /// Five UNION ALL branches, one per dependent kind:
    ///   * `pg_class` → only `relkind IN ('i','I')` (indexes, partitioned
    ///     indexes); other relations cannot have a column-level pg_depend
    ///     edge anyway.
    ///   * `pg_constraint` → CHECK, UNIQUE, EXCLUDE, and FOREIGN KEY
    ///     constraints all reach here; PRIMARY KEY constraints would also
    ///     appear, but their drop semantics are subsumed by the
    ///     accompanying index dependency.
    ///   * `pg_policy` → RLS policies that name the column in their
    ///     USING/WITH CHECK clauses.
    ///   * `pg_statistic_ext` → extended statistics whose `ON` list names
    ///     the column, as a plain key, inside an expression, or (PostgreSQL
    ///     19) as a virtual generated column. Every referenced column gets
    ///     its own `deptype = 'a'` edge, so dropping any one of them drops
    ///     the whole statistics object. `dep_schema` is the statistics
    ///     object's own namespace, which may differ from the table's;
    ///     `dep_table` is the table it is defined on.
    ///
    ///   * `pg_trigger` → a table's own triggers that name the column in
    ///     `UPDATE OF` or read it in `WHEN` (a normal dependency, which
    ///     blocks the column's drop rather than following it). From
    ///     PostgreSQL 13 on (`pg_version`) a partition's clone of its
    ///     parent's trigger (`tgparentid <> 0`) is left out: it goes, and
    ///     comes back, with the parent's trigger. Internal triggers — a
    ///     foreign key's, and before PostgreSQL 15 every clone — are left out
    ///     on every version.
    ///
    /// Each branch excludes extension-owned dependents via the standard
    /// `deptype = 'e'` anti-join, mirroring every other dump query, and
    /// requires `refobjsubid > 0` so we only pick up *column-level*
    /// edges (table-level edges have `refobjsubid = 0` and would
    /// duplicate the index/constraint/policy table dependency).
    fn build_column_dependents_query(schema_filter: &str, pg_version: i32) -> String {
        let not_a_clone = if pg_version >= 130000 {
            "and tg.tgparentid = 0"
        } else {
            ""
        };
        format!(
            "select
                quote_ident(refnsp.nspname) as table_schema,
                quote_ident(refcls.relname) as table_name,
                quote_ident(refatt.attname) as column_name,
                'index' as kind,
                quote_ident(ix_nsp.nspname) as dep_schema,
                quote_ident(ix_tbl.relname) as dep_table,
                quote_ident(ix_cls.relname) as dep_name
            from pg_catalog.pg_depend d
            join pg_catalog.pg_class refcls on refcls.oid = d.refobjid
            join pg_catalog.pg_namespace refnsp on refnsp.oid = refcls.relnamespace
            join pg_catalog.pg_attribute refatt
                on refatt.attrelid = d.refobjid and refatt.attnum = d.refobjsubid
            join pg_catalog.pg_class ix_cls on ix_cls.oid = d.objid
            join pg_catalog.pg_index ix on ix.indexrelid = ix_cls.oid
            join pg_catalog.pg_class ix_tbl on ix_tbl.oid = ix.indrelid
            join pg_catalog.pg_namespace ix_nsp on ix_nsp.oid = ix_tbl.relnamespace
            where d.refclassid = 'pg_catalog.pg_class'::regclass
              and d.classid = 'pg_catalog.pg_class'::regclass
              and d.refobjsubid > 0
              and d.deptype in ('a', 'n', 'i')
              and ix_cls.relkind in ('i', 'I')
              and refnsp.nspname in {schema_filter}
              and refatt.attisdropped = false
              and not exists (
                  select 1 from pg_catalog.pg_depend ext
                  where ext.classid = 'pg_catalog.pg_class'::regclass
                  and ext.objid = ix_cls.oid
                  and ext.objsubid = 0
                  and ext.deptype = 'e'
              )
            union all
            select
                quote_ident(refnsp.nspname),
                quote_ident(refcls.relname),
                quote_ident(refatt.attname),
                'constraint',
                quote_ident(con_nsp.nspname),
                quote_ident(con_tbl.relname),
                quote_ident(con.conname)
            from pg_catalog.pg_depend d
            join pg_catalog.pg_class refcls on refcls.oid = d.refobjid
            join pg_catalog.pg_namespace refnsp on refnsp.oid = refcls.relnamespace
            join pg_catalog.pg_attribute refatt
                on refatt.attrelid = d.refobjid and refatt.attnum = d.refobjsubid
            join pg_catalog.pg_constraint con on con.oid = d.objid
            join pg_catalog.pg_class con_tbl on con_tbl.oid = con.conrelid
            join pg_catalog.pg_namespace con_nsp on con_nsp.oid = con_tbl.relnamespace
            where d.refclassid = 'pg_catalog.pg_class'::regclass
              and d.classid = 'pg_catalog.pg_constraint'::regclass
              and d.refobjsubid > 0
              and d.deptype in ('a', 'n', 'i')
              and refnsp.nspname in {schema_filter}
              and refatt.attisdropped = false
              and not exists (
                  select 1 from pg_catalog.pg_depend ext
                  where ext.classid = 'pg_catalog.pg_constraint'::regclass
                  and ext.objid = con.oid
                  and ext.objsubid = 0
                  and ext.deptype = 'e'
              )
            union all
            select
                quote_ident(refnsp.nspname),
                quote_ident(refcls.relname),
                quote_ident(refatt.attname),
                'policy',
                quote_ident(pol_nsp.nspname),
                quote_ident(pol_tbl.relname),
                quote_ident(pol.polname)
            from pg_catalog.pg_depend d
            join pg_catalog.pg_class refcls on refcls.oid = d.refobjid
            join pg_catalog.pg_namespace refnsp on refnsp.oid = refcls.relnamespace
            join pg_catalog.pg_attribute refatt
                on refatt.attrelid = d.refobjid and refatt.attnum = d.refobjsubid
            join pg_catalog.pg_policy pol on pol.oid = d.objid
            join pg_catalog.pg_class pol_tbl on pol_tbl.oid = pol.polrelid
            join pg_catalog.pg_namespace pol_nsp on pol_nsp.oid = pol_tbl.relnamespace
            where d.refclassid = 'pg_catalog.pg_class'::regclass
              and d.classid = 'pg_catalog.pg_policy'::regclass
              and d.refobjsubid > 0
              and d.deptype in ('a', 'n', 'i')
              and refnsp.nspname in {schema_filter}
              and refatt.attisdropped = false
            union all
            select
                quote_ident(refnsp.nspname),
                quote_ident(refcls.relname),
                quote_ident(refatt.attname),
                'statistic',
                quote_ident(stx_nsp.nspname),
                quote_ident(refcls.relname),
                quote_ident(stx.stxname)
            from pg_catalog.pg_depend d
            join pg_catalog.pg_class refcls on refcls.oid = d.refobjid
            join pg_catalog.pg_namespace refnsp on refnsp.oid = refcls.relnamespace
            join pg_catalog.pg_attribute refatt
                on refatt.attrelid = d.refobjid and refatt.attnum = d.refobjsubid
            join pg_catalog.pg_statistic_ext stx on stx.oid = d.objid
            join pg_catalog.pg_namespace stx_nsp on stx_nsp.oid = stx.stxnamespace
            where d.refclassid = 'pg_catalog.pg_class'::regclass
              and d.classid = 'pg_catalog.pg_statistic_ext'::regclass
              and d.refobjsubid > 0
              and d.deptype in ('a', 'n', 'i')
              and refnsp.nspname in {schema_filter}
              and refatt.attisdropped = false
              and not exists (
                  select 1 from pg_catalog.pg_depend ext
                  where ext.classid = 'pg_catalog.pg_statistic_ext'::regclass
                  and ext.objid = stx.oid
                  and ext.objsubid = 0
                  and ext.deptype = 'e'
              )
            union all
            select
                quote_ident(refnsp.nspname),
                quote_ident(refcls.relname),
                quote_ident(refatt.attname),
                'trigger',
                quote_ident(tg_nsp.nspname),
                quote_ident(tg_tbl.relname),
                quote_ident(tg.tgname)
            from pg_catalog.pg_depend d
            join pg_catalog.pg_class refcls on refcls.oid = d.refobjid
            join pg_catalog.pg_namespace refnsp on refnsp.oid = refcls.relnamespace
            join pg_catalog.pg_attribute refatt
                on refatt.attrelid = d.refobjid and refatt.attnum = d.refobjsubid
            join pg_catalog.pg_trigger tg on tg.oid = d.objid
            join pg_catalog.pg_class tg_tbl on tg_tbl.oid = tg.tgrelid
            join pg_catalog.pg_namespace tg_nsp on tg_nsp.oid = tg_tbl.relnamespace
            where d.refclassid = 'pg_catalog.pg_class'::regclass
              and d.classid = 'pg_catalog.pg_trigger'::regclass
              and d.refobjsubid > 0
              and d.deptype in ('a', 'n', 'i')
              and refnsp.nspname in {schema_filter}
              and refatt.attisdropped = false
              and not tg.tgisinternal
              {not_a_clone}
              and not exists (
                  select 1 from pg_catalog.pg_depend ext
                  where ext.classid = 'pg_catalog.pg_trigger'::regclass
                  and ext.objid = tg.oid
                  and ext.objsubid = 0
                  and ext.deptype = 'e'
              )",
            schema_filter = schema_filter
        )
    }

    async fn fetch_column_dependents_standalone(
        pool: &PgPool,
        schema_filter: &str,
        pg_version: i32,
    ) -> Result<Vec<ColumnDependent>, Error> {
        let rows =
            sqlx::query(Self::build_column_dependents_query(schema_filter, pg_version).as_str())
                .fetch_all(pool)
                .await
                .map_err(|e| Error::other(format!("Failed to fetch column dependents: {e}.")))?;

        let mut dependents = Vec::with_capacity(rows.len());
        for row in rows {
            let kind_str: String = row.get("kind");
            let kind = match kind_str.as_str() {
                "index" => ColumnDependentKind::Index,
                "constraint" => ColumnDependentKind::Constraint,
                "policy" => ColumnDependentKind::Policy,
                "statistic" => ColumnDependentKind::Statistic,
                "trigger" => ColumnDependentKind::Trigger,
                // The query is the only producer of this column, so any
                // value other than the five literals above is a logic
                // error here, not bad data — drop the row rather than
                // letting an unknown kind propagate as a silent miss
                // downstream.
                _ => continue,
            };
            dependents.push(ColumnDependent {
                schema: row.get("table_schema"),
                table: row.get("table_name"),
                column: row.get("column_name"),
                kind,
                dep_schema: row.get("dep_schema"),
                dep_table: row.get("dep_table"),
                dep_name: row.get("dep_name"),
            });
        }
        Ok(dependents)
    }

    /// Rules outside the `_RETURN` rule of a view, with their `pg_depend`
    /// dependencies (see [`RuleDependencies`]).
    ///
    /// The three dependency arrays read only `pg_depend`, `pg_class`,
    /// `pg_attribute` and `pg_proc` columns that every supported server has, so
    /// the query is the same on PostgreSQL 14 through 19. A rule reading only
    /// some columns of a relation has `pg_depend` rows for those columns and
    /// none for the relation itself, so `rule_relations` is taken over both.
    fn build_rules_query(schema_filter: &str) -> String {
        format!(
            "select
                quote_ident(n.nspname) as rule_schema,
                quote_ident(c.relname) as rule_table,
                quote_ident(r.rulename) as rule_name,
                pg_get_ruledef(r.oid, true) as rule_definition,
                d.description as rule_comment,
                array(
                    select distinct dn.nspname || '.' || dc.relname
                    from pg_depend dep
                    join pg_class dc on dc.oid = dep.refobjid
                    join pg_namespace dn on dn.oid = dc.relnamespace
                    where dep.classid = 'pg_rewrite'::regclass
                      and dep.objid = r.oid
                      and dep.refclassid = 'pg_class'::regclass
                      and dep.deptype = 'n'
                    order by 1
                ) as rule_relations,
                array(
                    select distinct dn.nspname || '.' || dc.relname || '.' || da.attname
                    from pg_depend dep
                    join pg_class dc on dc.oid = dep.refobjid
                    join pg_namespace dn on dn.oid = dc.relnamespace
                    join pg_attribute da on da.attrelid = dc.oid and da.attnum = dep.refobjsubid
                    where dep.classid = 'pg_rewrite'::regclass
                      and dep.objid = r.oid
                      and dep.refclassid = 'pg_class'::regclass
                      and dep.deptype = 'n'
                      and dep.refobjsubid > 0
                    order by 1
                ) as rule_columns,
                array(
                    select distinct quote_ident(pn.nspname) || '.' || quote_ident(p.proname)
                        || '(' || pg_get_function_identity_arguments(p.oid) || ')'
                    from pg_depend dep
                    join pg_proc p on p.oid = dep.refobjid
                    join pg_namespace pn on pn.oid = p.pronamespace
                    where dep.classid = 'pg_rewrite'::regclass
                      and dep.objid = r.oid
                      and dep.refclassid = 'pg_proc'::regclass
                      and dep.deptype = 'n'
                    order by 1
                ) as rule_routines
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
                    where ext_dep.classid = 'pg_class'::regclass
                    and ext_dep.objid = c.oid
                    and ext_dep.objsubid = 0
                    and ext_dep.deptype = 'e'
                )
            order by n.nspname, c.relname, r.rulename",
            schema_filter
        )
    }

    async fn fetch_rules_standalone(
        pool: &PgPool,
        schema_filter: &str,
    ) -> Result<Vec<Rule>, Error> {
        let rows = sqlx::query(Self::build_rules_query(schema_filter).as_str())
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
                rule.depends_on = Some(RuleDependencies {
                    relations: row.get("rule_relations"),
                    columns: row.get("rule_columns"),
                    routines: row.get("rule_routines"),
                });
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

    fn build_collations_query(
        coll_locale_col: &str,
        icu_locale_col: &str,
        icu_rules_col: &str,
        schema_filter: &str,
    ) -> String {
        format!(
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
               AND NOT EXISTS
               (
                    SELECT 1
                    FROM
                        pg_depend ext
                    WHERE
                        ext.classid = 'pg_collation'::regclass AND
                        ext.objid = c.oid AND
                        ext.objsubid = 0 AND
                        ext.deptype = 'e'
                    )
             ORDER BY n.nspname, c.collname",
            coll_locale_col,
            icu_locale_col,
            icu_rules_col,
            schema_filter
        )
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

        let query = Self::build_collations_query(
            coll_locale_col,
            icu_locale_col,
            icu_rules_col,
            schema_filter,
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

    fn build_ts_configs_query(schema_filter: &str) -> String {
        format!(
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
               AND NOT EXISTS
               (
                    SELECT 1
                    FROM
                        pg_depend ext
                    WHERE
                        ext.classid = 'pg_ts_config'::regclass AND
                        ext.objid = c.oid AND
                        ext.objsubid = 0 AND
                        ext.deptype = 'e'
                    )
             ORDER BY n.nspname, c.cfgname",
            schema_filter
        )
    }

    fn build_ts_config_mappings_query(schema_filter: &str) -> String {
        format!(
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
               AND NOT EXISTS
               (
                    SELECT 1
                    FROM
                        pg_depend ext
                    WHERE
                        ext.classid = 'pg_ts_config'::regclass AND
                        ext.objid = c.oid AND
                        ext.objsubid = 0 AND
                        ext.deptype = 'e'
                )
             GROUP BY c.cfgnamespace, c.oid, c.cfgname, t.alias
             ORDER BY c.oid, t.alias",
            schema_filter
        )
    }

    fn build_ts_config_oids_query(schema_filter: &str) -> String {
        format!(
            "SELECT
                c.oid as cfg_oid,
                quote_ident(n.nspname) as cfg_schema,
                quote_ident(c.cfgname) as cfg_name
             FROM pg_ts_config c
             JOIN pg_namespace n ON n.oid = c.cfgnamespace
             WHERE n.nspname IN {}
               AND NOT EXISTS
               (
                    SELECT 1
                    FROM
                        pg_depend ext
                    WHERE
                        ext.classid = 'pg_ts_config'::regclass AND
                        ext.objid = c.oid AND
                        ext.objsubid = 0 AND
                        ext.deptype = 'e'
                    )
             ORDER BY n.nspname, c.cfgname",
            schema_filter
        )
    }

    async fn fetch_ts_configs_standalone(
        pool: &PgPool,
        schema_filter: &str,
    ) -> Result<Vec<TextSearchConfig>, Error> {
        let config_rows = sqlx::query(Self::build_ts_configs_query(schema_filter).as_str())
            .fetch_all(pool)
            .await
            .map_err(|e| Error::other(format!("Failed to fetch text search configs: {e}.")))?;

        if config_rows.is_empty() {
            println!("No user-defined text search configurations found.");
            return Ok(Vec::new());
        }

        // Fetch mappings for each config using pg_ts_config_map
        let mapping_rows =
            sqlx::query(Self::build_ts_config_mappings_query(schema_filter).as_str())
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
        let oid_rows = sqlx::query(Self::build_ts_config_oids_query(schema_filter).as_str())
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

    fn build_ts_dicts_query(schema_filter: &str) -> String {
        format!(
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
               AND NOT EXISTS
               (
                    SELECT 1
                    FROM
                        pg_depend ext
                    WHERE
                        ext.classid = 'pg_ts_dict'::regclass AND
                        ext.objid = d.oid AND
                        ext.objsubid = 0 AND
                        ext.deptype = 'e'
                )
             ORDER BY n.nspname, d.dictname",
            schema_filter
        )
    }

    async fn fetch_ts_dicts_standalone(
        pool: &PgPool,
        schema_filter: &str,
    ) -> Result<Vec<TextSearchDict>, Error> {
        let rows = sqlx::query(Self::build_ts_dicts_query(schema_filter).as_str())
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

    fn build_casts_query(schema_filter: &str) -> String {
        format!(
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
            WHERE NOT EXISTS
            (
                SELECT 1
                FROM
                    pg_depend pd
                WHERE
                    pd.classid = 'pg_cast'::regclass AND
                    pd.objid = c.oid AND
                    pd.objsubid = 0 AND
                    pd.deptype IN ('e', 'i')
            )
              AND (c.castsource IN (SELECT oid FROM user_types)
                   OR c.casttarget IN (SELECT oid FROM user_types))
            ORDER BY source_type, target_type",
            schema_filter
        )
    }

    async fn fetch_casts_standalone(
        pool: &PgPool,
        schema_filter: &str,
    ) -> Result<Vec<Cast>, Error> {
        let rows = sqlx::query(Self::build_casts_query(schema_filter).as_str())
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

    fn build_operators_query(schema_filter: &str) -> String {
        format!(
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
               AND NOT EXISTS
               (
                    SELECT 1
                    FROM
                        pg_depend ext
                    WHERE
                        ext.classid = 'pg_operator'::regclass AND
                        ext.objid = o.oid AND
                        ext.objsubid = 0 AND
                        ext.deptype = 'e'
                )
             ORDER BY n.nspname, o.oprname",
            schema_filter
        )
    }

    async fn fetch_operators_standalone(
        pool: &PgPool,
        schema_filter: &str,
    ) -> Result<Vec<Operator>, Error> {
        let rows = sqlx::query(Self::build_operators_query(schema_filter).as_str())
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

    /// The `pg_publication` query. Columns a server does not have are replaced
    /// by their default, as pg_dump does: `puballsequences` (PostgreSQL 19+)
    /// by `false`, `pubgencols` (PostgreSQL 18+) by NULL, which the dump reads
    /// as the default `none`.
    pub(crate) fn build_publications_query(pg_version: i32) -> String {
        let all_sequences = if pg_version >= 190000 {
            "p.puballsequences"
        } else {
            "false"
        };
        let generated_columns = if pg_version >= 180000 {
            "CASE p.pubgencols WHEN 'n' THEN NULL WHEN 's' THEN 'stored' ELSE p.pubgencols::text END"
        } else {
            "NULL::text"
        };
        format!(
            "SELECT
                quote_ident(p.pubname) as pub_name,
                COALESCE(quote_ident(r.rolname), '') as pub_owner,
                p.puballtables as all_tables,
                {all_sequences} as all_sequences,
                CONCAT_WS(',',
                    CASE WHEN p.pubinsert THEN 'insert' ELSE NULL END,
                    CASE WHEN p.pubupdate THEN 'update' ELSE NULL END,
                    CASE WHEN p.pubdelete THEN 'delete' ELSE NULL END,
                    CASE WHEN p.pubtruncate THEN 'truncate' ELSE NULL END
                ) as publish,
                p.pubviaroot as via_partition_root,
                {generated_columns} as publish_generated_columns,
                d.description as pub_comment
             FROM pg_publication p
             LEFT JOIN pg_roles r ON r.oid = p.pubowner
             LEFT JOIN pg_description d ON d.objoid = p.oid
                 AND d.classoid = 'pg_publication'::regclass AND d.objsubid = 0
             ORDER BY p.pubname"
        )
    }

    /// The `pg_publication_rel` query: one row per `FOR TABLE` member, and on
    /// PostgreSQL 19+ one per `FOR ALL TABLES EXCEPT` entry (`is_except`).
    ///
    /// Column lists and row filters exist from PostgreSQL 15; the column list
    /// is returned as `quote_ident`'ed names sorted by name, so the same set
    /// compares equal across databases whose attribute numbers differ. On 19
    /// the member branch must also exclude `prexcept` rows, or excluded tables
    /// would read as members; before 19 an all-tables publication has no rows.
    pub(crate) fn build_publication_rels_query(pg_version: i32) -> String {
        let (is_except, filter) = if pg_version >= 190000 {
            (
                "pr.prexcept",
                "(NOT p.puballtables AND NOT pr.prexcept) OR (p.puballtables AND pr.prexcept)",
            )
        } else {
            ("false", "NOT p.puballtables")
        };
        let (columns, row_filter) = if pg_version >= 150000 {
            (
                "CASE WHEN pr.prattrs IS NULL THEN NULL ELSE ARRAY(
                    SELECT quote_ident(a.attname)
                    FROM pg_attribute a
                    WHERE a.attrelid = pr.prrelid AND a.attnum = ANY (pr.prattrs)
                    ORDER BY a.attname
                ) END",
                "pg_get_expr(pr.prqual, pr.prrelid)",
            )
        } else {
            ("NULL::text[]", "NULL::text")
        };
        format!(
            "SELECT
                quote_ident(p.pubname) as pub_name,
                quote_ident(n.nspname) || '.' || quote_ident(c.relname) as table_name,
                {is_except} as is_except,
                {columns} as columns,
                {row_filter} as row_filter
             FROM pg_publication p
             JOIN pg_publication_rel pr ON pr.prpubid = p.oid
             JOIN pg_class c ON c.oid = pr.prrelid
             JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE {filter}
             ORDER BY p.pubname, n.nspname, c.relname"
        )
    }

    /// The `FOR TABLES IN SCHEMA` query (`pg_publication_namespace`), or `None`
    /// before PostgreSQL 15, which has neither the catalog nor the feature.
    pub(crate) fn build_publication_schemas_query(pg_version: i32) -> Option<&'static str> {
        (pg_version >= 150000).then_some(
            "SELECT
                quote_ident(p.pubname) as pub_name,
                quote_ident(n.nspname) as schema_name
             FROM pg_publication p
             JOIN pg_publication_namespace pn ON pn.pnpubid = p.oid
             JOIN pg_namespace n ON n.oid = pn.pnnspid
             ORDER BY p.pubname, n.nspname",
        )
    }

    async fn fetch_publications_standalone(
        pool: &PgPool,
        pg_version: i32,
    ) -> Result<Vec<Publication>, Error> {
        let pub_rows = sqlx::query(&Self::build_publications_query(pg_version))
            .fetch_all(pool)
            .await
            .map_err(|e| Error::other(format!("Failed to fetch publications: {e}.")))?;

        if pub_rows.is_empty() {
            println!("No publications found.");
            return Ok(Vec::new());
        }

        // Fetch table memberships (and, on PostgreSQL 19+, EXCEPT entries)
        let table_rows = sqlx::query(&Self::build_publication_rels_query(pg_version))
            .fetch_all(pool)
            .await
            .map_err(|e| Error::other(format!("Failed to fetch publication tables: {e}.")))?;

        let mut table_map: HashMap<String, Vec<String>> = HashMap::new();
        let mut spec_map: HashMap<String, BTreeMap<String, PublicationTableSpec>> = HashMap::new();
        let mut except_map: HashMap<String, Vec<String>> = HashMap::new();
        for row in table_rows {
            let pub_name: String = row.get("pub_name");
            let table_name: String = row.get("table_name");
            if row.get::<bool, _>("is_except") {
                except_map.entry(pub_name).or_default().push(table_name);
                continue;
            }
            let spec = PublicationTableSpec {
                columns: row
                    .get::<Option<Vec<String>>, _>("columns")
                    .unwrap_or_default(),
                row_filter: row.get("row_filter"),
            };
            if !spec.is_empty() {
                spec_map
                    .entry(pub_name.clone())
                    .or_default()
                    .insert(table_name.clone(), spec);
            }
            table_map.entry(pub_name).or_default().push(table_name);
        }

        let mut schema_map: HashMap<String, Vec<String>> = HashMap::new();
        if let Some(query) = Self::build_publication_schemas_query(pg_version) {
            let schema_rows = sqlx::query(query)
                .fetch_all(pool)
                .await
                .map_err(|e| Error::other(format!("Failed to fetch publication schemas: {e}.")))?;
            for row in schema_rows {
                let pub_name: String = row.get("pub_name");
                schema_map
                    .entry(pub_name)
                    .or_default()
                    .push(row.get("schema_name"));
            }
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
                all_sequences: row.get("all_sequences"),
                publish: row.get("publish"),
                tables,
                table_specs: spec_map.remove(&pub_name).unwrap_or_default(),
                schemas: schema_map.remove(&pub_name).unwrap_or_default(),
                except_tables: except_map.remove(&pub_name).unwrap_or_default(),
                via_partition_root: row.get("via_partition_root"),
                publish_generated_columns: row.get("publish_generated_columns"),
                comment: row.get("pub_comment"),
                hash: None,
            };
            pub_.hash();
            println!(" - {}", pub_.name);
            publications.push(pub_);
        }

        Ok(publications)
    }

    /// The `pg_subscription` query. `pg_version` gates the PG19 columns
    /// (`subserver`, `subretaindeadtuples`, `submaxretention`,
    /// `subwalrcvtimeout`); older servers get typed literals in their place,
    /// the same way `pg_dump` does.
    ///
    /// `pg_subscription` is a shared catalog, so rows are restricted to the
    /// current database — without that filter every database's
    /// subscriptions leaked into every dump. `subconninfo` is NULL for a
    /// PG19 `SERVER` subscription and is read as `''`.
    ///
    /// `subslotname` (every version) is read as whether it is NULL
    /// (`slot_name = NONE`) and its name when that is not the subscription's
    /// own (see `SubscriptionSlot::from_catalog`).
    fn build_subscriptions_query(pg_version: i32) -> String {
        let (server_col, server_join, rdt_col, mrd_col, wrt_col) = if pg_version >= 190000 {
            (
                "quote_ident(fs.srvname)",
                "\n             LEFT JOIN pg_foreign_server fs ON fs.oid = s.subserver",
                "s.subretaindeadtuples",
                "s.submaxretention",
                "NULLIF(s.subwalrcvtimeout, '-1')",
            )
        } else {
            ("NULL::text", "", "false", "0", "NULL::text")
        };
        format!(
            "SELECT
                quote_ident(s.subname) as sub_name,
                COALESCE(quote_ident(r.rolname), '') as sub_owner,
                COALESCE(s.subconninfo, '') as sub_conninfo,
                {server_col} as sub_server,
                COALESCE(s.subpublications, '{{}}'::text[]) as sub_publications,
                s.subenabled as sub_enabled,
                s.subslotname IS NULL as sub_slot_is_none,
                NULLIF(s.subslotname, s.subname)::text as sub_slot_name,
                {rdt_col} as sub_retain_dead_tuples,
                {mrd_col} as sub_max_retention,
                {wrt_col} as sub_wal_receiver_timeout,
                d.description as sub_comment
             FROM pg_subscription s
             LEFT JOIN pg_roles r ON r.oid = s.subowner{server_join}
             LEFT JOIN pg_description d ON d.objoid = s.oid
                 AND d.classoid = 'pg_subscription'::regclass AND d.objsubid = 0
             WHERE s.subdbid = (SELECT oid FROM pg_database WHERE datname = current_database())
             ORDER BY s.subname"
        )
    }

    /// Whether `e` is PostgreSQL's `insufficient_privilege` (SQLSTATE `42501`).
    ///
    /// `pg_subscription.subconninfo` is not readable by `PUBLIC`, so a dump
    /// taken by a role without `SELECT` on it (typically any non-superuser)
    /// fails with exactly this error; that case degrades to "no
    /// subscriptions", as it always has. Any other failure — a wrong catalog
    /// column, a lost connection — must not, because an empty list would make
    /// a compare drop every subscription.
    fn is_insufficient_privilege(e: &sqlx::Error) -> bool {
        e.as_database_error()
            .and_then(|db| db.code())
            .is_some_and(|code| code == "42501")
    }

    async fn fetch_subscriptions_standalone(
        pool: &PgPool,
        pg_version: i32,
    ) -> Result<Vec<Subscription>, Error> {
        let result = sqlx::query(&Self::build_subscriptions_query(pg_version))
            .fetch_all(pool)
            .await;

        match result {
            Err(e) if Self::is_insufficient_privilege(&e) => {
                println!("Warning: could not fetch subscriptions (insufficient privileges): {e}.");
                Ok(Vec::new())
            }
            Err(e) => Err(Error::other(format!("Failed to fetch subscriptions: {e}."))),
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
                            server: row.get("sub_server"),
                            publications: row.get("sub_publications"),
                            enabled: row.get("sub_enabled"),
                            slot_name: SubscriptionSlot::from_catalog(
                                row.get("sub_slot_is_none"),
                                row.get("sub_slot_name"),
                            ),
                            retain_dead_tuples: row.get("sub_retain_dead_tuples"),
                            max_retention_duration: row.get("sub_max_retention"),
                            wal_receiver_timeout: row.get("sub_wal_receiver_timeout"),
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

    /// The `pg_foreign_data_wrapper` query. `pg_version` gates the PG19
    /// `fdwconnection` column (the `CONNECTION` function `CREATE SUBSCRIPTION
    /// ... SERVER` calls); older servers get `NULL::text` in its place.
    /// Extension-owned wrappers (e.g. `postgres_fdw`) are excluded.
    fn build_fdws_query(pg_version: i32) -> String {
        let (connection_col, connection_join) = if pg_version >= 190000 {
            (
                "CASE WHEN fdw.fdwconnection != 0 THEN
                quote_ident(cn.nspname) || '.' || quote_ident(cp.proname)
            ELSE NULL END",
                "\n         LEFT JOIN pg_proc cp ON cp.oid = fdw.fdwconnection
         LEFT JOIN pg_namespace cn ON cn.oid = cp.pronamespace",
            )
        } else {
            ("NULL::text", "")
        };
        format!(
            "SELECT
            quote_ident(fdw.fdwname) as fdw_name,
            COALESCE(quote_ident(r.rolname), '') as fdw_owner,
            CASE WHEN fdw.fdwhandler != 0 THEN
                quote_ident(hn.nspname) || '.' || quote_ident(hp.proname)
            ELSE NULL END as handler_func,
            CASE WHEN fdw.fdwvalidator != 0 THEN
                quote_ident(vn.nspname) || '.' || quote_ident(vp.proname)
            ELSE NULL END as validator_func,
            {connection_col} as connection_func,
            COALESCE(fdw.fdwoptions, '{{}}'::text[]) as fdw_options,
            d.description as fdw_comment
         FROM pg_foreign_data_wrapper fdw
         LEFT JOIN pg_roles r ON r.oid = fdw.fdwowner
         LEFT JOIN pg_proc hp ON hp.oid = fdw.fdwhandler
         LEFT JOIN pg_namespace hn ON hn.oid = hp.pronamespace
         LEFT JOIN pg_proc vp ON vp.oid = fdw.fdwvalidator
         LEFT JOIN pg_namespace vn ON vn.oid = vp.pronamespace{connection_join}
         LEFT JOIN pg_description d ON d.objoid = fdw.oid
             AND d.classoid = 'pg_foreign_data_wrapper'::regclass AND d.objsubid = 0
         WHERE NOT EXISTS
         (
            SELECT 1
            FROM
                pg_depend ext
            WHERE
                ext.classid = 'pg_foreign_data_wrapper'::regclass AND
                ext.objid = fdw.oid AND
                ext.objsubid = 0 AND
                ext.deptype = 'e'
        )
         ORDER BY fdw.fdwname"
        )
    }

    async fn fetch_fdws_standalone(
        pool: &PgPool,
        pg_version: i32,
    ) -> Result<Vec<ForeignDataWrapper>, Error> {
        let rows = sqlx::query(&Self::build_fdws_query(pg_version))
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
                    connection_func: row.get("connection_func"),
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

    fn build_servers_query() -> &'static str {
        "SELECT
            quote_ident(s.srvname) as srv_name,
            COALESCE(quote_ident(r.rolname), '') as srv_owner,
            quote_ident(fdw.fdwname) as fdw_name,
            s.srvtype as srv_type,
            s.srvversion as srv_version,
            COALESCE(s.srvoptions, '{}'::text[]) as srv_options,
            d.description as srv_comment,
            COALESCE(s.srvacl::text[], '{}'::text[]) as srv_acl
         FROM pg_foreign_server s
         JOIN pg_foreign_data_wrapper fdw ON fdw.oid = s.srvfdw
         LEFT JOIN pg_roles r ON r.oid = s.srvowner
         LEFT JOIN pg_description d ON d.objoid = s.oid
             AND d.classoid = 'pg_foreign_server'::regclass AND d.objsubid = 0
         WHERE NOT EXISTS
         (
            SELECT 1
            FROM
                pg_depend ext
            WHERE
                ext.classid = 'pg_foreign_server'::regclass AND
                ext.objid = s.oid AND
                ext.objsubid = 0 AND
                ext.deptype = 'e'
        )
         ORDER BY s.srvname"
    }

    async fn fetch_servers_standalone(pool: &PgPool) -> Result<Vec<ForeignServer>, Error> {
        let rows = sqlx::query(Self::build_servers_query())
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
                    acl: row.get("srv_acl"),
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

    /// Read a dump from a file and deserialize it.
    ///
    /// Stored hashes are not always taken verbatim: routines whose source was
    /// saved with CRLF line endings, every user-defined type, and — in a dump
    /// written by pgc 1.0.x — every table with a collated column, are rehashed
    /// on load, so a dump written by an older pgc compares correctly against
    /// one written by this version.
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
        // Not in a SQL-standard body: a carriage return there is inside a
        // literal (see `Routine::is_sql_body`), and changing it changes the value.
        for routine in &mut dump.routines {
            if !routine.is_sql_body && routine.source_code.contains("\r\n") {
                routine.source_code = crate::utils::string_extensions::normalize_line_endings(
                    std::mem::take(&mut routine.source_code),
                );
                routine.hash();
            }
        }

        // Rehash every type. pgc 1.0.x hashed a type before its domain
        // constraints and enum labels were loaded, so the hash it stored
        // ignores both, and two such dumps that differ only in a domain CHECK
        // or an enum label look identical. `PgType::hash` reads serialized
        // fields only, so this recovers the right hash from any dump, and it
        // is a no-op for a dump this version wrote.
        for pg_type in &mut dump.types {
            pg_type.hash();
        }

        // pgc 1.0.x stored a materialized view's schema, name and owner raw
        // (see `build_materialized_views_query`), so its SQL named a view
        // `s.MyMv` that the server reads as `s.mymv`, and a 1.0.x dump would
        // not match a current dump of the same view.
        if dump.server_version_num.is_none() {
            dump.quote_legacy_materialized_view_names();
            dump.separate_legacy_partition_trigger_clones();
            dump.rehash_legacy_collated_tables();
        }

        Ok(dump)
    }

    /// Rehashes every table of a dump written by pgc 1.0.x
    /// (`server_version_num` is `None`) that has a column with a collation of
    /// its own.
    ///
    /// A column's collation is part of its table's hash from pgc 1.1 on — a
    /// collation change alone used to leave the hash as it was, so it was
    /// never migrated. The hash a 1.0.x dump stored for such a table leaves
    /// the collation out, and would not match a current dump of the same
    /// table. `Table::hash` reads serialized fields only, so rehashing gives
    /// the hash this version computes; every other table keeps the hash
    /// stored in the file, which the collation never touched.
    fn rehash_legacy_collated_tables(&mut self) {
        for table in &mut self.tables {
            if table
                .columns
                .iter()
                .any(|c| c.collation_name.as_deref().is_some_and(|n| !n.is_empty()))
            {
                table.hash();
            }
        }
    }

    /// Moves the partition trigger clones a dump written by pgc 1.0.x lists
    /// among a partition's own triggers to [`Table::trigger_clones`], where
    /// a current dump keeps them — or leaves them out, like a current dump,
    /// when they are in their parent trigger's state.
    ///
    /// pgc 1.0.x read every trigger that is not `tgisinternal`, which from
    /// PostgreSQL 15 on includes the clone of a partitioned parent's row
    /// trigger on each partition. It recorded no `tgparentid`, so a clone is
    /// recognised the way PostgreSQL names it: a trigger of a partition
    /// named after a row trigger its parent lists (the parent's own, or on
    /// a sub-partition the parent's clone), which a partition cannot have
    /// besides the clone. Its state is compared with that trigger's, and a
    /// table whose triggers change is rehashed; every other table keeps the
    /// hash stored in the file. A dump of PostgreSQL 14 or older lists no
    /// clone, so nothing changes.
    fn separate_legacy_partition_trigger_clones(&mut self) {
        let index: HashMap<String, usize> = self
            .tables
            .iter()
            .enumerate()
            .map(|(i, t)| (format!("{}.{}", t.schema, t.name), i))
            .collect();
        // Decided on the lists as stored, before any of them changes, so a
        // sub-partition's clone is compared with its parent's clone.
        let mut plans: Vec<(usize, Vec<(usize, bool)>)> = Vec::new();
        for (i, table) in self.tables.iter().enumerate() {
            let Some(&parent) = table.partition_of.as_deref().and_then(|reference| {
                index.get(reference).or_else(|| {
                    let unquoted = reference.replace('"', "");
                    index
                        .iter()
                        .find(|(key, _)| key.replace('"', "") == unquoted)
                        .map(|(_, i)| i)
                })
            }) else {
                continue;
            };
            let parent = &self.tables[parent];
            let clones: Vec<(usize, bool)> = table
                .triggers
                .iter()
                .enumerate()
                .filter_map(|(j, trigger)| {
                    let from = parent
                        .triggers
                        .iter()
                        .find(|p| p.name == trigger.name && p.is_row_level())?;
                    Some((
                        j,
                        trigger.enabled != from.enabled || trigger.comment.is_some(),
                    ))
                })
                .collect();
            if !clones.is_empty() {
                plans.push((i, clones));
            }
        }
        for (i, clones) in plans {
            let table = &mut self.tables[i];
            let triggers = std::mem::take(&mut table.triggers);
            for (j, trigger) in triggers.into_iter().enumerate() {
                match clones.iter().find(|(k, _)| *k == j) {
                    Some((_, true)) => table.trigger_clones.push(trigger),
                    Some((_, false)) => {}
                    None => table.triggers.push(trigger),
                }
            }
            table.hash();
        }
    }

    /// Spells the materialized-view schema, name and owner of a dump written by
    /// pgc 1.0.x (`server_version_num` is `None`) the way `quote_ident` does,
    /// which is how every other name in a dump, and every name in a current
    /// dump, is stored. A view whose names change is rehashed, since its name is
    /// part of the hash; every other view keeps the hash stored in the file.
    ///
    /// Such a dump does not say which server it came from, so a keyword that
    /// only PostgreSQL 16 or 17 quotes (`json_object`, `json`, `merge_action`)
    /// is left bare here, as PostgreSQL 14 and 15 leave it; the comparer
    /// quotes it once the other dump tells it the version (see
    /// [`Dump::requote_legacy_materialized_view_names`]).
    fn quote_legacy_materialized_view_names(&mut self) {
        self.requote_materialized_view_names(None);
    }

    /// Quotes, in a dump written by pgc 1.0.x, the materialized-view schemas,
    /// names and owners that `quote_ident` quotes on a server of
    /// `server_version_num` but that loading the dump left bare: the
    /// keywords PostgreSQL 16 and 17 added (see
    /// [`quote_identifier_for_server`]). Every other name was spelled on
    /// load. A view whose names change is rehashed. Nothing happens to a dump
    /// that records its own server version.
    ///
    /// The comparer calls it with the version of the other dump, the only
    /// one it knows: a pgc 1.0.x dump of a PostgreSQL 17 view named `json`
    /// then matches a current dump's `"json"`.
    ///
    /// [`quote_identifier_for_server`]: crate::utils::string_extensions::quote_identifier_for_server
    pub(crate) fn requote_legacy_materialized_view_names(&mut self, server_version_num: i32) {
        if self.server_version_num.is_none() {
            self.requote_materialized_view_names(Some(server_version_num));
        }
    }

    /// The loop of [`Dump::quote_legacy_materialized_view_names`] and
    /// [`Dump::requote_legacy_materialized_view_names`], with the keyword
    /// list of `server_version_num` (`None`: PostgreSQL 14's).
    fn requote_materialized_view_names(&mut self, server_version_num: Option<i32>) {
        for view in self.views.iter_mut().filter(|v| v.is_materialized) {
            let mut renamed = false;
            for name in [&mut view.schema, &mut view.name, &mut view.owner] {
                if let Some(quoted) = Self::quote_raw_identifier(name, server_version_num) {
                    *name = quoted;
                    renamed = true;
                }
            }
            if renamed {
                view.hash();
            }
        }
    }

    /// `quote_ident(raw)` on a server of `server_version_num` (`None`: the
    /// oldest supported, PostgreSQL 14) for a raw catalog name that server
    /// would quote, `None` for one it leaves bare (or an empty owner).
    ///
    /// The server leaves a name bare when it is a lowercase ASCII letter or `_`
    /// followed by lowercase letters, digits and `_`, and not a keyword that
    /// its `quote_ident` quotes — a reserved, column-name or type/function-name
    /// keyword of that version, so `left`, `int` and `position` are quoted as
    /// well as `select`, and `json_object` from PostgreSQL 16 on
    /// ([`quote_identifier_for_server`] decides it). A value already in
    /// quotes is taken to be quoted: no raw name pgc 1.0.x could store is
    /// told apart from a quoted one that way, but a hand-built dump may carry
    /// quoted names.
    ///
    /// [`quote_identifier_for_server`]: crate::utils::string_extensions::quote_identifier_for_server
    fn quote_raw_identifier(raw: &str, server_version_num: Option<i32>) -> Option<String> {
        let already_quoted = raw.len() >= 2 && raw.starts_with('"') && raw.ends_with('"');
        if raw.is_empty() || already_quoted {
            return None;
        }
        match crate::utils::string_extensions::quote_identifier_for_server(raw, server_version_num)
        {
            std::borrow::Cow::Borrowed(_) => None,
            std::borrow::Cow::Owned(quoted) => Some(quoted),
        }
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
        // A bad port is a configuration error, not a connection failure, and
        // saying so is the whole point of issue #244 — the old URL path
        // reported every unparseable credential as `invalid port number`.
        let connect_options = self.configuration.get_connect_options().map_err(|e| {
            Error::other(format!(
                "Invalid connection configuration ({}): {}.",
                self.configuration.get_masked_connection_string(),
                e
            ))
        })?;

        let pool = pool_options(max_connections)
            .connect_with(connect_options)
            .await
            .map_err(|e| {
                Error::other(format!(
                    "Failed to connect to database ({}): {}.",
                    self.configuration.get_masked_connection_string(),
                    e
                ))
            })?;

        self.fill(&pool, WithoutSchemas::ServerFactsOnly).await?;

        // After the fill, on its own: it needs no connection the fill's
        // parallel branches might be waiting for.
        if !self.schemas.is_empty() {
            let schema_filter = self.accessible_schema_filter();
            match super::drop_order::fetch_drop_dependencies(&pool, &schema_filter).await {
                Ok(dependencies) => self.drop_dependencies = dependencies,
                Err(e) => eprintln!(
                    "Warning: {e} The clear script is ordered by what the dumped objects \
                     show on their own, and may fail on a dependency it cannot see."
                ),
            }
        }

        pool.close().await;
        Ok(())
    }

    /// Generate a SQL script that drops all objects found in this dump.
    ///
    /// The drops come in sections — event triggers (ahead of every drop that
    /// would fire them), views (with the routines whose SQL-standard body
    /// reads a relation), foreign keys and tables, foreign tables,
    /// statistics, casts, operators, routines, sequences, types, text search
    /// configurations and dictionaries, collations, extensions, schemas — and
    /// are then reordered wherever PostgreSQL records that one object cannot
    /// be dropped before another ([`Dump::drop_dependencies`], read by
    /// [`inspect`](Self::inspect) from `pg_depend`): a domain whose `CHECK`
    /// calls a function before the function, a table whose rule reads a view
    /// before the view, an `INHERITS` child before its parent, an aggregate
    /// before its state function. A schema without such a dependency keeps
    /// the section order exactly.
    ///
    /// Database-wide objects the dump holds — subscriptions, user mappings,
    /// foreign servers, foreign-data wrappers, event triggers — are dropped
    /// only when one of those dependencies ties them to something the script
    /// drops inside the selected schemas (a wrapper using the handler of an
    /// extension installed in one of them, a server on that wrapper, a
    /// subscription on that server, an event trigger calling one of the
    /// routines), and never while something outside the selected schemas
    /// still depends on them (a foreign table in another schema on that
    /// server). Without dependency data (a hand-built dump) none of them is,
    /// and the order is inferred from the dumped objects: views on views,
    /// SQL-standard bodies on what they use, aggregates on their support
    /// functions, partitions and `INHERITS` children before their parents,
    /// domains before their base types and the routines their constraints
    /// call.
    ///
    /// `use_cascade` is destructive across schema boundaries: `CASCADE` can drop
    /// dependents outside the selected schemas. Without it, the ordering above
    /// is the only guarantee, and an object in another schema that depends on
    /// a dropped one, or a dependency cycle with no part that can be dropped on
    /// its own, makes the script fail rather than silently over-drop.
    ///
    /// ```
    /// # use pgc::config::dump_config::DumpConfig;
    /// use pgc::dump::core::Dump;
    /// use pgc::dump::schema::Schema;
    ///
    /// # let config = DumpConfig {
    /// #     host: "localhost".to_string(), port: "5432".to_string(),
    /// #     user: "postgres".to_string(), password: String::new(),
    /// #     database: "shop".to_string(), scheme: "public".to_string(),
    /// #     ssl: false, file: String::new(),
    /// # };
    /// let mut dump = Dump::new(config);
    /// dump.schemas.push(Schema::new("app".to_string(), "app".to_string(), None));
    ///
    /// let script = dump.generate_clear_script(
    ///     true,   // use_single_transaction
    ///     true,   // use_comments
    ///     false,  // use_cascade
    /// );
    ///
    /// assert!(script.contains("begin;"));
    /// assert!(script.contains("drop schema if exists app;"));
    /// assert!(script.contains("commit;"));
    /// assert!(!script.to_lowercase().contains("cascade"));
    /// ```
    pub fn generate_clear_script(
        &self,
        use_single_transaction: bool,
        use_comments: bool,
        use_cascade: bool,
    ) -> String {
        use crate::utils::string_extensions::{StringExt, block_comment_safe};

        let mut script = String::new();

        script.push_str("/*\n");
        script.append_block(" Script generated by PostgreSQL Comparer (clear command)");
        script.push_str(&format!(
            " Database: {}\n",
            block_comment_safe(&self.configuration.database)
        ));
        script.push_str(&format!(
            " Schema(s): {}\n",
            block_comment_safe(&self.configuration.scheme)
        ));
        script.push_str(&format!("\n{}\n", self.get_info()));
        script.append_block("*/");

        if use_single_transaction {
            script.append_block("begin;");
        }

        let (steps, views_with_routines) = self.clear_steps(use_cascade);
        let steps = self.select_clear_steps(steps);
        let mut section = None;
        for step in &self.order_clear_steps(steps) {
            if section != Some(step.section) {
                if use_comments {
                    script.append_block(step.section.banner(views_with_routines));
                }
                section = Some(step.section);
            }
            if use_comments {
                script.push_str(&step.comment);
            }
            script.push_str(&step.statement);
        }

        if use_single_transaction {
            script.append_block("\ncommit;");
        }

        script
    }

    /// The drops of the clear script in their default order: section by
    /// section (the order of [`ClearSection`]'s variants), and within a
    /// section in dump order, except where the dumped objects themselves show
    /// a dependency — views on views, SQL-standard bodies on what they use,
    /// aggregates on their support functions, partitions before their
    /// parents. Also returns whether the views section holds routines too,
    /// which changes its banner.
    ///
    /// Every database-wide object is listed; `Dump::select_clear_steps` keeps
    /// the ones that have to go.
    fn clear_steps(&self, use_cascade: bool) -> (Vec<ClearStep>, bool) {
        use crate::comparer::core::Comparer;
        use crate::dump::drop_order as keys;
        use crate::utils::string_extensions::{StringExt, block_comment_safe, unquote_ident};
        use std::collections::HashSet;

        let cascade_suffix = if use_cascade { " cascade" } else { "" };
        let step =
            |section: ClearSection, key: String, comment: String, statement: String| ClearStep {
                section,
                key,
                comment,
                statement: statement.with_empty_lines(),
                in_scope: !section.is_database_wide(),
            };
        let mut steps: Vec<ClearStep> = Vec::new();

        let routine_step = |section: ClearSection, routine: &Routine| {
            let drop_kind = match routine.kind.to_lowercase().as_str() {
                "window" => "function",
                "procedure" => "procedure",
                "aggregate" => "aggregate",
                _ => "function",
            };
            let args = if routine.kind.to_lowercase() == "aggregate" && routine.arguments.is_empty()
            {
                "*".to_string()
            } else {
                routine.arguments.clone()
            };
            step(
                section,
                keys::routine_key(&routine.dependency_key()),
                format!(
                    "/* Drop {}: {}.{} */\n",
                    block_comment_safe(&routine.kind),
                    block_comment_safe(&routine.schema),
                    block_comment_safe(&routine.name)
                ),
                format!(
                    "drop {} if exists {}.{} ({}){cascade_suffix};",
                    drop_kind, routine.schema, routine.name, args
                ),
            )
        };
        let unquoted_name = |routine: &Routine| {
            (
                routine.schema.to_lowercase().replace('"', ""),
                routine.name.to_lowercase().replace('"', ""),
            )
        };

        // A routine with a SQL-standard body (`BEGIN ATOMIC … END` / `RETURN
        // expr`) is a hard dependent of the relations it reads: PostgreSQL
        // refuses `drop table` / `drop view` / `drop sequence` under it. The
        // ones that read a relation this script drops are dropped with the
        // views, ahead of the relations, together with every SQL-standard
        // routine calling one of them and every aggregate built on one (each
        // would block that drop in turn). One that reads only relations the
        // script leaves alone (`information_schema.tables`) keeps its place
        // after the tables, whose defaults and constraints may call it.
        let dumped_relations: HashSet<String> = self
            .views
            .iter()
            .map(|v| format!("{}.{}", unquote_ident(&v.schema), unquote_ident(&v.name)))
            .chain(
                self.tables
                    .iter()
                    .map(|t| format!("{}.{}", t.raw_schema, t.raw_name)),
            )
            .chain(
                self.foreign_tables
                    .iter()
                    .map(|t| format!("{}.{}", unquote_ident(&t.schema), unquote_ident(&t.name))),
            )
            .chain(
                self.sequences
                    .iter()
                    .map(|s| format!("{}.{}", unquote_ident(&s.schema), unquote_ident(&s.name))),
            )
            .collect();
        let early_routines: Vec<usize> = {
            let mut early: Vec<usize> = self
                .routines
                .iter()
                .enumerate()
                .filter(|(_, r)| {
                    r.is_sql_body
                        && r.sql_body_dependencies
                            .relations
                            .iter()
                            .any(|rel| dumped_relations.contains(rel))
                })
                .map(|(i, _)| i)
                .collect();
            let mut in_early: HashSet<usize> = early.iter().copied().collect();
            loop {
                let keys: HashSet<String> = early
                    .iter()
                    .map(|&i| self.routines[i].dependency_key())
                    .collect();
                let names: Vec<(String, String)> = early
                    .iter()
                    .map(|&i| unquoted_name(&self.routines[i]))
                    .collect();
                let mut added = false;
                for (i, routine) in self.routines.iter().enumerate() {
                    if in_early.contains(&i) {
                        continue;
                    }
                    let calls_early = routine.is_sql_body
                        && routine
                            .sql_body_dependencies
                            .routines
                            .iter()
                            .any(|key| keys.contains(key));
                    let aggregates_early = names.iter().any(|(schema, name)| {
                        Comparer::aggregate_references_function(routine, schema, name)
                    });
                    if calls_early || aggregates_early {
                        in_early.insert(i);
                        early.push(i);
                        added = true;
                    }
                }
                if !added {
                    break;
                }
            }
            early
        };

        // 0. Event triggers, first: every other drop fires them, and one that
        //    logs DDL into a table of the schema being cleared would fail
        //    each drop from the moment that table is gone. Nothing depends on
        //    an event trigger, so dropping it first never blocks another drop.
        for trigger in &self.event_triggers {
            steps.push(step(
                ClearSection::EventTriggers,
                keys::event_trigger_key(&trigger.name),
                format!(
                    "/* Drop event trigger: {} */\n",
                    block_comment_safe(&trigger.name)
                ),
                format!(
                    "drop event trigger if exists {}{cascade_suffix};",
                    trigger.name
                ),
            ));
        }

        // 1. Views in dependency-safe order (topological sort on table_relation).
        //    Views that depend on other views are dropped first.  Tie-breaker:
        //    materialized views before regular views, then alphabetical by schema.name.
        //    The early routines above join the sort: after a view calling them,
        //    before a view they read, callers before callees.
        if !self.views.is_empty() || !early_routines.is_empty() {
            let view_count = self.views.len();
            let n = view_count + early_routines.len();

            // Qualified name for each view.
            let view_keys: Vec<String> = self
                .views
                .iter()
                .map(|v| format!("{}.{}", v.schema, v.name))
                .collect();

            // table_relation stores raw catalog names, while a regular view's schema
            // and name come from quote_ident, so the same view reads as `s."MyView"`
            // here and `s.MyView` there. Undo the quoting per part — schema and name
            // are separate fields, so no guessing where a dotted name splits — and the
            // two sides meet on the raw catalog name. Anything less exact would fuse
            // `s."A"` with `s.a`, which PostgreSQL keeps as different views.
            let lookup_keys: Vec<String> = self
                .views
                .iter()
                .map(|v| format!("{}.{}", unquote_ident(&v.schema), unquote_ident(&v.name)))
                .collect();

            // Map qualified name → index (only views, not tables).
            let key_to_idx: HashMap<&str, usize> = lookup_keys
                .iter()
                .enumerate()
                .map(|(i, k)| (k.as_str(), i))
                .collect();
            let routine_key_to_idx: HashMap<String, usize> = early_routines
                .iter()
                .enumerate()
                .map(|(k, &r)| (self.routines[r].dependency_key(), view_count + k))
                .collect();

            // Build the drop-order graph.
            // Edge i → j means view i depends on view j, so i must be dropped first.
            // in_degree[j] = number of views that must be dropped before j.
            let mut in_degree = vec![0usize; n];
            let mut edges: Vec<Vec<usize>> = vec![Vec::new(); n];

            for (i, view) in self.views.iter().enumerate() {
                for rel in &view.table_relation {
                    if let Some(&j) = key_to_idx.get(rel.trim())
                        && j != i
                    {
                        edges[i].push(j);
                        in_degree[j] += 1;
                    }
                }
                for (k, &r) in early_routines.iter().enumerate() {
                    let name = HashSet::from([unquoted_name(&self.routines[r])]);
                    if Comparer::definition_references_any(&view.definition, &name) {
                        edges[i].push(view_count + k);
                        in_degree[view_count + k] += 1;
                    }
                }
            }
            for (k, &r) in early_routines.iter().enumerate() {
                let i = view_count + k;
                let routine = &self.routines[r];
                let deps = &routine.sql_body_dependencies;
                let mut targets: Vec<usize> = Vec::new();
                for rel in &deps.relations {
                    if let Some(&j) = key_to_idx.get(rel.as_str()) {
                        targets.push(j);
                    }
                }
                for key in &deps.routines {
                    if let Some(&j) = routine_key_to_idx.get(key) {
                        targets.push(j);
                    }
                }
                for (k2, &r2) in early_routines.iter().enumerate() {
                    let (schema, name) = unquoted_name(&self.routines[r2]);
                    if k2 != k && Comparer::aggregate_references_function(routine, &schema, &name) {
                        targets.push(view_count + k2);
                    }
                }
                targets.sort_unstable();
                targets.dedup();
                for j in targets {
                    if j != i {
                        edges[i].push(j);
                        in_degree[j] += 1;
                    }
                }
            }

            // Kahn's algorithm with a deterministic tie-breaker.
            let sort_key = |idx: &usize| {
                if *idx < view_count {
                    let v = &self.views[*idx];
                    // materialized first (false < true, so negate), then alphabetical
                    (0u8, !v.is_materialized, view_keys[*idx].clone())
                } else {
                    let r = &self.routines[early_routines[*idx - view_count]];
                    (1u8, false, r.dependency_key())
                }
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

            for &idx in &drop_order {
                if idx >= view_count {
                    let routine = &self.routines[early_routines[idx - view_count]];
                    steps.push(routine_step(ClearSection::Views, routine));
                    continue;
                }
                let view = &self.views[idx];
                let kind = if view.is_materialized {
                    "materialized view"
                } else {
                    "view"
                };
                steps.push(step(
                    ClearSection::Views,
                    keys::relation_key(&view.schema, &view.name),
                    format!(
                        "/* Drop {kind}: {}.{} */\n",
                        block_comment_safe(&view.schema),
                        block_comment_safe(&view.name)
                    ),
                    format!(
                        "drop {} if exists {}.{}{cascade_suffix};",
                        view.view_keyword(),
                        view.schema,
                        view.name
                    ),
                ));
            }
        }

        // 2. Tables: foreign keys first, then the tables — partitions before
        //    their parents.
        for table in &self.tables {
            for constraint in &table.constraints {
                if constraint.constraint_type.to_lowercase() == "foreign key" {
                    steps.push(step(
                        ClearSection::Tables,
                        keys::foreign_key_key(
                            &constraint.schema,
                            &constraint.table_name,
                            &constraint.name,
                        ),
                        format!(
                            "/* Drop foreign key: {}.{}.{} */\n",
                            block_comment_safe(&constraint.schema),
                            block_comment_safe(&constraint.table_name),
                            block_comment_safe(&constraint.name)
                        ),
                        format!(
                            "alter table {}.{} drop constraint if exists {};",
                            constraint.schema, constraint.table_name, constraint.name
                        ),
                    ));
                }
            }
        }
        let partitions = self.tables.iter().filter(|t| t.partition_of.is_some());
        let others = self.tables.iter().filter(|t| t.partition_of.is_none());
        for table in partitions.chain(others) {
            steps.push(step(
                ClearSection::Tables,
                keys::relation_key(&table.schema, &table.name),
                format!(
                    "/* Drop table: {}.{} */\n",
                    block_comment_safe(&table.schema),
                    block_comment_safe(&table.name)
                ),
                format!(
                    "drop table if exists {}.{}{cascade_suffix};",
                    table.schema, table.name
                ),
            ));
        }

        // 3. Foreign tables.
        for ft in &self.foreign_tables {
            steps.push(step(
                ClearSection::ForeignTables,
                keys::relation_key(&ft.schema, &ft.name),
                format!(
                    "/* Drop foreign table: {}.{} */\n",
                    block_comment_safe(&ft.schema),
                    block_comment_safe(&ft.name)
                ),
                format!(
                    "drop foreign table if exists {}.{}{cascade_suffix};",
                    ft.schema, ft.name
                ),
            ));
        }

        // 4.–7. Subscriptions, user mappings, servers and foreign-data
        //    wrappers: a subscription and a user mapping sit on a server, a
        //    server on a wrapper, a wrapper on its handler functions.
        //    `DROP USER MAPPING` has no CASCADE form.
        for sub in &self.subscriptions {
            steps.push(step(
                ClearSection::Subscriptions,
                keys::subscription_key(&sub.name),
                format!(
                    "/* Drop subscription: {} */\n",
                    block_comment_safe(&sub.name)
                ),
                format!("drop subscription if exists {}{cascade_suffix};", sub.name),
            ));
        }
        for um in &self.user_mappings {
            steps.push(step(
                ClearSection::UserMappings,
                keys::user_mapping_key(&um.username, &um.server_name),
                format!(
                    "/* Drop user mapping: {} on {} */\n",
                    block_comment_safe(&um.username),
                    block_comment_safe(&um.server_name)
                ),
                format!(
                    "drop user mapping if exists for {} server {};",
                    um.username, um.server_name
                ),
            ));
        }
        for server in &self.foreign_servers {
            steps.push(step(
                ClearSection::Servers,
                keys::server_key(&server.name),
                format!("/* Drop server: {} */\n", block_comment_safe(&server.name)),
                format!("drop server if exists {}{cascade_suffix};", server.name),
            ));
        }
        for fdw in &self.foreign_data_wrappers {
            steps.push(step(
                ClearSection::ForeignDataWrappers,
                keys::fdw_key(&fdw.name),
                format!(
                    "/* Drop foreign-data wrapper: {} */\n",
                    block_comment_safe(&fdw.name)
                ),
                format!(
                    "drop foreign data wrapper if exists {}{cascade_suffix};",
                    fdw.name
                ),
            ));
        }

        // 8. Extended statistics.
        for stat in &self.statistics {
            steps.push(step(
                ClearSection::Statistics,
                keys::statistic_key(&stat.schema, &stat.name),
                format!(
                    "/* Drop statistics: {}.{} */\n",
                    block_comment_safe(&stat.schema),
                    block_comment_safe(&stat.name)
                ),
                format!(
                    "drop statistics if exists {}.{}{cascade_suffix};",
                    stat.schema, stat.name
                ),
            ));
        }

        // 10.–11. Casts and operators: each uses routines and types, so they
        //    go before them.
        for cast in &self.casts {
            steps.push(step(
                ClearSection::Casts,
                keys::cast_key(&cast.source_type, &cast.target_type),
                format!(
                    "/* Drop cast: {} as {} */\n",
                    block_comment_safe(&cast.source_type),
                    block_comment_safe(&cast.target_type)
                ),
                format!(
                    "drop cast if exists ({} as {}){cascade_suffix};",
                    cast.source_type, cast.target_type
                ),
            ));
        }
        for op in &self.operators {
            steps.push(step(
                ClearSection::Operators,
                keys::operator_key(&op.key()),
                format!(
                    "/* Drop operator: {}.{} */\n",
                    block_comment_safe(&op.schema),
                    block_comment_safe(&op.name)
                ),
                format!(
                    "drop operator if exists {}.{} ({}, {}){cascade_suffix};",
                    op.schema,
                    op.name,
                    op.left_type.as_deref().unwrap_or("NONE"),
                    op.right_type.as_deref().unwrap_or("NONE")
                ),
            ));
        }

        // 12. Routines (functions, procedures, aggregates), but the ones
        //    dropped with the views. In dump order, except that a routine with a
        //    SQL-standard body goes before the routines it calls, and an
        //    aggregate before its support functions (state, final, combine,
        //    serial, moving-aggregate): both are hard dependencies (a stable
        //    sort, so without them the order is the dump's).
        let late_routines: Vec<usize> = {
            let early: HashSet<usize> = early_routines.iter().copied().collect();
            (0..self.routines.len())
                .filter(|i| !early.contains(i))
                .collect()
        };
        if !late_routines.is_empty() {
            let key_to_pos: HashMap<String, usize> = late_routines
                .iter()
                .enumerate()
                .map(|(pos, &r)| (self.routines[r].dependency_key(), pos))
                .collect();
            let mut edges: Vec<(usize, usize)> = Vec::new();
            for (pos, &r) in late_routines.iter().enumerate() {
                let routine = &self.routines[r];
                if routine.is_sql_body {
                    for key in &routine.sql_body_dependencies.routines {
                        if let Some(&j) = key_to_pos.get(key) {
                            edges.push((pos, j));
                        }
                    }
                }
                if routine.aggregate_info.is_some() {
                    for (j, &r2) in late_routines.iter().enumerate() {
                        let (schema, name) = unquoted_name(&self.routines[r2]);
                        if Comparer::aggregate_references_function(routine, &schema, &name) {
                            edges.push((pos, j));
                        }
                    }
                }
            }
            for pos in
                super::drop_order::stable_topological_order(late_routines.len(), &edges).order
            {
                steps.push(routine_step(
                    ClearSection::Routines,
                    &self.routines[late_routines[pos]],
                ));
            }
        }

        // 13. Sequences.
        for sequence in &self.sequences {
            steps.push(step(
                ClearSection::Sequences,
                keys::relation_key(&sequence.schema, &sequence.name),
                format!(
                    "/* Drop sequence: {}.{} */\n",
                    block_comment_safe(&sequence.schema),
                    block_comment_safe(&sequence.name)
                ),
                format!(
                    "drop sequence if exists {}.{}{cascade_suffix};",
                    sequence.schema, sequence.name
                ),
            ));
        }

        // 14. Types (enums, composites, domains, ranges, multiranges).
        for pg_type in &self.types {
            steps.push(step(
                ClearSection::Types,
                keys::type_key(&pg_type.schema, &pg_type.typname),
                format!(
                    "/* Drop type: {}.{} */\n",
                    block_comment_safe(&pg_type.schema),
                    block_comment_safe(&pg_type.typname)
                ),
                format!(
                    "drop type if exists {}.{}{cascade_suffix};",
                    pg_type.schema, pg_type.typname
                ),
            ));
        }

        // 15.–17. Text search configurations (on their dictionaries), text
        //    search dictionaries and collations: columns, indexes and domains
        //    use all three.
        for cfg in &self.ts_configs {
            steps.push(step(
                ClearSection::TextSearchConfigs,
                keys::ts_config_key(&cfg.schema, &cfg.name),
                format!(
                    "/* Drop text search configuration: {}.{} */\n",
                    block_comment_safe(&cfg.schema),
                    block_comment_safe(&cfg.name)
                ),
                format!(
                    "drop text search configuration if exists {}.{}{cascade_suffix};",
                    cfg.schema, cfg.name
                ),
            ));
        }
        for dict in &self.ts_dicts {
            steps.push(step(
                ClearSection::TextSearchDicts,
                keys::ts_dict_key(&dict.schema, &dict.name),
                format!(
                    "/* Drop text search dictionary: {}.{} */\n",
                    block_comment_safe(&dict.schema),
                    block_comment_safe(&dict.name)
                ),
                format!(
                    "drop text search dictionary if exists {}.{}{cascade_suffix};",
                    dict.schema, dict.name
                ),
            ));
        }
        for collation in &self.collations {
            steps.push(step(
                ClearSection::Collations,
                keys::collation_key(&collation.schema, &collation.name),
                format!(
                    "/* Drop collation: {}.{} */\n",
                    block_comment_safe(&collation.schema),
                    block_comment_safe(&collation.name)
                ),
                format!(
                    "drop collation if exists {}.{}{cascade_suffix};",
                    collation.schema, collation.name
                ),
            ));
        }

        // 18. Extensions. The extensions query also reads every extension
        //    installed in `public`, selected or not; such an extension is
        //    dropped (as it always was), but it is not inside the selected
        //    schemas, so it does not make `Dump::select_clear_steps` drop the
        //    database-wide objects built on it.
        for ext in &self.extensions {
            let mut ext_step = step(
                ClearSection::Extensions,
                keys::extension_key(&ext.name),
                format!("/* Drop extension: {} */\n", block_comment_safe(&ext.name)),
                format!("drop extension if exists {}{cascade_suffix};", ext.name),
            );
            ext_step.in_scope = self.schemas.iter().any(|s| s.name == ext.schema);
            steps.push(ext_step);
        }

        // 19. Schemas (last, since everything else lives inside them).
        for schema in &self.schemas {
            steps.push(step(
                ClearSection::Schemas,
                keys::schema_key(&schema.name),
                format!("/* Drop schema: {} */\n", block_comment_safe(&schema.name)),
                format!("drop schema if exists {}{cascade_suffix};", schema.name),
            ));
        }

        (steps, !early_routines.is_empty())
    }

    /// The drops the clear script runs: every one of `steps` but the
    /// database-wide objects (`ClearSection::is_database_wide`), plus each of
    /// those that depends — directly, or through another one — on something
    /// the script drops inside the selected schemas, following
    /// [`Dump::drop_dependencies`]. PostgreSQL refuses the drop it blocks
    /// otherwise: a foreign-data wrapper using an extension's handler blocks
    /// `drop extension`, a server on that wrapper blocks the wrapper, and a
    /// subscription or user mapping on that server blocks the server.
    ///
    /// Two limits keep this from reaching past the selected schemas:
    ///
    /// - Only a drop of an object inside them pulls one in. An extension in
    ///   `public` that the extensions query reads although `public` is not
    ///   selected is still dropped, but pulls in nothing: its drop fails on
    ///   whatever uses it, as it always did.
    /// - A database-wide object that something outside the selected schemas
    ///   depends on ([`OUTSIDE_KEY`](super::drop_order::OUTSIDE_KEY), such as a
    ///   foreign table in another schema on a server), or that something the
    ///   script does not drop depends on, is never dropped, and neither is
    ///   any database-wide object it depends on in turn: its drop could only
    ///   fail, and the user mappings dropped ahead of it would be lost.
    ///   stderr names it, since the drop it blocks fails without
    ///   `--use-cascade`.
    ///
    /// Without dependency data none of them is dropped.
    fn select_clear_steps(&self, steps: Vec<ClearStep>) -> Vec<ClearStep> {
        let database_wide: Vec<bool> = steps.iter().map(|s| s.section.is_database_wide()).collect();
        if self.drop_dependencies.is_empty() {
            return steps
                .into_iter()
                .zip(database_wide)
                .filter_map(|(s, wide)| (!wide).then_some(s))
                .collect();
        }
        let mut by_key: HashMap<&str, Vec<usize>> = HashMap::new();
        for (i, s) in steps.iter().enumerate() {
            by_key.entry(s.key.as_str()).or_default().push(i);
        }
        let edges: Vec<(Option<&Vec<usize>>, &Vec<usize>)> = self
            .drop_dependencies
            .iter()
            .filter_map(|d| Some((by_key.get(d.before.as_str()), by_key.get(d.after.as_str())?)))
            .collect();

        // Database-wide objects that must stay: something the script does not
        // drop depends on them, or on a database-wide object depending on them.
        let mut blocked = vec![false; steps.len()];
        for (before, after) in &edges {
            if before.is_none() {
                for &a in *after {
                    blocked[a] |= database_wide[a];
                }
            }
        }
        loop {
            let mut added = false;
            for (before, after) in &edges {
                let Some(before) = before else { continue };
                if before.iter().any(|&b| blocked[b]) {
                    for &a in *after {
                        if database_wide[a] && !blocked[a] {
                            blocked[a] = true;
                            added = true;
                        }
                    }
                }
            }
            if !added {
                break;
            }
        }

        // What the drops inside the selected schemas need gone first.
        let mut pulled: Vec<bool> = steps.iter().map(|s| s.in_scope).collect();
        let mut left_in_place: Vec<usize> = Vec::new();
        loop {
            let mut added = false;
            for (before, after) in &edges {
                let Some(before) = before else { continue };
                if after.iter().any(|&a| pulled[a]) {
                    for &b in *before {
                        if !database_wide[b] || pulled[b] {
                            continue;
                        }
                        if blocked[b] {
                            if !left_in_place.contains(&b) {
                                left_in_place.push(b);
                            }
                        } else {
                            pulled[b] = true;
                            added = true;
                        }
                    }
                }
            }
            if !added {
                break;
            }
        }
        if !left_in_place.is_empty() {
            left_in_place.sort_unstable();
            let names: Vec<&str> = left_in_place
                .iter()
                .map(|&i| steps[i].key.as_str())
                .collect();
            eprintln!(
                "Warning: the clear script leaves {} in place: an object it does not drop \
                 (outside the selected schema(s)) depends on it, so without --use-cascade \
                 the drop of what it is built on fails.",
                names.join(", ")
            );
        }
        steps
            .into_iter()
            .zip(database_wide.into_iter().zip(pulled))
            .filter_map(|(s, (wide, pulled))| (!wide || pulled).then_some(s))
            .collect()
    }

    /// `steps` in the order the script runs them: their default order,
    /// changed only where a dependency forbids it
    /// (`drop_order::stable_topological_order`). The dependencies are
    /// [`Dump::drop_dependencies`] when the dump has them, and otherwise what
    /// `Dump::inferred_clear_dependencies` reads off the dumped objects.
    ///
    /// A dependency cycle cannot be ordered. When one of its edges is held by
    /// a part of a table that can be dropped on its own
    /// ([`DropDependency::breaker`]), that part is dropped first, just ahead
    /// of the earlier of the two objects, and the cycle is gone. A cycle with
    /// no such edge keeps its default order, and stderr names it.
    fn order_clear_steps(&self, steps: Vec<ClearStep>) -> Vec<ClearStep> {
        use super::drop_order::{edges_on_cycles, stable_topological_order};
        use crate::utils::string_extensions::{StringExt, block_comment_safe};

        let mut index: HashMap<&str, usize> = HashMap::new();
        for (i, s) in steps.iter().enumerate() {
            index.entry(s.key.as_str()).or_insert(i);
        }
        let edges: Vec<(usize, usize, Option<&str>)> = if self.drop_dependencies.is_empty() {
            self.inferred_clear_dependencies(&steps, &index)
                .into_iter()
                .map(|(before, after)| (before, after, None))
                .collect()
        } else {
            self.drop_dependencies
                .iter()
                .filter_map(|d| {
                    Some((
                        *index.get(d.before.as_str())?,
                        *index.get(d.after.as_str())?,
                        d.breaker.as_deref(),
                    ))
                })
                .collect()
        };
        drop(index);
        let plain: Vec<(usize, usize)> = edges.iter().map(|&(b, a, _)| (b, a)).collect();
        let sorted = stable_topological_order(steps.len(), &plain);
        if sorted.forced.is_empty() {
            return Self::take_in_order(steps, &sorted.order);
        }

        // Break every cycle an edge with a breaker closes: the breaker runs
        // just ahead of the earlier of the edge's two objects, and before
        // both of them; the edge itself goes.
        let on_cycle = edges_on_cycles(steps.len(), &plain);
        let mut breakers: Vec<(usize, String, Vec<usize>)> = Vec::new();
        let mut kept: Vec<(usize, usize)> = Vec::new();
        for (&(before, after, breaker), cyclic) in edges.iter().zip(on_cycle) {
            match breaker {
                Some(statement) if cyclic => {
                    match breakers.iter_mut().find(|(_, s, _)| s == statement) {
                        Some((anchor, _, targets)) => {
                            *anchor = (*anchor).min(before.min(after));
                            targets.extend([before, after]);
                        }
                        None => breakers.push((
                            before.min(after),
                            statement.to_string(),
                            vec![before, after],
                        )),
                    }
                }
                _ => kept.push((before, after)),
            }
        }
        // Renumber: each breaker right before its anchor.
        let mut position: Vec<usize> = Vec::with_capacity(steps.len());
        let mut breaker_position: Vec<usize> = vec![0; breakers.len()];
        let mut next = 0usize;
        for i in 0..steps.len() {
            for (b, (anchor, _, _)) in breakers.iter().enumerate() {
                if *anchor == i {
                    breaker_position[b] = next;
                    next += 1;
                }
            }
            position.push(next);
            next += 1;
        }
        let mut all: Vec<Option<ClearStep>> = (0..next).map(|_| None).collect();
        for (b, (anchor, statement, _)) in breakers.iter().enumerate() {
            // Under the banner of the drop it runs ahead of.
            let section = steps[*anchor].section;
            all[breaker_position[b]] = Some(ClearStep {
                section,
                key: String::new(),
                comment: format!(
                    "/* Break dependency cycle: {} */\n",
                    block_comment_safe(statement)
                ),
                statement: format!("{statement};").with_empty_lines(),
                in_scope: steps[*anchor].in_scope,
            });
        }
        let mut new_edges: Vec<(usize, usize)> = kept
            .iter()
            .map(|&(before, after)| (position[before], position[after]))
            .collect();
        for (b, (_, _, targets)) in breakers.iter().enumerate() {
            for &target in targets {
                new_edges.push((breaker_position[b], position[target]));
            }
        }
        for (i, step) in steps.into_iter().enumerate() {
            all[position[i]] = Some(step);
        }
        let all: Vec<ClearStep> = all.into_iter().flatten().collect();

        let sorted = stable_topological_order(all.len(), &new_edges);
        if !sorted.forced.is_empty() {
            let names: Vec<&str> = sorted.forced.iter().map(|&i| all[i].key.as_str()).collect();
            eprintln!(
                "Warning: the clear script cannot order {}: each is part of a dependency \
                 cycle that no drop order satisfies. Without --use-cascade the script \
                 fails there.",
                names.join(", ")
            );
        }
        Self::take_in_order(all, &sorted.order)
    }

    /// `steps` rearranged into `order` (a permutation of their positions).
    fn take_in_order(steps: Vec<ClearStep>, order: &[usize]) -> Vec<ClearStep> {
        let mut slots: Vec<Option<ClearStep>> = steps.into_iter().map(Some).collect();
        order.iter().filter_map(|&i| slots[i].take()).collect()
    }

    /// Drop-order dependencies read off the dumped objects, for a dump without
    /// [`Dump::drop_dependencies`] (one not built by [`inspect`](Self::inspect)).
    /// As `(before, after)` positions in `steps`, found through `index` (key →
    /// position):
    ///
    /// - an `INHERITS` child before its parent (a partition already comes
    ///   before its parent in the default order);
    /// - a domain before its base type, a composite before its attribute
    ///   types and a range before its subtype, when those are dumped types;
    /// - a domain before the routines its constraints and default call;
    /// - a table before the routines dropped with the views (SQL-standard
    ///   bodies reading a relation) that its constraints and column defaults
    ///   call.
    fn inferred_clear_dependencies(
        &self,
        steps: &[ClearStep],
        index: &HashMap<&str, usize>,
    ) -> Vec<(usize, usize)> {
        use crate::comparer::core::Comparer;
        use crate::dump::drop_order as keys;
        use std::collections::HashSet;

        let mut edges: Vec<(usize, usize)> = Vec::new();
        let mut edge = |before: &str, after: &str| {
            if let (Some(&b), Some(&a)) = (index.get(before), index.get(after)) {
                edges.push((b, a));
            }
        };

        for table in &self.tables {
            let child = keys::relation_key(&table.schema, &table.name);
            for parent in &table.inherits_from {
                edge(&child, &format!("rel:{parent}"));
            }
        }

        // A type as `format_type` names it: qualified unless on the dump
        // session's `search_path`, which in practice means `public`.
        let type_named = |name: &str| -> Option<String> {
            let name = name.trim().trim_end_matches("[]");
            self.types
                .iter()
                .find(|t| {
                    name == format!("{}.{}", t.schema, t.typname)
                        || (t.schema == "public" && name == t.typname)
                })
                .map(|t| keys::type_key(&t.schema, &t.typname))
        };
        let routine_names: Vec<(String, HashSet<(String, String)>)> = self
            .routines
            .iter()
            .map(|r| {
                (
                    keys::routine_key(&r.dependency_key()),
                    HashSet::from([(
                        r.schema.to_lowercase().replace('"', ""),
                        r.name.to_lowercase().replace('"', ""),
                    )]),
                )
            })
            .collect();
        for pg_type in &self.types {
            let this = keys::type_key(&pg_type.schema, &pg_type.typname);
            let used_types = pg_type
                .formatted_basetype
                .iter()
                .chain(pg_type.range_subtype.iter())
                .chain(pg_type.composite_attributes.iter().map(|a| &a.data_type));
            for used in used_types {
                if let Some(used) = type_named(used) {
                    edge(&this, &used);
                }
            }
            let texts: Vec<&str> = pg_type
                .domain_constraints
                .iter()
                .map(|c| c.definition.as_str())
                .chain(pg_type.typdefault.as_deref())
                .collect();
            for (routine, name) in &routine_names {
                if texts
                    .iter()
                    .any(|text| Comparer::definition_references_any(text, name))
                {
                    edge(&this, routine);
                }
            }
        }

        let early_routines: Vec<&(String, HashSet<(String, String)>)> = routine_names
            .iter()
            .filter(|(key, _)| {
                index
                    .get(key.as_str())
                    .is_some_and(|&i| steps[i].section == ClearSection::Views)
            })
            .collect();
        if !early_routines.is_empty() {
            for table in &self.tables {
                let this = keys::relation_key(&table.schema, &table.name);
                let texts: Vec<&str> = table
                    .constraints
                    .iter()
                    .filter(|c| !c.constraint_type.eq_ignore_ascii_case("foreign key"))
                    .filter_map(|c| c.definition.as_deref())
                    .chain(
                        table
                            .columns
                            .iter()
                            .filter_map(|c| c.column_default.as_deref()),
                    )
                    .collect();
                for (routine, name) in &early_routines {
                    if texts
                        .iter()
                        .any(|text| Comparer::definition_references_any(text, name))
                    {
                        edge(&this, routine);
                    }
                }
            }
        }

        edges
    }
}

/// A section of the clear script ([`Dump::generate_clear_script`]): the
/// banner it prints, and — by the order of the variants — where its drops go
/// when no dependency says otherwise.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClearSection {
    /// Event triggers, ahead of every drop that would fire them.
    EventTriggers,
    /// Views and materialized views, with the routines whose SQL-standard
    /// body reads a relation.
    Views,
    /// Foreign keys, then tables.
    Tables,
    ForeignTables,
    Subscriptions,
    UserMappings,
    Servers,
    ForeignDataWrappers,
    Statistics,
    Casts,
    Operators,
    Routines,
    Sequences,
    Types,
    TextSearchConfigs,
    TextSearchDicts,
    Collations,
    Extensions,
    Schemas,
}

impl ClearSection {
    /// The comment printed where the section starts (and again wherever a
    /// dependency moved a drop of this section after one of another).
    /// `views_with_routines`: the views section also drops routines.
    fn banner(self, views_with_routines: bool) -> &'static str {
        match self {
            ClearSection::Views if views_with_routines => {
                "\n/* ---> Drop Views and routines reading relations --------------- */"
            }
            ClearSection::Views => "\n/* ---> Drop Views --------------- */",
            ClearSection::Tables => "\n/* ---> Drop Tables --------------- */",
            ClearSection::ForeignTables => "\n/* ---> Drop Foreign Tables --------------- */",
            ClearSection::Subscriptions => "\n/* ---> Drop Subscriptions --------------- */",
            ClearSection::UserMappings => "\n/* ---> Drop User Mappings --------------- */",
            ClearSection::Servers => "\n/* ---> Drop Foreign Servers --------------- */",
            ClearSection::ForeignDataWrappers => {
                "\n/* ---> Drop Foreign-Data Wrappers --------------- */"
            }
            ClearSection::Statistics => "\n/* ---> Drop Statistics --------------- */",
            ClearSection::EventTriggers => "\n/* ---> Drop Event Triggers --------------- */",
            ClearSection::Casts => "\n/* ---> Drop Casts --------------- */",
            ClearSection::Operators => "\n/* ---> Drop Operators --------------- */",
            ClearSection::Routines => "\n/* ---> Drop Routines --------------- */",
            ClearSection::Sequences => "\n/* ---> Drop Sequences --------------- */",
            ClearSection::Types => "\n/* ---> Drop Types --------------- */",
            ClearSection::TextSearchConfigs => {
                "\n/* ---> Drop Text Search Configurations --------------- */"
            }
            ClearSection::TextSearchDicts => {
                "\n/* ---> Drop Text Search Dictionaries --------------- */"
            }
            ClearSection::Collations => "\n/* ---> Drop Collations --------------- */",
            ClearSection::Extensions => "\n/* ---> Drop Extensions --------------- */",
            ClearSection::Schemas => "\n/* ---> Drop Schemas --------------- */",
        }
    }

    /// Whether the section holds database-wide objects, which belong to no
    /// schema and are dropped only when something the script drops needs
    /// them gone first (`Dump::select_clear_steps`).
    fn is_database_wide(self) -> bool {
        matches!(
            self,
            ClearSection::Subscriptions
                | ClearSection::UserMappings
                | ClearSection::Servers
                | ClearSection::ForeignDataWrappers
                | ClearSection::EventTriggers
        )
    }
}

/// One drop of the clear script.
#[derive(Debug, Clone)]
struct ClearStep {
    section: ClearSection,
    /// The dropped object's key, as the [`drop_order`](super::drop_order)
    /// module builds it.
    key: String,
    /// The `/* Drop …: … */` line, written only with comments on.
    comment: String,
    /// The statement, with its trailing empty lines.
    statement: String,
    /// Whether the object lies inside the selected schemas, so that its drop
    /// can make `Dump::select_clear_steps` drop the database-wide objects
    /// depending on it. False for those database-wide objects themselves and
    /// for an extension in an unselected `public`.
    in_scope: bool,
}

#[cfg(test)]
#[path = "tests/core.rs"]
mod tests;
