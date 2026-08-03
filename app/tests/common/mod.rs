//! Fixture builders and scratch-directory helpers shared by the integration
//! tests.
//!
//! This is a `common/` subdirectory rather than a top-level `tests/*.rs` file
//! on purpose: Cargo compiles every top-level file in `tests/` as its own test
//! binary, but leaves subdirectories alone, so this module is included by the
//! test binaries that declare `mod common;` instead of becoming one itself.
//!
//! Everything here goes through `pgc`'s **public** API only. Tests that need
//! access to private internals belong with their module, under
//! `src/<module>/tests/`.

#![allow(dead_code)] // each test binary uses only part of this module

use pgc::config::dump_config::DumpConfig;
use pgc::dump::core::Dump;
use pgc::dump::extension::Extension;
use pgc::dump::routine::Routine;
use pgc::dump::schema::Schema;
use pgc::dump::sequence::Sequence;
use pgc::dump::table::Table;
use pgc::dump::table_column::TableColumn;
use pgc::dump::table_constraint::TableConstraint;
use pgc::dump::table_index::TableIndex;
use pgc::dump::view::View;
use sqlx::postgres::types::Oid;
use std::path::{Path, PathBuf};

/// A uniquely-named directory under the system temp dir, removed on drop.
///
/// Avoids a `tempfile` dev-dependency — the crate keeps a deliberately lean
/// dependency list, and the tests only need a scratch path.
pub struct ScratchDir {
    path: PathBuf,
}

impl ScratchDir {
    /// `label` only has to be unique per test binary; the pid and a counter
    /// disambiguate concurrent runs.
    pub fn new(label: &str) -> Self {
        use std::sync::atomic::{AtomicU32, Ordering};
        static COUNTER: AtomicU32 = AtomicU32::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("pgc-it-{label}-{}-{n}", std::process::id()));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).expect("create scratch dir");
        Self { path }
    }

    pub fn join(&self, name: &str) -> PathBuf {
        self.path.join(name)
    }

    /// Same as [`Self::join`], as the `&str` the `pgc` API takes.
    pub fn path_str(&self, name: &str) -> String {
        self.join(name).to_str().expect("utf-8 path").to_string()
    }
}

impl Drop for ScratchDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

/// Strip SQL comments so a generated script can be checked for "contains no
/// statements at all".
///
/// Handles `--` to end-of-line and nested `/* … */` blocks, which the headers
/// and section banners `pgc` emits are built from. It does *not* skip string
/// literals — it does not need to: a script containing a literal necessarily
/// contains a statement, so the caller's emptiness assertion fails either way.
pub fn strip_sql_comments(script: &str) -> String {
    let bytes: Vec<char> = script.chars().collect();
    let mut out = String::with_capacity(script.len());
    let mut depth = 0usize;
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == '/' && bytes.get(i + 1) == Some(&'*') {
            depth += 1;
            i += 2;
        } else if depth > 0 && bytes[i] == '*' && bytes.get(i + 1) == Some(&'/') {
            depth -= 1;
            i += 2;
        } else if depth > 0 {
            i += 1;
        } else if bytes[i] == '-' && bytes.get(i + 1) == Some(&'-') {
            while i < bytes.len() && bytes[i] != '\n' {
                i += 1;
            }
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    out
}

/// Panic with the full script unless it consists purely of comments and
/// whitespace — the shape a no-op migration must have.
pub fn assert_no_ddl(script: &str, context: &str) {
    let stripped = strip_sql_comments(script);
    let remaining = stripped.trim();
    assert!(
        remaining.is_empty(),
        "{context} emitted DDL:\n--- statements ---\n{remaining}\n--- full script ---\n{script}"
    );
}

/// Absolute path to a file under the repository's `data/` directory.
///
/// **Only pass files that are committed to git.** `data/` also holds
/// developer-local files that `.gitignore` excludes — `test.conf` among them —
/// and a test reading one of those passes on the machine that has it and fails
/// everywhere else, CI included. Panics with that reminder rather than letting
/// the caller hit a bare "No such file or directory".
pub fn data_path(relative: &str) -> PathBuf {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("app/ has a parent")
        .join("data")
        .join(relative);
    assert!(
        path.exists(),
        "data/{relative} is missing. Tests may only depend on git-tracked files \
         under data/; check whether this one is excluded by .gitignore."
    );
    path
}

/// A `DumpConfig` that is never connected to — dumps built in-process still
/// carry one, and `Dump::new` requires it.
pub fn offline_config(database: &str, scheme: &str) -> DumpConfig {
    DumpConfig {
        host: "localhost".to_string(),
        port: "5432".to_string(),
        user: "postgres".to_string(),
        password: String::new(),
        database: database.to_string(),
        scheme: scheme.to_string(),
        ssl: false,
        file: String::new(),
    }
}

pub fn empty_dump(database: &str) -> Dump {
    Dump::new(offline_config(database, "public"))
}

pub fn schema(name: &str) -> Schema {
    Schema::new(name.to_string(), name.to_string(), None)
}

pub fn extension(name: &str, version: &str, schema: &str) -> Extension {
    Extension::new(name.to_string(), version.to_string(), schema.to_string())
}

/// An `integer` column with everything else left at its catalog default.
pub fn int_column(schema: &str, table: &str, name: &str, ordinal: i32) -> TableColumn {
    TableColumn {
        catalog: "postgres".to_string(),
        schema: schema.to_string(),
        table: table.to_string(),
        name: name.to_string(),
        ordinal_position: ordinal,
        column_default: None,
        is_nullable: true,
        data_type: "integer".to_string(),
        character_maximum_length: None,
        character_octet_length: None,
        numeric_precision: Some(32),
        numeric_precision_radix: Some(2),
        numeric_scale: Some(0),
        datetime_precision: None,
        interval_type: None,
        interval_precision: None,
        character_set_catalog: None,
        character_set_schema: None,
        character_set_name: None,
        collation_catalog: None,
        collation_schema: None,
        collation_name: None,
        domain_catalog: None,
        domain_schema: None,
        domain_name: None,
        udt_catalog: None,
        udt_schema: None,
        udt_name: None,
        scope_catalog: None,
        scope_schema: None,
        scope_name: None,
        maximum_cardinality: None,
        dtd_identifier: None,
        is_self_referencing: false,
        is_identity: false,
        identity_generation: None,
        identity_start: None,
        identity_increment: None,
        identity_maximum: None,
        identity_minimum: None,
        identity_cycle: false,
        is_generated: "NEVER".to_string(),
        generation_expression: None,
        generation_type: None,
        is_updatable: true,
        related_views: None,
        comment: None,
        storage: None,
        compression: None,
        statistics_target: None,
        acl: vec![],
        serial_type: None,
    }
}

/// A table with the given integer columns, hashed and ready to compare.
pub fn table(schema_name: &str, name: &str, columns: &[&str]) -> Table {
    let cols = columns
        .iter()
        .enumerate()
        .map(|(i, c)| int_column(schema_name, name, c, i as i32 + 1))
        .collect();
    let mut t = Table::new(
        schema_name.to_string(),
        name.to_string(),
        schema_name.to_string(),
        name.to_string(),
        "postgres".to_string(),
        None,
        cols,
        vec![],
        vec![],
        vec![],
        None,
    );
    t.hash();
    t
}

/// A `FOREIGN KEY` constraint on `table` referencing `references`.
pub fn foreign_key(
    schema: &str,
    table: &str,
    name: &str,
    column: &str,
    references: &str,
    referenced_column: &str,
) -> TableConstraint {
    TableConstraint {
        catalog: "postgres".to_string(),
        schema: schema.to_string(),
        name: name.to_string(),
        table_name: table.to_string(),
        constraint_type: "FOREIGN KEY".to_string(),
        is_deferrable: false,
        initially_deferred: false,
        definition: Some(format!(
            "FOREIGN KEY ({column}) REFERENCES {references}({referenced_column})"
        )),
        coninhcount: 0,
        is_enforced: true,
        no_inherit: false,
        nulls_not_distinct: false,
        comment: None,
    }
}

/// A plain btree index on `column`.
pub fn index(schema: &str, table: &str, name: &str, column: &str) -> TableIndex {
    TableIndex {
        schema: schema.to_string(),
        table: table.to_string(),
        name: name.to_string(),
        catalog: Some("postgres".to_string()),
        indexdef: format!("CREATE INDEX {name} ON {schema}.{table} USING btree ({column})"),
        is_partition_index: false,
        comment: None,
    }
}

pub fn view(schema: &str, name: &str, definition: &str, relations: &[&str]) -> View {
    let mut v = View::new(
        name.to_string(),
        definition.to_string(),
        schema.to_string(),
        relations.iter().map(|r| r.to_string()).collect(),
    );
    v.hash();
    v
}

pub fn materialized_view(schema: &str, name: &str, definition: &str, relations: &[&str]) -> View {
    let mut v = view(schema, name, definition, relations);
    v.is_materialized = true;
    v.hash();
    v
}

pub fn routine(schema: &str, oid: u32, name: &str, return_type: &str, body: &str) -> Routine {
    let mut r = Routine::new(
        schema.to_string(),
        Oid(oid),
        name.to_string(),
        "sql".to_string(),
        "FUNCTION".to_string(),
        return_type.to_string(),
        "x integer".to_string(),
        None,
        None,
        body.to_string(),
    );
    r.hash();
    r
}

#[allow(clippy::too_many_arguments)]
pub fn sequence(schema: &str, name: &str) -> Sequence {
    let mut s = Sequence::new(
        schema.to_string(),
        name.to_string(),
        "postgres".to_string(),
        "bigint".to_string(),
        Some(1),
        Some(1),
        Some(i64::MAX),
        Some(1),
        false,
        Some(1),
        Some(1),
        None,
        None,
        None,
    );
    s.hash();
    s
}

/// A dump with one schema, two related tables, a view over them, a routine and
/// a sequence — enough object kinds that a self-comparison exercises most of
/// `Comparer`'s branches.
pub fn populated_dump(database: &str) -> Dump {
    let mut dump = empty_dump(database);
    dump.schemas.push(schema("app"));
    dump.extensions.push(extension("pgcrypto", "1.3", "public"));

    let parent = table("app", "customers", &["id", "region"]);
    let mut child = table("app", "orders", &["id", "customer_id"]);
    child.constraints.push(foreign_key(
        "app",
        "orders",
        "fk_orders_customer",
        "customer_id",
        "app.customers",
        "id",
    ));
    child.hash();
    dump.tables.push(parent);
    dump.tables.push(child);

    dump.views.push(view(
        "app",
        "order_summary",
        " SELECT o.id, c.region\n   FROM app.orders o JOIN app.customers c ON c.id = o.customer_id;",
        &["app.orders", "app.customers"],
    ));
    dump.views.push(materialized_view(
        "app",
        "region_totals",
        " SELECT region, count(*) AS n\n   FROM app.customers GROUP BY region;",
        &["app.customers"],
    ));
    dump.routines
        .push(routine("app", 900, "compute", "integer", "SELECT $1 * 2"));
    dump.sequences.push(sequence("app", "order_seq"));
    dump
}
