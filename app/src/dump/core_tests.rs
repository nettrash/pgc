use super::*;
use crate::dump::extension::Extension;
use crate::dump::routine::Routine;
use crate::dump::schema::Schema;
use crate::dump::sequence::Sequence;
use crate::dump::table::Table;
use crate::dump::table_constraint::TableConstraint;
use crate::dump::view::View;
use sqlx::postgres::types::Oid;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

/// RAII guard for a temp file used by a test. The path is deleted
/// on drop, including when an assertion panics earlier in the
/// test, so failures don't leave files behind in the temp dir.
/// Using a per-test-process atomic counter (plus PID) for the name
/// also avoids collisions when several tests in the same binary
/// reach for a temp file concurrently.
struct TempPath(PathBuf);

impl TempPath {
    fn new(prefix: &str, suffix: &str) -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let name = format!("{}_{}_{}.{}", prefix, std::process::id(), n, suffix);
        Self(std::env::temp_dir().join(name))
    }

    fn as_str(&self) -> std::borrow::Cow<'_, str> {
        self.0.to_string_lossy()
    }
}

impl Drop for TempPath {
    fn drop(&mut self) {
        // Ignore NotFound (e.g. test never wrote the file) and
        // any other error — best-effort cleanup must not mask
        // the original test failure.
        let _ = std::fs::remove_file(&self.0);
    }
}

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

#[test]
fn build_sequences_standalone_query_filters_by_pg_class() {
    let query = Dump::build_sequences_standalone_query("('public')");
    assert!(
        query.contains("seq_desc.classoid = 'pg_class'::regclass"),
        "expected pg_class classoid filter for sequence comments"
    );
}

/// Regression test for the streaming dump-write change: a Dump
/// serialized via [`Dump::write_to_file`] (which streams JSON
/// directly into the zip writer instead of materializing the whole
/// payload as a `String`) must still round-trip identically through
/// [`Dump::read_from_file`].
#[tokio::test]
async fn write_to_file_round_trips_via_read_from_file() {
    let mut dump = empty_dump();
    dump.schemas.push(make_schema("public"));
    dump.schemas.push(make_schema("data"));
    dump.extensions.push(make_extension("pgcrypto"));
    dump.tables.push(make_table("public", "users"));
    dump.tables.push(make_table("data", "events"));
    dump.views.push(make_view("public", "active_users"));
    dump.sequences.push(make_sequence("public", "users_id_seq"));
    dump.routines.push(make_routine("public", "noop"));

    // RAII guard cleans up the temp file even if a later assertion
    // panics, so failures don't pollute the temp dir.
    let path = TempPath::new("pgc_dump_roundtrip", "zip");

    dump.write_to_file(&path.as_str())
        .expect("write_to_file failed");

    let restored = Dump::read_from_file(&path.as_str())
        .await
        .expect("read_from_file failed");

    assert_eq!(restored.schemas.len(), dump.schemas.len());
    assert_eq!(restored.extensions.len(), dump.extensions.len());
    assert_eq!(restored.tables.len(), dump.tables.len());
    assert_eq!(restored.views.len(), dump.views.len());
    assert_eq!(restored.sequences.len(), dump.sequences.len());
    assert_eq!(restored.routines.len(), dump.routines.len());

    let restored_schemas: Vec<&str> = restored.schemas.iter().map(|s| s.name.as_str()).collect();
    assert!(restored_schemas.contains(&"public"));
    assert!(restored_schemas.contains(&"data"));

    let restored_table_keys: Vec<(&str, &str)> = restored
        .tables
        .iter()
        .map(|t| (t.schema.as_str(), t.name.as_str()))
        .collect();
    assert!(restored_table_keys.contains(&("public", "users")));
    assert!(restored_table_keys.contains(&("data", "events")));

    assert_eq!(restored.routines[0].name, "noop");
    assert_eq!(restored.sequences[0].name, "users_id_seq");
}

#[test]
fn require_view_definition_returns_definition_when_present() {
    let got = Dump::require_view_definition(Some("select 1".to_string()), "public", "v_one", false)
        .expect("Some should unwrap to Ok");
    assert_eq!(got, "select 1");
}

#[test]
fn require_view_definition_errors_with_view_kind_when_null() {
    let err = Dump::require_view_definition(None, "public", "v_one", false)
        .expect_err("None should produce an error");
    let msg = err.to_string();
    assert!(
        msg.contains("public.v_one"),
        "message must name the object: {msg}"
    );
    assert!(
        msg.contains(" view "),
        "message must say \"view\" (not \"materialized view\"): {msg}"
    );
    assert!(
        !msg.contains("materialized view"),
        "regular-view error must not mention materialized: {msg}"
    );
    assert!(
        msg.contains("SELECT privileges"),
        "message must hint at the privilege cause: {msg}"
    );
}

#[test]
fn require_view_definition_errors_with_materialized_kind_when_null() {
    let err = Dump::require_view_definition(None, "reporting", "mv_summary", true)
        .expect_err("None should produce an error");
    let msg = err.to_string();
    assert!(
        msg.contains("reporting.mv_summary"),
        "message must name the object: {msg}"
    );
    assert!(
        msg.contains("materialized view"),
        "materialized error must use the materialized wording: {msg}"
    );
    assert!(
        msg.contains("SELECT privileges"),
        "message must hint at the privilege cause: {msg}"
    );
}

fn assert_extension_filter_with_alias(
    query: &str,
    alias: &str,
    expected_classid: &str,
    deptype_fragment: &str,
    context: &str,
) {
    assert!(
        query.contains(&format!("{alias}.classid = '{expected_classid}'::regclass")),
        "{context}: missing classid guard '{alias}.classid = \\'{expected_classid}\\'::regclass'"
    );
    assert!(
        query.contains(&format!("{alias}.objsubid = 0")),
        "{context}: missing whole-object guard '{alias}.objsubid = 0'"
    );
    assert!(
        query.contains(&format!("{alias}.{deptype_fragment}")),
        "{context}: missing deptype guard '{alias}.{deptype_fragment}'"
    );
}

fn assert_extension_filter(query: &str, expected_classid: &str, context: &str) {
    assert_extension_filter_with_alias(
        query,
        "ext_dep",
        expected_classid,
        "deptype = 'e'",
        context,
    );
}

#[test]
fn build_tables_standalone_query_extension_filter_is_precise() {
    let query = Dump::build_tables_standalone_query("('public')");
    assert_extension_filter(&query, "pg_class", "tables");
}

#[test]
fn build_sequences_standalone_query_extension_filter_is_precise() {
    let query = Dump::build_sequences_standalone_query("('public')");
    assert_extension_filter(&query, "pg_class", "sequences");
}

#[test]
fn build_regular_views_query_extension_filter_is_precise() {
    let query = Dump::build_regular_views_query("('public')");
    assert_extension_filter(&query, "pg_class", "regular views");
}

#[test]
fn build_materialized_views_query_extension_filter_is_precise() {
    let query = Dump::build_materialized_views_query("('public')");
    assert_extension_filter(&query, "pg_class", "materialized views");
}

#[test]
fn build_routines_standalone_query_extension_filter_is_precise() {
    let query = Dump::build_routines_standalone_query("('public')");
    assert_extension_filter(&query, "pg_proc", "routines");
}

#[test]
fn build_composite_type_attributes_query_extension_filter_is_precise() {
    let query = Dump::build_composite_type_attributes_query("('public')");
    assert_extension_filter(&query, "pg_type", "composite type attributes");
}

#[test]
fn build_range_types_query_extension_filter_is_precise() {
    let query = Dump::build_range_types_query("('public')", "null::text as multirange_name", "");
    assert_extension_filter(&query, "pg_type", "range types");
}

#[test]
fn build_types_query_extension_filter_is_precise() {
    let query = Dump::build_types_query("('public')");
    assert_extension_filter(&query, "pg_type", "types");
}

#[test]
fn build_foreign_tables_query_extension_filter_is_precise() {
    let query = Dump::build_foreign_tables_query("('public')");
    assert_extension_filter(&query, "pg_class", "foreign tables");
}

#[test]
fn build_foreign_table_columns_query_extension_filter_is_precise() {
    let query = Dump::build_foreign_table_columns_query("('public')");
    assert_extension_filter(&query, "pg_class", "foreign table columns");
}

#[test]
fn build_statistics_query_extension_filter_is_precise() {
    let query = Dump::build_statistics_query("('public')");
    assert_extension_filter(&query, "pg_statistic_ext", "extended statistics");
}

#[test]
fn build_rules_query_extension_filter_is_precise() {
    let query = Dump::build_rules_query("('public')");
    assert_extension_filter(&query, "pg_class", "rules");
}

#[test]
fn build_domain_constraints_query_extension_filter_is_precise() {
    let query = Dump::build_domain_constraints_query("('public')");
    assert_extension_filter(&query, "pg_type", "domain constraints");
}

#[test]
fn build_enums_query_extension_filter_is_precise() {
    let query = Dump::build_enums_query();
    assert_extension_filter(query, "pg_type", "enums");
}

#[test]
fn build_collations_query_extension_filter_is_precise() {
    let query = Dump::build_collations_query(
        "c.colllocale",
        "c.colliculocale",
        "c.collicurules",
        "('public')",
    );
    assert_extension_filter_with_alias(
        &query,
        "ext",
        "pg_collation",
        "deptype = 'e'",
        "collations",
    );
}

#[test]
fn build_ts_configs_query_extension_filter_is_precise() {
    let query = Dump::build_ts_configs_query("('public')");
    assert_extension_filter_with_alias(
        &query,
        "ext",
        "pg_ts_config",
        "deptype = 'e'",
        "ts_configs",
    );
}

#[test]
fn build_ts_config_mappings_query_extension_filter_is_precise() {
    let query = Dump::build_ts_config_mappings_query("('public')");
    assert_extension_filter_with_alias(
        &query,
        "ext",
        "pg_ts_config",
        "deptype = 'e'",
        "ts_config_mappings",
    );
}

#[test]
fn build_ts_config_oids_query_extension_filter_is_precise() {
    let query = Dump::build_ts_config_oids_query("('public')");
    assert_extension_filter_with_alias(
        &query,
        "ext",
        "pg_ts_config",
        "deptype = 'e'",
        "ts_config_oids",
    );
}

#[test]
fn build_ts_dicts_query_extension_filter_is_precise() {
    let query = Dump::build_ts_dicts_query("('public')");
    assert_extension_filter_with_alias(&query, "ext", "pg_ts_dict", "deptype = 'e'", "ts_dicts");
}

#[test]
fn build_casts_query_extension_filter_is_precise() {
    let query = Dump::build_casts_query("('public')");
    assert_extension_filter_with_alias(&query, "pd", "pg_cast", "deptype IN ('e', 'i')", "casts");
}

#[test]
fn build_operators_query_extension_filter_is_precise() {
    let query = Dump::build_operators_query("('public')");
    assert_extension_filter_with_alias(&query, "ext", "pg_operator", "deptype = 'e'", "operators");
}

#[test]
fn build_fdws_query_extension_filter_is_precise() {
    let query = Dump::build_fdws_query();
    assert_extension_filter_with_alias(
        query,
        "ext",
        "pg_foreign_data_wrapper",
        "deptype = 'e'",
        "fdws",
    );
}

#[test]
fn build_servers_query_extension_filter_is_precise() {
    let query = Dump::build_servers_query();
    assert_extension_filter_with_alias(
        query,
        "ext",
        "pg_foreign_server",
        "deptype = 'e'",
        "foreign servers",
    );
}

#[test]
fn build_column_dependents_query_index_extension_filter_is_precise() {
    let query = Dump::build_column_dependents_query("('public')");
    assert_extension_filter_with_alias(
        &query,
        "ext",
        "pg_catalog.pg_class",
        "deptype = 'e'",
        "column dependents (index branch)",
    );
}

#[test]
fn build_column_dependents_query_constraint_extension_filter_is_precise() {
    let query = Dump::build_column_dependents_query("('public')");
    assert_extension_filter_with_alias(
        &query,
        "ext",
        "pg_catalog.pg_constraint",
        "deptype = 'e'",
        "column dependents (constraint branch)",
    );
}

#[test]
fn build_sequences_standalone_query_owner_dep_join_is_precise() {
    let query = Dump::build_sequences_standalone_query("('public')");
    assert!(
        query.contains("dep.classid = 'pg_class'::regclass"),
        "sequences: owner-dep join must filter dep.classid to 'pg_class'"
    );
    assert!(
        query.contains("dep.refclassid = 'pg_class'::regclass"),
        "sequences: owner-dep join must filter dep.refclassid to 'pg_class'"
    );
}
