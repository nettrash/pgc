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
        multirange_schema: None,
        domain_collation_name: None,
        comment: None,
        acl: Vec::new(),
        acl_explicitly_empty: false,
        typed_tables: None,
        composite_stored_in: None,
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

// A view's schema/name come from quote_ident, so a name needing quotes is stored
// as `"MyBase"`, while table_relation holds the raw catalog name `MyBase`. The drop
// order must still see the edge, or clear emits `drop view "MyBase"` before the
// view that depends on it and PostgreSQL rejects the script.
#[test]
fn clear_script_orders_quoted_view_names_by_dependency() {
    let mut dump = empty_dump();
    dump.schemas.push(make_schema("t"));
    dump.views.push(make_view("t", "\"MyBase\""));
    dump.views.push(make_view_with_deps(
        "t",
        "\"MyDependent\"",
        vec!["t.MyBase"],
    ));

    let script = dump.generate_clear_script(false, false, false);
    let dependent = script
        .find("drop view if exists t.\"MyDependent\"")
        .expect("dependent view must be dropped");
    let base = script
        .find("drop view if exists t.\"MyBase\"")
        .expect("base view must be dropped");
    assert!(
        dependent < base,
        "expected t.\"MyDependent\" to be dropped before t.\"MyBase\"; got:\n{script}"
    );
}

// "A" and a are different views. A key that folded case would map both to the same
// entry, so the edge from "DepOnA" would land on whichever view was stored last and
// "A" could be dropped while its dependent still stands.
#[test]
fn clear_script_keeps_case_distinct_view_names_apart() {
    let mut dump = empty_dump();
    dump.schemas.push(make_schema("t"));
    dump.views.push(make_view("t", "\"A\""));
    dump.views.push(make_view("t", "a"));
    dump.views
        .push(make_view_with_deps("t", "\"DepOnA\"", vec!["t.A"]));

    let script = dump.generate_clear_script(false, false, false);
    let dependent = script
        .find("drop view if exists t.\"DepOnA\"")
        .expect("dependent view must be dropped");
    let quoted_a = script
        .find("drop view if exists t.\"A\"")
        .expect("t.\"A\" must be dropped");
    assert!(
        dependent < quoted_a,
        "expected t.\"DepOnA\" to be dropped before t.\"A\"; got:\n{script}"
    );
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

// A view whose body only calls functions has no rows in view_table_usage, so an
// inner join against it dropped the view from the dump entirely (issue #219).
#[test]
fn build_regular_views_query_does_not_join_view_table_usage() {
    let query = Dump::build_regular_views_query("('public')");
    assert!(
        !query.contains("view_table_usage"),
        "regular view query must not join view_table_usage: it excludes views that \
         reference only functions, and hides views whose tables the dump role does not own"
    );
}

#[test]
fn view_queries_resolve_table_relation_through_pg_rewrite() {
    // The dependency on a referenced relation is recorded against the view's
    // _RETURN rewrite rule, not against the view's pg_class row, so a walk that
    // reads pg_depend for the relation oid directly always yields no relations.
    for (label, query) in [
        (
            "regular views",
            Dump::build_regular_views_query("('public')"),
        ),
        (
            "materialized views",
            Dump::build_materialized_views_query("('public')"),
        ),
    ] {
        assert!(
            query.contains("dep.classid = 'pg_rewrite'::regclass")
                && query.contains("r.ev_class = c.oid")
                && query.contains("and dep.objid = r.oid"),
            "expected {label} to resolve table_relation via the view's rewrite rule"
        );
        assert!(
            !query.contains("and dep.objid = c.oid"),
            "expected {label} not to anchor table_relation on the view's pg_class row: \
             a view's relation dependencies hang off its rewrite rule, so that walk \
             always returns no relations"
        );
        // Without this the walk also follows user-defined DO INSTEAD rules on the
        // view and credits their target tables to the view's definition.
        assert!(
            query.contains("and r.rulename = '_RETURN'"),
            "expected {label} to follow only the _RETURN rule that holds the view definition"
        );
    }
}

#[test]
fn view_queries_record_the_routines_the_view_calls() {
    // pre-cascade: the routines a view's _RETURN rule depends on, keyed like
    // `Routine::dependency_key`, so a routine's DROP ... CASCADE is matched
    // by signature rather than by name. Built-ins are never dropped by pgc.
    let subquery = Dump::view_routine_dependencies_subquery();
    for fragment in [
        "join pg_depend dep on dep.classid = 'pg_rewrite'::regclass and dep.objid = r.oid",
        "join pg_proc p on p.oid = dep.refobjid",
        "where r.ev_class = c.oid",
        "and r.rulename = '_RETURN'",
        "and dep.refclassid = 'pg_proc'::regclass",
        "and dep.deptype = 'n'",
        "and pn.nspname not in ('pg_catalog', 'information_schema')",
        "select distinct quote_ident(pn.nspname) || '.' || quote_ident(p.proname)",
        "|| '(' || pg_get_function_identity_arguments(p.oid) || ')'",
        "order by 1",
    ] {
        assert!(
            subquery.contains(fragment),
            "missing {fragment:?} in {subquery}"
        );
    }
    for query in [
        Dump::build_regular_views_query("('public')"),
        Dump::build_materialized_views_query("('public')"),
    ] {
        assert!(
            query.contains(&format!("{subquery} as routine_dependencies,")),
            "{query}"
        );
    }
}

#[test]
fn build_materialized_views_query_filters_by_pg_class() {
    let query = Dump::build_materialized_views_query("('public')");
    assert!(
        query.contains("d.classoid = 'pg_class'::regclass"),
        "expected pg_class classoid filter for materialized view comments"
    );
}

// Issue #235: a materialized view's indexes were fetched only by the table-side
// bulk query, which distributes rows into the dump's table list (relkind r/p) —
// so they matched nothing and never reached the dump.
#[test]
fn build_materialized_view_indexes_query_targets_matviews_only() {
    let query = Dump::build_materialized_view_indexes_query("('public')");
    assert!(
        query.contains("mv.relkind = 'm'"),
        "expected the index query to be anchored on the indexed relation being a \
         materialized view: {query}"
    );
    assert!(
        query.contains("mv.oid = idx.indrelid"),
        "the relkind filter has to be on the indexed relation, not the index itself"
    );
    assert!(
        query.contains("n.nspname in ('public')"),
        "expected the schema filter to be applied: {query}"
    );
    assert!(
        query.contains("d.classoid = 'pg_class'::regclass"),
        "expected pg_class classoid filter for materialized view index comments"
    );
    assert!(
        query.contains("ext_dep.deptype = 'e'"),
        "expected extension-owned indexes to be excluded, as every other dump query does"
    );
}

// The indexes are joined to their view on schema/name, so both queries have to
// spell them the same way — quote_ident, which is what the view stores —
// or the indexes of a view whose name needs quoting would silently be dropped
// again.
#[test]
fn build_materialized_view_indexes_query_join_keys_match_the_view_query() {
    let query = Dump::build_materialized_view_indexes_query("('public')");
    assert!(query.contains("quote_ident(n.nspname) as schemaname"));
    assert!(query.contains("quote_ident(mv.relname) as matviewname"));
    assert!(query.contains("quote_ident(ic.relname) as indexname"));
    let views = Dump::build_materialized_views_query("('public')");
    assert!(views.contains("quote_ident(mv.schemaname) as table_schema"));
    assert!(views.contains("quote_ident(mv.matviewname) as table_name"));
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

// `_` is a LIKE wildcard. Unescaped, `not like 'pg_%'` also hid every user
// table whose name merely starts with `pg` plus any one character
// (`pgbench_accounts`, `pg1_orders`) from every dump.
#[test]
fn build_tables_standalone_query_pg_prefix_filter_escapes_the_underscore() {
    let query = Dump::build_tables_standalone_query("('public')");
    assert!(
        query.contains("t.tablename not like 'pg\\_%'"),
        "the pg_ prefix filter must match a literal underscore"
    );
    assert!(
        !query.contains("not like 'pg_%'"),
        "an unescaped `_` matches any character and hides pgX... tables"
    );
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

// Extension-owned views are excluded from the view list, so fetching their columns
// would be pure waste (large extensions such as PostGIS ship many views); the columns
// query must carry the same extension filter as the view list queries.
#[test]
fn build_view_columns_query_extension_filter_is_precise() {
    let query = Dump::build_view_columns_query("('public')");
    assert_extension_filter(&query, "pg_class", "view columns");
}

// A broad --scheme pattern such as `%` must never resolve to system schemas: the
// per-session pg_temp_N / pg_toast_temp_N schemas cannot be recreated ("unacceptable
// schema name", issue #229) and pg_toast/pg_catalog are not dumpable either.
// PostgreSQL reserves the whole pg_ prefix, so filtering it excludes no user schema.
#[test]
fn build_schemas_query_excludes_all_pg_prefixed_schemas() {
    let query = Dump::build_schemas_query();
    assert!(
        query.contains("n.nspname not like 'pg\\_%'"),
        "schema resolution must exclude every pg_-prefixed schema"
    );
    assert!(
        query.contains("n.nspname <> 'information_schema'"),
        "information_schema must stay excluded"
    );
}

// Collations are schema-scoped: two different collations may share a bare name, and
// PostgreSQL rejects OR REPLACE across them ("cannot change collation of view column
// ... from \"mycoll\" to \"mycoll\"", verified live). The capture must therefore
// schema-qualify non-pg_catalog collations or or_replace_compatible could call two
// different collations equal and emit an OR REPLACE the server refuses.
#[test]
fn build_view_columns_query_qualifies_non_catalog_collations() {
    let query = Dump::build_view_columns_query("('public')");
    assert!(
        query.contains("nco.nspname || '.' || co.collname"),
        "non-pg_catalog collations must be captured schema-qualified"
    );
    assert!(
        query.contains("when nco.nspname = 'pg_catalog' then co.collname"),
        "pg_catalog collations must stay bare for dump stability"
    );
}

#[test]
fn build_materialized_views_query_extension_filter_is_precise() {
    let query = Dump::build_materialized_views_query("('public')");
    assert_extension_filter(&query, "pg_class", "materialized views");
}

#[test]
fn build_routines_standalone_query_extension_filter_is_precise() {
    let query = Dump::build_routines_standalone_query("('public')", 140000);
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
    let query = Dump::build_statistics_query("('public')", 140000);
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
    for pg_version in [140000, 190000] {
        let query = Dump::build_fdws_query(pg_version);
        assert_extension_filter_with_alias(
            &query,
            "ext",
            "pg_foreign_data_wrapper",
            "deptype = 'e'",
            "fdws",
        );
    }
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
    let query = Dump::build_column_dependents_query("('public')", 180000);
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
    let query = Dump::build_column_dependents_query("('public')", 180000);
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

// ==== pg19:publications tests ====

// Each column a server lacks must be replaced, never named: selecting
// `puballsequences` on 18 or `prqual` on 14 fails the whole dump.

#[test]
fn build_publications_query_gates_new_columns_by_version() {
    let pg14 = Dump::build_publications_query(140000);
    assert!(pg14.contains("false as all_sequences"), "{pg14}");
    assert!(
        pg14.contains("NULL::text as publish_generated_columns"),
        "{pg14}"
    );
    assert!(
        pg14.contains("p.pubviaroot as via_partition_root"),
        "{pg14}"
    );
    assert!(!pg14.contains("puballsequences"), "{pg14}");
    assert!(!pg14.contains("pubgencols"), "{pg14}");

    let pg18 = Dump::build_publications_query(180000);
    assert!(pg18.contains("false as all_sequences"), "{pg18}");
    assert!(
        pg18.contains(
            "CASE p.pubgencols WHEN 'n' THEN NULL WHEN 's' THEN 'stored' ELSE p.pubgencols::text END as publish_generated_columns"
        ),
        "{pg18}"
    );
    assert!(!pg18.contains("puballsequences"), "{pg18}");

    let pg19 = Dump::build_publications_query(190000);
    assert!(
        pg19.contains("p.puballsequences as all_sequences"),
        "{pg19}"
    );
    assert!(pg19.contains("p.pubgencols"), "{pg19}");
}

#[test]
fn build_publication_rels_query_gates_new_columns_by_version() {
    let pg14 = Dump::build_publication_rels_query(140000);
    assert!(pg14.contains("false as is_except"), "{pg14}");
    assert!(pg14.contains("NULL::text[] as columns"), "{pg14}");
    assert!(pg14.contains("NULL::text as row_filter"), "{pg14}");
    assert!(pg14.contains("WHERE NOT p.puballtables\n"), "{pg14}");
    for column in ["prexcept", "prattrs", "prqual"] {
        assert!(!pg14.contains(column), "{column} in {pg14}");
    }

    let pg15 = Dump::build_publication_rels_query(150000);
    assert!(
        pg15.contains("pg_get_expr(pr.prqual, pr.prrelid) as row_filter"),
        "{pg15}"
    );
    assert!(pg15.contains("a.attnum = ANY (pr.prattrs)"), "{pg15}");
    assert!(pg15.contains("ORDER BY a.attname"), "{pg15}");
    assert!(pg15.contains("WHERE NOT p.puballtables\n"), "{pg15}");
    assert!(!pg15.contains("prexcept"), "{pg15}");

    let pg19 = Dump::build_publication_rels_query(190000);
    assert!(pg19.contains("pr.prexcept as is_except"), "{pg19}");
    // EXCEPT rows live on all-tables publications and must not read as members.
    assert!(
        pg19.contains(
            "WHERE (NOT p.puballtables AND NOT pr.prexcept) OR (p.puballtables AND pr.prexcept)"
        ),
        "{pg19}"
    );
    assert!(pg19.contains("pr.prattrs"), "{pg19}");
}

#[test]
fn build_publication_schemas_query_exists_from_pg15() {
    assert_eq!(Dump::build_publication_schemas_query(140000), None);
    let pg15 = Dump::build_publication_schemas_query(150000).expect("PG15 has TABLES IN SCHEMA");
    assert!(pg15.contains("JOIN pg_publication_namespace pn ON pn.pnpubid = p.oid"));
    assert_eq!(
        Dump::build_publication_schemas_query(190000),
        Some(pg15),
        "no later change"
    );
}

// ==== pg19:subscriptions-fdw tests ====

#[test]
fn build_subscriptions_query_pre19_references_no_pg19_column() {
    // A PG19 column on PG14-18 would fail the query; before the SQLSTATE
    // narrowing that silently emptied the subscription list.
    for pg_version in [140000, 150000, 160000, 170000, 180000] {
        let query = Dump::build_subscriptions_query(pg_version);
        for column in [
            "subserver",
            "subretaindeadtuples",
            "submaxretention",
            "subwalrcvtimeout",
            "pg_foreign_server",
        ] {
            assert!(
                !query.contains(column),
                "{pg_version}: must not reference {column}:\n{query}"
            );
        }
        assert!(query.contains("NULL::text as sub_server"));
        assert!(query.contains("false as sub_retain_dead_tuples"));
        assert!(query.contains("0 as sub_max_retention"));
        assert!(query.contains("NULL::text as sub_wal_receiver_timeout"));
    }
}

#[test]
fn build_subscriptions_query_pg19_reads_server_and_options() {
    let query = Dump::build_subscriptions_query(190000);
    assert!(query.contains("quote_ident(fs.srvname) as sub_server"));
    assert!(query.contains("LEFT JOIN pg_foreign_server fs ON fs.oid = s.subserver"));
    assert!(query.contains("s.subretaindeadtuples as sub_retain_dead_tuples"));
    assert!(query.contains("s.submaxretention as sub_max_retention"));
    assert!(query.contains("NULLIF(s.subwalrcvtimeout, '-1') as sub_wal_receiver_timeout"));
    assert!(
        !query.contains("subretentionactive"),
        "subretentionactive is runtime state, not DDL"
    );
}

#[test]
fn build_subscriptions_query_is_scoped_to_the_current_database_and_null_safe() {
    // pg_subscription is a shared catalog: without the subdbid filter every
    // database's subscriptions leak into every dump. subconninfo is NULL for
    // a PG19 SERVER subscription and used to panic the String decode.
    for pg_version in [140000, 180000, 190000] {
        let query = Dump::build_subscriptions_query(pg_version);
        assert!(
            query.contains(
                "WHERE s.subdbid = (SELECT oid FROM pg_database WHERE datname = current_database())"
            ),
            "{pg_version}:\n{query}"
        );
        assert!(query.contains("COALESCE(s.subconninfo, '') as sub_conninfo"));
        assert!(
            query.contains("'{}'::text[]"),
            "format! must render the literal braces"
        );
    }
}

#[test]
fn build_fdws_query_gates_the_connection_function_on_pg19() {
    let pre19 = Dump::build_fdws_query(180000);
    assert!(pre19.contains("NULL::text as connection_func"));
    assert!(!pre19.contains("fdwconnection"), "{pre19}");
    assert!(pre19.contains("'{}'::text[]"));

    let pg19 = Dump::build_fdws_query(190000);
    assert!(pg19.contains("CASE WHEN fdw.fdwconnection != 0 THEN"));
    assert!(pg19.contains("LEFT JOIN pg_proc cp ON cp.oid = fdw.fdwconnection"));
    assert!(pg19.contains("LEFT JOIN pg_namespace cn ON cn.oid = cp.pronamespace"));
    assert!(pg19.contains("quote_ident(cn.nspname) || '.' || quote_ident(cp.proname)"));
}

/// A database error carrying only a SQLSTATE, to drive
/// `Dump::is_insufficient_privilege` without a server.
#[derive(Debug)]
struct SqlStateError(&'static str);

impl std::fmt::Display for SqlStateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "sqlstate {}", self.0)
    }
}

impl std::error::Error for SqlStateError {}

impl sqlx::error::DatabaseError for SqlStateError {
    fn message(&self) -> &str {
        "test"
    }
    fn code(&self) -> Option<std::borrow::Cow<'_, str>> {
        Some(std::borrow::Cow::Borrowed(self.0))
    }
    fn as_error(&self) -> &(dyn std::error::Error + Send + Sync + 'static) {
        self
    }
    fn as_error_mut(&mut self) -> &mut (dyn std::error::Error + Send + Sync + 'static) {
        self
    }
    fn into_error(self: Box<Self>) -> Box<dyn std::error::Error + Send + Sync + 'static> {
        self
    }
    fn kind(&self) -> sqlx::error::ErrorKind {
        sqlx::error::ErrorKind::Other
    }
}

#[test]
fn only_insufficient_privilege_counts_as_a_permission_failure() {
    let denied = sqlx::Error::Database(Box::new(SqlStateError("42501")));
    assert!(Dump::is_insufficient_privilege(&denied));

    // undefined_column: what a wrongly gated PG19 column raises on PG14-18.
    let undefined_column = sqlx::Error::Database(Box::new(SqlStateError("42703")));
    assert!(!Dump::is_insufficient_privilege(&undefined_column));
    assert!(!Dump::is_insufficient_privilege(&sqlx::Error::PoolTimedOut));
}

// ==== pg19:routines tests ====

#[test]
fn build_routines_standalone_query_reads_the_pg19_routine_columns_unconditionally() {
    // Aggregate SUPPORT (pg_proc.prosupport, PG12+), negative SSPACE / MSSPACE
    // (pg_aggregate.aggtransspace / aggmtransspace) and PG19's empty-list proconfig
    // all live in columns every supported server has, so the PG19 routine work
    // needs no version branch in the query: the raw values are already read, and
    // everything version-specific happens on emission.
    let query = Dump::build_routines_standalone_query("('public')", 140000);
    for column in [
        "r.prosupport::regproc::text",
        "agg.aggtransspace as agg_sspace",
        "agg.aggmtransspace as agg_msspace",
        "r.proconfig::text[] as proconfig",
        "r.proparallel::text as proparallel",
        "agg.aggkind::text as agg_kind",
        "agg.aggnumdirectargs as agg_numdirectargs",
    ] {
        assert!(
            query.contains(column),
            "routines query must select {column}"
        );
    }
}

// ==== pg19:statistics tests ====

#[test]
fn build_statistics_query_reads_the_deparsed_on_list_on_pg14_and_later() {
    for pg_version in [140000, 180000, 190000] {
        let query = Dump::build_statistics_query("('public')", pg_version);
        assert!(
            query.contains("pg_get_statisticsobjdef_columns(s.oid) as stat_columns"),
            "PG {pg_version}: the ON list must come from the server deparser: {query}"
        );
    }
}

#[test]
fn build_statistics_query_reads_the_statistics_target_as_int4() {
    // `smallint` on PG17+, `integer` before; the i32 field decodes only int4.
    for pg_version in [130000, 140000, 170000, 190000] {
        let query = Dump::build_statistics_query("('public')", pg_version);
        assert!(
            query.contains("s.stxstattarget::int4 as stat_target"),
            "PG {pg_version}: {query}"
        );
    }
}

#[test]
fn build_statistics_query_falls_back_to_null_before_pg14() {
    let query = Dump::build_statistics_query("('public')", 130000);
    assert!(
        query.contains("null::text as stat_columns"),
        "pre-14 servers have no pg_get_statisticsobjdef_columns: {query}"
    );
    assert!(!query.contains("pg_get_statisticsobjdef_columns"));
    // Everything else is shared by both branches.
    assert!(query.contains("pg_get_statisticsobjdef(s.oid) as stat_def"));
    assert_extension_filter(&query, "pg_statistic_ext", "extended statistics (pre-14)");
}

/// The branch of a column-dependents query that selects `kind`: from the
/// `union all` before it to the next one (or the end).
fn column_dependents_branch<'a>(query: &'a str, kind: &str) -> &'a str {
    let at = query
        .find(&format!("'{kind}',"))
        .unwrap_or_else(|| panic!("no {kind} branch: {query}"));
    let start = query[..at].rfind("union all").unwrap_or(0);
    let end = query[at..]
        .find("union all")
        .map_or(query.len(), |offset| at + offset);
    &query[start..end]
}

#[test]
fn build_column_dependents_query_has_a_statistics_branch() {
    let query = Dump::build_column_dependents_query("('public')", 180000);
    assert_eq!(
        query.matches("union all").count(),
        4,
        "five branches: index, constraint, policy, statistic, trigger"
    );
    for fragment in [
        "'statistic',",
        "quote_ident(stx_nsp.nspname),",
        "quote_ident(stx.stxname)",
        "join pg_catalog.pg_statistic_ext stx on stx.oid = d.objid",
        "join pg_catalog.pg_namespace stx_nsp on stx_nsp.oid = stx.stxnamespace",
        "and d.classid = 'pg_catalog.pg_statistic_ext'::regclass",
    ] {
        assert!(
            query.contains(fragment),
            "statistic branch is missing `{fragment}`: {query}"
        );
    }
    // The statistic branch must keep the column-level and live-column
    // guards every other branch has.
    let branch = column_dependents_branch(&query, "statistic");
    assert!(branch.contains("and d.refobjsubid > 0"));
    assert!(branch.contains("and refatt.attisdropped = false"));
    assert!(branch.contains("and refnsp.nspname in ('public')"));
}

#[test]
fn build_column_dependents_query_statistic_extension_filter_is_precise() {
    let query = Dump::build_column_dependents_query("('public')", 180000);
    let branch = column_dependents_branch(&query, "statistic");
    assert_extension_filter_with_alias(
        branch,
        "ext",
        "pg_catalog.pg_statistic_ext",
        "deptype = 'e'",
        "column dependents (statistic branch)",
    );
    assert!(branch.contains("ext.objid = stx.oid"));
}

#[test]
fn pre_columns_column_dependents_query_has_a_trigger_branch() {
    // A trigger naming a column (UPDATE OF, WHEN) blocks its drop: the
    // comparer drops it ahead of a column drop + add and creates it again.
    for version in [120000, 130000, 140000, 180000, 190000] {
        let query = Dump::build_column_dependents_query("('public')", version);
        let branch = column_dependents_branch(&query, "trigger");
        for fragment in [
            "quote_ident(tg_nsp.nspname),",
            "quote_ident(tg_tbl.relname),",
            "quote_ident(tg.tgname)",
            "join pg_catalog.pg_trigger tg on tg.oid = d.objid",
            "join pg_catalog.pg_class tg_tbl on tg_tbl.oid = tg.tgrelid",
            "and d.classid = 'pg_catalog.pg_trigger'::regclass",
            "and d.refobjsubid > 0",
            "and refatt.attisdropped = false",
            "and refnsp.nspname in ('public')",
            // A foreign key's triggers, and a PG13/14 clone, are internal.
            "and not tg.tgisinternal",
        ] {
            assert!(
                branch.contains(fragment),
                "{version}: trigger branch is missing `{fragment}`: {branch}"
            );
        }
        assert_extension_filter_with_alias(
            branch,
            "ext",
            "pg_catalog.pg_trigger",
            "deptype = 'e'",
            "column dependents (trigger branch)",
        );
        assert!(branch.contains("ext.objid = tg.oid"));
        // A partition's clone (PG13+, not internal from 15) goes and comes
        // back with its parent's trigger; tgparentid does not exist before 13.
        assert_eq!(
            branch.contains("and tg.tgparentid = 0"),
            version >= 130000,
            "{version}: {branch}"
        );
    }
    // The other four branches do not change with the version.
    let without_trigger = |version| {
        let query = Dump::build_column_dependents_query("('public')", version);
        let at = query.find("'trigger',").unwrap();
        query[..query[..at].rfind("union all").unwrap()].to_string()
    };
    assert_eq!(without_trigger(120000), without_trigger(190000));
}

// ==== pg19:misc tests ====

#[test]
fn session_setup_forces_standard_conforming_strings_only() {
    // A session SET overrides a server/database/role-level `off` on
    // PostgreSQL 14-18, like pg_dump's own connection setup.
    assert_eq!(SESSION_SETUP_SQL, "set standard_conforming_strings = on");
    // PostgreSQL 19 removed escape_string_warning and refuses a connection
    // that sets it, so it must never be part of the setup.
    assert!(!SESSION_SETUP_SQL.contains("escape_string_warning"));
}

#[test]
fn pool_options_keep_the_requested_pool_size() {
    assert_eq!(pool_options(7).get_max_connections(), 7);
    assert_eq!(pool_options(16).get_max_connections(), 16);
}

#[test]
fn build_available_extensions_query_reads_named_columns_of_both_views() {
    let query = Dump::build_available_extensions_query();
    assert!(query.contains("from pg_available_extensions a"), "{query}");
    assert!(
        query.contains("left join pg_available_extension_versions v on v.name = a.name"),
        "an extension without listed versions must still yield a row: {query}"
    );
    assert!(query.contains("quote_ident(a.name) as name"), "{query}");
    assert!(
        query.contains("coalesce(a.default_version, '') as default_version"),
        "{query}"
    );
    assert!(query.contains("as versions"), "{query}");
    // PostgreSQL 19 added `location` to both views: never select `*`.
    assert!(!query.contains('*'), "{query}");
    assert!(!query.contains("location"), "{query}");
    // Every column read exists on PostgreSQL 14, so nothing is gated.
    assert!(!query.contains("server_version"), "{query}");
}

#[test]
fn new_dump_has_no_available_extensions() {
    assert!(empty_dump().available_extensions.is_empty());
}

#[test]
fn build_standard_conforming_strings_off_query_reads_the_session_default() {
    let query = Dump::build_standard_conforming_strings_off_query();
    // reset_val, because every pgc connection has already SET the current
    // value to on (SESSION_SETUP_SQL): current_setting() would always say on.
    assert!(query.contains("reset_val = 'off'"), "{query}");
    assert!(query.contains("from pg_catalog.pg_settings"), "{query}");
    assert!(
        query.contains("where name = 'standard_conforming_strings'"),
        "{query}"
    );
    assert!(!query.contains("current_setting"), "{query}");
    // A missing row reads as on, never as a NULL the bool decode rejects.
    assert!(query.contains("coalesce("), "{query}");
    assert!(query.contains("false"), "{query}");
    // pg_settings.reset_val exists on PostgreSQL 14, so nothing is gated.
    assert!(!query.contains("server_version"), "{query}");
}

#[test]
fn new_dump_assumes_standard_conforming_strings_on() {
    assert!(!empty_dump().standard_conforming_strings_off);
}

// ==== fix:sqlbody tests ====

#[test]
fn build_routines_standalone_query_flags_sql_standard_bodies_from_pg14() {
    // pg_proc.prosqlbody arrived in PostgreSQL 14, pgc's floor; 19 reads it the
    // same way. The body itself is read by a second query, under an empty
    // search_path, so this one only flags the routines that have one.
    for pg_version in [140000, 180006, 190000] {
        let query = Dump::build_routines_standalone_query("('public')", pg_version);
        assert!(
            query.contains("(r.prosqlbody is not null) as has_sql_body,"),
            "{pg_version}: {query}"
        );
        assert!(!query.contains("false as has_sql_body"), "{query}");
        assert!(!query.contains("pg_get_function_sqlbody"), "{query}");
        // prosrc stays: it is the body of every string-body routine.
        assert!(query.contains("r.prosrc,"), "{query}");
    }
}

#[test]
fn build_routines_standalone_query_flags_no_sql_body_below_pg14() {
    let query = Dump::build_routines_standalone_query("('public')", 130000);
    assert!(query.contains("false as has_sql_body,"), "{query}");
    assert!(!query.contains("prosqlbody"), "{query}");
    assert!(query.contains("r.prosrc,"), "{query}");
}

#[test]
fn build_routines_standalone_query_differs_only_in_the_sql_body_column() {
    // Everything but the gated column is shared, so the extension filter the
    // other routine-query tests assert holds for every branch.
    let old = Dump::build_routines_standalone_query("('public')", 130000);
    let new = Dump::build_routines_standalone_query("('public')", 140000);
    assert_eq!(
        old.replace(
            "false as has_sql_body",
            "(r.prosqlbody is not null) as has_sql_body"
        ),
        new
    );
    assert_extension_filter(&old, "pg_proc", "routines");
}

#[test]
fn sql_bodies_are_deparsed_with_an_empty_search_path() {
    // Under the default `"$user", public` the deparser prints a public callee
    // as `z_callee(p)` and a public table as `FROM orders`, even when the
    // author wrote `public.`; the dependency scans only match qualified names,
    // so a SQL-standard caller was created before its callee. pg_dump reads
    // the body with an empty search_path; so does pgc, local to one
    // transaction, so the pooled connection gets its own path back.
    assert_eq!(
        EMPTY_SEARCH_PATH_SQL,
        "select pg_catalog.set_config('search_path', '', true)"
    );
    let query = Dump::build_routine_sql_bodies_query("('public', 'app')");
    assert!(
        query.contains("pg_catalog.pg_get_function_sqlbody(r.oid) as prosqlbody"),
        "{query}"
    );
    assert!(query.contains("where r.prosqlbody is not null"), "{query}");
    assert!(query.contains("n.nspname in ('public', 'app')"), "{query}");
    // Nothing in it depends on the path it runs under.
    assert!(query.contains("from pg_catalog.pg_proc r"), "{query}");
    assert!(
        query.contains("join pg_catalog.pg_namespace n on n.oid = r.pronamespace"),
        "{query}"
    );
}

#[test]
fn sql_body_dependencies_query_reads_the_hard_dependencies_of_the_body() {
    let query = Dump::build_routine_sql_body_dependencies_query("('public')");
    // Only routines with a SQL-standard body: a string body records none.
    assert!(query.contains("where r.prosqlbody is not null"), "{query}");
    assert!(query.contains("n.nspname in ('public')"), "{query}");
    for column in ["as relations", "as columns", "as routines", "as operators"] {
        assert!(query.contains(column), "{column}: {query}");
    }
    // The body's own `n` rows, never an extension's or an internal one.
    assert_eq!(
        query.matches("dep.classid = 'pg_proc'::regclass").count(),
        4,
        "{query}"
    );
    assert_eq!(query.matches("dep.deptype = 'n'").count(), 4, "{query}");
    // Relations the comparer drives, in View::table_relation's unquoted form;
    // a composite type's relation (relkind c) has a lifecycle of its own.
    assert_eq!(
        query
            .matches("dc.relkind in ('r', 'p', 'v', 'm', 'f', 'S')")
            .count(),
        2,
        "{query}"
    );
    assert!(
        query.contains("select distinct dn.nspname || '.' || dc.relname\n"),
        "{query}"
    );
    assert!(query.contains("and dep.refobjsubid > 0"), "{query}");
    // Routine and operator keys in the routine and operator queries' form.
    assert!(
        query.contains("quote_ident(pn.nspname) || '.' || quote_ident(p.proname)"),
        "{query}"
    );
    assert!(
        query.contains("|| '(' || pg_get_function_identity_arguments(p.oid) || ')'"),
        "{query}"
    );
    assert!(query.contains("and p.oid <> r.oid"), "{query}");
    assert!(
        query.contains("quote_ident(opn.nspname) || '.' || o.oprname || '('"),
        "{query}"
    );
    assert_eq!(query.matches("'NONE')").count(), 2, "{query}");
    // Read with the session's search_path, where those keys are rendered.
    assert!(!query.contains("set_config"), "{query}");
    assert!(!query.contains("pg_get_function_sqlbody"), "{query}");
}

#[test]
fn routine_body_prefers_the_sql_standard_body() {
    // A BEGIN ATOMIC / RETURN routine has an empty prosrc, which pgc 1.0.x
    // dumped as its body and replayed as `as $$$$`.
    let body = "BEGIN ATOMIC\n SELECT (p + 1);\nEND".to_string();
    assert_eq!(
        Dump::routine_body(String::new(), Some(body.clone())),
        (body, true)
    );
    assert_eq!(
        Dump::routine_body(String::new(), Some("RETURN (p + 1)".to_string())),
        ("RETURN (p + 1)".to_string(), true)
    );
}

#[test]
fn routine_body_keeps_a_carriage_return_inside_a_sql_standard_body() {
    // The deparser lays a body out with LF only, so a CR LF in it is part of
    // a literal: `SELECT E'a\r\nb'` deparses to a raw CR LF between quotes, and
    // normalising it would change the value the function returns.
    let body = "BEGIN ATOMIC\n SELECT 'a\r\nb'::text;\nEND".to_string();
    assert_eq!(
        Dump::routine_body(String::new(), Some(body.clone())),
        (body, true)
    );
}

#[test]
fn routine_body_of_a_string_body_is_prosrc_with_lf_line_endings() {
    assert_eq!(
        Dump::routine_body("\r\nbegin\r\n  return 1;\r\nend\r\n".to_string(), None),
        ("\nbegin\n  return 1;\nend\n".to_string(), false)
    );
    assert_eq!(
        Dump::routine_body(" select 1 ".to_string(), None),
        (" select 1 ".to_string(), false)
    );
}

/// `schema.name()` with a SQL-standard body whose `pg_depend` rows name
/// `relations` and `routines`.
fn sql_body_routine(schema: &str, name: &str, relations: &[&str], routines: &[&str]) -> Routine {
    let mut routine = make_routine(schema, name);
    routine.lang = "sql".to_string();
    routine.source_code = "RETURN 1".to_string();
    routine.is_sql_body = true;
    routine.sql_body_dependencies = crate::dump::routine::SqlBodyDependencies {
        relations: relations.iter().map(|r| r.to_string()).collect(),
        routines: routines.iter().map(|r| r.to_string()).collect(),
        ..Default::default()
    };
    routine.hash();
    routine
}

fn clear_script_position(script: &str, needle: &str) -> usize {
    script
        .find(needle)
        .unwrap_or_else(|| panic!("expected {needle:?} in:\n{script}"))
}

fn clear_dump() -> Dump {
    let mut dump = Dump::new(DumpConfig::default());
    dump.schemas
        .push(Schema::new("app".to_string(), "app".to_string(), None));
    dump
}

#[test]
fn clear_drops_a_sql_standard_reader_before_the_relations_it_reads() {
    // A BEGIN ATOMIC / RETURN body is a hard dependent of what it reads:
    // `drop table` failed under it when routines came after the tables.
    let mut dump = clear_dump();
    dump.tables.push(make_table("app", "orders"));
    dump.routines.push(make_routine("app", "plain"));
    dump.routines
        .push(sql_body_routine("app", "order_count", &["app.orders"], &[]));

    let script = dump.generate_clear_script(true, true, false);
    let reader = clear_script_position(&script, "drop function if exists app.order_count ();");
    assert!(
        reader < clear_script_position(&script, "drop table if exists app.orders;"),
        "{script}"
    );
    assert_eq!(
        script
            .matches("drop function if exists app.order_count")
            .count(),
        1
    );
    // A string body records no dependency and keeps its place after the tables.
    assert!(
        clear_script_position(&script, "drop table if exists app.orders;")
            < clear_script_position(&script, "drop function if exists app.plain ();"),
        "{script}"
    );
}

#[test]
fn clear_orders_sql_standard_readers_among_the_views() {
    // v_calls calls the routine, which reads v_base: v_calls, routine, v_base.
    let mut dump = clear_dump();
    dump.views
        .push(make_view_with_deps("app", "v_base", vec!["app.orders"]));
    let mut v_calls = make_view("app", "v_calls");
    v_calls.definition = " SELECT app.reads_view() AS c;".to_string();
    dump.views.push(v_calls);
    dump.routines
        .push(sql_body_routine("app", "reads_view", &["app.v_base"], &[]));

    let script = dump.generate_clear_script(true, true, false);
    let v_calls = clear_script_position(&script, "drop view if exists app.v_calls;");
    let routine = clear_script_position(&script, "drop function if exists app.reads_view ();");
    let v_base = clear_script_position(&script, "drop view if exists app.v_base;");
    assert!(v_calls < routine && routine < v_base, "{script}");
    assert!(
        script.contains("/* ---> Drop Views and routines reading relations --------------- */"),
        "{script}"
    );
}

#[test]
fn clear_drops_a_sql_standard_caller_before_its_callee() {
    // In dump order the callee comes first; PostgreSQL refuses to drop it
    // while the caller's body names it. A caller of an early routine (one
    // reading a relation) is dropped early too.
    let mut dump = clear_dump();
    dump.tables.push(make_table("app", "orders"));
    dump.routines
        .push(sql_body_routine("app", "callee", &[], &[]));
    dump.routines
        .push(sql_body_routine("app", "caller", &[], &["app.callee()"]));
    dump.routines
        .push(sql_body_routine("app", "reader", &["app.orders"], &[]));
    dump.routines.push(sql_body_routine(
        "app",
        "reader_caller",
        &[],
        &["app.reader()"],
    ));

    let script = dump.generate_clear_script(true, true, false);
    let pos = |name: &str| {
        clear_script_position(&script, &format!("drop function if exists app.{name} ();"))
    };
    assert!(pos("caller") < pos("callee"), "{script}");
    assert!(pos("reader_caller") < pos("reader"), "{script}");
    assert!(
        pos("reader") < clear_script_position(&script, "drop table if exists app.orders;"),
        "{script}"
    );
}

#[test]
fn clear_drops_an_aggregate_before_its_sql_standard_support_function() {
    let mut dump = clear_dump();
    dump.routines
        .push(sql_body_routine("app", "sfunc", &[], &[]));
    let mut aggregate = make_routine("app", "agg");
    aggregate.kind = "aggregate".to_string();
    aggregate.arguments = "integer".to_string();
    aggregate.aggregate_info = Some(crate::dump::routine::AggregateInfo {
        sfunc: "app.sfunc".to_string(),
        stype: "integer".to_string(),
        sspace: None,
        finalfunc: None,
        finalfunc_extra: false,
        finalfunc_modify: None,
        combinefunc: None,
        serialfunc: None,
        deserialfunc: None,
        initcond: None,
        msfunc: None,
        minvfunc: None,
        mstype: None,
        msspace: None,
        mfinalfunc: None,
        mfinalfunc_extra: false,
        mfinalfunc_modify: None,
        minitcond: None,
        sortop: None,
        kind: 'n',
        num_direct_args: 0,
    });
    dump.routines.push(aggregate);

    let script = dump.generate_clear_script(true, true, false);
    assert!(
        clear_script_position(&script, "drop aggregate if exists app.agg (integer);")
            < clear_script_position(&script, "drop function if exists app.sfunc ();"),
        "{script}"
    );
}

#[test]
fn clear_keeps_its_output_for_dumps_without_sql_standard_bodies() {
    // Nothing is reordered: routines in dump order after the tables, and the
    // views section under its usual banner.
    let mut dump = clear_dump();
    dump.tables.push(make_table("app", "orders"));
    dump.views
        .push(make_view_with_deps("app", "v_base", vec!["app.orders"]));
    dump.routines.push(make_routine("app", "z_first"));
    dump.routines.push(make_routine("app", "a_second"));

    let script = dump.generate_clear_script(true, true, false);
    assert!(
        script.contains("/* ---> Drop Views --------------- */"),
        "{script}"
    );
    assert!(!script.contains("routines reading relations"), "{script}");
    let table = clear_script_position(&script, "drop table if exists app.orders;");
    let z_first = clear_script_position(&script, "drop function if exists app.z_first ();");
    let a_second = clear_script_position(&script, "drop function if exists app.a_second ();");
    assert!(table < z_first && z_first < a_second, "{script}");
}

// ==== fix:domains tests ====

/// `Dump::fill` hashes types only after `fetch_domain_constraints_standalone`
/// and `fetch_enums_standalone` have filled them in. pgc 1.0.x hashed each
/// type as its row arrived, so its stored hash ignored both, and a change to
/// only a domain CHECK or only an enum label was invisible to the comparer.
#[test]
fn hash_types_covers_constraints_and_labels_filled_in_after_the_rows() {
    let mut domain = make_pg_type("app", "amount");
    domain.typtype = b'd' as i8;
    domain.formatted_basetype = Some("integer".to_string());
    domain.hash();
    let row_time_domain_hash = domain.hash.clone();
    domain.domain_constraints = vec![DomainConstraint {
        name: "amount_check".to_string(),
        definition: "CHECK ((VALUE > 0))".to_string(),
    }];

    let mut status = make_pg_type("app", "status");
    status.typtype = b'e' as i8;
    status.hash();
    let row_time_enum_hash = status.hash.clone();
    status.enum_labels = vec!["new".to_string(), "done".to_string()];

    let mut unconstrained = make_pg_type("app", "plain");
    unconstrained.typtype = b'd' as i8;
    unconstrained.hash();
    let unconstrained_hash = unconstrained.hash.clone();

    let mut types = vec![domain, status, unconstrained];
    Dump::hash_types(&mut types);

    assert_ne!(types[0].hash, row_time_domain_hash);
    assert_ne!(types[1].hash, row_time_enum_hash);
    // A type with nothing filled in afterwards keeps the hash it always had.
    assert_eq!(types[2].hash, unconstrained_hash);
    for pg_type in &types {
        let mut fresh = pg_type.clone();
        fresh.hash();
        assert_eq!(pg_type.hash, fresh.hash, "{}", pg_type.typname);
    }
}

// ==== fix:grants tests ====

// ==== fix:replident tests ====

/// The tables query names the index behind REPLICA IDENTITY USING INDEX,
/// and reports a table left on USING INDEX without one (its index was
/// dropped) as NOTHING, which is how the server treats it.
#[test]
fn build_tables_standalone_query_reads_the_replica_identity_index() {
    let query = Dump::build_tables_standalone_query("('public')");
    let compact = query.split_whitespace().collect::<Vec<_>>().join(" ");
    assert!(
        compact.contains(
            "case when c.relreplident = 'i' then ( select quote_ident(ric.relname) \
             from pg_index ri join pg_class ric on ric.oid = ri.indexrelid \
             where ri.indrelid = c.oid and ri.indisreplident limit 1 ) end \
             as replica_identity_index"
        ),
        "{query}"
    );
    assert!(
        compact.contains(
            "case when c.relreplident = 'i' and not exists ( select 1 from pg_index ri \
             where ri.indrelid = c.oid and ri.indisreplident ) then 'n' \
             else c.relreplident end as relreplident"
        ),
        "{query}"
    );
    // The index's columns, key columns first and in index order, so the
    // alter path can tell which column changes touch the index.
    assert!(
        compact.contains(
            "case when c.relreplident = 'i' then array( select quote_ident(a.attname) \
             from pg_index ri \
             cross join lateral unnest(ri.indkey::int2[]) with ordinality as k(attnum, ord) \
             join pg_attribute a on a.attrelid = ri.indrelid and a.attnum = k.attnum \
             where ri.indrelid = c.oid and ri.indisreplident order by k.ord ) end \
             as replica_identity_columns"
        ),
        "{query}"
    );
    // pg_index.indisreplident, pg_index.indkey and pg_class.relreplident
    // exist on every supported server, so nothing is gated.
    assert!(!query.contains("server_version"), "{query}");
}

// ==== fix:rules tests ====

#[test]
fn build_rules_query_never_reads_a_view_return_rule() {
    // pgc must never emit anything for the rule that backs a view.
    let query = Dump::build_rules_query("('public')");
    assert!(query.contains("r.rulename <> '_RETURN'"), "{query}");
}

#[test]
fn build_rules_query_collects_rule_dependencies() {
    let query = Dump::build_rules_query("('public')");
    // One array each for relations, columns and routines, all drawn from the
    // rule's own `pg_depend` rows with deptype 'n'.
    for alias in ["rule_relations", "rule_columns", "rule_routines"] {
        assert!(query.contains(&format!(") as {alias}")), "{alias}: {query}");
    }
    assert_eq!(
        query
            .matches("where dep.classid = 'pg_rewrite'::regclass")
            .count(),
        3,
        "{query}"
    );
    assert_eq!(query.matches("and dep.objid = r.oid").count(), 3, "{query}");
    assert_eq!(query.matches("and dep.deptype = 'n'").count(), 3, "{query}");
    assert_eq!(
        query
            .matches("and dep.refclassid = 'pg_class'::regclass")
            .count(),
        2,
        "{query}"
    );
    assert!(
        query.contains("and dep.refclassid = 'pg_proc'::regclass"),
        "{query}"
    );
    // Columns are the rows with a positive subid, resolved through
    // pg_attribute; relations take every pg_class row, since a rule that reads
    // only some columns of a relation has no whole-relation row for it.
    assert_eq!(query.matches("dep.refobjsubid > 0").count(), 1, "{query}");
    assert!(
        query.contains(
            "join pg_attribute da on da.attrelid = dc.oid and da.attnum = dep.refobjsubid"
        ),
        "{query}"
    );
    // Routines are spelled like Routine's (schema, name, arguments) key.
    assert!(
        query.contains(
            "quote_ident(pn.nspname) || '.' || quote_ident(p.proname)\n                        || '(' || pg_get_function_identity_arguments(p.oid) || ')'"
        ),
        "{query}"
    );
    // Relations and columns keep the raw catalog spelling View::table_relation
    // uses, so the comparer normalizes both the same way.
    assert!(
        query.contains("select distinct dn.nspname || '.' || dc.relname\n"),
        "{query}"
    );
    assert!(
        query.contains("select distinct dn.nspname || '.' || dc.relname || '.' || da.attname"),
        "{query}"
    );
}

#[test]
fn build_rules_query_is_the_same_for_every_server_version() {
    // Nothing in the query is version-gated: pg_depend, pg_attribute,
    // pg_proc and pg_get_function_identity_arguments are identical on
    // PostgreSQL 14 through 19 (verified live on 14 and 19). Guard against a
    // later edit introducing a column that only newer servers have.
    let query = Dump::build_rules_query("('public')");
    for newer_only in ["prosqlbody", "attgenerated", "conenforced", "prexcept"] {
        assert!(!query.contains(newer_only), "{newer_only}: {query}");
    }
}

// ==== fix:serial tests ====

// ==== fix:comments tests ====

/// Code a script leaves outside comments and quoted tokens, lexed as
/// PostgreSQL and psql do: a `--` comment ends at `\n` or a bare `\r`, block
/// comments nest, `""` / `''` escape a quote. An unterminated comment or
/// token swallows the rest, as it would on the server.
fn clear_script_code(script: &str) -> String {
    let chars: Vec<char> = script.chars().collect();
    let mut code = String::with_capacity(script.len());
    let mut i = 0;
    while i < chars.len() {
        let pair = (chars[i], chars.get(i + 1).copied());
        if pair == ('-', Some('-')) {
            while i < chars.len() && chars[i] != '\n' && chars[i] != '\r' {
                i += 1;
            }
        } else if pair == ('/', Some('*')) {
            let mut depth = 0usize;
            while i < chars.len() {
                match (chars[i], chars.get(i + 1).copied()) {
                    ('/', Some('*')) => {
                        depth += 1;
                        i += 2;
                    }
                    ('*', Some('/')) => {
                        depth -= 1;
                        i += 2;
                        if depth == 0 {
                            break;
                        }
                    }
                    _ => i += 1,
                }
            }
            code.push(' ');
        } else if pair.0 == '"' || pair.0 == '\'' {
            let quote = pair.0;
            i += 1;
            while i < chars.len() {
                i += 1;
                if chars[i - 1] == quote {
                    if chars.get(i) == Some(&quote) {
                        i += 1;
                    } else {
                        break;
                    }
                }
            }
            code.push(' ');
        } else {
            code.push(chars[i]);
            i += 1;
        }
    }
    code
}

#[test]
fn clear_script_comments_never_let_a_hostile_name_run() {
    // Every header of the clear script names an object; PostgreSQL lets a
    // name hold a line break (ends a `--` comment), `*/` (ends a block
    // comment) or `/*` (opens a nested one that swallows the rest).
    for raw in [
        "lf\ndrop table pgc_victim; --",
        "cr\rdrop table pgc_victim; --",
        "blk*/ drop table pgc_victim; /*",
        "nest/*ed pgc_victim",
    ] {
        let q = |suffix: &str| format!("\"{raw}{suffix}\"");
        let schema = q(" schema");
        let mut dump = empty_dump();
        dump.configuration.database = raw.to_string();
        dump.configuration.scheme = format!("{raw}|public");
        dump.schemas.push(make_schema(&schema));
        dump.extensions.push(make_extension(&q(" ext")));
        dump.tables
            .push(make_table_with_fk(&schema, &q(" table"), &q(" fk")));
        dump.views.push(make_view(&schema, &q(" view")));
        dump.views
            .push(make_materialized_view(&schema, &q(" matview")));
        dump.foreign_tables.push(ForeignTable::new(
            schema.clone(),
            q(" foreign"),
            "srv".to_string(),
            "postgres".to_string(),
            Vec::new(),
            Vec::new(),
        ));
        dump.statistics.push(Statistic::new(
            schema.clone(),
            q(" stat"),
            "postgres".to_string(),
            schema.clone(),
            q(" table"),
            vec!["ndistinct".to_string()],
            vec!["a".to_string(), "b".to_string()],
            String::new(),
        ));
        let mut routine = make_routine(&schema, &q(" fn"));
        routine.kind = raw.to_string();
        dump.routines.push(routine);
        dump.sequences.push(make_sequence(&schema, &q(" seq")));
        dump.types.push(make_pg_type(&schema, &q(" type")));

        for use_comments in [true, false] {
            for use_cascade in [false, true] {
                let script = dump.generate_clear_script(true, use_comments, use_cascade);
                let code = clear_script_code(&script);
                assert!(
                    !code.contains("pgc_victim"),
                    "{raw:?} (use_comments={use_comments}): payload reached SQL:\n{code}\n---\n{script}"
                );
                // Nothing swallowed by an unterminated comment either.
                assert!(
                    code.trim_end().ends_with("commit;") && code.contains("drop table if exists"),
                    "{raw:?} (use_comments={use_comments}): script cut short:\n{code}\n---\n{script}"
                );
            }
        }
    }
}

#[test]
fn clear_script_header_is_unchanged_for_ordinary_names() {
    let mut dump = empty_dump();
    dump.tables.push(make_table("public", "users"));
    let script = dump.generate_clear_script(false, true, false);
    assert!(
        script.starts_with(
            "/*\n Script generated by PostgreSQL Comparer (clear command)\n\n Database: testdb\n Schema(s): public\n"
        ),
        "{script}"
    );
    assert!(
        script.contains("/* Drop table: public.users */\n"),
        "{script}"
    );
}

// A materialized view's schema, name and owner go into every statement pgc
// writes for it. pgc read them raw, where every other object is read through
// quote_ident: a view named `m as select 1; drop table t; --` ran its payload
// in CREATE, ALTER ... OWNER TO and DROP, and `s."MyMv"` came out as `s.MyMv`,
// which the server reads as `s.mymv`, so it never converged.
#[test]
fn build_materialized_views_query_quotes_names_like_regular_views() {
    let query = Dump::build_materialized_views_query("('public')");
    for quoted in [
        "quote_ident(mv.schemaname) as table_schema",
        "quote_ident(mv.matviewname) as table_name",
        "quote_ident(mv.matviewowner) as view_owner",
    ] {
        assert!(query.contains(quoted), "{quoted}: {query}");
    }
    let regular = Dump::build_regular_views_query("('public')");
    assert!(regular.contains("quote_ident(v.table_name) as table_name"));
    assert!(regular.contains("quote_ident(pv.viewowner) as view_owner"));
}

#[test]
fn quote_raw_identifier_quotes_what_quote_ident_quotes() {
    for bare in ["mymv", "_x", "a1_b2", ""] {
        assert_eq!(Dump::quote_raw_identifier(bare, None), None, "{bare:?}");
    }
    for (raw, quoted) in [
        ("MyMv", "\"MyMv\""),
        ("1st", "\"1st\""),
        ("a b", "\"a b\""),
        ("a$b", "\"a$b\""),
        ("user", "\"user\""),
        ("select", "\"select\""),
        ("m as select 1; --", "\"m as select 1; --\""),
        ("say \"hi\"", "\"say \"\"hi\"\"\""),
    ] {
        assert_eq!(
            Dump::quote_raw_identifier(raw, None).as_deref(),
            Some(quoted),
            "{raw:?}"
        );
    }
    // Already quoted (a hand-built dump): left alone.
    assert_eq!(Dump::quote_raw_identifier("\"MyMv\"", None), None);
}

#[test]
fn legacy_materialized_view_names_are_quoted_and_only_renamed_views_rehashed() {
    let mut legacy = empty_dump();
    let mut mixed = make_materialized_view("App", "MyMv");
    mixed.owner = "Owner".to_string();
    mixed.hash();
    let plain = make_materialized_view("app", "plain_mv");
    let mut regular = make_view("app", "Regular");
    regular.hash = Some("stored".to_string());
    let mut plain_stored = plain.clone();
    plain_stored.hash = Some("stored".to_string());
    legacy.views = vec![mixed, plain_stored, regular];

    legacy.quote_legacy_materialized_view_names();

    let mut current = make_materialized_view("\"App\"", "\"MyMv\"");
    current.owner = "\"Owner\"".to_string();
    current.hash();
    assert_eq!(legacy.views[0].schema, "\"App\"");
    assert_eq!(legacy.views[0].name, "\"MyMv\"");
    assert_eq!(legacy.views[0].owner, "\"Owner\"");
    assert_eq!(
        legacy.views[0].hash, current.hash,
        "a renamed view hashes as a current dump of it does"
    );
    // Nothing to quote: the stored hash stands. A regular view was always
    // stored quoted and is not touched.
    assert_eq!(legacy.views[1].hash.as_deref(), Some("stored"));
    assert_eq!(legacy.views[2].name, "Regular");
    assert_eq!(legacy.views[2].hash.as_deref(), Some("stored"));
}

/// A row trigger (`FOR EACH ROW` unless `statement`) named `name` on
/// `s1.<table>`, as `pg_get_triggerdef` prints it.
fn pre_columns_trigger(
    name: &str,
    table: &str,
    enabled: &str,
    comment: Option<&str>,
    statement: bool,
) -> crate::dump::table_trigger::TableTrigger {
    crate::dump::table_trigger::TableTrigger {
        oid: Oid(1),
        name: name.to_string(),
        definition: format!(
            "CREATE TRIGGER {name} BEFORE INSERT ON s1.{table} FOR EACH {} EXECUTE FUNCTION s1.trg()",
            if statement { "STATEMENT" } else { "ROW" }
        ),
        enabled: enabled.to_string(),
        comment: comment.map(str::to_string),
    }
}

/// `s1.<name>` with `triggers`, a partition of `parent` when given, and a
/// stored hash of `stored` (what pgc 1.0.x wrote into the file).
fn pre_columns_table(
    name: &str,
    parent: Option<&str>,
    triggers: Vec<crate::dump::table_trigger::TableTrigger>,
) -> Table {
    let mut table = make_table("s1", name);
    table.triggers = triggers;
    table.partition_of = parent.map(|p| format!("s1.{p}"));
    table.hash = Some("stored".to_string());
    table
}

#[test]
fn pre_columns_legacy_partition_trigger_clones_are_separated() {
    // pgc 1.0.x listed PostgreSQL 15+'s partition clones among the
    // partition's own triggers. A clone in its parent's state is left out,
    // one in a state of its own becomes a recorded clone; the parent's own
    // triggers and a partition's own trigger stay.
    let mut legacy = empty_dump();
    legacy.tables = vec![
        pre_columns_table(
            "pt",
            None,
            vec![
                pre_columns_trigger("row_trg", "pt", "O", None, false),
                pre_columns_trigger("stmt_trg", "pt", "O", None, true),
            ],
        ),
        pre_columns_table(
            "pt_1",
            Some("pt"),
            vec![
                pre_columns_trigger("row_trg", "pt_1", "O", None, false),
                // Named after the parent's statement trigger, which is not
                // cloned: the partition's own.
                pre_columns_trigger("stmt_trg", "pt_1", "O", None, false),
            ],
        ),
        pre_columns_table(
            "pt_2",
            Some("pt"),
            vec![pre_columns_trigger("row_trg", "pt_2", "D", None, false)],
        ),
        // A sub-partition follows its parent's clone (disabled here).
        pre_columns_table(
            "pt_2a",
            Some("pt_2"),
            vec![pre_columns_trigger(
                "row_trg",
                "pt_2a",
                "D",
                Some("c"),
                false,
            )],
        ),
        pre_columns_table(
            "pt_2b",
            Some("pt_2"),
            vec![pre_columns_trigger("row_trg", "pt_2b", "D", None, false)],
        ),
        pre_columns_table(
            "plain",
            None,
            vec![pre_columns_trigger("row_trg", "plain", "O", None, false)],
        ),
    ];
    legacy.separate_legacy_partition_trigger_clones();

    let table = |name: &str| legacy.tables.iter().find(|t| t.name == name).unwrap();
    let names = |triggers: &[crate::dump::table_trigger::TableTrigger]| -> Vec<String> {
        triggers
            .iter()
            .map(|t| format!("{}:{}", t.name, t.enabled))
            .collect()
    };
    assert_eq!(names(&table("pt").triggers), ["row_trg:O", "stmt_trg:O"]);
    assert_eq!(names(&table("pt_1").triggers), ["stmt_trg:O"]);
    assert!(table("pt_1").trigger_clones.is_empty());
    assert!(table("pt_2").triggers.is_empty());
    assert_eq!(names(&table("pt_2").trigger_clones), ["row_trg:D"]);
    assert_eq!(names(&table("pt_2a").trigger_clones), ["row_trg:D"]);
    assert_eq!(
        table("pt_2a").trigger_clones[0].comment.as_deref(),
        Some("c")
    );
    assert!(table("pt_2b").triggers.is_empty() && table("pt_2b").trigger_clones.is_empty());

    // Changed tables are rehashed to what a current dump of them gives;
    // the others keep the hash stored in the file.
    for name in ["pt_1", "pt_2", "pt_2a", "pt_2b"] {
        let mut fresh = table(name).clone();
        fresh.hash();
        assert_eq!(table(name).hash, fresh.hash, "{name}");
        assert_ne!(table(name).hash.as_deref(), Some("stored"), "{name}");
    }
    for name in ["pt", "plain"] {
        assert_eq!(table(name).hash.as_deref(), Some("stored"), "{name}");
    }
}

#[test]
fn pre_columns_legacy_dump_without_clones_is_untouched() {
    // A dump of PostgreSQL 14 (clones internal, never listed).
    let mut legacy = empty_dump();
    legacy.tables = vec![
        pre_columns_table(
            "pt",
            None,
            vec![pre_columns_trigger("row_trg", "pt", "O", None, false)],
        ),
        pre_columns_table("pt_1", Some("pt"), vec![]),
    ];
    let before = serde_json::to_string(&legacy.tables).unwrap();
    legacy.separate_legacy_partition_trigger_clones();
    assert_eq!(serde_json::to_string(&legacy.tables).unwrap(), before);
}

// ==== pre-clear tests ====

use crate::dump::drop_order::DropDependency;
use crate::dump::pg_type::DomainConstraint;

fn from_json<T: serde::de::DeserializeOwned>(value: serde_json::Value) -> T {
    serde_json::from_value(value).expect("fixture JSON")
}

fn clear_domain(schema: &str, name: &str, base: &str, checks: &[&str]) -> PgType {
    let mut domain = make_pg_type(schema, name);
    domain.typtype = b'd' as i8;
    domain.formatted_basetype = Some(base.to_string());
    domain.domain_constraints = checks
        .iter()
        .enumerate()
        .map(|(i, check)| DomainConstraint {
            name: format!("{name}_check{i}"),
            definition: check.to_string(),
        })
        .collect();
    domain
}

fn clear_aggregate(schema: &str, name: &str, sfunc: &str, finalfunc: Option<&str>) -> Routine {
    let mut aggregate = make_routine(schema, name);
    aggregate.kind = "aggregate".to_string();
    aggregate.arguments = "integer".to_string();
    aggregate.aggregate_info = Some(crate::dump::routine::AggregateInfo {
        sfunc: sfunc.to_string(),
        stype: "bigint".to_string(),
        sspace: None,
        finalfunc: finalfunc.map(str::to_string),
        finalfunc_extra: false,
        finalfunc_modify: None,
        combinefunc: None,
        serialfunc: None,
        deserialfunc: None,
        initcond: None,
        msfunc: None,
        minvfunc: None,
        mstype: None,
        msspace: None,
        mfinalfunc: None,
        mfinalfunc_extra: false,
        mfinalfunc_modify: None,
        minitcond: None,
        sortop: None,
        kind: 'n',
        num_direct_args: 0,
    });
    aggregate.hash();
    aggregate
}

/// A plain `integer` column of `schema.table` whose default is `default`.
fn clear_column_with_default(
    schema: &str,
    table: &str,
    name: &str,
    default: &str,
) -> crate::dump::table_column::TableColumn {
    from_json(serde_json::json!({
        "catalog": "postgres", "schema": schema, "table": table, "name": name,
        "ordinal_position": 1, "column_default": default, "is_nullable": true,
        "data_type": "integer", "is_self_referencing": false, "is_identity": false,
        "identity_cycle": false, "is_generated": "NEVER", "is_updatable": true
    }))
}

fn dependency(before: &str, after: &str) -> DropDependency {
    DropDependency::new(before, after)
}

#[test]
fn clear_drops_an_aggregate_before_its_plain_state_and_final_functions() {
    // The usual creation order lists the plpgsql state function first; only a
    // SQL-standard one used to be ordered, so `drop function` ran first and
    // failed ("function app.my_agg(integer) depends on function …").
    let mut dump = clear_dump();
    dump.routines.push(make_routine("app", "my_sfunc"));
    dump.routines.push(make_routine("app", "my_final"));
    dump.routines.push(clear_aggregate(
        "app",
        "my_agg",
        "app.my_sfunc",
        Some("my_final"),
    ));

    let script = dump.generate_clear_script(true, true, false);
    let aggregate =
        clear_script_position(&script, "drop aggregate if exists app.my_agg (integer);");
    assert!(
        aggregate < clear_script_position(&script, "drop function if exists app.my_sfunc ();"),
        "{script}"
    );
    assert!(
        aggregate < clear_script_position(&script, "drop function if exists app.my_final ();"),
        "{script}"
    );
    // The two plain functions keep their dump order.
    assert!(
        clear_script_position(&script, "drop function if exists app.my_sfunc ();")
            < clear_script_position(&script, "drop function if exists app.my_final ();")
    );
}

#[test]
fn clear_follows_the_dependencies_read_from_pg_depend() {
    let mut dump = clear_dump();
    dump.routines.push(make_routine("app", "is_sku"));
    dump.types.push(clear_domain(
        "app",
        "sku",
        "text",
        &["CHECK (app.is_sku(VALUE))"],
    ));
    dump.types
        .push(clear_domain("app", "base_txt", "text", &[]));
    dump.types
        .push(clear_domain("app", "code", "app.base_txt", &[]));
    dump.tables.push(make_table("app", "a_parent"));
    dump.tables.push(make_table("app", "b_child"));
    dump.tables.push(make_table("app", "reader"));
    dump.views.push(make_view("app", "tag_view"));
    dump.drop_dependencies = vec![
        dependency("type:app.sku", "proc:app.is_sku()"),
        dependency("type:app.code", "type:app.base_txt"),
        dependency("rel:app.b_child", "rel:app.a_parent"),
        dependency("rel:app.reader", "rel:app.tag_view"),
        // Keys the script does not drop are ignored.
        dependency("rel:other.t", "rel:app.a_parent"),
    ];

    let script = dump.generate_clear_script(true, true, false);
    let pos = |needle: &str| clear_script_position(&script, needle);
    assert!(
        pos("drop type if exists app.sku;") < pos("drop function if exists app.is_sku ();"),
        "{script}"
    );
    assert!(
        pos("drop type if exists app.code;") < pos("drop type if exists app.base_txt;"),
        "{script}"
    );
    assert!(
        pos("drop table if exists app.b_child;") < pos("drop table if exists app.a_parent;"),
        "{script}"
    );
    assert!(
        pos("drop table if exists app.reader;") < pos("drop view if exists app.tag_view;"),
        "{script}"
    );
    assert!(pos("drop view if exists app.tag_view;") < pos("drop schema if exists app;"));
}

#[test]
fn clear_repeats_a_section_banner_where_a_dependency_moves_a_drop() {
    let mut dump = clear_dump();
    dump.routines.push(make_routine("app", "is_sku"));
    dump.routines.push(make_routine("app", "other"));
    dump.types.push(clear_domain(
        "app",
        "sku",
        "text",
        &["CHECK (app.is_sku(VALUE))"],
    ));
    dump.drop_dependencies = vec![dependency("type:app.sku", "proc:app.is_sku()")];

    let script = dump.generate_clear_script(false, true, false);
    let routines = "/* ---> Drop Routines --------------- */";
    assert_eq!(script.matches(routines).count(), 2, "{script}");
    let types = clear_script_position(&script, "/* ---> Drop Types --------------- */");
    assert!(
        clear_script_position(&script, "drop function if exists app.other ();") < types,
        "{script}"
    );
    assert!(
        types < clear_script_position(&script, "drop function if exists app.is_sku ();"),
        "{script}"
    );
    assert!(script.rfind(routines).unwrap() > types, "{script}");
}

/// Every kind of object in one dump, with `pg_depend` edges that agree with
/// the section order.
fn clear_dump_of_every_section() -> Dump {
    let mut dump = clear_dump();
    dump.extensions.push(make_extension("pg_trgm"));
    dump.views
        .push(make_view_with_deps("app", "v", vec!["app.t"]));
    dump.tables.push(make_table_with_fk("app", "t", "t_fk"));
    dump.statistics.push(from_json(serde_json::json!({
        "schema": "app", "name": "st", "owner": "postgres", "table_schema": "app",
        "table_name": "t", "kinds": ["d"], "columns": ["a", "b"],
        "definition": "CREATE STATISTICS app.st (dependencies) ON a, b FROM app.t"
    })));
    dump.routines.push(make_routine("app", "f"));
    dump.sequences.push(make_sequence("app", "s"));
    dump.types.push(make_pg_type("app", "pair"));
    dump.drop_dependencies = vec![
        dependency("rel:app.v", "rel:app.t"),
        dependency("fk:app.t.t_fk", "rel:app.t"),
        dependency("rel:app.t", "proc:app.f()"),
        dependency("rel:app.t", "rel:app.s"),
        dependency("stx:app.st", "proc:app.f()"),
        dependency("proc:app.f()", "type:app.pair"),
        dependency("proc:app.f()", "ext:pg_trgm"),
        dependency("type:app.pair", "nsp:app"),
        dependency("ext:pg_trgm", "nsp:app"),
    ];
    dump
}

#[test]
fn clear_keeps_the_section_order_when_every_dependency_agrees_with_it() {
    let with = clear_dump_of_every_section();
    let mut without = with.clone();
    without.drop_dependencies.clear();
    assert_eq!(
        with.generate_clear_script(true, true, false),
        without.generate_clear_script(true, true, false)
    );
    assert_eq!(
        with.generate_clear_script(false, false, true),
        without.generate_clear_script(false, false, true)
    );
}

fn clear_fdw(name: &str) -> crate::dump::fdw::ForeignDataWrapper {
    from_json(serde_json::json!({
        "name": name, "owner": "postgres", "handler_func": "public.postgres_fdw_handler",
        "validator_func": null, "options": [], "comment": null
    }))
}

fn clear_server(name: &str, fdw: &str) -> crate::dump::fdw::ForeignServer {
    from_json(serde_json::json!({
        "name": name, "owner": "postgres", "fdw_name": fdw, "server_type": null,
        "server_version": null, "options": [], "comment": null
    }))
}

fn clear_event_trigger(name: &str, function: &str) -> crate::dump::event_trigger::EventTrigger {
    crate::dump::event_trigger::EventTrigger::new(
        name.to_string(),
        "ddl_command_start".to_string(),
        function.to_string(),
        Vec::new(),
        "O".to_string(),
        "postgres".to_string(),
        None,
    )
}

/// A dump holding database-wide objects: `w_ext` uses a handler of the
/// extension `postgres_fdw`, installed in the selected schema `app`, `s1`
/// sits on it, with a user mapping and a subscription; `w_other` / `s2` /
/// `et2` depend on nothing the script drops.
fn clear_dump_with_database_wide_objects() -> Dump {
    let mut dump = clear_dump();
    dump.extensions.push(Extension::new(
        "postgres_fdw".to_string(),
        "1.1".to_string(),
        "app".to_string(),
    ));
    dump.routines.push(make_routine("app", "f"));
    dump.foreign_data_wrappers.push(clear_fdw("w_ext"));
    dump.foreign_data_wrappers.push(clear_fdw("w_other"));
    dump.foreign_servers.push(clear_server("s1", "w_ext"));
    dump.foreign_servers.push(clear_server("s2", "w_other"));
    dump.user_mappings.push(from_json(serde_json::json!({
        "server_name": "s1", "username": "PUBLIC", "options": []
    })));
    dump.subscriptions.push(from_json(serde_json::json!({
        "name": "sub1", "owner": "postgres", "connection": "", "server": "s1",
        "publications": ["p"], "enabled": false
    })));
    dump.event_triggers
        .push(clear_event_trigger("et1", "app.f"));
    dump.event_triggers
        .push(clear_event_trigger("et2", "other.g"));
    dump.drop_dependencies = vec![
        dependency("fdw:w_ext", "ext:postgres_fdw"),
        dependency("srv:s1", "fdw:w_ext"),
        dependency("srv:s2", "fdw:w_other"),
        dependency("um:PUBLIC SERVER s1", "srv:s1"),
        dependency("sub:sub1", "srv:s1"),
        dependency("evt:et1", "proc:app.f()"),
    ];
    dump
}

#[test]
fn clear_drops_database_wide_objects_only_when_a_dropped_object_needs_it() {
    let dump = clear_dump_with_database_wide_objects();
    let script = dump.generate_clear_script(true, true, false);
    let pos = |needle: &str| clear_script_position(&script, needle);

    let sub = pos("drop subscription if exists sub1;");
    let mapping = pos("drop user mapping if exists for PUBLIC server s1;");
    let server = pos("drop server if exists s1;");
    let fdw = pos("drop foreign data wrapper if exists w_ext;");
    let extension = pos("drop extension if exists postgres_fdw;");
    assert!(sub < server && mapping < server, "{script}");
    assert!(server < fdw && fdw < extension, "{script}");
    assert!(
        pos("drop event trigger if exists et1;") < pos("drop function if exists app.f ();"),
        "{script}"
    );
    for untouched in ["w_other", "s2", "et2"] {
        assert!(
            !script.contains(untouched),
            "{untouched} dropped:\n{script}"
        );
    }
    assert!(script.contains("/* Drop subscription: sub1 */"), "{script}");
    assert!(
        script.contains("/* ---> Drop Foreign-Data Wrappers --------------- */"),
        "{script}"
    );

    // Without dependency data none of them is dropped.
    let mut hand_built = dump.clone();
    hand_built.drop_dependencies.clear();
    let script = hand_built.generate_clear_script(true, true, false);
    for untouched in [
        "subscription",
        "user mapping",
        "drop server",
        "foreign data wrapper",
        "event trigger",
    ] {
        assert!(!script.contains(untouched), "{untouched}:\n{script}");
    }
}

#[test]
fn clear_leaves_database_wide_objects_on_an_unselected_public_extension_alone() {
    // The extensions query reads every extension in `public`, selected or
    // not. `--scheme app` used to drop every server and user mapping (with
    // its credentials) and every PostgreSQL 19 SERVER subscription built on
    // such an extension, database-wide, without --use-cascade.
    let mut dump = clear_dump_with_database_wide_objects();
    dump.extensions = vec![make_extension("postgres_fdw")];
    let script = dump.generate_clear_script(true, true, false);
    for untouched in [
        "drop subscription",
        "drop user mapping",
        "drop server",
        "drop foreign data wrapper",
    ] {
        assert!(!script.contains(untouched), "{untouched}:\n{script}");
    }
    // The extension itself is dropped as before (and fails on its wrapper);
    // an event trigger on a selected routine still goes.
    assert!(
        script.contains("drop extension if exists postgres_fdw;"),
        "{script}"
    );
    assert!(
        script.contains("drop event trigger if exists et1;"),
        "{script}"
    );

    // With `public` selected the extension is inside the selection again.
    dump.schemas.push(Schema::new(
        "public".to_string(),
        "public".to_string(),
        None,
    ));
    let script = dump.generate_clear_script(true, true, false);
    for dropped in [
        "drop subscription if exists sub1;",
        "drop user mapping if exists for PUBLIC server s1;",
        "drop server if exists s1;",
        "drop foreign data wrapper if exists w_ext;",
    ] {
        assert!(script.contains(dropped), "{dropped}:\n{script}");
    }
}

#[test]
fn clear_leaves_a_server_used_outside_the_selected_schemas_in_place() {
    // A foreign table in another schema uses `s1`: `drop server` can only
    // fail, and the user mapping dropped ahead of it used to be lost.
    let mut dump = clear_dump_with_database_wide_objects();
    dump.drop_dependencies
        .push(dependency(crate::dump::drop_order::OUTSIDE_KEY, "srv:s1"));
    let script = dump.generate_clear_script(true, true, false);
    for untouched in [
        "drop subscription",
        "drop user mapping",
        "drop server",
        // …nor the wrapper under the server, whose drop would fail too.
        "drop foreign data wrapper",
    ] {
        assert!(!script.contains(untouched), "{untouched}:\n{script}");
    }
    assert!(
        script.contains("drop extension if exists postgres_fdw;"),
        "{script}"
    );
    assert!(
        script.contains("drop event trigger if exists et1;"),
        "{script}"
    );

    // A dependent the script does not drop blocks the same way: here a user
    // mapping missing from the dump.
    let mut dump = clear_dump_with_database_wide_objects();
    dump.user_mappings.clear();
    let script = dump.generate_clear_script(true, true, false);
    assert!(!script.contains("drop server"), "{script}");
    assert!(!script.contains("drop foreign data wrapper"), "{script}");
    assert!(!script.contains("drop subscription"), "{script}");
}

#[test]
fn clear_drops_event_triggers_before_anything_that_fires_them() {
    // A DDL-audit trigger inserting into a table of the cleared schema made
    // `drop table` of that table fail (the trigger ran after it and inserted
    // into the table just dropped), and every later drop with it.
    let mut dump = clear_dump();
    dump.views
        .push(make_view_with_deps("app", "v", vec!["app.ddl_log"]));
    dump.tables.push(make_table("app", "ddl_log"));
    dump.routines.push(make_routine("app", "log_ddl"));
    dump.types.push(make_pg_type("app", "pair"));
    dump.event_triggers
        .push(clear_event_trigger("log_ddl", "app.log_ddl"));
    dump.drop_dependencies = vec![
        dependency("rel:app.v", "rel:app.ddl_log"),
        dependency("evt:log_ddl", "proc:app.log_ddl()"),
    ];
    let script = dump.generate_clear_script(true, true, false);
    let evt = clear_script_position(&script, "drop event trigger if exists log_ddl;");
    let first_other = [
        "drop view",
        "drop table",
        "drop function",
        "drop type",
        "drop schema",
    ]
    .iter()
    .map(|needle| clear_script_position(&script, needle))
    .min()
    .unwrap();
    assert!(evt < first_other, "{script}");
    assert!(
        clear_script_position(&script, "/* ---> Drop Event Triggers --------------- */")
            < clear_script_position(&script, "/* ---> Drop Views --------------- */"),
        "{script}"
    );
}

#[test]
fn clear_cascade_reaches_every_statement_but_a_user_mapping() {
    let dump = clear_dump_with_database_wide_objects();
    let script = dump.generate_clear_script(false, false, true);
    for line in script.lines().filter(|l| l.starts_with("drop ")) {
        if line.starts_with("drop user mapping") {
            assert_eq!(line, "drop user mapping if exists for PUBLIC server s1;");
        } else {
            assert!(line.ends_with(" cascade;"), "{line}");
        }
    }
}

#[test]
fn clear_drops_operators_casts_collations_and_text_search_objects() {
    let mut dump = clear_dump();
    dump.routines.push(make_routine("app", "f"));
    dump.types.push(make_pg_type("app", "pair"));
    dump.operators.push(from_json(serde_json::json!({
        "schema": "app", "name": "===", "owner": "postgres", "left_type": "integer",
        "right_type": "integer", "result_type": "boolean", "procedure": "app.f",
        "is_hashes": false, "is_merges": false
    })));
    dump.operators.push(from_json(serde_json::json!({
        "schema": "app", "name": "!!", "owner": "postgres", "left_type": null,
        "right_type": "integer", "result_type": "boolean", "procedure": "app.f",
        "is_hashes": false, "is_merges": false
    })));
    dump.casts.push(from_json(serde_json::json!({
        "source_type": "app.pair", "target_type": "text", "cast_method": "f",
        "function_name": "app.f()", "cast_context": "e"
    })));
    dump.collations.push(from_json(serde_json::json!({
        "schema": "app", "name": "c", "owner": "postgres", "provider": "c",
        "deterministic": true
    })));
    dump.ts_configs.push(from_json(serde_json::json!({
        "schema": "app", "name": "cfg", "owner": "postgres", "parser": "pg_catalog.default",
        "mappings": []
    })));
    dump.ts_dicts.push(from_json(serde_json::json!({
        "schema": "app", "name": "dict", "owner": "postgres", "template": "pg_catalog.simple",
        "options": []
    })));

    let script = dump.generate_clear_script(true, true, false);
    let pos = |needle: &str| clear_script_position(&script, needle);
    let routine = pos("drop function if exists app.f ();");
    let cast = pos("drop cast if exists (app.pair as text);");
    let operator = pos("drop operator if exists app.=== (integer, integer);");
    let prefix = pos("drop operator if exists app.!! (NONE, integer);");
    let pair = pos("drop type if exists app.pair;");
    let cfg = pos("drop text search configuration if exists app.cfg;");
    let dict = pos("drop text search dictionary if exists app.dict;");
    let collation = pos("drop collation if exists app.c;");
    let schema = pos("drop schema if exists app;");
    assert!(
        cast < operator && operator < prefix && prefix < routine,
        "{script}"
    );
    assert!(
        routine < pair && pair < cfg && cfg < dict && dict < collation,
        "{script}"
    );
    assert!(collation < schema, "{script}");
    for banner in [
        "/* ---> Drop Casts --------------- */",
        "/* ---> Drop Operators --------------- */",
        "/* ---> Drop Text Search Configurations --------------- */",
        "/* ---> Drop Text Search Dictionaries --------------- */",
        "/* ---> Drop Collations --------------- */",
    ] {
        assert!(script.contains(banner), "{banner}:\n{script}");
    }
    assert!(script.contains("/* Drop operator: app.=== */"), "{script}");
}

#[test]
fn clear_breaks_a_dependency_cycle_through_a_droppable_table_part() {
    // t's CHECK calls max_x(), whose SQL-standard body reads t: neither can be
    // dropped first, so the constraint goes ahead of both.
    let mut dump = clear_dump();
    dump.tables.push(make_table("app", "t"));
    dump.routines
        .push(sql_body_routine("app", "max_x", &["app.t"], &[]));
    let breaker = "alter table if exists app.t drop constraint if exists t_chk";
    dump.drop_dependencies = vec![
        dependency("rel:app.t", "proc:app.max_x()").with_breaker(breaker),
        dependency("proc:app.max_x()", "rel:app.t"),
    ];

    let script = dump.generate_clear_script(true, true, false);
    let pos = |needle: &str| clear_script_position(&script, needle);
    let broken = pos(&format!("{breaker};"));
    let function = pos("drop function if exists app.max_x ();");
    let table = pos("drop table if exists app.t;");
    assert!(broken < function && function < table, "{script}");
    assert!(
        script.contains(
            "/* Break dependency cycle: alter table if exists app.t drop constraint if exists t_chk */"
        ),
        "{script}"
    );
    assert_eq!(
        script.matches(breaker).count(),
        2,
        "comment + statement:\n{script}"
    );
}

#[test]
fn clear_uses_a_breaker_only_on_a_cycle() {
    // The same edge without the cycle orders the drops, and its breaker is
    // never run.
    let mut dump = clear_dump();
    dump.tables.push(make_table("app", "t"));
    dump.routines.push(make_routine("app", "f"));
    let breaker = "alter table if exists app.t drop constraint if exists t_chk";
    dump.drop_dependencies = vec![dependency("rel:app.t", "proc:app.f()").with_breaker(breaker)];
    let script = dump.generate_clear_script(true, true, false);
    assert!(!script.contains(breaker), "{script}");
}

#[test]
fn clear_keeps_the_default_order_of_a_cycle_it_cannot_break() {
    let mut dump = clear_dump();
    dump.tables.push(make_table("app", "t"));
    dump.routines
        .push(sql_body_routine("app", "max_x", &["app.t"], &[]));
    dump.drop_dependencies = vec![
        dependency("rel:app.t", "proc:app.max_x()"),
        dependency("proc:app.max_x()", "rel:app.t"),
    ];
    let script = dump.generate_clear_script(true, true, false);
    assert_eq!(script.matches("drop table if exists app.t;").count(), 1);
    assert_eq!(
        script
            .matches("drop function if exists app.max_x ();")
            .count(),
        1
    );
    // Dropped with the views, as without dependency data.
    assert!(
        clear_script_position(&script, "drop function if exists app.max_x ();")
            < clear_script_position(&script, "drop table if exists app.t;"),
        "{script}"
    );
}

#[test]
fn clear_infers_inheritance_domain_and_default_dependencies_without_pg_depend() {
    let mut dump = clear_dump();
    dump.tables.push(make_table("app", "a_parent"));
    let mut child = make_table("app", "b_child");
    child.inherits_from = vec!["app.a_parent".to_string()];
    dump.tables.push(child);
    dump.types
        .push(clear_domain("app", "base_txt", "text", &[]));
    dump.types
        .push(clear_domain("app", "code", "app.base_txt[]", &[]));
    dump.routines.push(make_routine("app", "is_sku"));
    dump.types.push(clear_domain(
        "app",
        "sku",
        "text",
        &["CHECK (app.is_sku(VALUE))"],
    ));
    // orders' default calls max_qty(), which reads limits: orders, then
    // max_qty(), then limits.
    let mut orders = make_table("app", "orders");
    orders.columns.push(clear_column_with_default(
        "app",
        "orders",
        "qty",
        "app.max_qty()",
    ));
    dump.tables.push(orders);
    dump.tables.push(make_table("app", "limits"));
    dump.routines
        .push(sql_body_routine("app", "max_qty", &["app.limits"], &[]));

    let script = dump.generate_clear_script(true, true, false);
    let pos = |needle: &str| clear_script_position(&script, needle);
    assert!(
        pos("drop table if exists app.b_child;") < pos("drop table if exists app.a_parent;"),
        "{script}"
    );
    assert!(
        pos("drop type if exists app.code;") < pos("drop type if exists app.base_txt;"),
        "{script}"
    );
    assert!(
        pos("drop type if exists app.sku;") < pos("drop function if exists app.is_sku ();"),
        "{script}"
    );
    let orders = pos("drop table if exists app.orders;");
    let max_qty = pos("drop function if exists app.max_qty ();");
    assert!(
        orders < max_qty && max_qty < pos("drop table if exists app.limits;"),
        "{script}"
    );
}

#[test]
fn clear_keeps_a_sql_standard_routine_reading_only_undumped_relations_after_the_tables() {
    // table_count() reads information_schema.tables, which the script does not
    // drop, and snap's default calls it: dropped with the views it blocked
    // `drop function` (pgc 1.0.27 got this right).
    let mut dump = clear_dump();
    let mut snap = make_table("app", "snap");
    snap.columns.push(clear_column_with_default(
        "app",
        "snap",
        "n",
        "app.table_count()",
    ));
    dump.tables.push(snap);
    dump.routines.push(sql_body_routine(
        "app",
        "table_count",
        &["information_schema.tables"],
        &[],
    ));

    let script = dump.generate_clear_script(true, true, false);
    assert!(
        clear_script_position(&script, "drop table if exists app.snap;")
            < clear_script_position(&script, "drop function if exists app.table_count ();"),
        "{script}"
    );
    assert!(!script.contains("routines reading relations"), "{script}");
}

#[test]
fn a_dump_does_not_write_drop_dependencies() {
    let dump = clear_dump_of_every_section();
    let json = serde_json::to_value(&dump).unwrap();
    assert!(json.get("drop_dependencies").is_some());
    let mut plain = dump;
    plain.drop_dependencies.clear();
    let json = serde_json::to_value(&plain).unwrap();
    assert!(json.get("drop_dependencies").is_none(), "{json}");
}

// ==== r3-ddl tests ====

/// `quote_ident` quotes the column-name and type/function-name keywords as
/// well as the reserved ones, so a 1.0.x materialized view named after one
/// (`left`, `int`, `position`) is quoted on load like a current dump stores
/// it; an unreserved keyword stays bare, as it does on the server.
#[test]
fn quote_raw_identifier_quotes_column_name_and_type_func_name_keywords() {
    for keyword in [
        "left",
        "right",
        "join",
        "int",
        "integer",
        "position",
        "time",
        "timestamp",
        "between",
        "values",
        "row",
        "overlaps",
        "collation",
        "xmlelement",
        "concurrently",
    ] {
        assert_eq!(
            Dump::quote_raw_identifier(keyword, None).as_deref(),
            Some(format!("\"{keyword}\"").as_str()),
            "{keyword}"
        );
    }
    for unreserved in ["name", "data", "version", "text", "action", "key"] {
        assert_eq!(
            Dump::quote_raw_identifier(unreserved, None),
            None,
            "{unreserved}"
        );
    }
}

/// The keywords PostgreSQL 16 and 17 added are quoted only on the version
/// that quotes them: a pgc 1.0.x dump does not say its server's version, so
/// on load (`None`) they stay bare, as PostgreSQL 14 and 15 store them; a
/// current dump of such a server stores `json_object`, and quoting it on
/// load dropped and recreated the view (review of r3-ddl).
#[test]
fn quote_raw_identifier_quotes_keywords_of_16_and_17_by_version() {
    for word in [
        "json_object",
        "json_array",
        "system_user",
        "json",
        "json_table",
        "merge_action",
    ] {
        assert_eq!(Dump::quote_raw_identifier(word, None), None, "{word}");
        assert_eq!(
            Dump::quote_raw_identifier(word, Some(150000)),
            None,
            "{word}"
        );
        assert_eq!(
            Dump::quote_raw_identifier(word, Some(170000)).as_deref(),
            Some(format!("\"{word}\"").as_str()),
            "{word}"
        );
    }
    assert_eq!(
        Dump::quote_raw_identifier("json_object", Some(160000)).as_deref(),
        Some("\"json_object\"")
    );
    assert_eq!(Dump::quote_raw_identifier("json", Some(160000)), None);
    assert_eq!(
        Dump::quote_raw_identifier("merge_action", Some(160000)),
        None
    );
}

/// On load a 1.0.x dump's `json_object` stays bare (PostgreSQL 14's
/// spelling, which a current dump of 14 or 15 stores), and a stored hash is
/// kept; once the comparer knows the other dump's server is 17, it quotes it
/// the way that server does, and rehashes the view.
#[test]
fn legacy_materialized_views_named_after_newer_keywords_follow_the_version() {
    let mut legacy = empty_dump();
    let mut stored = make_materialized_view("app", "json_object");
    stored.hash = Some("stored".to_string());
    let mut plain = make_materialized_view("app", "plain_mv");
    plain.hash = Some("stored".to_string());
    legacy.views = vec![stored, plain, make_materialized_view("app", "merge_action")];
    legacy.quote_legacy_materialized_view_names();
    assert_eq!(legacy.views[0].name, "json_object");
    assert_eq!(legacy.views[0].hash.as_deref(), Some("stored"));
    assert_eq!(legacy.views[2].name, "merge_action");

    // PostgreSQL 16 quotes json_object, not merge_action.
    let mut on_16 = legacy.clone();
    on_16.requote_legacy_materialized_view_names(160000);
    assert_eq!(on_16.views[0].name, "\"json_object\"");
    assert_eq!(on_16.views[2].name, "merge_action");

    legacy.requote_legacy_materialized_view_names(170000);
    for (view, name) in legacy
        .views
        .iter()
        .zip(["\"json_object\"", "plain_mv", "\"merge_action\""])
    {
        assert_eq!(view.name, name);
    }
    let mut current = make_materialized_view("app", "\"json_object\"");
    current.hash();
    assert_eq!(legacy.views[0].hash, current.hash);
    // Nothing to quote: the stored hash stands.
    assert_eq!(legacy.views[1].hash.as_deref(), Some("stored"));

    // A dump that records its server version is never touched.
    let mut modern = empty_dump();
    modern.server_version_num = Some(150000);
    modern.views = vec![make_materialized_view("app", "json_object")];
    modern.requote_legacy_materialized_view_names(170000);
    assert_eq!(modern.views[0].name, "json_object");
}

#[test]
fn legacy_materialized_views_named_after_keywords_hash_as_a_current_dump() {
    let mut legacy = empty_dump();
    legacy.views = vec![
        make_materialized_view("app", "left"),
        make_materialized_view("app", "position"),
    ];
    legacy.quote_legacy_materialized_view_names();
    for (view, name) in legacy.views.iter().zip(["\"left\"", "\"position\""]) {
        let mut current = make_materialized_view("app", name);
        current.hash();
        assert_eq!(view.name, name);
        assert_eq!(view.hash, current.hash, "{name}");
    }
}

/// A `text` column of `schema.table`, with `collation` (`schema`, `name`)
/// when given.
fn r3_ddl_column(
    schema: &str,
    table: &str,
    collation: Option<(&str, &str)>,
) -> crate::dump::table_column::TableColumn {
    let mut column = clear_column_with_default(schema, table, "label", "NULL");
    column.column_default = None;
    column.data_type = "text".to_string();
    if let Some((coll_schema, coll_name)) = collation {
        column.collation_catalog = Some("postgres".to_string());
        column.collation_schema = Some(coll_schema.to_string());
        column.collation_name = Some(coll_name.to_string());
    }
    column
}

/// A 1.0.x dump stored a table hash that leaves a column's collation out. A
/// table with a collated column is rehashed on load, so it matches a current
/// dump of the same table; any other table keeps the hash in the file.
#[test]
fn legacy_tables_with_a_collated_column_are_rehashed() {
    let mut collated = make_table("app", "t");
    collated
        .columns
        .push(r3_ddl_column("app", "t", Some(("pg_catalog", "C"))));
    collated.hash();
    let current_hash = collated.hash.clone();
    collated.hash = Some("stored-by-1.0".to_string());
    let mut plain = make_table("app", "u");
    plain.columns.push(r3_ddl_column("app", "u", None));
    plain.hash = Some("stored-by-1.0".to_string());

    let mut legacy = empty_dump();
    legacy.tables = vec![collated, plain];
    legacy.rehash_legacy_collated_tables();

    assert_eq!(legacy.tables[0].hash, current_hash);
    assert_eq!(legacy.tables[1].hash.as_deref(), Some("stored-by-1.0"));
}

// ---- r3:types — multirange schema and composite type usage ----

#[test]
fn multirange_columns_read_the_multirange_schema_separately() {
    let (columns, join) = Dump::multirange_columns(true);
    assert!(columns.contains("quote_ident(mt.typname) as multirange_name"));
    assert!(columns.contains("quote_ident(mtn.nspname) as multirange_schema"));
    assert!(join.contains("left join pg_type mt on mt.oid = r.rngmultitypid"));
    assert!(join.contains("left join pg_namespace mtn on mtn.oid = mt.typnamespace"));

    let query = Dump::build_range_types_query("('public')", columns, join);
    assert!(query.contains("as multirange_schema"), "{query}");
    assert!(query.contains("mtn.oid = mt.typnamespace"), "{query}");
}

#[test]
fn multirange_columns_without_rngmultitypid_select_nulls() {
    let (columns, join) = Dump::multirange_columns(false);
    assert!(columns.contains("null::text as multirange_name"));
    assert!(columns.contains("null::text as multirange_schema"));
    assert!(!columns.contains("rngmultitypid"));
    assert_eq!(join, "");
}

#[test]
fn build_composite_usage_query_follows_what_postgres_follows() {
    let query = Dump::build_composite_usage_query("('app')");
    // Only composite types (not a table's row type) in the dumped schemas.
    assert!(query.contains("n.nspname in ('app')"), "{query}");
    assert!(query.contains("t.typtype = 'c'"), "{query}");
    assert!(query.contains("c.relkind = 'c'"), "{query}");
    // Arrays and domains over it, and the row types of relations that do
    // not store it themselves: composite types, views, foreign tables.
    assert!(query.contains("select x.typelem, x.oid"), "{query}");
    assert!(
        query.contains("x.typcategory = 'A' and x.typelem <> 0"),
        "{query}"
    );
    assert!(query.contains("select x.typbasetype, x.oid"), "{query}");
    assert!(query.contains("where x.typtype = 'd'"), "{query}");
    assert!(query.contains("r.relkind in ('c', 'v', 'f')"), "{query}");
    // A single recursive reference: PostgreSQL rejects two.
    assert_eq!(query.matches("from usage u").count(), 2, "{query}");
    assert!(
        query.contains("join contained_in ci on ci.used_oid = u.used_oid"),
        "{query}"
    );
    // Stored in tables, partitioned tables and materialized views.
    assert!(query.contains("r.relkind in ('r', 'p', 'm')"), "{query}");
    // Typed tables, in any schema.
    assert!(query.contains("where tt.reloftype <> 0"), "{query}");
    // Set-wise: no subquery run once per composite type or per closure
    // row, each of which seq-scanned pg_attribute (26 s for 2000 types).
    for per_row in ["lateral", "array("] {
        assert!(!query.contains(per_row), "{per_row}: {query}");
    }
    assert_eq!(query.matches("group by type_oid").count(), 2, "{query}");
    assert!(
        query.contains("coalesce(s.stored_in, '{}'::text[])"),
        "{query}"
    );
    assert!(
        query.contains("coalesce(tt.typed_tables, '{}'::text[])"),
        "{query}"
    );
    // Nothing a PostgreSQL 14 catalog lacks.
    for newer in ["typsubscript", "attcompression", "attgenerated"] {
        assert!(!query.contains(newer), "{newer}: {query}");
    }
}

#[test]
fn r3c_fdw_queries_read_server_and_column_privileges_and_foreign_partitions() {
    // Read on every supported version: srvacl, attacl, relispartition and
    // relpartbound exist since PostgreSQL 10.
    assert!(
        Dump::build_servers_query().contains("COALESCE(s.srvacl::text[], '{}'::text[]) as srv_acl")
    );
    let columns = Dump::build_foreign_table_columns_query("('public')");
    assert!(columns.contains("coalesce(a.attacl::text[], array[]::text[]) as col_acl"));
    let tables = Dump::build_foreign_tables_query("('public')");
    assert!(tables.contains("case when c.relispartition then ("));
    assert!(tables.contains("pg_get_expr(c.relpartbound, c.oid)"));
    assert!(tables.contains("as ft_partition_of"));
    assert!(tables.contains("as ft_partition_bound"));
}

// ==== r3-compat tests ====

/// `fill` runs in full for a dump whose `--scheme` pattern matches no
/// accessible schema (`WithoutSchemas::DatabaseWide`), so that it reads the
/// same database-wide objects as a dump of any schema would. That is safe
/// only because such a dump's schema filter is a list matching no schema:
/// every schema-scoped query then returns nothing rather than everything.
#[test]
fn an_empty_dump_filters_by_a_list_matching_no_schema() {
    let dump = empty_dump();
    assert!(dump.schemas.is_empty());
    let filter = dump.accessible_schema_filter();
    assert_eq!(filter, "(NULL)");
    assert!(
        Dump::build_casts_query(&filter).contains("WHERE n.nspname IN (NULL)"),
        "casts are read only for the types of a dumped schema"
    );
    assert!(Dump::build_operators_query(&filter).contains("WHERE n.nspname IN (NULL)"));
}

// ---- r4:typed — composite attribute collations ----

#[test]
fn build_composite_type_attributes_query_reads_non_default_collations() {
    let query = Dump::build_composite_type_attributes_query("('app')");
    // Only a collation other than the data type's own default is kept, and
    // it comes back qualified and quoted.
    let collation = "case when a.attcollation <> 0
                    and a.attcollation <> (select at.typcollation from pg_type at where at.oid = a.atttypid)
                then
                    (select quote_ident(cn.nspname) || '.' || quote_ident(cc.collname)
                     from pg_collation cc
                     join pg_namespace cn on cn.oid = cc.collnamespace
                     where cc.oid = a.attcollation)
                else null end as collation";
    assert!(query.contains(collation), "{query}");
    // Every column read exists on PostgreSQL 14: no version gate.
    assert!(query.contains("pg_catalog.format_type(a.atttypid, a.atttypmod) as data_type,"));
    assert!(query.contains("n.nspname in ('app')"), "{query}");
    assert!(query.contains("order by t.oid, a.attnum"), "{query}");
}

// ==== r4-sub tests ====

#[test]
fn build_subscriptions_query_reads_the_slot_on_every_version() {
    // subslotname exists since PostgreSQL 10: no version gate. NULL is
    // slot_name = NONE; a name equal to the subscription's is the default
    // and comes back NULL from the NULLIF.
    for pg_version in [140000, 150000, 160000, 170000, 180000, 190000] {
        let query = Dump::build_subscriptions_query(pg_version);
        assert!(
            query.contains("s.subslotname IS NULL as sub_slot_is_none"),
            "{pg_version}:\n{query}"
        );
        assert!(
            query.contains("NULLIF(s.subslotname, s.subname)::text as sub_slot_name"),
            "{pg_version}:\n{query}"
        );
    }
}
