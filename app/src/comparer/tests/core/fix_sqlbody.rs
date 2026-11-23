//! Routines with a SQL-standard body (`BEGIN ATOMIC … END` / `RETURN expr`)
//! as hard dependents of what they use.
//!
//! Unlike a string body, such a body is resolved when the routine is created,
//! even with `check_function_bodies` off, and `pg_depend` ties the routine to
//! every relation, column, routine and operator in it
//! (`Routine::sql_body_dependencies`). So PostgreSQL refuses to drop or retype
//! any of those while the routine exists, a `DROP … CASCADE` of one takes the
//! routine along, and the routine cannot be created before all of them exist.
//! These tests pin how the comparer drops such routines ahead of the change
//! (`drop_views`), re-creates them after it, and holds back the ones that need
//! an operator or cast the later phases create.

use super::helpers::{int_column, issue179_compute_routine, issue179_items_table};
use crate::comparer::core::*;
use crate::config::dump_config::DumpConfig;
use crate::config::grants_mode::GrantsMode;
use crate::dump::cast::Cast;
use crate::dump::foreign_table::{ForeignTable, ForeignTableColumn};
use crate::dump::operator::Operator;
use crate::dump::routine::{AggregateInfo, Routine, SqlBodyDependencies};
use crate::dump::sequence::Sequence;
use crate::dump::table::Table;
use crate::dump::table_column::TableColumn;
use crate::dump::view::{View, ViewColumn};
use sqlx::postgres::types::Oid;

/// `s.<name>(<arguments>)`, LANGUAGE sql, with the SQL-standard `body` and the
/// given `pg_depend` rows.
fn sql_body_fn(
    name: &str,
    arguments: &str,
    return_type: &str,
    body: &str,
    dependencies: SqlBodyDependencies,
) -> Routine {
    let mut routine = Routine::new(
        "s".to_string(),
        Oid(800),
        name.to_string(),
        "sql".to_string(),
        "function".to_string(),
        return_type.to_string(),
        arguments.to_string(),
        None,
        None,
        String::new(),
    );
    routine.owner = "postgres".to_string();
    routine.source_code = body.to_string();
    routine.is_sql_body = true;
    routine.sql_body_dependencies = dependencies;
    routine.hash();
    routine
}

fn reads(relations: &[&str], columns: &[&str]) -> SqlBodyDependencies {
    SqlBodyDependencies {
        relations: relations.iter().map(|r| r.to_string()).collect(),
        columns: columns.iter().map(|c| c.to_string()).collect(),
        ..SqlBodyDependencies::default()
    }
}

fn calls(routines: &[&str]) -> SqlBodyDependencies {
    SqlBodyDependencies {
        routines: routines.iter().map(|r| r.to_string()).collect(),
        ..SqlBodyDependencies::default()
    }
}

fn applies(operators: &[&str]) -> SqlBodyDependencies {
    SqlBodyDependencies {
        operators: operators.iter().map(|o| o.to_string()).collect(),
        ..SqlBodyDependencies::default()
    }
}

fn bigint(column: TableColumn) -> TableColumn {
    TableColumn {
        data_type: "bigint".to_string(),
        numeric_precision: Some(64),
        ..column
    }
}

fn table(schema: &str, name: &str, columns: Vec<TableColumn>) -> Table {
    let mut table = Table::new(
        schema.to_string(),
        name.to_string(),
        schema.to_string(),
        name.to_string(),
        "postgres".to_string(),
        None,
        columns,
        vec![],
        vec![],
        vec![],
        None,
    );
    table.hash();
    table
}

/// `s.t (id integer, v integer)`, or with `v bigint`.
fn t_table(v_bigint: bool) -> Table {
    let v = int_column("s", "t", "v", 2);
    let v = if v_bigint { bigint(v) } else { v };
    table("s", "t", vec![int_column("s", "t", "id", 1), v])
}

/// `s.f()`, reading `s.t.v`.
fn reader_of_t_v() -> Routine {
    sql_body_fn(
        "f",
        "",
        "bigint",
        "RETURN (SELECT max(t.v) AS max FROM s.t)",
        reads(&["s.t"], &["s.t.v"]),
    )
}

fn sequence(schema: &str, name: &str) -> Sequence {
    Sequence::new(
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
        None,
        None,
        None,
        None,
    )
}

fn view(schema: &str, name: &str, definition: &str, columns: &[&str]) -> View {
    let mut view = View::new(
        name.to_string(),
        definition.to_string(),
        schema.to_string(),
        vec![],
    );
    view.owner = "postgres".to_string();
    view.columns = columns
        .iter()
        .map(|c| ViewColumn {
            name: c.to_string(),
            data_type: "integer".to_string(),
            collation: None,
        })
        .collect();
    view.hash();
    view
}

fn operator(name: &str, procedure: &str) -> Operator {
    let mut op = Operator {
        schema: "s".to_string(),
        name: name.to_string(),
        owner: "postgres".to_string(),
        left_type: Some("integer".to_string()),
        right_type: Some("integer".to_string()),
        result_type: "boolean".to_string(),
        procedure: procedure.to_string(),
        commutator: None,
        negator: None,
        restrict: None,
        join: None,
        is_hashes: false,
        is_merges: false,
        comment: None,
        hash: None,
    };
    op.hash();
    op
}

fn dumps() -> (Dump, Dump) {
    (
        Dump::new(DumpConfig::default()),
        Dump::new(DumpConfig::default()),
    )
}

async fn full_compare(from: Dump, to: Dump, use_drop: bool, grants: GrantsMode) -> String {
    let mut comparer = Comparer::new(from, to, use_drop, false, true, grants);
    comparer.compare().await.unwrap();
    comparer.get_script()
}

fn position(script: &str, needle: &str) -> usize {
    script
        .find(needle)
        .unwrap_or_else(|| panic!("expected {needle:?} in:\n{script}"))
}

const DROP_F: &str = "drop function if exists s.f () cascade;";
const CREATE_F: &str = "create or replace function s.f() returns bigint";

// ---- Dropped ahead of the relation DDL, re-created after it ----

#[tokio::test]
async fn a_from_only_reader_is_dropped_before_its_table_and_sequence() {
    // PostgreSQL refuses `drop table` / `drop sequence` under the routine; the
    // routine phase used to drop it only afterwards.
    let (mut from, to) = dumps();
    from.tables.push(table(
        "s",
        "legacy",
        vec![int_column("s", "legacy", "id", 1)],
    ));
    from.sequences.push(sequence("s", "legacy_seq"));
    from.routines.push(sql_body_fn(
        "legacy_count",
        "",
        "bigint",
        "RETURN (SELECT count(*) AS count FROM s.legacy)",
        reads(&["s.legacy"], &[]),
    ));
    from.routines.push(sql_body_fn(
        "legacy_next",
        "",
        "bigint",
        "RETURN nextval('s.legacy_seq'::regclass)",
        reads(&["s.legacy_seq"], &[]),
    ));

    let script = full_compare(from, to, true, GrantsMode::Ignore).await;
    let drop_count = "drop function if exists s.legacy_count () cascade;";
    let drop_next = "drop function if exists s.legacy_next () cascade;";
    assert!(
        position(&script, drop_count) < position(&script, "drop table if exists s.legacy;"),
        "{script}"
    );
    assert!(
        position(&script, drop_next) < position(&script, "drop sequence if exists s.legacy_seq;"),
        "{script}"
    );
    // Not dropped a second time by the routine phase.
    assert_eq!(script.matches(drop_count).count(), 1, "{script}");
    assert_eq!(script.matches(drop_next).count(), 1, "{script}");
}

#[tokio::test]
async fn an_unchanged_reader_of_a_retyped_column_is_dropped_and_re_created_with_its_grants() {
    // `cannot alter type of a column used by a function or procedure`, on every
    // run: the routine is unchanged, so nothing ever moved it out of the way.
    let (mut from, mut to) = dumps();
    from.tables.push(t_table(false));
    to.tables.push(t_table(true));
    for dump in [&mut from, &mut to] {
        let mut f = reader_of_t_v();
        f.acl = vec![
            "postgres=X/postgres".to_string(),
            "reader=X/postgres".to_string(),
        ];
        f.hash();
        dump.routines.push(f);
    }

    let script = full_compare(from, to, true, GrantsMode::Full).await;
    let alter = position(&script, "alter table s.t alter column v type bigint");
    assert!(position(&script, DROP_F) < alter, "{script}");
    assert!(alter < position(&script, CREATE_F), "{script}");
    // A re-created routine starts from the default ACL: PUBLIC's EXECUTE goes,
    // the explicit grant comes back.
    assert!(
        script.contains("REVOKE EXECUTE ON FUNCTION s.f() FROM PUBLIC;"),
        "{script}"
    );
    assert!(
        script.contains("GRANT EXECUTE ON FUNCTION s.f() TO reader;"),
        "{script}"
    );
}

#[tokio::test]
async fn a_changed_reader_of_a_dropped_column_is_dropped_before_the_column() {
    let (mut from, mut to) = dumps();
    from.tables.push(table(
        "s",
        "t",
        vec![
            int_column("s", "t", "id", 1),
            int_column("s", "t", "old_col", 2),
        ],
    ));
    to.tables
        .push(table("s", "t", vec![int_column("s", "t", "id", 1)]));
    from.routines.push(sql_body_fn(
        "f",
        "",
        "bigint",
        "RETURN (SELECT count(t.old_col) AS count FROM s.t)",
        reads(&["s.t"], &["s.t.old_col"]),
    ));
    to.routines.push(sql_body_fn(
        "f",
        "",
        "bigint",
        "RETURN (SELECT count(t.id) AS count FROM s.t)",
        reads(&["s.t"], &["s.t.id"]),
    ));

    let script = full_compare(from, to, true, GrantsMode::Ignore).await;
    let drop_column = position(&script, "drop column old_col");
    assert!(position(&script, DROP_F) < drop_column, "{script}");
    assert!(drop_column < position(&script, CREATE_F), "{script}");
    assert!(script.contains("count(t.id)"), "{script}");
}

#[tokio::test]
async fn a_reader_of_an_untouched_column_stays_in_place() {
    // PostgreSQL only refuses the columns the body reads: retyping `v` under a
    // reader of `id` alone is allowed.
    let (mut from, mut to) = dumps();
    from.tables.push(t_table(false));
    to.tables.push(t_table(true));
    let reader_of_id = sql_body_fn(
        "f",
        "",
        "bigint",
        "RETURN (SELECT max(t.id) AS max FROM s.t)",
        reads(&["s.t"], &["s.t.id"]),
    );
    from.routines.push(reader_of_id.clone());
    to.routines.push(reader_of_id);

    let script = full_compare(from, to, true, GrantsMode::Ignore).await;
    assert!(script.contains("alter column v type bigint"), "{script}");
    assert!(!script.contains("drop function"), "{script}");
    assert!(!script.contains(CREATE_F), "{script}");
}

#[tokio::test]
async fn a_string_body_reader_is_not_a_hard_dependent() {
    let (mut from, mut to) = dumps();
    from.tables.push(t_table(false));
    to.tables.push(t_table(true));
    let mut string_body = reader_of_t_v();
    string_body.is_sql_body = false;
    string_body.sql_body_dependencies = SqlBodyDependencies::default();
    string_body.source_code = " SELECT max(v) FROM s.t ".to_string();
    string_body.hash();
    from.routines.push(string_body.clone());
    to.routines.push(string_body);

    let script = full_compare(from, to, true, GrantsMode::Ignore).await;
    assert!(!script.contains("drop function"), "{script}");
    assert!(!script.contains(CREATE_F), "{script}");
}

#[tokio::test]
async fn nothing_is_dropped_ahead_without_use_drop() {
    // The table drops are commented out then too; the output stays what it was.
    let (mut from, mut to) = dumps();
    from.tables.push(t_table(false));
    to.tables.push(t_table(true));
    from.routines.push(reader_of_t_v());
    to.routines.push(reader_of_t_v());

    let script = full_compare(from, to, false, GrantsMode::Ignore).await;
    assert!(!script.contains("drop function"), "{script}");
    assert!(!script.contains(CREATE_F), "{script}");
}

#[tokio::test]
async fn a_reader_of_a_rebuilt_view_goes_first_and_comes_back_after_it() {
    // A column dropped from the middle of a view is a DROP VIEW + CREATE, which
    // PostgreSQL refuses while the routine reads the view; unchanged, the
    // routine never moved, so the migration failed on every run.
    let (mut from, mut to) = dumps();
    from.views.push(view(
        "s",
        "v",
        " SELECT t.id,\n    t.a,\n    t.b\n   FROM s.t;",
        &["id", "a", "b"],
    ));
    to.views.push(view(
        "s",
        "v",
        " SELECT t.id,\n    t.b\n   FROM s.t;",
        &["id", "b"],
    ));
    for dump in [&mut from, &mut to] {
        dump.routines.push(sql_body_fn(
            "f",
            "",
            "bigint",
            "RETURN (SELECT count(*) AS count FROM s.v)",
            reads(&["s.v"], &[]),
        ));
    }

    let script = full_compare(from, to, true, GrantsMode::Ignore).await;
    let drop_view = position(&script, "drop view if exists s.v;");
    let create_view = position(&script, "CREATE OR REPLACE VIEW s.v");
    assert!(position(&script, DROP_F) < drop_view, "{script}");
    assert!(create_view < position(&script, CREATE_F), "{script}");
}

#[tokio::test]
async fn a_view_calling_a_dropped_reader_is_dropped_before_it_and_re_created() {
    // The routine's CASCADE would take the view; it is dropped explicitly first
    // and comes back through `dropped_views`, after the routine.
    let (mut from, mut to) = dumps();
    from.tables.push(t_table(false));
    to.tables.push(t_table(true));
    for dump in [&mut from, &mut to] {
        dump.routines.push(reader_of_t_v());
        dump.views
            .push(view("s", "w", " SELECT s.f() AS m;", &["m"]));
    }

    let script = full_compare(from, to, true, GrantsMode::Ignore).await;
    assert!(
        position(&script, "drop view if exists s.w;") < position(&script, DROP_F),
        "{script}"
    );
    assert!(
        position(&script, CREATE_F) < position(&script, "CREATE OR REPLACE VIEW s.w"),
        "{script}"
    );
}

#[tokio::test]
async fn an_unchanged_caller_of_a_re_created_callee_is_dropped_and_re_created() {
    // The callee's return type changes: DROP … CASCADE silently took the
    // caller, and nothing re-created it because it had not changed.
    let callee = |return_type: &str| {
        sql_body_fn(
            "callee",
            "p integer",
            return_type,
            "RETURN (p + 1)",
            SqlBodyDependencies::default(),
        )
    };
    let caller = sql_body_fn(
        "caller",
        "p integer",
        "bigint",
        "RETURN (s.callee(p) * 2)",
        calls(&["s.callee(p integer)"]),
    );
    let (mut from, mut to) = dumps();
    from.routines.push(callee("integer"));
    to.routines.push(callee("bigint"));
    from.routines.push(caller.clone());
    to.routines.push(caller);

    let script = full_compare(from, to, true, GrantsMode::Ignore).await;
    let drop_caller = position(
        &script,
        "drop function if exists s.caller (p integer) cascade;",
    );
    let drop_callee = position(
        &script,
        "drop function if exists s.callee (p integer) cascade;",
    );
    let create_callee = position(
        &script,
        "create or replace function s.callee(p integer) returns bigint",
    );
    let create_caller = position(&script, "create or replace function s.caller(p integer)");
    assert!(drop_caller < drop_callee, "{script}");
    assert!(create_callee < create_caller, "{script}");
}

#[tokio::test]
async fn an_aggregate_over_a_dropped_support_function_is_dropped_first_and_re_created() {
    let mut sfunc = sql_body_fn(
        "sf",
        "state integer, v integer",
        "integer",
        "RETURN (state + (SELECT max(t.v) AS max FROM s.t))",
        reads(&["s.t"], &["s.t.v"]),
    );
    sfunc.hash();
    let mut aggregate = Routine::new(
        "s".to_string(),
        Oid(801),
        "agg".to_string(),
        "internal".to_string(),
        "aggregate".to_string(),
        "integer".to_string(),
        "integer".to_string(),
        None,
        None,
        String::new(),
    );
    aggregate.aggregate_info = Some(AggregateInfo {
        sfunc: "s.sf".to_string(),
        stype: "integer".to_string(),
        sspace: None,
        finalfunc: None,
        finalfunc_extra: false,
        finalfunc_modify: None,
        combinefunc: None,
        serialfunc: None,
        deserialfunc: None,
        initcond: Some("0".to_string()),
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

    let (mut from, mut to) = dumps();
    from.tables.push(t_table(false));
    to.tables.push(t_table(true));
    for dump in [&mut from, &mut to] {
        dump.routines.push(sfunc.clone());
        dump.routines.push(aggregate.clone());
    }

    let script = full_compare(from, to, true, GrantsMode::Ignore).await;
    let drop_agg = position(&script, "drop aggregate if exists s.agg (integer) cascade;");
    let drop_sfunc = position(
        &script,
        "drop function if exists s.sf (state integer, v integer) cascade;",
    );
    let create_sfunc = position(&script, "create or replace function s.sf(");
    let create_agg = position(&script, "create or replace aggregate s.agg(");
    assert!(drop_agg < drop_sfunc, "{script}");
    assert!(create_sfunc < create_agg, "{script}");
}

#[tokio::test]
async fn dependents_a_dropped_reader_cascades_away_are_restored() {
    // `test_deps.compute` reads a column that is retyped. Its CASCADE takes the
    // CHECK constraint, index, generated column, default and policy of the
    // unchanged `items` table that call it; Phase 7 brings them back.
    let compute = || {
        let mut routine = issue179_compute_routine(
            "integer",
            "RETURN (x + (SELECT max(rates.v) AS max FROM test_deps.rates))",
        );
        routine.is_sql_body = true;
        routine.sql_body_dependencies = reads(&["test_deps.rates"], &["test_deps.rates.v"]);
        routine.hash();
        routine
    };
    let rates = |v_bigint: bool| {
        let v = int_column("test_deps", "rates", "v", 1);
        table(
            "test_deps",
            "rates",
            vec![if v_bigint { bigint(v) } else { v }],
        )
    };
    let items = issue179_items_table("integer", "test_deps.compute(0)", "integer");

    let (mut from, mut to) = dumps();
    from.tables.push(rates(false));
    to.tables.push(rates(true));
    for dump in [&mut from, &mut to] {
        dump.tables.push(items.clone());
        dump.routines.push(compute());
    }

    let script = full_compare(from, to, true, GrantsMode::Ignore).await;
    let drop_compute = position(
        &script,
        "drop function if exists test_deps.compute (x integer) cascade;",
    );
    let create_compute = position(
        &script,
        "create or replace function test_deps.compute(x integer)",
    );
    let restore = position(&script, "Recreate dependents dropped by CASCADE: Start");
    assert!(
        drop_compute < create_compute && create_compute < restore,
        "{script}"
    );
    let restored = &script[restore..];
    for dependent in [
        "chk_compute",
        "idx_compute",
        "gen_col",
        "set default test_deps.compute(0)",
        "p_items",
    ] {
        assert!(
            restored.contains(dependent),
            "{dependent} not restored:\n{script}"
        );
    }
}

// ---- Created after the operators and casts it uses ----

#[tokio::test]
async fn a_routine_using_a_new_operator_is_created_after_it_with_its_dependents() {
    let op = operator("===", "s.eqf");
    let (from, mut to) = dumps();
    to.operators.push(op.clone());
    to.routines.push(sql_body_fn(
        "eqf",
        "integer, integer",
        "boolean",
        "RETURN ($1 = $2)",
        SqlBodyDependencies::default(),
    ));
    to.routines.push(sql_body_fn(
        "uses_op",
        "p integer",
        "boolean",
        "RETURN (p OPERATOR(s.===) 1)",
        applies(&[&op.key()]),
    ));
    to.views
        .push(view("s", "v_op", " SELECT s.uses_op(1) AS x;", &["x"]));

    let script = full_compare(from, to, true, GrantsMode::Ignore).await;
    let create_op = position(&script, "CREATE OPERATOR s.===");
    assert!(
        position(&script, "create or replace function s.eqf(") < create_op,
        "{script}"
    );
    let uses_op = position(&script, "create or replace function s.uses_op(");
    assert!(create_op < uses_op, "{script}");
    assert!(
        uses_op < position(&script, "CREATE OR REPLACE VIEW s.v_op"),
        "{script}"
    );
}

#[tokio::test]
async fn a_routine_using_a_new_cast_is_created_after_it() {
    // The cast only shows in pg_depend as its function; the body reads
    // `(p)::s.pair`.
    let mut cast = Cast {
        source_type: "integer".to_string(),
        target_type: "s.pair".to_string(),
        cast_method: "f".to_string(),
        function_name: Some("s.int_to_pair(integer)".to_string()),
        cast_context: "e".to_string(),
        comment: None,
        hash: None,
    };
    cast.hash();
    let (from, mut to) = dumps();
    to.casts.push(cast);
    to.routines.push(sql_body_fn(
        "uses_cast",
        "p integer",
        "s.pair",
        "RETURN (p)::s.pair",
        calls(&["s.int_to_pair(integer)"]),
    ));

    let script = full_compare(from, to, true, GrantsMode::Ignore).await;
    assert!(
        position(&script, "CREATE CAST (integer AS s.pair)")
            < position(&script, "create or replace function s.uses_cast("),
        "{script}"
    );
}

#[tokio::test]
async fn a_user_of_a_rebuilt_operator_is_dropped_before_it_and_created_after_it() {
    // DROP OPERATOR … CASCADE silently took the unchanged routine.
    let user = sql_body_fn(
        "uses_op",
        "p integer",
        "boolean",
        "RETURN (p OPERATOR(s.===) 1)",
        applies(&["s.===(integer, integer)"]),
    );
    let (mut from, mut to) = dumps();
    from.operators.push(operator("===", "s.eqf"));
    to.operators.push(operator("===", "s.eqf_or_null"));
    from.routines.push(user.clone());
    to.routines.push(user);

    let script = full_compare(from, to, true, GrantsMode::Ignore).await;
    let drop_user = position(
        &script,
        "drop function if exists s.uses_op (p integer) cascade;",
    );
    let drop_op = position(
        &script,
        "DROP OPERATOR IF EXISTS s.===(integer, integer) CASCADE;",
    );
    let create_op = position(&script, "CREATE OPERATOR s.===");
    let create_user = position(&script, "create or replace function s.uses_op(");
    assert!(drop_user < drop_op, "{script}");
    assert!(create_op < create_user, "{script}");
}

#[tokio::test]
async fn nothing_is_held_back_without_a_new_operator_or_cast() {
    // An operator both sides define alike leaves the routine where it was.
    let op = operator("===", "s.eqf");
    let user = sql_body_fn(
        "uses_op",
        "p integer",
        "boolean",
        "RETURN (p OPERATOR(s.===) 1)",
        applies(&[&op.key()]),
    );
    let (mut from, mut to) = dumps();
    from.operators.push(op.clone());
    to.operators.push(op);
    to.routines.push(user);

    let script = full_compare(from, to, true, GrantsMode::Ignore).await;
    let create_user = position(&script, "create or replace function s.uses_op(");
    let routines_end = position(&script, "Routines & Views: End section");
    assert!(create_user < routines_end, "{script}");
    assert!(
        !script.contains("using operators or casts created above"),
        "{script}"
    );
}

// ---- Ordered by what pg_depend records ----

#[tokio::test]
async fn a_sql_standard_reader_is_created_after_the_view_it_reads() {
    // Routines sort before views; the recorded relation is what orders them,
    // whatever the body text looks like.
    let (from, mut to) = dumps();
    to.views.push(view("s", "v", " SELECT 1 AS one;", &["one"]));
    to.routines.push(sql_body_fn(
        "f",
        "",
        "bigint",
        "RETURN (SELECT count(*) AS count FROM v)",
        reads(&["s.v"], &[]),
    ));

    let mut comparer = Comparer::new(from, to, true, false, true, GrantsMode::Ignore);
    comparer.compare_routines_and_views().await.unwrap();
    let script = comparer.get_script();
    assert!(
        position(&script, "CREATE OR REPLACE VIEW s.v") < position(&script, CREATE_F),
        "{script}"
    );
}

#[tokio::test]
async fn a_sql_standard_body_is_created_after_a_routine_it_only_calls_implicitly() {
    // A cast's function never appears in the body text (`(p)::s.pair`), but
    // pg_depend records the call.
    let (from, mut to) = dumps();
    to.routines.push(sql_body_fn(
        "a_uses",
        "p integer",
        "integer",
        "RETURN ((p)::s.pair).a",
        calls(&["s.z_conv(integer)"]),
    ));
    to.routines.push(sql_body_fn(
        "z_conv",
        "integer",
        "integer",
        "RETURN $1",
        SqlBodyDependencies::default(),
    ));

    let mut comparer = Comparer::new(from, to, true, false, true, GrantsMode::Ignore);
    comparer.compare_routines_and_views().await.unwrap();
    let script = comparer.get_script();
    assert!(
        position(&script, "create or replace function s.z_conv(")
            < position(&script, "create or replace function s.a_uses("),
        "{script}"
    );
}

/// `s.ft`, a foreign table on `server` with integer or bigint columns.
fn foreign_table(server: &str, columns: &[(&str, &str)]) -> ForeignTable {
    ForeignTable::new(
        "s".to_string(),
        "ft".to_string(),
        server.to_string(),
        "postgres".to_string(),
        vec![],
        columns
            .iter()
            .map(|(column, data_type)| ForeignTableColumn {
                name: column.to_string(),
                data_type: data_type.to_string(),
                is_nullable: true,
                column_default: None,
                options: vec![],
                acl: Vec::new(),
            })
            .collect(),
    )
}

/// `s.f()`, reading `s.ft.v`.
fn reader_of_ft_v() -> Routine {
    sql_body_fn(
        "f",
        "",
        "bigint",
        "RETURN (SELECT max(ft.v) AS max FROM s.ft)",
        reads(&["s.ft"], &["s.ft.v"]),
    )
}

#[tokio::test]
async fn a_reader_of_a_foreign_table_whose_server_changes_is_dropped_before_it() {
    // A server change is DROP FOREIGN TABLE (no CASCADE) + CREATE FOREIGN
    // TABLE, refused while the routine reads the foreign table.
    let (mut from, mut to) = dumps();
    from.foreign_tables
        .push(foreign_table("srv", &[("id", "integer"), ("v", "integer")]));
    to.foreign_tables.push(foreign_table(
        "srv2",
        &[("id", "integer"), ("v", "integer")],
    ));
    for dump in [&mut from, &mut to] {
        dump.routines.push(reader_of_ft_v());
    }

    let script = full_compare(from, to, true, GrantsMode::Ignore).await;
    let drop_ft = position(&script, "drop foreign table if exists s.ft;");
    let create_ft = position(&script, "create foreign table s.ft (");
    assert!(position(&script, DROP_F) < drop_ft, "{script}");
    assert!(create_ft < position(&script, CREATE_F), "{script}");
}

#[tokio::test]
async fn a_reader_of_a_retyped_foreign_table_column_is_dropped_before_it() {
    // ALTER FOREIGN TABLE ... ALTER COLUMN TYPE is refused like its table
    // counterpart; retyping a column the body does not read is not.
    let (mut from, mut to) = dumps();
    from.foreign_tables
        .push(foreign_table("srv", &[("id", "integer"), ("v", "integer")]));
    to.foreign_tables
        .push(foreign_table("srv", &[("id", "integer"), ("v", "bigint")]));
    for dump in [&mut from, &mut to] {
        dump.routines.push(reader_of_ft_v());
    }

    let script = full_compare(from.clone(), to.clone(), true, GrantsMode::Ignore).await;
    let retype = position(
        &script,
        "alter foreign table s.ft alter column v type bigint;",
    );
    assert!(position(&script, DROP_F) < retype, "{script}");
    assert!(retype < position(&script, CREATE_F), "{script}");

    to.foreign_tables[0] = foreign_table("srv", &[("id", "bigint"), ("v", "integer")]);
    let script = full_compare(from, to, true, GrantsMode::Ignore).await;
    assert!(!script.contains(DROP_F), "{script}");
}
