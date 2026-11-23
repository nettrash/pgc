//! Tests for `compare_routines` and `compare_routines_and_views`: drop and
//! recreate rules, dependency ordering, overloads, owner and config changes.

use super::helpers::issue189_view;
use crate::comparer::core::*;
use crate::config::dump_config::DumpConfig;
use crate::config::grants_mode::GrantsMode;
use crate::dump::routine::{AggregateInfo, Routine};
use crate::dump::schema::Schema;
use crate::dump::view::View;
use sqlx::postgres::types::Oid;

#[tokio::test]
async fn compare_routines_drops_and_recreates_on_return_type_change() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let from_routine = Routine::new(
        "public".to_string(),
        Oid(1),
        "test_func".to_string(),
        "plpgsql".to_string(),
        "FUNCTION".to_string(),
        "integer".to_string(),
        "".to_string(),
        None,
        None,
        "BEGIN RETURN 1; END".to_string(),
    );

    let to_routine = Routine::new(
        "public".to_string(),
        Oid(1),
        "test_func".to_string(),
        "plpgsql".to_string(),
        "FUNCTION".to_string(),
        "text".to_string(),
        "".to_string(),
        None,
        None,
        "BEGIN RETURN '1'; END".to_string(),
    );

    from_dump.routines.push(from_routine);
    to_dump.routines.push(to_routine);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Ignore);
    comparer.compare_routines().await.unwrap();
    let script = comparer.get_script();

    assert!(script.contains("drop function if exists public.test_func () cascade;"));
    assert!(script.contains("create or replace function public.test_func() returns text"));
}

#[tokio::test]
async fn compare_routines_drops_and_recreates_on_argument_change() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let from_routine = Routine::new(
        "public".to_string(),
        Oid(1),
        "test_func".to_string(),
        "plpgsql".to_string(),
        "FUNCTION".to_string(),
        "integer".to_string(),
        "a integer".to_string(),
        None,
        None,
        "BEGIN RETURN a; END".to_string(),
    );

    let to_routine = Routine::new(
        "public".to_string(),
        Oid(1),
        "test_func".to_string(),
        "plpgsql".to_string(),
        "FUNCTION".to_string(),
        "integer".to_string(),
        "a text".to_string(),
        None,
        None,
        "BEGIN RETURN 1; END".to_string(),
    );

    from_dump.routines.push(from_routine);
    to_dump.routines.push(to_routine);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Ignore);
    comparer.compare_routines().await.unwrap();
    let script = comparer.get_script();

    assert!(script.contains("drop function if exists public.test_func (a integer) cascade;"));
    assert!(script.contains("create or replace function public.test_func(a text) returns integer"));
}

#[tokio::test]
async fn compare_routines_applies_sql_routines_last() {
    let from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let plpgsql_routine = Routine::new(
        "public".to_string(),
        Oid(1),
        "fn_plpgsql".to_string(),
        "plpgsql".to_string(),
        "FUNCTION".to_string(),
        "integer".to_string(),
        "".to_string(),
        None,
        None,
        "BEGIN RETURN 1; END".to_string(),
    );

    let sql_routine = Routine::new(
        "public".to_string(),
        Oid(2),
        "fn_sql".to_string(),
        "sql".to_string(),
        "FUNCTION".to_string(),
        "integer".to_string(),
        "".to_string(),
        None,
        None,
        "SELECT 1;".to_string(),
    );

    // Intentionally add SQL first to ensure reordering happens.
    to_dump.routines.push(sql_routine);
    to_dump.routines.push(plpgsql_routine);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Ignore);
    comparer.compare_routines().await.unwrap();
    let script = comparer.get_script();

    let pos_plpgsql = script
        .find("create or replace function public.fn_plpgsql")
        .expect("plpgsql routine script not found");
    let pos_sql = script
        .find("create or replace function public.fn_sql")
        .expect("sql routine script not found");

    assert!(pos_plpgsql < pos_sql, "SQL routines should be applied last");
}

#[tokio::test]
async fn compare_routines_emits_owner_change() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_routine = Routine::new(
        "public".to_string(),
        Oid(1),
        "test_func".to_string(),
        "plpgsql".to_string(),
        "FUNCTION".to_string(),
        "integer".to_string(),
        "".to_string(),
        None,
        None,
        "BEGIN RETURN 1; END".to_string(),
    );
    from_routine.owner = "old_owner".to_string();
    from_routine.hash();

    let mut to_routine = Routine::new(
        "public".to_string(),
        Oid(1),
        "test_func".to_string(),
        "plpgsql".to_string(),
        "FUNCTION".to_string(),
        "integer".to_string(),
        "".to_string(),
        None,
        None,
        "BEGIN RETURN 1; END".to_string(),
    );
    to_routine.owner = "new_owner".to_string();
    to_routine.hash();

    from_dump.routines.push(from_routine);
    to_dump.routines.push(to_routine);

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_routines().await.unwrap();
    let script = comparer.get_script();

    assert!(script.contains("alter function public.test_func() owner to new_owner;"));
}

#[tokio::test]
async fn compare_routines_orders_by_dependencies() {
    let from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    // r_base_value: no dependencies
    let r_base = Routine::new(
        "test_schema".to_string(),
        Oid(1),
        "r_base_value".to_string(),
        "sql".to_string(),
        "FUNCTION".to_string(),
        "integer".to_string(),
        "".to_string(),
        None,
        None,
        "\n    SELECT 10;\n".to_string(),
    );

    // x_step_one: depends on r_base_value
    let x_step = Routine::new(
        "test_schema".to_string(),
        Oid(2),
        "x_step_one".to_string(),
        "sql".to_string(),
        "FUNCTION".to_string(),
        "integer".to_string(),
        "".to_string(),
        None,
        None,
        "\n    SELECT test_schema.r_base_value() + 5;\n".to_string(),
    );

    // a_middle_layer: depends on x_step_one and r_base_value
    let a_middle = Routine::new(
        "test_schema".to_string(),
        Oid(3),
        "a_middle_layer".to_string(),
        "sql".to_string(),
        "FUNCTION".to_string(),
        "integer".to_string(),
        "".to_string(),
        None,
        None,
        "\n    SELECT test_schema.x_step_one() * test_schema.r_base_value();\n".to_string(),
    );

    // z_final_report: depends on a_middle_layer
    let z_final = Routine::new(
        "test_schema".to_string(),
        Oid(4),
        "z_final_report".to_string(),
        "plpgsql".to_string(),
        "PROCEDURE".to_string(),
        "void".to_string(),
        "".to_string(),
        None,
        None,
        "\nDECLARE\n    result integer;\nBEGIN\n    SELECT test_schema.a_middle_layer() INTO result;\n    RAISE NOTICE 'Final result: %', result;\nEND;\n".to_string(),
    );

    // Push in deliberately wrong alphabetical / type order.
    to_dump.routines.push(z_final);
    to_dump.routines.push(x_step);
    to_dump.routines.push(a_middle);
    to_dump.routines.push(r_base);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Ignore);
    comparer.compare_routines().await.unwrap();
    let script = comparer.get_script();

    let pos_base = script
        .find("create or replace function test_schema.r_base_value")
        .expect("r_base_value not found");
    let pos_step = script
        .find("create or replace function test_schema.x_step_one")
        .expect("x_step_one not found");
    let pos_middle = script
        .find("create or replace function test_schema.a_middle_layer")
        .expect("a_middle_layer not found");
    let pos_final = script
        .find("create or replace procedure test_schema.z_final_report")
        .expect("z_final_report not found");

    assert!(
        pos_base < pos_step,
        "r_base_value must come before x_step_one (depends on it)"
    );
    assert!(
        pos_base < pos_middle,
        "r_base_value must come before a_middle_layer (depends on it)"
    );
    assert!(
        pos_step < pos_middle,
        "x_step_one must come before a_middle_layer (depends on it)"
    );
    assert!(
        pos_middle < pos_final,
        "a_middle_layer must come before z_final_report (depends on it)"
    );
}

#[tokio::test]
async fn compare_routines_drops_in_reverse_dependency_order() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());

    // r_base_value: no dependencies
    let r_base = Routine::new(
        "test_schema".to_string(),
        Oid(1),
        "r_base_value".to_string(),
        "sql".to_string(),
        "FUNCTION".to_string(),
        "integer".to_string(),
        "".to_string(),
        None,
        None,
        "\n    SELECT 10;\n".to_string(),
    );

    // x_step_one: depends on r_base_value
    let x_step = Routine::new(
        "test_schema".to_string(),
        Oid(2),
        "x_step_one".to_string(),
        "sql".to_string(),
        "FUNCTION".to_string(),
        "integer".to_string(),
        "".to_string(),
        None,
        None,
        "\n    SELECT test_schema.r_base_value() + 5;\n".to_string(),
    );

    // a_middle_layer: depends on x_step_one and r_base_value
    let a_middle = Routine::new(
        "test_schema".to_string(),
        Oid(3),
        "a_middle_layer".to_string(),
        "sql".to_string(),
        "FUNCTION".to_string(),
        "integer".to_string(),
        "".to_string(),
        None,
        None,
        "\n    SELECT test_schema.x_step_one() * test_schema.r_base_value();\n".to_string(),
    );

    from_dump.routines.push(r_base);
    from_dump.routines.push(x_step);
    from_dump.routines.push(a_middle);

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_routines().await.unwrap();
    let script = comparer.get_script();

    let pos_base = script
        .find("drop function if exists test_schema.r_base_value")
        .expect("r_base_value drop not found");
    let pos_step = script
        .find("drop function if exists test_schema.x_step_one")
        .expect("x_step_one drop not found");
    let pos_middle = script
        .find("drop function if exists test_schema.a_middle_layer")
        .expect("a_middle_layer drop not found");

    // Drops should go in reverse dependency order: dependents first.
    assert!(
        pos_middle < pos_step,
        "a_middle_layer must be dropped before x_step_one"
    );
    assert!(
        pos_step < pos_base,
        "x_step_one must be dropped before r_base_value"
    );
}

#[tokio::test]
async fn compare_routines_and_views_orders_by_dependencies() {
    let from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let r_base = Routine::new(
        "test_schema".to_string(),
        Oid(1),
        "r_base_value".to_string(),
        "sql".to_string(),
        "FUNCTION".to_string(),
        "integer".to_string(),
        "".to_string(),
        None,
        None,
        "\n    SELECT 10;\n".to_string(),
    );

    let x_step = Routine::new(
        "test_schema".to_string(),
        Oid(2),
        "x_step_one".to_string(),
        "sql".to_string(),
        "FUNCTION".to_string(),
        "integer".to_string(),
        "".to_string(),
        None,
        None,
        "\n    SELECT test_schema.r_base_value() + 5;\n".to_string(),
    );

    let a_middle = Routine::new(
        "test_schema".to_string(),
        Oid(3),
        "a_middle_layer".to_string(),
        "sql".to_string(),
        "FUNCTION".to_string(),
        "integer".to_string(),
        "".to_string(),
        None,
        None,
        "\n    SELECT test_schema.x_step_one() * test_schema.r_base_value();\n".to_string(),
    );

    let z_final = Routine::new(
        "test_schema".to_string(),
        Oid(4),
        "z_final_report".to_string(),
        "plpgsql".to_string(),
        "PROCEDURE".to_string(),
        "void".to_string(),
        "".to_string(),
        None,
        None,
        "\nDECLARE\n    result integer;\nBEGIN\n    SELECT test_schema.a_middle_layer() INTO result;\n    RAISE NOTICE 'Final result: %', result;\nEND;\n".to_string(),
    );

    // Push in deliberately wrong order.
    to_dump.routines.push(z_final);
    to_dump.routines.push(x_step);
    to_dump.routines.push(a_middle);
    to_dump.routines.push(r_base);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Ignore);
    comparer.compare_routines_and_views().await.unwrap();
    let script = comparer.get_script();

    let pos_base = script
        .find("create or replace function test_schema.r_base_value")
        .expect("r_base_value not found");
    let pos_step = script
        .find("create or replace function test_schema.x_step_one")
        .expect("x_step_one not found");
    let pos_middle = script
        .find("create or replace function test_schema.a_middle_layer")
        .expect("a_middle_layer not found");
    let pos_final = script
        .find("create or replace procedure test_schema.z_final_report")
        .expect("z_final_report not found");

    assert!(
        pos_base < pos_step,
        "r_base_value must come before x_step_one"
    );
    assert!(
        pos_base < pos_middle,
        "r_base_value must come before a_middle_layer"
    );
    assert!(
        pos_step < pos_middle,
        "x_step_one must come before a_middle_layer"
    );
    assert!(
        pos_middle < pos_final,
        "a_middle_layer must come before z_final_report"
    );
}

#[tokio::test]
async fn compare_creates_routines_and_views_in_dependency_order() {
    // Scenario from the user report:
    //   get_user_count()   – function, no view dependency
    //   v_user_stats       – view that calls get_user_count()
    //   report_user_stats  – function that reads v_user_stats
    //   print_user_stats   – procedure that reads v_user_stats
    //
    // Correct creation order: get_user_count → v_user_stats → report/print
    let from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    to_dump.schemas.push(Schema::new(
        "test_schema".to_string(),
        "test_schema".to_string(),
        None,
    ));

    let get_user_count = Routine::new(
        "test_schema".to_string(),
        Oid(1),
        "get_user_count".to_string(),
        "sql".to_string(),
        "FUNCTION".to_string(),
        "integer".to_string(),
        "".to_string(),
        None,
        None,
        "  SELECT count(*) FROM test_schema.users;\n".to_string(),
    );

    let report_user_stats = Routine::new(
        "test_schema".to_string(),
        Oid(2),
        "report_user_stats".to_string(),
        "sql".to_string(),
        "FUNCTION".to_string(),
        "text".to_string(),
        "".to_string(),
        None,
        None,
        "  SELECT 'Total users in view: ' || total_users\n  FROM test_schema.v_user_stats\n  LIMIT 1;\n".to_string(),
    );

    let print_user_stats = Routine::new(
        "test_schema".to_string(),
        Oid(3),
        "print_user_stats".to_string(),
        "plpgsql".to_string(),
        "PROCEDURE".to_string(),
        "void".to_string(),
        "".to_string(),
        None,
        None,
        "\nDECLARE\n    cnt int;\nBEGIN\n    SELECT total_users INTO cnt\n    FROM test_schema.v_user_stats\n    LIMIT 1;\n    RAISE NOTICE 'Total users in view: %', cnt;\nEND;\n".to_string(),
    );

    let mut v_user_stats = View::new(
        "v_user_stats".to_string(),
        " SELECT test_schema.get_user_count() AS total_users,\n    users.name\n   FROM test_schema.users;\n".to_string(),
        "test_schema".to_string(),
        vec!["test_schema.users".to_string()],
    );
    v_user_stats.owner = "postgres".to_string();
    v_user_stats.hash();

    // Intentionally add in wrong order to test sorting
    to_dump.routines.push(print_user_stats);
    to_dump.routines.push(report_user_stats);
    to_dump.routines.push(get_user_count);
    to_dump.views.push(v_user_stats);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();

    let pos_get_user_count = script
        .find("create or replace function test_schema.get_user_count")
        .expect("get_user_count not found in script");
    let pos_view = script
        .find("test_schema.v_user_stats")
        .expect("v_user_stats not found in script");
    let pos_report = script
        .find("create or replace function test_schema.report_user_stats")
        .expect("report_user_stats not found in script");
    let pos_print = script
        .find("create or replace procedure test_schema.print_user_stats")
        .expect("print_user_stats not found in script");

    assert!(
        pos_get_user_count < pos_view,
        "get_user_count() must be created before v_user_stats (function is used by view)"
    );
    assert!(
        pos_view < pos_report,
        "v_user_stats must be created before report_user_stats() (view is used by function)"
    );
    assert!(
        pos_view < pos_print,
        "v_user_stats must be created before print_user_stats() (view is used by procedure)"
    );
}

#[tokio::test]
async fn compare_creates_materialized_view_after_dependent_routine() {
    // Materialized view that uses a function should be created after that function.
    let from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let helper_fn = Routine::new(
        "public".to_string(),
        Oid(1),
        "helper".to_string(),
        "sql".to_string(),
        "FUNCTION".to_string(),
        "integer".to_string(),
        "".to_string(),
        None,
        None,
        "SELECT 42;\n".to_string(),
    );

    let mut mat_view = View::new(
        "mv_data".to_string(),
        " SELECT public.helper() AS value;\n".to_string(),
        "public".to_string(),
        vec![],
    );
    mat_view.is_materialized = true;
    mat_view.hash();

    to_dump.routines.push(helper_fn);
    to_dump.views.push(mat_view);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();

    let pos_fn = script
        .find("create or replace function public.helper")
        .expect("helper function not found");
    let pos_mv = script.find("public.mv_data").expect("mv_data not found");

    assert!(
        pos_fn < pos_mv,
        "helper() must be created before mv_data (materialized view depends on function)"
    );
}

#[tokio::test]
async fn compare_drops_routines_in_reverse_dependency_order() {
    // Routine A calls Routine B; when both are dropped, A should be dropped first.
    let mut from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());

    let routine_b = Routine::new(
        "public".to_string(),
        Oid(1),
        "base_fn".to_string(),
        "sql".to_string(),
        "FUNCTION".to_string(),
        "integer".to_string(),
        "".to_string(),
        None,
        None,
        "SELECT 1;\n".to_string(),
    );

    let routine_a = Routine::new(
        "public".to_string(),
        Oid(2),
        "caller_fn".to_string(),
        "sql".to_string(),
        "FUNCTION".to_string(),
        "integer".to_string(),
        "".to_string(),
        None,
        None,
        "SELECT public.base_fn();\n".to_string(),
    );

    // Add in wrong order
    from_dump.routines.push(routine_b);
    from_dump.routines.push(routine_a);

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();

    let pos_caller = script
        .find("drop function if exists public.caller_fn")
        .expect("caller_fn drop not found");
    let pos_base = script
        .find("drop function if exists public.base_fn")
        .expect("base_fn drop not found");

    assert!(
        pos_caller < pos_base,
        "caller_fn (dependent) must be dropped before base_fn"
    );
}

#[tokio::test]
async fn compare_routines_overloaded_identical_no_diff() {
    // Two routines with the same (schema, name) but different arguments.
    // Both overloads are identical in FROM and TO → no output expected.
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let overload_short_from = Routine::new(
        "myschema".to_string(),
        Oid(1),
        "notify_event".to_string(),
        "plpgsql".to_string(),
        "PROCEDURE".to_string(),
        "void".to_string(),
        "pjobid uuid, peventtype character varying, pattributes jsonb".to_string(),
        None,
        None,
        "BEGIN\n    CALL myschema.notify_event(pjobid, peventtype, null, pattributes, null);\nEND;"
            .to_string(),
    );
    let overload_long_from = Routine::new(
        "myschema".to_string(),
        Oid(2),
        "notify_event".to_string(),
        "plpgsql".to_string(),
        "PROCEDURE".to_string(),
        "void".to_string(),
        "pjobid uuid, peventtype character varying, puserid character varying, pattributes jsonb, psessionseed jsonb DEFAULT NULL::jsonb".to_string(),
        None,
        None,
        "BEGIN\n    RAISE NOTICE 'notify';\nEND;".to_string(),
    );

    let overload_short_to = Routine::new(
        "myschema".to_string(),
        Oid(1),
        "notify_event".to_string(),
        "plpgsql".to_string(),
        "PROCEDURE".to_string(),
        "void".to_string(),
        "pjobid uuid, peventtype character varying, pattributes jsonb".to_string(),
        None,
        None,
        "BEGIN\n    CALL myschema.notify_event(pjobid, peventtype, null, pattributes, null);\nEND;"
            .to_string(),
    );
    let overload_long_to = Routine::new(
        "myschema".to_string(),
        Oid(2),
        "notify_event".to_string(),
        "plpgsql".to_string(),
        "PROCEDURE".to_string(),
        "void".to_string(),
        "pjobid uuid, peventtype character varying, puserid character varying, pattributes jsonb, psessionseed jsonb DEFAULT NULL::jsonb".to_string(),
        None,
        None,
        "BEGIN\n    RAISE NOTICE 'notify';\nEND;".to_string(),
    );

    from_dump.routines.push(overload_short_from);
    from_dump.routines.push(overload_long_from);
    to_dump.routines.push(overload_short_to);
    to_dump.routines.push(overload_long_to);

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_routines().await.unwrap();
    let script = comparer.get_script();

    assert!(
        !script.contains("create or replace"),
        "Identical overloads must not produce CREATE, got: {script}"
    );
    assert!(
        !script.contains("drop procedure"),
        "Identical overloads must not produce DROP, got: {script}"
    );
}

#[tokio::test]
async fn compare_routines_overloaded_one_changed() {
    // Two overloads with the same (schema, name). Only the long overload
    // changes its body between FROM and TO. The short overload must remain
    // untouched while the long one is recreated.
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let overload_short_from = Routine::new(
        "myschema".to_string(),
        Oid(1),
        "notify_event".to_string(),
        "plpgsql".to_string(),
        "PROCEDURE".to_string(),
        "void".to_string(),
        "pjobid uuid, peventtype character varying, pattributes jsonb".to_string(),
        None,
        None,
        "BEGIN\n    CALL myschema.notify_event(pjobid, peventtype, null, pattributes, null);\nEND;"
            .to_string(),
    );
    let overload_long_from = Routine::new(
        "myschema".to_string(),
        Oid(2),
        "notify_event".to_string(),
        "plpgsql".to_string(),
        "PROCEDURE".to_string(),
        "void".to_string(),
        "pjobid uuid, peventtype character varying, puserid character varying, pattributes jsonb, psessionseed jsonb DEFAULT NULL::jsonb".to_string(),
        None,
        None,
        "BEGIN\n    RAISE NOTICE 'old body';\nEND;".to_string(),
    );

    // Short overload is identical to FROM
    let overload_short_to = Routine::new(
        "myschema".to_string(),
        Oid(1),
        "notify_event".to_string(),
        "plpgsql".to_string(),
        "PROCEDURE".to_string(),
        "void".to_string(),
        "pjobid uuid, peventtype character varying, pattributes jsonb".to_string(),
        None,
        None,
        "BEGIN\n    CALL myschema.notify_event(pjobid, peventtype, null, pattributes, null);\nEND;"
            .to_string(),
    );
    // Long overload has a different body → should be recreated
    let overload_long_to = Routine::new(
        "myschema".to_string(),
        Oid(2),
        "notify_event".to_string(),
        "plpgsql".to_string(),
        "PROCEDURE".to_string(),
        "void".to_string(),
        "pjobid uuid, peventtype character varying, puserid character varying, pattributes jsonb, psessionseed jsonb DEFAULT NULL::jsonb".to_string(),
        None,
        None,
        "BEGIN\n    RAISE NOTICE 'new body';\nEND;".to_string(),
    );

    from_dump.routines.push(overload_short_from);
    from_dump.routines.push(overload_long_from);
    to_dump.routines.push(overload_short_to);
    to_dump.routines.push(overload_long_to);

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_routines().await.unwrap();
    let script = comparer.get_script();

    // The changed (long) overload must be recreated
    assert!(
        script.contains("create or replace procedure myschema.notify_event(pjobid uuid, peventtype character varying, puserid character varying, pattributes jsonb, psessionseed jsonb DEFAULT NULL::jsonb)"),
        "Changed overload must be recreated, got: {script}"
    );
    // The short overload's signature must NOT appear in any CREATE statement
    let short_create = "create or replace procedure myschema.notify_event(pjobid uuid, peventtype character varying, pattributes jsonb)";
    assert!(
        !script.contains(short_create),
        "Unchanged overload must not be recreated, got: {script}"
    );
}

#[tokio::test]
async fn compare_routines_overloaded_drop_only_removed_overload() {
    // FROM has two overloads; TO has only the short one.
    // Only the long overload must be dropped; the short one must stay.
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let overload_short = Routine::new(
        "myschema".to_string(),
        Oid(1),
        "notify_event".to_string(),
        "plpgsql".to_string(),
        "PROCEDURE".to_string(),
        "void".to_string(),
        "pjobid uuid, pattributes jsonb".to_string(),
        None,
        None,
        "BEGIN\n    RAISE NOTICE 'short';\nEND;".to_string(),
    );
    let overload_long = Routine::new(
        "myschema".to_string(),
        Oid(2),
        "notify_event".to_string(),
        "plpgsql".to_string(),
        "PROCEDURE".to_string(),
        "void".to_string(),
        "pjobid uuid, pattributes jsonb, pseed jsonb".to_string(),
        None,
        None,
        "BEGIN\n    RAISE NOTICE 'long';\nEND;".to_string(),
    );

    from_dump.routines.push(overload_short.clone());
    from_dump.routines.push(overload_long);
    to_dump.routines.push(overload_short);

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_routines().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("drop procedure if exists myschema.notify_event (pjobid uuid, pattributes jsonb, pseed jsonb) cascade;"),
        "Removed overload must be dropped, got: {script}"
    );
    assert!(
        !script.contains("create or replace"),
        "Unchanged overload must not be recreated, got: {script}"
    );
}

#[tokio::test]
async fn compare_routines_procedure_with_config_params() {
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut routine = Routine::new(
        "public".to_string(),
        Oid(1),
        "test_proc".to_string(),
        "plpgsql".to_string(),
        "PROCEDURE".to_string(),
        "void".to_string(),
        "IN pvalue text".to_string(),
        None,
        None,
        "\nBEGIN\n    RAISE NOTICE 'value: %', pvalue;\nEND;\n".to_string(),
    );
    routine.config = vec![
        "search_path=public, pg_temp".to_string(),
        "lock_timeout=5s".to_string(),
    ];
    routine.hash();
    to_dump.routines.push(routine);

    let from_dump = Dump::new(DumpConfig::default());
    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Ignore);
    comparer.compare_routines().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("SET search_path = public, pg_temp"),
        "script must contain SET search_path, got:\n{}",
        script
    );
    assert!(
        script.contains("SET lock_timeout = '5s'"),
        "script must contain SET lock_timeout, got:\n{}",
        script
    );
}

#[tokio::test]
async fn compare_routines_function_with_config_params() {
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut routine = Routine::new(
        "public".to_string(),
        Oid(1),
        "my_func".to_string(),
        "plpgsql".to_string(),
        "FUNCTION".to_string(),
        "integer".to_string(),
        "".to_string(),
        None,
        None,
        "\nBEGIN\n    RETURN 1;\nEND;\n".to_string(),
    );
    routine.config = vec!["work_mem=256MB".to_string()];
    routine.hash();
    to_dump.routines.push(routine);

    let from_dump = Dump::new(DumpConfig::default());
    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Ignore);
    comparer.compare_routines().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("SET work_mem = '256MB'"),
        "script must contain SET work_mem, got:\n{}",
        script
    );
    assert!(
        script.contains("VOLATILE"),
        "function flags must still be present, got:\n{}",
        script
    );
}

#[tokio::test]
async fn compare_routines_config_change_triggers_update() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_routine = Routine::new(
        "public".to_string(),
        Oid(1),
        "test_func".to_string(),
        "plpgsql".to_string(),
        "FUNCTION".to_string(),
        "integer".to_string(),
        "".to_string(),
        None,
        None,
        "BEGIN RETURN 1; END".to_string(),
    );
    from_routine.config = vec!["search_path=public".to_string()];
    from_routine.hash();

    let mut to_routine = Routine::new(
        "public".to_string(),
        Oid(1),
        "test_func".to_string(),
        "plpgsql".to_string(),
        "FUNCTION".to_string(),
        "integer".to_string(),
        "".to_string(),
        None,
        None,
        "BEGIN RETURN 1; END".to_string(),
    );
    to_routine.config = vec![
        "search_path=public".to_string(),
        "lock_timeout=5s".to_string(),
    ];
    to_routine.hash();

    assert_ne!(
        from_routine.hash, to_routine.hash,
        "hashes must differ when config changes"
    );

    from_dump.routines.push(from_routine);
    to_dump.routines.push(to_routine);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Ignore);
    comparer.compare_routines().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("create or replace function"),
        "config change must trigger CREATE OR REPLACE, got:\n{}",
        script
    );
    assert!(
        script.contains("SET search_path = public"),
        "script must contain SET search_path, got:\n{}",
        script
    );
    assert!(
        script.contains("SET lock_timeout = '5s'"),
        "script must contain SET lock_timeout, got:\n{}",
        script
    );
}

#[tokio::test]
async fn compare_routines_config_removal_triggers_update() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_routine = Routine::new(
        "public".to_string(),
        Oid(1),
        "test_func".to_string(),
        "plpgsql".to_string(),
        "FUNCTION".to_string(),
        "integer".to_string(),
        "".to_string(),
        None,
        None,
        "BEGIN RETURN 1; END".to_string(),
    );
    from_routine.config = vec!["search_path=public".to_string()];
    from_routine.hash();

    let to_routine = Routine::new(
        "public".to_string(),
        Oid(1),
        "test_func".to_string(),
        "plpgsql".to_string(),
        "FUNCTION".to_string(),
        "integer".to_string(),
        "".to_string(),
        None,
        None,
        "BEGIN RETURN 1; END".to_string(),
    );

    assert_ne!(
        from_routine.hash, to_routine.hash,
        "hashes must differ when config is removed"
    );

    from_dump.routines.push(from_routine);
    to_dump.routines.push(to_routine);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Ignore);
    comparer.compare_routines().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("create or replace function"),
        "config removal must trigger CREATE OR REPLACE, got:\n{}",
        script
    );
    assert!(
        !script.contains("SET search_path"),
        "removed config must not appear in script, got:\n{}",
        script
    );
}

// ── issue #240: a mention is not a call ────────────────────────────────
// The dependency graph is built by reading qualified names out of a
// routine's source text. Text cannot tell a call from a name written in
// prose, so before the fix a comment naming another routine added an edge
// — and one invented edge pointing back along a real one is a cycle. Kahn's
// sort cannot order a cycle: it appends the whole blocked subgraph in plain
// name order, which is how a script came to create a routine before the
// routine it calls.

fn issue240_sql_fn(oid: u32, name: &str, body: &str) -> Routine {
    Routine::new(
        "test_schema".to_string(),
        Oid(oid),
        name.to_string(),
        "sql".to_string(),
        "FUNCTION".to_string(),
        "text".to_string(),
        "pvalue text".to_string(),
        None,
        None,
        body.to_string(),
    )
}

/// Position of a routine's `create` in the script, or a failure naming it.
fn create_pos(script: &str, name: &str) -> usize {
    script
        .find(&format!(
            "create or replace function test_schema.{name}(pvalue text)"
        ))
        .unwrap_or_else(|| panic!("{name} was never created:\n{script}"))
}

#[tokio::test]
async fn compare_routines_and_views_ignores_a_routine_named_only_in_a_comment() {
    let from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    // util_a is called by helper_d, and names helper_d back in a comment.
    // The real edge is helper_d -> util_a; the comment must not add the
    // reverse one.
    to_dump.routines.push(issue240_sql_fn(
        1,
        "util_a",
        "\n\t-- NB: mirrors test_schema.helper_d's normalization rules, keep them in sync.\n\
         \tselect translate(coalesce(pvalue, ''), 'ao', 'AO');\n",
    ));
    to_dump.routines.push(issue240_sql_fn(
        2,
        "helper_d",
        "\n\tselect regexp_replace(test_schema.util_a(pvalue), '[^A-Za-z -]', '', 'g');\n",
    ));
    // A third routine downstream of the pair. Before the fix this one was
    // the visible symptom: blocked behind the cycle, it was appended in name
    // order and so came out FIRST, ahead of everything it calls.
    to_dump.routines.push(issue240_sql_fn(
        3,
        "a_caller",
        "\n\tselect test_schema.helper_d(pvalue);\n",
    ));

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Ignore);
    comparer.compare_routines_and_views().await.unwrap();
    let script = comparer.get_script();

    let util_a = create_pos(&script, "util_a");
    let helper_d = create_pos(&script, "helper_d");
    let a_caller = create_pos(&script, "a_caller");

    assert!(
        util_a < helper_d,
        "util_a is called by helper_d and must be created first:\n{script}"
    );
    assert!(
        helper_d < a_caller,
        "helper_d is called by a_caller and must be created first:\n{script}"
    );
}

#[tokio::test]
async fn compare_routines_and_views_ignores_a_routine_named_only_in_a_literal() {
    // Same defect through the other door: a name inside dynamic SQL. The
    // routine is not resolved when the caller is created, so it is not a
    // creation-order dependency — and treating it as one closes the same
    // false loop.
    let from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    to_dump.routines.push(issue240_sql_fn(
        1,
        "util_a",
        "\n\tselect coalesce(pvalue, 'test_schema.helper_d');\n",
    ));
    to_dump.routines.push(issue240_sql_fn(
        2,
        "helper_d",
        "\n\tselect test_schema.util_a(pvalue);\n",
    ));
    to_dump.routines.push(issue240_sql_fn(
        3,
        "a_caller",
        "\n\tselect test_schema.helper_d(pvalue);\n",
    ));

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Ignore);
    comparer.compare_routines_and_views().await.unwrap();
    let script = comparer.get_script();

    assert!(
        create_pos(&script, "util_a") < create_pos(&script, "helper_d"),
        "util_a is called by helper_d and must be created first:\n{script}"
    );
    assert!(
        create_pos(&script, "helper_d") < create_pos(&script, "a_caller"),
        "helper_d is called by a_caller and must be created first:\n{script}"
    );
}

#[tokio::test]
async fn compare_routines_ignores_a_routine_named_only_in_a_comment() {
    // `compare_routines` runs the same scan over the same bodies through a
    // different entry point, and had the same bug.
    let from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    to_dump.routines.push(issue240_sql_fn(
        1,
        "util_a",
        "\n\t/* paired with test_schema.helper_d */\n\tselect pvalue;\n",
    ));
    to_dump.routines.push(issue240_sql_fn(
        2,
        "helper_d",
        "\n\tselect test_schema.util_a(pvalue);\n",
    ));
    to_dump.routines.push(issue240_sql_fn(
        3,
        "a_caller",
        "\n\tselect test_schema.helper_d(pvalue);\n",
    ));

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Ignore);
    comparer.compare_routines().await.unwrap();
    let script = comparer.get_script();

    assert!(
        create_pos(&script, "util_a") < create_pos(&script, "helper_d"),
        "util_a is called by helper_d and must be created first:\n{script}"
    );
    assert!(
        create_pos(&script, "helper_d") < create_pos(&script, "a_caller"),
        "helper_d is called by a_caller and must be created first:\n{script}"
    );
}

#[tokio::test]
async fn a_real_call_inside_a_body_is_still_a_dependency() {
    // The guard against over-blanking: only comments and literals go. A
    // plain call still orders the pair, and a body with nothing to blank
    // must be scanned exactly as before.
    let from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    to_dump
        .routines
        .push(issue240_sql_fn(1, "z_callee", "\n\tselect pvalue;\n"));
    to_dump.routines.push(issue240_sql_fn(
        2,
        "a_caller",
        "\n\tselect test_schema.z_callee(pvalue);\n",
    ));

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Ignore);
    comparer.compare_routines_and_views().await.unwrap();
    let script = comparer.get_script();

    assert!(
        create_pos(&script, "z_callee") < create_pos(&script, "a_caller"),
        "the call is real, so z_callee must still be created first:\n{script}"
    );
}

#[tokio::test]
async fn a_quoted_identifier_in_a_body_is_still_a_dependency() {
    // Double quotes are the one quoted form that survives blanking, because
    // a quoted identifier is a reference rather than a literal.
    let from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    to_dump.routines.push(Routine::new(
        "test_schema".to_string(),
        Oid(1),
        "\"ZCallee\"".to_string(),
        "sql".to_string(),
        "FUNCTION".to_string(),
        "text".to_string(),
        "pvalue text".to_string(),
        None,
        None,
        "\n\tselect pvalue;\n".to_string(),
    ));
    to_dump.routines.push(issue240_sql_fn(
        2,
        "a_caller",
        "\n\tselect test_schema.\"ZCallee\"(pvalue);\n",
    ));

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Ignore);
    comparer.compare_routines_and_views().await.unwrap();
    let script = comparer.get_script();

    let callee = script
        .find("create or replace function test_schema.\"ZCallee\"(pvalue text)")
        .expect("ZCallee was never created");
    assert!(
        callee < create_pos(&script, "a_caller"),
        "the quoted call is real, so \"ZCallee\" must be created first:\n{script}"
    );
}

// ---- SQL-standard bodies (BEGIN ATOMIC … END / RETURN expr, PG14+) ----
//
// pgc 1.0.x read only `prosrc`, which such a routine leaves empty: the body was
// dumped as '' (so a body change never showed up in the hash) and replayed as
// `as $$$$`, which fails for a function and turns a procedure into a no-op.

/// `test_schema.<name>(p integer)`, LANGUAGE sql. `sql_body` says whether
/// `body` is a SQL-standard body (as `pg_get_function_sqlbody` deparses it) or
/// a string body (`prosrc`).
fn sqlbody_routine(kind: &str, name: &str, body: &str, sql_body: bool) -> Routine {
    let return_type = if kind == "procedure" {
        "void"
    } else {
        "integer"
    };
    let mut routine = Routine::new(
        "test_schema".to_string(),
        Oid(700),
        name.to_string(),
        "sql".to_string(),
        kind.to_string(),
        return_type.to_string(),
        "p integer".to_string(),
        None,
        None,
        String::new(),
    );
    routine.source_code = body.to_string();
    routine.is_sql_body = sql_body;
    routine.hash();
    routine
}

#[tokio::test]
async fn compare_routines_replaces_a_changed_sql_standard_body_in_place() {
    let from = sqlbody_routine(
        "function",
        "pg19_sqlbody_upsert",
        "BEGIN ATOMIC\n INSERT INTO test_schema.t (id)\n   VALUES (pg19_sqlbody_upsert.p) ON CONFLICT(id) DO NOTHING\n   RETURNING t.id;\nEND",
        true,
    );
    let to = sqlbody_routine(
        "function",
        "pg19_sqlbody_upsert",
        "BEGIN ATOMIC\n INSERT INTO test_schema.t (id)\n   VALUES (pg19_sqlbody_upsert.p) ON CONFLICT(id) DO SELECT FOR UPDATE\n   RETURNING t.id;\nEND",
        true,
    );

    let script = compare_one_routine(Some(from), to.clone()).await;
    assert!(
        script.contains(&format!(
            "create or replace function test_schema.pg19_sqlbody_upsert(p integer) returns integer \
             language sql VOLATILE PARALLEL UNSAFE {};",
            to.source_code
        )),
        "{script}"
    );
    // No ALTER exists for a body, and no DROP is needed for one.
    assert!(!script.contains("drop function"), "{script}");
    assert!(!script.contains("$$"), "{script}");
}

#[tokio::test]
async fn compare_routines_leaves_an_unchanged_sql_standard_body_alone() {
    let routine = sqlbody_routine("function", "pg19_sqlbody_same", "RETURN (p + 1)", true);
    let script = compare_one_routine(Some(routine.clone()), routine).await;
    assert!(!script.contains("create or replace"), "{script}");
}

#[tokio::test]
async fn compare_routines_switches_between_string_and_sql_standard_bodies() {
    // CREATE OR REPLACE converts either form into the other.
    let string_body = sqlbody_routine("function", "pg19_sqlbody_switch", " SELECT p * 2 ", false);
    let sql_body = sqlbody_routine(
        "function",
        "pg19_sqlbody_switch",
        "BEGIN ATOMIC\n SELECT (pg19_sqlbody_switch.p * 2);\nEND",
        true,
    );

    let script = compare_one_routine(Some(string_body.clone()), sql_body.clone()).await;
    assert!(
        script.contains("PARALLEL UNSAFE BEGIN ATOMIC\n SELECT (pg19_sqlbody_switch.p * 2);\nEND;"),
        "{script}"
    );
    assert!(!script.contains("drop function"), "{script}");

    let script = compare_one_routine(Some(sql_body), string_body).await;
    assert!(
        script.contains("PARALLEL UNSAFE as $$ SELECT p * 2 $$;"),
        "{script}"
    );
    assert!(!script.contains("drop function"), "{script}");
}

#[tokio::test]
async fn compare_routines_repairs_a_sql_standard_body_a_1_0_x_dump_lost() {
    // A pgc 1.0.x FROM dump holds the routine with an empty string body. Its
    // hash differs from the one a current TO dump computes, so the migration
    // re-creates the routine with its real body — once.
    let legacy = sqlbody_routine("procedure", "pg19_sqlbody_proc", "", false);
    let current = sqlbody_routine(
        "procedure",
        "pg19_sqlbody_proc",
        "BEGIN ATOMIC\n DELETE FROM test_schema.t\n  WHERE (t.id = pg19_sqlbody_proc.p);\nEND",
        true,
    );

    let script = compare_one_routine(Some(legacy), current.clone()).await;
    assert!(
        script.contains(&format!(
            "create or replace procedure test_schema.pg19_sqlbody_proc(p integer) language sql {};",
            current.source_code
        )),
        "{script}"
    );
    assert!(!script.contains("drop procedure"), "{script}");
}

#[tokio::test]
async fn compare_routines_drops_a_from_only_sql_standard_body_procedure() {
    let from = sqlbody_routine(
        "procedure",
        "pg19_sqlbody_gone",
        "BEGIN ATOMIC\n SELECT 1;\nEND",
        true,
    );
    let mut from_dump = Dump::new(DumpConfig::default());
    from_dump.routines.push(from);
    let mut comparer = Comparer::new(
        from_dump,
        Dump::new(DumpConfig::default()),
        true,
        false,
        true,
        GrantsMode::Ignore,
    );
    comparer.compare_routines().await.unwrap();
    let script = comparer.get_script();
    assert!(
        script.contains(
            "drop procedure if exists test_schema.pg19_sqlbody_gone (p integer) cascade;"
        ),
        "{script}"
    );
}

#[tokio::test]
async fn compare_routines_and_views_creates_a_sql_standard_callee_first() {
    // A SQL-standard body is parsed at CREATE time whatever
    // check_function_bodies says, so a callee created later is a hard error.
    // The deparsed body names it qualified, and the dependency scan reads it.
    let from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());
    to_dump.routines.push(sqlbody_routine(
        "function",
        "pg19_sqlbody_a_caller",
        "RETURN (test_schema.pg19_sqlbody_z_callee(p) * 10)",
        true,
    ));
    to_dump.routines.push(sqlbody_routine(
        "function",
        "pg19_sqlbody_z_callee",
        "RETURN (p - 1)",
        true,
    ));

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Ignore);
    comparer.compare_routines_and_views().await.unwrap();
    let script = comparer.get_script();

    let pos = |name: &str| {
        script
            .find(&format!(
                "create or replace function test_schema.{name}(p integer)"
            ))
            .unwrap_or_else(|| panic!("{name} was never created:\n{script}"))
    };
    assert!(
        pos("pg19_sqlbody_z_callee") < pos("pg19_sqlbody_a_caller"),
        "{script}"
    );
}

// ---- PG19: aggregate modify path, kind changes, SET <list GUC> TO NULL ----

/// `test_schema.pg19_agg(bigint)`, a count-shaped aggregate. `kind` / `num_direct_args`
/// feed `AggregateInfo` so the ordered-set / hypothetical-set cases can share it.
fn pg19_aggregate(arguments: &str, kind: char, num_direct_args: i16) -> Routine {
    let mut routine = Routine::new(
        "test_schema".to_string(),
        Oid(1),
        "pg19_agg".to_string(),
        "internal".to_string(),
        "aggregate".to_string(),
        "bigint".to_string(),
        arguments.to_string(),
        None,
        None,
        "aggregate_dummy".to_string(),
    );
    routine.aggregate_info = Some(AggregateInfo {
        sfunc: "int8inc_any".to_string(),
        stype: "bigint".to_string(),
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
        kind,
        num_direct_args,
    });
    routine.hash();
    routine
}

fn pg19_function(name: &str, kind: &str, return_type: &str, arguments: &str) -> Routine {
    Routine::new(
        "test_schema".to_string(),
        Oid(2),
        name.to_string(),
        "sql".to_string(),
        kind.to_string(),
        return_type.to_string(),
        arguments.to_string(),
        None,
        None,
        "select 1".to_string(),
    )
}

async fn compare_one_routine(from: Option<Routine>, to: Routine) -> String {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());
    from_dump.routines.extend(from);
    to_dump.routines.push(to);
    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_routines().await.unwrap();
    comparer.get_script()
}

#[tokio::test]
async fn compare_routines_updates_aggregate_support_in_place() {
    // PG19 SUPPORT can only be changed by CREATE OR REPLACE AGGREGATE: there is no
    // ALTER form. The identity is unchanged, so no DROP (it would cascade to views).
    let from = pg19_aggregate("bigint", 'n', 0);
    let mut to = pg19_aggregate("bigint", 'n', 0);
    to.support_function = Some("int8inc_support".to_string());
    to.hash();

    let script = compare_one_routine(Some(from.clone()), to.clone()).await;
    assert!(
        script.contains(
            "create or replace aggregate test_schema.pg19_agg(bigint) (\n    SFUNC = int8inc_any,\n    STYPE = bigint,\n    INITCOND = '0',\n    SUPPORT = int8inc_support\n);"
        ),
        "{script}"
    );
    assert!(!script.contains("drop aggregate"), "{script}");

    // Removing it is the same statement without the option.
    let script = compare_one_routine(Some(to), from).await;
    assert!(
        script.contains("create or replace aggregate test_schema.pg19_agg(bigint)"),
        "{script}"
    );
    assert!(!script.contains("SUPPORT"), "{script}");
    assert!(!script.contains("drop aggregate"), "{script}");
}

#[tokio::test]
async fn compare_routines_updates_aggregate_state_space_and_parallel_in_place() {
    let from = pg19_aggregate("bigint", 'n', 0);
    let mut to = pg19_aggregate("bigint", 'n', 0);
    to.aggregate_info.as_mut().unwrap().sspace = Some(-1);
    to.parallel = "safe".to_string();
    to.hash();
    assert_ne!(
        from.hash, to.hash,
        "SSPACE = -1 must be visible to the hash"
    );

    let script = compare_one_routine(Some(from), to).await;
    assert!(script.contains("    SSPACE = -1,\n"), "{script}");
    assert!(script.contains("    PARALLEL = SAFE\n);"), "{script}");
    assert!(
        script.contains("create or replace aggregate test_schema.pg19_agg(bigint)"),
        "{script}"
    );
    assert!(!script.contains("drop aggregate"), "{script}");
}

#[tokio::test]
async fn compare_routines_drops_aggregate_whose_kind_changes() {
    // Ordered-set -> hypothetical-set keeps the identity arguments, but
    // CREATE OR REPLACE refuses it ("cannot change routine kind").
    let args = "VARIADIC \"any\" ORDER BY VARIADIC \"any\"";
    let from = pg19_aggregate(args, 'o', 1);
    let to = pg19_aggregate(args, 'h', 1);

    let script = compare_one_routine(Some(from), to).await;
    let drop_pos = script
        .find("drop aggregate if exists test_schema.pg19_agg (VARIADIC \"any\" ORDER BY VARIADIC \"any\") cascade;")
        .unwrap_or_else(|| panic!("aggregate kind change must drop first:\n{script}"));
    let create_pos = script
        .find("create or replace aggregate test_schema.pg19_agg(")
        .expect("aggregate must be recreated");
    assert!(drop_pos < create_pos, "{script}");
}

#[tokio::test]
async fn compare_routines_drops_function_that_becomes_an_aggregate() {
    let from = pg19_function("pg19_agg", "function", "bigint", "bigint");
    let to = pg19_aggregate("bigint", 'n', 0);

    let script = compare_one_routine(Some(from), to).await;
    // DROP ROUTINE, so a replay (where it is already an aggregate) still works.
    let drop_pos = script
        .find("drop routine if exists test_schema.pg19_agg (bigint) cascade;")
        .unwrap_or_else(|| panic!("routine kind change must drop first:\n{script}"));
    let create_pos = script
        .find("create or replace aggregate test_schema.pg19_agg(bigint)")
        .expect("aggregate must be created");
    assert!(drop_pos < create_pos, "{script}");
}

#[tokio::test]
async fn compare_routines_drops_function_that_becomes_a_procedure() {
    // Same return type ("void") and arguments, different prokind.
    let from = pg19_function("pg19_proc", "function", "void", "");
    let to = pg19_function("pg19_proc", "procedure", "void", "");

    let script = compare_one_routine(Some(from), to).await;
    let drop_pos = script
        .find("drop routine if exists test_schema.pg19_proc () cascade;")
        .unwrap_or_else(|| panic!("routine kind change must drop first:\n{script}"));
    let create_pos = script
        .find("create or replace procedure test_schema.pg19_proc()")
        .expect("procedure must be created");
    assert!(drop_pos < create_pos, "{script}");
}

#[test]
fn routine_requires_drop_only_for_what_create_or_replace_refuses() {
    let agg = pg19_aggregate("bigint", 'n', 0);
    let mut changed = agg.clone();
    changed.support_function = Some("int8inc_support".to_string());
    changed.parallel = "safe".to_string();
    changed.aggregate_info.as_mut().unwrap().sspace = Some(-1);
    changed.aggregate_info.as_mut().unwrap().initcond = Some("1".to_string());
    assert!(!Comparer::routine_requires_drop(&agg, &changed));

    let args = "integer ORDER BY integer";
    assert!(Comparer::routine_requires_drop(
        &pg19_aggregate(args, 'o', 1),
        &pg19_aggregate(args, 'h', 1)
    ));
    assert!(Comparer::routine_requires_drop(
        &pg19_aggregate(args, 'o', 1),
        &pg19_aggregate(args, 'o', 0)
    ));

    let f = pg19_function("f", "function", "void", "");
    // The dump spells kinds in lower case; the comparison ignores case.
    assert!(!Comparer::routine_requires_drop(
        &f,
        &pg19_function("f", "FUNCTION", "void", "")
    ));
    assert!(Comparer::routine_requires_drop(
        &f,
        &pg19_function("f", "procedure", "void", "")
    ));
    assert!(Comparer::routine_requires_drop(
        &f,
        &pg19_function("f", "window", "void", "")
    ));
    assert!(Comparer::routine_requires_drop(
        &f,
        &pg19_function("f", "function", "integer", "")
    ));
    assert!(Comparer::routine_requires_drop(
        &f,
        &pg19_function("f", "function", "void", "a integer")
    ));
}

#[tokio::test]
async fn compare_routines_and_views_recreates_view_after_routine_kind_change() {
    // The kind-change DROP cascades exactly like a signature change, so Phase 7
    // must bring back the unchanged view that referenced the routine.
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    from_dump.routines.push(Routine::new(
        "test_deps".to_string(),
        Oid(900),
        "compute".to_string(),
        "sql".to_string(),
        "function".to_string(),
        "integer".to_string(),
        "x integer".to_string(),
        None,
        None,
        "SELECT x".to_string(),
    ));
    let mut aggregate = pg19_aggregate("x integer", 'n', 0);
    aggregate.schema = "test_deps".to_string();
    aggregate.name = "compute".to_string();
    aggregate.return_type = "integer".to_string();
    aggregate.hash();
    to_dump.routines.push(aggregate);

    from_dump.views.push(issue189_view("v_things", false));
    to_dump.views.push(issue189_view("v_things", false));

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_routines_and_views().await.unwrap();
    let script = comparer.get_script();

    let drop_pos = script
        .find("drop routine if exists test_deps.compute (x integer) cascade;")
        .unwrap_or_else(|| panic!("kind change must drop first:\n{script}"));
    let create_pos = script
        .find("create or replace aggregate test_deps.compute(x integer)")
        .expect("aggregate must be created");
    let view_pos = script
        .find("CREATE OR REPLACE VIEW test_deps.v_things")
        .unwrap_or_else(|| panic!("CASCADE-dropped view must be recreated:\n{script}"));
    assert!(drop_pos < create_pos && create_pos < view_pos, "{script}");
}

/// A TO-side function whose `proconfig` holds PG19's empty list.
fn empty_search_path_function() -> Routine {
    let mut routine = pg19_function("pg19_guc_null", "function", "integer", "");
    routine.config = vec!["search_path=".to_string()];
    routine.hash();
    routine
}

async fn compare_empty_list_against(from_version: Option<i32>) -> String {
    let mut from_dump = Dump::new(DumpConfig::default());
    from_dump.server_version_num = from_version;
    let mut to_dump = Dump::new(DumpConfig::default());
    to_dump.server_version_num = Some(190000);
    to_dump.routines.push(empty_search_path_function());
    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_routines().await.unwrap();
    let mut script = comparer.get_script();

    // compare_routines_and_views takes the same decision.
    let mut from_dump = Dump::new(DumpConfig::default());
    from_dump.server_version_num = from_version;
    let mut to_dump = Dump::new(DumpConfig::default());
    to_dump.routines.push(empty_search_path_function());
    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_routines_and_views().await.unwrap();
    script.push_str(&comparer.get_script());
    script
}

#[tokio::test]
async fn compare_routines_emits_set_to_null_only_for_a_pg19_migrated_server() {
    // The FROM database is the one migrated; the TO dump's version is irrelevant.
    let script = compare_empty_list_against(Some(190000)).await;
    assert_eq!(
        script.matches("SET search_path TO NULL").count(),
        2,
        "{script}"
    );
    assert!(!script.contains("SET search_path = ''"), "{script}");

    // Known pre-19: TO NULL is a syntax error there, so the portable form,
    // behind a note that names it.
    let script = compare_empty_list_against(Some(180006)).await;
    assert_eq!(
        script.matches("SET search_path = ''").count(),
        2,
        "{script}"
    );
    assert_eq!(
        script.matches("TO NULL").count(),
        script
            .matches("SET search_path TO NULL (an empty list) needs PostgreSQL 19")
            .count(),
        "TO NULL only inside the note: {script}"
    );

    // Unknown (a FROM dump written before server_version_num existed): the
    // portable form, byte-identical to pgc 1.0.x.
    let script = compare_empty_list_against(None).await;
    assert_eq!(
        script.matches("SET search_path = ''").count(),
        2,
        "{script}"
    );
    assert!(!script.contains("TO NULL"), "{script}");
}

/// The FROM routine as a pre-19 server holds it after the `= ''` fallback.
fn portable_search_path_function() -> Routine {
    let mut routine = empty_search_path_function();
    routine.config = vec!["search_path=\"\"".to_string()];
    routine.hash();
    routine
}

async fn compare_empty_list_from(from_version: Option<i32>, from: Option<Routine>) -> String {
    let mut from_dump = Dump::new(DumpConfig::default());
    from_dump.server_version_num = from_version;
    from_dump.routines.extend(from);
    let mut to_dump = Dump::new(DumpConfig::default());
    to_dump.server_version_num = Some(190000);
    to_dump.routines.push(empty_search_path_function());
    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    comparer.get_script()
}

#[tokio::test]
async fn empty_list_fallback_converges_on_a_pre_19_migrated_server() {
    let note = "SET search_path TO NULL (an empty list) needs PostgreSQL 19; the migrated server \
                (server_version_num 180006) gets the equivalent SET ... = '' instead";

    // FROM already holds what the fallback stores: only the note, every time.
    let script = compare_empty_list_from(Some(180006), Some(portable_search_path_function())).await;
    assert!(script.contains(note), "{script}");
    assert!(!script.contains("create or replace function"), "{script}");

    // FROM holds something else: the note and the fallback.
    let mut other = portable_search_path_function();
    other.config = vec!["search_path=public".to_string()];
    other.hash();
    let script = compare_empty_list_from(Some(180006), Some(other)).await;
    assert!(script.contains(note), "{script}");
    assert_eq!(
        script
            .matches("create or replace function test_schema.pg19_guc_null()")
            .count(),
        1,
        "{script}"
    );
    assert!(script.contains("SET search_path = ''"), "{script}");

    // New on FROM: the note and the fallback.
    let script = compare_empty_list_from(Some(180006), None).await;
    assert!(script.contains(note), "{script}");
    assert!(script.contains("SET search_path = ''"), "{script}");

    // A PostgreSQL 19 FROM stores the empty list itself: no note.
    let script = compare_empty_list_from(Some(190000), Some(portable_search_path_function())).await;
    assert!(!script.contains("needs PostgreSQL 19"), "{script}");
    assert!(script.contains("SET search_path TO NULL as"), "{script}");

    // Unknown version (a pgc 1.0.x FROM dump): the fallback as before, and
    // no note.
    let script = compare_empty_list_from(None, Some(portable_search_path_function())).await;
    assert!(!script.contains("needs PostgreSQL 19"), "{script}");
    assert!(script.contains("SET search_path = ''"), "{script}");
}

// ---- Privileges of a routine the migration drops and recreates ----

/// `routine` owned by `postgres`, with `acl` as its `proacl`.
fn owned_with_acl(mut routine: Routine, acl: &[&str]) -> Routine {
    routine.owner = "postgres".to_string();
    routine.acl = acl.iter().map(|a| a.to_string()).collect();
    routine.hash();
    routine
}

async fn compare_routine_grants(
    from: Routine,
    to: Routine,
    use_drop: bool,
    grants_mode: GrantsMode,
) -> String {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());
    from_dump.routines.push(from);
    to_dump.routines.push(to);
    let mut comparer = Comparer::new(from_dump, to_dump, use_drop, false, true, grants_mode);
    comparer.compare_grants().await.unwrap();
    comparer.get_script()
}

const GRANTED_TO_APP: [&str; 3] = ["=X/postgres", "postgres=X/postgres", "app=X/postgres"];

#[tokio::test]
async fn compare_grants_regrants_a_routine_recreated_for_a_kind_change() {
    // The DROP ROUTINE ... CASCADE takes the grant with it, so the unchanged ACL
    // must still be applied to the new aggregate (it used to take a second run).
    let from = owned_with_acl(
        pg19_function("pg19_agg", "function", "bigint", "bigint"),
        &GRANTED_TO_APP,
    );
    let to = owned_with_acl(pg19_aggregate("bigint", 'n', 0), &GRANTED_TO_APP);
    for mode in [GrantsMode::Full, GrantsMode::AddOnly] {
        let script = compare_routine_grants(from.clone(), to.clone(), true, mode).await;
        assert!(
            script.contains("GRANT EXECUTE ON FUNCTION test_schema.pg19_agg(bigint) TO app;"),
            "{script}"
        );
        // PUBLIC already has EXECUTE on a new routine.
        assert!(!script.contains("TO PUBLIC"), "{script}");
        assert!(!script.contains("REVOKE"), "{script}");
    }
}

#[tokio::test]
async fn compare_grants_revokes_public_from_a_recreated_routine() {
    // REVOKE EXECUTE ... FROM PUBLIC on both sides: the recreated routine starts
    // with PUBLIC's EXECUTE again, which full mode must take away.
    let revoked = ["postgres=X/postgres"];
    let from = owned_with_acl(
        pg19_function("pg19_rt", "function", "bigint", "bigint"),
        &revoked,
    );
    let to = owned_with_acl(
        pg19_function("pg19_rt", "function", "integer", "bigint"),
        &revoked,
    );
    let script = compare_routine_grants(from.clone(), to.clone(), true, GrantsMode::Full).await;
    assert!(
        script.contains("REVOKE EXECUTE ON FUNCTION test_schema.pg19_rt(bigint) FROM PUBLIC;"),
        "{script}"
    );
    assert!(!script.contains("GRANT EXECUTE"), "{script}");

    // Add-only mode never revokes.
    let script = compare_routine_grants(from, to, true, GrantsMode::AddOnly).await;
    assert!(!script.contains("REVOKE"), "{script}");
}

#[tokio::test]
async fn compare_grants_leaves_a_recreated_routine_with_a_default_acl_alone() {
    // TO's NULL proacl is exactly what the recreated routine gets: comparing
    // against the FROM grants would revoke PUBLIC's (and app's) EXECUTE from it.
    let from = owned_with_acl(
        pg19_function("pg19_rt", "function", "bigint", "bigint"),
        &GRANTED_TO_APP,
    );
    let to = owned_with_acl(
        pg19_function("pg19_rt", "function", "integer", "bigint"),
        &[],
    );
    let script = compare_routine_grants(from, to, true, GrantsMode::Full).await;
    assert!(!script.contains("REVOKE"), "{script}");
    assert!(!script.contains("GRANT"), "{script}");
}

#[tokio::test]
async fn compare_grants_keeps_the_from_acl_when_the_routine_is_not_dropped() {
    // With USE_DROP=false the DROP is commented out, so the routine keeps its
    // privileges; and a change CREATE OR REPLACE applies in place keeps them too.
    let from = owned_with_acl(
        pg19_function("pg19_rt", "function", "bigint", "bigint"),
        &GRANTED_TO_APP,
    );
    let to = owned_with_acl(
        pg19_function("pg19_rt", "function", "integer", "bigint"),
        &GRANTED_TO_APP,
    );
    let script = compare_routine_grants(from.clone(), to, false, GrantsMode::Full).await;
    assert!(!script.contains("GRANT EXECUTE"), "{script}");

    let mut body_only = from.clone();
    body_only.source_code = "select 2".to_string();
    body_only.hash();
    let script = compare_routine_grants(from, body_only, true, GrantsMode::Full).await;
    assert!(!script.contains("GRANT EXECUTE"), "{script}");
    assert!(!script.contains("REVOKE"), "{script}");
}

// ==== pre-clear tests ====

/// `compare_routines` (and, with `and_views`, `compare_routines_and_views`)
/// over one routine, with the FROM dump taken from a server whose
/// `server_version_num` is `from_version` (`None`: a pgc 1.0.x dump).
async fn compare_one_routine_from(
    from_version: Option<i32>,
    from: Option<Routine>,
    to: Routine,
    and_views: bool,
) -> String {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());
    from_dump.server_version_num = from_version;
    to_dump.server_version_num = Some(190000);
    from_dump.routines.extend(from);
    to_dump.routines.push(to);
    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    if and_views {
        comparer.compare_routines_and_views().await.unwrap();
    } else {
        comparer.compare_routines().await.unwrap();
    }
    comparer.get_script()
}

fn pg19_aggregate_with_support() -> Routine {
    let mut to = pg19_aggregate("bigint", 'n', 0);
    to.support_function = Some("int8inc_support".to_string());
    to.hash();
    to
}

const SUPPORT_NOTE: &str = "/* Aggregate test_schema.pg19_agg(bigint): SUPPORT = int8inc_support is left out. \
    It needs PostgreSQL 19, and the migrated server (server_version_num 180006) accepts it \
    with a WARNING but does not store it. */\n";

#[tokio::test]
async fn aggregate_support_alone_is_only_noted_for_a_server_older_than_19() {
    // PostgreSQL 14-18 take SUPPORT = … with a WARNING and store nothing, so
    // re-creating the aggregate would repeat on every compare.
    for and_views in [false, true] {
        let script = compare_one_routine_from(
            Some(180006),
            Some(pg19_aggregate("bigint", 'n', 0)),
            pg19_aggregate_with_support(),
            and_views,
        )
        .await;
        assert!(script.contains(SUPPORT_NOTE), "{script}");
        assert!(!script.contains("create or replace aggregate"), "{script}");
        assert!(!script.contains("    SUPPORT = "), "{script}");
    }
}

#[tokio::test]
async fn aggregate_support_is_left_out_of_an_aggregate_created_on_a_server_older_than_19() {
    for and_views in [false, true] {
        // A new aggregate …
        let script =
            compare_one_routine_from(Some(180006), None, pg19_aggregate_with_support(), and_views)
                .await;
        let note = script.find(SUPPORT_NOTE).expect(&script);
        let create = script
            .find(
                "create or replace aggregate test_schema.pg19_agg(bigint) (\n    SFUNC = int8inc_any,\n    STYPE = bigint,\n    INITCOND = '0'\n);",
            )
            .unwrap_or_else(|| panic!("{script}"));
        assert!(note < create, "{script}");
        assert!(!script.contains("    SUPPORT = "), "{script}");

        // … and one that changes otherwise too.
        let mut from = pg19_aggregate("bigint", 'n', 0);
        from.aggregate_info.as_mut().unwrap().initcond = Some("1".to_string());
        from.hash();
        let script = compare_one_routine_from(
            Some(180006),
            Some(from),
            pg19_aggregate_with_support(),
            and_views,
        )
        .await;
        assert!(script.contains(SUPPORT_NOTE), "{script}");
        assert!(
            script.contains("    INITCOND = '0'\n);"),
            "the other change is applied: {script}"
        );
        assert!(!script.contains("    SUPPORT = "), "{script}");
    }
}

#[tokio::test]
async fn aggregate_support_note_names_an_aggregate_over_star() {
    let mut from = pg19_aggregate("", 'n', 0);
    from.hash();
    let mut to = pg19_aggregate("", 'n', 0);
    to.support_function = Some("int8inc_support".to_string());
    to.hash();
    let script = compare_one_routine_from(Some(180006), Some(from), to, false).await;
    assert!(
        script.contains(
            "/* Aggregate test_schema.pg19_agg(*): SUPPORT = int8inc_support is left out."
        ),
        "{script}"
    );
}

#[tokio::test]
async fn aggregate_support_is_emitted_for_19_and_for_a_server_of_unknown_version() {
    // PostgreSQL 19 stores it; a pgc 1.0.x FROM dump carries no version, and
    // gets the statement exactly as before.
    for version in [Some(190000), None] {
        let script = compare_one_routine_from(
            version,
            Some(pg19_aggregate("bigint", 'n', 0)),
            pg19_aggregate_with_support(),
            false,
        )
        .await;
        assert!(
            script.contains("    SUPPORT = int8inc_support\n);"),
            "{version:?}: {script}"
        );
        assert!(!script.contains("is left out"), "{version:?}: {script}");
    }
}

#[tokio::test]
async fn an_aggregate_without_support_is_untouched_by_the_pre_19_rule() {
    let mut from = pg19_aggregate("bigint", 'n', 0);
    from.aggregate_info.as_mut().unwrap().initcond = Some("1".to_string());
    from.hash();
    let old = compare_one_routine_from(
        Some(180006),
        Some(from.clone()),
        pg19_aggregate("bigint", 'n', 0),
        false,
    )
    .await;
    let new = compare_one_routine_from(
        Some(190000),
        Some(from),
        pg19_aggregate("bigint", 'n', 0),
        false,
    )
    .await;
    assert_eq!(old, new);
    assert!(!old.contains("is left out"), "{old}");
}

#[test]
fn without_aggregate_support_strips_only_an_aggregates_support_function() {
    let agg = pg19_aggregate_with_support();
    let stripped = agg.without_aggregate_support().unwrap();
    assert_eq!(stripped.support_function, None);
    assert_eq!(stripped.hash, pg19_aggregate("bigint", 'n', 0).hash);
    assert!(
        pg19_aggregate("bigint", 'n', 0)
            .without_aggregate_support()
            .is_none()
    );
    // A plain function's SUPPORT is valid on every supported server.
    let mut function = pg19_function("f", "function", "bigint", "bigint");
    function.support_function = Some("int8inc_support".to_string());
    function.hash();
    assert!(function.without_aggregate_support().is_none());
}

#[test]
fn migrated_server_known_below_needs_a_known_older_version() {
    let comparer = |version: Option<i32>| {
        let mut from = Dump::new(DumpConfig::default());
        from.server_version_num = version;
        Comparer::new(
            from,
            Dump::new(DumpConfig::default()),
            true,
            false,
            true,
            GrantsMode::Ignore,
        )
    };
    assert_eq!(
        comparer(Some(180006)).migrated_server_known_below(190000),
        Some(180006)
    );
    assert_eq!(
        comparer(Some(190000)).migrated_server_known_below(190000),
        None
    );
    assert_eq!(comparer(None).migrated_server_known_below(190000), None);
}
