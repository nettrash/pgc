//! Tests for `compare_routines` and `compare_routines_and_views`: drop and
//! recreate rules, dependency ordering, overloads, owner and config changes.

use crate::comparer::core::*;
use super::helpers::*;
use crate::config::dump_config::DumpConfig;
use crate::config::grants_mode::GrantsMode;
use crate::dump::routine::Routine;
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
