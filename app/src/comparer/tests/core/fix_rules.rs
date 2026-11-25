//! Rules: how a changed rule is applied, where rule statements go in the
//! migration, and the rules the migration drops on the way and has to put back.
//!
//! Every case here was reproduced live (PostgreSQL 14 and 19) before the fix:
//! a changed rule was emitted as `CREATE RULE ...;;` and failed with "already
//! exists"; rules were created before the routines and views their actions or
//! relations need; a FROM-only rule blocked the `DROP TABLE` of a table it
//! wrote to; a rule reading a retyped column blocked the `ALTER COLUMN ...
//! TYPE`; and a rule on a recreated table or view, or calling a recreated
//! routine, was silently lost.

use crate::comparer::core::*;
use crate::config::dump_config::DumpConfig;
use crate::config::grants_mode::GrantsMode;
use crate::dump::foreign_table::{ForeignTable, ForeignTableColumn};
use crate::dump::routine::{Routine, SqlBodyDependencies};
use crate::dump::rule::{Rule, RuleDependencies};
use crate::dump::table::Table;
use crate::dump::table_column::TableColumn;
use crate::dump::view::View;
use sqlx::postgres::types::Oid;

use super::helpers::{int_column, issue179_compute_routine};

/// A rule as the dump holds it: `pg_get_ruledef` text, which starts with
/// `CREATE RULE` and ends with `;`.
fn rule(table: &str, name: &str, body: &str, depends_on: Option<RuleDependencies>) -> Rule {
    let mut rule = Rule::new(
        "repro".to_string(),
        table.to_string(),
        name.to_string(),
        format!("CREATE RULE {name} AS\n    {body};"),
        None,
    );
    rule.depends_on = depends_on;
    rule
}

fn deps(relations: &[&str], columns: &[&str], routines: &[&str]) -> Option<RuleDependencies> {
    Some(RuleDependencies {
        relations: relations.iter().map(|s| s.to_string()).collect(),
        columns: columns.iter().map(|s| s.to_string()).collect(),
        routines: routines.iter().map(|s| s.to_string()).collect(),
    })
}

fn table(name: &str, columns: Vec<TableColumn>) -> Table {
    let mut table = Table::new(
        "repro".to_string(),
        name.to_string(),
        "repro".to_string(),
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

fn bigint_column(table: &str, name: &str, ordinal: i32) -> TableColumn {
    let mut column = int_column("repro", table, name, ordinal);
    column.data_type = "bigint".to_string();
    column.udt_name = Some("int8".to_string());
    column.numeric_precision = Some(64);
    column
}

fn view(name: &str, relations: &[&str], columns: Option<&[&str]>) -> View {
    let mut view = View::new(
        name.to_string(),
        " SELECT id\n   FROM repro.t;".to_string(),
        "repro".to_string(),
        relations.iter().map(|s| s.to_string()).collect(),
    );
    view.column_relation = columns.map(|c| c.iter().map(|s| s.to_string()).collect());
    view.hash();
    view
}

fn empty() -> Dump {
    Dump::new(DumpConfig::default())
}

async fn compare(from: Dump, to: Dump) -> String {
    compare_with(from, to, true, false).await
}

async fn compare_with(from: Dump, to: Dump, use_drop: bool, production: bool) -> String {
    let mut comparer = Comparer::new(from, to, use_drop, false, true, GrantsMode::Ignore);
    comparer.set_output_for_production(production);
    comparer.compare().await.unwrap();
    comparer.get_script()
}

fn pos(script: &str, needle: &str) -> usize {
    script
        .find(needle)
        .unwrap_or_else(|| panic!("`{needle}` missing from:\n{script}"))
}

#[tokio::test]
async fn changed_rule_is_replaced_in_place() {
    let mut from = empty();
    let mut to = empty();
    from.tables
        .push(table("t", vec![int_column("repro", "t", "id", 1)]));
    to.tables
        .push(table("t", vec![int_column("repro", "t", "id", 1)]));
    from.rules.push(rule(
        "t",
        "r_chg",
        "ON DELETE TO repro.t DO INSTEAD NOTHING",
        deps(&["repro.t"], &[], &[]),
    ));
    to.rules.push(rule(
        "t",
        "r_chg",
        "ON UPDATE TO repro.t DO ALSO NOTHING",
        deps(&["repro.t"], &[], &[]),
    ));

    let script = compare(from, to).await;
    assert!(
        script.contains(
            "CREATE OR REPLACE RULE r_chg AS\n    ON UPDATE TO repro.t DO ALSO NOTHING;\n"
        ),
        "{script}"
    );
    assert!(!script.contains("CREATE RULE r_chg"), "{script}");
    assert!(!script.contains(";;"), "{script}");
    // Nothing had to be dropped ahead of the change.
    assert!(!script.contains("drop rule"), "{script}");
    assert!(!script.contains("Rules DROP"), "{script}");
}

#[tokio::test]
async fn unchanged_rule_emits_nothing() {
    let mut from = empty();
    let mut to = empty();
    for dump in [&mut from, &mut to] {
        dump.tables
            .push(table("t", vec![int_column("repro", "t", "id", 1)]));
        dump.rules.push(rule(
            "t",
            "r_same",
            "ON DELETE TO repro.t DO INSTEAD NOTHING",
            deps(&["repro.t"], &[], &[]),
        ));
    }

    let script = compare(from, to).await;
    assert!(!script.contains("RULE"), "{script}");
    assert!(!script.contains("drop rule"), "{script}");
}

#[tokio::test]
async fn rules_are_created_after_routines_views_operators_and_casts() {
    // A new rule on a new view, calling a new function: both exist only once
    // the Routines & Views section has run.
    let mut from = empty();
    let mut to = empty();
    for dump in [&mut from, &mut to] {
        dump.tables
            .push(table("t", vec![int_column("repro", "t", "id", 1)]));
    }
    to.views
        .push(view("v", &["repro.t"], Some(&["repro.t.id"])));
    to.rules.push(rule(
        "v",
        "v_ins",
        "ON INSERT TO repro.v DO INSTEAD  INSERT INTO repro.t (id)\n  VALUES (new.id)",
        deps(&["repro.t", "repro.v"], &["repro.t.id"], &[]),
    ));

    let script = compare(from, to).await;
    let view_create = pos(&script, "CREATE OR REPLACE VIEW repro.v as");
    let routines_end = pos(&script, "/* ---> Routines & Views: End section");
    let casts = pos(&script, "/* ---> Compare Casts");
    let rules_header = pos(&script, "/* ---> Compare Rules");
    let rule_create = pos(&script, "CREATE RULE v_ins AS");
    assert!(view_create < routines_end, "{script}");
    assert!(casts < rules_header, "{script}");
    assert!(rules_header < rule_create, "{script}");
    assert!(
        rule_create < pos(&script, "/* ---> Compare Event Triggers"),
        "{script}"
    );
    assert!(!script.contains(";;"), "{script}");
}

#[tokio::test]
async fn removed_rule_is_dropped_before_the_table_it_writes_to() {
    // DROP TABLE runs without CASCADE, so a rule writing to the table blocks
    // it until the rule is gone.
    let mut from = empty();
    let mut to = empty();
    for dump in [&mut from, &mut to] {
        dump.tables
            .push(table("t", vec![int_column("repro", "t", "id", 1)]));
    }
    from.tables
        .push(table("audit", vec![int_column("repro", "audit", "id", 1)]));
    from.rules.push(rule(
        "t",
        "to_audit",
        "ON DELETE TO repro.t DO  INSERT INTO repro.audit (id)\n  VALUES (old.id)",
        deps(
            &["repro.audit", "repro.t"],
            &["repro.audit.id", "repro.t.id"],
            &[],
        ),
    ));

    let script = compare(from, to).await;
    let rules_drop = pos(&script, "/* ---> Rules DROP: Start section");
    let drop_rule = pos(
        &script,
        "drop rule if exists \"to_audit\" on repro.\"t\" cascade;",
    );
    let views_drop = pos(&script, "/* ---> Views DROP: Start section");
    let drop_table = pos(&script, "drop table if exists repro.audit;");
    assert!(rules_drop < drop_rule, "{script}");
    assert!(drop_rule < views_drop, "{script}");
    assert!(views_drop < drop_table, "{script}");
    // Dropped once, and never re-created.
    assert_eq!(script.matches("drop rule").count(), 1, "{script}");
    assert!(!script.contains("RULE to_audit"), "{script}");
}

#[tokio::test]
async fn removed_rules_are_dropped_in_dump_order() {
    let mut from = empty();
    let mut to = empty();
    for dump in [&mut from, &mut to] {
        dump.tables
            .push(table("t", vec![int_column("repro", "t", "id", 1)]));
    }
    for name in ["r_a", "r_b", "r_c", "r_d"] {
        from.rules.push(rule(
            "t",
            name,
            "ON DELETE TO repro.t DO INSTEAD NOTHING",
            None,
        ));
    }

    let script = compare(from, to).await;
    let positions: Vec<usize> = ["r_a", "r_b", "r_c", "r_d"]
        .iter()
        .map(|n| pos(&script, &format!("drop rule if exists \"{n}\"")))
        .collect();
    assert!(positions.windows(2).all(|w| w[0] < w[1]), "{script}");
}

#[tokio::test]
async fn removed_rule_drop_is_commented_out_without_use_drop() {
    let mut from = empty();
    let mut to = empty();
    for dump in [&mut from, &mut to] {
        dump.tables
            .push(table("t", vec![int_column("repro", "t", "id", 1)]));
    }
    from.rules.push(rule(
        "t",
        "r_gone",
        "ON DELETE TO repro.t DO INSTEAD NOTHING",
        None,
    ));

    let script = compare_with(from, to, false, false).await;
    assert!(
        script.contains("-- drop rule if exists \"r_gone\" on repro.\"t\" cascade;"),
        "{script}"
    );
    assert!(
        !script.contains("\ndrop rule if exists \"r_gone\""),
        "{script}"
    );
}

#[tokio::test]
async fn rule_reading_a_retyped_column_is_dropped_first_and_recreated() {
    // `ALTER COLUMN ... TYPE` is refused while a rule depends on the column,
    // even though the rule's definition does not change.
    let mut from = empty();
    let mut to = empty();
    from.tables.push(table(
        "t",
        vec![
            int_column("repro", "t", "id", 1),
            int_column("repro", "t", "qty", 2),
        ],
    ));
    to.tables.push(table(
        "t",
        vec![
            int_column("repro", "t", "id", 1),
            bigint_column("t", "qty", 2),
        ],
    ));
    for dump in [&mut from, &mut to] {
        dump.rules.push(rule(
            "t",
            "r_qty",
            "ON UPDATE TO repro.t\n   WHERE new.qty < 0 DO INSTEAD NOTHING",
            deps(&["repro.t"], &["repro.t.qty"], &[]),
        ));
    }

    let script = compare(from, to).await;
    let drop_rule = pos(
        &script,
        "drop rule if exists \"r_qty\" on repro.\"t\" cascade;",
    );
    let retype = pos(&script, "alter column qty type bigint");
    let recreate = pos(&script, "CREATE OR REPLACE RULE r_qty AS");
    assert!(drop_rule < retype, "{script}");
    assert!(retype < recreate, "{script}");
}

#[tokio::test]
async fn rule_reading_an_unchanged_column_is_left_alone() {
    // Only the columns PostgreSQL would refuse to drop or retype matter:
    // retyping `qty` does not touch a rule that reads only `id`.
    let mut from = empty();
    let mut to = empty();
    from.tables.push(table(
        "t",
        vec![
            int_column("repro", "t", "id", 1),
            int_column("repro", "t", "qty", 2),
        ],
    ));
    to.tables.push(table(
        "t",
        vec![
            int_column("repro", "t", "id", 1),
            bigint_column("t", "qty", 2),
        ],
    ));
    for dump in [&mut from, &mut to] {
        dump.rules.push(rule(
            "t",
            "r_id",
            "ON UPDATE TO repro.t\n   WHERE new.id < 0 DO INSTEAD NOTHING",
            deps(&["repro.t"], &["repro.t.id"], &[]),
        ));
    }

    let script = compare(from, to).await;
    assert!(script.contains("alter column qty type bigint"), "{script}");
    assert!(!script.contains("r_id"), "{script}");
}

#[tokio::test]
async fn rule_from_a_dump_without_dependencies_is_not_dropped_early() {
    // A 1.0.x dump does not know what the rule reads. It keeps the old
    // behaviour rather than guessing.
    let mut from = empty();
    let mut to = empty();
    from.tables.push(table(
        "t",
        vec![
            int_column("repro", "t", "id", 1),
            int_column("repro", "t", "qty", 2),
        ],
    ));
    to.tables.push(table(
        "t",
        vec![
            int_column("repro", "t", "id", 1),
            bigint_column("t", "qty", 2),
        ],
    ));
    for dump in [&mut from, &mut to] {
        dump.rules.push(rule(
            "t",
            "r_qty",
            "ON UPDATE TO repro.t\n   WHERE new.qty < 0 DO INSTEAD NOTHING",
            None,
        ));
    }

    let script = compare(from, to).await;
    assert!(!script.contains("drop rule"), "{script}");
    assert!(!script.contains("r_qty"), "{script}");
}

#[tokio::test]
async fn rule_on_a_recreated_table_is_recreated_after_it() {
    // A partition-key change drops and recreates the table, and its rules go
    // with it — known from the rule's own relation, dependencies or not.
    let mut from = empty();
    let mut to = empty();
    let mut from_part = table(
        "pt",
        vec![
            int_column("repro", "pt", "id", 1),
            int_column("repro", "pt", "k", 2),
        ],
    );
    from_part.partition_key = Some("RANGE (k)".to_string());
    from_part.hash();
    let mut to_part = from_part.clone();
    to_part.partition_key = Some("LIST (k)".to_string());
    to_part.hash();
    from.tables.push(from_part);
    to.tables.push(to_part);
    for dump in [&mut from, &mut to] {
        dump.rules.push(rule(
            "pt",
            "pt_del",
            "ON DELETE TO repro.pt DO INSTEAD NOTHING",
            None,
        ));
    }

    let script = compare(from, to).await;
    let recreate_table = pos(&script, "drop table if exists repro.pt;");
    let recreate_rule = pos(&script, "CREATE OR REPLACE RULE pt_del AS");
    assert!(recreate_table < recreate_rule, "{script}");
    assert!(!script.contains("drop rule"), "{script}");
}

#[tokio::test]
async fn rule_on_a_rebuilt_view_is_recreated_after_the_view() {
    // The view reads a column that is retyped, so it is dropped and rebuilt,
    // and its INSTEAD rule goes with it.
    let mut from = empty();
    let mut to = empty();
    from.tables.push(table(
        "t",
        vec![
            int_column("repro", "t", "id", 1),
            int_column("repro", "t", "qty", 2),
        ],
    ));
    to.tables.push(table(
        "t",
        vec![
            int_column("repro", "t", "id", 1),
            bigint_column("t", "qty", 2),
        ],
    ));
    for dump in [&mut from, &mut to] {
        dump.views.push(view(
            "v",
            &["repro.t"],
            Some(&["repro.t.id", "repro.t.qty"]),
        ));
        dump.rules.push(rule(
            "v",
            "v_ins",
            "ON INSERT TO repro.v DO INSTEAD  INSERT INTO repro.t (id)\n  VALUES (new.id)",
            None,
        ));
    }

    let script = compare(from, to).await;
    let drop_view = pos(&script, "drop view if exists repro.v;");
    let create_view = pos(&script, "CREATE OR REPLACE VIEW repro.v as");
    let recreate_rule = pos(&script, "CREATE OR REPLACE RULE v_ins AS");
    assert!(drop_view < create_view, "{script}");
    assert!(create_view < recreate_rule, "{script}");
}

#[tokio::test]
async fn rule_reading_a_rebuilt_view_is_dropped_before_it() {
    // A rule on `t` that reads view `v`: DROP VIEW (no CASCADE) is refused
    // while the rule exists, so the rule goes first and comes back after.
    let mut from = empty();
    let mut to = empty();
    from.tables.push(table(
        "t",
        vec![
            int_column("repro", "t", "id", 1),
            int_column("repro", "t", "qty", 2),
        ],
    ));
    to.tables.push(table(
        "t",
        vec![
            int_column("repro", "t", "id", 1),
            bigint_column("t", "qty", 2),
        ],
    ));
    for dump in [&mut from, &mut to] {
        dump.views.push(view(
            "v",
            &["repro.t"],
            Some(&["repro.t.id", "repro.t.qty"]),
        ));
        dump.rules.push(rule(
            "t",
            "t_reads_v",
            "ON UPDATE TO repro.t DO  SELECT v.id\n   FROM repro.v",
            deps(&["repro.v"], &["repro.v.id"], &[]),
        ));
    }

    let script = compare(from, to).await;
    let drop_rule = pos(
        &script,
        "drop rule if exists \"t_reads_v\" on repro.\"t\" cascade;",
    );
    let drop_view = pos(&script, "drop view if exists repro.v;");
    let create_view = pos(&script, "CREATE OR REPLACE VIEW repro.v as");
    let recreate_rule = pos(&script, "CREATE OR REPLACE RULE t_reads_v AS");
    assert!(drop_rule < drop_view, "{script}");
    assert!(create_view < recreate_rule, "{script}");
}

#[tokio::test]
async fn rule_calling_a_recreated_routine_is_recreated_after_it() {
    // A return-type change is DROP FUNCTION ... CASCADE + CREATE, and the
    // CASCADE takes every rule calling the function along silently.
    let mut from = empty();
    let mut to = empty();
    from.routines
        .push(issue179_compute_routine("integer", "SELECT x * 2;"));
    to.routines.push(issue179_compute_routine(
        "bigint",
        "SELECT (x * 2)::bigint;",
    ));
    for dump in [&mut from, &mut to] {
        dump.tables
            .push(table("t", vec![int_column("repro", "t", "id", 1)]));
        dump.rules.push(rule(
            "t",
            "t_tag",
            "ON UPDATE TO repro.t DO  SELECT test_deps.compute(new.id) AS compute",
            deps(
                &["repro.t"],
                &["repro.t.id"],
                &["test_deps.compute(x integer)"],
            ),
        ));
    }

    let script = compare(from, to).await;
    let drop_rule = pos(
        &script,
        "drop rule if exists \"t_tag\" on repro.\"t\" cascade;",
    );
    let drop_fn = pos(
        &script,
        "drop function if exists test_deps.compute (x integer) cascade;",
    );
    let create_fn = pos(
        &script,
        "create or replace function test_deps.compute(x integer) returns bigint",
    );
    let recreate_rule = pos(&script, "CREATE OR REPLACE RULE t_tag AS");
    assert!(drop_rule < drop_fn, "{script}");
    assert!(create_fn < recreate_rule, "{script}");
}

#[tokio::test]
async fn rule_on_a_view_taken_by_a_routine_cascade_is_recreated_after_the_view() {
    // The view only calls the recreated routine, so its definition and hash
    // are unchanged and it is not in `dropped_views`: the routine's
    // `DROP ... CASCADE` takes it, and its rule, and the routine-dependents
    // pass recreates the view. The rule has to follow.
    let mut from = empty();
    let mut to = empty();
    from.routines
        .push(issue179_compute_routine("integer", "SELECT x * 2;"));
    to.routines.push(issue179_compute_routine(
        "bigint",
        "SELECT (x * 2)::bigint;",
    ));
    for dump in [&mut from, &mut to] {
        dump.tables
            .push(table("t", vec![int_column("repro", "t", "id", 1)]));
        let mut v = View::new(
            "v".to_string(),
            " SELECT id\n   FROM repro.t\n  WHERE test_deps.compute(id) > 0;".to_string(),
            "repro".to_string(),
            vec!["repro.t".to_string()],
        );
        v.column_relation = Some(vec!["repro.t.id".to_string()]);
        v.hash();
        dump.views.push(v);
        dump.rules.push(rule(
            "v",
            "v_del",
            "ON DELETE TO repro.v DO INSTEAD  DELETE FROM repro.t\n  WHERE t.id = old.id",
            deps(&["repro.t", "repro.v"], &["repro.t.id", "repro.v.id"], &[]),
        ));
    }

    let script = compare(from, to).await;
    let drop_fn = pos(
        &script,
        "drop function if exists test_deps.compute (x integer) cascade;",
    );
    let recreate_view = pos(&script, "Recreate dependents dropped by CASCADE: Start");
    let recreate_rule = pos(&script, "CREATE OR REPLACE RULE v_del AS");
    assert!(drop_fn < recreate_view, "{script}");
    assert!(recreate_view < recreate_rule, "{script}");
    // Nothing to drop early: the rule reads nothing that goes away.
    assert!(!script.contains("drop rule"), "{script}");
}

#[tokio::test]
async fn rule_calling_a_replaced_routine_is_left_alone() {
    // A body-only change is CREATE OR REPLACE FUNCTION, which keeps the
    // function and every rule calling it.
    let mut from = empty();
    let mut to = empty();
    from.routines
        .push(issue179_compute_routine("integer", "SELECT x * 2;"));
    to.routines
        .push(issue179_compute_routine("integer", "SELECT x * 3;"));
    for dump in [&mut from, &mut to] {
        dump.tables
            .push(table("t", vec![int_column("repro", "t", "id", 1)]));
        dump.rules.push(rule(
            "t",
            "t_tag",
            "ON UPDATE TO repro.t DO  SELECT test_deps.compute(new.id) AS compute",
            deps(
                &["repro.t"],
                &["repro.t.id"],
                &["test_deps.compute(x integer)"],
            ),
        ));
    }

    let script = compare(from, to).await;
    assert!(!script.contains("t_tag"), "{script}");
}

#[tokio::test]
async fn rule_reading_a_dropped_sequence_is_dropped_first() {
    // DROP SEQUENCE runs without CASCADE. The rule changes (TO no longer
    // calls nextval), so it comes back in its TO form.
    let mut from = empty();
    let mut to = empty();
    for dump in [&mut from, &mut to] {
        dump.tables
            .push(table("t", vec![int_column("repro", "t", "id", 1)]));
    }
    from.sequences.push(crate::dump::sequence::Sequence::new(
        "repro".to_string(),
        "s".to_string(),
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
    ));
    from.rules.push(rule(
        "t",
        "t_seq",
        "ON UPDATE TO repro.t DO  SELECT nextval('repro.s'::regclass) AS nextval",
        deps(&["repro.s", "repro.t"], &[], &[]),
    ));
    to.rules.push(rule(
        "t",
        "t_seq",
        "ON UPDATE TO repro.t DO INSTEAD NOTHING",
        deps(&["repro.t"], &[], &[]),
    ));

    let script = compare(from, to).await;
    let drop_rule = pos(
        &script,
        "drop rule if exists \"t_seq\" on repro.\"t\" cascade;",
    );
    let drop_seq = pos(&script, "drop sequence if exists repro.s;");
    let recreate_rule = pos(
        &script,
        "CREATE OR REPLACE RULE t_seq AS\n    ON UPDATE TO repro.t DO INSTEAD NOTHING;",
    );
    assert!(drop_rule < drop_seq, "{script}");
    assert!(drop_seq < recreate_rule, "{script}");
}

#[tokio::test]
async fn production_mode_creates_new_rules_replayably() {
    let mut from = empty();
    let mut to = empty();
    for dump in [&mut from, &mut to] {
        dump.tables
            .push(table("t", vec![int_column("repro", "t", "id", 1)]));
    }
    to.rules.push(rule(
        "t",
        "r_new",
        "ON DELETE TO repro.t DO INSTEAD NOTHING",
        None,
    ));

    let default_script = compare_with(from.clone(), to.clone(), true, false).await;
    assert!(
        default_script
            .contains("CREATE RULE r_new AS\n    ON DELETE TO repro.t DO INSTEAD NOTHING;"),
        "{default_script}"
    );

    let production_script = compare_with(from, to, true, true).await;
    assert!(
        production_script.contains(
            "CREATE OR REPLACE RULE r_new AS\n    ON DELETE TO repro.t DO INSTEAD NOTHING;"
        ),
        "{production_script}"
    );
    assert!(
        !production_script.contains("CREATE RULE r_new"),
        "{production_script}"
    );
}

// ---- Review fixes: rules the migration still lost or stood in the way of ----

fn with_comment(mut rule: Rule, comment: &str) -> Rule {
    rule.comment = Some(comment.to_string());
    rule.hash();
    rule
}

/// A view on `repro.t` that calls `test_deps.compute`, so a return-type change
/// of that routine takes it along in `DROP FUNCTION ... CASCADE`.
fn compute_view(name: &str) -> View {
    let mut v = View::new(
        name.to_string(),
        " SELECT id\n   FROM repro.t\n  WHERE test_deps.compute(id) > 0;".to_string(),
        "repro".to_string(),
        vec!["repro.t".to_string()],
    );
    v.column_relation = Some(vec!["repro.t.id".to_string()]);
    v.hash();
    v
}

fn foreign_table(name: &str, server: &str, columns: &[(&str, &str)]) -> ForeignTable {
    ForeignTable::new(
        "repro".to_string(),
        name.to_string(),
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

#[tokio::test]
async fn rule_recreated_after_a_cascade_drops_a_removed_comment() {
    // The view's rule is recreated with CREATE OR REPLACE RULE because the
    // routine's CASCADE may have taken it. The view can be flagged by a textual
    // match only (another overload, an unqualified name), in which case
    // nothing was dropped and the FROM comment is still there: the recreate
    // must clear it, as the plain alter path does.
    let mut from = empty();
    let mut to = empty();
    from.routines
        .push(issue179_compute_routine("integer", "SELECT x * 2;"));
    to.routines.push(issue179_compute_routine(
        "bigint",
        "SELECT (x * 2)::bigint;",
    ));
    let body = "ON DELETE TO repro.v DO INSTEAD  DELETE FROM repro.t\n  WHERE t.id = old.id";
    let rule_deps = || deps(&["repro.t", "repro.v"], &["repro.t.id", "repro.v.id"], &[]);
    for dump in [&mut from, &mut to] {
        dump.tables
            .push(table("t", vec![int_column("repro", "t", "id", 1)]));
        dump.views.push(compute_view("v"));
    }
    from.rules.push(with_comment(
        rule("v", "v_del", body, rule_deps()),
        "old comment",
    ));
    to.rules.push(rule("v", "v_del", body, rule_deps()));

    let script = compare(from.clone(), to.clone()).await;
    let recreate_rule = pos(&script, "CREATE OR REPLACE RULE v_del AS");
    let clear_comment = pos(&script, "comment on rule \"v_del\" on repro.\"v\" is null;");
    assert!(recreate_rule < clear_comment, "{script}");

    // Without use_drop a removed comment is left in place, as in
    // `Rule::get_alter_script`.
    let script = compare_with(from, to, false, false).await;
    assert!(!script.contains("is null;"), "{script}");
}

#[tokio::test]
async fn rule_recreated_on_the_way_keeps_its_target_comment() {
    // The TO comment is written whatever FROM had.
    let mut from = empty();
    let mut to = empty();
    from.routines
        .push(issue179_compute_routine("integer", "SELECT x * 2;"));
    to.routines.push(issue179_compute_routine(
        "bigint",
        "SELECT (x * 2)::bigint;",
    ));
    let body = "ON DELETE TO repro.v DO INSTEAD  DELETE FROM repro.t\n  WHERE t.id = old.id";
    for dump in [&mut from, &mut to] {
        dump.tables
            .push(table("t", vec![int_column("repro", "t", "id", 1)]));
        dump.views.push(compute_view("v"));
        dump.rules
            .push(with_comment(rule("v", "v_del", body, None), "kept comment"));
    }

    let script = compare(from, to).await;
    let recreate_rule = pos(&script, "CREATE OR REPLACE RULE v_del AS");
    let comment = pos(
        &script,
        "comment on rule \"v_del\" on repro.\"v\" is 'kept comment';",
    );
    assert!(recreate_rule < comment, "{script}");
    assert!(!script.contains("is null;"), "{script}");
}

#[tokio::test]
async fn rule_reading_a_view_taken_by_a_routine_cascade_is_recreated_after_the_view() {
    // The rule sits on another table and only reads the view. The routine's
    // DROP ... CASCADE takes the view and, through it, the rule; the
    // routine-dependents pass brings the view back, and the rule has to
    // follow. Known only from the rule's recorded dependencies.
    let mut from = empty();
    let mut to = empty();
    from.routines
        .push(issue179_compute_routine("integer", "SELECT x * 2;"));
    to.routines.push(issue179_compute_routine(
        "bigint",
        "SELECT (x * 2)::bigint;",
    ));
    for dump in [&mut from, &mut to] {
        dump.tables
            .push(table("t", vec![int_column("repro", "t", "id", 1)]));
        dump.tables
            .push(table("log", vec![int_column("repro", "log", "id", 1)]));
        dump.views.push(compute_view("v"));
        dump.rules.push(rule(
            "log",
            "log_reads_v",
            "ON UPDATE TO repro.log DO  INSERT INTO repro.log (id)  SELECT v.id\n           FROM repro.v",
            deps(&["repro.log", "repro.v"], &["repro.log.id", "repro.v.id"], &[]),
        ));
    }

    let script = compare(from.clone(), to.clone()).await;
    let recreate_view = pos(&script, "Recreate dependents dropped by CASCADE: Start");
    let recreate_rule = pos(&script, "CREATE OR REPLACE RULE log_reads_v AS");
    assert!(recreate_view < recreate_rule, "{script}");
    // The CASCADE removes the rule; nothing has to drop it first.
    assert!(!script.contains("drop rule"), "{script}");

    // A dump without recorded dependencies cannot tell.
    for dump in [&mut from, &mut to] {
        dump.rules[0].depends_on = None;
    }
    let script = compare(from, to).await;
    assert!(!script.contains("log_reads_v"), "{script}");
}

#[tokio::test]
async fn rule_on_a_table_replaced_by_a_view_is_recreated_on_the_view() {
    // FROM has table `tv`, TO a view `tv` with the same rule. DROP TABLE takes
    // the rule, whether or not the dump recorded dependencies — a NOTIFY-only
    // rule has none — so the rule is recreated once the view exists.
    for depends_on in [None, deps(&[], &[], &[])] {
        let mut from = empty();
        let mut to = empty();
        for dump in [&mut from, &mut to] {
            dump.tables
                .push(table("t", vec![int_column("repro", "t", "id", 1)]));
        }
        from.tables
            .push(table("tv", vec![int_column("repro", "tv", "id", 1)]));
        to.views
            .push(view("tv", &["repro.t"], Some(&["repro.t.id"])));
        for dump in [&mut from, &mut to] {
            dump.rules.push(rule(
                "tv",
                "tv_notify",
                "ON INSERT TO repro.tv DO\n NOTIFY chan",
                depends_on.clone(),
            ));
        }

        let script = compare(from.clone(), to.clone()).await;
        let drop_table = pos(&script, "drop table if exists repro.tv;");
        let create_view = pos(&script, "CREATE OR REPLACE VIEW repro.tv as");
        let recreate_rule = pos(&script, "CREATE OR REPLACE RULE tv_notify AS");
        assert!(drop_table < create_view, "{script}");
        assert!(create_view < recreate_rule, "{script}");
        // DROP TABLE removes the table's own rules; no early drop needed.
        assert!(!script.contains("drop rule"), "{script}");
    }
}

#[tokio::test]
async fn rule_reading_a_dropped_foreign_table_is_dropped_first() {
    // DROP FOREIGN TABLE runs without CASCADE and is refused while a rule
    // reads the table.
    let mut from = empty();
    let mut to = empty();
    for dump in [&mut from, &mut to] {
        dump.tables
            .push(table("t", vec![int_column("repro", "t", "id", 1)]));
        dump.tables
            .push(table("log", vec![int_column("repro", "log", "id", 1)]));
    }
    from.foreign_tables
        .push(foreign_table("ft", "srv", &[("id", "integer")]));
    from.rules.push(rule(
        "t",
        "t_ft",
        "ON UPDATE TO repro.t DO  INSERT INTO repro.log (id)  SELECT ft.id\n           FROM repro.ft",
        deps(
            &["repro.ft", "repro.log", "repro.t"],
            &["repro.ft.id", "repro.log.id"],
            &[],
        ),
    ));
    to.rules.push(rule(
        "t",
        "t_ft",
        "ON UPDATE TO repro.t DO  INSERT INTO repro.log (id)\n  VALUES (new.id)",
        deps(&["repro.log", "repro.t"], &["repro.log.id"], &[]),
    ));

    let script = compare(from.clone(), to.clone()).await;
    let drop_rule = pos(
        &script,
        "drop rule if exists \"t_ft\" on repro.\"t\" cascade;",
    );
    let drop_ft = pos(&script, "drop foreign table if exists repro.ft;");
    let recreate_rule = pos(&script, "CREATE OR REPLACE RULE t_ft AS");
    assert!(drop_rule < drop_ft, "{script}");
    assert!(drop_ft < recreate_rule, "{script}");

    // Without use_drop the foreign table stays, and so does the rule until
    // its own change.
    let script = compare_with(from, to, false, false).await;
    assert!(!script.contains("drop rule"), "{script}");
}

#[tokio::test]
async fn rule_reading_a_recreated_foreign_table_is_dropped_first() {
    // A server change is DROP FOREIGN TABLE + CREATE FOREIGN TABLE.
    let mut from = empty();
    let mut to = empty();
    for dump in [&mut from, &mut to] {
        dump.tables
            .push(table("t", vec![int_column("repro", "t", "id", 1)]));
        dump.rules.push(rule(
            "t",
            "t_ft",
            "ON UPDATE TO repro.t DO  SELECT ft.id\n   FROM repro.ft",
            deps(&["repro.ft", "repro.t"], &["repro.ft.id"], &[]),
        ));
    }
    from.foreign_tables
        .push(foreign_table("ft", "srv", &[("id", "integer")]));
    to.foreign_tables
        .push(foreign_table("ft", "srv2", &[("id", "integer")]));

    let script = compare(from, to).await;
    let drop_rule = pos(
        &script,
        "drop rule if exists \"t_ft\" on repro.\"t\" cascade;",
    );
    let drop_ft = pos(&script, "drop foreign table if exists repro.ft;");
    let create_ft = pos(&script, "create foreign table repro.ft (");
    let recreate_rule = pos(&script, "CREATE OR REPLACE RULE t_ft AS");
    assert!(drop_rule < drop_ft, "{script}");
    assert!(create_ft < recreate_rule, "{script}");
}

#[tokio::test]
async fn rule_reading_a_retyped_foreign_table_column_is_dropped_first() {
    // ALTER FOREIGN TABLE ... ALTER COLUMN TYPE is refused while a rule reads
    // the column; a column the rule does not read is no reason to touch it.
    let mut from = empty();
    let mut to = empty();
    for dump in [&mut from, &mut to] {
        dump.tables
            .push(table("t", vec![int_column("repro", "t", "id", 1)]));
        dump.rules.push(rule(
            "t",
            "t_ft",
            "ON UPDATE TO repro.t DO  SELECT ft.v\n   FROM repro.ft",
            deps(&["repro.ft", "repro.t"], &["repro.ft.v"], &[]),
        ));
    }
    from.foreign_tables.push(foreign_table(
        "ft",
        "srv",
        &[("id", "integer"), ("v", "integer")],
    ));
    to.foreign_tables.push(foreign_table(
        "ft",
        "srv",
        &[("id", "bigint"), ("v", "bigint")],
    ));

    let script = compare(from.clone(), to.clone()).await;
    let drop_rule = pos(
        &script,
        "drop rule if exists \"t_ft\" on repro.\"t\" cascade;",
    );
    let retype = pos(
        &script,
        "alter foreign table repro.ft alter column v type bigint;",
    );
    let recreate_rule = pos(&script, "CREATE OR REPLACE RULE t_ft AS");
    assert!(drop_rule < retype, "{script}");
    assert!(retype < recreate_rule, "{script}");

    // Only `id` changes: the rule reads `v`, and is left alone.
    to.foreign_tables[0] = foreign_table("ft", "srv", &[("id", "bigint"), ("v", "integer")]);
    let script = compare(from, to).await;
    assert!(!script.contains("t_ft"), "{script}");
}

/// `repro.f()`, a routine with a SQL-standard body reading `repro.t.v`.
fn sql_body_reader_of_t_v() -> Routine {
    let mut routine = Routine::new(
        "repro".to_string(),
        Oid(900),
        "f".to_string(),
        "sql".to_string(),
        "function".to_string(),
        "bigint".to_string(),
        String::new(),
        None,
        None,
        String::new(),
    );
    routine.owner = "postgres".to_string();
    routine.source_code = "RETURN (SELECT max(t.v) AS max FROM repro.t)".to_string();
    routine.is_sql_body = true;
    routine.sql_body_dependencies = SqlBodyDependencies {
        relations: vec!["repro.t".to_string()],
        columns: vec!["repro.t.v".to_string()],
        ..SqlBodyDependencies::default()
    };
    routine.hash();
    routine
}

#[tokio::test]
async fn rule_calling_a_sql_body_routine_dropped_ahead_is_dropped_first_and_recreated() {
    // The unchanged routine reads a column the migration retypes, so
    // `drop_views` drops it — with CASCADE — ahead of the table DDL. That
    // CASCADE would take the unchanged rule calling it along silently, so the
    // rule is dropped before the routine and recreated after the routine
    // phase has re-created it.
    let mut from = empty();
    let mut to = empty();
    from.tables.push(table(
        "t",
        vec![
            int_column("repro", "t", "id", 1),
            int_column("repro", "t", "v", 2),
        ],
    ));
    to.tables.push(table(
        "t",
        vec![
            int_column("repro", "t", "id", 1),
            bigint_column("t", "v", 2),
        ],
    ));
    for dump in [&mut from, &mut to] {
        dump.tables
            .push(table("log", vec![int_column("repro", "log", "id", 1)]));
        dump.routines.push(sql_body_reader_of_t_v());
        dump.rules.push(rule(
            "log",
            "log_f",
            "ON INSERT TO repro.log DO  SELECT repro.f() AS f",
            deps(&["repro.log"], &[], &["repro.f()"]),
        ));
    }

    let script = compare(from, to).await;
    let drop_rule = pos(
        &script,
        "drop rule if exists \"log_f\" on repro.\"log\" cascade;",
    );
    let drop_fn = pos(&script, "drop function if exists repro.f () cascade;");
    let retype = pos(&script, "alter column v type bigint");
    let create_fn = pos(
        &script,
        "create or replace function repro.f() returns bigint",
    );
    let recreate_rule = pos(&script, "CREATE OR REPLACE RULE log_f AS");
    assert!(drop_rule < drop_fn, "{script}");
    assert!(drop_fn < retype, "{script}");
    assert!(retype < create_fn, "{script}");
    assert!(create_fn < recreate_rule, "{script}");
}
