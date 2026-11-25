//! Extended statistics: emission and restoration after column rewrites.
//!
//! PostgreSQL drops a statistics object together with any column it names,
//! and together with its table. pgc rewrites some column changes as
//! `DROP COLUMN` + `ADD COLUMN` (a virtual generated column changing its
//! expression, a plain column becoming generated), drops and recreates a
//! table whose partition key changes, and drops a routine with `CASCADE`
//! when its signature changes. None of those used to bring the statistics
//! back, so the next compare found them missing. Statistics on materialized
//! views have the same problem when the view is rebuilt, and on a view that
//! is new they have to wait until it exists.
//!
//! PostgreSQL keeps the statistics when a column is altered in place, but
//! `ALTER COLUMN ... TYPE` and `SET EXPRESSION` rebuild them from their
//! definition and reset their statistics target, which pgc then sets again;
//! such a column must not trigger a restore.
//!
//! Also here: the partition child whose parent changes a column type (not
//! dropped, so its statistics must be left alone), and statistics targets
//! compared with a TO dump written before pgc 1.1.0 (which did not read them
//! on PostgreSQL 17+).

use super::helpers::*;
use crate::comparer::core::*;
use crate::config::dump_config::DumpConfig;
use crate::config::grants_mode::GrantsMode;
use crate::dump::column_dependent::{ColumnDependent, ColumnDependentKind};
use crate::dump::statistic::Statistic;
use crate::dump::table::Table;
use crate::dump::table_column::TableColumn;

/// A statistics object as the dump builds it from the server. `on` is the
/// `pg_get_statisticsobjdef_columns` text; `kinds` the mapped `stxkind`.
fn statistic(schema: &str, name: &str, table: &str, kinds: &[&str], on: &str) -> Statistic {
    let kinds: Vec<String> = kinds.iter().map(|k| k.to_string()).collect();
    let kind_list = kinds
        .iter()
        .filter(|k| *k != "expressions")
        .cloned()
        .collect::<Vec<_>>()
        .join(", ");
    let definition =
        format!("CREATE STATISTICS {schema}.{name} ({kind_list}) ON {on} FROM test_schema.{table}");
    let mut stat = Statistic::new(
        schema.to_string(),
        name.to_string(),
        "postgres".to_string(),
        "test_schema".to_string(),
        table.to_string(),
        kinds,
        on.split(',').map(|c| c.trim().to_string()).collect(),
        definition,
    );
    stat.columns_def = Some(on.to_string());
    stat.hash();
    stat
}

fn statistic_dependent(column: &str, dep_schema: &str, dep_name: &str) -> ColumnDependent {
    ColumnDependent {
        schema: "test_schema".to_string(),
        table: "vstat".to_string(),
        column: column.to_string(),
        kind: ColumnDependentKind::Statistic,
        dep_schema: dep_schema.to_string(),
        dep_table: "vstat".to_string(),
        dep_name: dep_name.to_string(),
    }
}

fn virtual_column(name: &str, ordinal: i32, expression: &str) -> TableColumn {
    let mut column = int_column("test_schema", "vstat", name, ordinal);
    column.is_generated = "ALWAYS".to_string();
    column.generation_expression = Some(expression.to_string());
    column.generation_type = Some("v".to_string());
    column
}

/// `test_schema.vstat (id, qty, doubled VIRTUAL AS <doubled>)`.
fn vstat_table(doubled: &str) -> Table {
    let mut table = Table::new(
        "test_schema".to_string(),
        "vstat".to_string(),
        "test_schema".to_string(),
        "vstat".to_string(),
        "postgres".to_string(),
        None,
        vec![
            int_column("test_schema", "vstat", "id", 1),
            int_column("test_schema", "vstat", "qty", 2),
            virtual_column("doubled", 3, doubled),
        ],
        vec![],
        vec![],
        vec![],
        None,
    );
    table.hash();
    table
}

/// FROM and TO dumps where `doubled` changes its expression (so pgc drops
/// and re-adds it) and `st_rewrite` on it is unchanged.
fn virtual_rewrite_dumps() -> (Dump, Dump) {
    let mut from = Dump::new(DumpConfig::default());
    let mut to = Dump::new(DumpConfig::default());
    from.tables.push(vstat_table("(qty * 2)"));
    to.tables.push(vstat_table("(qty * 3)"));
    let stat = statistic(
        "test_schema",
        "st_rewrite",
        "vstat",
        &["ndistinct", "expressions"],
        "qty, (doubled)",
    );
    from.statistics.push(stat.clone());
    to.statistics.push(stat);
    from.column_dependents
        .push(statistic_dependent("doubled", "test_schema", "st_rewrite"));
    (from, to)
}

#[tokio::test]
async fn virtual_column_rewrite_restores_statistics_after_add_column() {
    let (from, to) = virtual_rewrite_dumps();
    let mut comparer = Comparer::new(from, to, true, false, true, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();

    let add = script
        .find("alter table test_schema.vstat add column doubled")
        .expect("the expression change is a drop + add");
    let drop_stat = script
        .find("drop statistics if exists test_schema.st_rewrite;")
        .expect("the restore starts with a guarded drop");
    let create = script
        .find("create statistics test_schema.st_rewrite (ndistinct) on qty, (doubled) from test_schema.vstat;")
        .expect("the CASCADE-dropped statistics must be recreated");
    assert!(add < drop_stat && drop_stat < create, "{script}");
    assert!(
        script.contains("Recreate dependents dropped by virtual-column rewrite"),
        "{script}"
    );
    assert_eq!(
        script.matches("create statistics").count(),
        1,
        "the hash did not change, so compare_statistics adds nothing: {script}"
    );
}

#[tokio::test]
async fn virtual_column_rewrite_restore_carries_target_and_comment() {
    let (mut from, mut to) = virtual_rewrite_dumps();
    for dump in [&mut from, &mut to] {
        let stat = &mut dump.statistics[0];
        stat.stxstattarget = Some(500);
        stat.comment = Some("it's kept".to_string());
        stat.hash();
    }
    let mut comparer = Comparer::new(from, to, true, false, true, GrantsMode::Ignore);
    comparer.compare_tables().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("alter statistics test_schema.st_rewrite set statistics 500;"),
        "{script}"
    );
    assert!(
        script.contains("comment on statistics test_schema.st_rewrite is 'it''s kept';"),
        "{script}"
    );
}

#[tokio::test]
async fn statistics_on_two_rewritten_columns_are_restored_once() {
    // Both columns the object names are dropped and re-added; PostgreSQL
    // drops it once, and it must be recreated once.
    let (mut from, mut to) = virtual_rewrite_dumps();
    for (dump, expression) in [(&mut from, "(qty + 1)"), (&mut to, "(qty + 2)")] {
        let table = &mut dump.tables[0];
        table.columns.push(virtual_column("plus", 4, expression));
        table.hash();
    }
    for dump in [&mut from, &mut to] {
        let stat = &mut dump.statistics[0];
        stat.columns_def = Some("(doubled), (plus)".to_string());
        stat.hash();
    }
    from.column_dependents
        .push(statistic_dependent("plus", "test_schema", "st_rewrite"));

    let mut comparer = Comparer::new(from, to, true, false, true, GrantsMode::Ignore);
    comparer.compare_tables().await.unwrap();
    let script = comparer.get_script();

    assert_eq!(
        script
            .matches("create statistics test_schema.st_rewrite")
            .count(),
        1,
        "{script}"
    );
    let last_add = script
        .rfind("add column")
        .expect("both columns are re-added");
    let create = script.find("create statistics").unwrap();
    assert!(
        last_add < create,
        "restored after the columns it names: {script}"
    );
}

#[tokio::test]
async fn virtual_column_rewrite_comments_the_restore_out_without_use_drop() {
    let (from, to) = virtual_rewrite_dumps();
    let mut comparer = Comparer::new(from, to, false, false, true, GrantsMode::Ignore);
    comparer.compare_tables().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("-- create statistics test_schema.st_rewrite"),
        "{script}"
    );
    assert!(!script.contains("\ncreate statistics"), "{script}");
    assert!(!script.contains("\ndrop statistics"), "{script}");
}

#[tokio::test]
async fn virtual_column_rewrite_restores_statistics_in_another_schema() {
    // The dependent's schema is the statistics object's own, not the table's;
    // the owner-table lookup the other kinds use would miss it.
    let (mut from, mut to) = virtual_rewrite_dumps();
    let stat = statistic(
        "stats",
        "st_elsewhere",
        "vstat",
        &["mcv", "expressions"],
        "id, (doubled)",
    );
    from.statistics.push(stat.clone());
    to.statistics.push(stat);
    from.column_dependents
        .push(statistic_dependent("doubled", "stats", "st_elsewhere"));

    let mut comparer = Comparer::new(from, to, true, false, true, GrantsMode::Ignore);
    comparer.compare_tables().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains(
            "create statistics stats.st_elsewhere (mcv) on id, (doubled) from test_schema.vstat;"
        ),
        "{script}"
    );
}

#[tokio::test]
async fn virtual_column_rewrite_skips_statistics_gone_from_to_or_moved() {
    let (from, mut to) = virtual_rewrite_dumps();
    // Gone from TO: compare_statistics drops it (IF EXISTS), nothing to restore.
    to.statistics.clear();
    let mut comparer = Comparer::new(from, to, true, false, true, GrantsMode::Ignore);
    comparer.compare_tables().await.unwrap();
    assert!(!comparer.get_script().contains("statistics"));

    // Moved to another table: not a restore, compare_statistics recreates it.
    let (from, mut to) = virtual_rewrite_dumps();
    to.statistics[0].table_name = "elsewhere".to_string();
    to.statistics[0].hash();
    let mut comparer = Comparer::new(from, to, true, false, true, GrantsMode::Ignore);
    comparer.compare_tables().await.unwrap();
    assert!(!comparer.get_script().contains("statistics"));
}

/// `virtual_rewrite_dumps` with `doubled` of generation type `kind` (`s` or
/// `v`), migrating a server of `version`, and `st_rewrite` carrying
/// statistics target `target` on both sides.
fn expression_change_dumps(kind: &str, version: Option<i32>, target: Option<i32>) -> (Dump, Dump) {
    let (mut from, mut to) = virtual_rewrite_dumps();
    from.server_version_num = version;
    for dump in [&mut from, &mut to] {
        let table = &mut dump.tables[0];
        table.columns[2].generation_type = Some(kind.to_string());
        table.hash();
        let stat = &mut dump.statistics[0];
        if kind == "s" {
            // A STORED column is an ordinary key, not an expression.
            stat.kinds = vec!["ndistinct".to_string()];
            stat.columns_def = Some("qty, doubled".to_string());
        }
        stat.stxstattarget = target;
        stat.hash();
    }
    (from, to)
}

#[tokio::test]
async fn statistics_are_not_restored_when_set_expression_keeps_the_column() {
    // Where the migrated server can SET EXPRESSION — STORED from PostgreSQL
    // 17, VIRTUAL from 19 — the column is altered in place and keeps the
    // statistics on it: nothing is dropped, so nothing is restored.
    for (kind, version) in [("s", 170000), ("s", 190000), ("v", 190000)] {
        let (from, to) = expression_change_dumps(kind, Some(version), None);
        let mut comparer = Comparer::new(from, to, true, false, true, GrantsMode::Ignore);
        comparer.compare().await.unwrap();
        let script = comparer.get_script();

        assert!(
            script.contains(
                "alter table only test_schema.vstat alter column doubled set expression as (qty * 3);"
            ),
            "{kind} on {version}: {script}"
        );
        assert!(!script.contains("drop column"), "{script}");
        assert!(
            !script.contains("statistics"),
            "{kind} on {version}: {script}"
        );
    }
}

#[tokio::test]
async fn statistics_are_restored_where_the_server_cannot_set_expression() {
    // STORED before PostgreSQL 17 (or on a server of unknown version): the
    // expression change is a drop + add, which takes the statistics along.
    for version in [Some(160000), None] {
        let (from, to) = expression_change_dumps("s", version, Some(500));
        let mut comparer = Comparer::new(from, to, true, false, true, GrantsMode::Ignore);
        comparer.compare().await.unwrap();
        let script = comparer.get_script();

        let add = script
            .find("alter table test_schema.vstat add column doubled")
            .expect("the expression change is a drop + add");
        let create = script
            .find("create statistics test_schema.st_rewrite (ndistinct) on qty, doubled from test_schema.vstat;")
            .unwrap_or_else(|| panic!("{version:?}: {script}"));
        assert!(add < create, "{script}");
        assert_eq!(
            script
                .matches("alter statistics test_schema.st_rewrite set statistics 500;")
                .count(),
            1,
            "the restore carries the target, once: {script}"
        );
    }
}

#[tokio::test]
async fn set_expression_sets_the_statistics_target_again() {
    // PostgreSQL rebuilds the statistics on a column SET EXPRESSION changes,
    // from their definition, and the rebuilt object has the default target.
    for (kind, version) in [("s", 170000), ("v", 190000)] {
        let (from, to) = expression_change_dumps(kind, Some(version), Some(500));
        let mut comparer = Comparer::new(from, to, true, false, true, GrantsMode::Ignore);
        comparer.compare().await.unwrap();
        let script = comparer.get_script();

        let set_expression = script
            .find("alter column doubled set expression as (qty * 3);")
            .unwrap_or_else(|| panic!("{kind} on {version}: {script}"));
        let target = script
            .find("alter statistics test_schema.st_rewrite set statistics 500;")
            .unwrap_or_else(|| panic!("{kind} on {version}: the target is set again: {script}"));
        assert!(set_expression < target, "{script}");
        assert!(!script.contains("create statistics"), "{script}");
        assert!(!script.contains("drop statistics"), "{script}");
    }
}

/// `test_schema.retype (id, a <a_type>, b)` with `st_ab` (target
/// `target`) on `a, b` and `st_b` (target 70) on `b` alone. `edges`: record
/// which columns each object reads, as a pgc 1.1.0 dump does.
fn retype_dumps(target: Option<i32>, edges: bool) -> (Dump, Dump) {
    let make = |a_type: &str| {
        let mut a = int_column("test_schema", "retype", "a", 2);
        a.data_type = a_type.to_string();
        let mut table = Table::new(
            "test_schema".to_string(),
            "retype".to_string(),
            "test_schema".to_string(),
            "retype".to_string(),
            "postgres".to_string(),
            None,
            vec![
                int_column("test_schema", "retype", "id", 1),
                a,
                int_column("test_schema", "retype", "b", 3),
            ],
            vec![],
            vec![],
            vec![],
            None,
        );
        table.hash();
        table
    };
    let mut from = Dump::new(DumpConfig::default());
    let mut to = Dump::new(DumpConfig::default());
    from.tables.push(make("integer"));
    to.tables.push(make("bigint"));
    for dump in [&mut from, &mut to] {
        let mut ab = statistic("test_schema", "st_ab", "retype", &["ndistinct"], "a, b");
        ab.stxstattarget = target;
        ab.hash();
        dump.statistics.push(ab);
        let mut b = statistic("test_schema", "st_b", "retype", &["ndistinct"], "b, id");
        b.stxstattarget = Some(70);
        b.hash();
        dump.statistics.push(b);
        if edges {
            for (column, name) in [
                ("a", "st_ab"),
                ("b", "st_ab"),
                ("b", "st_b"),
                ("id", "st_b"),
            ] {
                dump.column_dependents.push(ColumnDependent {
                    schema: "test_schema".to_string(),
                    table: "retype".to_string(),
                    column: column.to_string(),
                    kind: ColumnDependentKind::Statistic,
                    dep_schema: "test_schema".to_string(),
                    dep_table: "retype".to_string(),
                    dep_name: name.to_string(),
                });
            }
        }
    }
    (from, to)
}

#[tokio::test]
async fn a_column_type_change_sets_the_statistics_target_again() {
    // ALTER COLUMN ... TYPE rebuilds the statistics that read the column on
    // every supported version, resetting their target. `st_b` does not read
    // `a` and keeps its target.
    let (from, to) = retype_dumps(Some(300), true);
    let mut comparer = Comparer::new(from, to, true, false, true, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();

    let retype = script
        .find("alter table test_schema.retype alter column a type bigint;")
        .expect("the type change is in place");
    let target = script
        .find("alter statistics test_schema.st_ab set statistics 300;")
        .expect("the rebuilt object gets its target back");
    assert!(retype < target, "{script}");
    assert!(!script.contains("st_b "), "{script}");
    assert!(!script.contains("create statistics"), "{script}");

    // With the default target there is nothing to set again.
    let (from, to) = retype_dumps(None, true);
    let mut comparer = Comparer::new(from, to, true, false, true, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    assert!(!comparer.get_script().contains("statistics"));
}

#[tokio::test]
async fn a_column_type_change_without_dependency_edges_sets_every_target_on_the_table() {
    // Dumps written before pgc 1.1.0 do not record which columns an object
    // reads, so every object on the table gets its target set again; for
    // one the rebuild did not touch, that is a no-op.
    let (from, to) = retype_dumps(Some(300), false);
    let mut comparer = Comparer::new(from, to, true, false, true, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("alter statistics test_schema.st_ab set statistics 300;"),
        "{script}"
    );
    assert!(
        script.contains("alter statistics test_schema.st_b set statistics 70;"),
        "{script}"
    );
}

#[tokio::test]
async fn a_rebuilt_statistics_target_is_set_once() {
    // A target change of its own already sets the new target.
    let (mut from, to) = retype_dumps(Some(300), true);
    from.statistics[0].stxstattarget = Some(100);
    from.statistics[0].hash();
    let mut comparer = Comparer::new(from, to, true, false, true, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();
    assert_eq!(script.matches("set statistics 300;").count(), 1, "{script}");

    // A live recreate carries it in its CREATE.
    let (mut from, to) = retype_dumps(Some(300), true);
    from.statistics[0].kinds = vec!["mcv".to_string()];
    from.statistics[0].hash();
    let mut comparer = Comparer::new(from, to, true, false, true, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();
    let create = script
        .find("create statistics test_schema.st_ab (ndistinct) on a, b")
        .unwrap();
    let target = script
        .find("alter statistics test_schema.st_ab set statistics 300;")
        .unwrap();
    assert!(create < target, "{script}");
    assert_eq!(script.matches("set statistics 300;").count(), 1, "{script}");

    // With use_drop off that recreate is commented out, but the type change
    // still rebuilds the object, so the target is set live.
    let (mut from, to) = retype_dumps(Some(300), true);
    from.statistics[0].kinds = vec!["mcv".to_string()];
    from.statistics[0].hash();
    let mut comparer = Comparer::new(from, to, false, false, true, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();
    assert!(
        script.contains("-- create statistics test_schema.st_ab (ndistinct) on a, b"),
        "{script}"
    );
    assert!(
        script.contains("\nalter statistics test_schema.st_ab set statistics 300;"),
        "{script}"
    );
}

#[tokio::test]
async fn phase7_restores_statistics_on_a_cascade_dropped_generated_column() {
    // A routine signature change drops the generated column that calls it,
    // and with it the statistics on that column.
    let mut from = Dump::new(DumpConfig::default());
    let mut to = Dump::new(DumpConfig::default());
    from.routines
        .push(issue179_compute_routine("integer", "SELECT x * 2;"));
    to.routines.push(issue179_compute_routine(
        "bigint",
        "SELECT (x * 2)::bigint;",
    ));
    from.tables.push(issue179_items_table(
        "integer",
        "test_deps.compute(0)::integer",
        "integer",
    ));
    to.tables.push(issue179_items_table(
        "bigint",
        "test_deps.compute(0)",
        "bigint",
    ));
    let mut stat = statistic(
        "test_deps",
        "st_gen",
        "items",
        &["ndistinct"],
        "value, gen_col",
    );
    stat.table_schema = "test_deps".to_string();
    stat.hash();
    from.statistics.push(stat.clone());
    to.statistics.push(stat);
    from.column_dependents.push(ColumnDependent {
        schema: "test_deps".to_string(),
        table: "items".to_string(),
        column: "gen_col".to_string(),
        kind: ColumnDependentKind::Statistic,
        dep_schema: "test_deps".to_string(),
        dep_table: "items".to_string(),
        dep_name: "st_gen".to_string(),
    });

    let mut comparer = Comparer::new(from, to, true, false, true, GrantsMode::Ignore);
    comparer.compare_routines_and_views().await.unwrap();
    let script = comparer.get_script();

    let add = script
        .find("add column if not exists gen_col")
        .expect("Phase 7 re-adds the generated column");
    let create = script
        .find("create statistics test_deps.st_gen (ndistinct) on value, gen_col from test_deps.items;")
        .expect("the statistics on it must be restored");
    assert!(add < create, "{script}");
}

/// A partitioned table whose partition key changes between FROM and TO, with
/// one unchanged statistics object on it.
fn recreated_table_dumps() -> (Dump, Dump) {
    let make = |key: &str| {
        let mut table = Table::new(
            "test_schema".to_string(),
            "parted".to_string(),
            "test_schema".to_string(),
            "parted".to_string(),
            "postgres".to_string(),
            None,
            vec![
                int_column("test_schema", "parted", "a", 1),
                int_column("test_schema", "parted", "b", 2),
            ],
            vec![],
            vec![],
            vec![],
            None,
        );
        table.partition_key = Some(key.to_string());
        table.hash();
        table
    };
    let mut from = Dump::new(DumpConfig::default());
    let mut to = Dump::new(DumpConfig::default());
    from.tables.push(make("RANGE (a)"));
    to.tables.push(make("RANGE (b)"));
    let stat = statistic("test_schema", "st_parted", "parted", &["ndistinct"], "a, b");
    from.statistics.push(stat.clone());
    to.statistics.push(stat);
    (from, to)
}

#[tokio::test]
async fn statistics_on_a_recreated_table_are_created_again() {
    let (from, to) = recreated_table_dumps();
    let mut comparer = Comparer::new(from, to, true, false, true, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();

    let drop_table = script
        .find("drop table if exists test_schema.parted")
        .expect("partition-key change drops the table");
    let create = script
        .find(
            "create statistics test_schema.st_parted (ndistinct) on a, b from test_schema.parted;",
        )
        .expect("DROP TABLE took the statistics with it; they must be recreated");
    // The statistics went with the table, so the guarded drop before the
    // create is a no-op here; it is there for a table that was not dropped.
    let drop_stat = script
        .find("drop statistics if exists test_schema.st_parted;")
        .expect("the create is preceded by a guarded drop");
    assert!(drop_table < drop_stat && drop_stat < create, "{script}");
    assert_eq!(script.matches("create statistics").count(), 1, "{script}");
}

/// A partitioned parent and one partition, both with columns `id, v, w`,
/// and a statistics object on the partition. `v_type` is the type of `v`.
fn partition_child_dumps(v_type: &str) -> Vec<Table> {
    let make = |name: &str, partition_of: Option<&str>| {
        let mut v = int_column("test_schema", name, "v", 2);
        v.data_type = v_type.to_string();
        let mut table = Table::new(
            "test_schema".to_string(),
            name.to_string(),
            "test_schema".to_string(),
            name.to_string(),
            "postgres".to_string(),
            None,
            vec![
                int_column("test_schema", name, "id", 1),
                v,
                int_column("test_schema", name, "w", 3),
            ],
            vec![],
            vec![],
            vec![],
            None,
        );
        match partition_of {
            Some(parent) => {
                table.partition_of = Some(parent.to_string());
                table.partition_bound = Some("FOR VALUES FROM (0) TO (100)".to_string());
            }
            None => table.partition_key = Some("RANGE (id)".to_string()),
        }
        table.hash();
        table
    };
    vec![make("p", None), make("p1", Some("test_schema.p"))]
}

#[tokio::test]
async fn partition_child_column_type_change_keeps_its_statistics() {
    // A column type change on a partitioned parent reaches its partitions
    // through the parent's ALTER; the child is not dropped, even though
    // `Table::will_be_dropped_and_recreated` says so for it (it feeds
    // `recreated_tables`). Its unchanged statistics must be left alone —
    // creating them again fails with "already exists".
    let mut from = Dump::new(DumpConfig::default());
    let mut to = Dump::new(DumpConfig::default());
    from.tables = partition_child_dumps("integer");
    to.tables = partition_child_dumps("bigint");
    assert!(
        from.tables[1].will_be_dropped_and_recreated(&to.tables[1]),
        "precondition: the predicate over-reports this child"
    );
    let stat = statistic("test_schema", "st_child", "p1", &["ndistinct"], "v, w");
    from.statistics.push(stat.clone());
    to.statistics.push(stat);

    let mut comparer = Comparer::new(from, to, true, false, true, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("alter table test_schema.p alter column v type bigint"),
        "{script}"
    );
    assert!(!script.contains("drop table"), "{script}");
    assert!(!script.contains("statistics"), "{script}");
}

#[tokio::test]
async fn partition_child_column_type_change_sets_the_statistics_target_again() {
    // The parent's ALTER COLUMN ... TYPE recurses to the partition and
    // rebuilds the statistics there too, which resets their target.
    let mut from = Dump::new(DumpConfig::default());
    let mut to = Dump::new(DumpConfig::default());
    from.tables = partition_child_dumps("integer");
    to.tables = partition_child_dumps("bigint");
    let mut stat = statistic("test_schema", "st_child", "p1", &["ndistinct"], "v, w");
    stat.stxstattarget = Some(400);
    stat.hash();
    from.statistics.push(stat.clone());
    to.statistics.push(stat);
    for column in ["v", "w"] {
        from.column_dependents.push(ColumnDependent {
            schema: "test_schema".to_string(),
            table: "p1".to_string(),
            column: column.to_string(),
            kind: ColumnDependentKind::Statistic,
            dep_schema: "test_schema".to_string(),
            dep_table: "p1".to_string(),
            dep_name: "st_child".to_string(),
        });
    }

    let mut comparer = Comparer::new(from, to, true, false, true, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();

    let retype = script
        .find("alter table test_schema.p alter column v type bigint")
        .expect("the parent's type change");
    let target = script
        .find("alter statistics test_schema.st_child set statistics 400;")
        .expect("the partition's rebuilt statistics get their target back");
    assert!(retype < target, "{script}");
    assert!(!script.contains("create statistics"), "{script}");
}

#[tokio::test]
async fn partition_child_column_type_change_still_applies_a_statistics_change() {
    // Same child, but its statistics change kind: the ordinary drop +
    // create must run, not a bare create for a table that was never dropped.
    let mut from = Dump::new(DumpConfig::default());
    let mut to = Dump::new(DumpConfig::default());
    from.tables = partition_child_dumps("integer");
    to.tables = partition_child_dumps("bigint");
    from.statistics.push(statistic(
        "test_schema",
        "st_child",
        "p1",
        &["ndistinct"],
        "v, w",
    ));
    to.statistics
        .push(statistic("test_schema", "st_child", "p1", &["mcv"], "v, w"));

    let mut comparer = Comparer::new(from, to, true, false, true, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();

    let drop_stat = script
        .find("drop statistics if exists test_schema.st_child;")
        .expect("the kind change is a drop + create");
    let create = script
        .find("create statistics test_schema.st_child (mcv) on v, w from test_schema.p1;")
        .unwrap();
    assert!(drop_stat < create, "{script}");
    assert_eq!(script.matches("create statistics").count(), 1, "{script}");
}

#[tokio::test]
async fn statistics_on_the_partition_of_a_recreated_parent_are_created_again() {
    // A partition-key change drops the parent and, with it, every partition
    // and the statistics on them; the partition is created again, and so
    // are its statistics.
    let mut from = Dump::new(DumpConfig::default());
    let mut to = Dump::new(DumpConfig::default());
    from.tables = partition_child_dumps("integer");
    to.tables = partition_child_dumps("integer");
    to.tables[0].partition_key = Some("RANGE (w)".to_string());
    to.tables[0].hash();
    to.tables[1].partition_bound = Some("FOR VALUES FROM (0) TO (50)".to_string());
    to.tables[1].hash();
    let stat = statistic("test_schema", "st_child", "p1", &["ndistinct"], "v, w");
    from.statistics.push(stat.clone());
    to.statistics.push(stat);

    let mut comparer = Comparer::new(from, to, true, false, true, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();

    let create_child = script
        .find("create table test_schema.p1")
        .expect("the partition is created again");
    let create_stat = script
        .find("create statistics test_schema.st_child (ndistinct) on v, w from test_schema.p1;")
        .expect("and so are its statistics");
    assert!(create_child < create_stat, "{script}");
}

#[tokio::test]
async fn statistics_on_a_recreated_table_are_left_alone_without_use_drop() {
    // With use_drop off the DROP TABLE is commented out, so the statistics
    // survive and an unchanged one must not be created a second time.
    let (from, to) = recreated_table_dumps();
    let mut comparer = Comparer::new(from, to, false, false, true, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    assert!(!comparer.get_script().contains("create statistics"));
}

#[tokio::test]
async fn statistics_moved_onto_a_recreated_table_drop_the_old_object_first() {
    let (mut from, to) = recreated_table_dumps();
    // In FROM the object of that name sits on another table, which is not
    // being dropped, so it still exists when the new one is created.
    from.statistics[0].table_name = "other".to_string();
    from.statistics[0].hash();
    let mut comparer = Comparer::new(from, to, true, false, true, GrantsMode::Ignore);
    comparer.compare_tables().await.unwrap();
    comparer.compare_statistics().await.unwrap();
    let script = comparer.get_script();

    let drop_stat = script
        .find("drop statistics if exists test_schema.st_parted;")
        .expect("the FROM object is still there");
    let create = script
        .find(
            "create statistics test_schema.st_parted (ndistinct) on a, b from test_schema.parted;",
        )
        .unwrap();
    assert!(drop_stat < create, "{script}");
}

#[tokio::test]
async fn from_only_statistics_are_dropped_in_dump_order() {
    // The drops used to follow HashMap iteration order, which changes from
    // run to run; they now follow the dump (ordered by schema, name).
    let mut from = Dump::new(DumpConfig::default());
    let to = Dump::new(DumpConfig::default());
    let names = ["st_a", "st_b", "st_c", "st_d", "st_e", "st_f"];
    for name in names {
        from.statistics
            .push(statistic("test_schema", name, "t", &["ndistinct"], "a, b"));
    }
    let mut comparer = Comparer::new(from, to, true, false, true, GrantsMode::Ignore);
    comparer.compare_statistics().await.unwrap();
    let script = comparer.get_script();

    let positions: Vec<usize> = names
        .iter()
        .map(|name| {
            script
                .find(&format!("drop statistics if exists test_schema.{name};"))
                .unwrap()
        })
        .collect();
    assert!(positions.windows(2).all(|w| w[0] < w[1]), "{script}");
}

#[tokio::test]
async fn expression_statistics_are_created_with_a_valid_kind_list() {
    let from = Dump::new(DumpConfig::default());
    let mut to = Dump::new(DumpConfig::default());
    to.statistics.push(statistic(
        "test_schema",
        "st_multi",
        "t",
        &["ndistinct", "dependencies", "mcv", "expressions"],
        "a, (v)",
    ));
    to.statistics.push(statistic(
        "test_schema",
        "st_uni",
        "t",
        &["expressions"],
        "(v)",
    ));
    let mut comparer = Comparer::new(from, to, true, false, true, GrantsMode::Ignore);
    comparer.compare_statistics().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains(
            "create statistics test_schema.st_multi (ndistinct, dependencies, mcv) on a, (v) from test_schema.t;"
        ),
        "{script}"
    );
    assert!(
        script.contains("create statistics test_schema.st_uni on (v) from test_schema.t;"),
        "{script}"
    );
    assert!(!script.contains("expressions"), "{script}");
}

/// `test_deps.<name> (value, gen_col GENERATED ALWAYS AS
/// (test_deps.compute(value)) STORED)`, partitioned by `value` when
/// `partition_of` is `None`, else a partition of that table.
fn generated_table(name: &str, partition_of: Option<&str>) -> Table {
    let mut gen_col = int_column("test_deps", name, "gen_col", 2);
    gen_col.is_generated = "ALWAYS".to_string();
    gen_col.generation_expression = Some("test_deps.compute(value)".to_string());
    gen_col.generation_type = Some("s".to_string());
    let mut table = Table::new(
        "test_deps".to_string(),
        name.to_string(),
        "test_deps".to_string(),
        name.to_string(),
        "postgres".to_string(),
        None,
        vec![int_column("test_deps", name, "value", 1), gen_col],
        vec![],
        vec![],
        vec![],
        None,
    );
    match partition_of {
        Some(parent) => {
            table.partition_of = Some(parent.to_string());
            table.partition_bound = Some("FOR VALUES FROM (0) TO (100)".to_string());
        }
        None => table.partition_key = Some("RANGE (value)".to_string()),
    }
    table.hash();
    table
}

fn gen_col_statistic(name: &str, table: &str) -> Statistic {
    let mut stat = statistic("test_deps", name, table, &["ndistinct"], "value, gen_col");
    stat.table_schema = "test_deps".to_string();
    stat.hash();
    stat
}

fn gen_col_dependent(table: &str, name: &str) -> ColumnDependent {
    ColumnDependent {
        schema: "test_deps".to_string(),
        table: table.to_string(),
        column: "gen_col".to_string(),
        kind: ColumnDependentKind::Statistic,
        dep_schema: "test_deps".to_string(),
        dep_table: table.to_string(),
        dep_name: name.to_string(),
    }
}

/// FROM / TO dumps where `test_deps.compute` changes its return type, so
/// Phase 7 drops it with CASCADE and re-adds the generated columns calling it.
fn compute_signature_change() -> (Dump, Dump) {
    let mut from = Dump::new(DumpConfig::default());
    let mut to = Dump::new(DumpConfig::default());
    from.routines
        .push(issue179_compute_routine("integer", "SELECT x * 2;"));
    to.routines.push(issue179_compute_routine(
        "bigint",
        "SELECT (x * 2)::bigint;",
    ));
    (from, to)
}

#[tokio::test]
async fn phase7_restores_statistics_on_the_partitions_of_a_readded_column() {
    // Phase 7 re-adds the column on the partitioned parent only, but a
    // statistics object defined on a partition names the partition's own
    // column, so its pg_depend edge is anchored there.
    let (mut from, mut to) = compute_signature_change();
    for dump in [&mut from, &mut to] {
        dump.tables.push(generated_table("gp", None));
        dump.tables
            .push(generated_table("gp1", Some("test_deps.gp")));
        dump.statistics.push(gen_col_statistic("st_parent", "gp"));
        dump.statistics.push(gen_col_statistic("st_child", "gp1"));
    }
    from.column_dependents
        .push(gen_col_dependent("gp", "st_parent"));
    from.column_dependents
        .push(gen_col_dependent("gp1", "st_child"));

    let mut comparer = Comparer::new(from, to, true, false, true, GrantsMode::Ignore);
    comparer.compare_routines_and_views().await.unwrap();
    let script = comparer.get_script();

    let add = script
        .find("alter table test_deps.gp add column if not exists gen_col")
        .expect("Phase 7 re-adds the generated column on the parent");
    assert!(
        !script.contains("alter table test_deps.gp1 add column"),
        "the parent's ADD COLUMN reaches the partition: {script}"
    );
    for (name, table) in [("st_parent", "gp"), ("st_child", "gp1")] {
        let create = script
            .find(&format!(
                "create statistics test_deps.{name} (ndistinct) on value, gen_col from test_deps.{table};"
            ))
            .unwrap_or_else(|| panic!("{name} went with the column: {script}"));
        assert!(add < create, "{script}");
    }
}

#[tokio::test]
async fn phase7_restores_to_only_statistics_created_before_the_cascade() {
    // compare_statistics creates the new object before Phase 7's DROP
    // FUNCTION ... CASCADE takes the column, and the object with it. Only
    // the TO dump's column graph knows it names the column.
    let (mut from, mut to) = compute_signature_change();
    from.tables.push(generated_table("items_new", None));
    to.tables.push(generated_table("items_new", None));
    to.statistics.push(gen_col_statistic("st_new", "items_new"));
    to.column_dependents
        .push(gen_col_dependent("items_new", "st_new"));

    let mut comparer = Comparer::new(from, to, true, false, true, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();

    let first_create = script
        .find("create statistics test_deps.st_new")
        .expect("compare_statistics creates it");
    let cascade = script
        .find("drop function if exists test_deps.compute")
        .expect("the signature change drops the routine");
    let add = script
        .find("add column if not exists gen_col")
        .expect("Phase 7 re-adds the column");
    let restore = script
        .rfind("create statistics test_deps.st_new (ndistinct) on value, gen_col from test_deps.items_new;")
        .unwrap();
    assert!(
        first_create < cascade && cascade < add && add < restore,
        "{script}"
    );
}

/// `test_schema.<name>` as a materialized view over `test_schema.t`.
fn matview(name: &str, definition: &str) -> View {
    let mut view = View::new(
        name.to_string(),
        definition.to_string(),
        "test_schema".to_string(),
        vec!["test_schema.t".to_string()],
    );
    view.is_materialized = true;
    view.hash();
    view
}

#[tokio::test]
async fn statistics_on_a_new_materialized_view_follow_the_view() {
    let from = Dump::new(DumpConfig::default());
    let mut to = Dump::new(DumpConfig::default());
    to.views
        .push(matview("mv2", " SELECT a, c FROM test_schema.t;"));
    to.statistics.push(statistic(
        "test_schema",
        "st_mv2",
        "mv2",
        &["ndistinct"],
        "a, c",
    ));

    let mut comparer = Comparer::new(from, to, true, false, true, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();

    let view = script
        .find("create materialized view test_schema.mv2")
        .expect("the view is created");
    let stat = script
        .find("create statistics test_schema.st_mv2 (ndistinct) on a, c from test_schema.mv2;")
        .expect("and its statistics");
    assert!(view < stat, "{script}");
}

#[tokio::test]
async fn statistics_on_a_rebuilt_materialized_view_are_created_again() {
    // A changed materialized view is dropped before the tables and created
    // again later; the statistics on it went with the drop although they
    // did not change.
    let mut from = Dump::new(DumpConfig::default());
    let mut to = Dump::new(DumpConfig::default());
    from.views
        .push(matview("mv", " SELECT a, b FROM test_schema.t;"));
    to.views
        .push(matview("mv", " SELECT a, b, c FROM test_schema.t;"));
    let stat = statistic("test_schema", "st_mv", "mv", &["ndistinct"], "a, b");
    from.statistics.push(stat.clone());
    to.statistics.push(stat);

    let mut comparer = Comparer::new(from, to, true, false, true, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();

    let drop_view = script
        .find("drop materialized view if exists test_schema.mv")
        .expect("the changed view is dropped");
    let create_view = script
        .find("create materialized view test_schema.mv")
        .expect("and created again");
    let stat = script
        .find("create statistics test_schema.st_mv (ndistinct) on a, b from test_schema.mv;")
        .expect("its statistics are created again");
    assert!(drop_view < create_view && create_view < stat, "{script}");
    assert_eq!(script.matches("create statistics").count(), 1, "{script}");

    // With use_drop off the view is not dropped, so neither are they.
    let mut from = Dump::new(DumpConfig::default());
    let mut to = Dump::new(DumpConfig::default());
    from.views
        .push(matview("mv", " SELECT a, b FROM test_schema.t;"));
    to.views
        .push(matview("mv", " SELECT a, b, c FROM test_schema.t;"));
    let stat = statistic("test_schema", "st_mv", "mv", &["ndistinct"], "a, b");
    from.statistics.push(stat.clone());
    to.statistics.push(stat);
    let mut comparer = Comparer::new(from, to, false, false, true, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    assert!(!comparer.get_script().contains("statistics"));
}

#[tokio::test]
async fn statistics_moved_onto_a_new_materialized_view_drop_the_old_object_first() {
    // In FROM the object of that name sits on a table that stays; it must be
    // dropped before the new one is created on the new view — and with
    // use_drop off neither may run.
    let dumps = || {
        let mut from = Dump::new(DumpConfig::default());
        let mut to = Dump::new(DumpConfig::default());
        from.statistics.push(statistic(
            "test_schema",
            "st_moved",
            "t",
            &["ndistinct"],
            "a, c",
        ));
        to.views
            .push(matview("mv2", " SELECT a, c FROM test_schema.t;"));
        to.statistics.push(statistic(
            "test_schema",
            "st_moved",
            "mv2",
            &["ndistinct"],
            "a, c",
        ));
        (from, to)
    };

    let (from, to) = dumps();
    let mut comparer = Comparer::new(from, to, true, false, true, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();
    let view = script
        .find("create materialized view test_schema.mv2")
        .unwrap();
    let drop_stat = script
        .find("drop statistics if exists test_schema.st_moved;")
        .expect("the FROM object is still there");
    let create = script
        .find("create statistics test_schema.st_moved (ndistinct) on a, c from test_schema.mv2;")
        .unwrap();
    assert!(view < drop_stat && drop_stat < create, "{script}");

    let (from, to) = dumps();
    let mut comparer = Comparer::new(from, to, false, false, true, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();
    assert!(
        script.contains("-- drop statistics if exists test_schema.st_moved;"),
        "{script}"
    );
    assert!(
        script.contains("-- create statistics test_schema.st_moved (ndistinct)"),
        "{script}"
    );
    assert!(!script.contains("\ncreate statistics"), "{script}");
}

#[tokio::test]
async fn phase7_restores_statistics_on_a_cascade_dropped_materialized_view() {
    let (mut from, mut to) = compute_signature_change();
    for dump in [&mut from, &mut to] {
        dump.views.push(issue189_view("mv_compute", true));
        let mut stat = statistic("test_deps", "st_mv", "mv_compute", &["ndistinct"], "c");
        stat.table_schema = "test_deps".to_string();
        stat.hash();
        dump.statistics.push(stat);
    }

    let mut comparer = Comparer::new(from, to, true, false, true, GrantsMode::Ignore);
    comparer.compare_routines_and_views().await.unwrap();
    let script = comparer.get_script();

    let view = script
        .find("create materialized view if not exists test_deps.mv_compute")
        .expect("Phase 7 restores the view");
    let stat = script
        .find("create statistics test_deps.st_mv (ndistinct) on c from test_deps.mv_compute;")
        .expect("and the statistics on it");
    assert!(view < stat, "{script}");
}

/// A statistics object with target 77 in FROM and none in TO, with FROM
/// taken from `from_version` and TO from `to_version` (`None`: a dump
/// written before pgc 1.1.0).
async fn target_reset_script(
    from_version: Option<i32>,
    to_version: Option<i32>,
    to_kinds: &[&str],
) -> String {
    let mut from = Dump::new(DumpConfig::default());
    let mut to = Dump::new(DumpConfig::default());
    from.server_version_num = from_version;
    to.server_version_num = to_version;
    let mut stat = statistic("test_schema", "st", "t", &["mcv"], "a, b");
    stat.stxstattarget = Some(77);
    stat.hash();
    from.statistics.push(stat);
    to.statistics
        .push(statistic("test_schema", "st", "t", to_kinds, "a, b"));
    let mut comparer = Comparer::new(from, to, true, false, true, GrantsMode::Ignore);
    comparer.compare_statistics().await.unwrap();
    comparer.get_script()
}

#[tokio::test]
async fn a_pre_1_1_to_dump_of_pg17_does_not_reset_statistics_targets() {
    // pgc 1.0.x could not read stxstattarget on PostgreSQL 17+, so a TO
    // dump it wrote has no target anywhere; that is "unknown", not "reset".
    let script = target_reset_script(Some(190000), None, &["mcv"]).await;
    assert!(!script.contains("set statistics"), "{script}");

    // A redefinition keeps the FROM target rather than dropping it.
    let script = target_reset_script(Some(170000), None, &["ndistinct"]).await;
    assert!(
        script.contains("create statistics test_schema.st (ndistinct) on a, b from test_schema.t;"),
        "{script}"
    );
    assert!(
        script.contains("alter statistics test_schema.st set statistics 77;"),
        "{script}"
    );
}

#[tokio::test]
async fn statistics_targets_are_reset_whenever_the_to_dump_can_tell() {
    let reset = "alter statistics test_schema.st set statistics -1;";
    // Both dumps written by pgc 1.1.0.
    let script = target_reset_script(Some(190000), Some(190000), &["mcv"]).await;
    assert!(script.contains(reset), "{script}");
    // Both written by pgc 1.0.x: compared exactly as before.
    let script = target_reset_script(None, None, &["mcv"]).await;
    assert!(script.contains(reset), "{script}");
    // FROM on PostgreSQL 16, where pgc 1.0.x read targets too.
    let script = target_reset_script(Some(160000), None, &["mcv"]).await;
    assert!(script.contains(reset), "{script}");

    // A TO dump in which some object has a target could read them.
    let mut from = Dump::new(DumpConfig::default());
    let mut to = Dump::new(DumpConfig::default());
    from.server_version_num = Some(190000);
    let mut stat = statistic("test_schema", "st", "t", &["mcv"], "a, b");
    stat.stxstattarget = Some(77);
    stat.hash();
    from.statistics.push(stat);
    to.statistics
        .push(statistic("test_schema", "st", "t", &["mcv"], "a, b"));
    let mut other = statistic("test_schema", "st_other", "t", &["mcv"], "b, a");
    other.stxstattarget = Some(10);
    other.hash();
    from.statistics.push(other.clone());
    to.statistics.push(other);
    let mut comparer = Comparer::new(from, to, true, false, true, GrantsMode::Ignore);
    comparer.compare_statistics().await.unwrap();
    assert!(comparer.get_script().contains(reset));
}

// ---- pre-columns: plain-column order ---------------------------------------

async fn pre_columns_statistics_script(from: Statistic, to: Statistic) -> String {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());
    from_dump.statistics.push(from);
    to_dump.statistics.push(to);
    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_statistics().await.unwrap();
    comparer.get_script()
}

#[tokio::test]
async fn pre_columns_statistics_differing_only_in_plain_column_order_are_left_alone() {
    // PostgreSQL prints the plain columns in attnum order: on a table whose
    // `qty` was dropped and re-added (or added later on one side) the same
    // object prints `ON flag, qty` here and `ON qty, flag` there. It used to
    // be dropped and created again on every compare.
    let from = statistic("test_schema", "st", "t", &["ndistinct"], "flag, qty");
    let to = statistic("test_schema", "st", "t", &["ndistinct"], "qty, flag");
    assert_ne!(from.hash, to.hash);
    let script = pre_columns_statistics_script(from, to).await;
    assert!(!script.contains("statistics test_schema.st"), "{script}");

    // With expressions, which keep their order after the plain columns.
    let from = statistic(
        "test_schema",
        "st",
        "t",
        &["ndistinct", "expressions"],
        "flag, qty, lower(code), (qty + 1)",
    );
    let to = statistic(
        "test_schema",
        "st",
        "t",
        &["ndistinct", "expressions"],
        "qty, flag, lower(code), (qty + 1)",
    );
    let script = pre_columns_statistics_script(from, to).await;
    assert!(!script.contains("statistics test_schema.st"), "{script}");
}

#[tokio::test]
async fn pre_columns_statistics_on_other_columns_are_still_re_created() {
    let from = statistic("test_schema", "st", "t", &["ndistinct"], "flag, qty");
    let to = statistic("test_schema", "st", "t", &["ndistinct"], "qty, code");
    let script = pre_columns_statistics_script(from, to).await;
    let drop = script
        .find("drop statistics if exists test_schema.st;")
        .expect(&script);
    let create = script
        .find("create statistics test_schema.st (ndistinct) on qty, code from test_schema.t;")
        .expect(&script);
    assert!(drop < create, "{script}");

    // Expressions in another order are another object.
    let from = statistic(
        "test_schema",
        "st",
        "t",
        &["ndistinct", "expressions"],
        "qty, lower(code), (qty + 1)",
    );
    let to = statistic(
        "test_schema",
        "st",
        "t",
        &["ndistinct", "expressions"],
        "qty, (qty + 1), lower(code)",
    );
    let script = pre_columns_statistics_script(from, to).await;
    assert!(
        script.contains("drop statistics if exists test_schema.st;"),
        "{script}"
    );
}
