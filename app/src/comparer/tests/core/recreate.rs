//! Tests for tables the table phase drops and creates again from TO (a
//! partition-key change, a plain table turned into a partitioned one): the
//! partitions their `DROP TABLE` takes along (`Comparer::recreated_families`),
//! the sequences their columns own, and the foreign keys referencing them
//! (`Comparer::plan_recreate_fk_predrops`).

use super::helpers::*;
use crate::comparer::core::*;
use crate::config::dump_config::DumpConfig;
use crate::config::grants_mode::GrantsMode;
use crate::dump::sequence::Sequence;
use crate::dump::statistic::Statistic;
use crate::dump::table::Table;
use crate::dump::table_constraint::TableConstraint;
use crate::dump::table_trigger::TableTrigger;
use sqlx::postgres::types::Oid;

/// A table `s.<name>` with integer columns `id` and `k`.
fn table(name: &str) -> Table {
    Table::new(
        "s".to_string(),
        name.to_string(),
        "s".to_string(),
        name.to_string(),
        "postgres".to_string(),
        None,
        vec![
            int_column("s", name, "id", 1),
            int_column("s", name, "k", 2),
        ],
        vec![],
        vec![],
        vec![],
        None,
    )
}

/// `s.<name>`, partitioned by `key`.
fn partitioned(name: &str, key: &str) -> Table {
    let mut t = table(name);
    t.partition_key = Some(key.to_string());
    t.hash();
    t
}

/// `s.<name>`, a partition of `s.<parent>` for `bound`.
fn partition(name: &str, parent: &str, bound: &str) -> Table {
    let mut t = table(name);
    t.partition_of = Some(format!("s.{parent}"));
    t.partition_bound = Some(bound.to_string());
    t.hash();
    t
}

/// A foreign key on `s.<table>` named `name` with `definition`.
fn foreign_key(table: &str, name: &str, definition: &str) -> TableConstraint {
    TableConstraint {
        catalog: "postgres".to_string(),
        schema: "s".to_string(),
        name: name.to_string(),
        table_name: table.to_string(),
        constraint_type: "FOREIGN KEY".to_string(),
        is_deferrable: false,
        initially_deferred: false,
        definition: Some(definition.to_string()),
        coninhcount: 0,
        is_enforced: true,
        no_inherit: false,
        nulls_not_distinct: false,
        comment: None,
    }
}

/// `s.<name>` with foreign keys `fks`.
fn with_fks(mut t: Table, fks: Vec<TableConstraint>) -> Table {
    t.constraints = fks;
    t.hash();
    t
}

/// A row trigger `name` on `s.<table>`.
fn trigger(table: &str, name: &str) -> TableTrigger {
    TableTrigger {
        oid: Oid(1),
        name: name.to_string(),
        definition: format!(
            "CREATE TRIGGER {name} BEFORE INSERT ON s.{table} FOR EACH ROW EXECUTE FUNCTION s.touch()"
        ),
        enabled: "O".to_string(),
        comment: None,
    }
}

/// FROM `s.pt` partitioned by RANGE (id) with the unchanged partition `pt1`,
/// TO the same partitioned by RANGE (k): a partition-key change whose
/// partition keeps its bound, so its own hash does not change.
fn key_change_dumps() -> (Dump, Dump) {
    let mut from = Dump::new(DumpConfig::default());
    let mut to = Dump::new(DumpConfig::default());
    from.tables.push(partitioned("pt", "RANGE (id)"));
    from.tables
        .push(partition("pt1", "pt", "FOR VALUES FROM (0) TO (10)"));
    to.tables.push(partitioned("pt", "RANGE (k)"));
    to.tables
        .push(partition("pt1", "pt", "FOR VALUES FROM (0) TO (10)"));
    (from, to)
}

async fn compared(from: Dump, to: Dump, use_drop: bool) -> (Comparer, String) {
    let mut comparer = Comparer::new(from, to, use_drop, false, true, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();
    (comparer, script)
}

fn position(script: &str, needle: &str) -> usize {
    script
        .find(needle)
        .unwrap_or_else(|| panic!("`{needle}` not found in:\n{script}"))
}

#[test]
fn recreated_families_take_every_partition_to_the_topmost_root() {
    // pt is a root (key change); its partition pt1 is itself sub-partitioned
    // and changes its own key too, but it goes with pt, and so does pt1's
    // unchanged partition pt1a. other is unchanged and stays out.
    let mut from = Dump::new(DumpConfig::default());
    let mut to = Dump::new(DumpConfig::default());
    from.tables.push(partitioned("pt", "RANGE (id)"));
    to.tables.push(partitioned("pt", "RANGE (k)"));
    let mut pt1_from = partition("pt1", "pt", "FOR VALUES FROM (0) TO (10)");
    pt1_from.partition_key = Some("LIST (id)".to_string());
    pt1_from.hash();
    let mut pt1_to = pt1_from.clone();
    pt1_to.partition_key = Some("LIST (k)".to_string());
    pt1_to.hash();
    from.tables.push(pt1_from);
    to.tables.push(pt1_to);
    let pt1a = partition("pt1a", "pt1", "FOR VALUES IN (1)");
    from.tables.push(pt1a.clone());
    to.tables.push(pt1a);
    from.tables.push(table("other"));
    to.tables.push(table("other"));

    let comparer = Comparer::new(from, to, true, false, true, GrantsMode::Ignore);
    let families = comparer.recreated_families();
    let pt = Comparer::table_key("s", "pt");
    assert_eq!(families.roots, [pt.clone()].into_iter().collect());
    assert_eq!(
        families.root_of(&Comparer::table_key("s", "pt1")),
        Some(pt.as_str())
    );
    assert_eq!(
        families.root_of(&Comparer::table_key("s", "pt1a")),
        Some(pt.as_str())
    );
    assert_eq!(families.root_of(&Comparer::table_key("s", "other")), None);
}

#[test]
fn recreated_families_are_empty_without_a_recreate() {
    let (from, _) = key_change_dumps();
    let comparer = Comparer::new(from.clone(), from, true, false, true, GrantsMode::Ignore);
    let families = comparer.recreated_families();
    assert!(families.is_empty());
    assert!(families.members.is_empty());
}

#[tokio::test]
async fn unchanged_partition_is_created_again_with_its_parent() {
    let (from, to) = key_change_dumps();
    let (comparer, script) = compared(from, to, true).await;

    let drop_parent = position(&script, "drop table if exists s.pt;");
    let create_parent = position(&script, "create table s.pt (");
    let create_child = position(&script, "create table s.pt1 partition of s.pt");
    assert!(
        drop_parent < create_parent && create_parent < create_child,
        "{script}"
    );
    assert_eq!(script.matches("create table s.pt1 ").count(), 1, "{script}");
    // The later passes restore what went with the partition.
    let key = Comparer::table_key("s", "pt1");
    assert!(comparer.recreated_tables.contains(&key));
    assert!(comparer.dropped_and_recreated_tables.contains(&key));
}

#[tokio::test]
async fn unchanged_grandchild_of_a_recreated_partition_is_created_again() {
    // pt1 changes its bound (so its own hash differs) and keeps its own
    // sub-partitioning; the grandchild pt1a is unchanged.
    let mut from = Dump::new(DumpConfig::default());
    let mut to = Dump::new(DumpConfig::default());
    from.tables.push(partitioned("pt", "RANGE (id)"));
    to.tables.push(partitioned("pt", "RANGE (k)"));
    for (dump, bound) in [
        (&mut from, "FOR VALUES FROM (0) TO (10)"),
        (&mut to, "FOR VALUES FROM (0) TO (20)"),
    ] {
        let mut pt1 = partition("pt1", "pt", bound);
        pt1.partition_key = Some("LIST (k)".to_string());
        pt1.hash();
        dump.tables.push(pt1);
        dump.tables
            .push(partition("pt1a", "pt1", "FOR VALUES IN (1)"));
    }
    // A partition only TO has waits until its recreated parent is back.
    to.tables
        .push(partition("pt1b", "pt1", "FOR VALUES IN (2)"));

    let (_, script) = compared(from, to, true).await;
    let create_pt1 = position(&script, "create table s.pt1 partition of s.pt");
    let create_pt1a = position(&script, "create table s.pt1a partition of s.pt1");
    let create_pt1b = position(&script, "create table s.pt1b partition of s.pt1");
    assert!(
        create_pt1 < create_pt1a && create_pt1 < create_pt1b,
        "{script}"
    );
    assert!(!script.contains("detach partition"), "{script}");
}

#[tokio::test]
async fn unchanged_partition_is_left_alone_without_use_drop() {
    // The DROP TABLE is only commented out, so the partition is still there.
    let (from, to) = key_change_dumps();
    let (comparer, script) = compared(from, to, false).await;
    assert!(script.contains("-- drop table if exists s.pt;"), "{script}");
    assert!(!script.contains("create table s.pt1 "), "{script}");
    assert!(
        !comparer
            .recreated_tables
            .contains(&Comparer::table_key("s", "pt1"))
    );
}

#[tokio::test]
async fn view_and_statistics_on_an_unchanged_partition_are_restored() {
    let (mut from, mut to) = key_change_dumps();
    let mut view = View::new(
        "v_pt1".to_string(),
        " SELECT id\n   FROM s.pt1;".to_string(),
        "s".to_string(),
        vec!["s.pt1".to_string()],
    );
    view.hash();
    from.views.push(view.clone());
    to.views.push(view);
    let mut stat = Statistic::new(
        "s".to_string(),
        "st_pt1".to_string(),
        "postgres".to_string(),
        "s".to_string(),
        "pt1".to_string(),
        vec!["ndistinct".to_string()],
        vec!["id".to_string(), "k".to_string()],
        "CREATE STATISTICS s.st_pt1 (ndistinct) ON id, k FROM s.pt1".to_string(),
    );
    stat.columns_def = Some("id, k".to_string());
    stat.hash();
    from.statistics.push(stat.clone());
    to.statistics.push(stat);

    let (_, script) = compared(from, to, true).await;
    let drop_view = position(&script, "drop view if exists s.v_pt1;");
    let drop_parent = position(&script, "drop table if exists s.pt;");
    let create_child = position(&script, "create table s.pt1 partition of s.pt");
    let create_stat = position(
        &script,
        "create statistics s.st_pt1 (ndistinct) on id, k from s.pt1;",
    );
    let create_view = position(&script, "CREATE OR REPLACE VIEW s.v_pt1");
    assert!(drop_view < drop_parent, "{script}");
    assert!(
        create_child < create_stat && create_child < create_view,
        "{script}"
    );
}

#[tokio::test]
async fn grants_on_an_unchanged_partition_are_granted_again() {
    let (mut from, mut to) = key_change_dumps();
    from.tables[1].acl = vec!["reader=r/postgres".to_string()];
    to.tables[1].acl = vec!["reader=r/postgres".to_string()];
    let mut comparer = Comparer::new(from, to, true, false, true, GrantsMode::Full);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();
    let create_child = position(&script, "create table s.pt1 partition of s.pt");
    let grant = position(&script, "GRANT SELECT ON TABLE s.pt1 TO reader;");
    assert!(create_child < grant, "{script}");
}

#[tokio::test]
async fn parent_trigger_clones_are_not_created_on_recreated_partitions() {
    // PostgreSQL clones the parent's row trigger onto every partition; the
    // dump keeps the clone apart from the partition's own triggers (see
    // `Table::trigger_clones`). The partition created under the recreated
    // parent gets it by itself; only its own trigger is created.
    let (mut from, mut to) = key_change_dumps();
    for dump in [&mut from, &mut to] {
        dump.tables[0].triggers = vec![trigger("pt", "trg_parent")];
        dump.tables[0].hash();
        dump.tables[1].triggers = vec![trigger("pt1", "trg_own")];
        dump.tables[1].hash();
    }
    // The clone's own state differs from its parent's, so TO records it:
    // set it again.
    let mut clone = trigger("pt1", "trg_parent");
    clone.enabled = "D".to_string();
    to.tables[1].trigger_clones = vec![clone];
    to.tables[1].hash();

    let (_, script) = compared(from, to, true).await;
    assert_eq!(
        script
            .matches("CREATE TRIGGER trg_parent BEFORE INSERT ON s.pt FOR")
            .count(),
        1,
        "{script}"
    );
    assert!(
        !script.contains("CREATE TRIGGER trg_parent BEFORE INSERT ON s.pt1 "),
        "{script}"
    );
    assert!(
        script.contains("CREATE TRIGGER trg_own BEFORE INSERT ON s.pt1 "),
        "{script}"
    );
    assert!(
        script.contains("alter table s.pt1 disable trigger trg_parent;"),
        "{script}"
    );
}

#[tokio::test]
async fn recreated_partition_root_leaves_its_parent_trigger_clones_out() {
    // pt stays; its partition pt1 changes its own partition key, so pt1 is
    // the root. Creating it as a partition of pt clones pt's row trigger.
    let mut from = Dump::new(DumpConfig::default());
    let mut to = Dump::new(DumpConfig::default());
    for (dump, key) in [(&mut from, "LIST (id)"), (&mut to, "LIST (k)")] {
        let mut pt = partitioned("pt", "RANGE (id)");
        pt.triggers = vec![trigger("pt", "trg_parent")];
        pt.hash();
        // pt1's clone of pt's row trigger is in its parent's state, so the
        // dump does not list it (see `Table::trigger_clones`).
        let mut pt1 = partition("pt1", "pt", "FOR VALUES FROM (0) TO (10)");
        pt1.partition_key = Some(key.to_string());
        pt1.hash();
        dump.tables.push(pt);
        dump.tables.push(pt1);
    }
    let (_, script) = compared(from, to, true).await;
    position(&script, "drop table if exists s.pt1;");
    position(&script, "create table s.pt1 partition of s.pt");
    assert!(!script.contains("ON s.pt1 FOR EACH ROW"), "{script}");
}

/// The `s.pt_id_seq` sequence of a `serial` column `s.pt.id`.
fn serial_sequence() -> Sequence {
    Sequence::new(
        "s".to_string(),
        "pt_id_seq".to_string(),
        "postgres".to_string(),
        "integer".to_string(),
        Some(1),
        Some(1),
        Some(i64::from(i32::MAX)),
        Some(1),
        false,
        Some(1),
        Some(42),
        Some("s".to_string()),
        Some("pt".to_string()),
        Some("id".to_string()),
    )
}

/// `key_change_dumps` with `s.pt.id` a `serial` column owning `s.pt_id_seq`.
fn serial_key_change_dumps() -> (Dump, Dump) {
    let (mut from, mut to) = key_change_dumps();
    for dump in [&mut from, &mut to] {
        let pt = &mut dump.tables[0];
        pt.columns[0].column_default = Some("nextval('s.pt_id_seq'::regclass)".to_string());
        pt.columns[0].is_nullable = false;
        pt.hash();
        dump.sequences.push(serial_sequence());
    }
    (from, to)
}

#[tokio::test]
async fn serial_sequence_is_detached_before_the_drop_and_owned_again_after() {
    let (from, to) = serial_key_change_dumps();
    let (_, script) = compared(from, to, true).await;

    let detach = position(&script, "alter sequence s.pt_id_seq owned by none;");
    let drop_parent = position(&script, "drop table if exists s.pt;");
    let create_parent = position(&script, "create table s.pt (");
    let create_child = position(&script, "create table s.pt1 partition of s.pt");
    let reattach = position(&script, "alter sequence s.pt_id_seq owned by s.pt.id;");
    assert!(detach < drop_parent, "{script}");
    assert!(
        create_parent < reattach && create_child < reattach,
        "{script}"
    );
    // Kept, not recreated: its value and privileges survive.
    assert!(!script.contains("create sequence"), "{script}");
    assert!(!script.contains("drop sequence"), "{script}");
    assert!(
        script.contains("id integer default nextval('s.pt_id_seq'::regclass) not null"),
        "{script}"
    );
}

#[tokio::test]
async fn serial_sequence_stays_owned_without_use_drop() {
    let (from, to) = serial_key_change_dumps();
    let (_, script) = compared(from, to, false).await;
    assert!(!script.contains("owned by none"), "{script}");
    assert!(
        !script.contains("alter sequence s.pt_id_seq owned by"),
        "{script}"
    );
}

#[tokio::test]
async fn identity_sequence_is_not_detached() {
    // PostgreSQL refuses to detach an identity sequence; the identity column
    // of the new table creates its own.
    let (mut from, mut to) = key_change_dumps();
    for dump in [&mut from, &mut to] {
        let mut sequence = serial_sequence();
        sequence.set_is_identity(true);
        dump.sequences.push(sequence);
    }
    let (_, script) = compared(from, to, true).await;
    assert!(!script.contains("owned by none"), "{script}");
}

/// `key_change_dumps` plus `s.pt1_ref`, whose FROM foreign key
/// `ref_fk` references `s.pt`, and `to_fk` as the TO side of it.
fn referencing_dumps(to_fk: Option<&str>) -> (Dump, Dump) {
    let (mut from, mut to) = key_change_dumps();
    let from_fk = foreign_key("ref", "ref_fk", "FOREIGN KEY (a, b) REFERENCES s.pt(id, k)");
    from.tables.push(with_fks(table("ref"), vec![from_fk]));
    let to_fks = to_fk
        .map(|def| vec![foreign_key("ref", "ref_fk", def)])
        .unwrap_or_default();
    to.tables.push(with_fks(table("ref"), to_fks));
    (from, to)
}

#[tokio::test]
async fn referencing_foreign_key_is_dropped_before_the_recreate_and_added_back() {
    let (from, to) = referencing_dumps(Some("FOREIGN KEY (a, b) REFERENCES s.pt(id, k)"));
    let (comparer, script) = compared(from, to, true).await;

    let drop_fk = position(&script, "alter table s.ref drop constraint ref_fk;");
    let drop_parent = position(&script, "drop table if exists s.pt;");
    let create_child = position(&script, "create table s.pt1 partition of s.pt");
    let add_fk = position(
        &script,
        "alter table s.ref add constraint ref_fk foreign key (a, b) references s.pt(id, k)",
    );
    assert!(drop_fk < drop_parent && create_child < add_fk, "{script}");
    assert_eq!(
        script.matches("drop constraint ref_fk").count(),
        1,
        "{script}"
    );
    assert_eq!(
        script.matches("add constraint ref_fk").count(),
        1,
        "{script}"
    );
    // The FROM tables are the dump's own again after the table phase.
    assert_eq!(comparer.recreate_fk_predrops.len(), 1);
}

#[tokio::test]
async fn changed_referencing_foreign_key_is_dropped_once_and_added_from_to() {
    let (from, to) = referencing_dumps(Some(
        "FOREIGN KEY (a, b) REFERENCES s.pt(id, k) ON DELETE CASCADE",
    ));
    let (_, script) = compared(from, to, true).await;
    assert_eq!(
        script.matches("drop constraint ref_fk").count(),
        1,
        "{script}"
    );
    assert_eq!(
        script.matches("add constraint ref_fk").count(),
        1,
        "{script}"
    );
    assert!(
        script.contains("references s.pt(id, k) on delete cascade"),
        "{script}"
    );
    let drop_fk = position(&script, "alter table s.ref drop constraint ref_fk;");
    assert!(
        drop_fk < position(&script, "drop table if exists s.pt;"),
        "{script}"
    );
}

#[tokio::test]
async fn referencing_foreign_key_gone_from_to_is_dropped_once() {
    let (from, to) = referencing_dumps(None);
    let (_, script) = compared(from, to, true).await;
    assert_eq!(
        script.matches("drop constraint ref_fk").count(),
        1,
        "{script}"
    );
    assert!(!script.contains("add constraint ref_fk"), "{script}");
}

#[tokio::test]
async fn foreign_key_referencing_an_unchanged_partition_is_dropped_first() {
    let (mut from, mut to) = key_change_dumps();
    let fk = foreign_key(
        "ref",
        "ref_pt1_fk",
        "FOREIGN KEY (a, b) REFERENCES s.pt1(id, k)",
    );
    from.tables.push(with_fks(table("ref"), vec![fk.clone()]));
    to.tables.push(with_fks(table("ref"), vec![fk]));
    let (_, script) = compared(from, to, true).await;
    let drop_fk = position(&script, "alter table s.ref drop constraint ref_pt1_fk;");
    assert!(
        drop_fk < position(&script, "drop table if exists s.pt;"),
        "{script}"
    );
    let add_fk = position(&script, "alter table s.ref add constraint ref_pt1_fk");
    assert!(
        position(&script, "create table s.pt1 partition of s.pt") < add_fk,
        "{script}"
    );
}

#[tokio::test]
async fn foreign_keys_are_left_in_place_without_use_drop() {
    let (from, to) = referencing_dumps(Some("FOREIGN KEY (a, b) REFERENCES s.pt(id, k)"));
    let (comparer, script) = compared(from, to, false).await;
    assert!(comparer.recreate_fk_predrops.is_empty());
    assert!(!script.contains("ref_fk"), "{script}");
}

#[test]
fn foreign_keys_inside_one_family_or_on_a_dropped_table_are_not_predropped() {
    let (mut from, mut to) = key_change_dumps();
    // pt1 references its own parent: both go with the same DROP TABLE.
    let own = foreign_key("pt1", "pt1_self_fk", "FOREIGN KEY (k) REFERENCES s.pt(id)");
    from.tables[1] = with_fks(from.tables[1].clone(), vec![own.clone()]);
    to.tables[1] = with_fks(to.tables[1].clone(), vec![own]);
    // gone is FROM-only: the table phase drops it before any alter.
    from.tables.push(with_fks(
        table("gone"),
        vec![foreign_key(
            "gone",
            "gone_fk",
            "FOREIGN KEY (id) REFERENCES s.pt(id)",
        )],
    ));
    // keep references an unchanged table.
    from.tables.push(table("plain"));
    to.tables.push(table("plain"));
    let keep = foreign_key("keep", "keep_fk", "FOREIGN KEY (id) REFERENCES s.plain(id)");
    from.tables
        .push(with_fks(table("keep"), vec![keep.clone()]));
    to.tables.push(with_fks(table("keep"), vec![keep]));
    // ref references pt, unqualified (resolved against its own schema).
    let referencing = foreign_key("ref", "ref_fk", "FOREIGN KEY (id) REFERENCES pt(id)");
    from.tables
        .push(with_fks(table("ref"), vec![referencing.clone()]));
    to.tables.push(with_fks(table("ref"), vec![referencing]));

    let comparer = Comparer::new(from, to, true, false, true, GrantsMode::Ignore);
    let families = comparer.recreated_families();
    let predrops: Vec<String> = comparer
        .plan_recreate_fk_predrops(&families)
        .into_iter()
        .map(|c| c.name)
        .collect();
    assert_eq!(predrops, vec!["ref_fk".to_string()]);
}

#[tokio::test]
async fn recreated_table_gets_its_own_foreign_keys_back() {
    let (mut from, mut to) = key_change_dumps();
    from.tables.push(table("cust"));
    to.tables.push(table("cust"));
    let fk = foreign_key("pt", "pt_cust_fk", "FOREIGN KEY (k) REFERENCES s.cust(id)");
    from.tables[0] = with_fks(from.tables[0].clone(), vec![fk.clone()]);
    to.tables[0] = with_fks(to.tables[0].clone(), vec![fk]);
    let (_, script) = compared(from, to, true).await;
    let add_fk = position(
        &script,
        "alter table s.pt add constraint pt_cust_fk foreign key (k) references s.cust(id)",
    );
    assert!(
        position(&script, "create table s.pt (") < add_fk,
        "{script}"
    );
    // Nothing to drop: it went with the table.
    assert!(!script.contains("drop constraint pt_cust_fk"), "{script}");
}

/// Production output of `key_change_dumps` with an outgoing foreign key on
/// the recreated partitioned `s.pt` and one on `s.ref` referencing it, for a
/// FROM server of `version`.
async fn production_foreign_keys(version: Option<i32>) -> String {
    let (mut from, mut to) = referencing_dumps(Some("FOREIGN KEY (a, b) REFERENCES s.pt(id, k)"));
    from.tables.push(table("cust"));
    to.tables.push(table("cust"));
    let fk = foreign_key("pt", "pt_cust_fk", "FOREIGN KEY (k) REFERENCES s.cust(id)");
    from.tables[0] = with_fks(from.tables[0].clone(), vec![fk.clone()]);
    to.tables[0] = with_fks(to.tables[0].clone(), vec![fk]);
    from.server_version_num = version;
    to.server_version_num = version;
    let mut comparer = Comparer::new(from, to, true, true, true, GrantsMode::Ignore);
    comparer.set_output_for_production(true);
    comparer.compare().await.unwrap();
    comparer.get_script()
}

#[tokio::test]
async fn production_adds_foreign_keys_not_valid_and_validates_after_commit() {
    let script = production_foreign_keys(Some(180_000)).await;
    let commit = position(&script, "commit;");
    assert!(
        position(
            &script,
            "alter table s.ref add constraint ref_fk foreign key (a, b) references s.pt(id, k) not valid;"
        ) < commit,
        "{script}"
    );
    assert!(
        position(&script, "alter table s.ref validate constraint ref_fk;") > commit,
        "{script}"
    );
    // PostgreSQL 18 adds it NOT VALID to the partitioned table too.
    assert!(
        position(&script, "alter table s.pt validate constraint pt_cust_fk;") > commit,
        "{script}"
    );
}

#[tokio::test]
async fn production_adds_a_partitioned_tables_foreign_key_in_one_statement_before_pg18() {
    for version in [Some(170_000), None] {
        let script = production_foreign_keys(version).await;
        let commit = position(&script, "commit;");
        let add = position(
            &script,
            "alter table s.pt add constraint pt_cust_fk foreign key (k) references s.cust(id)",
        );
        assert!(add < commit, "{script}");
        assert!(
            !script.contains("pt_cust_fk foreign key (k) references s.cust(id) not valid"),
            "{version:?}: {script}"
        );
        assert!(
            !script.contains("validate constraint pt_cust_fk"),
            "{script}"
        );
        // A plain table referencing the partitioned one still takes NOT VALID.
        assert!(
            script.contains("alter table s.ref validate constraint ref_fk;"),
            "{script}"
        );
    }
}

#[tokio::test]
async fn unaffected_schema_keeps_its_output() {
    // No recreate: nothing is planned and the table phase is not swapped.
    let (from, _) = key_change_dumps();
    let mut to = from.clone();
    to.tables[1].partition_bound = Some("FOR VALUES FROM (0) TO (20)".to_string());
    to.tables[1].hash();
    let (comparer, script) = compared(from, to, true).await;
    assert!(comparer.recreate_fk_predrops.is_empty());
    assert!(comparer.dropped_and_recreated_tables.is_empty());
    assert!(
        script.contains("alter table s.pt detach partition s.pt1;"),
        "{script}"
    );
    assert!(!script.contains("create table"), "{script}");
}

/// A table `<schema>.<name>`, both parts as the dump spells them (quoted
/// when they need it), with integer columns `id` and `k`.
fn table_in(schema: &str, name: &str) -> Table {
    let raw = |s: &str| s.trim_matches('"').to_string();
    let mut t = Table::new(
        schema.to_string(),
        name.to_string(),
        raw(schema),
        raw(name),
        "postgres".to_string(),
        None,
        vec![
            int_column(schema, name, "id", 1),
            int_column(schema, name, "k", 2),
        ],
        vec![],
        vec![],
        vec![],
        None,
    );
    t.hash();
    t
}

#[tokio::test]
async fn standalone_table_joining_a_recreated_parent_is_attached_after_it_is_back() {
    // `aa` sorts before `pt`: taken in FROM order, its ATTACH to the old
    // `pt` ran first and the DROP TABLE of `pt` took it along, rows and all.
    let (mut from, mut to) = key_change_dumps();
    from.tables.push(table("aa"));
    to.tables
        .push(partition("aa", "pt", "FOR VALUES FROM (10) TO (20)"));
    let (_, script) = compared(from, to, true).await;
    let drop_parent = position(&script, "drop table if exists s.pt;");
    let create_parent = position(&script, "create table s.pt (");
    let attach = position(
        &script,
        "alter table s.pt attach partition s.aa FOR VALUES FROM (10) TO (20);",
    );
    assert!(
        drop_parent < create_parent && create_parent < attach,
        "{script}"
    );
    assert!(!script.contains("create table s.aa"), "{script}");
    assert!(!script.contains("drop table if exists s.aa"), "{script}");
}

#[tokio::test]
async fn partition_moving_into_a_recreated_sub_partition_root_waits_for_it() {
    // `mover` is a partition of `top` at depth 1; `leaf` at depth 2 changes
    // its own key, so it is the root, and `mover` moves under it.
    let mut from = Dump::new(DumpConfig::default());
    let mut to = Dump::new(DumpConfig::default());
    for (dump, leaf_key) in [(&mut from, "LIST (id)"), (&mut to, "LIST (k)")] {
        dump.tables.push(partitioned("top", "RANGE (id)"));
        let mut mid = partition("mid", "top", "FOR VALUES FROM (0) TO (100)");
        mid.partition_key = Some("RANGE (id)".to_string());
        mid.hash();
        dump.tables.push(mid);
        let mut leaf = partition("leaf", "mid", "FOR VALUES FROM (0) TO (50)");
        leaf.partition_key = Some(leaf_key.to_string());
        leaf.hash();
        dump.tables.push(leaf);
    }
    from.tables
        .push(partition("mover", "top", "FOR VALUES FROM (100) TO (200)"));
    to.tables
        .push(partition("mover", "leaf", "FOR VALUES IN (2)"));
    let (_, script) = compared(from, to, true).await;
    let create_leaf = position(&script, "create table s.leaf partition of s.mid");
    let detach = position(&script, "alter table s.top detach partition s.mover;");
    let attach = position(
        &script,
        "alter table s.leaf attach partition s.mover FOR VALUES IN (2);",
    );
    assert!(create_leaf < detach && detach < attach, "{script}");
}

#[tokio::test]
async fn recreated_root_joining_a_recreated_parent_waits_with_its_partitions() {
    // `aa` changes its own key and moves under `pt`, whose key changes too,
    // and `pt` comes later in FROM order. `aa`'s partition `aa1` goes with
    // `aa`'s DROP TABLE and becomes a plain table in TO; `aa2` is new.
    let (mut from, mut to) = key_change_dumps();
    from.tables.push(partitioned("aa", "LIST (id)"));
    from.tables
        .push(partition("aa1", "aa", "FOR VALUES IN (1)"));
    let mut aa = partition("aa", "pt", "FOR VALUES FROM (10) TO (20)");
    aa.partition_key = Some("LIST (k)".to_string());
    aa.hash();
    to.tables.push(aa);
    to.tables.push(table("aa1"));
    to.tables.push(partition("aa2", "aa", "FOR VALUES IN (2)"));
    let (_, script) = compared(from, to, true).await;
    let create_pt = position(&script, "create table s.pt (");
    let drop_aa = position(&script, "drop table if exists s.aa;");
    let create_aa = position(&script, "create table s.aa partition of s.pt");
    let create_aa1 = position(&script, "create table s.aa1 (");
    let create_aa2 = position(&script, "create table s.aa2 partition of s.aa");
    assert!(create_pt < drop_aa, "{script}");
    assert!(drop_aa < create_aa1, "{script}");
    assert!(create_aa < create_aa2, "{script}");
}

#[tokio::test]
async fn partitions_own_trigger_named_like_a_statement_trigger_of_the_parent_is_created() {
    // PostgreSQL clones only row triggers. `trg_audit` on the partition is
    // its own row trigger, named like the parent's statement trigger.
    let (mut from, mut to) = key_change_dumps();
    for dump in [&mut from, &mut to] {
        let mut statement = trigger("pt", "trg_audit");
        statement.definition = statement
            .definition
            .replace("FOR EACH ROW", "FOR EACH STATEMENT");
        dump.tables[0].triggers = vec![statement];
        dump.tables[0].hash();
        dump.tables[1].triggers = vec![trigger("pt1", "trg_audit")];
        dump.tables[1].hash();
    }
    let (_, script) = compared(from, to, true).await;
    assert!(
        script.contains("CREATE TRIGGER trg_audit BEFORE INSERT ON s.pt1 FOR EACH ROW"),
        "{script}"
    );
    assert!(
        script.contains("CREATE TRIGGER trg_audit BEFORE INSERT ON s.pt FOR EACH STATEMENT"),
        "{script}"
    );
}

#[tokio::test]
async fn partitions_own_row_trigger_that_differs_from_the_parents_is_created() {
    // Same name and both row triggers, but the partition's fires on another
    // event: it is not the parent's clone (PostgreSQL would refuse the name
    // on a clone; this one was created on the partition before the parent's
    // trigger was renamed to match).
    let (mut from, mut to) = key_change_dumps();
    for dump in [&mut from, &mut to] {
        dump.tables[0].triggers = vec![trigger("pt", "trg")];
        dump.tables[0].hash();
        let mut own = trigger("pt1", "trg");
        own.definition = own.definition.replace("BEFORE INSERT", "BEFORE UPDATE");
        dump.tables[1].triggers = vec![own];
        dump.tables[1].hash();
    }
    let (_, script) = compared(from, to, true).await;
    assert!(
        script.contains("CREATE TRIGGER trg BEFORE UPDATE ON s.pt1 FOR EACH ROW"),
        "{script}"
    );
}

/// FROM `"My S"."Orders"` partitioned by RANGE ("Id") with `"Orders P1"`,
/// TO the same partitioned by RANGE ("K"), with the new `"Orders P2"`, and
/// the parent's row trigger `"Row Trg"`, which each partition clones (in the
/// parent's state, so the dump does not list the clones).
fn quoted_key_change_dumps() -> (Dump, Dump) {
    let mut from = Dump::new(DumpConfig::default());
    let mut to = Dump::new(DumpConfig::default());
    let row_trigger = |table: &str| TableTrigger {
        oid: Oid(1),
        name: "\"Row Trg\"".to_string(),
        definition: format!(
            "CREATE TRIGGER \"Row Trg\" BEFORE INSERT ON \"My S\".{table} FOR EACH ROW EXECUTE FUNCTION \"My S\".tf()"
        ),
        enabled: "O".to_string(),
        comment: None,
    };
    for (dump, key) in [(&mut from, "RANGE (\"Id\")"), (&mut to, "RANGE (\"K\")")] {
        let mut parent = table_in("\"My S\"", "\"Orders\"");
        parent.partition_key = Some(key.to_string());
        parent.triggers = vec![row_trigger("\"Orders\"")];
        parent.hash();
        dump.tables.push(parent);
        let mut p1 = table_in("\"My S\"", "\"Orders P1\"");
        p1.partition_of = Some("\"My S\".\"Orders\"".to_string());
        p1.partition_bound = Some("FOR VALUES FROM (0) TO (100)".to_string());
        p1.hash();
        dump.tables.push(p1);
    }
    let mut p2 = table_in("\"My S\"", "\"Orders P2\"");
    p2.partition_of = Some("\"My S\".\"Orders\"".to_string());
    p2.partition_bound = Some("FOR VALUES FROM (100) TO (200)".to_string());
    p2.hash();
    to.tables.push(p2);
    (from, to)
}

#[tokio::test]
async fn new_partition_of_a_recreated_quoted_parent_waits_and_clones_its_trigger() {
    let (from, to) = quoted_key_change_dumps();
    let (_, script) = compared(from, to, true).await;
    let create_parent = position(&script, "create table \"My S\".\"Orders\" (");
    let create_p2 = position(
        &script,
        "create table \"My S\".\"Orders P2\" partition of \"My S\".\"Orders\"",
    );
    assert!(create_parent < create_p2, "{script}");
    assert_eq!(
        script
            .matches("create table \"My S\".\"Orders P2\"")
            .count(),
        1,
        "{script}"
    );
    // The partitions clone the parent's row trigger by themselves.
    assert_eq!(script.matches("CREATE TRIGGER").count(), 1, "{script}");
}

#[tokio::test]
async fn foreign_key_into_public_from_another_schema_is_dropped_first() {
    // The dump's `search_path` reaches `public`, so the definition names
    // the referenced table unqualified, from a table in `app`.
    let mut from = Dump::new(DumpConfig::default());
    let mut to = Dump::new(DumpConfig::default());
    let mut orders_from = table_in("public", "orders");
    orders_from.hash();
    let mut orders_to = table_in("public", "orders");
    orders_to.partition_key = Some("RANGE (k)".to_string());
    orders_to.hash();
    from.tables.push(orders_from);
    to.tables.push(orders_to);
    let mut fk = foreign_key(
        "lines",
        "lines_order_fk",
        "FOREIGN KEY (a, b) REFERENCES orders(id, k)",
    );
    fk.schema = "app".to_string();
    for dump in [&mut from, &mut to] {
        let mut lines = table_in("app", "lines");
        lines.constraints = vec![fk.clone()];
        lines.hash();
        dump.tables.push(lines);
    }
    let (_, script) = compared(from, to, true).await;
    let drop_fk = position(
        &script,
        "alter table app.lines drop constraint lines_order_fk;",
    );
    let drop_orders = position(&script, "drop table if exists public.orders;");
    let add_fk = position(
        &script,
        "alter table app.lines add constraint lines_order_fk",
    );
    assert!(drop_fk < drop_orders && drop_orders < add_fk, "{script}");
}

#[test]
fn unqualified_foreign_key_target_resolves_to_every_table_it_may_be() {
    let tables = vec![
        table_in("app", "orders"),
        table_in("public", "orders"),
        table_in("other", "only_here"),
        table_in("\"My S\"", "\"Orders\""),
    ];
    let from = FromTablesUnquoted::new(&tables);
    let names = |def: &str, owner: &str| -> Vec<String> {
        Comparer::fk_referenced_candidates(&from, def, owner)
            .into_iter()
            .map(|t| format!("{}.{}", t.schema, t.name))
            .collect()
    };
    // Qualified: that table alone.
    assert_eq!(
        names("FOREIGN KEY (a) REFERENCES app.orders(id)", "public"),
        vec!["app.orders"]
    );
    // Unqualified: the owner's schema and `public`.
    assert_eq!(
        names("FOREIGN KEY (a) REFERENCES orders(id)", "app"),
        vec!["app.orders", "public.orders"]
    );
    assert_eq!(
        names("FOREIGN KEY (a) REFERENCES orders(id)", "public"),
        vec!["public.orders"]
    );
    // Neither has it: any schema that does.
    assert_eq!(
        names("FOREIGN KEY (a) REFERENCES only_here(id)", "app"),
        vec!["other.only_here"]
    );
    assert_eq!(
        names(
            "FOREIGN KEY (a) REFERENCES \"My S\".\"Orders\"(\"Id\")",
            "app"
        ),
        vec!["\"My S\".\"Orders\""]
    );
    assert!(names("FOREIGN KEY (a) REFERENCES missing(id)", "app").is_empty());
}

#[tokio::test]
async fn foreign_key_referencing_an_ancestor_of_a_recreated_partition_root_is_dropped_first() {
    // `pt` stays; its partition `pt1` changes its own key, so it is the
    // root. PostgreSQL copies `ref_fk`, which references `pt`, onto every
    // partition of `pt`, and that copy blocks `DROP TABLE s.pt1`.
    let mut from = Dump::new(DumpConfig::default());
    let mut to = Dump::new(DumpConfig::default());
    for (dump, key) in [(&mut from, "LIST (id)"), (&mut to, "LIST (k)")] {
        dump.tables.push(partitioned("pt", "RANGE (id)"));
        let mut pt1 = partition("pt1", "pt", "FOR VALUES FROM (0) TO (10)");
        pt1.partition_key = Some(key.to_string());
        pt1.hash();
        dump.tables.push(pt1);
        dump.tables.push(with_fks(
            table("ref"),
            vec![foreign_key(
                "ref",
                "ref_fk",
                "FOREIGN KEY (a, b) REFERENCES s.pt(id, k)",
            )],
        ));
    }
    let (_, script) = compared(from, to, true).await;
    let drop_fk = position(&script, "alter table s.ref drop constraint ref_fk;");
    let drop_root = position(&script, "drop table if exists s.pt1;");
    let add_fk = position(&script, "alter table s.ref add constraint ref_fk");
    assert!(drop_fk < drop_root && drop_root < add_fk, "{script}");
    // `pt` itself is not recreated.
    assert!(!script.contains("drop table if exists s.pt;"), "{script}");
}

#[tokio::test]
async fn recreated_quoted_tables_foreign_keys_keep_their_case() {
    // `"Lines"` references the recreated `"Orders"`: dropped first and
    // added back from TO with every quoted identifier as it is.
    let (mut from, mut to) = quoted_key_change_dumps();
    to.tables.pop();
    let mut fk = foreign_key(
        "\"Lines\"",
        "\"Lines_Order_FK\"",
        "FOREIGN KEY (\"OrderId\", \"K\") REFERENCES \"My S\".\"Orders\"(\"Id\", \"K\")",
    );
    fk.schema = "\"My S\"".to_string();
    for dump in [&mut from, &mut to] {
        let mut lines = table_in("\"My S\"", "\"Lines\"");
        lines.constraints = vec![fk.clone()];
        lines.hash();
        dump.tables.push(lines);
    }
    let (_, script) = compared(from, to, true).await;
    let drop_fk = position(
        &script,
        "alter table \"My S\".\"Lines\" drop constraint \"Lines_Order_FK\";",
    );
    let add_fk = position(
        &script,
        "alter table \"My S\".\"Lines\" add constraint \"Lines_Order_FK\" foreign key (\"OrderId\", \"K\") references \"My S\".\"Orders\"(\"Id\", \"K\")",
    );
    assert!(
        drop_fk < position(&script, "drop table if exists \"My S\".\"Orders\";"),
        "{script}"
    );
    assert!(
        position(&script, "create table \"My S\".\"Orders P1\"") < add_fk,
        "{script}"
    );
}
