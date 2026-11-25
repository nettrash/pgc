//! Publications: membership (FOR ALL TABLES / SEQUENCES / EXCEPT / TABLES IN SCHEMA / table list) transitions and options.
//!
//! `Publication::get_alter_script_with` is covered in `dump/tests/publication.rs`;
//! these tests pin what `compare_publications` feeds it: the migrated server's
//! version, and the tables the migration's table section drops or recreates.

use super::helpers::int_column;
use crate::comparer::core::*;
use crate::config::dump_config::DumpConfig;
use crate::config::grants_mode::GrantsMode;
use crate::dump::publication::{Publication, PublicationTableSpec};
use crate::dump::table::Table;

fn table(schema: &str, name: &str) -> Table {
    let mut t = Table::new(
        schema.to_string(),
        name.to_string(),
        schema.to_string(),
        name.to_string(),
        "postgres".to_string(),
        None,
        vec![int_column(schema, name, "id", 1)],
        vec![],
        vec![],
        vec![],
        None,
    );
    t.hash();
    t
}

fn publication(tables: &[&str], all_tables: bool) -> Publication {
    let mut p = Publication {
        name: "p".into(),
        owner: "postgres".into(),
        all_tables,
        publish: "insert,update,delete,truncate".into(),
        tables: tables.iter().map(|t| t.to_string()).collect(),
        ..Default::default()
    };
    p.hash();
    p
}

fn dumps(
    from_version: Option<i32>,
    from_tables: Vec<Table>,
    to_tables: Vec<Table>,
    from_pub: Publication,
    to_pub: Publication,
) -> (Dump, Dump) {
    let mut from = Dump::new(DumpConfig::default());
    from.server_version_num = from_version;
    from.tables = from_tables;
    from.publications.push(from_pub);
    let mut to = Dump::new(DumpConfig::default());
    to.tables = to_tables;
    to.publications.push(to_pub);
    (from, to)
}

async fn compare_publications(from: Dump, to: Dump, use_drop: bool) -> String {
    let mut comparer = Comparer::new(from, to, use_drop, false, false, GrantsMode::Ignore);
    comparer.compare_publications().await.unwrap();
    comparer.get_script()
}

#[tokio::test]
async fn list_to_all_tables_is_altered_in_place_when_the_from_server_is_pg19() {
    let (from, to) = dumps(
        Some(190000),
        vec![table("s", "t")],
        vec![table("s", "t")],
        publication(&["s.t"], false),
        publication(&[], true),
    );
    let script = compare_publications(from, to, false).await;
    assert!(
        script.contains(
            "ALTER PUBLICATION p DROP TABLE ONLY s.t;\n\nALTER PUBLICATION p SET ALL TABLES;"
        ),
        "got: {script}"
    );
    assert!(!script.contains("DROP PUBLICATION"), "got: {script}");
}

#[tokio::test]
async fn list_to_all_tables_is_recreated_below_pg19_or_for_an_unknown_version() {
    // Only the migrated (FROM) server matters: a PG19 TO dump does not help.
    for from_version in [None, Some(180006)] {
        let (from, mut to) = dumps(
            from_version,
            vec![table("s", "t")],
            vec![table("s", "t")],
            publication(&["s.t"], false),
            publication(&[], true),
        );
        to.server_version_num = Some(190000);
        let script = compare_publications(from, to, true).await;
        assert!(
            script.contains(
                "DROP PUBLICATION IF EXISTS p;\n\nCREATE PUBLICATION p FOR ALL TABLES WITH ("
            ),
            "{from_version:?}: {script}"
        );
        assert!(!script.contains("SET ALL"), "{from_version:?}: {script}");
    }
}

#[tokio::test]
async fn members_dropped_by_the_migration_are_left_out_of_drop_table() {
    let from_tables = || vec![table("s", "gone"), table("s", "t")];
    let (from, to) = dumps(
        Some(160000),
        from_tables(),
        vec![table("s", "t")],
        publication(&["s.gone", "s.t"], false),
        publication(&[], false),
    );
    let script = compare_publications(from, to, true).await;
    assert!(
        script.contains("ALTER PUBLICATION p DROP TABLE ONLY s.t;"),
        "got: {script}"
    );

    // Without use_drop the table drop is commented out, so the table and its
    // membership row are still there.
    let (from, to) = dumps(
        Some(160000),
        from_tables(),
        vec![table("s", "t")],
        publication(&["s.gone", "s.t"], false),
        publication(&[], false),
    );
    let script = compare_publications(from, to, false).await;
    assert!(
        script.contains("ALTER PUBLICATION p DROP TABLE ONLY s.gone, ONLY s.t;"),
        "got: {script}"
    );
}

#[tokio::test]
async fn members_recreated_by_the_migration_get_their_row_back() {
    // A partition-key change drops and recreates s.pt, which takes its
    // pg_publication_rel row with it; the publication itself is unchanged.
    let mut from_pt = table("s", "pt");
    from_pt.partition_key = Some("RANGE (id)".to_string());
    from_pt.hash();
    let mut to_pt = table("s", "pt");
    to_pt.partition_key = Some("LIST (id)".to_string());
    to_pt.hash();

    let (from, to) = dumps(
        Some(170000),
        vec![from_pt, table("s", "t")],
        vec![to_pt, table("s", "t")],
        publication(&["s.pt", "s.t"], false),
        publication(&["s.pt", "s.t"], false),
    );
    assert_eq!(from.publications[0].hash, to.publications[0].hash);

    let mut comparer = Comparer::new(from, to, true, false, false, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();

    let recreate = script
        .find("drop table if exists s.pt")
        .unwrap_or_else(|| panic!("s.pt must be recreated, got: {script}"));
    let set_table = script
        .find("ALTER PUBLICATION p SET TABLE s.pt, s.t;")
        .unwrap_or_else(|| panic!("membership must be restored, got: {script}"));
    assert!(recreate < set_table, "got: {script}");
}

#[tokio::test]
async fn unchanged_publication_emits_nothing() {
    let (from, to) = dumps(
        Some(190000),
        vec![table("s", "t")],
        vec![table("s", "t")],
        publication(&["s.t"], false),
        publication(&["s.t"], false),
    );
    let script = compare_publications(from, to, true).await;
    assert!(!script.contains("PUBLICATION p"), "got: {script}");
}

fn partition_key_change(name: &str) -> (Table, Table) {
    let mut from = table("s", name);
    from.partition_key = Some("RANGE (id)".to_string());
    from.hash();
    let mut to = table("s", name);
    to.partition_key = Some("LIST (id)".to_string());
    to.hash();
    (from, to)
}

#[tokio::test]
async fn members_recreated_without_use_drop_are_still_members() {
    // Without use_drop the recreate's DROP TABLE is commented out, so s.pt
    // and its pg_publication_rel row are still there when publications run:
    // it has to stay in the DROP TABLE list that empties the publication,
    // or SET ALL is refused and the row is left behind.
    for (to_pub, expected) in [
        (
            publication(&[], true),
            "ALTER PUBLICATION p DROP TABLE ONLY s.pt, ONLY s.t;\n\nALTER PUBLICATION p SET ALL TABLES;",
        ),
        (
            publication(&[], false),
            "ALTER PUBLICATION p DROP TABLE ONLY s.pt, ONLY s.t;",
        ),
    ] {
        let (from_pt, to_pt) = partition_key_change("pt");
        let (from, to) = dumps(
            Some(190000),
            vec![from_pt, table("s", "t")],
            vec![to_pt, table("s", "t")],
            publication(&["s.pt", "s.t"], false),
            to_pub,
        );
        // use_comments keeps the commented-out DROP in the script.
        let mut comparer = Comparer::new(from, to, false, false, true, GrantsMode::Ignore);
        comparer.compare().await.unwrap();
        let script = comparer.get_script();
        assert!(
            script.contains("-- drop table if exists s.pt"),
            "the recreate must be commented out, got: {script}"
        );
        assert!(script.contains(expected), "got: {script}");
    }

    // An unchanged publication lost nothing, so there is nothing to restore.
    let (from_pt, to_pt) = partition_key_change("pt");
    let (from, to) = dumps(
        Some(190000),
        vec![from_pt, table("s", "t")],
        vec![to_pt, table("s", "t")],
        publication(&["s.pt", "s.t"], false),
        publication(&["s.pt", "s.t"], false),
    );
    let mut comparer = Comparer::new(from, to, false, false, false, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();
    assert!(!script.contains("ALTER PUBLICATION p"), "got: {script}");
}

fn child_of(name: &str, parent: &str) -> Table {
    let mut t = table("s", name);
    t.inherits_from = vec![parent.to_string()];
    t.hash();
    t
}

fn filtered(name: &str, tables: &[&str], filter: &str) -> Publication {
    let mut p = Publication {
        name: name.into(),
        ..publication(tables, false)
    };
    for t in tables {
        p.table_specs.insert(
            t.to_string(),
            PublicationTableSpec {
                columns: vec![],
                row_filter: Some(filter.to_string()),
            },
        );
    }
    p.hash();
    p
}

#[tokio::test]
async fn inheritance_parents_are_written_only() {
    // Each `tables` entry is one pg_publication_rel row. Without ONLY,
    // `SET TABLE s.parent` also adds s.child (so the migration never
    // converges), and a parent listed with its child and a row filter is
    // refused ("conflicting or redundant WHERE clauses").
    let tables = || {
        vec![
            child_of("child", "s.parent"),
            table("s", "parent"),
            table("s", "t"),
        ]
    };
    let (from, mut to) = dumps(
        Some(150000),
        tables(),
        tables(),
        publication(&["s.t"], false),
        publication(&["s.parent", "s.t"], false),
    );
    to.publications
        .push(filtered("q", &["s.child", "s.parent"], "(id > 0)"));
    let script = compare_publications(from, to, true).await;
    assert!(
        script.contains("ALTER PUBLICATION p SET TABLE ONLY s.parent, s.t;"),
        "got: {script}"
    );
    assert!(
        script.contains(
            "CREATE PUBLICATION q FOR TABLE s.child WHERE ((id > 0)), ONLY s.parent WHERE ((id > 0)) WITH ("
        ),
        "got: {script}"
    );

    // The recreate that leaves FOR ALL TABLES writes the list the same way.
    let (from, to) = dumps(
        Some(150000),
        tables(),
        tables(),
        publication(&[], true),
        publication(&["s.parent", "s.t"], false),
    );
    let script = compare_publications(from, to, true).await;
    assert!(
        script.contains("CREATE PUBLICATION p FOR TABLE ONLY s.parent, s.t WITH ("),
        "got: {script}"
    );

    // A parent whose child only FROM still has counts too: without use_drop
    // the child is not dropped.
    let (from, to) = dumps(
        Some(150000),
        tables(),
        vec![table("s", "parent"), table("s", "t")],
        publication(&["s.t"], false),
        publication(&["s.parent"], false),
    );
    let script = compare_publications(from, to, false).await;
    assert!(
        script.contains("ALTER PUBLICATION p SET TABLE ONLY s.parent;"),
        "got: {script}"
    );

    // Tables without inheritance children keep the pgc 1.0.x spelling.
    let (from, to) = dumps(
        Some(150000),
        vec![table("s", "parent"), table("s", "t")],
        vec![table("s", "parent"), table("s", "t")],
        publication(&["s.t"], false),
        publication(&["s.parent", "s.t"], false),
    );
    let script = compare_publications(from, to, true).await;
    assert!(
        script.contains("ALTER PUBLICATION p SET TABLE s.parent, s.t;"),
        "got: {script}"
    );
}

/// A partitioned `s.pt` and its partition `s.pt1`, both with columns
/// `id, v`, plus a plain `s.t`. `v_type` is the type of `v`: a change of it
/// is carried by the parent's ALTER, and the partition is never dropped.
fn retyped_partition_tables(v_type: &str) -> Vec<Table> {
    let make = |name: &str, partition_of: Option<&str>| {
        let mut v = int_column("s", name, "v", 2);
        v.data_type = v_type.to_string();
        let mut t = table("s", name);
        t.columns.push(v);
        match partition_of {
            Some(parent) => {
                t.partition_of = Some(parent.to_string());
                t.partition_bound = Some("FOR VALUES FROM (0) TO (10)".to_string());
            }
            None => t.partition_key = Some("RANGE (id)".to_string()),
        }
        t.hash();
        t
    };
    vec![make("pt", None), make("pt1", Some("s.pt")), table("s", "t")]
}

#[tokio::test]
async fn partition_retyped_through_its_parent_keeps_its_membership() {
    // s.pt1 is in `recreated_tables` (its column type changes), but only
    // the parent is altered: its pg_publication_rel row is still there and
    // must be dropped like any other member's, or SET ALL TABLES is refused
    // ("publication does not support ALL TABLES operations") and an emptied
    // list keeps it.
    for (to_pub, expected) in [
        (
            publication(&[], true),
            "ALTER PUBLICATION p DROP TABLE ONLY s.pt1, ONLY s.t;\n\nALTER PUBLICATION p SET ALL TABLES;",
        ),
        (
            publication(&[], false),
            "ALTER PUBLICATION p DROP TABLE ONLY s.pt1, ONLY s.t;",
        ),
    ] {
        let (from, to) = dumps(
            Some(190000),
            retyped_partition_tables("integer"),
            retyped_partition_tables("bigint"),
            publication(&["s.pt1", "s.t"], false),
            to_pub,
        );
        let mut comparer = Comparer::new(from, to, true, false, false, GrantsMode::Ignore);
        comparer.compare().await.unwrap();
        let script = comparer.get_script();
        assert!(
            script.contains("alter table s.pt alter column v type bigint;"),
            "got: {script}"
        );
        assert!(!script.contains("drop table"), "got: {script}");
        assert!(script.contains(expected), "got: {script}");
    }

    // An unchanged publication lost nothing: no spurious SET TABLE.
    let (from, to) = dumps(
        Some(190000),
        retyped_partition_tables("integer"),
        retyped_partition_tables("bigint"),
        publication(&["s.pt1", "s.t"], false),
        publication(&["s.pt1", "s.t"], false),
    );
    let mut comparer = Comparer::new(from, to, true, false, false, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();
    assert!(!script.contains("ALTER PUBLICATION p"), "got: {script}");
}

// ==== fix:pre-grants tests ====

/// `s.<name>` with columns `id`, then each of `extra` as an integer, or as
/// the given type.
fn table_with(name: &str, extra: &[(&str, &str)]) -> Table {
    let mut t = table("s", name);
    for (i, (column, data_type)) in extra.iter().enumerate() {
        let mut c = int_column("s", name, column, i as i32 + 2);
        c.data_type = data_type.to_string();
        t.columns.push(c);
    }
    t.hash();
    t
}

fn with_spec(
    mut p: Publication,
    table: &str,
    columns: &[&str],
    filter: Option<&str>,
) -> Publication {
    p.table_specs.insert(
        table.to_string(),
        PublicationTableSpec {
            columns: columns.iter().map(|c| c.to_string()).collect(),
            row_filter: filter.map(str::to_string),
        },
    );
    p.hash();
    p
}

async fn compare_all(from: Dump, to: Dump, use_drop: bool) -> String {
    let mut comparer = Comparer::new(from, to, use_drop, false, true, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    comparer.get_script()
}

fn at(script: &str, needle: &str) -> usize {
    script
        .find(needle)
        .unwrap_or_else(|| panic!("missing {needle:?} in: {script}"))
}

const RELEASE_HEADER: &str = "/* ---> Publications: release members that block table changes";

#[tokio::test]
async fn member_whose_column_list_loses_a_column_is_released_before_the_drop() {
    // PostgreSQL refuses DROP COLUMN qty while the column list names it; the
    // publications section only comes after the tables.
    let (from, to) = dumps(
        Some(170000),
        vec![table_with("t", &[("code", "text"), ("qty", "integer")])],
        vec![table_with("t", &[("code", "text")])],
        with_spec(
            publication(&["s.t"], false),
            "s.t",
            &["code", "id", "qty"],
            None,
        ),
        with_spec(publication(&["s.t"], false), "s.t", &["code", "id"], None),
    );
    let script = compare_all(from, to, true).await;
    let release = at(&script, "ALTER PUBLICATION p DROP TABLE ONLY s.t;");
    let drop = at(&script, "alter table s.t drop column qty;");
    let restore = at(&script, "ALTER PUBLICATION p SET TABLE s.t (code, id);");
    assert!(at(&script, RELEASE_HEADER) < release, "{script}");
    assert!(release < drop && drop < restore, "{script}");
}

#[tokio::test]
async fn unchanged_member_whose_row_filter_column_is_retyped_is_released_and_restored() {
    // Nothing about the publication changes, so without the release nothing
    // ever unblocked the ALTER COLUMN ... TYPE.
    let spec = |p| with_spec(p, "s.t", &[], Some("(qty > 0)"));
    let (from, to) = dumps(
        Some(170000),
        vec![table_with("t", &[("qty", "integer")])],
        vec![table_with("t", &[("qty", "bigint")])],
        spec(publication(&["s.t"], false)),
        spec(publication(&["s.t"], false)),
    );
    assert_eq!(from.publications[0].hash, to.publications[0].hash);
    let script = compare_all(from, to, true).await;
    let release = at(&script, "ALTER PUBLICATION p DROP TABLE ONLY s.t;");
    let retype = at(&script, "alter table s.t alter column qty type bigint");
    let restore = at(
        &script,
        "ALTER PUBLICATION p SET TABLE s.t WHERE ((qty > 0));",
    );
    assert!(release < retype && retype < restore, "{script}");
}

#[tokio::test]
async fn members_that_block_nothing_are_left_alone() {
    // A member without a column list or row filter does not stop a retype,
    // and a spec on other columns does not either: no section, no statement.
    let (from, to) = dumps(
        Some(170000),
        vec![table_with("t", &[("code", "text"), ("qty", "integer")])],
        vec![table_with("t", &[("code", "text"), ("qty", "bigint")])],
        with_spec(
            publication(&["s.t"], false),
            "s.t",
            &["code", "id"],
            Some("(id > 0)"),
        ),
        with_spec(
            publication(&["s.t"], false),
            "s.t",
            &["code", "id"],
            Some("(id > 0)"),
        ),
    );
    let script = compare_all(from, to, true).await;
    assert!(!script.contains(RELEASE_HEADER), "{script}");
    assert!(!script.contains("ALTER PUBLICATION"), "{script}");
}

#[tokio::test]
async fn dropped_column_is_not_released_without_use_drop() {
    // The DROP COLUMN is commented out, so the column list keeps working.
    let (from, to) = dumps(
        Some(170000),
        vec![table_with("t", &[("qty", "integer")])],
        vec![table_with("t", &[])],
        with_spec(publication(&["s.t"], false), "s.t", &["id", "qty"], None),
        with_spec(publication(&["s.t"], false), "s.t", &["id", "qty"], None),
    );
    let script = compare_all(from, to, false).await;
    assert!(!script.contains(RELEASE_HEADER), "{script}");
}

#[tokio::test]
async fn member_made_unlogged_is_released_before_set_unlogged() {
    // Unlogged tables cannot be published, so TO leaves it out.
    let from_t = table("s", "t");
    let mut to_t = table("s", "t");
    to_t.is_unlogged = true;
    to_t.hash();
    let (from, to) = dumps(
        Some(140000),
        vec![from_t, table("s", "u")],
        vec![to_t, table("s", "u")],
        publication(&["s.t", "s.u"], false),
        publication(&["s.u"], false),
    );
    let script = compare_all(from, to, true).await;
    let release = at(&script, "ALTER PUBLICATION p DROP TABLE ONLY s.t;");
    let unlogged = at(&script, "alter table s.t set unlogged;");
    let restore = at(&script, "ALTER PUBLICATION p SET TABLE s.u;");
    assert!(release < unlogged && unlogged < restore, "{script}");
}

#[tokio::test]
async fn except_entries_made_unlogged_or_attached_are_released_on_pg19() {
    let from_log = table("s", "log");
    let mut to_log = table("s", "log");
    to_log.is_unlogged = true;
    to_log.hash();
    let mut parent = table("s", "parent");
    parent.partition_key = Some("RANGE (id)".to_string());
    parent.hash();
    let from_part = table("s", "part");
    let mut to_part = table("s", "part");
    to_part.partition_of = Some("s.parent".to_string());
    to_part.partition_bound = Some("FOR VALUES FROM (0) TO (10)".to_string());
    to_part.hash();
    let mut from_pub = publication(&[], true);
    from_pub.except_tables = vec!["s.keep".into(), "s.log".into(), "s.part".into()];
    from_pub.hash();
    let mut to_pub = publication(&[], true);
    to_pub.except_tables = vec!["s.keep".into()];
    to_pub.hash();
    let (from, to) = dumps(
        Some(190000),
        vec![table("s", "keep"), from_log, parent.clone(), from_part],
        vec![table("s", "keep"), to_log, parent, to_part],
        from_pub,
        to_pub,
    );
    let script = compare_all(from, to, true).await;
    let release = at(
        &script,
        "ALTER PUBLICATION p SET ALL TABLES EXCEPT (TABLE ONLY s.keep);",
    );
    assert!(
        release < at(&script, "alter table s.log set unlogged;"),
        "{script}"
    );
    assert!(release < at(&script, "attach partition s.part"), "{script}");
    // The publications section sets the target list again (the except list
    // differs from FROM's), which the release already left in place.
    let restore = script.rfind("ALTER PUBLICATION p SET ALL TABLES EXCEPT (TABLE ONLY s.keep);");
    assert!(restore > Some(release), "{script}");
}

#[tokio::test]
async fn partition_retyped_through_its_parent_is_released_when_its_column_list_uses_the_column() {
    // `will_be_dropped_and_recreated` answers true for s.pt1, but it is never
    // dropped: its column list would block the parent's recursing ALTER.
    let spec = |p| with_spec(p, "s.pt1", &["id", "v"], None);
    let (from, to) = dumps(
        Some(170000),
        retyped_partition_tables("integer"),
        retyped_partition_tables("bigint"),
        spec(publication(&["s.pt1", "s.t"], false)),
        spec(publication(&["s.pt1", "s.t"], false)),
    );
    let script = compare_all(from, to, true).await;
    let release = at(&script, "ALTER PUBLICATION p DROP TABLE ONLY s.pt1;");
    let retype = at(&script, "alter table s.pt alter column v type bigint;");
    let restore = at(&script, "ALTER PUBLICATION p SET TABLE s.pt1 (id, v), s.t;");
    assert!(release < retype && retype < restore, "{script}");
}

#[tokio::test]
async fn from_only_publication_is_released_before_the_tables_and_dropped_after() {
    let mut from = Dump::new(DumpConfig::default());
    from.server_version_num = Some(170000);
    from.tables = vec![table_with("t", &[("qty", "integer")])];
    from.publications.push(with_spec(
        publication(&["s.t"], false),
        "s.t",
        &["id", "qty"],
        None,
    ));
    let mut to = Dump::new(DumpConfig::default());
    to.tables = vec![table_with("t", &[("qty", "bigint")])];
    let script = compare_all(from, to, true).await;
    let release = at(&script, "ALTER PUBLICATION p DROP TABLE ONLY s.t;");
    let retype = at(&script, "alter table s.t alter column qty type bigint");
    let drop = at(&script, "DROP PUBLICATION IF EXISTS p;");
    assert!(release < retype && retype < drop, "{script}");
}

// ==== fix:pre-grants review tests ====

/// A FROM-only publication without `use_drop` is kept, so nothing else puts
/// back the member released for the retype: it comes back after the tables
/// with its own row filter. Before, the membership was lost silently (and
/// round 2 was empty).
#[tokio::test]
async fn from_only_publication_kept_without_use_drop_gets_its_released_member_back() {
    let mut from = Dump::new(DumpConfig::default());
    from.server_version_num = Some(170000);
    from.tables = vec![table_with("t", &[("qty", "integer")])];
    from.publications.push(with_spec(
        publication(&["s.t"], false),
        "s.t",
        &[],
        Some("(qty > 0)"),
    ));
    let mut to = Dump::new(DumpConfig::default());
    to.tables = vec![table_with("t", &[("qty", "bigint")])];
    let script = compare_all(from.clone(), to.clone(), false).await;
    let release = at(&script, "ALTER PUBLICATION p DROP TABLE ONLY s.t;");
    let retype = at(&script, "alter table s.t alter column qty type bigint");
    let drop = at(&script, "-- DROP PUBLICATION IF EXISTS p;");
    let restore = at(
        &script,
        "ALTER PUBLICATION p SET TABLE s.t WHERE ((qty > 0));",
    );
    assert!(
        release < retype && retype < drop && drop < restore,
        "{script}"
    );
    // With use_drop the publication goes, and nothing is put back.
    let script = compare_all(from, to, true).await;
    assert!(!script.contains("SET TABLE"), "{script}");
}

/// A member made UNLOGGED cannot be put back into a kept publication: a note
/// says so instead.
#[tokio::test]
async fn from_only_publication_kept_without_use_drop_notes_an_unlogged_member() {
    let mut to_t = table("s", "t");
    to_t.is_unlogged = true;
    to_t.hash();
    let mut from = Dump::new(DumpConfig::default());
    from.server_version_num = Some(140000);
    from.tables = vec![table("s", "t"), table("s", "u")];
    from.publications.push(publication(&["s.t", "s.u"], false));
    let mut to = Dump::new(DumpConfig::default());
    to.tables = vec![to_t, table("s", "u")];
    let script = compare_all(from, to, false).await;
    let release = at(&script, "ALTER PUBLICATION p DROP TABLE ONLY s.t;");
    let note = at(
        &script,
        "-- publication p: member s.t was dropped because the migration makes it UNLOGGED, which a publication cannot hold\n",
    );
    assert!(release < note, "{script}");
    // s.u keeps its row; with nothing to put back no SET is needed.
    assert!(!script.contains("SET TABLE"), "{script}");
}

/// A publication whose alter is a drop+recreate (here `FOR TABLE` to
/// `FOR ALL TABLES` before PostgreSQL 19) stays as it is without `use_drop`:
/// its released member comes back with its FROM column list. With `use_drop`
/// the recreate is live and restores the target instead.
#[tokio::test]
async fn commented_out_recreate_restores_the_released_member() {
    let (from, to) = dumps(
        Some(170000),
        vec![table_with("t", &[("qty", "integer")])],
        vec![table_with("t", &[("qty", "bigint")])],
        with_spec(publication(&["s.t"], false), "s.t", &["id", "qty"], None),
        publication(&[], true),
    );
    let script = compare_all(from.clone(), to.clone(), false).await;
    let release = at(&script, "ALTER PUBLICATION p DROP TABLE ONLY s.t;");
    let retype = at(&script, "alter table s.t alter column qty type bigint");
    let recreate = at(
        &script,
        "-- use_drop=false: publication p requires drop+recreate",
    );
    let restore = at(&script, "ALTER PUBLICATION p SET TABLE s.t (id, qty);");
    assert!(
        release < retype && retype < recreate && recreate < restore,
        "{script}"
    );

    let script = compare_all(from, to, true).await;
    assert!(
        script.contains("CREATE PUBLICATION p FOR ALL TABLES"),
        "{script}"
    );
    assert!(!script.contains("SET TABLE"), "{script}");
}

/// PostgreSQL 19: an EXCEPT entry taken out for an attach, in a kept
/// FROM-only publication, gets a note — a partition cannot be excepted.
#[tokio::test]
async fn from_only_publication_kept_without_use_drop_notes_a_released_except_entry() {
    let mut parent = table("s", "parent");
    parent.partition_key = Some("RANGE (id)".to_string());
    parent.hash();
    let mut to_part = table("s", "part");
    to_part.partition_of = Some("s.parent".to_string());
    to_part.partition_bound = Some("FOR VALUES FROM (0) TO (10)".to_string());
    to_part.hash();
    let mut from_pub = publication(&[], true);
    from_pub.except_tables = vec!["s.part".into()];
    from_pub.hash();
    let mut from = Dump::new(DumpConfig::default());
    from.server_version_num = Some(190000);
    from.tables = vec![parent.clone(), table("s", "part")];
    from.publications.push(from_pub);
    let mut to = Dump::new(DumpConfig::default());
    to.tables = vec![parent, to_part];
    let script = compare_all(from, to, false).await;
    let release = at(&script, "ALTER PUBLICATION p SET ALL TABLES;");
    let note = at(
        &script,
        "-- publication p: s.part was taken out of EXCEPT because the migration makes it UNLOGGED or a partition, which EXCEPT cannot hold\n",
    );
    assert!(release < at(&script, "attach partition s.part"), "{script}");
    assert!(release < note, "{script}");
}

/// Publications altered in place restore the target, as before, and get no
/// second `SET` of their FROM list, whatever `use_drop` says.
#[tokio::test]
async fn publication_altered_in_place_restores_nothing_extra() {
    let spec = |p| with_spec(p, "s.t", &[], Some("(qty > 0)"));
    let (from, to) = dumps(
        Some(170000),
        vec![table_with("t", &[("qty", "integer")])],
        vec![table_with("t", &[("qty", "bigint")])],
        spec(publication(&["s.t"], false)),
        spec(publication(&["s.t"], false)),
    );
    for use_drop in [false, true] {
        let script = compare_all(from.clone(), to.clone(), use_drop).await;
        assert_eq!(
            script.matches("ALTER PUBLICATION p SET TABLE").count(),
            1,
            "{script}"
        );
        assert!(
            script.contains("ALTER PUBLICATION p SET TABLE s.t WHERE ((qty > 0));"),
            "{script}"
        );
    }
}

// ==== r3-gencol tests ====

/// `s.<name>` (`id`, `a <a_type>`, `g integer GENERATED ALWAYS AS (a * 2)
/// STORED`), partitioned by `id` or a partition of `s.<parent>`.
fn gen_over(name: &str, a_type: &str, partition_of: Option<&str>, partitioned: bool) -> Table {
    let mut t = table("s", name);
    let mut a = int_column("s", name, "a", 2);
    a.data_type = a_type.to_string();
    let mut g = int_column("s", name, "g", 3);
    g.is_generated = "ALWAYS".to_string();
    g.generation_expression = Some("(a * 2)".to_string());
    g.generation_type = Some("s".to_string());
    t.columns.push(a);
    t.columns.push(g);
    if let Some(parent) = partition_of {
        t.partition_of = Some(parent.to_string());
        t.partition_bound = Some("FOR VALUES FROM (0) TO (10)".to_string());
    }
    if partitioned {
        t.partition_key = Some("RANGE (id)".to_string());
    }
    t.hash();
    t
}

#[tokio::test]
async fn member_using_a_generated_column_recreated_around_a_retype_is_released() {
    // The generated column itself does not change, but the retype of the
    // column it reads drops and re-adds it: the member's row filter blocks
    // that DROP COLUMN, on the table and on a partition its parent's
    // recursing drop reaches.
    for (member, tables_of) in [
        (
            "s.t",
            (|a: &str| vec![gen_over("t", a, None, false)]) as fn(&str) -> Vec<Table>,
        ),
        ("s.pt1", |a: &str| {
            vec![
                gen_over("pt", a, None, true),
                gen_over("pt1", a, Some("s.pt"), false),
            ]
        }),
    ] {
        let spec = |p| with_spec(p, member, &[], Some("(g > 0)"));
        let (from, to) = dumps(
            Some(170000),
            tables_of("integer"),
            tables_of("bigint"),
            spec(publication(&[member], false)),
            spec(publication(&[member], false)),
        );
        let script = compare_all(from, to, true).await;
        let owner = if member == "s.t" { "s.t" } else { "s.pt" };
        let release = at(
            &script,
            &format!("ALTER PUBLICATION p DROP TABLE ONLY {member};"),
        );
        let drop = at(&script, &format!("alter table {owner} drop column g;"));
        let retype = at(
            &script,
            &format!("alter table {owner} alter column a type bigint;"),
        );
        let add = at(
            &script,
            &format!("alter table {owner} add column g integer"),
        );
        let restore = at(
            &script,
            &format!("ALTER PUBLICATION p SET TABLE {member} WHERE ((g > 0));"),
        );
        assert!(
            release < drop && drop < retype && retype < add && add < restore,
            "{script}"
        );
    }
}
