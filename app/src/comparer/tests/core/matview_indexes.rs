//! Issue #235 — indexes on a materialized view.

use crate::comparer::core::*;
use super::helpers::*;
use crate::config::dump_config::DumpConfig;
use crate::config::grants_mode::GrantsMode;
use crate::dump::table_index::TableIndex;
use crate::dump::view::View;

fn mv235_index(name: &str, indexdef: &str) -> TableIndex {
    TableIndex {
        schema: "test_schema".to_string(),
        table: "mv".to_string(),
        name: name.to_string(),
        catalog: None,
        indexdef: indexdef.to_string(),
        is_partition_index: false,
        comment: None,
    }
}

fn mv235_view(definition: &str, indexes: Vec<TableIndex>) -> View {
    let mut view = View::new(
        "mv".to_string(),
        definition.to_string(),
        "test_schema".to_string(),
        vec!["test_schema.base".to_string()],
    );
    view.is_materialized = true;
    view.indexes = indexes;
    view.hash();
    view
}

const MV235_IX_VAL: &str = "CREATE INDEX ix_val ON test_schema.mv USING btree (val)";

const MV235_IX_ID: &str = "CREATE UNIQUE INDEX ix_id ON test_schema.mv USING btree (id)";

#[tokio::test]
async fn issue235_recreated_matview_restores_its_indexes() {
    // The definition changes, so the view is dropped and rebuilt. DROP
    // MATERIALIZED VIEW takes the indexes with it and nothing put them back:
    // the migration silently left the view unindexed, and because neither dump
    // carried the indexes the round-2 diff was empty and never reported it.
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    from_dump.views.push(mv235_view(
        "SELECT id, val FROM test_schema.base;",
        vec![
            mv235_index("ix_id", MV235_IX_ID),
            mv235_index("ix_val", MV235_IX_VAL),
        ],
    ));
    to_dump.views.push(mv235_view(
        "SELECT id, val, num FROM test_schema.base;",
        vec![
            mv235_index("ix_id", MV235_IX_ID),
            mv235_index("ix_val", MV235_IX_VAL),
        ],
    ));

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.drop_views().await.unwrap();
    comparer.compare_routines_and_views().await.unwrap();
    let script = comparer.get_script();

    let drop_pos = script
        .find("drop materialized view if exists test_schema.mv;")
        .expect("changed materialized view must be dropped");
    let create_pos = script
        .find("create materialized view test_schema.mv as")
        .expect("changed materialized view must be recreated");
    let id_pos = script
        .find("CREATE UNIQUE INDEX ix_id ON test_schema.mv USING btree (id);")
        .expect("the unique index must be recreated with the view");
    let val_pos = script
        .find("CREATE INDEX ix_val ON test_schema.mv USING btree (val);")
        .expect("the plain index must be recreated with the view");

    assert!(drop_pos < create_pos, "drop must precede create:\n{script}");
    assert!(
        create_pos < id_pos && create_pos < val_pos,
        "indexes must be built after the view exists:\n{script}"
    );
}

#[tokio::test]
async fn issue235_unchanged_matview_reconciles_indexes_in_place() {
    // Same definition on both sides, so the view survives; only the index set
    // moves. It must not be dropped and rebuilt just to change an index.
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let definition = "SELECT id, val FROM test_schema.base;";
    let mut recommented = mv235_index("ix_cmt", "CREATE INDEX ix_cmt ON test_schema.mv (num)");
    recommented.comment = Some("after".to_string());

    from_dump.views.push(mv235_view(
        definition,
        vec![
            mv235_index("ix_cmt", "CREATE INDEX ix_cmt ON test_schema.mv (num)"),
            mv235_index("ix_val", MV235_IX_VAL),
        ],
    ));
    to_dump.views.push(mv235_view(
        definition,
        vec![
            mv235_index("ix_cmt", "CREATE INDEX ix_cmt ON test_schema.mv (num)"),
            mv235_index("ix_id", MV235_IX_ID),
            recommented,
        ],
    ));

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.drop_views().await.unwrap();
    comparer.compare_routines_and_views().await.unwrap();
    let script = comparer.get_script();

    assert!(
        !script.to_lowercase().contains("drop materialized view"),
        "an index change must not rebuild the view's contents:\n{script}"
    );
    assert!(
        !script.to_lowercase().contains("create materialized view"),
        "an index change must not rebuild the view's contents:\n{script}"
    );
    assert!(
        script.contains("drop index if exists test_schema.ix_val;"),
        "the removed index must be dropped:\n{script}"
    );
    assert!(
        script.contains("CREATE UNIQUE INDEX ix_id ON test_schema.mv USING btree (id);"),
        "the added index must be created:\n{script}"
    );
    assert!(
        script.contains("comment on index test_schema.ix_cmt is 'after';"),
        "a comment-only change must not touch the index itself:\n{script}"
    );
    assert!(
        !script.contains("drop index if exists test_schema.ix_cmt;"),
        "a comment-only change must not drop the index:\n{script}"
    );
}

#[tokio::test]
async fn issue235_matview_with_identical_indexes_emits_nothing() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let definition = "SELECT id, val FROM test_schema.base;";
    from_dump.views.push(mv235_view(
        definition,
        vec![mv235_index("ix_val", MV235_IX_VAL)],
    ));
    to_dump.views.push(mv235_view(
        definition,
        vec![mv235_index("ix_val", MV235_IX_VAL)],
    ));

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, false, GrantsMode::Ignore);
    comparer.drop_views().await.unwrap();
    comparer.compare_routines_and_views().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.trim().is_empty(),
        "an unchanged view with unchanged indexes must produce no SQL:\n{script}"
    );
}

#[tokio::test]
async fn issue235_matview_index_drop_is_commented_out_when_use_drop_is_false() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let definition = "SELECT id, val FROM test_schema.base;";
    from_dump.views.push(mv235_view(
        definition,
        vec![mv235_index("ix_val", MV235_IX_VAL)],
    ));
    to_dump.views.push(mv235_view(definition, Vec::new()));

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Ignore);
    comparer.drop_views().await.unwrap();
    comparer.compare_routines_and_views().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("-- drop index if exists test_schema.ix_val;"),
        "use_drop=false must comment the index drop out, as it does for a table:\n{script}"
    );
    assert!(
        !script
            .lines()
            .any(|l| !l.trim_start().starts_with("--") && l.contains("drop index")),
        "no active drop may survive use_drop=false:\n{script}"
    );
}

#[tokio::test]
async fn issue235_production_mode_builds_matview_indexes_concurrently_after_commit() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    from_dump.views.push(mv235_view(
        "SELECT id, val FROM test_schema.base;",
        vec![mv235_index(
            "ix_stale",
            "CREATE INDEX ix_stale ON test_schema.mv (num)",
        )],
    ));
    to_dump.views.push(mv235_view(
        "SELECT id, val, num FROM test_schema.base;",
        vec![mv235_index("ix_id", MV235_IX_ID)],
    ));

    let mut comparer = Comparer::new(from_dump, to_dump, true, true, true, GrantsMode::Ignore);
    comparer.set_output_for_production(true);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();

    let commit_pos = script.find("commit;").expect("script must contain commit;");
    let create_pos = script
        .find("create materialized view if not exists test_schema.mv as")
        .expect("the view itself is still built inside the transaction");
    let concurrent_pos = script
        .find("CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS ix_id ON test_schema.mv USING btree (id);")
        .expect("a materialized view's index must be built concurrently in production mode");

    assert!(create_pos < commit_pos, "view create is in-txn:\n{script}");
    assert!(
        concurrent_pos > commit_pos,
        "CREATE INDEX CONCURRENTLY cannot run inside a transaction block:\n{script}"
    );
}

#[tokio::test]
async fn issue235_production_mode_drops_matview_indexes_concurrently() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let definition = "SELECT id, val FROM test_schema.base;";
    from_dump.views.push(mv235_view(
        definition,
        vec![mv235_index("ix_val", MV235_IX_VAL)],
    ));
    to_dump.views.push(mv235_view(definition, Vec::new()));

    let mut comparer = Comparer::new(from_dump, to_dump, true, true, true, GrantsMode::Ignore);
    comparer.set_output_for_production(true);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();

    let commit_pos = script.find("commit;").expect("script must contain commit;");
    let drop_pos = script
        .find("drop index concurrently if exists test_schema.ix_val;")
        .expect("an in-place index drop must be concurrent in production mode");
    assert!(
        drop_pos > commit_pos,
        "DROP INDEX CONCURRENTLY cannot run inside a transaction block:\n{script}"
    );
}

#[tokio::test]
async fn issue235_cascade_recreated_matview_guards_its_indexes() {
    // Phase 7 restores a materialized view that DROP FUNCTION ... CASCADE may
    // have taken out. The match is textual and can false-positive on a view
    // PostgreSQL never dropped, so the recreate is guarded — and the indexes
    // that would have gone with it need the same guard, or an unconditional
    // CREATE INDEX fails against the surviving one.
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    from_dump
        .routines
        .push(issue179_compute_routine("integer", "SELECT x * 2;"));
    to_dump.routines.push(issue179_compute_routine(
        "bigint",
        "SELECT (x * 2)::bigint;",
    ));

    let mut view = issue189_view("mv_things", true);
    let index = TableIndex {
        schema: "test_deps".to_string(),
        table: "mv_things".to_string(),
        name: "ix_mv_things".to_string(),
        catalog: None,
        indexdef: "CREATE INDEX ix_mv_things ON test_deps.mv_things USING btree (c)".to_string(),
        is_partition_index: false,
        comment: None,
    };
    view.indexes = vec![index];
    from_dump.views.push(view.clone());
    to_dump.views.push(view);

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_routines_and_views().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("create materialized view if not exists test_deps.mv_things as"),
        "the cascade recreate must stay guarded:\n{script}"
    );
    assert!(
        script.contains(
            "CREATE INDEX IF NOT EXISTS ix_mv_things ON test_deps.mv_things USING btree (c);"
        ),
        "the recreated view's indexes must be guarded the same way:\n{script}"
    );
}
