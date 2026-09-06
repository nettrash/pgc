//! Issue #242 — which views a table change actually drags with it.
//!
//! `dependent_view_keys` decides what has to be dropped before the table DDL
//! runs. It used to decide at table granularity: any view over a table whose
//! hash differed at all was dropped first. For a materialized view that means a
//! `DROP` and a full rebuild — the data gone until the refresh finishes, every
//! index rebuilt — triggered by a metadata-only `ADD COLUMN` the view never
//! reads.
//!
//! PostgreSQL's own rule is narrower and these tests pin pgc to it: a column a
//! view depends on cannot be dropped or retyped, and nothing else about the
//! table is forbidden (verified live on PostgreSQL 16). The dependency set comes
//! from `View::column_relation`, which `pg_depend` records for the view's
//! `_RETURN` rule.

use crate::comparer::core::*;
use crate::config::dump_config::DumpConfig;
use crate::config::grants_mode::GrantsMode;
use crate::dump::table::Table;
use crate::dump::table_column::TableColumn;
use crate::dump::view::View;

use super::helpers::int_column;

/// A `text` column, since the fixtures below key on a type change.
fn text_column(table: &str, name: &str, ordinal: i32) -> TableColumn {
    let mut column = int_column("repro", table, name, ordinal);
    column.data_type = "text".to_string();
    column.udt_name = Some("text".to_string());
    column.numeric_precision = None;
    column.numeric_precision_radix = None;
    column.numeric_scale = None;
    column
}

fn orders(columns: Vec<TableColumn>) -> Table {
    let mut table = Table::new(
        "repro".to_string(),
        "orders".to_string(),
        "repro".to_string(),
        "orders".to_string(),
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

/// A view over `repro.orders` that reads exactly `columns`.
fn view_over_orders(name: &str, is_materialized: bool, columns: &[&str]) -> View {
    let mut view = View::new(
        name.to_string(),
        " SELECT status\n   FROM repro.orders;".to_string(),
        "repro".to_string(),
        vec!["repro.orders".to_string()],
    );
    view.is_materialized = is_materialized;
    view.column_relation = Some(
        columns
            .iter()
            .map(|c| format!("repro.orders.{c}"))
            .collect(),
    );
    view.hash();
    view
}

/// FROM has `id, status, extra`; TO is whatever the caller builds. Both dumps
/// carry the same pair of views, one materialized and one regular, each reading
/// only `status`.
async fn script_for(to_table: Table) -> String {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    from_dump.tables.push(orders(vec![
        int_column("repro", "orders", "id", 1),
        text_column("orders", "status", 2),
        int_column("repro", "orders", "extra", 3),
    ]));
    to_dump.tables.push(to_table);

    for dump in [&mut from_dump, &mut to_dump] {
        dump.views.push(view_over_orders("mv", true, &["status"]));
        dump.views.push(view_over_orders("v", false, &["status"]));
    }

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.drop_views().await.unwrap();
    comparer.get_script()
}

#[tokio::test]
async fn adding_an_unrelated_column_touches_no_view() {
    // The reported case. `created_at` is new, so no view can be reading it.
    let script = script_for(orders(vec![
        int_column("repro", "orders", "id", 1),
        text_column("orders", "status", 2),
        int_column("repro", "orders", "extra", 3),
        int_column("repro", "orders", "created_at", 4),
    ]))
    .await;

    assert!(!script.contains("drop materialized view"), "{script}");
    assert!(!script.contains("drop view"), "{script}");
}

#[tokio::test]
async fn dropping_an_unread_column_touches_no_view() {
    // PostgreSQL allows this with both views in place: neither depends on
    // `extra`.
    let script = script_for(orders(vec![
        int_column("repro", "orders", "id", 1),
        text_column("orders", "status", 2),
    ]))
    .await;

    assert!(!script.contains("drop materialized view"), "{script}");
    assert!(!script.contains("drop view"), "{script}");
}

#[tokio::test]
async fn retyping_an_unread_column_touches_no_view() {
    let mut extra = int_column("repro", "orders", "extra", 3);
    extra.data_type = "bigint".to_string();
    extra.udt_name = Some("int8".to_string());
    let script = script_for(orders(vec![
        int_column("repro", "orders", "id", 1),
        text_column("orders", "status", 2),
        extra,
    ]))
    .await;

    assert!(!script.contains("drop materialized view"), "{script}");
    assert!(!script.contains("drop view"), "{script}");
}

#[tokio::test]
async fn retyping_a_column_the_views_read_still_drops_them() {
    // The half that must not regress: PostgreSQL refuses "alter type of a
    // column used by a view or rule", so the views have to go first.
    let mut status = text_column("orders", "status", 2);
    status.data_type = "character varying".to_string();
    status.udt_name = Some("varchar".to_string());
    status.character_maximum_length = Some(50);
    let script = script_for(orders(vec![
        int_column("repro", "orders", "id", 1),
        status,
        int_column("repro", "orders", "extra", 3),
    ]))
    .await;

    assert!(
        script.contains("drop materialized view if exists repro.mv"),
        "{script}"
    );
    assert!(script.contains("drop view if exists repro.v"), "{script}");
}

#[tokio::test]
async fn dropping_a_column_the_views_read_still_drops_them() {
    let script = script_for(orders(vec![
        int_column("repro", "orders", "id", 1),
        int_column("repro", "orders", "extra", 3),
    ]))
    .await;

    assert!(
        script.contains("drop materialized view if exists repro.mv"),
        "{script}"
    );
    assert!(script.contains("drop view if exists repro.v"), "{script}");
}

#[tokio::test]
async fn changing_only_nullability_of_a_read_column_touches_no_view() {
    // A view reading `status` survives `SET NOT NULL` on it — only the column's
    // *type* is protected, which is why `type_differs` and not a whole-column
    // hash decides this.
    let mut status = text_column("orders", "status", 2);
    status.is_nullable = false;
    let script = script_for(orders(vec![
        int_column("repro", "orders", "id", 1),
        status,
        int_column("repro", "orders", "extra", 3),
    ]))
    .await;

    assert!(!script.contains("drop materialized view"), "{script}");
    assert!(!script.contains("drop view"), "{script}");
}

#[tokio::test]
async fn a_view_reading_no_column_survives_a_column_drop() {
    // `select count(*) from t` depends on the relation but on none of its
    // columns, so nothing about a column can force it out. `Some(vec![])` is
    // that answer, and is deliberately different from `None`.
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());
    from_dump.tables.push(orders(vec![
        int_column("repro", "orders", "id", 1),
        int_column("repro", "orders", "extra", 2),
    ]));
    to_dump
        .tables
        .push(orders(vec![int_column("repro", "orders", "id", 1)]));
    for dump in [&mut from_dump, &mut to_dump] {
        dump.views.push(view_over_orders("mv_count", true, &[]));
    }

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.drop_views().await.unwrap();

    assert!(
        !comparer.get_script().contains("drop materialized view"),
        "{}",
        comparer.get_script()
    );
}

#[tokio::test]
async fn a_dropped_table_still_takes_its_views() {
    // No column survives, so column granularity has nothing to say: the view
    // goes because the relation it names does.
    let mut from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());
    from_dump.tables.push(orders(vec![
        int_column("repro", "orders", "id", 1),
        text_column("orders", "status", 2),
    ]));
    from_dump
        .views
        .push(view_over_orders("mv", true, &["status"]));

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.drop_views().await.unwrap();

    assert!(
        comparer
            .get_script()
            .contains("drop materialized view if exists repro.mv"),
        "{}",
        comparer.get_script()
    );
}

#[tokio::test]
async fn a_dump_without_column_dependencies_keeps_the_table_level_answer() {
    // Backward compatibility: a dump written before `column_relation` existed
    // has `None`, and those views must keep being dropped on any change to a
    // table they read — over-eager, but never a broken migration.
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());
    from_dump.tables.push(orders(vec![
        int_column("repro", "orders", "id", 1),
        text_column("orders", "status", 2),
    ]));
    to_dump.tables.push(orders(vec![
        int_column("repro", "orders", "id", 1),
        text_column("orders", "status", 2),
        int_column("repro", "orders", "created_at", 3),
    ]));
    for dump in [&mut from_dump, &mut to_dump] {
        let mut view = view_over_orders("mv", true, &["status"]);
        view.column_relation = None; // as an older pgc wrote it
        dump.views.push(view);
    }

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.drop_views().await.unwrap();

    assert!(
        comparer
            .get_script()
            .contains("drop materialized view if exists repro.mv"),
        "{}",
        comparer.get_script()
    );
}

// ── PR #247 review: what counts as a "retype" ──────────────────────────
// The gate has to answer for the statement the comparer actually emits,
// not for a hand-written list of type attributes. `TableColumn::
// get_alter_script` emits `ALTER COLUMN ... TYPE` whenever the rendered
// type *clause* differs, and that clause carries the interval qualifier
// and the collation as well as the type name — neither of which a field
// list remembers. PostgreSQL rejects the statement for a collation-only
// change exactly as firmly as for a real one (verified live on
// PostgreSQL 16).

#[tokio::test]
async fn a_collation_change_on_a_read_column_still_drops_the_views() {
    // The collation alone is invisible to the table hash, so this pairs it
    // with a default change — which is what makes the comparer produce an
    // alter script for the table at all, and therefore what makes the
    // collation reach `ALTER COLUMN ... TYPE`.
    let mut from_status = text_column("orders", "status", 2);
    from_status.collation_name = Some("C".to_string());
    from_status.column_default = Some("'x'::text".to_string());

    let mut to_status = text_column("orders", "status", 2);
    to_status.collation_name = Some("POSIX".to_string());
    to_status.column_default = Some("'y'::text".to_string());

    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());
    from_dump.tables.push(orders(vec![
        int_column("repro", "orders", "id", 1),
        from_status,
    ]));
    to_dump.tables.push(orders(vec![
        int_column("repro", "orders", "id", 1),
        to_status,
    ]));
    for dump in [&mut from_dump, &mut to_dump] {
        dump.views.push(view_over_orders("mv", true, &["status"]));
        dump.views.push(view_over_orders("v", false, &["status"]));
    }

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.drop_views().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("drop materialized view if exists repro.mv"),
        "a collation change is emitted as ALTER COLUMN ... TYPE, which \
         PostgreSQL refuses while the view exists:\n{script}"
    );
    assert!(script.contains("drop view if exists repro.v"), "{script}");
}

#[tokio::test]
async fn an_interval_qualifier_change_on_a_read_column_still_drops_the_views() {
    // Same shape, the other field `render_type_clause` carries and a field
    // list forgets: `interval` -> `interval day`.
    let mut from_status = text_column("orders", "status", 2);
    from_status.data_type = "interval".to_string();
    from_status.udt_name = Some("interval".to_string());
    from_status.column_default = Some("'1 day'::interval".to_string());

    let mut to_status = from_status.clone();
    to_status.interval_type = Some("DAY".to_string());
    to_status.column_default = Some("'2 days'::interval".to_string());

    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());
    from_dump.tables.push(orders(vec![
        int_column("repro", "orders", "id", 1),
        from_status,
    ]));
    to_dump.tables.push(orders(vec![
        int_column("repro", "orders", "id", 1),
        to_status,
    ]));
    for dump in [&mut from_dump, &mut to_dump] {
        dump.views.push(view_over_orders("mv", true, &["status"]));
    }

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.drop_views().await.unwrap();

    assert!(
        comparer
            .get_script()
            .contains("drop materialized view if exists repro.mv"),
        "{}",
        comparer.get_script()
    );
}
