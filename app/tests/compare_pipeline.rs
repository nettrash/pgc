//! End-to-end tests for the `compare` command's public path.
//!
//! These drive the same sequence `main.rs` does — build two [`Dump`]s, write
//! them out, read them back, hand them to [`Comparer`], and save the script —
//! so they cover the dump→file→dump hop that the in-memory unit tests skip.

mod common;

use common::{ScratchDir, assert_no_ddl, index, populated_dump, schema, table, view};
use pgc::comparer::core::Comparer;
use pgc::config::grants_mode::GrantsMode;
use pgc::dump::core::Dump;

/// Round-trip both dumps through the filesystem, then compare them, exactly as
/// `pgc --command compare --from … --to …` does.
async fn compare_via_files(from: Dump, to: Dump, use_drop: bool, label: &str) -> String {
    let dir = ScratchDir::new(label);
    let from_path = dir.path_str("dump.from");
    let to_path = dir.path_str("dump.to");
    let out_path = dir.path_str("output.sql");

    from.write_to_file(&from_path).expect("write FROM dump");
    to.write_to_file(&to_path).expect("write TO dump");

    let from = Dump::read_from_file(&from_path)
        .await
        .expect("read FROM dump");
    let to = Dump::read_from_file(&to_path).await.expect("read TO dump");

    let mut comparer = Comparer::new(from, to, use_drop, false, true, GrantsMode::Ignore);
    comparer.compare().await.expect("compare");
    comparer.save_script(&out_path).await.expect("save script");

    std::fs::read_to_string(&out_path).expect("read generated script")
}

/// The core correctness property, at the level this test can reach without a
/// live server: a schema compared against itself needs no migration.
#[tokio::test]
async fn comparing_a_dump_against_itself_emits_no_ddl() {
    let script = compare_via_files(
        populated_dump("shop"),
        populated_dump("shop"),
        true,
        "self-compare",
    )
    .await;

    assert_no_ddl(&script, "self-comparison");
}

#[tokio::test]
async fn a_new_table_is_created_and_a_removed_one_is_dropped() {
    let from = populated_dump("shop");
    let mut to = populated_dump("shop");
    to.tables.push(table("app", "invoices", &["id", "total"]));
    to.tables.retain(|t| t.name != "orders");
    // `order_summary` reads `app.orders`; drop it too so the TO side is
    // self-consistent.
    to.views.retain(|v| v.name != "order_summary");

    let script = compare_via_files(from, to, true, "add-drop-table").await;

    assert!(
        script.contains("create table") || script.contains("CREATE TABLE"),
        "expected a CREATE TABLE for app.invoices:\n{script}"
    );
    assert!(script.contains("invoices"), "new table missing:\n{script}");
    assert!(
        script.to_lowercase().contains("drop table"),
        "expected a DROP TABLE for app.orders:\n{script}"
    );
}

/// With `use_drop = false` the destructive statements must still be *present*,
/// but commented out, so a reviewer can see and opt into them.
#[tokio::test]
async fn destructive_statements_are_commented_out_when_use_drop_is_false() {
    let from = populated_dump("shop");
    let mut to = populated_dump("shop");
    to.tables.retain(|t| t.name != "orders");
    to.views.retain(|v| v.name != "order_summary");

    let script = compare_via_files(from, to, false, "no-drop").await;

    let uncommented_drop = script.lines().any(|l| {
        let l = l.trim().to_lowercase();
        l.starts_with("drop table")
    });
    assert!(
        !uncommented_drop,
        "use_drop=false must not emit an active DROP TABLE:\n{script}"
    );
}

#[tokio::test]
async fn a_changed_view_definition_is_re_emitted() {
    let from = populated_dump("shop");
    let mut to = populated_dump("shop");
    to.views.retain(|v| v.name != "order_summary");
    to.views.push(view(
        "app",
        "order_summary",
        " SELECT o.id\n   FROM app.orders o;",
        &["app.orders"],
    ));

    let script = compare_via_files(from, to, true, "view-change").await;
    let lower = script.to_lowercase();
    assert!(
        lower.contains("view app.order_summary") || lower.contains("view order_summary"),
        "expected app.order_summary to be re-emitted:\n{script}"
    );
}

#[tokio::test]
async fn single_transaction_wraps_the_script_in_begin_and_commit() {
    let dir = ScratchDir::new("single-tx");
    let out_path = dir.path_str("output.sql");

    let mut to = populated_dump("shop");
    to.tables.push(table("app", "invoices", &["id"]));

    let mut comparer = Comparer::new(
        populated_dump("shop"),
        to,
        true,
        true, // use_single_transaction
        true,
        GrantsMode::Ignore,
    );
    comparer.compare().await.expect("compare");
    comparer.save_script(&out_path).await.expect("save script");

    let script = std::fs::read_to_string(&out_path).expect("read script");
    let lower = script.to_lowercase();
    let begin = lower
        .find("begin;")
        .expect("script must open a transaction");
    let commit = lower.rfind("commit;").expect("script must commit");
    assert!(begin < commit, "begin must precede commit:\n{script}");
}

/// `--output-for-production` adds idempotency guards and moves statements that
/// cannot run inside a transaction into a post-commit section. Off by default.
///
/// The assertions below deliberately name whole statements rather than looking
/// for a bare `if not exists`. That substring is **not** production-specific:
/// `Schema::get_script` and `Extension::get_script` emit `create schema if not
/// exists` / `create extension if not exists` on the default path too, so the
/// fixture below — which adds a schema — contains it either way. A test
/// matching only the substring would keep passing if the rewrite stopped
/// working entirely.
#[tokio::test]
async fn production_mode_is_opt_in() {
    let build = |production: bool| async move {
        let mut to = populated_dump("shop");
        // A new schema: emits `create schema if not exists` on BOTH paths, and
        // is what makes the naive substring check useless.
        to.schemas.push(schema("reporting"));
        // A new table with an index: `create table` gains a guard and the index
        // build becomes CONCURRENTLY, only under production mode.
        let mut invoices = table("app", "invoices", &["id"]);
        invoices
            .indexes
            .push(index("app", "invoices", "ix_invoices_id", "id"));
        invoices.hash();
        to.tables.push(invoices);

        let mut comparer = Comparer::new(
            populated_dump("shop"),
            to,
            true,
            true,
            true,
            GrantsMode::Ignore,
        );
        comparer.set_output_for_production(production);
        comparer.compare().await.expect("compare");
        let dir = ScratchDir::new(if production { "prod-on" } else { "prod-off" });
        let out = dir.path_str("output.sql");
        comparer.save_script(&out).await.expect("save script");
        std::fs::read_to_string(&out)
            .expect("read script")
            .to_lowercase()
    };

    let default_script = build(false).await;
    let production_script = build(true).await;

    // The confound, pinned: present on both paths, so it discriminates nothing.
    for (label, script) in [
        ("default", &default_script),
        ("production", &production_script),
    ] {
        assert!(
            script.contains("create schema if not exists reporting;"),
            "{label} output should carry the unguarded-by-production schema create:\n{script}"
        );
    }

    // Default path: no guard on CREATE TABLE, index built inline, no
    // post-commit section.
    assert!(
        default_script.contains("create table app.invoices"),
        "default output must create the table without a guard:\n{default_script}"
    );
    assert!(
        !default_script.contains("create table if not exists app.invoices"),
        "the CREATE TABLE guard is production-only:\n{default_script}"
    );
    assert!(
        !default_script.contains("concurrently"),
        "default output must build indexes inline:\n{default_script}"
    );
    assert!(
        !default_script.contains("post-commit"),
        "default output must not carry a post-commit section:\n{default_script}"
    );

    // Production path: guarded CREATE TABLE, CONCURRENTLY index, and the
    // statement that cannot run in a transaction emitted after the commit.
    assert!(
        production_script.contains("create table if not exists app.invoices"),
        "production output must guard CREATE TABLE:\n{production_script}"
    );
    assert!(
        production_script.contains("create index concurrently if not exists ix_invoices_id"),
        "production output must build the index concurrently:\n{production_script}"
    );

    let commit = production_script
        .rfind("commit;")
        .expect("production script commits");
    let concurrent_index = production_script
        .find("create index concurrently")
        .expect("production script builds the index concurrently");
    assert!(
        concurrent_index > commit,
        "CREATE INDEX CONCURRENTLY cannot run inside a transaction, so it must \
         be emitted after commit:\n{production_script}"
    );
}
