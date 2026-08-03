//! Integration tests for the `clear` command's script generation.
//!
//! `Dump::generate_clear_script` is the only part of `clear` that does not need
//! a live server: the drop order it produces is the whole correctness argument,
//! since PostgreSQL rejects drops that run before their dependents. The shape
//! asserted here matches `data/test/clear_expected.sql`.

mod common;

use common::{
    empty_dump, extension, foreign_key, materialized_view, populated_dump, routine, schema,
    sequence, table, view,
};

/// Offset of the first occurrence of `needle`, or a panic naming the script —
/// every ordering assertion below reads better as a byte offset.
///
/// Needles must be specific enough to skip the section banners: the script
/// contains `/* ---> Drop Views --- */` and `/* ---> Drop Tables --- */`, whose
/// plural forms would otherwise match `drop view` and `drop table` ahead of the
/// statements they introduce. Matching on the `if exists` suffix pins each
/// needle to a real statement.
fn position_of(script: &str, needle: &str) -> usize {
    script
        .find(needle)
        .unwrap_or_else(|| panic!("expected {needle:?} in clear script:\n{script}"))
}

fn clear_script_of_populated_dump() -> String {
    let mut dump = populated_dump("shop");
    dump.types.clear(); // building a PgType needs 30+ catalog columns; not what this file covers
    dump.generate_clear_script(true, true, false)
}

#[test]
fn objects_are_dropped_in_dependency_safe_order() {
    let script = clear_script_of_populated_dump().to_lowercase();

    let matview = position_of(&script, "drop materialized view if exists");
    let regular_view = position_of(&script, "drop view if exists");
    let fk = position_of(&script, "drop constraint if exists");
    let table = position_of(&script, "drop table if exists");
    let routine = position_of(&script, "drop function if exists");
    let sequence = position_of(&script, "drop sequence if exists");
    let extension = position_of(&script, "drop extension if exists");
    let schema = position_of(&script, "drop schema if exists");

    assert!(matview < regular_view, "materialized views drop first");
    assert!(regular_view < fk, "views drop before foreign keys");
    assert!(fk < table, "foreign keys drop before their tables");
    assert!(table < routine, "tables drop before routines");
    assert!(routine < sequence, "routines drop before sequences");
    assert!(sequence < extension, "sequences drop before extensions");
    assert!(extension < schema, "extensions drop last before schemas");
}

#[test]
fn a_view_is_dropped_before_the_view_it_depends_on() {
    let mut dump = empty_dump("shop");
    dump.schemas.push(schema("app"));
    dump.tables.push(table("app", "customers", &["id"]));
    dump.views.push(view(
        "app",
        "base",
        " SELECT id FROM app.customers;",
        &["app.customers"],
    ));
    dump.views.push(view(
        "app",
        "derived",
        " SELECT id FROM app.base;",
        &["app.base"],
    ));

    let script = dump.generate_clear_script(true, true, false).to_lowercase();
    assert!(
        position_of(&script, "drop view if exists app.derived")
            < position_of(&script, "drop view if exists app.base"),
        "a dependent view must be dropped before its source:\n{script}"
    );
}

#[test]
fn foreign_keys_are_dropped_before_the_tables_they_constrain() {
    let mut dump = empty_dump("shop");
    dump.schemas.push(schema("app"));
    dump.tables.push(table("app", "customers", &["id"]));
    let mut orders = table("app", "orders", &["id", "customer_id"]);
    orders.constraints.push(foreign_key(
        "app",
        "orders",
        "fk_orders_customer",
        "customer_id",
        "app.customers",
        "id",
    ));
    dump.tables.push(orders);

    let script = dump.generate_clear_script(true, true, false).to_lowercase();
    assert!(
        position_of(&script, "drop constraint if exists fk_orders_customer")
            < position_of(&script, "drop table if exists"),
        "every FK must be dropped before any table:\n{script}"
    );
}

#[test]
fn single_transaction_wraps_the_clear_script() {
    let with_tx = clear_script_of_populated_dump().to_lowercase();
    assert!(with_tx.contains("begin;"), "expected begin:\n{with_tx}");
    assert!(with_tx.contains("commit;"), "expected commit:\n{with_tx}");

    let mut dump = populated_dump("shop");
    dump.types.clear();
    let without_tx = dump
        .generate_clear_script(false, true, false)
        .to_lowercase();
    assert!(
        !without_tx.contains("begin;"),
        "use_single_transaction=false must not open a transaction:\n{without_tx}"
    );
}

/// `use_comments = false` drops the per-statement annotations and the section
/// banners, but deliberately keeps the generated-by header: it records which
/// database and schemas the script came from, which is provenance rather than
/// commentary.
#[test]
fn use_comments_false_strips_the_per_statement_commentary() {
    let mut dump = populated_dump("shop");
    dump.types.clear();
    let script = dump.generate_clear_script(true, false, false);

    assert!(
        script.starts_with("/*") && script.contains("Script generated by"),
        "the generated-by header stays:\n{script}"
    );
    let after_header = &script[script.find("*/").expect("header is closed") + 2..];
    for line in after_header.lines() {
        let line = line.trim();
        assert!(
            !line.starts_with("/*") && !line.starts_with("--"),
            "use_comments=false left a comment behind: {line}"
        );
    }
    assert!(
        !after_header.contains("---> Drop"),
        "section banners are dropped"
    );
    // Stripping comments must not strip the statements.
    assert!(after_header.to_lowercase().contains("drop table if exists"));
}

#[test]
fn cascade_is_opt_in() {
    let mut dump = populated_dump("shop");
    dump.types.clear();

    let plain = dump.generate_clear_script(true, true, false).to_lowercase();
    assert!(
        !plain.contains("cascade"),
        "cascade must not appear unless requested:\n{plain}"
    );

    let cascading = dump.generate_clear_script(true, true, true).to_lowercase();
    assert!(
        cascading.contains("cascade"),
        "use_cascade=true must emit CASCADE:\n{cascading}"
    );
}

#[test]
fn an_empty_dump_produces_a_script_with_no_drops() {
    let script = empty_dump("blank")
        .generate_clear_script(true, true, false)
        .to_lowercase();

    assert!(
        !script.contains("drop "),
        "nothing to drop, so nothing should be dropped:\n{script}"
    );
}

#[test]
fn every_object_kind_present_in_the_dump_is_dropped() {
    let mut dump = empty_dump("shop");
    dump.schemas.push(schema("app"));
    dump.extensions.push(extension("pgcrypto", "1.3", "public"));
    dump.tables.push(table("app", "customers", &["id"]));
    dump.views.push(view(
        "app",
        "v",
        " SELECT id FROM app.customers;",
        &["app.customers"],
    ));
    dump.views.push(materialized_view(
        "app",
        "mv",
        " SELECT id FROM app.customers;",
        &["app.customers"],
    ));
    dump.routines
        .push(routine("app", 900, "compute", "integer", "SELECT $1"));
    dump.sequences.push(sequence("app", "s"));

    let script = dump.generate_clear_script(true, true, false).to_lowercase();

    for object in [
        "app.customers",
        "app.v",
        "app.mv",
        "app.compute",
        "app.s",
        "pgcrypto",
        "app",
    ] {
        assert!(
            script.contains(object),
            "clear script never mentions {object}:\n{script}"
        );
    }
}
