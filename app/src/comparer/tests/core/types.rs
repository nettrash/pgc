//! Tests for `compare_types` and `compare_enums`, including composite and
//! multirange types.

use super::helpers::*;
use crate::comparer::core::*;
use crate::config::dump_config::DumpConfig;
use crate::config::grants_mode::GrantsMode;
use crate::dump::pg_type::PgType;
use crate::dump::pg_type::{CompositeAttribute, DomainConstraint};
use crate::dump::routine::Routine;
use crate::dump::table::Table;
use crate::dump::table_column::TableColumn;
use crate::dump::view::View;
use sqlx::postgres::types::Oid;

fn make_composite_type(
    schema: &str,
    name: &str,
    oid: u32,
    attributes: Vec<(&str, &str)>,
) -> PgType {
    let mut composite_type = make_domain_type(schema, name, oid);
    composite_type.typtype = 'c' as i8;
    composite_type.typcategory = 'C' as i8;
    composite_type.typinput = "record_in".to_string();
    composite_type.typoutput = "record_out".to_string();
    composite_type.typbasetype = None;
    composite_type.formatted_basetype = None;
    composite_type.domain_constraints.clear();
    composite_type.composite_attributes = attributes
        .into_iter()
        .map(|(attribute_name, data_type)| CompositeAttribute {
            name: attribute_name.to_string(),
            data_type: data_type.to_string(),
            collation: None,
        })
        .collect();
    composite_type.hash();
    composite_type
}

#[tokio::test]
async fn compare_drops_types_after_routines() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());

    let dropped_type = make_domain_type("test_schema", "status_type", 501);
    from_dump.types.push(dropped_type);

    let dropped_routine = Routine::new(
        "test_schema".to_string(),
        Oid(1),
        "get_users_by_status".to_string(),
        "plpgsql".to_string(),
        "FUNCTION".to_string(),
        "integer".to_string(),
        "status test_schema.status_type".to_string(),
        None,
        None,
        "BEGIN RETURN 1; END".to_string(),
    );
    from_dump.routines.push(dropped_routine);

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();

    let routine_drop_pos = script
        .find("drop function if exists test_schema.get_users_by_status")
        .expect("routine drop script not found");
    let type_drop_pos = script
        .find("drop type if exists test_schema.status_type cascade;")
        .expect("type drop script not found");

    assert!(
        routine_drop_pos < type_drop_pos,
        "Type drops must be emitted after routine drops"
    );
}

#[tokio::test]
async fn compare_drops_enums_after_routines() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());

    let dropped_enum = make_enum_type(
        "test_schema",
        "status_enum",
        502,
        vec!["active", "inactive"],
    );
    from_dump.types.push(dropped_enum);

    let dropped_routine = Routine::new(
        "test_schema".to_string(),
        Oid(2),
        "get_users_by_status_enum".to_string(),
        "plpgsql".to_string(),
        "FUNCTION".to_string(),
        "integer".to_string(),
        "status test_schema.status_enum".to_string(),
        None,
        None,
        "BEGIN RETURN 1; END".to_string(),
    );
    from_dump.routines.push(dropped_routine);

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();

    let routine_drop_pos = script
        .find("drop function if exists test_schema.get_users_by_status_enum")
        .expect("routine drop script not found");
    let enum_drop_pos = script
        .find("drop type if exists test_schema.status_enum cascade;")
        .expect("enum drop script not found");

    assert!(
        routine_drop_pos < enum_drop_pos,
        "Enum drops must be emitted after routine drops"
    );
}

#[tokio::test]
async fn compare_composite_types_drops_removed_and_creates_new() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    from_dump.types.push(make_composite_type(
        "test_schema",
        "test_type_A",
        601,
        vec![
            ("first_name_2", "varchar(50)"),
            ("last_name_2", "varchar(50)"),
        ],
    ));
    to_dump.types.push(make_composite_type(
        "test_schema",
        "test_type_B",
        602,
        vec![("street", "varchar(255)"), ("city", "varchar(100)")],
    ));

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();

    assert!(script.contains("create type test_schema.test_type_B as ("));
    assert!(script.contains("\"street\" varchar(255)"));
    assert!(script.contains("\"city\" varchar(100)"));
    assert!(script.contains("drop type if exists test_schema.test_type_A cascade;"));
}

/// Multirange types are auto-dropped when their associated range type is
/// dropped.  The comparer must NOT emit a separate DROP for the multirange,
/// otherwise PostgreSQL rejects it ("cannot drop type … because type …
/// requires it").
#[tokio::test]
async fn compare_types_multirange_not_dropped_independently() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());

    // Range type present only in FROM → will be dropped.
    let mut range_type = make_domain_type("test_schema", "old_range", 600);
    range_type.typtype = 'r' as i8;
    range_type.range_subtype = Some("integer".to_string());

    // Associated multirange type present only in FROM.
    let mut mr_type = make_domain_type("test_schema", "old_multirange", 601);
    mr_type.typtype = 'm' as i8;

    from_dump.types.push(range_type);
    from_dump.types.push(mr_type);

    let mut comparer = Comparer::new(from_dump, to_dump, false, true, true, GrantsMode::Full);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("drop type if exists test_schema.old_range cascade;"),
        "Range type must be dropped, got: {script}"
    );
    let has_mr_drop = script.contains("drop type if exists test_schema.old_multirange");
    assert!(
        !has_mr_drop,
        "Multirange type must NOT be dropped independently, got: {script}"
    );
}

/// Symmetric to the drop-side test above: multirange types are auto-CREATED
/// by PostgreSQL when the range type is created. The comparer must NOT emit
/// any per-multirange output in the main script — the `CREATE TYPE … AS
/// RANGE` for the range is enough. Previously the CREATE loop skipped only
/// enums, so a new range also produced a stray `-- Multirange …` comment
/// that made new-range diffs look noisy and was the leading explanation for
/// "the diff looks empty" reports on fresh schema_b dumps.
#[tokio::test]
async fn compare_types_multirange_not_created_independently() {
    let from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    // New range type only in TO.
    let mut range_type = make_domain_type("test_schema", "int_range", 800);
    range_type.typtype = 'r' as i8;
    range_type.range_subtype = Some("integer".to_string());
    range_type.hash();

    // Its auto-generated multirange, also only in TO.
    let mut mr_type = make_domain_type("test_schema", "int_range_multirange", 801);
    mr_type.typtype = 'm' as i8;
    mr_type.hash();

    to_dump.types.push(range_type);
    to_dump.types.push(mr_type);

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("create type test_schema.int_range as range"),
        "Range type must be created, got: {script}"
    );
    let has_mr_comment = script
        .contains("Multirange type test_schema.int_range_multirange is created automatically");
    assert!(
        !has_mr_comment,
        "Multirange must not emit a stand-alone comment block, got: {script}"
    );
    let has_mr_create = script.contains("create type test_schema.int_range_multirange");
    assert!(
        !has_mr_create,
        "Multirange must not be CREATED independently, got: {script}"
    );
}

/// A multirange that exists in BOTH dumps but whose owner or comment has
/// changed must still emit an ALTER (COMMENT ON TYPE / ALTER TYPE OWNER).
/// Regression guard against over-broad `'m'` skipping: the skip lives in the
/// new-in-`to` branch only, so metadata drift on existing multiranges still
/// propagates via `get_alter_script`'s comment/owner diff tail.
#[tokio::test]
async fn compare_types_multirange_comment_change_still_emits_alter() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_mr = make_domain_type("test_schema", "my_range_multirange", 900);
    from_mr.typtype = 'm' as i8;
    from_mr.comment = None;
    from_mr.hash();

    let mut to_mr = make_domain_type("test_schema", "my_range_multirange", 900);
    to_mr.typtype = 'm' as i8;
    to_mr.comment = Some("updated description".to_string());
    to_mr.hash();

    from_dump.types.push(from_mr);
    to_dump.types.push(to_mr);

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script
            .contains("comment on type test_schema.my_range_multirange is 'updated description';"),
        "Metadata ALTER on existing multirange must still emit, got: {script}"
    );
}

// ---- Domain CHECK changes run after what they check against ----

fn constrained_domain(name: &str, constraints: &[(&str, &str)]) -> PgType {
    let mut domain = make_domain_type("test_schema", name, 610);
    domain.formatted_basetype = Some("integer".to_string());
    domain.domain_constraints = constraints
        .iter()
        .map(|(name, definition)| DomainConstraint {
            name: name.to_string(),
            definition: definition.to_string(),
        })
        .collect();
    domain.hash();
    domain
}

fn legacy_table() -> Table {
    let mut table = Table::new(
        "test_schema".to_string(),
        "legacy".to_string(),
        "test_schema".to_string(),
        "legacy".to_string(),
        "postgres".to_string(),
        None,
        vec![int_column("test_schema", "legacy", "q", 1)],
        vec![],
        vec![],
        vec![],
        None,
    );
    table.hash();
    table
}

fn position(script: &str, needle: &str) -> usize {
    script
        .find(needle)
        .unwrap_or_else(|| panic!("`{needle}` not found in:\n{script}"))
}

async fn compare_script(from: Dump, to: Dump, use_single_transaction: bool) -> String {
    let mut comparer = Comparer::new(
        from,
        to,
        true,
        use_single_transaction,
        true,
        GrantsMode::Ignore,
    );
    comparer.compare().await.unwrap();
    comparer.get_script()
}

/// A tightened CHECK scans every column of the domain, so it has to wait
/// until the tables the migration drops are gone — they may hold values only
/// the old CHECK allowed. The drop of the old CHECK stays early.
#[tokio::test]
async fn compare_adds_changed_domain_check_after_dropping_tables() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());
    from_dump.types.push(constrained_domain(
        "qty",
        &[("qty_range", "CHECK ((VALUE > 0))")],
    ));
    to_dump.types.push(constrained_domain(
        "qty",
        &[("qty_range", "CHECK ((VALUE > 5))")],
    ));
    from_dump.tables.push(legacy_table());

    let script = compare_script(from_dump, to_dump, true).await;

    let drop_check = position(
        &script,
        "alter domain test_schema.qty drop constraint \"qty_range\";",
    );
    let drop_table = position(&script, "drop table if exists test_schema.legacy");
    let add_check = position(
        &script,
        "alter domain test_schema.qty add constraint \"qty_range\" CHECK ((VALUE > 5));",
    );
    assert!(drop_check < drop_table, "{script}");
    assert!(drop_table < add_check, "{script}");
    assert!(add_check < position(&script, "commit;"), "{script}");
    // The domain's only change is deferred, so it gets no "no changes" note.
    assert!(
        !script.contains("requires no supported changes"),
        "{script}"
    );
    assert!(
        !script.contains("No supported alterations for this type"),
        "{script}"
    );
}

/// VALIDATE CONSTRAINT scans the same data an ADD does.
#[tokio::test]
async fn compare_validates_domain_check_after_dropping_tables() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());
    from_dump.types.push(constrained_domain(
        "qty",
        &[("qty_nonneg", "CHECK ((VALUE >= 0)) NOT VALID")],
    ));
    to_dump.types.push(constrained_domain(
        "qty",
        &[("qty_nonneg", "CHECK ((VALUE >= 0))")],
    ));
    from_dump.tables.push(legacy_table());

    let script = compare_script(from_dump, to_dump, false).await;

    let drop_table = position(&script, "drop table if exists test_schema.legacy");
    let validate = position(
        &script,
        "alter domain test_schema.qty validate constraint \"qty_nonneg\";",
    );
    assert!(drop_table < validate, "{script}");
}

/// A CHECK that calls a routine new in TO can only be added once the
/// routine exists.
#[tokio::test]
async fn compare_adds_domain_check_after_the_routine_it_calls() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());
    from_dump.types.push(constrained_domain("code", &[]));
    to_dump.types.push(constrained_domain(
        "code",
        &[("code_fmt", "CHECK (test_schema.is_code(VALUE))")],
    ));
    let mut routine = Routine::new(
        "test_schema".to_string(),
        Oid(611),
        "is_code".to_string(),
        "sql".to_string(),
        "FUNCTION".to_string(),
        "boolean".to_string(),
        "v integer".to_string(),
        None,
        None,
        "select v > 0".to_string(),
    );
    routine.hash();
    to_dump.routines.push(routine);

    let script = compare_script(from_dump, to_dump, true).await;

    let create_routine = position(&script, "function test_schema.is_code(");
    let add_check = position(
        &script,
        "alter domain test_schema.code add constraint \"code_fmt\" CHECK (test_schema.is_code(VALUE));",
    );
    assert!(create_routine < add_check, "{script}");
    // Deferred to its own section, headed by the type it belongs to.
    let section = position(&script, "/* ---> Domain constraints: Add section");
    assert!(section < add_check, "{script}");
    assert!(
        script[section..add_check].contains("/* Type: test_schema.code */"),
        "{script}"
    );
}

/// Relaxing (dropping) stays in the types section, and a domain with both
/// an in-place and a deferred change is headed in both places.
#[tokio::test]
async fn compare_keeps_domain_check_drops_in_the_types_section() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());
    from_dump.types.push(constrained_domain(
        "qty",
        &[("old_chk", "CHECK ((VALUE <> 7))")],
    ));
    to_dump.types.push(constrained_domain(
        "qty",
        &[("new_chk", "CHECK ((VALUE > 0))")],
    ));

    let script = compare_script(from_dump, to_dump, false).await;

    let types_end = position(&script, "/* ---> User-defined types: End");
    let drop_check = position(
        &script,
        "alter domain test_schema.qty drop constraint \"old_chk\";",
    );
    let add_check = position(
        &script,
        "alter domain test_schema.qty add constraint \"new_chk\" CHECK ((VALUE > 0));",
    );
    assert!(drop_check < types_end, "{script}");
    assert!(types_end < add_check, "{script}");
    assert_eq!(script.matches("/* Type: test_schema.qty */").count(), 2);
}

// ---- Enum values added before a single transaction ----

fn level_enums(from_labels: Vec<&str>, to_labels: Vec<&str>) -> (Dump, Dump) {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());
    from_dump
        .types
        .push(make_enum_type("test_schema", "level", 620, from_labels));
    to_dump
        .types
        .push(make_enum_type("test_schema", "level", 620, to_labels));
    (from_dump, to_dump)
}

const ADD_MEDIUM: &str =
    "alter type test_schema.level add value if not exists 'medium' before 'high';";

/// A value added inside a transaction block cannot be used before it
/// commits, so a single-transaction migration adds it before `begin;`.
#[tokio::test]
async fn compare_adds_enum_values_before_begin_in_a_single_transaction() {
    let (from_dump, to_dump) = level_enums(vec!["low", "high"], vec!["low", "medium", "high"]);

    let script = compare_script(from_dump, to_dump, true).await;

    assert_eq!(script.matches(ADD_MEDIUM).count(), 1, "{script}");
    assert!(
        position(&script, ADD_MEDIUM) < position(&script, "begin;"),
        "{script}"
    );
    assert!(
        position(&script, "/* ---> Enum values: Start") < position(&script, ADD_MEDIUM),
        "{script}"
    );
    // Nothing else changed, so the in-transaction enum section is bare.
    let enums = position(&script, "/* ---> Enums: Start");
    let enums_end = position(&script, "/* ---> Enums: End");
    assert!(
        !script[enums..enums_end].contains("test_schema.level"),
        "{script}"
    );
}

/// Without a wrapping transaction each statement commits on its own, so
/// the enum section is exactly what it always was.
#[tokio::test]
async fn compare_keeps_enum_values_in_the_enum_section_without_a_transaction() {
    let (from_dump, to_dump) = level_enums(vec!["low", "high"], vec!["low", "medium", "high"]);

    let script = compare_script(from_dump, to_dump, false).await;

    assert!(!script.contains("Enum values: Start"), "{script}");
    assert!(
        script.contains(&format!(
            "/* ---> Enums: Start --------------- */\n\n/* Enum: test_schema.level */\n{ADD_MEDIUM}\n\n"
        )),
        "{script}"
    );
}

/// The labels are literals, so they are added after the
/// `standard_conforming_strings` guard that makes them read as written.
#[tokio::test]
async fn compare_adds_enum_values_after_the_standard_strings_guard() {
    let (mut from_dump, mut to_dump) =
        level_enums(vec!["low", "high"], vec!["low", "medium", "high"]);
    from_dump.standard_conforming_strings_off = true;
    to_dump.server_version_num = Some(180000);

    let script = compare_script(from_dump, to_dump, true).await;

    let guard = position(&script, "set standard_conforming_strings = on;");
    let add = position(&script, ADD_MEDIUM);
    assert!(guard < add && add < position(&script, "begin;"), "{script}");
}

/// A label only FROM has stays with the rest of the enum in the transaction;
/// only the ADD VALUE moves.
#[tokio::test]
async fn compare_splits_enum_notes_from_values_added_before_begin() {
    let (from_dump, to_dump) = level_enums(
        vec!["low", "retired", "high"],
        vec!["low", "medium", "high"],
    );

    let script = compare_script(from_dump, to_dump, true).await;

    let begin = position(&script, "begin;");
    assert!(position(&script, ADD_MEDIUM) < begin, "{script}");
    assert!(
        begin
            < position(
                &script,
                "-- Enum test_schema.level cannot automatically remove value 'retired'."
            ),
        "{script}"
    );
}

/// PostgreSQL cannot reorder enum values: say so, rather than "requires no
/// changes", when the labels both sides have are in a different order.
#[tokio::test]
async fn compare_notes_an_enum_reorder_instead_of_no_changes() {
    let (from_dump, to_dump) = level_enums(vec!["a", "b", "c"], vec!["c", "b", "a"]);

    let script = compare_script(from_dump, to_dump, false).await;

    assert!(!script.contains("requires no changes"), "{script}");
    assert!(
        script.contains(
            "-- Enum test_schema.level has its values in a different order ('a', 'b', 'c') than the target ('c', 'b', 'a'); PostgreSQL cannot reorder enum values (manual intervention needed).\n"
        ),
        "{script}"
    );
}

// ---- r3:types — composite attributes, typed tables, multirange schema ----

async fn compare_all(from_dump: Dump, to_dump: Dump) -> String {
    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    comparer.get_script()
}

fn at(script: &str, needle: &str) -> usize {
    script
        .find(needle)
        .unwrap_or_else(|| panic!("`{needle}` missing from:\n{script}"))
}

/// The fixture's `shared_schema.address_type` / `test_schema.user_profile`
/// cases: an attribute added at the end used to reach only the "Alter
/// script not implemented" comment, which `USE_COMMENTS=false` strips.
#[tokio::test]
async fn compare_composite_type_attribute_changes_are_migrated() {
    let mut from = make_composite_type(
        "shared_schema",
        "address_type",
        701,
        vec![("street", "character varying(255)"), ("zip", "integer")],
    );
    from.composite_stored_in = Some(vec!["test_schema.orders.shipping_address".to_string()]);
    from.typed_tables = Some(Vec::new());
    let to = make_composite_type(
        "shared_schema",
        "address_type",
        701,
        vec![
            ("street", "character varying(255)"),
            ("zip", "integer"),
            ("region", "character varying(100)"),
        ],
    );
    let mut from_dump = Dump::new(DumpConfig::default());
    from_dump.types.push(from);
    let mut to_dump = Dump::new(DumpConfig::default());
    to_dump.types.push(to);

    let script = compare_all(from_dump, to_dump).await;

    assert!(
        script.contains(
            "/* Type: shared_schema.address_type */\nalter type shared_schema.address_type add attribute \"region\" character varying(100);\n\n"
        ),
        "{script}"
    );
    assert!(!script.contains("not implemented"), "{script}");
}

#[tokio::test]
async fn composite_attribute_of_a_new_type_is_added_after_that_type_is_created() {
    // `app.addr` comes first in the TO dump, `app.zip_code` (new) after it.
    let from = make_composite_type("app", "addr", 710, vec![("street", "text")]);
    let to = make_composite_type(
        "app",
        "addr",
        710,
        vec![("street", "text"), ("zip", "app.zip_code")],
    );
    let new_domain = make_domain_type("app", "zip_code", 711);
    let mut from_dump = Dump::new(DumpConfig::default());
    from_dump.types.push(from);
    let mut to_dump = Dump::new(DumpConfig::default());
    to_dump.types.push(to);
    to_dump.types.push(new_domain);

    let script = compare_all(from_dump, to_dump).await;

    let create = at(&script, "create domain app.zip_code as text");
    let add = at(
        &script,
        "alter type app.addr add attribute \"zip\" app.zip_code;",
    );
    assert!(create < add, "{script}");
}

/// A typed table's columns come from its type: `ALTER TABLE` can neither
/// add, drop nor retype one, so the type's `… CASCADE` does it and the table
/// diff only sets what the table adds on top.
#[tokio::test]
async fn typed_table_columns_follow_their_type() {
    let from_type = make_composite_type("app", "tt", 720, vec![("x", "integer"), ("y", "integer")]);
    let to_type = make_composite_type("app", "tt", 720, vec![("x", "bigint"), ("z", "integer")]);

    let typed = |columns: Vec<TableColumn>| {
        let mut table = Table::new(
            "app".to_string(),
            "typed".to_string(),
            "app".to_string(),
            "typed".to_string(),
            "postgres".to_string(),
            None,
            columns,
            vec![],
            vec![],
            vec![],
            None,
        );
        table.typed_table_type = Some("app.tt".to_string());
        table.hash();
        table
    };
    let from_table = typed(vec![
        int_column("app", "typed", "x", 1),
        int_column("app", "typed", "y", 2),
    ]);
    let mut x = int_column("app", "typed", "x", 1);
    x.data_type = "bigint".to_string();
    x.numeric_precision = Some(64);
    let mut z = int_column("app", "typed", "z", 3);
    z.column_default = Some("5".to_string());
    z.comment = Some("from the type".to_string());
    let to_table = typed(vec![x, z]);

    let mut from_dump = Dump::new(DumpConfig::default());
    // A pgc 1.0.x FROM dump: the typed table is found among its tables.
    from_dump.types.push(from_type);
    from_dump.tables.push(from_table);
    let mut to_dump = Dump::new(DumpConfig::default());
    to_dump.types.push(to_type);
    to_dump.tables.push(to_table);

    let script = compare_all(from_dump, to_dump).await;

    let drop = at(
        &script,
        "alter type app.tt drop attribute if exists \"y\" cascade;",
    );
    let retype = at(
        &script,
        "alter type app.tt alter attribute \"x\" set data type bigint cascade;",
    );
    let add = at(
        &script,
        "alter type app.tt add attribute \"z\" integer cascade;",
    );
    let default = at(
        &script,
        "alter table app.typed alter column z set default 5;",
    );
    let comment = at(&script, "comment on column app.typed.z is 'from the type';");
    assert!(drop < retype && retype < add && add < default && default < comment);
    for refused in [
        "alter table app.typed add column",
        "alter table app.typed drop column",
        "alter table app.typed alter column x type",
    ] {
        assert!(!script.contains(refused), "{refused}: {script}");
    }
}

#[tokio::test]
async fn composite_usage_of_a_legacy_dump_finds_typed_tables_among_its_tables() {
    let from_type = make_composite_type("public", "tt", 730, vec![("x", "integer")]);
    let mut from_dump = Dump::new(DumpConfig::default());
    from_dump.types.push(from_type.clone());
    for (name, of) in [("a", "tt"), ("b", "public.tt"), ("c", "other.tt")] {
        let mut table = Table::new(
            "public".to_string(),
            name.to_string(),
            "public".to_string(),
            name.to_string(),
            "postgres".to_string(),
            None,
            vec![],
            vec![],
            vec![],
            vec![],
            None,
        );
        table.typed_table_type = Some(of.to_string());
        from_dump.tables.push(table);
    }
    let comparer = Comparer::new(
        from_dump,
        Dump::new(DumpConfig::default()),
        true,
        false,
        true,
        GrantsMode::Ignore,
    );

    let usage = comparer.composite_usage(&from_type);
    assert_eq!(usage.typed_tables, vec!["public.a", "public.b"]);
    assert_eq!(usage.stored_in, None);

    // A dump that recorded them is taken at its word.
    let mut recorded = from_type.clone();
    recorded.typed_tables = Some(vec!["elsewhere.t".to_string()]);
    recorded.composite_stored_in = Some(Vec::new());
    let usage = comparer.composite_usage(&recorded);
    assert_eq!(usage.typed_tables, vec!["elsewhere.t"]);
    assert_eq!(usage.stored_in, Some(Vec::new()));
}

fn range_type(multirange_schema: Option<&str>, subtype: &str) -> PgType {
    let mut range = make_domain_type("app", "span", 740);
    range.typtype = 'r' as i8;
    range.typcategory = 'R' as i8;
    range.typbasetype = None;
    range.formatted_basetype = None;
    range.range_subtype = Some(subtype.to_string());
    range.range_opclass = Some("pg_catalog.int4_ops".to_string());
    range.multirange_name = Some("span_multirange".to_string());
    range.multirange_schema = multirange_schema.map(str::to_string);
    range.hash();
    range
}

#[tokio::test]
async fn misplaced_multirange_is_moved_to_its_range_schema() {
    let mut from_dump = Dump::new(DumpConfig::default());
    from_dump.types.push(range_type(Some("public"), "integer"));
    let mut to_dump = Dump::new(DumpConfig::default());
    to_dump.types.push(range_type(Some("app"), "integer"));

    let script = compare_all(from_dump, to_dump).await;
    assert!(
        script.contains("alter type public.span_multirange set schema app;"),
        "{script}"
    );
    assert!(!script.contains("drop type"), "{script}");

    // A recreated range is created in place instead.
    let mut from_dump = Dump::new(DumpConfig::default());
    from_dump.types.push(range_type(Some("public"), "integer"));
    let mut to_dump = Dump::new(DumpConfig::default());
    to_dump.types.push(range_type(Some("app"), "bigint"));
    let script = compare_all(from_dump, to_dump).await;
    assert!(!script.contains("set schema"), "{script}");
    assert!(
        script.contains("multirange_type_name = app.span_multirange\n"),
        "{script}"
    );

    // Unknown placement (a pgc 1.0.x FROM dump): nothing to move.
    let mut from_dump = Dump::new(DumpConfig::default());
    from_dump.types.push(range_type(None, "integer"));
    let mut to_dump = Dump::new(DumpConfig::default());
    to_dump.types.push(range_type(Some("app"), "integer"));
    let script = compare_all(from_dump, to_dump).await;
    assert!(!script.contains("set schema"), "{script}");
    assert!(!script.contains("/* Type: app.span"), "{script}");
}

#[tokio::test]
async fn new_range_type_names_its_multirange_with_a_schema() {
    let mut to_dump = Dump::new(DumpConfig::default());
    to_dump.types.push(range_type(Some("app"), "integer"));
    let script = compare_all(Dump::new(DumpConfig::default()), to_dump).await;
    assert!(
        script.contains("multirange_type_name = app.span_multirange\n"),
        "{script}"
    );
}

/// `app.typed OF <of>` with `columns`.
fn typed_table(of: &str, columns: Vec<TableColumn>) -> Table {
    let mut table = Table::new(
        "app".to_string(),
        "typed".to_string(),
        "app".to_string(),
        "typed".to_string(),
        "postgres".to_string(),
        None,
        columns,
        vec![],
        vec![],
        vec![],
        None,
    );
    table.typed_table_type = Some(of.to_string());
    table.hash();
    table
}

fn bigint_column(schema: &str, table: &str, name: &str, ordinal: i32) -> TableColumn {
    let mut column = int_column(schema, table, name, ordinal);
    column.data_type = "bigint".to_string();
    column.numeric_precision = Some(64);
    column
}

/// Review finding: the typed table's retype (`ALTER ATTRIBUTE … TYPE …
/// CASCADE`) ran in the types section, before "Views DROP", and PostgreSQL
/// refused it while the view read the column ("cannot alter type of a column
/// used by a view or rule"); the view was then re-created on the old type,
/// every run. It now runs after the view is dropped, as an ordinary table's
/// `ALTER COLUMN … TYPE` does.
#[tokio::test]
async fn typed_table_retype_runs_after_the_views_reading_it_are_dropped() {
    let from_type =
        make_composite_type("app", "tt", 750, vec![("id", "integer"), ("gone", "text")]);
    let to_type = make_composite_type("app", "tt", 750, vec![("id", "bigint")]);
    let mut gone = int_column("app", "typed", "gone", 2);
    gone.data_type = "text".to_string();
    gone.numeric_precision = None;
    let from_table = typed_table("app.tt", vec![int_column("app", "typed", "id", 1), gone]);
    let to_table = typed_table("app.tt", vec![bigint_column("app", "typed", "id", 1)]);
    let view = |definition: &str, columns: &[&str]| {
        let mut view = View::new(
            "v".to_string(),
            definition.to_string(),
            "app".to_string(),
            vec!["app.typed".to_string()],
        );
        view.column_relation = Some(columns.iter().map(|c| c.to_string()).collect());
        view.hash();
        view
    };

    let mut from_dump = Dump::new(DumpConfig::default());
    from_dump.types.push(from_type);
    from_dump.tables.push(from_table);
    from_dump.views.push(view(
        " SELECT typed.id, typed.gone\n   FROM app.typed;",
        &["app.typed.id", "app.typed.gone"],
    ));
    let mut to_dump = Dump::new(DumpConfig::default());
    to_dump.types.push(to_type);
    to_dump.tables.push(to_table);
    to_dump.views.push(view(
        " SELECT typed.id\n   FROM app.typed;",
        &["app.typed.id"],
    ));

    let script = compare_all(from_dump, to_dump).await;

    let drop_view = at(&script, "drop view if exists app.v;");
    let drop_attribute = at(
        &script,
        "alter type app.tt drop attribute if exists \"gone\" cascade;",
    );
    let retype = at(
        &script,
        "alter type app.tt alter attribute \"id\" set data type bigint cascade;",
    );
    let create_view = at(&script, "CREATE OR REPLACE VIEW app.v");
    assert!(
        drop_view < drop_attribute && drop_attribute < retype && retype < create_view,
        "{script}"
    );
    // After the types section, in a section of its own.
    let types_end = at(&script, "User-defined types: End");
    let section = at(
        &script,
        "Composite types: attribute changes (after the view drops)",
    );
    assert!(types_end < section && section < drop_attribute, "{script}");
}

/// Composite alterations wait for the view drops even without a typed
/// table, and appear once.
#[tokio::test]
async fn composite_alterations_follow_the_view_drops_once() {
    let from = make_composite_type("app", "addr", 760, vec![("street", "text")]);
    let to = make_composite_type(
        "app",
        "addr",
        760,
        vec![("street", "text"), ("zip", "text")],
    );
    let mut from_dump = Dump::new(DumpConfig::default());
    from_dump.types.push(from);
    let mut to_dump = Dump::new(DumpConfig::default());
    to_dump.types.push(to);

    let script = compare_all(from_dump, to_dump).await;
    let add = "alter type app.addr add attribute \"zip\" text;";
    assert_eq!(script.matches(add).count(), 1, "{script}");
    assert!(
        at(&script, "Views DROP: End") < at(&script, add),
        "{script}"
    );
    assert!(at(&script, add) < at(&script, "Tables: Start"), "{script}");
}

/// Review finding: a table typed by a type outside the dumped schemas had
/// its column changes left to an `ALTER TYPE` that never came, with no
/// statement, note or warning.
#[tokio::test]
async fn typed_table_of_a_type_outside_the_dump_gets_a_note() {
    let from_table = typed_table("ext.tt", vec![int_column("app", "typed", "id", 1)]);
    let to_table = typed_table(
        "ext.tt",
        vec![
            int_column("app", "typed", "id", 1),
            int_column("app", "typed", "extra", 2),
        ],
    );
    let mut from_dump = Dump::new(DumpConfig::default());
    from_dump.tables.push(from_table.clone());
    let mut to_dump = Dump::new(DumpConfig::default());
    to_dump.tables.push(to_table.clone());

    let script = compare_all(from_dump, to_dump).await;
    assert!(
        script.contains(
            "/* table app.typed is typed by ext.tt, which is not among the compared types, so its column changes (added extra) are not migrated: ALTER TABLE cannot make them on a typed table, and they need ALTER TYPE ext.tt ... CASCADE (manual intervention needed). */\n"
        ),
        "{script}"
    );
    assert!(!script.contains("add column"), "{script}");

    // A compared type carries the change, and there is no note.
    let from_type = make_composite_type("ext", "tt", 770, vec![("id", "integer")]);
    let to_type = make_composite_type(
        "ext",
        "tt",
        770,
        vec![("id", "integer"), ("extra", "integer")],
    );
    let mut from_dump = Dump::new(DumpConfig::default());
    from_dump.types.push(from_type);
    from_dump.tables.push(from_table);
    let mut to_dump = Dump::new(DumpConfig::default());
    to_dump.types.push(to_type);
    to_dump.tables.push(to_table);
    let script = compare_all(from_dump, to_dump).await;
    assert!(!script.contains("not among the compared types"), "{script}");
    at(
        &script,
        "alter type ext.tt add attribute \"extra\" integer cascade;",
    );
}

// ---- integration: what is in the way of a typed table's column retype ----

/// `app.typed OF app.tt (x <x_type>)` under the policy `typed_pol` on `x`
/// and a `BEFORE UPDATE OF x` trigger, with the FROM graph's edges.
fn typed_table_with_blockers(x_type: &str, with_edges: bool) -> Dump {
    use crate::dump::column_dependent::{ColumnDependent, ColumnDependentKind};
    use crate::dump::table_policy::TablePolicy;
    use crate::dump::table_trigger::TableTrigger;

    let mut dump = Dump::new(DumpConfig::default());
    dump.server_version_num = Some(190000);
    let mut composite = make_composite_type("app", "tt", 730, vec![("x", x_type)]);
    composite.typed_tables = Some(vec!["app.typed".to_string()]);
    composite.composite_stored_in = Some(Vec::new());
    dump.types.push(composite);
    let mut x = int_column("app", "typed", "x", 1);
    x.data_type = x_type.to_string();
    let mut table = Table::new(
        "app".to_string(),
        "typed".to_string(),
        "app".to_string(),
        "typed".to_string(),
        "postgres".to_string(),
        None,
        vec![x],
        vec![],
        vec![],
        vec![],
        None,
    );
    table.typed_table_type = Some("app.tt".to_string());
    table.has_rowsecurity = true;
    table.policies.push(TablePolicy {
        schema: "app".to_string(),
        table: "typed".to_string(),
        name: "typed_pol".to_string(),
        command: "all".to_string(),
        permissive: true,
        roles: vec![],
        using_clause: Some("(x > 0)".to_string()),
        check_clause: None,
    });
    table.triggers.push(TableTrigger {
        oid: Oid(9),
        name: "typed_upd".to_string(),
        definition: "CREATE TRIGGER typed_upd BEFORE UPDATE OF x ON app.typed FOR EACH ROW EXECUTE FUNCTION app.touch()".to_string(),
        enabled: "O".to_string(),
        comment: None,
    });
    table.hash();
    dump.tables.push(table);
    if with_edges {
        let edge = |kind, name: &str| ColumnDependent {
            schema: "app".to_string(),
            table: "typed".to_string(),
            column: "x".to_string(),
            kind,
            dep_schema: "app".to_string(),
            dep_table: "typed".to_string(),
            dep_name: name.to_string(),
        };
        dump.column_dependents = vec![
            edge(ColumnDependentKind::Policy, "typed_pol"),
            edge(ColumnDependentKind::Trigger, "typed_upd"),
        ];
    }
    dump
}

/// A composite type's `ALTER ATTRIBUTE … TYPE … CASCADE` retypes its typed
/// tables' column, which PostgreSQL refuses while a policy or a trigger
/// depends on it, as it refuses `ALTER COLUMN … TYPE` (checked on 17). It
/// runs before the table phase, whose retype blockers came too late for it:
/// they are dropped ahead of the composite section now, and put back after
/// the table alters like any other retype blocker.
#[tokio::test]
async fn policies_and_triggers_on_a_typed_tables_retyped_column_are_dropped_before_its_type() {
    let script = compare_all(
        typed_table_with_blockers("integer", true),
        typed_table_with_blockers("bigint", false),
    )
    .await;

    let section = at(
        &script,
        "Dependents dropped ahead of a typed table's column retype: Start",
    );
    let drop_policy = at(&script, "drop policy if exists typed_pol on app.typed;");
    let drop_trigger = at(&script, "drop trigger if exists typed_upd on app.typed;");
    let retype = at(
        &script,
        "alter type app.tt alter attribute \"x\" set data type bigint cascade;",
    );
    assert!(
        section < drop_policy && drop_policy < retype && drop_trigger < retype,
        "{script}"
    );
    let create_policy = at(
        &script,
        "create policy typed_pol on app.typed for all to public using (x > 0);",
    );
    let create_trigger = at(&script, "CREATE TRIGGER typed_upd BEFORE UPDATE OF x");
    assert!(
        retype < create_policy && retype < create_trigger,
        "{script}"
    );
    assert_eq!(
        script.matches("create policy typed_pol").count(),
        1,
        "{script}"
    );
    assert_eq!(
        script.matches("CREATE TRIGGER typed_upd").count(),
        1,
        "{script}"
    );

    // Nothing ahead of the composite section without use_drop, or without
    // the FROM graph (a pgc 1.0.x dump).
    let mut comparer = Comparer::new(
        typed_table_with_blockers("integer", true),
        typed_table_with_blockers("bigint", false),
        false,
        false,
        true,
        GrantsMode::Ignore,
    );
    comparer.compare().await.unwrap();
    assert!(
        !comparer
            .get_script()
            .contains("ahead of a typed table's column retype"),
        "{}",
        comparer.get_script()
    );
    let script = compare_all(
        typed_table_with_blockers("integer", false),
        typed_table_with_blockers("bigint", false),
    )
    .await;
    assert!(
        !script.contains("ahead of a typed table's column retype"),
        "{script}"
    );
}

// ---- r4:typed — what a composite type's retype reaches ----

use crate::dump::column_dependent::{ColumnDependent, ColumnDependentKind};
use crate::dump::table_policy::TablePolicy;
use crate::dump::table_trigger::TableTrigger;
use std::collections::HashSet;

/// `app.ct (id integer, amount <amount_type>)`, with its typed tables.
fn r4t_type(amount_type: &str, typed_tables: &[&str]) -> PgType {
    let mut composite = make_composite_type(
        "app",
        "ct",
        780,
        vec![("id", "integer"), ("amount", amount_type)],
    );
    composite.typed_tables = Some(typed_tables.iter().map(|t| t.to_string()).collect());
    composite.composite_stored_in = Some(Vec::new());
    composite
}

fn r4t_column(table: &str, name: &str, ordinal: i32, data_type: &str) -> TableColumn {
    let mut column = int_column("app", table, name, ordinal);
    column.data_type = data_type.to_string();
    column
}

/// `app.<name> (id, amount <amount_type>)`, typed by `app.ct` when `typed`.
fn r4t_table(name: &str, amount_type: &str, typed: bool) -> Table {
    let mut table = Table::new(
        "app".to_string(),
        name.to_string(),
        "app".to_string(),
        name.to_string(),
        "postgres".to_string(),
        None,
        vec![
            r4t_column(name, "id", 1, "integer"),
            r4t_column(name, "amount", 2, amount_type),
        ],
        vec![],
        vec![],
        vec![],
        None,
    );
    if typed {
        table.typed_table_type = Some("app.ct".to_string());
    }
    table.hash();
    table
}

fn r4t_policy(table: &mut Table, name: &str, using: &str) {
    table.has_rowsecurity = true;
    table.policies.push(TablePolicy {
        schema: "app".to_string(),
        table: table.name.clone(),
        name: name.to_string(),
        command: "all".to_string(),
        permissive: true,
        roles: vec![],
        using_clause: Some(using.to_string()),
        check_clause: None,
    });
    table.hash();
}

fn r4t_trigger(table: &mut Table, name: &str, column: &str) {
    table.triggers.push(TableTrigger {
        oid: Oid(11),
        name: name.to_string(),
        definition: format!(
            "CREATE TRIGGER {name} BEFORE UPDATE OF {column} ON app.{} FOR EACH ROW EXECUTE FUNCTION app.touch()",
            table.name
        ),
        enabled: "O".to_string(),
        comment: None,
    });
    table.hash();
}

fn r4t_edge(
    table: &str,
    column: &str,
    kind: ColumnDependentKind,
    dep_table: &str,
    dep: &str,
) -> ColumnDependent {
    ColumnDependent {
        schema: "app".to_string(),
        table: table.to_string(),
        column: column.to_string(),
        kind,
        dep_schema: "app".to_string(),
        dep_table: dep_table.to_string(),
        dep_name: dep.to_string(),
    }
}

fn r4t_dump() -> Dump {
    let mut dump = Dump::new(DumpConfig::default());
    dump.server_version_num = Some(170000);
    dump
}

const R4T_RETYPE: &str =
    "alter type app.ct alter attribute \"amount\" set data type bigint cascade;";

/// `app.tp OF app.ct PARTITION BY RANGE (id)` and its partition `app.tp1`,
/// which has a policy and an `UPDATE OF amount` trigger on `amount`.
fn r4t_partition_dump(amount_type: &str, with_edges: bool) -> Dump {
    let mut dump = r4t_dump();
    dump.types.push(r4t_type(amount_type, &["app.tp"]));
    let mut tp = r4t_table("tp", amount_type, true);
    tp.partition_key = Some("RANGE (id)".to_string());
    tp.hash();
    let mut tp1 = r4t_table("tp1", amount_type, false);
    tp1.partition_of = Some("app.tp".to_string());
    tp1.partition_bound = Some("FOR VALUES FROM (0) TO (100)".to_string());
    r4t_policy(&mut tp1, "tp1_pol", "(amount > 0)");
    r4t_trigger(&mut tp1, "tp1_trg", "amount");
    dump.tables.extend([tp, tp1]);
    if with_edges {
        dump.column_dependents = vec![
            r4t_edge(
                "tp1",
                "amount",
                ColumnDependentKind::Policy,
                "tp1",
                "tp1_pol",
            ),
            r4t_edge(
                "tp1",
                "amount",
                ColumnDependentKind::Trigger,
                "tp1",
                "tp1_trg",
            ),
        ];
    }
    dump
}

/// Review finding: `ALTER ATTRIBUTE … TYPE … CASCADE` retypes the partitions
/// of a typed partitioned table too, and PostgreSQL refused it while a
/// partition's policy or trigger depended on the column: only the typed
/// table's own were dropped ahead of it.
#[tokio::test]
async fn r4t_blockers_on_a_typed_tables_partition_are_dropped_before_its_type() {
    let script = compare_all(
        r4t_partition_dump("integer", true),
        r4t_partition_dump("bigint", false),
    )
    .await;

    let retype = at(&script, R4T_RETYPE);
    let drop_policy = at(&script, "drop policy if exists tp1_pol on app.tp1;");
    let drop_trigger = at(&script, "drop trigger if exists tp1_trg on app.tp1;");
    assert!(drop_policy < retype && drop_trigger < retype, "{script}");
    let create_policy = at(
        &script,
        "create policy tp1_pol on app.tp1 for all to public using (amount > 0);",
    );
    let create_trigger = at(&script, "CREATE TRIGGER tp1_trg BEFORE UPDATE OF amount");
    assert!(
        retype < create_policy && retype < create_trigger,
        "{script}"
    );
    // Put back once; the table phase does not drop them a second time.
    assert_eq!(
        script.matches("create policy tp1_pol").count(),
        1,
        "{script}"
    );
    assert_eq!(
        script.matches("CREATE TRIGGER tp1_trg").count(),
        1,
        "{script}"
    );
    assert_eq!(
        script
            .matches("drop policy if exists tp1_pol on app.tp1;")
            .count(),
        2,
        "the drop ahead and the restore block only:\n{script}"
    );
    assert_eq!(
        script
            .matches("drop trigger if exists tp1_trg on app.tp1;")
            .count(),
        2,
        "the drop ahead and the restore block only:\n{script}"
    );
}

/// `app.tt OF app.ct` and its `INHERITS` child `app.ti (id, amount, extra, g
/// <amount_type> GENERATED ALWAYS AS (amount * 2) STORED)`, with a policy on
/// `amount` and one on `g`; `app.other`'s policy reads `app.tt.amount`.
fn r4t_inheritance_dump(amount_type: &str, with_edges: bool) -> Dump {
    let mut dump = r4t_dump();
    dump.types.push(r4t_type(amount_type, &["app.tt"]));
    let tt = r4t_table("tt", amount_type, true);
    let mut ti = r4t_table("ti", amount_type, false);
    ti.inherits_from = vec!["app.tt".to_string()];
    ti.columns.push(r4t_column("ti", "extra", 3, "integer"));
    let mut g = r4t_column("ti", "g", 4, amount_type);
    g.is_generated = "ALWAYS".to_string();
    g.generation_expression = Some("(amount * 2)".to_string());
    g.generation_type = Some("s".to_string());
    ti.columns.push(g);
    r4t_policy(&mut ti, "ti_pol", "(amount > 0)");
    r4t_policy(&mut ti, "ti_gpol", "(g > 0)");
    let mut other = r4t_table("other", "bigint", false);
    r4t_policy(
        &mut other,
        "other_pol",
        "(amount IN ( SELECT tt.amount FROM app.tt))",
    );
    dump.tables.extend([tt, ti, other]);
    if with_edges {
        dump.column_dependents = vec![
            r4t_edge("ti", "amount", ColumnDependentKind::Policy, "ti", "ti_pol"),
            r4t_edge("ti", "g", ColumnDependentKind::Policy, "ti", "ti_gpol"),
            r4t_edge(
                "tt",
                "amount",
                ColumnDependentKind::Policy,
                "other",
                "other_pol",
            ),
        ];
    }
    dump
}

/// Review findings: the retype reaches a typed table's inheritance child,
/// whose policy blocked it, and a generated column there reading the column
/// ("cannot alter type of a column used by a generated column"), which the
/// table phase only dropped after it. Both go ahead of it now, the generated
/// column's own policy first; the child adds the column back in its alter.
#[tokio::test]
async fn r4t_an_inheritance_childs_blockers_and_generated_column_go_before_the_type() {
    let script = compare_all(
        r4t_inheritance_dump("integer", true),
        r4t_inheritance_dump("bigint", false),
    )
    .await;

    let retype = at(&script, R4T_RETYPE);
    let drop_gpol = at(&script, "drop policy if exists ti_gpol on app.ti;");
    let drop_g = at(&script, "alter table app.ti drop column g;");
    let drop_pol = at(&script, "drop policy if exists ti_pol on app.ti;");
    let drop_other = at(&script, "drop policy if exists other_pol on app.other;");
    assert!(
        drop_gpol < drop_g && drop_g < retype && drop_pol < retype && drop_other < retype,
        "{script}"
    );
    let add_g = at(
        &script,
        "alter table app.ti add column g bigint generated always as (amount * 2) stored;",
    );
    assert!(retype < add_g, "{script}");
    assert_eq!(
        script.matches("alter table app.ti drop column g;").count(),
        1,
        "{script}"
    );
    for policy in [
        "create policy ti_pol",
        "create policy ti_gpol",
        "create policy other_pol",
    ] {
        assert!(add_g < at(&script, policy), "{policy}:\n{script}");
        assert_eq!(script.matches(policy).count(), 1, "{policy}:\n{script}");
    }
    // The child's inherited column is its parent's to retype, which is the
    // type's here: no table statement retypes it.
    assert!(!script.contains("alter column amount type"), "{script}");
}

#[tokio::test]
async fn r4t_the_generated_column_moves_ahead_without_the_graph_too() {
    // A pgc 1.0.x FROM dump has no column-dependent graph, but the
    // generated column is in the way all the same.
    let script = compare_all(
        r4t_inheritance_dump("integer", false),
        r4t_inheritance_dump("bigint", false),
    )
    .await;
    let drop_g = at(&script, "alter table app.ti drop column g;");
    assert!(drop_g < at(&script, R4T_RETYPE), "{script}");
    assert_eq!(
        script.matches("alter table app.ti drop column g;").count(),
        1,
        "{script}"
    );
}

/// Review finding: a typed table TO drops is still there when the composite
/// section retypes its column, and its policy blocked it; its drop only
/// comes in the table phase.
#[tokio::test]
async fn r4t_blockers_on_a_typed_table_to_drops_go_before_the_type_and_stay_gone() {
    let mut from = r4t_dump();
    from.types.push(r4t_type("integer", &["app.tt"]));
    let mut tt = r4t_table("tt", "integer", true);
    r4t_policy(&mut tt, "tt_pol", "(amount > 0)");
    r4t_trigger(&mut tt, "tt_trg", "amount");
    let mut ti = r4t_table("ti", "integer", false);
    ti.inherits_from = vec!["app.tt".to_string()];
    let mut g = r4t_column("ti", "g", 3, "integer");
    g.is_generated = "ALWAYS".to_string();
    g.generation_expression = Some("(amount * 2)".to_string());
    g.generation_type = Some("s".to_string());
    ti.columns.push(g);
    ti.hash();
    from.tables.extend([tt, ti]);
    from.column_dependents = vec![
        r4t_edge("tt", "amount", ColumnDependentKind::Policy, "tt", "tt_pol"),
        r4t_edge("tt", "amount", ColumnDependentKind::Trigger, "tt", "tt_trg"),
    ];
    let mut to = r4t_dump();
    to.types.push(r4t_type("bigint", &[]));

    let script = compare_all(from, to).await;

    let retype = at(&script, R4T_RETYPE);
    assert!(
        at(&script, "drop policy if exists tt_pol on app.tt;") < retype,
        "{script}"
    );
    assert!(
        at(&script, "drop trigger if exists tt_trg on app.tt;") < retype,
        "{script}"
    );
    // Its child, dropped too, loses the generated column in the way first.
    assert!(
        at(&script, "alter table app.ti drop column g;") < retype,
        "{script}"
    );
    let drop_table = at(&script, "drop table if exists app.tt;");
    assert!(retype < drop_table, "{script}");
    assert!(!script.contains("create policy tt_pol"), "{script}");
    assert!(!script.contains("CREATE TRIGGER tt_trg"), "{script}");
    assert!(
        !script[drop_table..].contains("drop policy if exists tt_pol"),
        "{script}"
    );
}

/// `app.tp OF app.ct` partitioned by `key` with its own policy, and
/// `app.reader`, whose policy reads `app.tp.amount`.
fn r4t_recreate_dump(amount_type: &str, key: &str, with_edges: bool) -> Dump {
    let mut dump = r4t_dump();
    dump.types.push(r4t_type(amount_type, &["app.tp"]));
    let mut tp = r4t_table("tp", amount_type, true);
    tp.partition_key = Some(key.to_string());
    tp.columns[0].is_nullable = false;
    r4t_policy(&mut tp, "tp_pol", "(amount > 0)");
    let mut reader = r4t_table("reader", "bigint", false);
    r4t_policy(
        &mut reader,
        "reader_pol",
        "(amount IN ( SELECT tp.amount FROM app.tp))",
    );
    dump.tables.extend([tp, reader]);
    if with_edges {
        dump.column_dependents = vec![
            r4t_edge("tp", "amount", ColumnDependentKind::Policy, "tp", "tp_pol"),
            r4t_edge(
                "tp",
                "amount",
                ColumnDependentKind::Policy,
                "reader",
                "reader_pol",
            ),
        ];
    }
    dump
}

/// Review findings: another table's policy reading a typed table the table
/// phase recreates was dropped ahead of the type's retype and never created
/// again; and the recreated typed table came back with `CREATE TABLE … OF
/// <type> (<column> <type>, …)`, a syntax error.
#[tokio::test]
async fn r4t_a_recreated_typed_table_and_the_policy_reading_it_come_back() {
    let script = compare_all(
        r4t_recreate_dump("integer", "LIST (id)", true),
        r4t_recreate_dump("bigint", "RANGE (id)", false),
    )
    .await;

    let retype = at(&script, R4T_RETYPE);
    assert!(
        at(&script, "drop policy if exists reader_pol on app.reader;") < retype,
        "{script}"
    );
    let create_table = at(
        &script,
        "create table app.tp of app.ct (\n    id with options not null\n)\npartition by RANGE (id);",
    );
    let create_reader = at(&script, "create policy reader_pol on app.reader");
    assert!(
        retype < create_table && create_table < create_reader,
        "{script}"
    );
    assert_eq!(
        script.matches("create policy reader_pol").count(),
        1,
        "{script}"
    );
    // Its own policy comes back with it, once.
    let create_own = at(&script, "create policy tp_pol on app.tp");
    assert!(create_table < create_own, "{script}");
    assert_eq!(
        script.matches("create policy tp_pol").count(),
        1,
        "{script}"
    );
}

/// `app.ct (id integer, note text [collate <collation>])` and its typed
/// table `app.tt`, with a policy and an `UPDATE OF note` trigger on `note`;
/// `table_collation` is what the dump records on the table's column.
fn r4t_collation_dump(
    type_collation: Option<&str>,
    table_collation: Option<&str>,
    with_edges: bool,
) -> Dump {
    let mut dump = r4t_dump();
    let mut composite =
        make_composite_type("app", "ct", 790, vec![("id", "integer"), ("note", "text")]);
    composite.composite_attributes[1].collation = type_collation.map(str::to_string);
    composite.typed_tables = Some(vec!["app.tt".to_string()]);
    composite.composite_stored_in = Some(Vec::new());
    composite.hash();
    dump.types.push(composite);
    let mut tt = r4t_table("tt", "integer", true);
    tt.columns[1] = r4t_column("tt", "note", 2, "text");
    tt.columns[1].collation_name = table_collation.map(str::to_string);
    tt.hash();
    r4t_policy(&mut tt, "tt_pol", "(note <> ''::text)");
    r4t_trigger(&mut tt, "tt_trg", "note");
    dump.tables.push(tt);
    if with_edges {
        dump.column_dependents = vec![
            r4t_edge("tt", "note", ColumnDependentKind::Policy, "tt", "tt_pol"),
            r4t_edge("tt", "note", ColumnDependentKind::Trigger, "tt", "tt_trg"),
        ];
    }
    dump
}

/// Review finding: a composite attribute's collation was not dumped, so a
/// change of it was never migrated, while the typed table's column counted
/// as retyped: its policy and trigger were dropped and created again on
/// every run. The type migrates it now, with the typed table's blockers out
/// of the way.
#[tokio::test]
async fn r4t_a_composite_attribute_collation_change_is_migrated_by_the_type() {
    let script = compare_all(
        r4t_collation_dump(None, None, true),
        r4t_collation_dump(Some("pg_catalog.\"C\""), Some("C"), false),
    )
    .await;

    let retype = at(
        &script,
        "alter type app.ct alter attribute \"note\" set data type text collate pg_catalog.\"C\" cascade;",
    );
    assert!(
        at(&script, "drop policy if exists tt_pol on app.tt;") < retype,
        "{script}"
    );
    assert!(
        at(&script, "drop trigger if exists tt_trg on app.tt;") < retype,
        "{script}"
    );
    assert!(retype < at(&script, "create policy tt_pol"), "{script}");
    assert!(retype < at(&script, "CREATE TRIGGER tt_trg"), "{script}");
    assert_eq!(
        script.matches("create policy tt_pol").count(),
        1,
        "{script}"
    );
    assert!(
        !script.contains("alter table app.tt alter column note"),
        "{script}"
    );
}

#[tokio::test]
async fn r4t_a_typed_column_change_its_type_does_not_make_is_not_a_retype() {
    // The typed table's column differs, but the type does not: nothing
    // retypes the column, so nothing in its way is dropped and re-created
    // (it was, on every run).
    let script = compare_all(
        r4t_collation_dump(None, None, true),
        r4t_collation_dump(None, Some("C"), false),
    )
    .await;
    for churn in [
        "drop policy",
        "create policy",
        "drop trigger",
        "CREATE TRIGGER",
        "alter type",
    ] {
        assert!(!script.contains(churn), "{churn}:\n{script}");
    }
}

#[tokio::test]
async fn r4t_a_legacy_from_dump_takes_the_to_attribute_collations() {
    // A pgc 1.0.x FROM dump did not record attribute collations: the
    // migration leaves them as they are rather than retype on a guess.
    let mut from = r4t_collation_dump(None, Some("C"), true);
    from.server_version_num = None;
    let to = r4t_collation_dump(Some("pg_catalog.\"C\""), Some("C"), false);
    let mut comparer = Comparer::new(from, to, true, false, true, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();
    assert!(!script.contains("alter type app.ct"), "{script}");
    assert!(!script.contains("drop policy"), "{script}");

    // A pgc 1.1 FROM dump says what it has, and the change is migrated.
    let script = compare_all(
        r4t_collation_dump(None, Some("C"), true),
        r4t_collation_dump(Some("pg_catalog.\"C\""), Some("C"), false),
    )
    .await;
    at(
        &script,
        "alter type app.ct alter attribute \"note\" set data type text collate pg_catalog.\"C\" cascade;",
    );
}

#[test]
fn r4t_typed_table_retypes_reach_partitions_descendants_and_dropped_tables() {
    let mut from = r4t_inheritance_dump("integer", false);
    // A grandchild, and a partitioned typed table with its partition.
    let grandchild = |amount_type: &str| {
        let mut grandchild = r4t_table("tg", amount_type, false);
        grandchild.inherits_from = vec!["app.ti".to_string()];
        grandchild.hash();
        grandchild
    };
    from.tables.push(grandchild("integer"));
    let mut tp = r4t_table("tp", "integer", true);
    tp.partition_key = Some("RANGE (id)".to_string());
    let mut tp1 = r4t_table("tp1", "integer", false);
    tp1.partition_of = Some("app.tp".to_string());
    from.tables.extend([tp, tp1]);
    // TO drops app.tp and its partition.
    let mut to = r4t_inheritance_dump("bigint", false);
    to.tables.push(grandchild("bigint"));
    let comparer = Comparer::new(from, to, true, false, true, GrantsMode::Ignore);

    let key =
        |table: &str, column: &str| ("app".to_string(), table.to_string(), column.to_string());
    let retyped = comparer.typed_table_retypes();
    let expected: HashSet<(String, String, String)> = ["tt", "ti", "tg", "tp", "tp1"]
        .into_iter()
        .map(|table| key(table, "amount"))
        .collect();
    assert_eq!(retyped, expected);

    // The table phase leaves every column a typed table has from its type,
    // and their copies, to that type; not app.ti's own extra and g, nor
    // app.other's columns. app.tp is not typed in TO (it is gone).
    let family = comparer.typed_family_columns();
    for (table, column) in [
        ("tt", "id"),
        ("tt", "amount"),
        ("ti", "amount"),
        ("tg", "id"),
    ] {
        assert!(
            family.contains(&key(table, column)),
            "{table}.{column}: {family:?}"
        );
    }
    for (table, column) in [
        ("ti", "extra"),
        ("ti", "g"),
        ("other", "amount"),
        ("tp", "amount"),
    ] {
        assert!(
            !family.contains(&key(table, column)),
            "{table}.{column}: {family:?}"
        );
    }
}

// ---- r4:typed review fixes ----

/// `app.qt (id integer, order <order_type>, Amt <amt_type>)`: attribute names
/// as the catalog spells them, which `quote_ident` quotes on a table column.
fn r4t_quoted_type(order_type: &str, amt_type: &str) -> PgType {
    let mut composite = make_composite_type(
        "app",
        "qt",
        781,
        vec![("id", "integer"), ("order", order_type), ("Amt", amt_type)],
    );
    composite.typed_tables = Some(vec!["app.qtt".to_string()]);
    composite.composite_stored_in = Some(Vec::new());
    composite.hash();
    composite
}

/// `app.qtt OF app.qt` with a policy on `"order"`, and its `INHERITS` child
/// `app.qti` with a generated column reading `"Amt"`.
fn r4t_quoted_dump(amount_type: &str, with_edges: bool) -> Dump {
    let mut dump = r4t_dump();
    dump.types.push(r4t_quoted_type(amount_type, amount_type));
    let mut qtt = r4t_table("qtt", amount_type, false);
    qtt.columns = vec![
        r4t_column("qtt", "id", 1, "integer"),
        r4t_column("qtt", "\"order\"", 2, amount_type),
        r4t_column("qtt", "\"Amt\"", 3, amount_type),
    ];
    qtt.typed_table_type = Some("app.qt".to_string());
    r4t_policy(&mut qtt, "qtt_pol", "(\"order\" > 0)");
    let mut qti = r4t_table("qti", amount_type, false);
    qti.columns = vec![
        r4t_column("qti", "id", 1, "integer"),
        r4t_column("qti", "\"order\"", 2, amount_type),
        r4t_column("qti", "\"Amt\"", 3, amount_type),
    ];
    let mut g = r4t_column("qti", "g", 4, amount_type);
    g.is_generated = "ALWAYS".to_string();
    g.generation_expression = Some("(\"Amt\" * 2)".to_string());
    g.generation_type = Some("s".to_string());
    qti.columns.push(g);
    qti.inherits_from = vec!["app.qtt".to_string()];
    qti.hash();
    dump.tables.extend([qtt, qti]);
    if with_edges {
        dump.column_dependents = vec![r4t_edge(
            "qtt",
            "\"order\"",
            ColumnDependentKind::Policy,
            "qtt",
            "qtt_pol",
        )];
    }
    dump
}

/// Review finding: a composite attribute is named as the catalog spells it
/// (`order`, `Amt`), a table column in `quote_ident` form (`"order"`,
/// `"Amt"`), so an attribute that needs quoting reached no typed table: the
/// policy on it was not dropped ahead of the type's retype, nor the
/// generated column reading it in an inheritance child ("cannot alter type
/// of a column used in a policy definition" / "... by a generated column").
#[tokio::test]
async fn r4t_attributes_that_need_quoting_reach_their_typed_columns() {
    let from = r4t_quoted_dump("integer", true);
    let to = r4t_quoted_dump("bigint", false);
    let comparer = Comparer::new(
        from.clone(),
        to.clone(),
        true,
        false,
        true,
        GrantsMode::Ignore,
    );
    let retyped = comparer.typed_table_retypes();
    for (table, column) in [
        ("qtt", "order"),
        ("qtt", "amt"),
        ("qti", "order"),
        ("qti", "amt"),
    ] {
        assert!(
            retyped.contains(&("app".to_string(), table.to_string(), column.to_string())),
            "{table}.{column}: {retyped:?}"
        );
    }

    let script = compare_all(from, to).await;
    let order_retype = at(
        &script,
        "alter type app.qt alter attribute \"order\" set data type bigint cascade;",
    );
    let amt_retype = at(
        &script,
        "alter type app.qt alter attribute \"Amt\" set data type bigint cascade;",
    );
    let first_retype = order_retype.min(amt_retype);
    assert!(
        at(&script, "drop policy if exists qtt_pol on app.qtt;") < first_retype,
        "{script}"
    );
    assert!(
        at(&script, "alter table app.qti drop column g;") < first_retype,
        "{script}"
    );
    let add_g = at(
        &script,
        "alter table app.qti add column g bigint generated always as (\"Amt\" * 2) stored;",
    );
    assert!(amt_retype < add_g, "{script}");
    assert!(
        order_retype < at(&script, "create policy qtt_pol"),
        "{script}"
    );
    assert_eq!(
        script.matches("create policy qtt_pol").count(),
        1,
        "{script}"
    );
}

/// `app.tt OF app.ct` and `app.ti`, which inherits it when `inherits` and
/// is a table of its own with `amount <amount_type>` otherwise; `app.ti`
/// has a policy on `amount`.
fn r4t_leaving_dump(inherits: bool, amount_type: &str, with_edges: bool) -> Dump {
    let mut dump = r4t_dump();
    dump.types.push(r4t_type("integer", &["app.tt"]));
    let tt = r4t_table("tt", "integer", true);
    let mut ti = r4t_table("ti", amount_type, false);
    if inherits {
        ti.inherits_from = vec!["app.tt".to_string()];
    }
    ti.columns.push(r4t_column("ti", "extra", 3, "integer"));
    r4t_policy(&mut ti, "ti_pol", "(amount > 0)");
    dump.tables.extend([tt, ti]);
    if with_edges {
        dump.column_dependents = vec![r4t_edge(
            "ti",
            "amount",
            ColumnDependentKind::Policy,
            "ti",
            "ti_pol",
        )];
    }
    dump
}

/// Review finding: an inheritance child TO takes out of a typed table's
/// family retypes the column it had from the type with its own alter (after
/// its `NO INHERIT`), and that alter's blockers were no longer dropped: the
/// column was left to the type, which does not change.
#[tokio::test]
async fn r4t_a_descendant_leaving_the_typed_family_retypes_with_its_blockers_out_of_the_way() {
    let from = r4t_leaving_dump(true, "integer", true);
    let to = r4t_leaving_dump(false, "bigint", false);
    let comparer = Comparer::new(
        from.clone(),
        to.clone(),
        true,
        false,
        true,
        GrantsMode::Ignore,
    );
    let family = comparer.typed_family_columns();
    let key = |table: &str| ("app".to_string(), table.to_string(), "amount".to_string());
    assert!(family.contains(&key("tt")), "{family:?}");
    assert!(!family.contains(&key("ti")), "{family:?}");

    let script = compare_all(from, to).await;
    let no_inherit = at(&script, "alter table app.ti no inherit app.tt;");
    let retype = at(
        &script,
        "alter table app.ti alter column amount type bigint;",
    );
    assert!(no_inherit < retype, "{script}");
    assert!(
        at(&script, "drop policy if exists ti_pol on app.ti;") < retype,
        "{script}"
    );
    assert!(retype < at(&script, "create policy ti_pol"), "{script}");
    assert!(!script.contains("alter type app.ct"), "{script}");
    // (One that stays in the family is still left to the type: see
    // `r4t_typed_table_retypes_reach_partitions_descendants_and_dropped_tables`.)
}

/// A generated column `g` (`amount * 2`) of `<amount_type>`, plain when not
/// `generated`.
fn r4t_g(table: &str, ordinal: i32, amount_type: &str, generated: bool) -> TableColumn {
    let mut g = r4t_column(table, "g", ordinal, amount_type);
    if generated {
        g.is_generated = "ALWAYS".to_string();
        g.generation_expression = Some("(amount * 2)".to_string());
        g.generation_type = Some("s".to_string());
    }
    g
}

/// FROM: `app.tt OF app.ct`, `app.c1 (g GENERATED ALWAYS AS (amount * 2))
/// INHERITS (app.tt)`, `app.c2 INHERITS (app.c1)`, which inherits `g`.
fn r4t_three_level_from() -> Dump {
    let mut dump = r4t_dump();
    dump.types.push(r4t_type("integer", &["app.tt"]));
    let tt = r4t_table("tt", "integer", true);
    let mut c1 = r4t_table("c1", "integer", false);
    c1.inherits_from = vec!["app.tt".to_string()];
    c1.columns.push(r4t_g("c1", 3, "integer", true));
    c1.hash();
    let mut c2 = r4t_table("c2", "integer", false);
    c2.inherits_from = vec!["app.c1".to_string()];
    c2.columns.push(r4t_g("c2", 3, "integer", true));
    c2.columns.push(r4t_column("c2", "x", 4, "integer"));
    c2.hash();
    dump.tables.extend([tt, c1, c2]);
    dump
}

/// Review finding: a table the table phase drops lost each generated column
/// in the way of the type's retype on its own, even one it inherits from an
/// ancestor whose own statement ahead of the retype already reached it: the
/// ancestor's `DROP COLUMN` had taken it ("column "g" of relation "c2" does
/// not exist"), or its `DROP EXPRESSION` left an inherited plain column
/// ("cannot drop inherited column").
#[tokio::test]
async fn r4t_a_dropped_descendants_inherited_generated_column_is_left_to_its_ancestor() {
    // Both children go: c1's drop takes c2's inherited copy along, and a
    // copy c2 defines itself as well goes with `if exists`.
    let mut to = r4t_dump();
    to.types.push(r4t_type("bigint", &[]));
    to.tables.push(r4t_table("tt", "bigint", true));
    let script = compare_all(r4t_three_level_from(), to).await;
    let retype = at(&script, R4T_RETYPE);
    let c1 = at(&script, "alter table app.c1 drop column g;");
    let c2 = at(&script, "alter table app.c2 drop column if exists g;");
    assert!(c1 < c2 && c2 < retype, "{script}");
    assert!(
        !script.contains("alter table app.c2 drop column g;"),
        "{script}"
    );

    // c1 stays, with g a plain column: its `DROP EXPRESSION` reaches c2's
    // copy, which may not be dropped on its own.
    let mut to = r4t_dump();
    to.types.push(r4t_type("bigint", &[]));
    to.tables.push(r4t_table("tt", "bigint", true));
    let mut c1 = r4t_table("c1", "bigint", false);
    c1.inherits_from = vec!["app.tt".to_string()];
    c1.columns.push(r4t_g("c1", 3, "bigint", false));
    c1.hash();
    to.tables.push(c1);
    let script = compare_all(r4t_three_level_from(), to).await;
    let retype = at(&script, R4T_RETYPE);
    assert!(
        at(
            &script,
            "alter table app.c1 alter column g drop expression;"
        ) < retype,
        "{script}"
    );
    assert!(
        !script.contains("alter table app.c2 drop column"),
        "{script}"
    );
}

/// `app.tt OF ext.ct` (a type outside the compared schemas) with
/// `amount <amount_type>`, a view reading it, and a publication whose
/// column list and row filter use it.
fn r4t_uncompared_dump(amount_type: &str) -> Dump {
    use crate::dump::publication::{Publication, PublicationTableSpec};
    let mut dump = r4t_dump();
    let mut tt = r4t_table("tt", amount_type, false);
    tt.typed_table_type = Some("ext.ct".to_string());
    tt.hash();
    dump.tables.push(tt);
    let mut view = View::new(
        "v".to_string(),
        " SELECT id,\n    amount\n   FROM app.tt;".to_string(),
        "app".to_string(),
        vec!["app.tt".to_string()],
    );
    view.column_relation = Some(vec!["app.tt.id".to_string(), "app.tt.amount".to_string()]);
    view.hash();
    dump.views.push(view);
    let mut publication = Publication {
        name: "p".into(),
        owner: "postgres".into(),
        publish: "insert,update,delete,truncate".into(),
        tables: vec!["app.tt".to_string()],
        ..Default::default()
    };
    publication.table_specs.insert(
        "app.tt".to_string(),
        PublicationTableSpec {
            columns: vec!["amount".to_string(), "id".to_string()],
            row_filter: Some("(amount > 0)".to_string()),
        },
    );
    publication.hash();
    dump.publications.push(publication);
    dump
}

/// Review finding: a typed table's column that its type does not retype
/// (the type is not compared here) still counted as retyped for what reads
/// it, so the view over it was dropped and created again on every run, and
/// the publication member using it released and set again.
#[tokio::test]
async fn r4t_a_typed_column_its_type_does_not_retype_keeps_its_readers() {
    let script = compare_all(
        r4t_uncompared_dump("integer"),
        r4t_uncompared_dump("bigint"),
    )
    .await;
    for churn in ["drop view", "CREATE OR REPLACE VIEW", "ALTER PUBLICATION"] {
        assert!(!script.contains(churn), "{churn}:\n{script}");
    }
    // The existing warning still says the change is not migrated.
    assert!(
        script.contains("retyped amount (integer -> bigint)"),
        "{script}"
    );

    // An ordinary table's retype still takes them out of the way.
    let untyped = |amount_type: &str| {
        let mut dump = r4t_uncompared_dump(amount_type);
        dump.tables[0].typed_table_type = None;
        dump.tables[0].hash();
        dump
    };
    let script = compare_all(untyped("integer"), untyped("bigint")).await;
    let retype = at(
        &script,
        "alter table app.tt alter column amount type bigint",
    );
    assert!(
        at(&script, "drop view if exists app.v;") < retype,
        "{script}"
    );
    assert!(
        at(&script, "ALTER PUBLICATION p DROP TABLE ONLY app.tt;") < retype,
        "{script}"
    );
}
