//! Tests for `compare_types` and `compare_enums`, including composite and
//! multirange types.

use super::helpers::*;
use crate::comparer::core::*;
use crate::config::dump_config::DumpConfig;
use crate::config::grants_mode::GrantsMode;
use crate::dump::pg_type::CompositeAttribute;
use crate::dump::pg_type::PgType;
use crate::dump::routine::Routine;
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
