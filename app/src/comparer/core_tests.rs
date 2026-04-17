use super::*;
use crate::config::dump_config::DumpConfig;
use crate::config::grants_mode::GrantsMode;
use crate::dump::default_privilege::DefaultPrivilege;
use crate::dump::extension::Extension;
use crate::dump::pg_type::{CompositeAttribute, PgType};
use crate::dump::routine::Routine;
use crate::dump::schema::Schema;
use sqlx::postgres::types::Oid;

fn make_domain_type(schema: &str, name: &str, oid: u32) -> PgType {
    PgType::new(
        Oid(oid),
        schema.to_string(),
        name.to_string(),
        Oid(2200),
        Oid(10),
        "postgres".to_string(),
        -1,
        false,
        'd' as i8,
        'U' as i8,
        false,
        true,
        ',' as i8,
        None,
        None,
        None,
        None,
        "domain_in".to_string(),
        "domain_out".to_string(),
        None,
        None,
        None,
        None,
        None,
        'i' as i8,
        'x' as i8,
        false,
        Some(Oid(25)),
        None,
        0,
        None,
        None,
        Some("text".to_string()),
        Vec::new(),
        Vec::new(),
        None,
    )
}

fn make_enum_type(schema: &str, name: &str, oid: u32, labels: Vec<&str>) -> PgType {
    let mut enum_type = make_domain_type(schema, name, oid);
    enum_type.typtype = 'e' as i8;
    enum_type.typcategory = 'E' as i8;
    enum_type.typinput = "enum_in".to_string();
    enum_type.typoutput = "enum_out".to_string();
    enum_type.typbasetype = None;
    enum_type.formatted_basetype = None;
    enum_type.enum_labels = labels.into_iter().map(|label| label.to_string()).collect();
    enum_type.domain_constraints.clear();
    enum_type.hash();
    enum_type
}

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
async fn compare_routines_drops_and_recreates_on_return_type_change() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let from_routine = Routine::new(
        "public".to_string(),
        Oid(1),
        "test_func".to_string(),
        "plpgsql".to_string(),
        "FUNCTION".to_string(),
        "integer".to_string(),
        "".to_string(),
        None,
        None,
        "BEGIN RETURN 1; END".to_string(),
    );

    let to_routine = Routine::new(
        "public".to_string(),
        Oid(1),
        "test_func".to_string(),
        "plpgsql".to_string(),
        "FUNCTION".to_string(),
        "text".to_string(),
        "".to_string(),
        None,
        None,
        "BEGIN RETURN '1'; END".to_string(),
    );

    from_dump.routines.push(from_routine);
    to_dump.routines.push(to_routine);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Ignore);
    comparer.compare_routines().await.unwrap();
    let script = comparer.get_script();

    assert!(script.contains("drop function if exists public.test_func () cascade;"));
    assert!(script.contains("create or replace function public.test_func() returns text"));
}

#[tokio::test]
async fn compare_routines_drops_and_recreates_on_argument_change() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let from_routine = Routine::new(
        "public".to_string(),
        Oid(1),
        "test_func".to_string(),
        "plpgsql".to_string(),
        "FUNCTION".to_string(),
        "integer".to_string(),
        "a integer".to_string(),
        None,
        None,
        "BEGIN RETURN a; END".to_string(),
    );

    let to_routine = Routine::new(
        "public".to_string(),
        Oid(1),
        "test_func".to_string(),
        "plpgsql".to_string(),
        "FUNCTION".to_string(),
        "integer".to_string(),
        "a text".to_string(),
        None,
        None,
        "BEGIN RETURN 1; END".to_string(),
    );

    from_dump.routines.push(from_routine);
    to_dump.routines.push(to_routine);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Ignore);
    comparer.compare_routines().await.unwrap();
    let script = comparer.get_script();

    assert!(script.contains("drop function if exists public.test_func (a integer) cascade;"));
    assert!(script.contains("create or replace function public.test_func(a text) returns integer"));
}

#[tokio::test]
async fn compare_routines_applies_sql_routines_last() {
    let from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let plpgsql_routine = Routine::new(
        "public".to_string(),
        Oid(1),
        "fn_plpgsql".to_string(),
        "plpgsql".to_string(),
        "FUNCTION".to_string(),
        "integer".to_string(),
        "".to_string(),
        None,
        None,
        "BEGIN RETURN 1; END".to_string(),
    );

    let sql_routine = Routine::new(
        "public".to_string(),
        Oid(2),
        "fn_sql".to_string(),
        "sql".to_string(),
        "FUNCTION".to_string(),
        "integer".to_string(),
        "".to_string(),
        None,
        None,
        "SELECT 1;".to_string(),
    );

    // Intentionally add SQL first to ensure reordering happens.
    to_dump.routines.push(sql_routine);
    to_dump.routines.push(plpgsql_routine);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Ignore);
    comparer.compare_routines().await.unwrap();
    let script = comparer.get_script();

    let pos_plpgsql = script
        .find("create or replace function public.fn_plpgsql")
        .expect("plpgsql routine script not found");
    let pos_sql = script
        .find("create or replace function public.fn_sql")
        .expect("sql routine script not found");

    assert!(pos_plpgsql < pos_sql, "SQL routines should be applied last");
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

#[tokio::test]
async fn compare_schemas_emits_owner_change() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_schema = Schema::new("public".to_string(), "public".to_string(), None);
    from_schema.owner = "old_owner".to_string();
    from_schema.hash();

    let mut to_schema = Schema::new("public".to_string(), "public".to_string(), None);
    to_schema.owner = "new_owner".to_string();
    to_schema.hash();

    from_dump.schemas.push(from_schema);
    to_dump.schemas.push(to_schema);

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_schemas().await.unwrap();
    let script = comparer.get_script();

    assert!(script.contains("alter schema public owner to new_owner;"));
}

#[tokio::test]
async fn compare_extensions_notes_owner_change_as_unsupported() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_ext = Extension::new(
        "hstore".to_string(),
        "1.0".to_string(),
        "public".to_string(),
    );
    from_ext.owner = "old_owner".to_string();

    let mut to_ext = Extension::new(
        "hstore".to_string(),
        "1.0".to_string(),
        "public".to_string(),
    );
    to_ext.owner = "new_owner".to_string();

    from_dump.extensions.push(from_ext);
    to_dump.extensions.push(to_ext);

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_extensions().await.unwrap();
    let script = comparer.get_script();

    assert!(script.contains(
        "-- Extension owner change is not supported by PostgreSQL ALTER EXTENSION syntax (old_owner -> new_owner)."
    ));
}

#[tokio::test]
async fn compare_routines_emits_owner_change() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_routine = Routine::new(
        "public".to_string(),
        Oid(1),
        "test_func".to_string(),
        "plpgsql".to_string(),
        "FUNCTION".to_string(),
        "integer".to_string(),
        "".to_string(),
        None,
        None,
        "BEGIN RETURN 1; END".to_string(),
    );
    from_routine.owner = "old_owner".to_string();
    from_routine.hash();

    let mut to_routine = Routine::new(
        "public".to_string(),
        Oid(1),
        "test_func".to_string(),
        "plpgsql".to_string(),
        "FUNCTION".to_string(),
        "integer".to_string(),
        "".to_string(),
        None,
        None,
        "BEGIN RETURN 1; END".to_string(),
    );
    to_routine.owner = "new_owner".to_string();
    to_routine.hash();

    from_dump.routines.push(from_routine);
    to_dump.routines.push(to_routine);

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_routines().await.unwrap();
    let script = comparer.get_script();

    assert!(script.contains("alter function public.test_func() owner to new_owner;"));
}

#[tokio::test]
async fn compare_routines_orders_by_dependencies() {
    let from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    // r_base_value: no dependencies
    let r_base = Routine::new(
        "test_schema".to_string(),
        Oid(1),
        "r_base_value".to_string(),
        "sql".to_string(),
        "FUNCTION".to_string(),
        "integer".to_string(),
        "".to_string(),
        None,
        None,
        "\n    SELECT 10;\n".to_string(),
    );

    // x_step_one: depends on r_base_value
    let x_step = Routine::new(
        "test_schema".to_string(),
        Oid(2),
        "x_step_one".to_string(),
        "sql".to_string(),
        "FUNCTION".to_string(),
        "integer".to_string(),
        "".to_string(),
        None,
        None,
        "\n    SELECT test_schema.r_base_value() + 5;\n".to_string(),
    );

    // a_middle_layer: depends on x_step_one and r_base_value
    let a_middle = Routine::new(
        "test_schema".to_string(),
        Oid(3),
        "a_middle_layer".to_string(),
        "sql".to_string(),
        "FUNCTION".to_string(),
        "integer".to_string(),
        "".to_string(),
        None,
        None,
        "\n    SELECT test_schema.x_step_one() * test_schema.r_base_value();\n".to_string(),
    );

    // z_final_report: depends on a_middle_layer
    let z_final = Routine::new(
        "test_schema".to_string(),
        Oid(4),
        "z_final_report".to_string(),
        "plpgsql".to_string(),
        "PROCEDURE".to_string(),
        "void".to_string(),
        "".to_string(),
        None,
        None,
        "\nDECLARE\n    result integer;\nBEGIN\n    SELECT test_schema.a_middle_layer() INTO result;\n    RAISE NOTICE 'Final result: %', result;\nEND;\n".to_string(),
    );

    // Push in deliberately wrong alphabetical / type order.
    to_dump.routines.push(z_final);
    to_dump.routines.push(x_step);
    to_dump.routines.push(a_middle);
    to_dump.routines.push(r_base);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Ignore);
    comparer.compare_routines().await.unwrap();
    let script = comparer.get_script();

    let pos_base = script
        .find("create or replace function test_schema.r_base_value")
        .expect("r_base_value not found");
    let pos_step = script
        .find("create or replace function test_schema.x_step_one")
        .expect("x_step_one not found");
    let pos_middle = script
        .find("create or replace function test_schema.a_middle_layer")
        .expect("a_middle_layer not found");
    let pos_final = script
        .find("create or replace procedure test_schema.z_final_report")
        .expect("z_final_report not found");

    assert!(
        pos_base < pos_step,
        "r_base_value must come before x_step_one (depends on it)"
    );
    assert!(
        pos_base < pos_middle,
        "r_base_value must come before a_middle_layer (depends on it)"
    );
    assert!(
        pos_step < pos_middle,
        "x_step_one must come before a_middle_layer (depends on it)"
    );
    assert!(
        pos_middle < pos_final,
        "a_middle_layer must come before z_final_report (depends on it)"
    );
}

#[tokio::test]
async fn compare_routines_drops_in_reverse_dependency_order() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());

    // r_base_value: no dependencies
    let r_base = Routine::new(
        "test_schema".to_string(),
        Oid(1),
        "r_base_value".to_string(),
        "sql".to_string(),
        "FUNCTION".to_string(),
        "integer".to_string(),
        "".to_string(),
        None,
        None,
        "\n    SELECT 10;\n".to_string(),
    );

    // x_step_one: depends on r_base_value
    let x_step = Routine::new(
        "test_schema".to_string(),
        Oid(2),
        "x_step_one".to_string(),
        "sql".to_string(),
        "FUNCTION".to_string(),
        "integer".to_string(),
        "".to_string(),
        None,
        None,
        "\n    SELECT test_schema.r_base_value() + 5;\n".to_string(),
    );

    // a_middle_layer: depends on x_step_one and r_base_value
    let a_middle = Routine::new(
        "test_schema".to_string(),
        Oid(3),
        "a_middle_layer".to_string(),
        "sql".to_string(),
        "FUNCTION".to_string(),
        "integer".to_string(),
        "".to_string(),
        None,
        None,
        "\n    SELECT test_schema.x_step_one() * test_schema.r_base_value();\n".to_string(),
    );

    from_dump.routines.push(r_base);
    from_dump.routines.push(x_step);
    from_dump.routines.push(a_middle);

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_routines().await.unwrap();
    let script = comparer.get_script();

    let pos_base = script
        .find("drop function if exists test_schema.r_base_value")
        .expect("r_base_value drop not found");
    let pos_step = script
        .find("drop function if exists test_schema.x_step_one")
        .expect("x_step_one drop not found");
    let pos_middle = script
        .find("drop function if exists test_schema.a_middle_layer")
        .expect("a_middle_layer drop not found");

    // Drops should go in reverse dependency order: dependents first.
    assert!(
        pos_middle < pos_step,
        "a_middle_layer must be dropped before x_step_one"
    );
    assert!(
        pos_step < pos_base,
        "x_step_one must be dropped before r_base_value"
    );
}

#[tokio::test]
async fn compare_routines_and_views_orders_by_dependencies() {
    let from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let r_base = Routine::new(
        "test_schema".to_string(),
        Oid(1),
        "r_base_value".to_string(),
        "sql".to_string(),
        "FUNCTION".to_string(),
        "integer".to_string(),
        "".to_string(),
        None,
        None,
        "\n    SELECT 10;\n".to_string(),
    );

    let x_step = Routine::new(
        "test_schema".to_string(),
        Oid(2),
        "x_step_one".to_string(),
        "sql".to_string(),
        "FUNCTION".to_string(),
        "integer".to_string(),
        "".to_string(),
        None,
        None,
        "\n    SELECT test_schema.r_base_value() + 5;\n".to_string(),
    );

    let a_middle = Routine::new(
        "test_schema".to_string(),
        Oid(3),
        "a_middle_layer".to_string(),
        "sql".to_string(),
        "FUNCTION".to_string(),
        "integer".to_string(),
        "".to_string(),
        None,
        None,
        "\n    SELECT test_schema.x_step_one() * test_schema.r_base_value();\n".to_string(),
    );

    let z_final = Routine::new(
        "test_schema".to_string(),
        Oid(4),
        "z_final_report".to_string(),
        "plpgsql".to_string(),
        "PROCEDURE".to_string(),
        "void".to_string(),
        "".to_string(),
        None,
        None,
        "\nDECLARE\n    result integer;\nBEGIN\n    SELECT test_schema.a_middle_layer() INTO result;\n    RAISE NOTICE 'Final result: %', result;\nEND;\n".to_string(),
    );

    // Push in deliberately wrong order.
    to_dump.routines.push(z_final);
    to_dump.routines.push(x_step);
    to_dump.routines.push(a_middle);
    to_dump.routines.push(r_base);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Ignore);
    comparer.compare_routines_and_views().await.unwrap();
    let script = comparer.get_script();

    let pos_base = script
        .find("create or replace function test_schema.r_base_value")
        .expect("r_base_value not found");
    let pos_step = script
        .find("create or replace function test_schema.x_step_one")
        .expect("x_step_one not found");
    let pos_middle = script
        .find("create or replace function test_schema.a_middle_layer")
        .expect("a_middle_layer not found");
    let pos_final = script
        .find("create or replace procedure test_schema.z_final_report")
        .expect("z_final_report not found");

    assert!(
        pos_base < pos_step,
        "r_base_value must come before x_step_one"
    );
    assert!(
        pos_base < pos_middle,
        "r_base_value must come before a_middle_layer"
    );
    assert!(
        pos_step < pos_middle,
        "x_step_one must come before a_middle_layer"
    );
    assert!(
        pos_middle < pos_final,
        "a_middle_layer must come before z_final_report"
    );
}

use crate::dump::sequence::Sequence;
use crate::dump::table::Table;
use crate::dump::table_column::TableColumn;
use crate::dump::table_constraint::TableConstraint;
use crate::dump::table_trigger::TableTrigger;
use crate::dump::view::View;

fn int_column(schema: &str, table: &str, name: &str, ordinal: i32) -> TableColumn {
    TableColumn {
        catalog: "postgres".to_string(),
        schema: schema.to_string(),
        table: table.to_string(),
        name: name.to_string(),
        ordinal_position: ordinal,
        column_default: None,
        is_nullable: true,
        data_type: "integer".to_string(),
        character_maximum_length: None,
        character_octet_length: None,
        numeric_precision: Some(32),
        numeric_precision_radix: Some(2),
        numeric_scale: Some(0),
        datetime_precision: None,
        interval_type: None,
        interval_precision: None,
        character_set_catalog: None,
        character_set_schema: None,
        character_set_name: None,
        collation_catalog: None,
        collation_schema: None,
        collation_name: None,
        domain_catalog: None,
        domain_schema: None,
        domain_name: None,
        udt_catalog: None,
        udt_schema: None,
        udt_name: None,
        scope_catalog: None,
        scope_schema: None,
        scope_name: None,
        maximum_cardinality: None,
        dtd_identifier: None,
        is_self_referencing: false,
        is_identity: false,
        identity_generation: None,
        identity_start: None,
        identity_increment: None,
        identity_maximum: None,
        identity_minimum: None,
        identity_cycle: false,
        is_generated: "NEVER".to_string(),
        generation_expression: None,
        generation_type: None,
        is_updatable: true,
        related_views: None,
        comment: None,
        storage: None,
        compression: None,
        statistics_target: None,
        acl: vec![],
        serial_type: None,
    }
}

#[tokio::test]
async fn compare_sequences_skips_owned_by_serial_column() {
    let from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    // Sequence owned by table column
    let sequence = Sequence::new(
        "public".to_string(),
        "test_id_seq".to_string(),
        "postgres".to_string(),
        "bigint".to_string(),
        Some(1),
        Some(1),
        Some(1000),
        Some(1),
        false,
        Some(1),
        Some(1),
        Some("public".to_string()),
        Some("test".to_string()),
        Some("id".to_string()),
    );
    to_dump.sequences.push(sequence);

    // Table with serial column
    let column = TableColumn {
        catalog: "postgres".to_string(),
        schema: "public".to_string(),
        table: "test".to_string(),
        name: "id".to_string(),
        ordinal_position: 1,
        column_default: Some("nextval('test_id_seq'::regclass)".to_string()),
        is_nullable: false,
        data_type: "bigint".to_string(), // PostgreSQL reports bigserial as bigint with nextval default
        character_maximum_length: None,
        character_octet_length: None,
        numeric_precision: Some(64),
        numeric_precision_radix: Some(2),
        numeric_scale: Some(0),
        datetime_precision: None,
        interval_type: None,
        interval_precision: None,
        character_set_catalog: None,
        character_set_schema: None,
        character_set_name: None,
        collation_catalog: None,
        collation_schema: None,
        collation_name: None,
        domain_catalog: None,
        domain_schema: None,
        domain_name: None,
        udt_catalog: None,
        udt_schema: None,
        udt_name: None,
        scope_catalog: None,
        scope_schema: None,
        scope_name: None,
        maximum_cardinality: None,
        dtd_identifier: None,
        is_self_referencing: false,
        is_identity: false,
        identity_generation: None,
        identity_start: None,
        identity_increment: None,
        identity_maximum: None,
        identity_minimum: None,
        identity_cycle: false,
        is_generated: "NEVER".to_string(),
        generation_expression: None,
        generation_type: None,
        is_updatable: true,
        related_views: None,
        comment: None,
        storage: None,
        compression: None,
        statistics_target: None,
        acl: vec![],
        serial_type: None,
    };

    let table = Table::new(
        "public".to_string(),
        "test".to_string(),
        "public".to_string(),
        "test".to_string(),
        "postgres".to_string(),
        None,
        vec![column],
        vec![],
        vec![],
        vec![],
        None,
    );
    to_dump.tables.push(table);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Ignore);
    comparer.compare_sequences().await.unwrap();
    let script = comparer.get_script();

    assert!(script.contains(
        "Skipping sequence public.test_id_seq as it will be created by column public.test.id"
    ));
    assert!(!script.contains("create sequence \"public\".\"test_id_seq\""));
}

#[tokio::test]
async fn compare_sequences_skips_owned_by_identity_column() {
    let from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    // Sequence owned by table column
    let sequence = Sequence::new(
        "public".to_string(),
        "test_id_seq".to_string(),
        "postgres".to_string(),
        "bigint".to_string(),
        Some(1),
        Some(1),
        Some(1000),
        Some(1),
        false,
        Some(1),
        Some(1),
        Some("public".to_string()),
        Some("test".to_string()),
        Some("id".to_string()),
    );
    to_dump.sequences.push(sequence);

    // Table with identity column
    let column = TableColumn {
        catalog: "postgres".to_string(),
        schema: "public".to_string(),
        table: "test".to_string(),
        name: "id".to_string(),
        ordinal_position: 1,
        column_default: None,
        is_nullable: false,
        data_type: "bigint".to_string(),
        character_maximum_length: None,
        character_octet_length: None,
        numeric_precision: Some(64),
        numeric_precision_radix: Some(2),
        numeric_scale: Some(0),
        datetime_precision: None,
        interval_type: None,
        interval_precision: None,
        character_set_catalog: None,
        character_set_schema: None,
        character_set_name: None,
        collation_catalog: None,
        collation_schema: None,
        collation_name: None,
        domain_catalog: None,
        domain_schema: None,
        domain_name: None,
        udt_catalog: None,
        udt_schema: None,
        udt_name: None,
        scope_catalog: None,
        scope_schema: None,
        scope_name: None,
        maximum_cardinality: None,
        dtd_identifier: None,
        is_self_referencing: false,
        is_identity: true, // This triggers the skip
        identity_generation: Some("ALWAYS".to_string()),
        identity_start: Some("1".to_string()),
        identity_increment: Some("1".to_string()),
        identity_maximum: None,
        identity_minimum: None,
        identity_cycle: false,
        is_generated: "NEVER".to_string(),
        generation_expression: None,
        generation_type: None,
        is_updatable: true,
        related_views: None,
        comment: None,
        storage: None,
        compression: None,
        statistics_target: None,
        acl: vec![],
        serial_type: None,
    };

    let table = Table::new(
        "public".to_string(),
        "test".to_string(),
        "public".to_string(),
        "test".to_string(),
        "postgres".to_string(),
        None,
        vec![column],
        vec![],
        vec![],
        vec![],
        None,
    );
    to_dump.tables.push(table);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Ignore);
    comparer.compare_sequences().await.unwrap();
    let script = comparer.get_script();

    assert!(script.contains(
        "Skipping sequence public.test_id_seq as it will be created by column public.test.id"
    ));
    assert!(!script.contains("create sequence \"public\".\"test_id_seq\""));
}

#[tokio::test]
async fn compare_sequences_does_not_skip_normal_sequence() {
    let from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    // Normal sequence not owned by any column
    let sequence = Sequence::new(
        "public".to_string(),
        "test_seq".to_string(),
        "postgres".to_string(),
        "bigint".to_string(),
        Some(1),
        Some(1),
        Some(1000),
        Some(1),
        false,
        Some(1),
        Some(1),
        None,
        None,
        None,
    );
    to_dump.sequences.push(sequence);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Ignore);
    comparer.compare_sequences().await.unwrap();
    let script = comparer.get_script();

    assert!(!script.contains("Skipping sequence"));
    assert!(script.contains("create sequence public.test_seq"));
}

#[tokio::test]
async fn compare_sequences_emits_owner_change() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let from_sequence = Sequence::new(
        "public".to_string(),
        "test_seq".to_string(),
        "old_owner".to_string(),
        "bigint".to_string(),
        Some(1),
        Some(1),
        Some(1000),
        Some(1),
        false,
        Some(1),
        Some(1),
        None,
        None,
        None,
    );

    let to_sequence = Sequence::new(
        "public".to_string(),
        "test_seq".to_string(),
        "new_owner".to_string(),
        "bigint".to_string(),
        Some(1),
        Some(1),
        Some(1000),
        Some(1),
        false,
        Some(1),
        Some(1),
        None,
        None,
        None,
    );

    from_dump.sequences.push(from_sequence);
    to_dump.sequences.push(to_sequence);

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_sequences().await.unwrap();
    let script = comparer.get_script();

    assert!(script.contains("alter sequence public.test_seq owner to new_owner;"));
}

/// When MINVALUE is raised above the sequence's effective current position (last_value if
/// known, otherwise old start_value), the comparer must emit RESTART WITH so PostgreSQL does
/// not fall back to an old recorded start value that violates the new MINVALUE:
///
///   ERROR: RESTART value (1) cannot be less than MINVALUE (10000000)
#[tokio::test]
async fn compare_sequences_emits_restart_when_effective_current_below_new_minvalue() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    // last_value is None → effective_current falls back to start_value (1), which is < 10M.
    let from_sequence = Sequence::new(
        "my_schema".to_string(),
        "my_sequence_id_seq".to_string(),
        "postgres".to_string(),
        "bigint".to_string(),
        Some(1),
        Some(1),
        Some(999_999_999),
        Some(1),
        false,
        Some(1),
        None,
        None,
        None,
        None,
    );
    let to_sequence = Sequence::new(
        "my_schema".to_string(),
        "my_sequence_id_seq".to_string(),
        "postgres".to_string(),
        "bigint".to_string(),
        Some(10_000_000),
        Some(10_000_000),
        Some(999_999_999),
        Some(1),
        true,
        Some(1),
        None,
        None,
        None,
        None,
    );

    from_dump.sequences.push(from_sequence);
    to_dump.sequences.push(to_sequence);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Ignore);
    comparer.compare_sequences().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("start with 10000000"),
        "script must contain START WITH: {script}"
    );
    assert!(
        script.contains("restart with 10000000"),
        "script must contain RESTART WITH to prevent RESTART value < MINVALUE error: {script}"
    );
}

/// When last_value is already above the new MINVALUE, RESTART WITH must NOT be emitted
/// even though start_value and MINVALUE are both raised.  Emitting it would rewind the
/// live sequence and risk duplicate-key violations.
#[tokio::test]
async fn compare_sequences_no_restart_when_last_value_already_above_new_minvalue() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let from_sequence = Sequence::new(
        "public".to_string(),
        "busy_seq".to_string(),
        "postgres".to_string(),
        "bigint".to_string(),
        Some(1),
        Some(1),
        Some(999_999_999),
        Some(1),
        false,
        Some(1),
        Some(15_000_000), // last_value is already well above the new MINVALUE (10M)
        None,
        None,
        None,
    );
    let to_sequence = Sequence::new(
        "public".to_string(),
        "busy_seq".to_string(),
        "postgres".to_string(),
        "bigint".to_string(),
        Some(10_000_000), // start_value raised
        Some(10_000_000), // MINVALUE raised — but last_value (15M) already satisfies it
        Some(999_999_999),
        Some(1),
        false,
        Some(1),
        None,
        None,
        None,
        None,
    );

    from_dump.sequences.push(from_sequence);
    to_dump.sequences.push(to_sequence);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Ignore);
    comparer.compare_sequences().await.unwrap();
    let script = comparer.get_script();

    assert!(
        !script.contains("restart with"),
        "script must NOT contain RESTART WITH when last_value is already above new MINVALUE: {script}"
    );
    assert!(
        script.contains("alter sequence public.busy_seq"),
        "script must still emit ALTER SEQUENCE to update start_value/minvalue: {script}"
    );
}

/// When only non-start/minvalue parameters change (here: cycle) and the effective current
/// position is already within the new bounds, RESTART WITH must NOT be emitted.
#[tokio::test]
async fn compare_sequences_no_restart_when_only_other_params_change() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let from_sequence = Sequence::new(
        "public".to_string(),
        "live_seq".to_string(),
        "postgres".to_string(),
        "bigint".to_string(),
        Some(1),
        Some(1),
        Some(9999),
        Some(1),
        false, // cycle was false
        Some(1),
        Some(500_000),
        None,
        None,
        None,
    );
    let to_sequence = Sequence::new(
        "public".to_string(),
        "live_seq".to_string(),
        "postgres".to_string(),
        "bigint".to_string(),
        Some(1), // start_value unchanged
        Some(1),
        Some(9999),
        Some(1),
        true, // only cycle changed
        Some(1),
        None,
        None,
        None,
        None,
    );

    from_dump.sequences.push(from_sequence);
    to_dump.sequences.push(to_sequence);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Ignore);
    comparer.compare_sequences().await.unwrap();
    let script = comparer.get_script();

    assert!(
        !script.contains("restart with"),
        "script must NOT contain RESTART WITH when start_value is unchanged: {script}"
    );
    assert!(
        script.contains("alter sequence public.live_seq"),
        "script must still contain the ALTER SEQUENCE: {script}"
    );
}

#[tokio::test]
async fn compare_sequences_skips_drop_if_owned_by_dropped_table() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());

    // Sequence owned by table column
    let sequence = Sequence::new(
        "public".to_string(),
        "test_id_seq".to_string(),
        "postgres".to_string(),
        "bigint".to_string(),
        Some(1),
        Some(1),
        Some(1000),
        Some(1),
        false,
        Some(1),
        Some(1),
        Some("public".to_string()),
        Some("test".to_string()),
        Some("id".to_string()),
    );
    from_dump.sequences.push(sequence);

    // Table that owns the sequence
    let column = TableColumn {
        catalog: "postgres".to_string(),
        schema: "public".to_string(),
        table: "test".to_string(),
        name: "id".to_string(),
        ordinal_position: 1,
        column_default: None,
        is_nullable: false,
        data_type: "bigint".to_string(),
        character_maximum_length: None,
        character_octet_length: None,
        numeric_precision: Some(64),
        numeric_precision_radix: Some(2),
        numeric_scale: Some(0),
        datetime_precision: None,
        interval_type: None,
        interval_precision: None,
        character_set_catalog: None,
        character_set_schema: None,
        character_set_name: None,
        collation_catalog: None,
        collation_schema: None,
        collation_name: None,
        domain_catalog: None,
        domain_schema: None,
        domain_name: None,
        udt_catalog: None,
        udt_schema: None,
        udt_name: None,
        scope_catalog: None,
        scope_schema: None,
        scope_name: None,
        maximum_cardinality: None,
        dtd_identifier: None,
        is_self_referencing: false,
        is_identity: true,
        identity_generation: Some("ALWAYS".to_string()),
        identity_start: Some("1".to_string()),
        identity_increment: Some("1".to_string()),
        identity_maximum: None,
        identity_minimum: None,
        identity_cycle: false,
        is_generated: "NEVER".to_string(),
        generation_expression: None,
        generation_type: None,
        is_updatable: true,
        related_views: None,
        comment: None,
        storage: None,
        compression: None,
        statistics_target: None,
        acl: vec![],
        serial_type: None,
    };

    let table = Table::new(
        "public".to_string(),
        "test".to_string(),
        "public".to_string(),
        "test".to_string(),
        "postgres".to_string(),
        None,
        vec![column],
        vec![],
        vec![],
        vec![],
        None,
    );
    from_dump.tables.push(table);

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_sequences().await.unwrap();
    let script = comparer.get_script();

    assert!(script.contains("Skipping drop of sequence public.test_id_seq as it is owned by table public.test which will be dropped."));
}

#[tokio::test]
async fn compare_sequences_skips_drop_if_owned_by_identity_column() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    // Sequence owned by table column
    let sequence = Sequence::new(
        "public".to_string(),
        "test_id_seq".to_string(),
        "postgres".to_string(),
        "bigint".to_string(),
        Some(1),
        Some(1),
        Some(1000),
        Some(1),
        false,
        Some(1),
        Some(1),
        Some("public".to_string()),
        Some("test".to_string()),
        Some("id".to_string()),
    );
    from_dump.sequences.push(sequence);

    // Table with identity column in FROM
    let from_column = TableColumn {
        catalog: "postgres".to_string(),
        schema: "public".to_string(),
        table: "test".to_string(),
        name: "id".to_string(),
        ordinal_position: 1,
        column_default: None,
        is_nullable: false,
        data_type: "bigint".to_string(),
        character_maximum_length: None,
        character_octet_length: None,
        numeric_precision: Some(64),
        numeric_precision_radix: Some(2),
        numeric_scale: Some(0),
        datetime_precision: None,
        interval_type: None,
        interval_precision: None,
        character_set_catalog: None,
        character_set_schema: None,
        character_set_name: None,
        collation_catalog: None,
        collation_schema: None,
        collation_name: None,
        domain_catalog: None,
        domain_schema: None,
        domain_name: None,
        udt_catalog: None,
        udt_schema: None,
        udt_name: None,
        scope_catalog: None,
        scope_schema: None,
        scope_name: None,
        maximum_cardinality: None,
        dtd_identifier: None,
        is_self_referencing: false,
        is_identity: true,
        identity_generation: Some("ALWAYS".to_string()),
        identity_start: Some("1".to_string()),
        identity_increment: Some("1".to_string()),
        identity_maximum: None,
        identity_minimum: None,
        identity_cycle: false,
        is_generated: "NEVER".to_string(),
        generation_expression: None,
        generation_type: None,
        is_updatable: true,
        related_views: None,
        comment: None,
        storage: None,
        compression: None,
        statistics_target: None,
        acl: vec![],
        serial_type: None,
    };

    let from_table = Table::new(
        "public".to_string(),
        "test".to_string(),
        "public".to_string(),
        "test".to_string(),
        "postgres".to_string(),
        None,
        vec![from_column],
        vec![],
        vec![],
        vec![],
        None,
    );
    from_dump.tables.push(from_table);

    // Table in TO (exists, but maybe column changed or sequence changed)
    // Even if column is same, if sequence is missing in TO (simulated here by not adding it to to_dump.sequences),
    // we should skip drop if it's identity.
    let to_column = TableColumn {
        catalog: "postgres".to_string(),
        schema: "public".to_string(),
        table: "test".to_string(),
        name: "id".to_string(),
        ordinal_position: 1,
        column_default: None,
        is_nullable: false,
        data_type: "bigint".to_string(),
        character_maximum_length: None,
        character_octet_length: None,
        numeric_precision: Some(64),
        numeric_precision_radix: Some(2),
        numeric_scale: Some(0),
        datetime_precision: None,
        interval_type: None,
        interval_precision: None,
        character_set_catalog: None,
        character_set_schema: None,
        character_set_name: None,
        collation_catalog: None,
        collation_schema: None,
        collation_name: None,
        domain_catalog: None,
        domain_schema: None,
        domain_name: None,
        udt_catalog: None,
        udt_schema: None,
        udt_name: None,
        scope_catalog: None,
        scope_schema: None,
        scope_name: None,
        maximum_cardinality: None,
        dtd_identifier: None,
        is_self_referencing: false,
        is_identity: true, // Still identity
        identity_generation: Some("ALWAYS".to_string()),
        identity_start: Some("1".to_string()),
        identity_increment: Some("1".to_string()),
        identity_maximum: None,
        identity_minimum: None,
        identity_cycle: false,
        is_generated: "NEVER".to_string(),
        generation_expression: None,
        generation_type: None,
        is_updatable: true,
        related_views: None,
        comment: None,
        storage: None,
        compression: None,
        statistics_target: None,
        acl: vec![],
        serial_type: None,
    };

    let to_table = Table::new(
        "public".to_string(),
        "test".to_string(),
        "public".to_string(),
        "test".to_string(),
        "postgres".to_string(),
        None,
        vec![to_column],
        vec![],
        vec![],
        vec![],
        None,
    );
    to_dump.tables.push(to_table);

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_sequences().await.unwrap();
    let script = comparer.get_script();

    assert!(script.contains("Skipping drop of sequence public.test_id_seq as it is owned by identity column public.test.id."));
}

#[tokio::test]
async fn tables_create_parent_before_partition_and_fk_after_tables() {
    let from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    // Parent partitioned table
    let mut parent = Table::new(
        "public".to_string(),
        "parent".to_string(),
        "public".to_string(),
        "parent".to_string(),
        "postgres".to_string(),
        None,
        vec![int_column("public", "parent", "id", 1)],
        vec![],
        vec![],
        vec![],
        None,
    );
    parent.partition_key = Some("LIST (id)".to_string());
    parent.hash();

    // Partition table
    let mut part = Table::new(
        "public".to_string(),
        "child".to_string(),
        "public".to_string(),
        "child".to_string(),
        "postgres".to_string(),
        None,
        vec![int_column("public", "child", "id", 1)],
        vec![],
        vec![],
        vec![],
        None,
    );
    part.partition_of = Some("public.parent".to_string());
    part.partition_bound = Some("FOR VALUES IN (1)".to_string());
    part.hash();

    // Referencing table with FK to parent
    let mut orders = Table::new(
        "public".to_string(),
        "orders".to_string(),
        "public".to_string(),
        "orders".to_string(),
        "postgres".to_string(),
        None,
        vec![
            int_column("public", "orders", "id", 1),
            int_column("public", "orders", "parent_id", 2),
        ],
        vec![TableConstraint {
            catalog: "postgres".to_string(),
            schema: "public".to_string(),
            name: "orders_parent_fk".to_string(),
            table_name: "orders".to_string(),
            constraint_type: "FOREIGN KEY".to_string(),
            is_deferrable: false,
            initially_deferred: false,
            definition: Some("FOREIGN KEY (parent_id) REFERENCES public.parent(id)".to_string()),
            coninhcount: 0,
            is_enforced: true,
            no_inherit: false,
            nulls_not_distinct: false,
            comment: None,
        }],
        vec![],
        vec![],
        None,
    );
    orders.hash();

    to_dump.tables.push(parent);
    to_dump.tables.push(part);
    to_dump.tables.push(orders);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();

    let pos_parent = script
        .find("create table public.parent")
        .expect("parent table not created");
    let pos_child = script
        .find("create table public.child partition of public.parent")
        .expect("partition table not created");
    let pos_orders = script
        .find("create table public.orders")
        .expect("orders table not created");
    let pos_fk = script
        .find("alter table public.orders add constraint orders_parent_fk")
        .expect("fk not emitted");

    assert!(
        pos_parent < pos_child,
        "parent should be created before partition"
    );
    assert!(
        pos_fk > pos_parent && pos_fk > pos_orders,
        "foreign key should be created after tables"
    );
}

#[tokio::test]
async fn compare_tables_emits_owner_change() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_table = Table::new(
        "public".to_string(),
        "users".to_string(),
        "public".to_string(),
        "users".to_string(),
        "old_owner".to_string(),
        None,
        vec![int_column("public", "users", "id", 1)],
        vec![],
        vec![],
        vec![],
        None,
    );
    from_table.hash();

    let mut to_table = Table::new(
        "public".to_string(),
        "users".to_string(),
        "public".to_string(),
        "users".to_string(),
        "new_owner".to_string(),
        None,
        vec![int_column("public", "users", "id", 1)],
        vec![],
        vec![],
        vec![],
        None,
    );
    to_table.hash();

    from_dump.tables.push(from_table);
    to_dump.tables.push(to_table);

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_tables().await.unwrap();
    let script = comparer.get_script();

    assert!(script.contains("alter table public.users owner to new_owner;"));
}

#[tokio::test]
async fn create_views_emits_owner_change_for_existing_view() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_view = View::new(
        "active_users".to_string(),
        "select id from public.users".to_string(),
        "public".to_string(),
        vec!["public.users".to_string()],
    );
    from_view.owner = "old_owner".to_string();
    from_view.hash();

    let mut to_view = View::new(
        "active_users".to_string(),
        "select id from public.users".to_string(),
        "public".to_string(),
        vec!["public.users".to_string()],
    );
    to_view.owner = "new_owner".to_string();
    to_view.hash();

    from_dump.views.push(from_view);
    to_dump.views.push(to_view);

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.create_views().await.unwrap();
    let script = comparer.get_script();

    assert!(script.contains("alter view public.active_users owner to new_owner;"));
}

#[tokio::test]
async fn compare_creates_routines_and_views_in_dependency_order() {
    // Scenario from the user report:
    //   get_user_count()   – function, no view dependency
    //   v_user_stats       – view that calls get_user_count()
    //   report_user_stats  – function that reads v_user_stats
    //   print_user_stats   – procedure that reads v_user_stats
    //
    // Correct creation order: get_user_count → v_user_stats → report/print
    let from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    to_dump.schemas.push(Schema::new(
        "test_schema".to_string(),
        "test_schema".to_string(),
        None,
    ));

    let get_user_count = Routine::new(
        "test_schema".to_string(),
        Oid(1),
        "get_user_count".to_string(),
        "sql".to_string(),
        "FUNCTION".to_string(),
        "integer".to_string(),
        "".to_string(),
        None,
        None,
        "  SELECT count(*) FROM test_schema.users;\n".to_string(),
    );

    let report_user_stats = Routine::new(
        "test_schema".to_string(),
        Oid(2),
        "report_user_stats".to_string(),
        "sql".to_string(),
        "FUNCTION".to_string(),
        "text".to_string(),
        "".to_string(),
        None,
        None,
        "  SELECT 'Total users in view: ' || total_users\n  FROM test_schema.v_user_stats\n  LIMIT 1;\n".to_string(),
    );

    let print_user_stats = Routine::new(
        "test_schema".to_string(),
        Oid(3),
        "print_user_stats".to_string(),
        "plpgsql".to_string(),
        "PROCEDURE".to_string(),
        "void".to_string(),
        "".to_string(),
        None,
        None,
        "\nDECLARE\n    cnt int;\nBEGIN\n    SELECT total_users INTO cnt\n    FROM test_schema.v_user_stats\n    LIMIT 1;\n    RAISE NOTICE 'Total users in view: %', cnt;\nEND;\n".to_string(),
    );

    let mut v_user_stats = View::new(
        "v_user_stats".to_string(),
        " SELECT test_schema.get_user_count() AS total_users,\n    users.name\n   FROM test_schema.users;\n".to_string(),
        "test_schema".to_string(),
        vec!["test_schema.users".to_string()],
    );
    v_user_stats.owner = "postgres".to_string();
    v_user_stats.hash();

    // Intentionally add in wrong order to test sorting
    to_dump.routines.push(print_user_stats);
    to_dump.routines.push(report_user_stats);
    to_dump.routines.push(get_user_count);
    to_dump.views.push(v_user_stats);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();

    let pos_get_user_count = script
        .find("create or replace function test_schema.get_user_count")
        .expect("get_user_count not found in script");
    let pos_view = script
        .find("test_schema.v_user_stats")
        .expect("v_user_stats not found in script");
    let pos_report = script
        .find("create or replace function test_schema.report_user_stats")
        .expect("report_user_stats not found in script");
    let pos_print = script
        .find("create or replace procedure test_schema.print_user_stats")
        .expect("print_user_stats not found in script");

    assert!(
        pos_get_user_count < pos_view,
        "get_user_count() must be created before v_user_stats (function is used by view)"
    );
    assert!(
        pos_view < pos_report,
        "v_user_stats must be created before report_user_stats() (view is used by function)"
    );
    assert!(
        pos_view < pos_print,
        "v_user_stats must be created before print_user_stats() (view is used by procedure)"
    );
}

#[tokio::test]
async fn compare_creates_materialized_view_after_dependent_routine() {
    // Materialized view that uses a function should be created after that function.
    let from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let helper_fn = Routine::new(
        "public".to_string(),
        Oid(1),
        "helper".to_string(),
        "sql".to_string(),
        "FUNCTION".to_string(),
        "integer".to_string(),
        "".to_string(),
        None,
        None,
        "SELECT 42;\n".to_string(),
    );

    let mut mat_view = View::new(
        "mv_data".to_string(),
        " SELECT public.helper() AS value;\n".to_string(),
        "public".to_string(),
        vec![],
    );
    mat_view.is_materialized = true;
    mat_view.hash();

    to_dump.routines.push(helper_fn);
    to_dump.views.push(mat_view);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();

    let pos_fn = script
        .find("create or replace function public.helper")
        .expect("helper function not found");
    let pos_mv = script.find("public.mv_data").expect("mv_data not found");

    assert!(
        pos_fn < pos_mv,
        "helper() must be created before mv_data (materialized view depends on function)"
    );
}

#[tokio::test]
async fn compare_drops_routines_in_reverse_dependency_order() {
    // Routine A calls Routine B; when both are dropped, A should be dropped first.
    let mut from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());

    let routine_b = Routine::new(
        "public".to_string(),
        Oid(1),
        "base_fn".to_string(),
        "sql".to_string(),
        "FUNCTION".to_string(),
        "integer".to_string(),
        "".to_string(),
        None,
        None,
        "SELECT 1;\n".to_string(),
    );

    let routine_a = Routine::new(
        "public".to_string(),
        Oid(2),
        "caller_fn".to_string(),
        "sql".to_string(),
        "FUNCTION".to_string(),
        "integer".to_string(),
        "".to_string(),
        None,
        None,
        "SELECT public.base_fn();\n".to_string(),
    );

    // Add in wrong order
    from_dump.routines.push(routine_b);
    from_dump.routines.push(routine_a);

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();

    let pos_caller = script
        .find("drop function if exists public.caller_fn")
        .expect("caller_fn drop not found");
    let pos_base = script
        .find("drop function if exists public.base_fn")
        .expect("base_fn drop not found");

    assert!(
        pos_caller < pos_base,
        "caller_fn (dependent) must be dropped before base_fn"
    );
}

#[tokio::test]
async fn tables_multilevel_partitions_created_in_depth_order() {
    // Hierarchy: grandparent (RANGE) -> parent_2023 (LIST, sub-partition) -> child_2023_a (leaf)
    let from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    // Level 0: grandparent partitioned by RANGE
    let mut grandparent = Table::new(
        "public".to_string(),
        "events".to_string(),
        "public".to_string(),
        "events".to_string(),
        "postgres".to_string(),
        None,
        vec![int_column("public", "events", "id", 1)],
        vec![],
        vec![],
        vec![],
        None,
    );
    grandparent.partition_key = Some("RANGE (id)".to_string());
    grandparent.hash();

    // Level 1: sub-partition parent (is both a partition child AND partitioned by LIST)
    let mut sub_parent = Table::new(
        "public".to_string(),
        "events_2023".to_string(),
        "public".to_string(),
        "events_2023".to_string(),
        "postgres".to_string(),
        None,
        vec![int_column("public", "events_2023", "id", 1)],
        vec![],
        vec![],
        vec![],
        None,
    );
    sub_parent.partition_of = Some("\"public\".\"events\"".to_string());
    sub_parent.partition_bound = Some("FOR VALUES FROM (2023) TO (2024)".to_string());
    sub_parent.partition_key = Some("LIST (id)".to_string());
    sub_parent.hash();

    // Level 2: leaf partition
    let mut leaf = Table::new(
        "public".to_string(),
        "events_2023_a".to_string(),
        "public".to_string(),
        "events_2023_a".to_string(),
        "postgres".to_string(),
        None,
        vec![int_column("public", "events_2023_a", "id", 1)],
        vec![],
        vec![],
        vec![],
        None,
    );
    leaf.partition_of = Some("\"public\".\"events_2023\"".to_string());
    leaf.partition_bound = Some("FOR VALUES IN (1)".to_string());
    leaf.hash();

    // Push in reverse order to stress the sorting
    to_dump.tables.push(leaf);
    to_dump.tables.push(grandparent);
    to_dump.tables.push(sub_parent);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();

    let pos_gp = script
        .find("create table public.events")
        .expect("grandparent not created");
    let pos_sp = script
        .find("create table public.events_2023 partition of")
        .expect("sub-partition parent not created");
    let pos_leaf = script
        .find("create table public.events_2023_a partition of")
        .expect("leaf partition not created");

    assert!(
        pos_gp < pos_sp,
        "grandparent must be created before sub-partition parent"
    );
    assert!(
        pos_sp < pos_leaf,
        "sub-partition parent must be created before leaf partition"
    );
}

#[tokio::test]
async fn tables_multilevel_partitions_dropped_in_reverse_depth_order() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());

    let mut grandparent = Table::new(
        "public".to_string(),
        "events".to_string(),
        "public".to_string(),
        "events".to_string(),
        "postgres".to_string(),
        None,
        vec![int_column("public", "events", "id", 1)],
        vec![],
        vec![],
        vec![],
        None,
    );
    grandparent.partition_key = Some("RANGE (id)".to_string());
    grandparent.hash();

    let mut sub_parent = Table::new(
        "public".to_string(),
        "events_2023".to_string(),
        "public".to_string(),
        "events_2023".to_string(),
        "postgres".to_string(),
        None,
        vec![int_column("public", "events_2023", "id", 1)],
        vec![],
        vec![],
        vec![],
        None,
    );
    sub_parent.partition_of = Some("public.events".to_string());
    sub_parent.partition_bound = Some("FOR VALUES FROM (2023) TO (2024)".to_string());
    sub_parent.partition_key = Some("LIST (id)".to_string());
    sub_parent.hash();

    let mut leaf = Table::new(
        "public".to_string(),
        "events_2023_a".to_string(),
        "public".to_string(),
        "events_2023_a".to_string(),
        "postgres".to_string(),
        None,
        vec![int_column("public", "events_2023_a", "id", 1)],
        vec![],
        vec![],
        vec![],
        None,
    );
    leaf.partition_of = Some("public.events_2023".to_string());
    leaf.partition_bound = Some("FOR VALUES IN (1)".to_string());
    leaf.hash();

    from_dump.tables.push(grandparent);
    from_dump.tables.push(sub_parent);
    from_dump.tables.push(leaf);

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();

    let pos_gp = script
        .find("drop table if exists public.events;")
        .expect("grandparent drop not found");
    let pos_sp = script
        .find("drop table if exists public.events_2023;")
        .expect("sub-partition parent drop not found");
    let pos_leaf = script
        .find("drop table if exists public.events_2023_a;")
        .expect("leaf partition drop not found");

    assert!(
        pos_leaf < pos_sp,
        "leaf must be dropped before sub-partition parent"
    );
    assert!(
        pos_sp < pos_gp,
        "sub-partition parent must be dropped before grandparent"
    );
}

#[tokio::test]
async fn serial_column_uses_serial_type_in_table_script() {
    // When a serial/bigserial column's sequence is skipped, the table script
    // should use serial/bigserial type instead of integer/bigint with nextval default.
    let from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    // serial column (integer + nextval)
    let serial_seq = Sequence::new(
        "test_schema".to_string(),
        "test_serial_id_seq".to_string(),
        "postgres".to_string(),
        "integer".to_string(),
        Some(1),
        Some(1),
        Some(2147483647),
        Some(1),
        false,
        Some(1),
        Some(1),
        Some("test_schema".to_string()),
        Some("test_serial".to_string()),
        Some("id".to_string()),
    );
    to_dump.sequences.push(serial_seq);

    let serial_col = TableColumn {
        catalog: "postgres".to_string(),
        schema: "test_schema".to_string(),
        table: "test_serial".to_string(),
        name: "id".to_string(),
        ordinal_position: 1,
        column_default: Some("nextval('test_schema.test_serial_id_seq'::regclass)".to_string()),
        is_nullable: false,
        data_type: "integer".to_string(),
        character_maximum_length: None,
        character_octet_length: None,
        numeric_precision: Some(32),
        numeric_precision_radix: Some(2),
        numeric_scale: Some(0),
        datetime_precision: None,
        interval_type: None,
        interval_precision: None,
        character_set_catalog: None,
        character_set_schema: None,
        character_set_name: None,
        collation_catalog: None,
        collation_schema: None,
        collation_name: None,
        domain_catalog: None,
        domain_schema: None,
        domain_name: None,
        udt_catalog: None,
        udt_schema: None,
        udt_name: None,
        scope_catalog: None,
        scope_schema: None,
        scope_name: None,
        maximum_cardinality: None,
        dtd_identifier: None,
        is_self_referencing: false,
        is_identity: false,
        identity_generation: None,
        identity_start: None,
        identity_increment: None,
        identity_maximum: None,
        identity_minimum: None,
        identity_cycle: false,
        is_generated: "NEVER".to_string(),
        generation_expression: None,
        generation_type: None,
        is_updatable: true,
        related_views: None,
        comment: None,
        storage: None,
        compression: None,
        statistics_target: None,
        acl: vec![],
        serial_type: None,
    };
    let serial_table = Table::new(
        "test_schema".to_string(),
        "test_serial".to_string(),
        "test_schema".to_string(),
        "test_serial".to_string(),
        "postgres".to_string(),
        None,
        vec![serial_col],
        vec![],
        vec![],
        vec![],
        None,
    );
    to_dump.tables.push(serial_table);

    // bigserial column (bigint + nextval)
    let bigserial_seq = Sequence::new(
        "test_schema".to_string(),
        "test_bigserial_id_seq".to_string(),
        "postgres".to_string(),
        "bigint".to_string(),
        Some(1),
        Some(1),
        Some(9223372036854775807),
        Some(1),
        false,
        Some(1),
        Some(1),
        Some("test_schema".to_string()),
        Some("test_bigserial".to_string()),
        Some("id".to_string()),
    );
    to_dump.sequences.push(bigserial_seq);

    let bigserial_col = TableColumn {
        catalog: "postgres".to_string(),
        schema: "test_schema".to_string(),
        table: "test_bigserial".to_string(),
        name: "id".to_string(),
        ordinal_position: 1,
        column_default: Some("nextval('test_schema.test_bigserial_id_seq'::regclass)".to_string()),
        is_nullable: false,
        data_type: "bigint".to_string(),
        character_maximum_length: None,
        character_octet_length: None,
        numeric_precision: Some(64),
        numeric_precision_radix: Some(2),
        numeric_scale: Some(0),
        datetime_precision: None,
        interval_type: None,
        interval_precision: None,
        character_set_catalog: None,
        character_set_schema: None,
        character_set_name: None,
        collation_catalog: None,
        collation_schema: None,
        collation_name: None,
        domain_catalog: None,
        domain_schema: None,
        domain_name: None,
        udt_catalog: None,
        udt_schema: None,
        udt_name: None,
        scope_catalog: None,
        scope_schema: None,
        scope_name: None,
        maximum_cardinality: None,
        dtd_identifier: None,
        is_self_referencing: false,
        is_identity: false,
        identity_generation: None,
        identity_start: None,
        identity_increment: None,
        identity_maximum: None,
        identity_minimum: None,
        identity_cycle: false,
        is_generated: "NEVER".to_string(),
        generation_expression: None,
        generation_type: None,
        is_updatable: true,
        related_views: None,
        comment: None,
        storage: None,
        compression: None,
        statistics_target: None,
        acl: vec![],
        serial_type: None,
    };
    let bigserial_table = Table::new(
        "test_schema".to_string(),
        "test_bigserial".to_string(),
        "test_schema".to_string(),
        "test_bigserial".to_string(),
        "postgres".to_string(),
        None,
        vec![bigserial_col],
        vec![],
        vec![],
        vec![],
        None,
    );
    to_dump.tables.push(bigserial_table);

    to_dump.schemas.push(crate::dump::schema::Schema::new(
        "test_schema".to_string(),
        "test_schema".to_string(),
        None,
    ));

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();

    // Sequences should be skipped
    assert!(
        script.contains("Skipping sequence test_schema.test_serial_id_seq"),
        "serial sequence should be skipped"
    );
    assert!(
        script.contains("Skipping sequence test_schema.test_bigserial_id_seq"),
        "bigserial sequence should be skipped"
    );
    assert!(
        !script.contains("create sequence test_schema.test_serial_id_seq"),
        "serial sequence should not be created separately"
    );
    assert!(
        !script.contains("create sequence test_schema.test_bigserial_id_seq"),
        "bigserial sequence should not be created separately"
    );

    // Table columns should use serial/bigserial types
    assert!(
        script.contains("id serial"),
        "serial column should use 'serial' type, got:\n{script}"
    );
    assert!(
        script.contains("id bigserial"),
        "bigserial column should use 'bigserial' type, got:\n{script}"
    );

    // Should NOT contain nextval defaults for these columns
    assert!(
        !script.contains("nextval('test_schema.test_serial_id_seq'"),
        "serial column should not have explicit nextval default"
    );
    assert!(
        !script.contains("nextval('test_schema.test_bigserial_id_seq'"),
        "bigserial column should not have explicit nextval default"
    );
}

#[tokio::test]
async fn use_single_transaction_should_add_begin_commit() {
    let from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut new_table = Table::new(
        "public".to_string(),
        "\"my-table\"".to_string(),
        "public".to_string(),
        "my-table".to_string(),
        "postgres".to_string(),
        None,
        vec![int_column("public", "\"my-table\"", "id", 1)],
        vec![],
        vec![],
        vec![],
        None,
    );

    new_table.hash();

    to_dump.tables.push(new_table);

    let mut comparer = Comparer::new(from_dump, to_dump, false, true, true, GrantsMode::Ignore);

    comparer.compare().await.unwrap();

    let script = comparer.get_script();

    const SCRIPT_BODY_START_PATTERN: &str = "*/\n\n";

    let script_body_start_index = script
        .find(SCRIPT_BODY_START_PATTERN)
        .map(|index| index + SCRIPT_BODY_START_PATTERN.len())
        .expect("Script header was not found");

    let script_body = &script[script_body_start_index..];

    assert!(script_body.starts_with("begin;\n\n"));
    assert!(script_body.ends_with("\ncommit;"));
}

#[tokio::test]
async fn use_comments_false_strips_block_and_line_comments() {
    let from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());
    let mut comparer = Comparer::new(from_dump, to_dump, false, false, false, GrantsMode::Ignore);
    comparer.script =
        "/* header comment */\nCREATE TABLE t1 (id int); -- inline comment\n/* trailing */\n"
            .to_string();
    let result = comparer.get_script();
    assert_eq!(result, "CREATE TABLE t1 (id int);\n");
}

#[tokio::test]
async fn use_comments_false_strips_singly_nested_block_comment() {
    let from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());
    let mut comparer = Comparer::new(from_dump, to_dump, false, false, false, GrantsMode::Ignore);
    // /* outer /* inner */ still outer */ must all be stripped.
    comparer.script = "SELECT /* outer /* inner */ still outer */ 1;\n".to_string();
    let result = comparer.get_script();
    assert_eq!(result, "SELECT  1;\n");
}

#[tokio::test]
async fn use_comments_false_strips_deeply_nested_block_comment() {
    let from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());
    let mut comparer = Comparer::new(from_dump, to_dump, false, false, false, GrantsMode::Ignore);
    // Three levels of nesting.
    comparer.script = "SELECT /* a /* b /* c */ b */ a */ 1;\n".to_string();
    let result = comparer.get_script();
    assert_eq!(result, "SELECT  1;\n");
}

#[tokio::test]
async fn use_comments_false_strips_adjacent_nested_block_comments() {
    let from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());
    let mut comparer = Comparer::new(from_dump, to_dump, false, false, false, GrantsMode::Ignore);
    // Two independent outer comments each with their own inner comment.
    // Any space left before SELECT after stripping the first comment is removed by get_script()'s trim().
    comparer.script = "/* a /* b */ a */ SELECT /* c /* d */ c */ 1;\n".to_string();
    let result = comparer.get_script();
    assert_eq!(result, "SELECT  1;\n");
}

#[tokio::test]
async fn use_comments_false_nested_block_comment_before_statement() {
    let from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());
    let mut comparer = Comparer::new(from_dump, to_dump, false, false, false, GrantsMode::Ignore);
    // Nested comment as a header; SQL that follows must be preserved intact.
    comparer.script = "/* header /* nested */ end */\nCREATE TABLE t (id int);\n".to_string();
    let result = comparer.get_script();
    assert_eq!(result, "CREATE TABLE t (id int);\n");
}

#[tokio::test]
async fn use_comments_false_nested_comment_only_script_returns_empty() {
    let from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());
    let mut comparer = Comparer::new(from_dump, to_dump, false, false, false, GrantsMode::Ignore);
    // A script consisting only of a nested block comment produces no output.
    comparer.script = "/* outer /* inner */ outer */\n".to_string();
    let result = comparer.get_script();
    assert_eq!(result, "");
}

#[tokio::test]
async fn use_comments_false_nested_comment_sql_between_levels() {
    let from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());
    let mut comparer = Comparer::new(from_dump, to_dump, false, false, false, GrantsMode::Ignore);
    // Text that sits between the outer /* and its matching */ must be stripped
    // even when inner comment pairs appear in the middle.
    comparer.script = "SELECT /* before /* mid */ after */ 42;\n".to_string();
    let result = comparer.get_script();
    assert_eq!(result, "SELECT  42;\n");
}

#[tokio::test]
async fn use_comments_false_nested_comment_immediately_after_keyword() {
    let from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());
    let mut comparer = Comparer::new(from_dump, to_dump, false, false, false, GrantsMode::Ignore);
    // No space between the keyword and the nested comment; scanner must not
    // be confused by the /* that immediately follows non-comment text.
    comparer.script = "SELECT/* /* nested */ */1;\n".to_string();
    let result = comparer.get_script();
    assert_eq!(result, "SELECT1;\n");
}

#[tokio::test]
async fn use_comments_false_nested_comment_followed_by_line_comment() {
    let from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());
    let mut comparer = Comparer::new(from_dump, to_dump, false, false, false, GrantsMode::Ignore);
    // After the nested block comment closes, a line comment on the same line
    // must also be stripped.
    comparer.script = "SELECT /* a /* b */ a */ 1; -- strip me\n".to_string();
    let result = comparer.get_script();
    assert_eq!(result, "SELECT  1;\n");
}

#[tokio::test]
async fn use_comments_false_nested_comment_inside_single_quoted_string_not_stripped() {
    let from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());
    let mut comparer = Comparer::new(from_dump, to_dump, false, false, false, GrantsMode::Ignore);
    // Nested-comment-like sequences inside a string literal must be preserved.
    comparer.script = "SELECT '/* outer /* inner */ outer */' AS val;\n".to_string();
    let result = comparer.get_script();
    assert_eq!(result, "SELECT '/* outer /* inner */ outer */' AS val;\n");
}

#[tokio::test]
async fn use_comments_false_nested_comment_inside_e_string_not_stripped() {
    let from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());
    let mut comparer = Comparer::new(from_dump, to_dump, false, false, false, GrantsMode::Ignore);
    // Nested-comment-like sequences inside an E-string must also be preserved.
    comparer.script = "SELECT E'/* outer /* inner */ outer */' AS val;\n".to_string();
    let result = comparer.get_script();
    assert_eq!(result, "SELECT E'/* outer /* inner */ outer */' AS val;\n");
}

#[tokio::test]
async fn use_comments_false_nested_comment_inside_double_quoted_identifier_not_stripped() {
    let from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());
    let mut comparer = Comparer::new(from_dump, to_dump, false, false, false, GrantsMode::Ignore);
    // Nested-comment-like sequences inside a double-quoted identifier must be preserved.
    comparer.script = "SELECT 1 AS \"/* outer /* inner */ outer */\";\n".to_string();
    let result = comparer.get_script();
    assert_eq!(result, "SELECT 1 AS \"/* outer /* inner */ outer */\";\n");
}

#[tokio::test]
async fn use_comments_false_preserves_dollar_quoted_comments() {
    let from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());
    let mut comparer = Comparer::new(from_dump, to_dump, false, false, false, GrantsMode::Ignore);
    comparer.script = "CREATE FUNCTION f() RETURNS void AS $$\n-- inside body\n/* also inside */\n$$ LANGUAGE plpgsql;\n".to_string();
    let result = comparer.get_script();
    assert_eq!(
        result,
        "CREATE FUNCTION f() RETURNS void AS $$\n-- inside body\n/* also inside */\n$$ LANGUAGE plpgsql;\n"
    );
}

#[tokio::test]
async fn use_comments_false_preserves_named_dollar_tag_comments() {
    let from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());
    let mut comparer = Comparer::new(from_dump, to_dump, false, false, false, GrantsMode::Ignore);
    comparer.script =
        "CREATE FUNCTION g() RETURNS void AS $body$\n-- comment inside\n$body$ LANGUAGE plpgsql;\n"
            .to_string();
    let result = comparer.get_script();
    assert_eq!(
        result,
        "CREATE FUNCTION g() RETURNS void AS $body$\n-- comment inside\n$body$ LANGUAGE plpgsql;\n"
    );
}

#[tokio::test]
async fn use_comments_false_preserves_single_quoted_comments() {
    let from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());
    let mut comparer = Comparer::new(from_dump, to_dump, false, false, false, GrantsMode::Ignore);
    comparer.script = "SELECT '-- not a comment' AS val, '/* also not */' AS val2;\n".to_string();
    let result = comparer.get_script();
    assert_eq!(
        result,
        "SELECT '-- not a comment' AS val, '/* also not */' AS val2;\n"
    );
}

#[tokio::test]
async fn use_comments_false_preserves_e_string_backslash_escaped_quote() {
    let from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());
    let mut comparer = Comparer::new(from_dump, to_dump, false, false, false, GrantsMode::Ignore);
    // \' inside E'...' is an escaped quote and must NOT terminate the string.
    // The comment-like content after it must be preserved, not stripped.
    comparer.script = "SELECT E'it\\'s fine -- not a comment' AS val; -- strip\n".to_string();
    let result = comparer.get_script();
    assert_eq!(result, "SELECT E'it\\'s fine -- not a comment' AS val;\n");
}

#[tokio::test]
async fn use_comments_false_preserves_e_string_block_comment_lookalike() {
    let from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());
    let mut comparer = Comparer::new(from_dump, to_dump, false, false, false, GrantsMode::Ignore);
    // /* ... */ inside an E-string must not be treated as a block comment.
    comparer.script = "SELECT E'/* not a comment */' AS val; /* strip */\n".to_string();
    let result = comparer.get_script();
    assert_eq!(result, "SELECT E'/* not a comment */' AS val;\n");
}

#[tokio::test]
async fn use_comments_false_preserves_e_string_backslash_backslash_then_quote() {
    let from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());
    let mut comparer = Comparer::new(from_dump, to_dump, false, false, false, GrantsMode::Ignore);
    // \\\' is an escaped backslash (\\) followed by an escaped quote (\').
    // The string should continue after that sequence.
    comparer.script =
        "SELECT E'backslash\\\\\\'quote -- still inside' AS val; -- strip\n".to_string();
    let result = comparer.get_script();
    assert_eq!(
        result,
        "SELECT E'backslash\\\\\\'quote -- still inside' AS val;\n"
    );
}

#[tokio::test]
async fn use_comments_false_preserves_e_string_doubled_quote_escape() {
    let from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());
    let mut comparer = Comparer::new(from_dump, to_dump, false, false, false, GrantsMode::Ignore);
    // '' inside E'...' is also a valid quote escape; must not terminate string early.
    comparer.script = "SELECT E'it''s fine -- not a comment' AS val; -- strip\n".to_string();
    let result = comparer.get_script();
    assert_eq!(result, "SELECT E'it''s fine -- not a comment' AS val;\n");
}

#[tokio::test]
async fn use_comments_false_preserves_lowercase_e_string() {
    let from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());
    let mut comparer = Comparer::new(from_dump, to_dump, false, false, false, GrantsMode::Ignore);
    // Lowercase e'...' prefix must be handled identically to E'...'.
    comparer.script = "SELECT e'it\\'s fine -- not a comment' AS val; -- strip\n".to_string();
    let result = comparer.get_script();
    assert_eq!(result, "SELECT e'it\\'s fine -- not a comment' AS val;\n");
}

#[tokio::test]
async fn use_comments_false_does_not_treat_standalone_e_as_e_string() {
    let from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());
    let mut comparer = Comparer::new(from_dump, to_dump, false, false, false, GrantsMode::Ignore);
    // A bare column/alias named "e" followed immediately by a plain string
    // literal must not be misidentified as an E-string prefix.
    // Here "e" is a table alias and 'text' is a separate literal.
    comparer.script = "SELECT e, 'text -- not a comment' FROM t; -- strip\n".to_string();
    let result = comparer.get_script();
    assert_eq!(result, "SELECT e, 'text -- not a comment' FROM t;\n");
}

#[tokio::test]
async fn use_comments_false_does_not_treat_uppercase_e_identifier_as_e_string() {
    let from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());
    let mut comparer = Comparer::new(from_dump, to_dump, false, false, false, GrantsMode::Ignore);
    // Uppercase E as a standalone identifier (column name), not followed by
    // a quote, must not be confused with an E-string prefix.
    comparer.script = "SELECT E FROM t; -- strip\n".to_string();
    let result = comparer.get_script();
    assert_eq!(result, "SELECT E FROM t;\n");
}

#[tokio::test]
async fn use_comments_false_preserves_empty_e_string() {
    let from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());
    let mut comparer = Comparer::new(from_dump, to_dump, false, false, false, GrantsMode::Ignore);
    // An empty E-string E'' must not confuse the state machine.
    comparer.script = "SELECT E'' AS val; /* strip */\n".to_string();
    let result = comparer.get_script();
    assert_eq!(result, "SELECT E'' AS val;\n");
}

#[tokio::test]
async fn use_comments_false_preserves_e_string_with_other_backslash_sequences() {
    let from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());
    let mut comparer = Comparer::new(from_dump, to_dump, false, false, false, GrantsMode::Ignore);
    // \n and \t are everyday escape sequences; the scanner must copy them
    // verbatim and must not mistake the character after the backslash for
    // anything other than the second byte of the pair.
    comparer.script =
        "SELECT E'line1\\nline2\\ttabbed -- not a comment' AS val; -- strip\n".to_string();
    let result = comparer.get_script();
    assert_eq!(
        result,
        "SELECT E'line1\\nline2\\ttabbed -- not a comment' AS val;\n"
    );
}

#[tokio::test]
async fn use_comments_false_preserves_multiple_e_strings_in_one_statement() {
    let from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());
    let mut comparer = Comparer::new(from_dump, to_dump, false, false, false, GrantsMode::Ignore);
    // Multiple E-strings in one statement; real trailing comment stripped.
    comparer.script = "INSERT INTO t VALUES (E'val\\'1 -- x', E'val/*2*/'); -- strip\n".to_string();
    let result = comparer.get_script();
    assert_eq!(
        result,
        "INSERT INTO t VALUES (E'val\\'1 -- x', E'val/*2*/');\n"
    );
}

#[tokio::test]
async fn use_comments_false_preserves_e_string_adjacent_to_double_quoted_identifier() {
    let from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());
    let mut comparer = Comparer::new(from_dump, to_dump, false, false, false, GrantsMode::Ignore);
    // E-string and double-quoted identifier in the same statement; both
    // preserved, real trailing comment stripped.
    comparer.script =
        "INSERT INTO \"my--table\" (col) VALUES (E'it\\'s -- ok'); -- strip\n".to_string();
    let result = comparer.get_script();
    assert_eq!(
        result,
        "INSERT INTO \"my--table\" (col) VALUES (E'it\\'s -- ok');\n"
    );
}

#[tokio::test]
async fn use_comments_false_preserves_double_quoted_identifier_comments() {
    let from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());
    let mut comparer = Comparer::new(from_dump, to_dump, false, false, false, GrantsMode::Ignore);
    // Double-quoted identifiers containing sequences that look like comment
    // starters must be passed through verbatim and must NOT be stripped.
    comparer.script =
        "SELECT 1 AS \"col--name\", 2 AS \"/*not a comment*/\"; -- real comment\n".to_string();
    let result = comparer.get_script();
    assert_eq!(
        result,
        "SELECT 1 AS \"col--name\", 2 AS \"/*not a comment*/\";\n"
    );
}

#[tokio::test]
async fn use_comments_false_preserves_double_quoted_identifier_with_escaped_quote() {
    let from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());
    let mut comparer = Comparer::new(from_dump, to_dump, false, false, false, GrantsMode::Ignore);
    // A doubled double-quote inside a quoted identifier is an escape sequence
    // and must survive comment stripping intact.
    comparer.script =
        "ALTER TABLE t RENAME COLUMN \"col\"\"--name\" TO new_name; /* drop this */\n".to_string();
    let result = comparer.get_script();
    assert_eq!(
        result,
        "ALTER TABLE t RENAME COLUMN \"col\"\"--name\" TO new_name;\n"
    );
}

#[tokio::test]
async fn use_comments_false_preserves_multiple_double_quoted_identifiers() {
    let from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());
    let mut comparer = Comparer::new(from_dump, to_dump, false, false, false, GrantsMode::Ignore);
    // Several double-quoted identifiers in one statement, each containing
    // comment-like sequences; only the trailing real comment should be stripped.
    comparer.script = "SELECT \"a--b\", \"c/*d*/e\", \"f--g\" FROM t; -- strip me\n".to_string();
    let result = comparer.get_script();
    assert_eq!(result, "SELECT \"a--b\", \"c/*d*/e\", \"f--g\" FROM t;\n");
}

#[tokio::test]
async fn use_comments_false_preserves_qualified_double_quoted_name() {
    let from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());
    let mut comparer = Comparer::new(from_dump, to_dump, false, false, false, GrantsMode::Ignore);
    // Quoted schema + quoted table, both containing comment-like sequences.
    comparer.script =
        "CREATE TABLE \"my--schema\".\"my/*table*/\" (id int); /* strip */\n".to_string();
    let result = comparer.get_script();
    assert_eq!(
        result,
        "CREATE TABLE \"my--schema\".\"my/*table*/\" (id int);\n"
    );
}

#[tokio::test]
async fn use_comments_false_preserves_empty_double_quoted_identifier() {
    let from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());
    let mut comparer = Comparer::new(from_dump, to_dump, false, false, false, GrantsMode::Ignore);
    // "" is a valid (if unusual) quoted identifier; must not confuse the state machine.
    comparer.script = "ALTER INDEX \"\" RENAME TO x; /* strip */\n".to_string();
    let result = comparer.get_script();
    assert_eq!(result, "ALTER INDEX \"\" RENAME TO x;\n");
}

#[tokio::test]
async fn use_comments_false_strips_comment_after_double_quoted_identifier() {
    let from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());
    let mut comparer = Comparer::new(from_dump, to_dump, false, false, false, GrantsMode::Ignore);
    // The parser must exit the double-quote state correctly so the real block
    // comment that follows is still stripped.
    comparer.script = "SELECT \"col\" /* strip this */ FROM t;\n".to_string();
    let result = comparer.get_script();
    assert_eq!(result, "SELECT \"col\"  FROM t;\n");
}

#[tokio::test]
async fn use_comments_false_mixed_double_and_single_quoted_with_comment() {
    let from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());
    let mut comparer = Comparer::new(from_dump, to_dump, false, false, false, GrantsMode::Ignore);
    // Double-quoted identifier and single-quoted string both containing
    // comment-like bytes; trailing real comment must still be stripped.
    comparer.script =
        "INSERT INTO \"my--table\" (col) VALUES ('/* not */ a -- val'); -- strip\n".to_string();
    let result = comparer.get_script();
    assert_eq!(
        result,
        "INSERT INTO \"my--table\" (col) VALUES ('/* not */ a -- val');\n"
    );
}

#[tokio::test]
async fn use_comments_false_returns_empty_for_comment_only_script() {
    let from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());
    let mut comparer = Comparer::new(from_dump, to_dump, false, false, false, GrantsMode::Ignore);
    comparer.script = "/* only a comment */\n-- another comment\n".to_string();
    let result = comparer.get_script();
    assert_eq!(result, "");
}

#[tokio::test]
async fn use_comments_false_collapses_excess_newlines() {
    let from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());
    let mut comparer = Comparer::new(from_dump, to_dump, false, false, false, GrantsMode::Ignore);
    comparer.script =
        "CREATE TABLE t1 (id int);\n/* removed */\n\n\n\nCREATE TABLE t2 (id int);\n".to_string();
    let result = comparer.get_script();
    assert_eq!(
        result,
        "CREATE TABLE t1 (id int);\n\nCREATE TABLE t2 (id int);\n"
    );
}

#[tokio::test]
async fn use_comments_true_preserves_all_comments() {
    let from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());
    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Ignore);
    comparer.script = "/* header */\nCREATE TABLE t1 (id int); -- inline\n".to_string();
    let result = comparer.get_script();
    assert_eq!(
        result,
        "/* header */\nCREATE TABLE t1 (id int); -- inline\n"
    );
}

#[tokio::test]
async fn use_comments_false_preserves_utf8() {
    let from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());
    let mut comparer = Comparer::new(from_dump, to_dump, false, false, false, GrantsMode::Ignore);
    comparer.script =
        "/* comment */\nCOMMENT ON TABLE t IS '数据表 — 描述';\nSELECT $$函数体$$;\n".to_string();
    let result = comparer.get_script();
    assert_eq!(
        result,
        "COMMENT ON TABLE t IS '数据表 — 描述';\nSELECT $$函数体$$;\n"
    );
}

/// Partition child must not be dropped+recreated when a non-partition-key
/// column changes type on the parent.
#[tokio::test]
async fn partition_child_non_pk_col_type_change_no_recreate() {
    fn numeric_column(
        schema: &str,
        table: &str,
        name: &str,
        ordinal: i32,
        precision: i32,
        scale: i32,
    ) -> TableColumn {
        let mut col = int_column(schema, table, name, ordinal);
        col.data_type = "numeric".to_string();
        col.numeric_precision = Some(precision);
        col.numeric_scale = Some(scale);
        col.numeric_precision_radix = Some(10);
        col
    }
    fn date_column(schema: &str, table: &str, name: &str, ordinal: i32) -> TableColumn {
        let mut col = int_column(schema, table, name, ordinal);
        col.data_type = "date".to_string();
        col.numeric_precision = None;
        col.numeric_precision_radix = None;
        col.numeric_scale = None;
        col
    }
    fn bigint_column(schema: &str, table: &str, name: &str, ordinal: i32) -> TableColumn {
        let mut col = int_column(schema, table, name, ordinal);
        col.data_type = "bigint".to_string();
        col.numeric_precision = Some(64);
        col
    }

    let tbl = "s6_issue2_expenses";
    let child_tbl = "s6_issue2_expenses_2024_01";
    let schema = "\"pt_test\"";

    // --- FROM dump ---
    let mut from_parent = Table::new(
        schema.to_string(),
        tbl.to_string(),
        "pt_test".to_string(),
        tbl.to_string(),
        "postgres".to_string(),
        None,
        vec![
            bigint_column(schema, tbl, "id", 1),
            date_column(schema, tbl, "expense_date", 2),
            numeric_column(schema, tbl, "amount", 3, 10, 2),
        ],
        vec![],
        vec![],
        vec![],
        None,
    );
    from_parent.partition_key = Some("RANGE (expense_date)".to_string());
    from_parent.hash();

    let mut from_child = Table::new(
        schema.to_string(),
        child_tbl.to_string(),
        "pt_test".to_string(),
        child_tbl.to_string(),
        "postgres".to_string(),
        None,
        vec![
            bigint_column(schema, child_tbl, "id", 1),
            date_column(schema, child_tbl, "expense_date", 2),
            numeric_column(schema, child_tbl, "amount", 3, 10, 2),
        ],
        vec![],
        vec![],
        vec![],
        None,
    );
    from_child.partition_of = Some(format!("{}.{}", schema, tbl));
    from_child.partition_bound =
        Some("FOR VALUES FROM ('2024-01-01') TO ('2024-02-01')".to_string());
    from_child.hash();

    // --- TO dump ---
    let mut to_parent = Table::new(
        schema.to_string(),
        tbl.to_string(),
        "pt_test".to_string(),
        tbl.to_string(),
        "postgres".to_string(),
        None,
        vec![
            bigint_column(schema, tbl, "id", 1),
            date_column(schema, tbl, "expense_date", 2),
            numeric_column(schema, tbl, "amount", 3, 15, 4),
        ],
        vec![],
        vec![],
        vec![],
        None,
    );
    to_parent.partition_key = Some("RANGE (expense_date)".to_string());
    to_parent.hash();

    let mut to_child = Table::new(
        schema.to_string(),
        child_tbl.to_string(),
        "pt_test".to_string(),
        child_tbl.to_string(),
        "postgres".to_string(),
        None,
        vec![
            bigint_column(schema, child_tbl, "id", 1),
            date_column(schema, child_tbl, "expense_date", 2),
            numeric_column(schema, child_tbl, "amount", 3, 15, 4),
        ],
        vec![],
        vec![],
        vec![],
        None,
    );
    to_child.partition_of = Some(format!("{}.{}", schema, tbl));
    to_child.partition_bound = Some("FOR VALUES FROM ('2024-01-01') TO ('2024-02-01')".to_string());
    to_child.hash();

    let mut from_dump = Dump::new(DumpConfig::default());
    from_dump.tables.push(from_parent);
    from_dump.tables.push(from_child);

    let mut to_dump = Dump::new(DumpConfig::default());
    to_dump.tables.push(to_parent);
    to_dump.tables.push(to_child);

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_tables().await.unwrap();
    let script = comparer.get_script();

    // Parent should get ALTER COLUMN
    assert!(
        script.contains("alter column"),
        "Parent must get ALTER COLUMN for amount, got: {script}"
    );
    // Child should NOT be dropped
    assert!(
        !script.contains("drop table"),
        "Partition child must not be dropped for non-partition-key column type change, got: {script}"
    );
    // Child should NOT be recreated
    assert!(
        !script.to_lowercase().contains(&format!(
            "create table {}.{} partition of",
            schema, child_tbl
        )),
        "Partition child must not be recreated, got: {script}"
    );
    assert!(
        !script.contains("Data loss"),
        "No data loss warning expected, got: {script}"
    );
}

// =========================================================================
// compare_grants tests
// =========================================================================

#[tokio::test]
async fn compare_grants_ignore_mode_produces_no_output() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_schema = Schema::new("public".to_string(), "public".to_string(), None);
    from_schema.acl = vec!["reader=U/owner".to_string()];
    let mut to_schema = Schema::new("public".to_string(), "public".to_string(), None);
    to_schema.acl = vec!["reader=U/owner".to_string(), "writer=UC/owner".to_string()];

    from_dump.schemas.push(from_schema);
    to_dump.schemas.push(to_schema);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Ignore);
    comparer.compare_grants().await.unwrap();
    let script = comparer.get_script();

    assert!(
        !script.contains("GRANT"),
        "Ignore mode must not emit GRANT, got: {script}"
    );
    assert!(
        !script.contains("REVOKE"),
        "Ignore mode must not emit REVOKE, got: {script}"
    );
}

#[tokio::test]
async fn compare_grants_addonly_adds_missing_schema_grant() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_schema = Schema::new("myschema".to_string(), "myschema".to_string(), None);
    from_schema.acl = vec!["reader=U/owner".to_string()];
    let mut to_schema = Schema::new("myschema".to_string(), "myschema".to_string(), None);
    to_schema.acl = vec!["reader=U/owner".to_string(), "writer=UC/owner".to_string()];

    from_dump.schemas.push(from_schema);
    to_dump.schemas.push(to_schema);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::AddOnly);
    comparer.compare_grants().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("GRANT CREATE, USAGE ON SCHEMA myschema TO writer;"),
        "AddOnly must add missing grant, got: {script}"
    );
    assert!(
        !script.contains("REVOKE"),
        "AddOnly must not emit REVOKE, got: {script}"
    );
}

#[tokio::test]
async fn compare_grants_addonly_does_not_revoke_removed_grant() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_schema = Schema::new("myschema".to_string(), "myschema".to_string(), None);
    from_schema.acl = vec!["reader=U/owner".to_string(), "writer=UC/owner".to_string()];
    let mut to_schema = Schema::new("myschema".to_string(), "myschema".to_string(), None);
    to_schema.acl = vec!["reader=U/owner".to_string()];

    from_dump.schemas.push(from_schema);
    to_dump.schemas.push(to_schema);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::AddOnly);
    comparer.compare_grants().await.unwrap();
    let script = comparer.get_script();

    assert!(
        !script.contains("REVOKE"),
        "AddOnly must not revoke, got: {script}"
    );
}

#[tokio::test]
async fn compare_grants_full_revokes_removed_schema_grant() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_schema = Schema::new("myschema".to_string(), "myschema".to_string(), None);
    from_schema.acl = vec!["reader=U/owner".to_string(), "writer=UC/owner".to_string()];
    let mut to_schema = Schema::new("myschema".to_string(), "myschema".to_string(), None);
    to_schema.acl = vec!["reader=U/owner".to_string()];

    from_dump.schemas.push(from_schema);
    to_dump.schemas.push(to_schema);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Full);
    comparer.compare_grants().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("REVOKE CREATE, USAGE ON SCHEMA myschema FROM writer;"),
        "Full mode must revoke removed grant, got: {script}"
    );
}

#[tokio::test]
async fn compare_grants_full_table_add_and_revoke() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_table = Table::new(
        "public".to_string(),
        "users".to_string(),
        "public".to_string(),
        "users".to_string(),
        "owner".to_string(),
        None,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        None,
    );
    from_table.acl = vec!["reader=r/owner".to_string()];

    let mut to_table = Table::new(
        "public".to_string(),
        "users".to_string(),
        "public".to_string(),
        "users".to_string(),
        "owner".to_string(),
        None,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        None,
    );
    to_table.acl = vec!["writer=rw/owner".to_string()];

    from_dump.tables.push(from_table);
    to_dump.tables.push(to_table);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Full);
    comparer.compare_grants().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("GRANT SELECT, UPDATE ON TABLE public.users TO writer;"),
        "Full must add new grant, got: {script}"
    );
    assert!(
        script.contains("REVOKE SELECT ON TABLE public.users FROM reader;"),
        "Full must revoke removed grant, got: {script}"
    );
}

#[tokio::test]
async fn compare_grants_sequence() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_seq = Sequence::new(
        "public".to_string(),
        "my_seq".to_string(),
        "owner".to_string(),
        "bigint".to_string(),
        Some(1),
        Some(1),
        Some(9223372036854775807),
        Some(1),
        false,
        Some(1),
        Some(1),
        None,
        None,
        None,
    );
    from_seq.acl = vec![];

    let mut to_seq = Sequence::new(
        "public".to_string(),
        "my_seq".to_string(),
        "owner".to_string(),
        "bigint".to_string(),
        Some(1),
        Some(1),
        Some(9223372036854775807),
        Some(1),
        false,
        Some(1),
        Some(1),
        None,
        None,
        None,
    );
    to_seq.acl = vec!["reader=U/owner".to_string()];

    from_dump.sequences.push(from_seq);
    to_dump.sequences.push(to_seq);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::AddOnly);
    comparer.compare_grants().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("GRANT USAGE ON SEQUENCE public.my_seq TO reader;"),
        "Must add sequence grant, got: {script}"
    );
}

#[tokio::test]
async fn compare_grants_view() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_view = View::new(
        "my_view".to_string(),
        "SELECT 1".to_string(),
        "public".to_string(),
        Vec::new(),
    );
    from_view.acl = vec!["reader=r/owner".to_string()];

    let mut to_view = View::new(
        "my_view".to_string(),
        "SELECT 1".to_string(),
        "public".to_string(),
        Vec::new(),
    );
    to_view.acl = vec!["reader=r/owner".to_string(), "writer=rw/owner".to_string()];

    from_dump.views.push(from_view);
    to_dump.views.push(to_view);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Full);
    comparer.compare_grants().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("GRANT SELECT, UPDATE ON TABLE public.my_view TO writer;"),
        "Must add view grant, got: {script}"
    );
    assert!(
        !script.contains("REVOKE"),
        "No revoke expected when unchanged grant remains, got: {script}"
    );
}

#[tokio::test]
async fn compare_grants_dropped_view_restores_all_grants() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    // Both FROM and TO have the same grant on the view.
    let mut from_view = View::new(
        "my_view".to_string(),
        "SELECT 1".to_string(),
        "public".to_string(),
        Vec::new(),
    );
    from_view.acl = vec!["reader=r/owner".to_string()];

    let mut to_view = View::new(
        "my_view".to_string(),
        "SELECT 1".to_string(),
        "public".to_string(),
        Vec::new(),
    );
    to_view.acl = vec!["reader=r/owner".to_string()];

    from_dump.views.push(from_view);
    to_dump.views.push(to_view);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Full);
    // Simulate that the view was dropped earlier in the script
    // (e.g. as a dependency of an altered table).
    comparer
        .dropped_views
        .insert(Comparer::normalized_view_key("public", "my_view"), true);
    comparer.compare_grants().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("GRANT SELECT ON TABLE public.my_view TO reader;"),
        "Dropped view must restore grants even when FROM has the same ACL, got: {script}"
    );
}

/// When a view's DROP was only commented out (use_drop=false), the view still
/// exists in the database.  compare_grants must keep the original from_acl so
/// that identical ACLs produce no diff (no redundant GRANTs/REVOKEs).
#[tokio::test]
async fn compare_grants_commented_drop_keeps_from_acl() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_view = View::new(
        "my_view".to_string(),
        "SELECT 1".to_string(),
        "public".to_string(),
        Vec::new(),
    );
    from_view.acl = vec!["reader=r/owner".to_string()];

    let mut to_view = View::new(
        "my_view".to_string(),
        "SELECT 1".to_string(),
        "public".to_string(),
        Vec::new(),
    );
    to_view.acl = vec!["reader=r/owner".to_string()];

    from_dump.views.push(from_view);
    to_dump.views.push(to_view);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Full);
    // Simulate a commented-out drop (use_drop=false → stored as false).
    comparer
        .dropped_views
        .insert(Comparer::normalized_view_key("public", "my_view"), false);
    comparer.compare_grants().await.unwrap();
    let script = comparer.get_script();

    // ACLs are identical and the view was NOT actually dropped,
    // so no GRANT/REVOKE should appear.
    let has_grant_stmt = script
        .lines()
        .any(|l| l.trim_start().to_lowercase().starts_with("grant "));
    assert!(
        !has_grant_stmt,
        "Commented-out drop must not cause redundant GRANTs, got: {script}"
    );
    let has_revoke_stmt = script
        .lines()
        .any(|l| l.trim_start().to_lowercase().starts_with("revoke "));
    assert!(
        !has_revoke_stmt,
        "Commented-out drop must not cause redundant REVOKEs, got: {script}"
    );
}

#[tokio::test]
async fn compare_grants_routine_function() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_routine = Routine::new(
        "public".to_string(),
        Oid(1),
        "my_func".to_string(),
        "plpgsql".to_string(),
        "function".to_string(),
        "void".to_string(),
        "".to_string(),
        None,
        None,
        "BEGIN END".to_string(),
    );
    from_routine.acl = vec![];

    let mut to_routine = Routine::new(
        "public".to_string(),
        Oid(1),
        "my_func".to_string(),
        "plpgsql".to_string(),
        "function".to_string(),
        "void".to_string(),
        "".to_string(),
        None,
        None,
        "BEGIN END".to_string(),
    );
    to_routine.acl = vec!["app=X/owner".to_string()];

    from_dump.routines.push(from_routine);
    to_dump.routines.push(to_routine);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::AddOnly);
    comparer.compare_grants().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("GRANT EXECUTE ON FUNCTION public.my_func() TO app;"),
        "Must add function grant, got: {script}"
    );
}

#[tokio::test]
async fn compare_grants_routine_procedure() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_routine = Routine::new(
        "public".to_string(),
        Oid(2),
        "my_proc".to_string(),
        "plpgsql".to_string(),
        "procedure".to_string(),
        "void".to_string(),
        "days integer".to_string(),
        None,
        None,
        "BEGIN END".to_string(),
    );
    from_routine.acl = vec!["app=X/owner".to_string()];

    let mut to_routine = Routine::new(
        "public".to_string(),
        Oid(2),
        "my_proc".to_string(),
        "plpgsql".to_string(),
        "procedure".to_string(),
        "void".to_string(),
        "days integer".to_string(),
        None,
        None,
        "BEGIN END".to_string(),
    );
    to_routine.acl = vec![];

    from_dump.routines.push(from_routine);
    to_dump.routines.push(to_routine);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Full);
    comparer.compare_grants().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("REVOKE EXECUTE ON PROCEDURE public.my_proc(days integer) FROM app;"),
        "Full must revoke removed procedure grant, got: {script}"
    );
}

#[tokio::test]
async fn compare_grants_with_grant_option() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_schema = Schema::new("myschema".to_string(), "myschema".to_string(), None);
    from_schema.acl = vec!["reader=U/owner".to_string()];
    let mut to_schema = Schema::new("myschema".to_string(), "myschema".to_string(), None);
    to_schema.acl = vec!["reader=U*/owner".to_string()];

    from_dump.schemas.push(from_schema);
    to_dump.schemas.push(to_schema);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Full);
    comparer.compare_grants().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("GRANT USAGE ON SCHEMA myschema TO reader WITH GRANT OPTION;"),
        "Must add grant with grant option, got: {script}"
    );
    assert!(
        !script.contains("REVOKE"),
        "Upgrading to WITH GRANT OPTION must not REVOKE, got: {script}"
    );
}

#[tokio::test]
async fn compare_grants_no_comments_mode() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_schema = Schema::new("myschema".to_string(), "myschema".to_string(), None);
    from_schema.acl = vec![];
    let mut to_schema = Schema::new("myschema".to_string(), "myschema".to_string(), None);
    to_schema.acl = vec!["reader=U/owner".to_string()];

    from_dump.schemas.push(from_schema);
    to_dump.schemas.push(to_schema);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, false, GrantsMode::AddOnly);
    comparer.compare_grants().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("GRANT USAGE ON SCHEMA myschema TO reader;"),
        "Grant must still be emitted, got: {script}"
    );
    assert!(
        !script.contains("/* Grants for schema"),
        "Comments must be suppressed, got: {script}"
    );
}

#[tokio::test]
async fn compare_grants_new_object_no_from_acl() {
    let from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    // Table exists only in TO
    let mut to_table = Table::new(
        "public".to_string(),
        "new_tbl".to_string(),
        "public".to_string(),
        "new_tbl".to_string(),
        "owner".to_string(),
        None,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        None,
    );
    to_table.acl = vec!["reader=r/owner".to_string()];

    to_dump.tables.push(to_table);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::AddOnly);
    comparer.compare_grants().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("GRANT SELECT ON TABLE public.new_tbl TO reader;"),
        "Must grant on new object with empty FROM acl, got: {script}"
    );
}

#[tokio::test]
async fn compare_grants_table_addonly_no_revoke() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_table = Table::new(
        "public".to_string(),
        "orders".to_string(),
        "public".to_string(),
        "orders".to_string(),
        "owner".to_string(),
        None,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        None,
    );
    from_table.acl = vec!["reader=r/owner".to_string(), "old_app=rw/owner".to_string()];

    let mut to_table = Table::new(
        "public".to_string(),
        "orders".to_string(),
        "public".to_string(),
        "orders".to_string(),
        "owner".to_string(),
        None,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        None,
    );
    to_table.acl = vec![
        "reader=r/owner".to_string(),
        "new_app=rwd/owner".to_string(),
    ];

    from_dump.tables.push(from_table);
    to_dump.tables.push(to_table);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::AddOnly);
    comparer.compare_grants().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("GRANT DELETE, SELECT, UPDATE ON TABLE public.orders TO new_app;"),
        "AddOnly must add new_app grant, got: {script}"
    );
    assert!(
        !script.contains("REVOKE"),
        "AddOnly must not revoke old_app, got: {script}"
    );
}

#[tokio::test]
async fn compare_grants_sequence_full_revoke() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_seq = Sequence::new(
        "public".to_string(),
        "order_id_seq".to_string(),
        "owner".to_string(),
        "bigint".to_string(),
        Some(1),
        Some(1),
        Some(9223372036854775807),
        Some(1),
        false,
        Some(1),
        Some(1),
        None,
        None,
        None,
    );
    from_seq.acl = vec!["app=U/owner".to_string(), "old_svc=U/owner".to_string()];

    let mut to_seq = Sequence::new(
        "public".to_string(),
        "order_id_seq".to_string(),
        "owner".to_string(),
        "bigint".to_string(),
        Some(1),
        Some(1),
        Some(9223372036854775807),
        Some(1),
        false,
        Some(1),
        Some(1),
        None,
        None,
        None,
    );
    to_seq.acl = vec!["app=U/owner".to_string()];

    from_dump.sequences.push(from_seq);
    to_dump.sequences.push(to_seq);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Full);
    comparer.compare_grants().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("REVOKE USAGE ON SEQUENCE public.order_id_seq FROM old_svc;"),
        "Full must revoke removed sequence grant, got: {script}"
    );
    assert!(
        !script.contains("GRANT"),
        "No new grants expected, got: {script}"
    );
}

#[tokio::test]
async fn compare_grants_sequence_addonly_no_revoke() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_seq = Sequence::new(
        "public".to_string(),
        "s1".to_string(),
        "owner".to_string(),
        "bigint".to_string(),
        Some(1),
        Some(1),
        Some(9223372036854775807),
        Some(1),
        false,
        Some(1),
        Some(1),
        None,
        None,
        None,
    );
    from_seq.acl = vec!["old_svc=U/owner".to_string()];

    let mut to_seq = Sequence::new(
        "public".to_string(),
        "s1".to_string(),
        "owner".to_string(),
        "bigint".to_string(),
        Some(1),
        Some(1),
        Some(9223372036854775807),
        Some(1),
        false,
        Some(1),
        Some(1),
        None,
        None,
        None,
    );
    to_seq.acl = vec![];

    from_dump.sequences.push(from_seq);
    to_dump.sequences.push(to_seq);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::AddOnly);
    comparer.compare_grants().await.unwrap();
    let script = comparer.get_script();

    assert!(
        !script.contains("REVOKE"),
        "AddOnly must not revoke sequence grants, got: {script}"
    );
    assert!(
        !script.contains("GRANT"),
        "No grants expected, got: {script}"
    );
}

#[tokio::test]
async fn compare_grants_view_full_revoke() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_view = View::new(
        "report_v".to_string(),
        "SELECT 1".to_string(),
        "reports".to_string(),
        Vec::new(),
    );
    from_view.acl = vec!["analyst=r/owner".to_string(), "intern=r/owner".to_string()];

    let mut to_view = View::new(
        "report_v".to_string(),
        "SELECT 1".to_string(),
        "reports".to_string(),
        Vec::new(),
    );
    to_view.acl = vec!["analyst=r/owner".to_string()];

    from_dump.views.push(from_view);
    to_dump.views.push(to_view);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Full);
    comparer.compare_grants().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("REVOKE SELECT ON TABLE reports.report_v FROM intern;"),
        "Full must revoke removed view grant, got: {script}"
    );
    assert!(
        !script.contains("GRANT"),
        "No new grants expected, got: {script}"
    );
}

#[tokio::test]
async fn compare_grants_routine_function_full_revoke() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_routine = Routine::new(
        "public".to_string(),
        Oid(10),
        "calc".to_string(),
        "plpgsql".to_string(),
        "function".to_string(),
        "integer".to_string(),
        "x integer".to_string(),
        None,
        None,
        "BEGIN RETURN x; END".to_string(),
    );
    from_routine.acl = vec!["app=X/owner".to_string(), "old_svc=X/owner".to_string()];

    let mut to_routine = Routine::new(
        "public".to_string(),
        Oid(10),
        "calc".to_string(),
        "plpgsql".to_string(),
        "function".to_string(),
        "integer".to_string(),
        "x integer".to_string(),
        None,
        None,
        "BEGIN RETURN x; END".to_string(),
    );
    to_routine.acl = vec!["app=X/owner".to_string()];

    from_dump.routines.push(from_routine);
    to_dump.routines.push(to_routine);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Full);
    comparer.compare_grants().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("REVOKE EXECUTE ON FUNCTION public.calc(x integer) FROM old_svc;"),
        "Full must revoke removed function grant, got: {script}"
    );
    assert!(
        !script.contains("GRANT"),
        "No new grants expected, got: {script}"
    );
}

#[tokio::test]
async fn compare_grants_grantor_only_diff_produces_no_output() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    // Schema: same grantee + privileges, different grantor
    let mut from_schema = Schema::new("app".to_string(), "app".to_string(), None);
    from_schema.acl = vec!["reader=UC/old_owner".to_string()];
    let mut to_schema = Schema::new("app".to_string(), "app".to_string(), None);
    to_schema.acl = vec!["reader=UC/new_owner".to_string()];
    from_dump.schemas.push(from_schema);
    to_dump.schemas.push(to_schema);

    // Table: same grantee + privileges, different grantor
    let mut from_table = Table::new(
        "app".to_string(),
        "t1".to_string(),
        "app".to_string(),
        "t1".to_string(),
        "owner".to_string(),
        None,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        None,
    );
    from_table.acl = vec!["reader=rw/old_owner".to_string()];
    let mut to_table = Table::new(
        "app".to_string(),
        "t1".to_string(),
        "app".to_string(),
        "t1".to_string(),
        "owner".to_string(),
        None,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        None,
    );
    to_table.acl = vec!["reader=rw/new_owner".to_string()];
    from_dump.tables.push(from_table);
    to_dump.tables.push(to_table);

    // Sequence: same grantee + privileges, different grantor
    let mut from_seq = Sequence::new(
        "app".to_string(),
        "s1".to_string(),
        "owner".to_string(),
        "bigint".to_string(),
        Some(1),
        Some(1),
        Some(9223372036854775807),
        Some(1),
        false,
        Some(1),
        Some(1),
        None,
        None,
        None,
    );
    from_seq.acl = vec!["reader=U/old_owner".to_string()];
    let mut to_seq = Sequence::new(
        "app".to_string(),
        "s1".to_string(),
        "owner".to_string(),
        "bigint".to_string(),
        Some(1),
        Some(1),
        Some(9223372036854775807),
        Some(1),
        false,
        Some(1),
        Some(1),
        None,
        None,
        None,
    );
    to_seq.acl = vec!["reader=U/new_owner".to_string()];
    from_dump.sequences.push(from_seq);
    to_dump.sequences.push(to_seq);

    // View: same grantee + privileges, different grantor
    let mut from_view = View::new(
        "v1".to_string(),
        "SELECT 1".to_string(),
        "app".to_string(),
        Vec::new(),
    );
    from_view.acl = vec!["reader=r/old_owner".to_string()];
    let mut to_view = View::new(
        "v1".to_string(),
        "SELECT 1".to_string(),
        "app".to_string(),
        Vec::new(),
    );
    to_view.acl = vec!["reader=r/new_owner".to_string()];
    from_dump.views.push(from_view);
    to_dump.views.push(to_view);

    // Routine: same grantee + privileges, different grantor
    let mut from_routine = Routine::new(
        "app".to_string(),
        Oid(99),
        "do_it".to_string(),
        "plpgsql".to_string(),
        "function".to_string(),
        "void".to_string(),
        "".to_string(),
        None,
        None,
        "BEGIN END".to_string(),
    );
    from_routine.acl = vec!["runner=X/old_owner".to_string()];
    let mut to_routine = Routine::new(
        "app".to_string(),
        Oid(99),
        "do_it".to_string(),
        "plpgsql".to_string(),
        "function".to_string(),
        "void".to_string(),
        "".to_string(),
        None,
        None,
        "BEGIN END".to_string(),
    );
    to_routine.acl = vec!["runner=X/new_owner".to_string()];
    from_dump.routines.push(from_routine);
    to_dump.routines.push(to_routine);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Full);
    comparer.compare_grants().await.unwrap();
    let script = comparer.get_script();

    assert!(
        !script.contains("GRANT"),
        "Grantor-only diff must not emit GRANT, got: {script}"
    );
    assert!(
        !script.contains("REVOKE"),
        "Grantor-only diff must not emit REVOKE, got: {script}"
    );
}

#[tokio::test]
async fn compare_grants_full_grant_option_downgrade() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_table = Table::new(
        "public".to_string(),
        "items".to_string(),
        "public".to_string(),
        "items".to_string(),
        "owner".to_string(),
        None,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        None,
    );
    from_table.acl = vec!["admin=r*/owner".to_string()];

    let mut to_table = Table::new(
        "public".to_string(),
        "items".to_string(),
        "public".to_string(),
        "items".to_string(),
        "owner".to_string(),
        None,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        None,
    );
    to_table.acl = vec!["admin=r/owner".to_string()];

    from_dump.tables.push(from_table);
    to_dump.tables.push(to_table);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Full);
    comparer.compare_grants().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("REVOKE GRANT OPTION FOR SELECT ON TABLE public.items FROM admin;"),
        "Full must revoke grant option when downgrading, got: {script}"
    );
    assert!(
        !script.contains("GRANT SELECT"),
        "No new grant expected for downgrade, got: {script}"
    );
    // Should only contain REVOKE GRANT OPTION FOR, not a bare REVOKE SELECT
    assert!(
        !script.contains("REVOKE SELECT ON TABLE"),
        "Must not fully revoke the privilege on downgrade, got: {script}"
    );
}

#[tokio::test]
async fn compare_grants_addonly_ignores_grant_option_downgrade() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_table = Table::new(
        "public".to_string(),
        "items".to_string(),
        "public".to_string(),
        "items".to_string(),
        "owner".to_string(),
        None,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        None,
    );
    from_table.acl = vec!["admin=r*/owner".to_string()];

    let mut to_table = Table::new(
        "public".to_string(),
        "items".to_string(),
        "public".to_string(),
        "items".to_string(),
        "owner".to_string(),
        None,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        None,
    );
    to_table.acl = vec!["admin=r/owner".to_string()];

    from_dump.tables.push(from_table);
    to_dump.tables.push(to_table);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::AddOnly);
    comparer.compare_grants().await.unwrap();
    let script = comparer.get_script();

    assert!(
        !script.contains("REVOKE"),
        "AddOnly must not revoke grant option, got: {script}"
    );
    assert!(
        !script.contains("GRANT"),
        "No new grant expected, got: {script}"
    );
}

#[tokio::test]
async fn compare_grants_excludes_owner_acl_entries() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    // Table with ownership change: old_owner → new_owner
    let mut from_table = Table::new(
        "public".to_string(),
        "data".to_string(),
        "public".to_string(),
        "data".to_string(),
        "old_owner".to_string(),
        None,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        None,
    );
    from_table.acl = vec![
        "old_owner=arwdDxt/old_owner".to_string(),
        "reader=r/old_owner".to_string(),
    ];

    let mut to_table = Table::new(
        "public".to_string(),
        "data".to_string(),
        "public".to_string(),
        "data".to_string(),
        "new_owner".to_string(),
        None,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        None,
    );
    to_table.acl = vec![
        "new_owner=arwdDxt/new_owner".to_string(),
        "reader=r/new_owner".to_string(),
    ];

    from_dump.tables.push(from_table);
    to_dump.tables.push(to_table);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Full);
    comparer.compare_grants().await.unwrap();
    let script = comparer.get_script();

    assert!(
        !script.contains("old_owner"),
        "Must not REVOKE from old owner, got: {script}"
    );
    assert!(
        !script.contains("new_owner"),
        "Must not GRANT to new owner, got: {script}"
    );
    assert!(
        !script.contains("GRANT"),
        "No grants expected (reader unchanged), got: {script}"
    );
    assert!(
        !script.contains("REVOKE"),
        "No revokes expected (reader unchanged), got: {script}"
    );
}

#[tokio::test]
async fn compare_grants_owner_excluded_nonowner_still_diffed() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_routine = Routine::new(
        "public".to_string(),
        Oid(50),
        "process".to_string(),
        "plpgsql".to_string(),
        "function".to_string(),
        "void".to_string(),
        "".to_string(),
        None,
        None,
        "BEGIN END".to_string(),
    );
    from_routine.owner = "the_owner".to_string();
    from_routine.acl = vec![
        "the_owner=X/the_owner".to_string(),
        "old_app=X/the_owner".to_string(),
    ];

    let mut to_routine = Routine::new(
        "public".to_string(),
        Oid(50),
        "process".to_string(),
        "plpgsql".to_string(),
        "function".to_string(),
        "void".to_string(),
        "".to_string(),
        None,
        None,
        "BEGIN END".to_string(),
    );
    to_routine.owner = "the_owner".to_string();
    to_routine.acl = vec![
        "the_owner=X/the_owner".to_string(),
        "new_app=X/the_owner".to_string(),
    ];

    from_dump.routines.push(from_routine);
    to_dump.routines.push(to_routine);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Full);
    comparer.compare_grants().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("GRANT EXECUTE ON FUNCTION public.process() TO new_app;"),
        "Must grant to non-owner, got: {script}"
    );
    assert!(
        script.contains("REVOKE EXECUTE ON FUNCTION public.process() FROM old_app;"),
        "Must revoke from non-owner, got: {script}"
    );
    assert!(
        !script.contains("the_owner"),
        "Must not reference owner in grants/revokes, got: {script}"
    );
}

#[tokio::test]
async fn compare_routines_overloaded_identical_no_diff() {
    // Two routines with the same (schema, name) but different arguments.
    // Both overloads are identical in FROM and TO → no output expected.
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let overload_short_from = Routine::new(
        "myschema".to_string(),
        Oid(1),
        "notify_event".to_string(),
        "plpgsql".to_string(),
        "PROCEDURE".to_string(),
        "void".to_string(),
        "pjobid uuid, peventtype character varying, pattributes jsonb".to_string(),
        None,
        None,
        "BEGIN\n    CALL myschema.notify_event(pjobid, peventtype, null, pattributes, null);\nEND;"
            .to_string(),
    );
    let overload_long_from = Routine::new(
        "myschema".to_string(),
        Oid(2),
        "notify_event".to_string(),
        "plpgsql".to_string(),
        "PROCEDURE".to_string(),
        "void".to_string(),
        "pjobid uuid, peventtype character varying, puserid character varying, pattributes jsonb, psessionseed jsonb DEFAULT NULL::jsonb".to_string(),
        None,
        None,
        "BEGIN\n    RAISE NOTICE 'notify';\nEND;".to_string(),
    );

    let overload_short_to = Routine::new(
        "myschema".to_string(),
        Oid(1),
        "notify_event".to_string(),
        "plpgsql".to_string(),
        "PROCEDURE".to_string(),
        "void".to_string(),
        "pjobid uuid, peventtype character varying, pattributes jsonb".to_string(),
        None,
        None,
        "BEGIN\n    CALL myschema.notify_event(pjobid, peventtype, null, pattributes, null);\nEND;"
            .to_string(),
    );
    let overload_long_to = Routine::new(
        "myschema".to_string(),
        Oid(2),
        "notify_event".to_string(),
        "plpgsql".to_string(),
        "PROCEDURE".to_string(),
        "void".to_string(),
        "pjobid uuid, peventtype character varying, puserid character varying, pattributes jsonb, psessionseed jsonb DEFAULT NULL::jsonb".to_string(),
        None,
        None,
        "BEGIN\n    RAISE NOTICE 'notify';\nEND;".to_string(),
    );

    from_dump.routines.push(overload_short_from);
    from_dump.routines.push(overload_long_from);
    to_dump.routines.push(overload_short_to);
    to_dump.routines.push(overload_long_to);

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_routines().await.unwrap();
    let script = comparer.get_script();

    assert!(
        !script.contains("create or replace"),
        "Identical overloads must not produce CREATE, got: {script}"
    );
    assert!(
        !script.contains("drop procedure"),
        "Identical overloads must not produce DROP, got: {script}"
    );
}

#[tokio::test]
async fn compare_routines_overloaded_one_changed() {
    // Two overloads with the same (schema, name). Only the long overload
    // changes its body between FROM and TO. The short overload must remain
    // untouched while the long one is recreated.
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let overload_short_from = Routine::new(
        "myschema".to_string(),
        Oid(1),
        "notify_event".to_string(),
        "plpgsql".to_string(),
        "PROCEDURE".to_string(),
        "void".to_string(),
        "pjobid uuid, peventtype character varying, pattributes jsonb".to_string(),
        None,
        None,
        "BEGIN\n    CALL myschema.notify_event(pjobid, peventtype, null, pattributes, null);\nEND;"
            .to_string(),
    );
    let overload_long_from = Routine::new(
        "myschema".to_string(),
        Oid(2),
        "notify_event".to_string(),
        "plpgsql".to_string(),
        "PROCEDURE".to_string(),
        "void".to_string(),
        "pjobid uuid, peventtype character varying, puserid character varying, pattributes jsonb, psessionseed jsonb DEFAULT NULL::jsonb".to_string(),
        None,
        None,
        "BEGIN\n    RAISE NOTICE 'old body';\nEND;".to_string(),
    );

    // Short overload is identical to FROM
    let overload_short_to = Routine::new(
        "myschema".to_string(),
        Oid(1),
        "notify_event".to_string(),
        "plpgsql".to_string(),
        "PROCEDURE".to_string(),
        "void".to_string(),
        "pjobid uuid, peventtype character varying, pattributes jsonb".to_string(),
        None,
        None,
        "BEGIN\n    CALL myschema.notify_event(pjobid, peventtype, null, pattributes, null);\nEND;"
            .to_string(),
    );
    // Long overload has a different body → should be recreated
    let overload_long_to = Routine::new(
        "myschema".to_string(),
        Oid(2),
        "notify_event".to_string(),
        "plpgsql".to_string(),
        "PROCEDURE".to_string(),
        "void".to_string(),
        "pjobid uuid, peventtype character varying, puserid character varying, pattributes jsonb, psessionseed jsonb DEFAULT NULL::jsonb".to_string(),
        None,
        None,
        "BEGIN\n    RAISE NOTICE 'new body';\nEND;".to_string(),
    );

    from_dump.routines.push(overload_short_from);
    from_dump.routines.push(overload_long_from);
    to_dump.routines.push(overload_short_to);
    to_dump.routines.push(overload_long_to);

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_routines().await.unwrap();
    let script = comparer.get_script();

    // The changed (long) overload must be recreated
    assert!(
        script.contains("create or replace procedure myschema.notify_event(pjobid uuid, peventtype character varying, puserid character varying, pattributes jsonb, psessionseed jsonb DEFAULT NULL::jsonb)"),
        "Changed overload must be recreated, got: {script}"
    );
    // The short overload's signature must NOT appear in any CREATE statement
    let short_create = "create or replace procedure myschema.notify_event(pjobid uuid, peventtype character varying, pattributes jsonb)";
    assert!(
        !script.contains(short_create),
        "Unchanged overload must not be recreated, got: {script}"
    );
}

#[tokio::test]
async fn compare_routines_overloaded_drop_only_removed_overload() {
    // FROM has two overloads; TO has only the short one.
    // Only the long overload must be dropped; the short one must stay.
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let overload_short = Routine::new(
        "myschema".to_string(),
        Oid(1),
        "notify_event".to_string(),
        "plpgsql".to_string(),
        "PROCEDURE".to_string(),
        "void".to_string(),
        "pjobid uuid, pattributes jsonb".to_string(),
        None,
        None,
        "BEGIN\n    RAISE NOTICE 'short';\nEND;".to_string(),
    );
    let overload_long = Routine::new(
        "myschema".to_string(),
        Oid(2),
        "notify_event".to_string(),
        "plpgsql".to_string(),
        "PROCEDURE".to_string(),
        "void".to_string(),
        "pjobid uuid, pattributes jsonb, pseed jsonb".to_string(),
        None,
        None,
        "BEGIN\n    RAISE NOTICE 'long';\nEND;".to_string(),
    );

    from_dump.routines.push(overload_short.clone());
    from_dump.routines.push(overload_long);
    to_dump.routines.push(overload_short);

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_routines().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("drop procedure if exists myschema.notify_event (pjobid uuid, pattributes jsonb, pseed jsonb) cascade;"),
        "Removed overload must be dropped, got: {script}"
    );
    assert!(
        !script.contains("create or replace"),
        "Unchanged overload must not be recreated, got: {script}"
    );
}

#[tokio::test]
async fn new_partition_children_deferred_until_parent_is_recreated() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    // Root partitioned table (unchanged in both dumps)
    let mut root = Table::new(
        "data".to_string(),
        "events".to_string(),
        "data".to_string(),
        "events".to_string(),
        "postgres".to_string(),
        None,
        vec![int_column("data", "events", "id", 1)],
        vec![],
        vec![],
        vec![],
        None,
    );
    root.partition_key = Some("RANGE (id)".to_string());
    root.hash();

    // FROM: events_2023 exists but is NOT sub-partitioned (no partition_key)
    let mut from_events_2023 = Table::new(
        "data".to_string(),
        "events_2023".to_string(),
        "data".to_string(),
        "events_2023".to_string(),
        "postgres".to_string(),
        None,
        vec![int_column("data", "events_2023", "id", 1)],
        vec![],
        vec![],
        vec![],
        None,
    );
    from_events_2023.partition_of = Some("\"data\".\"events\"".to_string());
    from_events_2023.partition_bound = Some("FOR VALUES FROM (2023) TO (2024)".to_string());
    from_events_2023.hash();

    // TO: events_2023 now gains a partition_key (LIST region)
    let mut to_events_2023 = Table::new(
        "data".to_string(),
        "events_2023".to_string(),
        "data".to_string(),
        "events_2023".to_string(),
        "postgres".to_string(),
        None,
        vec![int_column("data", "events_2023", "id", 1)],
        vec![],
        vec![],
        vec![],
        None,
    );
    to_events_2023.partition_of = Some("\"data\".\"events\"".to_string());
    to_events_2023.partition_bound = Some("FOR VALUES FROM (2023) TO (2024)".to_string());
    to_events_2023.partition_key = Some("LIST (region)".to_string());
    to_events_2023.hash();

    let mut leaf_eu = Table::new(
        "data".to_string(),
        "events_2023_eu".to_string(),
        "data".to_string(),
        "events_2023_eu".to_string(),
        "postgres".to_string(),
        None,
        vec![int_column("data", "events_2023_eu", "id", 1)],
        vec![],
        vec![],
        vec![],
        None,
    );
    leaf_eu.partition_of = Some("\"data\".\"events_2023\"".to_string());
    leaf_eu.partition_bound = Some("FOR VALUES IN ('eu')".to_string());
    leaf_eu.hash();

    let mut leaf_us = Table::new(
        "data".to_string(),
        "events_2023_us".to_string(),
        "data".to_string(),
        "events_2023_us".to_string(),
        "postgres".to_string(),
        None,
        vec![int_column("data", "events_2023_us", "id", 1)],
        vec![],
        vec![],
        vec![],
        None,
    );
    leaf_us.partition_of = Some("\"data\".\"events_2023\"".to_string());
    leaf_us.partition_bound = Some("FOR VALUES IN ('us')".to_string());
    leaf_us.hash();

    // FROM dump: root + old events_2023 (no sub-partition key)
    from_dump.tables.push(root.clone());
    from_dump.tables.push(from_events_2023);

    // TO dump: root + new events_2023 (with sub-partition key) + two leaves
    // Push in reverse depth order to stress the sorting
    to_dump.tables.push(leaf_us);
    to_dump.tables.push(leaf_eu);
    to_dump.tables.push(to_events_2023);
    to_dump.tables.push(root);

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();

    let pos_recreate = script
        .find("create table data.events_2023 partition of")
        .expect("events_2023 recreate not found");
    let pos_eu = script
        .find("create table data.events_2023_eu partition of")
        .expect("events_2023_eu create not found");
    let pos_us = script
        .find("create table data.events_2023_us partition of")
        .expect("events_2023_us create not found");

    assert!(
        pos_recreate < pos_eu,
        "events_2023 must be recreated before events_2023_eu is created (got recreate={pos_recreate}, eu={pos_eu})"
    );
    assert!(
        pos_recreate < pos_us,
        "events_2023 must be recreated before events_2023_us is created (got recreate={pos_recreate}, us={pos_us})"
    );

    let pos_drop = script
        .find("drop table if exists data.events_2023")
        .expect("events_2023 drop not found");
    assert!(
        pos_drop < pos_eu,
        "events_2023 must be dropped before events_2023_eu is created"
    );
    assert!(
        pos_drop < pos_us,
        "events_2023 must be dropped before events_2023_us is created"
    );
}

#[tokio::test]
async fn fk_pre_drop_commented_when_use_drop_false() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    // "from" has a table referenced by FK
    let mut referenced = Table::new(
        "public".to_string(),
        "users".to_string(),
        "public".to_string(),
        "users".to_string(),
        "postgres".to_string(),
        None,
        vec![int_column("public", "users", "id", 1)],
        vec![],
        vec![],
        vec![],
        None,
    );
    referenced.hash();

    // "from" has a table with FK referencing "users"
    let mut referencing = Table::new(
        "public".to_string(),
        "orders".to_string(),
        "public".to_string(),
        "orders".to_string(),
        "postgres".to_string(),
        None,
        vec![
            int_column("public", "orders", "id", 1),
            int_column("public", "orders", "user_id", 2),
        ],
        vec![TableConstraint {
            catalog: "postgres".to_string(),
            schema: "public".to_string(),
            name: "orders_user_fk".to_string(),
            table_name: "orders".to_string(),
            constraint_type: "FOREIGN KEY".to_string(),
            is_deferrable: false,
            initially_deferred: false,
            definition: Some("FOREIGN KEY (user_id) REFERENCES public.users(id)".to_string()),
            coninhcount: 0,
            is_enforced: true,
            no_inherit: false,
            nulls_not_distinct: false,
            comment: None,
        }],
        vec![],
        vec![],
        None,
    );
    referencing.hash();

    from_dump.tables.push(referenced.clone());
    from_dump.tables.push(referencing.clone());

    // "to" has only "orders" — "users" is being dropped, so its FK must be pre-dropped
    to_dump.tables.push(referencing);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();

    // FK drop should be commented out because use_drop=false
    let has_commented_fk_drop = script.lines().any(|l| {
        l.starts_with("--") && l.contains("drop constraint") && l.contains("orders_user_fk")
    });
    assert!(
        has_commented_fk_drop,
        "FK pre-drop should be commented out when use_drop=false, script:\n{}",
        script
    );

    // Should NOT have an active (uncommented) drop constraint for the FK
    let has_active_fk_drop = script.lines().any(|l| {
        !l.starts_with("--") && l.contains("drop constraint") && l.contains("orders_user_fk")
    });
    assert!(
        !has_active_fk_drop,
        "FK pre-drop should NOT be active when use_drop=false"
    );
}

#[tokio::test]
async fn fk_pre_drop_active_when_use_drop_true() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut referenced = Table::new(
        "public".to_string(),
        "users".to_string(),
        "public".to_string(),
        "users".to_string(),
        "postgres".to_string(),
        None,
        vec![int_column("public", "users", "id", 1)],
        vec![],
        vec![],
        vec![],
        None,
    );
    referenced.hash();

    let mut referencing = Table::new(
        "public".to_string(),
        "orders".to_string(),
        "public".to_string(),
        "orders".to_string(),
        "postgres".to_string(),
        None,
        vec![
            int_column("public", "orders", "id", 1),
            int_column("public", "orders", "user_id", 2),
        ],
        vec![TableConstraint {
            catalog: "postgres".to_string(),
            schema: "public".to_string(),
            name: "orders_user_fk".to_string(),
            table_name: "orders".to_string(),
            constraint_type: "FOREIGN KEY".to_string(),
            is_deferrable: false,
            initially_deferred: false,
            definition: Some("FOREIGN KEY (user_id) REFERENCES public.users(id)".to_string()),
            coninhcount: 0,
            is_enforced: true,
            no_inherit: false,
            nulls_not_distinct: false,
            comment: None,
        }],
        vec![],
        vec![],
        None,
    );
    referencing.hash();

    from_dump.tables.push(referenced.clone());
    from_dump.tables.push(referencing.clone());
    to_dump.tables.push(referencing);

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();

    let has_active_fk_drop = script.lines().any(|l| {
        !l.starts_with("--") && l.contains("drop constraint") && l.contains("orders_user_fk")
    });
    assert!(
        has_active_fk_drop,
        "FK pre-drop should be active when use_drop=true, script:\n{}",
        script
    );
}

#[tokio::test]
async fn trigger_pre_drop_commented_when_use_drop_false() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());

    // "from" has a table with a trigger; table is absent in "to"
    let mut table_with_trigger = Table::new(
        "public".to_string(),
        "events".to_string(),
        "public".to_string(),
        "events".to_string(),
        "postgres".to_string(),
        None,
        vec![int_column("public", "events", "id", 1)],
        vec![],
        vec![],
        vec![TableTrigger {
            oid: Oid(9999),
            name: "trg_events_audit".to_string(),
            definition: "before insert on events for each row execute function audit()".to_string(),
            enabled: "O".to_string(),
            comment: None,
        }],
        None,
    );
    table_with_trigger.hash();

    from_dump.tables.push(table_with_trigger);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();

    // Trigger drop should be commented out when use_drop=false
    let has_commented_trigger_drop = script.lines().any(|l| {
        l.starts_with("--") && l.contains("drop trigger") && l.contains("trg_events_audit")
    });
    assert!(
        has_commented_trigger_drop,
        "Trigger pre-drop should be commented when use_drop=false, script:\n{}",
        script
    );

    let has_active_trigger_drop = script.lines().any(|l| {
        !l.starts_with("--") && l.contains("drop trigger") && l.contains("trg_events_audit")
    });
    assert!(
        !has_active_trigger_drop,
        "Trigger pre-drop should NOT be active when use_drop=false"
    );
}

#[tokio::test]
async fn trigger_pre_drop_active_when_use_drop_true() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());

    let mut table_with_trigger = Table::new(
        "public".to_string(),
        "events".to_string(),
        "public".to_string(),
        "events".to_string(),
        "postgres".to_string(),
        None,
        vec![int_column("public", "events", "id", 1)],
        vec![],
        vec![],
        vec![TableTrigger {
            oid: Oid(9999),
            name: "trg_events_audit".to_string(),
            definition: "before insert on events for each row execute function audit()".to_string(),
            enabled: "O".to_string(),
            comment: None,
        }],
        None,
    );
    table_with_trigger.hash();

    from_dump.tables.push(table_with_trigger);

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();

    let has_active_trigger_drop = script.lines().any(|l| {
        !l.starts_with("--") && l.contains("drop trigger") && l.contains("trg_events_audit")
    });
    assert!(
        has_active_trigger_drop,
        "Trigger pre-drop should be active when use_drop=true, script:\n{}",
        script
    );
}

// ------ Kind-transition tests (regular <-> materialized) ------

#[tokio::test]
async fn kind_transition_regular_to_materialized_use_drop_true() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_view = View::new(
        "my_view".to_string(),
        "SELECT 1".to_string(),
        "public".to_string(),
        vec![],
    );
    from_view.is_materialized = false;
    from_view.hash();
    from_dump.views.push(from_view);

    let mut to_view = View::new(
        "my_view".to_string(),
        "SELECT 1".to_string(),
        "public".to_string(),
        vec![],
    );
    to_view.is_materialized = true;
    to_view.hash();
    to_dump.views.push(to_view);

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.drop_views().await.unwrap();
    comparer.create_views().await.unwrap();
    let script = comparer.get_script();

    // DROP VIEW (regular) should be active
    let has_active_drop = script
        .lines()
        .any(|l| !l.starts_with("--") && l.to_lowercase().contains("drop view"));
    assert!(
        has_active_drop,
        "Regular→mat: DROP VIEW should be active when use_drop=true, script:\n{}",
        script
    );
    // CREATE MATERIALIZED VIEW should be active
    let has_active_create = script
        .lines()
        .any(|l| !l.starts_with("--") && l.to_lowercase().contains("create materialized view"));
    assert!(
        has_active_create,
        "Regular→mat: CREATE MATERIALIZED VIEW should be active when use_drop=true, script:\n{}",
        script
    );
}

#[tokio::test]
async fn kind_transition_regular_to_materialized_use_drop_false() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_view = View::new(
        "my_view".to_string(),
        "SELECT 1".to_string(),
        "public".to_string(),
        vec![],
    );
    from_view.is_materialized = false;
    from_view.hash();
    from_dump.views.push(from_view);

    let mut to_view = View::new(
        "my_view".to_string(),
        "SELECT 1".to_string(),
        "public".to_string(),
        vec![],
    );
    to_view.is_materialized = true;
    to_view.hash();
    to_dump.views.push(to_view);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Ignore);
    comparer.drop_views().await.unwrap();
    comparer.create_views().await.unwrap();
    let script = comparer.get_script();

    // DROP should be commented
    let has_active_drop = script
        .lines()
        .any(|l| !l.starts_with("--") && l.to_lowercase().contains("drop view"));
    assert!(
        !has_active_drop,
        "Regular→mat: DROP VIEW should be commented when use_drop=false, script:\n{}",
        script
    );
    // CREATE should be commented (manual intervention needed)
    let has_active_create = script
        .lines()
        .any(|l| !l.starts_with("--") && l.to_lowercase().contains("create materialized view"));
    assert!(
        !has_active_create,
        "Regular→mat: CREATE MATERIALIZED VIEW should be commented when use_drop=false, script:\n{}",
        script
    );
    assert!(
        script.contains("manual intervention needed"),
        "Should contain manual intervention warning, script:\n{}",
        script
    );
}

#[tokio::test]
async fn kind_transition_materialized_to_regular_use_drop_true() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_view = View::new(
        "my_view".to_string(),
        "SELECT 1".to_string(),
        "public".to_string(),
        vec![],
    );
    from_view.is_materialized = true;
    from_view.hash();
    from_dump.views.push(from_view);

    let mut to_view = View::new(
        "my_view".to_string(),
        "SELECT 1".to_string(),
        "public".to_string(),
        vec![],
    );
    to_view.is_materialized = false;
    to_view.hash();
    to_dump.views.push(to_view);

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.drop_views().await.unwrap();
    comparer.create_views().await.unwrap();
    let script = comparer.get_script();

    // DROP MATERIALIZED VIEW should be active
    let has_active_drop = script
        .lines()
        .any(|l| !l.starts_with("--") && l.to_lowercase().contains("drop materialized view"));
    assert!(
        has_active_drop,
        "Mat→regular: DROP MATERIALIZED VIEW should be active when use_drop=true, script:\n{}",
        script
    );
    // CREATE OR REPLACE VIEW should be active
    let has_active_create = script
        .lines()
        .any(|l| !l.starts_with("--") && l.to_lowercase().contains("create or replace view"));
    assert!(
        has_active_create,
        "Mat→regular: CREATE OR REPLACE VIEW should be active when use_drop=true, script:\n{}",
        script
    );
}

#[tokio::test]
async fn kind_transition_materialized_to_regular_use_drop_false() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_view = View::new(
        "my_view".to_string(),
        "SELECT 1".to_string(),
        "public".to_string(),
        vec![],
    );
    from_view.is_materialized = true;
    from_view.hash();
    from_dump.views.push(from_view);

    let mut to_view = View::new(
        "my_view".to_string(),
        "SELECT 1".to_string(),
        "public".to_string(),
        vec![],
    );
    to_view.is_materialized = false;
    to_view.hash();
    to_dump.views.push(to_view);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Ignore);
    comparer.drop_views().await.unwrap();
    comparer.create_views().await.unwrap();
    let script = comparer.get_script();

    // DROP should be commented
    let has_active_drop = script
        .lines()
        .any(|l| !l.starts_with("--") && l.to_lowercase().contains("drop materialized view"));
    assert!(
        !has_active_drop,
        "Mat→regular: DROP MATERIALIZED VIEW should be commented when use_drop=false, script:\n{}",
        script
    );
    // CREATE OR REPLACE VIEW should also be commented (kind transition)
    let has_active_create = script
        .lines()
        .any(|l| !l.starts_with("--") && l.to_lowercase().contains("create or replace view"));
    assert!(
        !has_active_create,
        "Mat→regular: CREATE OR REPLACE VIEW should be commented when use_drop=false, script:\n{}",
        script
    );
    assert!(
        script.contains("manual intervention needed"),
        "Should contain manual intervention warning, script:\n{}",
        script
    );
}

/// FROM-only views (present in FROM, absent in TO) must still appear in the output
/// when use_drop=false — as a commented-out DROP statement so the user is aware the
/// view should be removed.  Previously `should_drop` gated `is_from_only` behind
/// `self.use_drop`, which suppressed the DROP entirely.
#[tokio::test]
async fn from_only_view_commented_drop_when_use_drop_false() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());

    let mut view = View::new(
        "obsolete_view".to_string(),
        "SELECT 1".to_string(),
        "public".to_string(),
        vec![],
    );
    view.is_materialized = false;
    view.hash();
    from_dump.views.push(view);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Ignore);
    comparer.drop_views().await.unwrap();
    let script = comparer.get_script();

    // The DROP must appear in the output …
    assert!(
        script.to_lowercase().contains("drop view"),
        "FROM-only view must produce a DROP statement even with use_drop=false, script:\n{}",
        script
    );
    // … but it must be commented out, not active SQL.
    let has_active_drop = script
        .lines()
        .any(|l| !l.starts_with("--") && l.to_lowercase().contains("drop view"));
    assert!(
        !has_active_drop,
        "FROM-only view DROP should be commented when use_drop=false, script:\n{}",
        script
    );
}

/// Counterpart: with use_drop=true the DROP for a FROM-only view must be active SQL.
#[tokio::test]
async fn from_only_view_active_drop_when_use_drop_true() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());

    let mut view = View::new(
        "obsolete_view".to_string(),
        "SELECT 1".to_string(),
        "public".to_string(),
        vec![],
    );
    view.is_materialized = false;
    view.hash();
    from_dump.views.push(view);

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.drop_views().await.unwrap();
    let script = comparer.get_script();

    let has_active_drop = script
        .lines()
        .any(|l| !l.starts_with("--") && l.to_lowercase().contains("drop view"));
    assert!(
        has_active_drop,
        "FROM-only view DROP should be active SQL when use_drop=true, script:\n{}",
        script
    );
}

// --- Tests for get_script() newline collapsing vs dollar-quoted bodies ---

/// Helper: build a Comparer with use_comments=false and a given script body.
fn comparer_with_script(script: &str) -> Comparer {
    let from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());
    let mut c = Comparer::new(from_dump, to_dump, false, false, false, GrantsMode::Ignore);
    c.script = script.to_string();
    c
}

#[test]
fn get_script_collapses_triple_newlines_outside_dollar_quotes() {
    let input = "SELECT 1;\n\n\n\nSELECT 2;\n";
    let c = comparer_with_script(input);
    let out = c.get_script();
    // 4 newlines should be collapsed to 2
    assert_eq!(out, "SELECT 1;\n\nSELECT 2;\n");
}

#[test]
fn get_script_preserves_triple_newlines_inside_dollar_quotes() {
    let input = concat!(
        "CREATE OR REPLACE PROCEDURE public.test_proc() LANGUAGE plpgsql AS $$\n",
        "BEGIN\n",
        "  RAISE NOTICE 'block 1';\n",
        "\n",
        "\n",
        "\n",
        "  RAISE NOTICE 'block 2';\n",
        "END;\n",
        "$$;\n",
    );
    let c = comparer_with_script(input);
    let out = c.get_script();
    // The three consecutive newlines inside $$ must survive
    assert!(
        out.contains("'block 1';\n\n\n\n  RAISE NOTICE 'block 2'"),
        "blank lines inside $$ body must be preserved, got:\n{}",
        out
    );
}

#[test]
fn get_script_preserves_newlines_inside_tagged_dollar_quotes() {
    let input = concat!(
        "CREATE FUNCTION f() RETURNS void LANGUAGE plpgsql AS $body$\n",
        "BEGIN\n",
        "\n",
        "\n",
        "\n",
        "  NULL;\n",
        "END;\n",
        "$body$;\n",
    );
    let c = comparer_with_script(input);
    let out = c.get_script();
    assert!(
        out.contains("BEGIN\n\n\n\n  NULL;"),
        "blank lines inside $body$ must be preserved, got:\n{}",
        out
    );
}

#[test]
fn get_script_collapses_newlines_between_dollar_quoted_blocks() {
    // Newlines *outside* dollar-quoted blocks should still be collapsed
    let input = "$$body1$$;\n\n\n\n$$body2$$;\n";
    let c = comparer_with_script(input);
    let out = c.get_script();
    assert_eq!(out, "$$body1$$;\n\n$$body2$$;\n");
}

#[test]
fn get_script_mixed_dollar_quote_and_outside_newlines() {
    let input = concat!(
        "SELECT 1;\n\n\n\n",
        "CREATE FUNCTION f() RETURNS void AS $$\n",
        "BEGIN\n",
        "\n\n\n",
        "  NULL;\n",
        "END;\n",
        "$$;\n",
        "\n\n\n\n",
        "SELECT 2;\n",
    );
    let c = comparer_with_script(input);
    let out = c.get_script();

    // Outside: collapsed
    assert!(
        !out.contains("SELECT 1;\n\n\n"),
        "newlines before $$ block should be collapsed"
    );
    assert!(
        !out.contains("$$;\n\n\n"),
        "newlines after $$ block should be collapsed"
    );
    // Inside: preserved
    assert!(
        out.contains("BEGIN\n\n\n\n  NULL;"),
        "blank lines inside $$ must be preserved, got:\n{}",
        out
    );
}

#[test]
fn get_script_with_use_comments_true_returns_verbatim() {
    let input = "SELECT 1;\n\n\n\nSELECT 2;\n";
    let from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());
    let mut c = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Ignore);
    c.script = input.to_string();
    let out = c.get_script();
    // With use_comments=true, script is returned as-is
    assert_eq!(out, input);
}

#[test]
fn get_script_strips_comments_but_preserves_dollar_body_newlines() {
    let input = concat!(
        "-- a comment\n",
        "CREATE FUNCTION f() RETURNS void AS $$\n",
        "BEGIN\n",
        "\n\n\n",
        "  NULL;\n",
        "END;\n",
        "$$;\n",
    );
    let c = comparer_with_script(input);
    let out = c.get_script();
    // Comment removed
    assert!(
        !out.contains("-- a comment"),
        "line comment should be removed"
    );
    // Dollar body preserved
    assert!(
        out.contains("BEGIN\n\n\n\n  NULL;"),
        "blank lines inside $$ body must be preserved, got:\n{}",
        out
    );
}

#[test]
fn get_script_empty_dollar_body_not_corrupted() {
    let input = "CREATE FUNCTION f() AS $$$$;\n";
    let c = comparer_with_script(input);
    let out = c.get_script();
    assert!(out.contains("$$$$"), "empty dollar body must be preserved");
}

#[test]
fn get_script_unterminated_dollar_quote_copies_to_end() {
    // Unterminated dollar-quote: everything after opening tag should be
    // copied verbatim (same as the comment-stripping pass behaviour).
    let input = "CREATE FUNCTION f() AS $$\nBEGIN\n\n\n\n  NULL;\n";
    let c = comparer_with_script(input);
    let out = c.get_script();
    assert!(
        out.contains("\n\n\n\n"),
        "unterminated $$ body newlines must be preserved, got:\n{}",
        out
    );
}

#[test]
fn get_script_preserves_newlines_inside_single_quoted_string() {
    // Multi-line COMMENT body in a single-quoted literal must not be collapsed.
    let input = "COMMENT ON TABLE t IS 'line1\n\n\n\nline5';\n";
    let c = comparer_with_script(input);
    let out = c.get_script();
    assert!(
        out.contains("'line1\n\n\n\nline5'"),
        "newlines inside single-quoted string must be preserved, got:\n{}",
        out
    );
}

#[test]
fn get_script_preserves_newlines_inside_e_string() {
    // E-string literal with multi-line content.
    let input = "SELECT E'first\n\n\n\nlast';\n";
    let c = comparer_with_script(input);
    let out = c.get_script();
    assert!(
        out.contains("E'first\n\n\n\nlast'"),
        "newlines inside E-string must be preserved, got:\n{}",
        out
    );
}

#[test]
fn get_script_preserves_newlines_inside_double_quoted_identifier() {
    // Unusual but legal: double-quoted identifiers can contain newlines.
    let input = "SELECT \"col\n\n\n\nname\";\n";
    let c = comparer_with_script(input);
    let out = c.get_script();
    assert!(
        out.contains("\"col\n\n\n\nname\""),
        "newlines inside double-quoted identifier must be preserved, got:\n{}",
        out
    );
}

#[test]
fn get_script_collapses_newlines_between_single_quoted_strings() {
    // Newlines *outside* quoted strings should still be collapsed.
    let input = "SELECT 'a';\n\n\n\nSELECT 'b';\n";
    let c = comparer_with_script(input);
    let out = c.get_script();
    assert_eq!(out, "SELECT 'a';\n\nSELECT 'b';\n");
}

#[test]
fn get_script_e_string_with_escaped_quote_and_newlines() {
    // E-string with \' inside — must not terminate early.
    let input = "SELECT E'it\\'s\n\n\n\nfine';\n";
    let c = comparer_with_script(input);
    let out = c.get_script();
    assert!(
        out.contains("E'it\\'s\n\n\n\nfine'"),
        "E-string with escaped quote and newlines must be preserved, got:\n{}",
        out
    );
}

#[test]
fn get_script_single_quoted_doubled_quote_and_newlines() {
    // Standard single-quoted string with '' escape and embedded newlines.
    let input = "SELECT 'it''s\n\n\n\nfine';\n";
    let c = comparer_with_script(input);
    let out = c.get_script();
    assert!(
        out.contains("'it''s\n\n\n\nfine'"),
        "single-quoted string with doubled quote and newlines must be preserved, got:\n{}",
        out
    );
}

#[test]
fn get_script_lowercase_e_string_preserves_newlines() {
    // Lowercase e should be recognised as an E-string opener too.
    let input = "SELECT e'first\n\n\n\nlast';\n";
    let c = comparer_with_script(input);
    let out = c.get_script();
    assert!(
        out.contains("e'first\n\n\n\nlast'"),
        "newlines inside lowercase e-string must be preserved, got:\n{}",
        out
    );
}

#[test]
fn get_script_unterminated_single_quoted_string_copies_to_end() {
    let input = "SELECT 'unterminated\n\n\n\nstring\n";
    let c = comparer_with_script(input);
    let out = c.get_script();
    assert!(
        out.contains("\n\n\n\n"),
        "unterminated single-quoted string newlines must be preserved, got:\n{}",
        out
    );
}

#[test]
fn get_script_unterminated_e_string_copies_to_end() {
    let input = "SELECT E'unterminated\n\n\n\nstring\n";
    let c = comparer_with_script(input);
    let out = c.get_script();
    assert!(
        out.contains("\n\n\n\n"),
        "unterminated E-string newlines must be preserved, got:\n{}",
        out
    );
}

#[test]
fn get_script_unterminated_double_quoted_identifier_copies_to_end() {
    let input = "SELECT \"unterminated\n\n\n\nident\n";
    let c = comparer_with_script(input);
    let out = c.get_script();
    assert!(
        out.contains("\n\n\n\n"),
        "unterminated double-quoted identifier newlines must be preserved, got:\n{}",
        out
    );
}

#[test]
fn get_script_empty_single_quoted_string_no_corruption() {
    // Empty string '' should not confuse the scanner.
    let input = "SELECT '';\n\n\n\nSELECT 1;\n";
    let c = comparer_with_script(input);
    let out = c.get_script();
    assert_eq!(out, "SELECT '';\n\nSELECT 1;\n");
}

#[test]
fn get_script_newline_count_resets_after_single_quoted_literal() {
    // Two newlines before a quoted literal, then two newlines after it —
    // neither run alone exceeds 2 so nothing should be collapsed.
    let input = "A;\n\n'inside\n\n\n\ntext';\n\nB;\n";
    let c = comparer_with_script(input);
    let out = c.get_script();
    assert!(
        out.contains("'inside\n\n\n\ntext'"),
        "newlines inside literal must be preserved, got:\n{}",
        out
    );
    // The two newlines before and after the literal should survive.
    assert!(
        out.contains("A;\n\n'inside"),
        "two newlines before literal should survive, got:\n{}",
        out
    );
    assert!(
        out.contains("';\n\nB;"),
        "two newlines after literal should survive, got:\n{}",
        out
    );
}

#[test]
fn get_script_collapses_after_quoted_literal_with_excess_newlines() {
    // Excess newlines *after* a quoted literal should still be collapsed.
    let input = "SELECT 'hello';\n\n\n\nSELECT 'world';\n";
    let c = comparer_with_script(input);
    let out = c.get_script();
    assert_eq!(out, "SELECT 'hello';\n\nSELECT 'world';\n");
}

#[test]
fn get_script_mixed_literal_types_with_newlines() {
    // Mix of dollar-quoted, single-quoted, E-string, and double-quoted
    // literals each containing newlines that must be preserved, separated
    // by excessive newlines that should be collapsed.
    let input = concat!(
        "COMMENT ON TABLE t IS 'line1\n\n\n\nline5';\n",
        "\n\n\n\n",
        "SELECT E'a\n\n\n\nb';\n",
        "\n\n\n\n",
        "SELECT \"id\n\n\n\ncol\";\n",
        "\n\n\n\n",
        "CREATE FUNCTION f() AS $$\nBEGIN\n\n\n\n  NULL;\nEND;\n$$;\n",
    );
    let c = comparer_with_script(input);
    let out = c.get_script();
    // Inside literals: preserved
    assert!(
        out.contains("'line1\n\n\n\nline5'"),
        "single-quoted newlines must be preserved"
    );
    assert!(
        out.contains("E'a\n\n\n\nb'"),
        "E-string newlines must be preserved"
    );
    assert!(
        out.contains("\"id\n\n\n\ncol\""),
        "double-quoted newlines must be preserved"
    );
    assert!(
        out.contains("BEGIN\n\n\n\n  NULL;"),
        "dollar-quoted newlines must be preserved"
    );
    // Outside literals: collapsed (no run of 3+ newlines between statements)
    let between_stmts = out
        .split("'line1\n\n\n\nline5';")
        .nth(1)
        .unwrap()
        .split("E'a\n\n\n\nb'")
        .next()
        .unwrap();
    assert!(
        !between_stmts.contains("\n\n\n"),
        "newlines between statements should be collapsed, got segment: {:?}",
        between_stmts
    );
}

#[test]
fn get_script_e_string_escaped_backslash_then_newlines() {
    // E'foo\\' — the \\\\ is an escaped backslash, so the next ' closes
    // the string.  Newlines outside should be collapsed.
    let input = "SELECT E'foo\\\\';\n\n\n\nSELECT 1;\n";
    let c = comparer_with_script(input);
    let out = c.get_script();
    assert_eq!(out, "SELECT E'foo\\\\';\n\nSELECT 1;\n");
}

#[test]
fn get_script_double_quoted_doubled_escape_and_newlines() {
    // Double-quoted identifier with "" escape and embedded newlines.
    let input = "SELECT \"col\"\"\n\n\n\nname\";\n";
    let c = comparer_with_script(input);
    let out = c.get_script();
    assert!(
        out.contains("\"col\"\"\n\n\n\nname\""),
        "double-quoted identifier with escaped quote and newlines must be preserved, got:\n{}",
        out
    );
}

#[test]
fn get_script_comment_stripped_but_single_quoted_newlines_preserved() {
    // The comment-stripping pass runs first; the collapsing pass must
    // still preserve newlines inside single-quoted strings.
    let input = concat!(
        "-- strip this\n",
        "COMMENT ON TABLE t IS 'multi\n\n\n\nline';\n",
    );
    let c = comparer_with_script(input);
    let out = c.get_script();
    assert!(!out.contains("-- strip this"), "comment should be removed");
    assert!(
        out.contains("'multi\n\n\n\nline'"),
        "single-quoted newlines must survive comment stripping + collapsing, got:\n{}",
        out
    );
}

#[test]
fn get_script_block_comment_stripped_but_e_string_newlines_preserved() {
    let input = concat!("/* block comment */\n", "SELECT E'keep\n\n\n\nme';\n",);
    let c = comparer_with_script(input);
    let out = c.get_script();
    assert!(
        !out.contains("block comment"),
        "block comment should be removed"
    );
    assert!(
        out.contains("E'keep\n\n\n\nme'"),
        "E-string newlines must survive after block comment stripping, got:\n{}",
        out
    );
}

#[test]
fn get_script_adjacent_single_quoted_strings_both_preserved() {
    // Two single-quoted strings back-to-back, each with internal newlines.
    let input = "SELECT 'a\n\n\n\nb' || 'c\n\n\n\nd';\n";
    let c = comparer_with_script(input);
    let out = c.get_script();
    assert!(
        out.contains("'a\n\n\n\nb'"),
        "first literal newlines must be preserved, got:\n{}",
        out
    );
    assert!(
        out.contains("'c\n\n\n\nd'"),
        "second literal newlines must be preserved, got:\n{}",
        out
    );
}

#[test]
fn get_script_dollar_body_containing_single_quoted_newlines() {
    // A dollar-quoted body that itself contains a single-quoted string
    // with many newlines — everything inside $$ is already copied verbatim
    // by the dollar-quote branch, so the inner literal is preserved too.
    let input = concat!(
        "CREATE FUNCTION f() AS $$\n",
        "BEGIN\n",
        "  RAISE NOTICE 'msg\n\n\n\nend';\n",
        "END;\n",
        "$$;\n",
    );
    let c = comparer_with_script(input);
    let out = c.get_script();
    assert!(
        out.contains("'msg\n\n\n\nend'"),
        "single-quoted literal inside dollar body must be preserved, got:\n{}",
        out
    );
}

#[test]
fn get_script_single_quoted_string_without_excess_newlines_unchanged() {
    // A single-quoted string with exactly 2 newlines (not excess) — should
    // pass through without any modification.
    let input = "SELECT 'a\n\nb';\n";
    let c = comparer_with_script(input);
    let out = c.get_script();
    assert_eq!(out, "SELECT 'a\n\nb';\n");
}

#[test]
fn get_script_e_identifier_not_confused_with_e_string() {
    // A column named "E" followed by a comparison — the E is followed by
    // a space, not a quote, so it must not be mistaken for an E-string.
    let input = "SELECT E = 1;\n\n\n\nSELECT 2;\n";
    let c = comparer_with_script(input);
    let out = c.get_script();
    assert_eq!(out, "SELECT E = 1;\n\nSELECT 2;\n");
}

#[test]
fn get_script_multiple_e_strings_on_same_line() {
    let input = "SELECT E'x\n\n\n\ny', E'a\n\n\n\nb';\n";
    let c = comparer_with_script(input);
    let out = c.get_script();
    assert!(
        out.contains("E'x\n\n\n\ny'"),
        "first E-string newlines must be preserved, got:\n{}",
        out
    );
    assert!(
        out.contains("E'a\n\n\n\nb'"),
        "second E-string newlines must be preserved, got:\n{}",
        out
    );
}

#[tokio::test]
async fn compare_routines_procedure_with_config_params() {
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut routine = Routine::new(
        "public".to_string(),
        Oid(1),
        "test_proc".to_string(),
        "plpgsql".to_string(),
        "PROCEDURE".to_string(),
        "void".to_string(),
        "IN pvalue text".to_string(),
        None,
        None,
        "\nBEGIN\n    RAISE NOTICE 'value: %', pvalue;\nEND;\n".to_string(),
    );
    routine.config = vec![
        "search_path=public, pg_temp".to_string(),
        "lock_timeout=5s".to_string(),
    ];
    routine.hash();
    to_dump.routines.push(routine);

    let from_dump = Dump::new(DumpConfig::default());
    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Ignore);
    comparer.compare_routines().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("SET search_path = 'public, pg_temp'"),
        "script must contain SET search_path, got:\n{}",
        script
    );
    assert!(
        script.contains("SET lock_timeout = '5s'"),
        "script must contain SET lock_timeout, got:\n{}",
        script
    );
}

#[tokio::test]
async fn compare_routines_function_with_config_params() {
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut routine = Routine::new(
        "public".to_string(),
        Oid(1),
        "my_func".to_string(),
        "plpgsql".to_string(),
        "FUNCTION".to_string(),
        "integer".to_string(),
        "".to_string(),
        None,
        None,
        "\nBEGIN\n    RETURN 1;\nEND;\n".to_string(),
    );
    routine.config = vec!["work_mem=256MB".to_string()];
    routine.hash();
    to_dump.routines.push(routine);

    let from_dump = Dump::new(DumpConfig::default());
    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Ignore);
    comparer.compare_routines().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("SET work_mem = '256MB'"),
        "script must contain SET work_mem, got:\n{}",
        script
    );
    assert!(
        script.contains("VOLATILE"),
        "function flags must still be present, got:\n{}",
        script
    );
}

#[tokio::test]
async fn compare_routines_config_change_triggers_update() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_routine = Routine::new(
        "public".to_string(),
        Oid(1),
        "test_func".to_string(),
        "plpgsql".to_string(),
        "FUNCTION".to_string(),
        "integer".to_string(),
        "".to_string(),
        None,
        None,
        "BEGIN RETURN 1; END".to_string(),
    );
    from_routine.config = vec!["search_path=public".to_string()];
    from_routine.hash();

    let mut to_routine = Routine::new(
        "public".to_string(),
        Oid(1),
        "test_func".to_string(),
        "plpgsql".to_string(),
        "FUNCTION".to_string(),
        "integer".to_string(),
        "".to_string(),
        None,
        None,
        "BEGIN RETURN 1; END".to_string(),
    );
    to_routine.config = vec![
        "search_path=public".to_string(),
        "lock_timeout=5s".to_string(),
    ];
    to_routine.hash();

    assert_ne!(
        from_routine.hash, to_routine.hash,
        "hashes must differ when config changes"
    );

    from_dump.routines.push(from_routine);
    to_dump.routines.push(to_routine);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Ignore);
    comparer.compare_routines().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("create or replace function"),
        "config change must trigger CREATE OR REPLACE, got:\n{}",
        script
    );
    assert!(
        script.contains("SET search_path = 'public'"),
        "script must contain SET search_path, got:\n{}",
        script
    );
    assert!(
        script.contains("SET lock_timeout = '5s'"),
        "script must contain SET lock_timeout, got:\n{}",
        script
    );
}

#[tokio::test]
async fn compare_routines_config_removal_triggers_update() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_routine = Routine::new(
        "public".to_string(),
        Oid(1),
        "test_func".to_string(),
        "plpgsql".to_string(),
        "FUNCTION".to_string(),
        "integer".to_string(),
        "".to_string(),
        None,
        None,
        "BEGIN RETURN 1; END".to_string(),
    );
    from_routine.config = vec!["search_path=public".to_string()];
    from_routine.hash();

    let to_routine = Routine::new(
        "public".to_string(),
        Oid(1),
        "test_func".to_string(),
        "plpgsql".to_string(),
        "FUNCTION".to_string(),
        "integer".to_string(),
        "".to_string(),
        None,
        None,
        "BEGIN RETURN 1; END".to_string(),
    );

    assert_ne!(
        from_routine.hash, to_routine.hash,
        "hashes must differ when config is removed"
    );

    from_dump.routines.push(from_routine);
    to_dump.routines.push(to_routine);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Ignore);
    comparer.compare_routines().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("create or replace function"),
        "config removal must trigger CREATE OR REPLACE, got:\n{}",
        script
    );
    assert!(
        !script.contains("SET search_path"),
        "removed config must not appear in script, got:\n{}",
        script
    );
}

/// A table tracked in `recreated_tables` (e.g. due to partition key change)
/// must use the FROM default privilege ACL as its effective from_acl in full
/// grants mode, so that no spurious REVOKEs appear on repeated runs.
#[tokio::test]
async fn compare_grants_recreated_table_uses_default_acl() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    // Both FROM and TO have the same table with the same ACL.
    let mut from_table = Table::new(
        "public".to_string(),
        "orders".to_string(),
        "public".to_string(),
        "orders".to_string(),
        "owner".to_string(),
        None,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        None,
    );
    from_table.acl = vec!["reader=r/owner".to_string()];

    let mut to_table = Table::new(
        "public".to_string(),
        "orders".to_string(),
        "public".to_string(),
        "orders".to_string(),
        "owner".to_string(),
        None,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        None,
    );
    to_table.acl = vec!["reader=r/owner".to_string()];

    // Default privilege that auto-grants SELECT to reader on new tables.
    from_dump.default_privileges.push(DefaultPrivilege {
        role_name: "owner".to_string(),
        schema_name: "public".to_string(),
        object_type: "r".to_string(),
        acl: vec!["reader=r/owner".to_string()],
        hash: None,
    });

    from_dump.tables.push(from_table);
    to_dump.tables.push(to_table);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Full);
    // Simulate that the table was recreated (e.g. partition key change).
    comparer
        .recreated_tables
        .insert(Comparer::table_key("public", "orders"));
    comparer.compare_grants().await.unwrap();
    let script = comparer.get_script();

    // The default privilege matches the TO ACL, so no GRANT or REVOKE needed.
    let has_grant = script
        .lines()
        .any(|l| l.trim_start().to_lowercase().starts_with("grant "));
    let has_revoke = script
        .lines()
        .any(|l| l.trim_start().to_lowercase().starts_with("revoke "));
    assert!(
        !has_grant && !has_revoke,
        "Recreated table with matching default ACL must produce no GRANT/REVOKE, got: {script}"
    );
}

/// A recreated table whose TO ACL differs from the default privilege ACL
/// must produce the correct GRANT to bridge the gap.
#[tokio::test]
async fn compare_grants_recreated_table_grants_extra_over_default() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_table = Table::new(
        "public".to_string(),
        "orders".to_string(),
        "public".to_string(),
        "orders".to_string(),
        "owner".to_string(),
        None,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        None,
    );
    from_table.acl = vec!["reader=r/owner".to_string(), "writer=rw/owner".to_string()];

    let mut to_table = Table::new(
        "public".to_string(),
        "orders".to_string(),
        "public".to_string(),
        "orders".to_string(),
        "owner".to_string(),
        None,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        None,
    );
    to_table.acl = vec!["reader=r/owner".to_string(), "writer=rw/owner".to_string()];

    // Default privilege only grants SELECT to reader (no writer grant).
    from_dump.default_privileges.push(DefaultPrivilege {
        role_name: "owner".to_string(),
        schema_name: "public".to_string(),
        object_type: "r".to_string(),
        acl: vec!["reader=r/owner".to_string()],
        hash: None,
    });

    from_dump.tables.push(from_table);
    to_dump.tables.push(to_table);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Full);
    comparer
        .recreated_tables
        .insert(Comparer::table_key("public", "orders"));
    comparer.compare_grants().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("GRANT SELECT, UPDATE ON TABLE public.orders TO writer;"),
        "Must grant writer privileges beyond default ACL, got: {script}"
    );
    // reader already has SELECT via default, so no GRANT for reader.
    let reader_grant = script.lines().any(|l| l.contains("TO reader"));
    assert!(
        !reader_grant,
        "reader grant already covered by default ACL, got: {script}"
    );
}

/// A non-recreated table that exists in both FROM and TO must use the
/// original FROM ACL, not the default privilege ACL.
#[tokio::test]
async fn compare_grants_non_recreated_table_uses_from_acl() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_table = Table::new(
        "public".to_string(),
        "orders".to_string(),
        "public".to_string(),
        "orders".to_string(),
        "owner".to_string(),
        None,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        None,
    );
    from_table.acl = vec!["reader=r/owner".to_string()];

    let mut to_table = Table::new(
        "public".to_string(),
        "orders".to_string(),
        "public".to_string(),
        "orders".to_string(),
        "owner".to_string(),
        None,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        None,
    );
    to_table.acl = vec!["reader=r/owner".to_string()];

    // Even though default privilege differs, we must use FROM ACL.
    from_dump.default_privileges.push(DefaultPrivilege {
        role_name: "owner".to_string(),
        schema_name: "public".to_string(),
        object_type: "r".to_string(),
        acl: vec![],
        hash: None,
    });

    from_dump.tables.push(from_table);
    to_dump.tables.push(to_table);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Full);
    // Do NOT insert into recreated_tables — table is not recreated.
    comparer.compare_grants().await.unwrap();
    let script = comparer.get_script();

    // FROM and TO ACLs match, so no diff should be produced.
    let has_grant = script
        .lines()
        .any(|l| l.trim_start().to_lowercase().starts_with("grant "));
    let has_revoke = script
        .lines()
        .any(|l| l.trim_start().to_lowercase().starts_with("revoke "));
    assert!(
        !has_grant && !has_revoke,
        "Non-recreated table with identical ACLs must produce no GRANT/REVOKE, got: {script}"
    );
}

/// A dropped+recreated view (use_drop=true) in full grants mode must use
/// the default privilege ACL as the effective from_acl, matching the table
/// recreated-object logic.
#[tokio::test]
async fn compare_grants_dropped_view_uses_default_acl_full_mode() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_view = View::new(
        "my_view".to_string(),
        "SELECT 1".to_string(),
        "public".to_string(),
        Vec::new(),
    );
    from_view.acl = vec!["reader=r/owner".to_string()];

    let mut to_view = View::new(
        "my_view".to_string(),
        "SELECT 1".to_string(),
        "public".to_string(),
        Vec::new(),
    );
    to_view.acl = vec!["reader=r/owner".to_string()];

    // Default privilege auto-grants SELECT to reader on new tables/views.
    from_dump.default_privileges.push(DefaultPrivilege {
        role_name: "owner".to_string(),
        schema_name: "public".to_string(),
        object_type: "r".to_string(),
        acl: vec!["reader=r/owner".to_string()],
        hash: None,
    });

    from_dump.views.push(from_view);
    to_dump.views.push(to_view);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Full);
    // Simulate that the view was actually dropped (use_drop=true).
    comparer
        .dropped_views
        .insert(Comparer::normalized_view_key("public", "my_view"), true);
    comparer.compare_grants().await.unwrap();
    let script = comparer.get_script();

    // Default ACL matches TO ACL, so no diff needed.
    let has_grant = script
        .lines()
        .any(|l| l.trim_start().to_lowercase().starts_with("grant "));
    let has_revoke = script
        .lines()
        .any(|l| l.trim_start().to_lowercase().starts_with("revoke "));
    assert!(
        !has_grant && !has_revoke,
        "Dropped view with matching default ACL must produce no GRANT/REVOKE, got: {script}"
    );
}

/// A dropped view (use_drop=true) in full mode whose TO ACL has more
/// privileges than the default must produce GRANTs to bridge the gap.
#[tokio::test]
async fn compare_grants_dropped_view_grants_extra_over_default() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_view = View::new(
        "my_view".to_string(),
        "SELECT 1".to_string(),
        "public".to_string(),
        Vec::new(),
    );
    from_view.acl = vec!["reader=r/owner".to_string(), "writer=rw/owner".to_string()];

    let mut to_view = View::new(
        "my_view".to_string(),
        "SELECT 1".to_string(),
        "public".to_string(),
        Vec::new(),
    );
    to_view.acl = vec!["reader=r/owner".to_string(), "writer=rw/owner".to_string()];

    // Default only gives reader SELECT.
    from_dump.default_privileges.push(DefaultPrivilege {
        role_name: "owner".to_string(),
        schema_name: "public".to_string(),
        object_type: "r".to_string(),
        acl: vec!["reader=r/owner".to_string()],
        hash: None,
    });

    from_dump.views.push(from_view);
    to_dump.views.push(to_view);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Full);
    comparer
        .dropped_views
        .insert(Comparer::normalized_view_key("public", "my_view"), true);
    comparer.compare_grants().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("GRANT SELECT, UPDATE ON TABLE public.my_view TO writer;"),
        "Must grant writer privileges beyond default ACL for dropped view, got: {script}"
    );
}

/// When table ownership changes between FROM and TO, column-level ACL
/// diffing must exclude both old and new owners from the diff so that
/// implicit-privilege entries do not produce spurious GRANT/REVOKE.
#[tokio::test]
async fn compare_column_grants_excludes_both_old_and_new_owner() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    // FROM table owned by old_owner with column ACL for old_owner
    let mut from_col = int_column("public", "users", "secret", 1);
    from_col.acl = vec!["old_owner=r/old_owner".to_string()];
    let from_table = Table::new(
        "public".to_string(),
        "users".to_string(),
        "public".to_string(),
        "users".to_string(),
        "old_owner".to_string(),
        None,
        vec![from_col],
        Vec::new(),
        Vec::new(),
        Vec::new(),
        None,
    );

    // TO table owned by new_owner with column ACL for new_owner
    let mut to_col = int_column("public", "users", "secret", 1);
    to_col.acl = vec!["new_owner=r/new_owner".to_string()];
    let to_table = Table::new(
        "public".to_string(),
        "users".to_string(),
        "public".to_string(),
        "users".to_string(),
        "new_owner".to_string(),
        None,
        vec![to_col],
        Vec::new(),
        Vec::new(),
        Vec::new(),
        None,
    );

    from_dump.tables.push(from_table);
    to_dump.tables.push(to_table);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Full);
    comparer.compare_grants().await.unwrap();
    let script = comparer.get_script();

    // Both old_owner and new_owner have implicit privileges as owners,
    // so no GRANT or REVOKE should appear for them on the column.
    let has_col_grant = script
        .lines()
        .any(|l| l.contains("secret") && l.trim_start().to_lowercase().starts_with("grant "));
    let has_col_revoke = script
        .lines()
        .any(|l| l.contains("secret") && l.trim_start().to_lowercase().starts_with("revoke "));
    assert!(
        !has_col_grant && !has_col_revoke,
        "Owner ACL entries must be excluded for both old and new owner, got: {script}"
    );
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
