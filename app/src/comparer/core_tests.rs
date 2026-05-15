use super::*;
use crate::config::dump_config::DumpConfig;
use crate::config::grants_mode::GrantsMode;
use crate::dump::default_privilege::DefaultPrivilege;
use crate::dump::extension::Extension;
use crate::dump::foreign_table::ForeignTable;
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
async fn compare_grants_foreign_table_add_and_revoke() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_ft = ForeignTable::new(
        "public".to_string(),
        "ft_orders".to_string(),
        "fdw_server".to_string(),
        "owner".to_string(),
        Vec::new(),
        Vec::new(),
    );
    from_ft.acl = vec!["reader=r/owner".to_string()];

    let mut to_ft = ForeignTable::new(
        "public".to_string(),
        "ft_orders".to_string(),
        "fdw_server".to_string(),
        "owner".to_string(),
        Vec::new(),
        Vec::new(),
    );
    to_ft.acl = vec!["writer=rw/owner".to_string()];

    from_dump.foreign_tables.push(from_ft);
    to_dump.foreign_tables.push(to_ft);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Full);
    comparer.compare_grants().await.unwrap();
    let script = comparer.get_script();

    // PostgreSQL's GRANT syntax has no `ON FOREIGN TABLE` form — foreign
    // tables share the regular `ON TABLE` grant syntax. Pre-fix the
    // comparer emitted `ON FOREIGN TABLE`, which produced invalid SQL
    // (`syntax error at or near "TABLE"`) when the diff was applied.
    assert!(
        script.contains("GRANT SELECT, UPDATE ON TABLE public.ft_orders TO writer;"),
        "Full must add foreign table grant via ON TABLE syntax, got: {script}"
    );
    assert!(
        script.contains("REVOKE SELECT ON TABLE public.ft_orders FROM reader;"),
        "Full must revoke removed foreign table grant via ON TABLE syntax, got: {script}"
    );
    assert!(
        !script.contains("ON FOREIGN TABLE"),
        "Foreign table grants must not use `ON FOREIGN TABLE` (invalid SQL), got: {script}"
    );
}

/// User-reported regression: when ownership changes AND TO has an explicit
/// grant to the former owner, the migration must emit exactly one GRANT
/// (for the explicit privilege in TO) and zero REVOKEs (the implicit-owner
/// ACL row is stripped by ALTER OWNER alone). Replays the exact ACL shape
/// you'd see in the schema_a → schema_b owner-change scenario after both
/// FROM and TO have run their explicit GRANTs and PG has materialised the
/// implicit-owner row.
#[tokio::test]
async fn compare_grants_owner_change_with_explicit_grant_to_former_owner_is_idempotent() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    // FROM table: owned by pgc_owner_from. relacl carries the implicit-
    // owner row (pg materialises it once any GRANT exists) plus the two
    // explicit grants to reader/writer.
    let mut from_table = Table::new(
        "test_schema".to_string(),
        "users".to_string(),
        "test_schema".to_string(),
        "users".to_string(),
        "pgc_owner_from".to_string(),
        None,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        None,
    );
    from_table.acl = vec![
        "pgc_owner_from=arwdDxt/pgc_owner_from".to_string(),
        "pgc_grant_reader=r/pgc_owner_from".to_string(),
        "pgc_grant_writer=arw/pgc_owner_from".to_string(),
    ];

    // TO table: owned by pgc_owner_to. relacl has the new implicit-owner
    // row, the same reader grant, the writer with UPDATE removed, and an
    // explicit grant to the former owner pgc_owner_from.
    let mut to_table = Table::new(
        "test_schema".to_string(),
        "users".to_string(),
        "test_schema".to_string(),
        "users".to_string(),
        "pgc_owner_to".to_string(),
        None,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        None,
    );
    to_table.acl = vec![
        "pgc_owner_to=arwdDxt/pgc_owner_to".to_string(),
        "pgc_owner_from=r/pgc_owner_to".to_string(),
        "pgc_grant_reader=r/pgc_owner_to".to_string(),
        "pgc_grant_writer=ar/pgc_owner_to".to_string(),
    ];

    from_dump.tables.push(from_table);
    to_dump.tables.push(to_table);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Full);
    comparer.compare_grants().await.unwrap();
    let script = comparer.get_script();

    // Exactly two statements expected:
    //   - GRANT SELECT TO pgc_owner_from (the new explicit grant in TO)
    //   - REVOKE UPDATE FROM pgc_grant_writer (UPDATE removed in TO)
    assert!(
        script.contains("GRANT SELECT ON TABLE test_schema.users TO pgc_owner_from;"),
        "Must emit explicit grant to former owner, got: {script}"
    );
    assert!(
        script.contains("REVOKE UPDATE ON TABLE test_schema.users FROM pgc_grant_writer;"),
        "Must revoke writer's UPDATE removed in TO, got: {script}"
    );
    // No REVOKE/GRANT for pgc_owner_to (TO owner — implicit privileges).
    // No REVOKE for pgc_owner_from's old implicit-owner row — ALTER OWNER
    // strips it. Specifically NO REVOKE on pgc_owner_from for the 7 other
    // privileges, which is the bug this regression test guards against.
    assert!(
        !script.contains("FROM pgc_owner_from"),
        "Must not REVOKE anything from former owner — ALTER OWNER strips the implicit row, got: {script}"
    );
    assert!(
        !script.contains("pgc_owner_to"),
        "Current owner must not appear in grants output, got: {script}"
    );
}

/// Regression: a TO-only foreign table must inherit the FROM database's
/// default-table privileges as the effective `from_acl` under `full` mode,
/// because PostgreSQL auto-applies them on CREATE. Without this, the diff
/// is non-idempotent — re-running compare after applying it would emit
/// `REVOKE` statements for the auto-granted privileges that the migration
/// itself is responsible for cleaning up.
#[tokio::test]
async fn compare_grants_new_foreign_table_revokes_default_priv_grants() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    // FROM has a default-privilege rule that grants SELECT to `reader` on
    // any new table in `public`. No explicit grants in TO → after CREATE,
    // the auto-applied SELECT must be revoked in this same diff.
    let dp = DefaultPrivilege {
        role_name: String::new(),
        schema_name: "public".to_string(),
        object_type: "r".to_string(),
        acl: vec!["reader=r/owner".to_string()],
        hash: Some("dp".to_string()),
    };
    from_dump.default_privileges.push(dp);

    // TO-only foreign table (no FROM counterpart, no explicit ACL).
    let to_ft = ForeignTable::new(
        "public".to_string(),
        "ft_new".to_string(),
        "fdw_server".to_string(),
        "owner".to_string(),
        Vec::new(),
        Vec::new(),
    );
    to_dump.foreign_tables.push(to_ft);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Full);
    comparer.compare_grants().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("REVOKE SELECT ON TABLE public.ft_new FROM reader;"),
        "New foreign table must revoke auto-applied default-privilege grants under full mode, got: {script}"
    );
    assert!(
        !script.contains("ON FOREIGN TABLE"),
        "Foreign table grants must use `ON TABLE` syntax, got: {script}"
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

    // The `old_owner=arwdDxt/old_owner` and `new_owner=arwdDxt/new_owner`
    // entries are PostgreSQL's implicit-owner ACL rows (materialised once
    // any GRANT exists). `ALTER TABLE ... OWNER TO new_owner` removes
    // old_owner's implicit row and adds new_owner's automatically — no
    // REVOKE/GRANT is needed for those rows. Reader is unchanged on both
    // sides. Net diff: empty. Pre-fix the comparer treated the implicit
    // FROM-owner row as if it would persist post-migration and emitted a
    // long REVOKE, then on the next compare run had nothing to compare
    // against and emitted GRANTs — a non-idempotent oscillation.
    assert!(
        !script.contains("REVOKE"),
        "ALTER OWNER alone strips the implicit-owner entry; no REVOKE should be emitted, got: {script}"
    );
    assert!(
        !script.contains("GRANT"),
        "No grants expected — reader is unchanged and new_owner gets implicit privileges via ALTER OWNER, got: {script}"
    );
    assert!(
        !script.contains("new_owner"),
        "Must not reference the new owner explicitly, got: {script}"
    );
    assert!(
        !script.contains("old_owner"),
        "Must not reference the former owner explicitly when only the implicit-owner ACL row needs to migrate, got: {script}"
    );
}

#[tokio::test]
async fn compare_grants_full_revokes_explicit_grants_from_former_owner() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_schema = Schema::new("billing".to_string(), "billing".to_string(), None);
    from_schema.owner = "old_owner".to_string();
    from_schema.acl = vec!["old_owner=UC/old_owner".to_string()];

    let mut to_schema = Schema::new("billing".to_string(), "billing".to_string(), None);
    to_schema.owner = "new_owner".to_string();

    let mut from_table = Table::new(
        "billing".to_string(),
        "invoice".to_string(),
        "billing".to_string(),
        "invoice".to_string(),
        "old_owner".to_string(),
        None,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        None,
    );
    from_table.acl = vec!["old_owner=ar/old_owner".to_string()];

    let to_table = Table::new(
        "billing".to_string(),
        "invoice".to_string(),
        "billing".to_string(),
        "invoice".to_string(),
        "new_owner".to_string(),
        None,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        None,
    );

    from_dump.schemas.push(from_schema);
    from_dump.tables.push(from_table);
    to_dump.schemas.push(to_schema);
    to_dump.tables.push(to_table);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Full);
    comparer.compare_grants().await.unwrap();
    let script = comparer.get_script();

    // Same reasoning as `compare_grants_excludes_owner_acl_entries`:
    // `old_owner=UC/old_owner` and `old_owner=ar/old_owner` are
    // implicit-owner ACL entries that `ALTER ... OWNER TO new_owner`
    // strips automatically. Comparing against TO (which has no entries
    // at all) should produce an empty diff, not REVOKE statements.
    assert!(
        !script.contains("REVOKE"),
        "Implicit-owner ACL entries are removed by ALTER OWNER alone; no REVOKE should be emitted, got: {script}"
    );
    assert!(
        !script.contains("new_owner"),
        "Current owner must not appear in grant/revoke output, got: {script}"
    );
    assert!(
        !script.contains("old_owner"),
        "Former owner must not appear in grant/revoke output for the implicit-owner ACL row, got: {script}"
    );
}

#[tokio::test]
async fn compare_grants_emits_explicit_grants_to_former_owner() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_schema = Schema::new("billing".to_string(), "billing".to_string(), None);
    from_schema.owner = "old_owner".to_string();

    let mut to_schema = Schema::new("billing".to_string(), "billing".to_string(), None);
    to_schema.owner = "new_owner".to_string();
    to_schema.acl = vec![
        "old_owner=UC/new_owner".to_string(),
        "app_user=U/new_owner".to_string(),
    ];

    let from_table = Table::new(
        "billing".to_string(),
        "invoice".to_string(),
        "billing".to_string(),
        "invoice".to_string(),
        "old_owner".to_string(),
        None,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        None,
    );

    let mut to_table = Table::new(
        "billing".to_string(),
        "invoice".to_string(),
        "billing".to_string(),
        "invoice".to_string(),
        "new_owner".to_string(),
        None,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        None,
    );
    to_table.acl = vec![
        "old_owner=ar/new_owner".to_string(),
        "app_user=r/new_owner".to_string(),
    ];

    from_dump.schemas.push(from_schema);
    from_dump.tables.push(from_table);
    to_dump.schemas.push(to_schema);
    to_dump.tables.push(to_table);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::AddOnly);
    comparer.compare_grants().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("GRANT CREATE, USAGE ON SCHEMA billing TO old_owner;"),
        "Former schema owner must receive explicit TO grant, got: {script}"
    );
    assert!(
        script.contains("GRANT USAGE ON SCHEMA billing TO app_user;"),
        "Non-owner schema grant must still be emitted, got: {script}"
    );
    assert!(
        script.contains("GRANT INSERT, SELECT ON TABLE billing.invoice TO old_owner;"),
        "Former table owner must receive explicit TO grant, got: {script}"
    );
    assert!(
        script.contains("GRANT SELECT ON TABLE billing.invoice TO app_user;"),
        "Non-owner table grant must still be emitted, got: {script}"
    );
    assert!(
        !script.contains("TO new_owner"),
        "Current owner must not receive explicit grants, got: {script}"
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
/// diffing must keep former-owner entries diffable while suppressing
/// current-owner implicit privilege entries.
#[tokio::test]
async fn compare_column_grants_revokes_former_owner_and_excludes_current_owner() {
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

    assert!(
        script.contains("REVOKE SELECT (secret) ON TABLE public.users FROM old_owner;"),
        "Former owner column ACL must remain diffable in full mode, got: {script}"
    );
    assert!(
        !script.contains("new_owner"),
        "Current owner column ACL entries must be suppressed, got: {script}"
    );
    assert!(
        !script
            .lines()
            .any(|l| l.contains("secret") && l.trim_start().to_lowercase().starts_with("grant ")),
        "Unexpected column GRANT for owner ACL entries, got: {script}"
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

// --- Post-buffer ordering (Comparer::compare concatenates several ordered
//     script buffers: main → sequence_post → type_post → enum_post →
//     trigger_post. These tests pin the emission order so dependency-aware
//     rearrangements don't regress silently). ---

#[tokio::test]
async fn buffer_ordering_type_drop_before_enum_drop() {
    // Both buffers are populated when the FROM dump carries a domain type
    // AND an enum that are both absent in the TO dump. type_post must come
    // before enum_post in the final script (so enums outlive types that may
    // reference them).
    let mut from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());

    from_dump
        .types
        .push(make_domain_type("test_schema", "dropped_domain", 701));
    from_dump.types.push(make_enum_type(
        "test_schema",
        "dropped_enum",
        702,
        vec!["a", "b"],
    ));

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();

    let type_drop_pos = script
        .find("drop type if exists test_schema.dropped_domain cascade;")
        .unwrap_or_else(|| panic!("domain drop missing in:\n{script}"));
    let enum_drop_pos = script
        .find("drop type if exists test_schema.dropped_enum cascade;")
        .unwrap_or_else(|| panic!("enum drop missing in:\n{script}"));
    assert!(
        type_drop_pos < enum_drop_pos,
        "type_post_script must precede enum_post_script, got:\n{script}"
    );
}

#[tokio::test]
async fn buffer_ordering_enum_drop_before_trigger_create() {
    // FROM has an enum to drop (populates enum_post_script).
    // TO has a brand-new table with a trigger (populates trigger_post_script
    // for the CREATE TRIGGER). enum_post must come before trigger_post so
    // that triggers referencing newly-created routines/types run after all
    // type-dependency cleanup.
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    from_dump.types.push(make_enum_type(
        "test_schema",
        "legacy_status",
        703,
        vec!["ok", "err"],
    ));

    let mut new_table = Table::new(
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
            definition:
                "create trigger trg_events_audit before insert on public.events for each row execute function audit()"
                    .to_string(),
            enabled: "O".to_string(),
            comment: None,
        }],
        None,
    );
    new_table.hash();
    to_dump.tables.push(new_table);

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();

    let enum_drop_pos = script
        .find("drop type if exists test_schema.legacy_status cascade;")
        .unwrap_or_else(|| panic!("enum drop missing in:\n{script}"));
    let trigger_create_pos = script
        .find("create trigger trg_events_audit")
        .unwrap_or_else(|| panic!("CREATE TRIGGER missing in:\n{script}"));

    assert!(
        enum_drop_pos < trigger_create_pos,
        "enum_post_script must precede trigger_post_script, got:\n{script}"
    );
}

#[tokio::test]
async fn buffer_ordering_sequence_drop_before_type_drop() {
    // FROM has an unowned sequence and a domain type, both absent in TO.
    // sequence_post_script is emitted before type_post_script so that
    // sequences with default-value dependencies on types are dropped first.
    let mut from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());

    let seq = crate::dump::sequence::Sequence::new(
        "test_schema".to_string(),
        "dropped_seq".to_string(),
        "postgres".to_string(),
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
    from_dump.sequences.push(seq);
    from_dump
        .types
        .push(make_domain_type("test_schema", "dropped_domain", 704));

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();

    let seq_drop_pos = script
        .find("drop sequence if exists \"test_schema\".\"dropped_seq\"")
        .or_else(|| script.find("drop sequence if exists test_schema.dropped_seq"))
        .unwrap_or_else(|| panic!("sequence drop missing in:\n{script}"));
    let type_drop_pos = script
        .find("drop type if exists test_schema.dropped_domain cascade;")
        .unwrap_or_else(|| panic!("type drop missing in:\n{script}"));

    assert!(
        seq_drop_pos < type_drop_pos,
        "sequence_post_script must precede type_post_script, got:\n{script}"
    );
}

/// Regression for the dependency-scan needle bug. Dump fields are populated
/// via `quote_ident`, so a mixed-case identifier comes back literally
/// quoted (`"MyView"`). Previously the needle kept the quotes and the
/// quote-stripped haystack flavour could never match an unquoted reference,
/// silently dropping a real dependency.
#[test]
fn text_references_qualified_name_pre_matches_unquoted_reference() {
    let (lower, unquoted_lower) = Comparer::prelower_pair("SELECT * FROM public.regular_view;");
    // Needle as built from `quote_ident` for a mixed-case identifier.
    assert!(Comparer::text_references_qualified_name_pre(
        &lower,
        &unquoted_lower,
        "\"public\"",
        "\"regular_view\"",
    ));
}

#[test]
fn text_references_qualified_name_pre_still_matches_quoted_reference() {
    let (lower, unquoted_lower) = Comparer::prelower_pair("SELECT * FROM \"MySchema\".\"MyView\";");
    assert!(Comparer::text_references_qualified_name_pre(
        &lower,
        &unquoted_lower,
        "\"myschema\"",
        "\"myview\"",
    ));
}

/// Counterpart to `compare_column_grants_revokes_former_owner_and_excludes_current_owner`:
/// when ownership changes and the new TO has *no* explicit column ACL at all
/// (only the implicit owner privileges), a former owner's column grant in
/// FROM must still be revoked under `full` mode. Without this we would leak
/// the old owner's column-level access into the post-migration database.
#[tokio::test]
async fn compare_column_grants_revokes_former_owner_when_to_has_no_column_acl() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

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

    // No column ACL in TO.
    let to_col = int_column("public", "users", "secret", 1);
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

    assert!(
        script.contains("REVOKE SELECT (secret) ON TABLE public.users FROM old_owner;"),
        "Former owner column ACL must be revoked even without ACL in TO, got: {script}"
    );
    assert!(
        !script.contains("new_owner"),
        "Current owner must never appear in column grant output, got: {script}"
    );
}

/// Regression test for the per-table column-ACL HashMap rewrite. Previously
/// each TO column did a linear scan over `from_cols`; the rewrite indexes
/// `from_cols` by name once per table. This test exercises a table with
/// multiple columns where each column's effective `from_acl` differs, to
/// catch off-by-one mistakes that a single-column test would miss.
#[tokio::test]
async fn compare_column_grants_dispatches_per_column_acl_correctly() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    // FROM: three columns with distinct ACL states.
    let mut from_a = int_column("public", "t", "a", 1);
    from_a.acl = vec!["reader=r/owner".to_string()];
    let mut from_b = int_column("public", "t", "b", 2);
    from_b.acl = vec!["reader=r/owner".to_string()];
    let from_c = int_column("public", "t", "c", 3); // no ACL in FROM

    let from_table = Table::new(
        "public".to_string(),
        "t".to_string(),
        "public".to_string(),
        "t".to_string(),
        "owner".to_string(),
        None,
        vec![from_a, from_b, from_c],
        Vec::new(),
        Vec::new(),
        Vec::new(),
        None,
    );

    // TO: a kept, b loses its grant, c gains a grant.
    let mut to_a = int_column("public", "t", "a", 1);
    to_a.acl = vec!["reader=r/owner".to_string()];
    let to_b = int_column("public", "t", "b", 2); // grant should be revoked
    let mut to_c = int_column("public", "t", "c", 3);
    to_c.acl = vec!["writer=a/owner".to_string()]; // INSERT grant added

    let to_table = Table::new(
        "public".to_string(),
        "t".to_string(),
        "public".to_string(),
        "t".to_string(),
        "owner".to_string(),
        None,
        vec![to_a, to_b, to_c],
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

    // a: identical → nothing emitted for column a.
    assert!(
        !script.contains("(a)"),
        "column a is unchanged and must not appear, got: {script}"
    );
    // b: REVOKE for the dropped grant.
    assert!(
        script.contains("REVOKE SELECT (b) ON TABLE public.t FROM reader;"),
        "expected REVOKE for column b, got: {script}"
    );
    // c: GRANT for the added INSERT privilege.
    assert!(
        script.contains("GRANT INSERT (c) ON TABLE public.t TO writer;"),
        "expected GRANT INSERT on column c, got: {script}"
    );
    // Sanity: no cross-talk where column b's REVOKE refers to writer/c, etc.
    assert!(
        !script.contains("REVOKE SELECT (c)"),
        "column c had no FROM grant and must not be revoked, got: {script}"
    );
    assert!(
        !script.contains("GRANT INSERT (a)") && !script.contains("GRANT INSERT (b)"),
        "INSERT grant must be scoped to column c only, got: {script}"
    );
}

/// Regression test for the `serial_columns` key change from a joined
/// `"schema.table.column"` `String` to a `(String, String, String)` tuple.
/// The old form was parsed back via `splitn(3, '.')`, which silently
/// misparsed any identifier containing a literal `.` (legal in PostgreSQL
/// when quoted). With the tuple key, dotted identifiers round-trip cleanly
/// and `mark_serial_columns` still finds the target column.
#[tokio::test]
async fn mark_serial_columns_handles_dotted_identifier_names() {
    let from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    // Schema, table, and column names all contain a literal dot — the
    // pre-fix `splitn(3, '.')` would slice these in the wrong place and
    // fail to locate the column.
    let schema = "weird.schema";
    let table = "weird.table";
    let column = "weird.id";

    let serial_seq = Sequence::new(
        schema.to_string(),
        format!("{table}_{column}_seq"),
        "postgres".to_string(),
        "integer".to_string(),
        Some(1),
        Some(1),
        Some(2147483647),
        Some(1),
        false,
        Some(1),
        Some(1),
        Some(schema.to_string()),
        Some(table.to_string()),
        Some(column.to_string()),
    );
    to_dump.sequences.push(serial_seq);

    let mut col = int_column(schema, table, column, 1);
    col.column_default = Some(format!(
        "nextval('{schema}.{table}_{column}_seq'::regclass)"
    ));
    col.is_nullable = false;

    let table_obj = Table::new(
        schema.to_string(),
        table.to_string(),
        schema.to_string(),
        table.to_string(),
        "postgres".to_string(),
        None,
        vec![col],
        Vec::new(),
        Vec::new(),
        Vec::new(),
        None,
    );
    to_dump.tables.push(table_obj);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Ignore);
    comparer.compare_sequences().await.unwrap();
    comparer.mark_serial_columns();

    let to_table = comparer
        .to
        .tables
        .iter()
        .find(|t| t.schema == schema && t.name == table)
        .expect("table must round-trip");
    let to_column = to_table
        .columns
        .iter()
        .find(|c| c.name == column)
        .expect("column must round-trip");
    assert_eq!(
        to_column.serial_type.as_deref(),
        Some("serial"),
        "dotted-name column must still be marked as serial"
    );
}

// ─────────────────────────────────────────────────────────────────────
// Issue #179 — DROP FUNCTION ... CASCADE silently drops dependent
// objects (functional indexes, CHECK constraints, generated columns,
// column DEFAULT expressions, RLS policies). Phase 7 of
// `compare_routines_and_views` re-emits them.
// ─────────────────────────────────────────────────────────────────────

use crate::dump::table_index::TableIndex;
use crate::dump::table_policy::TablePolicy;

/// Build a `Routine` mirroring `test_deps.compute(x integer)` from the
/// issue report, parameterised by return type so a single helper covers
/// both the FROM (integer) and TO (bigint) sides.
fn issue179_compute_routine(return_type: &str, body: &str) -> Routine {
    let mut routine = Routine::new(
        "test_deps".to_string(),
        Oid(900),
        "compute".to_string(),
        "sql".to_string(),
        "FUNCTION".to_string(),
        return_type.to_string(),
        "x integer".to_string(),
        None,
        None,
        body.to_string(),
    );
    routine.hash();
    routine
}

/// Construct an `items` table that mirrors the issue's example: each
/// dependent (functional index, CHECK constraint, generated column,
/// column DEFAULT, RLS policy) references `test_deps.compute`.
fn issue179_items_table(value_type: &str, def_default: &str, gen_type: &str) -> Table {
    let mut def_col = int_column("test_deps", "items", "def_col", 2);
    def_col.data_type = value_type.to_string();
    def_col.column_default = Some(def_default.to_string());

    let mut gen_col = int_column("test_deps", "items", "gen_col", 3);
    gen_col.data_type = gen_type.to_string();
    gen_col.is_generated = "ALWAYS".to_string();
    gen_col.generation_expression = Some("test_deps.compute(value)".to_string());
    gen_col.generation_type = Some("s".to_string());

    let mut value_col = int_column("test_deps", "items", "value", 1);
    value_col.is_nullable = false;

    let chk_constraint = TableConstraint {
        catalog: "postgres".to_string(),
        schema: "test_deps".to_string(),
        name: "chk_compute".to_string(),
        table_name: "items".to_string(),
        constraint_type: "CHECK".to_string(),
        is_deferrable: false,
        initially_deferred: false,
        definition: Some("CHECK (test_deps.compute(value) > 0)".to_string()),
        coninhcount: 0,
        is_enforced: true,
        no_inherit: false,
        nulls_not_distinct: false,
        comment: None,
    };

    let idx = TableIndex {
        schema: "test_deps".to_string(),
        table: "items".to_string(),
        name: "idx_compute".to_string(),
        catalog: Some("postgres".to_string()),
        indexdef:
            "CREATE INDEX idx_compute ON test_deps.items USING btree (test_deps.compute(value))"
                .to_string(),
        is_partition_index: false,
        comment: None,
    };

    let policy = TablePolicy {
        schema: "test_deps".to_string(),
        table: "items".to_string(),
        name: "p_items".to_string(),
        command: "all".to_string(),
        permissive: true,
        roles: vec![],
        using_clause: Some("(test_deps.compute(value) > 0)".to_string()),
        check_clause: None,
    };

    let mut table = Table::new(
        "test_deps".to_string(),
        "items".to_string(),
        "test_deps".to_string(),
        "items".to_string(),
        "postgres".to_string(),
        None,
        vec![value_col, def_col, gen_col],
        vec![chk_constraint],
        vec![idx],
        vec![],
        None,
    );
    table.policies = vec![policy];
    table.has_rowsecurity = true;
    table.hash();
    table
}

#[tokio::test]
async fn issue179_signature_change_recreates_all_cascade_dependents() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    from_dump
        .routines
        .push(issue179_compute_routine("integer", "SELECT x * 2;"));
    to_dump.routines.push(issue179_compute_routine(
        "bigint",
        "SELECT (x * 2)::bigint;",
    ));

    from_dump.tables.push(issue179_items_table(
        "integer",
        "test_deps.compute(0)::integer",
        "integer",
    ));
    to_dump.tables.push(issue179_items_table(
        "bigint",
        "test_deps.compute(0)",
        "bigint",
    ));

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_routines_and_views().await.unwrap();
    let script = comparer.get_script();

    let drop_pos = script
        .find("drop function if exists test_deps.compute (x integer) cascade;")
        .expect("CASCADE drop must be emitted for the signature change");
    let create_pos = script
        .find("create or replace function test_deps.compute(x integer) returns bigint")
        .expect("function recreate must be emitted");
    assert!(drop_pos < create_pos);

    // The recreate phase must run AFTER the function is recreated so the
    // dependent objects can be created against the new function.
    let chk_pos = script
        .find("alter table test_deps.items add constraint chk_compute")
        .expect("CHECK constraint must be re-added after CASCADE");
    assert!(
        create_pos < chk_pos,
        "CHECK must be re-added after function recreate"
    );
    assert!(
        script.contains("alter table test_deps.items drop constraint if exists chk_compute;"),
        "drop-if-exists guard for CHECK constraint missing: {}",
        script
    );

    let idx_pos = script
        .find("CREATE INDEX IF NOT EXISTS idx_compute ON test_deps.items")
        .expect("functional index must be re-created (CREATE INDEX IF NOT EXISTS) after CASCADE");
    assert!(create_pos < idx_pos);
    // Index recreate is now non-destructive: no separate DROP INDEX is
    // emitted (a false-positive match must not silently invalidate a
    // surviving index — see Phase 7 / issue #179 review thread).
    assert!(
        !script.contains("drop index if exists test_deps.idx_compute;"),
        "DROP INDEX must not be emitted; recreate uses CREATE INDEX IF NOT EXISTS"
    );

    // Generated column recreate is non-destructive: ADD COLUMN IF NOT
    // EXISTS, no DROP COLUMN. A drop here would cascade to attached
    // indexes / FKs / constraints that Phase 7 cannot restore.
    assert!(
        !script.contains("alter table test_deps.items drop column if exists gen_col"),
        "DROP COLUMN must not be emitted for generated column recreate"
    );
    assert!(
        script.contains("alter table test_deps.items add column if not exists gen_col bigint generated always as (test_deps.compute(value)) stored;"),
        "generated column must be re-added (IF NOT EXISTS) with TO type/expression"
    );

    // Column DEFAULT: column survives, only the default is gone.
    assert!(
        script.contains(
            "alter table test_deps.items alter column def_col set default test_deps.compute(0);"
        ),
        "column default must be restored from TO"
    );
    // We must NOT drop+re-add a non-generated column whose default was cascaded:
    assert!(
        !script.contains("drop column if exists def_col"),
        "non-generated column must survive — only its DEFAULT clause was cascaded"
    );

    let policy_pos = script
        .find("create policy p_items on test_deps.items")
        .expect("policy must be re-created after CASCADE");
    assert!(create_pos < policy_pos);
    assert!(
        script.contains("drop policy if exists p_items on test_deps.items;"),
        "drop-if-exists guard for policy missing"
    );
}

#[tokio::test]
async fn issue179_recreate_skipped_when_routine_unchanged() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let routine = issue179_compute_routine("integer", "SELECT x * 2;");
    from_dump.routines.push(routine.clone());
    to_dump.routines.push(routine);

    from_dump.tables.push(issue179_items_table(
        "integer",
        "test_deps.compute(0)",
        "integer",
    ));
    to_dump.tables.push(issue179_items_table(
        "integer",
        "test_deps.compute(0)",
        "integer",
    ));

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_routines_and_views().await.unwrap();
    let script = comparer.get_script();

    assert!(
        !script.contains("Recreate dependents dropped by CASCADE"),
        "no CASCADE drop happened — recreate phase must stay silent: {}",
        script
    );
    assert!(!script.contains("alter table test_deps.items add constraint chk_compute"));
    assert!(!script.contains("CREATE INDEX idx_compute"));
    assert!(!script.contains("create policy p_items"));
}

#[tokio::test]
async fn issue179_recreate_skipped_when_to_dependent_missing() {
    // Function is dropped entirely. The dependent objects are also gone
    // in TO (user removed both function and dependents). We must NOT
    // resurrect the dependents — they're intentionally absent.
    let mut from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());

    from_dump
        .routines
        .push(issue179_compute_routine("integer", "SELECT x * 2;"));
    from_dump.tables.push(issue179_items_table(
        "integer",
        "test_deps.compute(0)",
        "integer",
    ));

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_routines_and_views().await.unwrap();
    let script = comparer.get_script();

    assert!(
        !script.contains("alter table test_deps.items add constraint chk_compute"),
        "CHECK must not be resurrected when neither it nor the function exist in TO"
    );
    assert!(
        !script.contains("CREATE INDEX idx_compute"),
        "index must not be resurrected when neither it nor the function exist in TO"
    );
    assert!(
        !script.contains("create policy p_items"),
        "policy must not be resurrected when neither it nor the function exist in TO"
    );
}

/// Variant of [`issue179_items_table`] that mirrors PostgreSQL's
/// deparser output when the function is reachable via `search_path`:
/// the dependent texts use *unqualified* `compute(value)` instead of
/// `test_deps.compute(value)`. The `pg_get_*` family routinely drops
/// the schema qualifier in this case.
fn issue179_items_table_unqualified(value_type: &str, def_default: &str, gen_type: &str) -> Table {
    let mut def_col = int_column("test_deps", "items", "def_col", 2);
    def_col.data_type = value_type.to_string();
    def_col.column_default = Some(def_default.to_string());

    let mut gen_col = int_column("test_deps", "items", "gen_col", 3);
    gen_col.data_type = gen_type.to_string();
    gen_col.is_generated = "ALWAYS".to_string();
    // Unqualified function call.
    gen_col.generation_expression = Some("compute(value)".to_string());
    gen_col.generation_type = Some("s".to_string());

    let mut value_col = int_column("test_deps", "items", "value", 1);
    value_col.is_nullable = false;

    let chk_constraint = TableConstraint {
        catalog: "postgres".to_string(),
        schema: "test_deps".to_string(),
        name: "chk_compute".to_string(),
        table_name: "items".to_string(),
        constraint_type: "CHECK".to_string(),
        is_deferrable: false,
        initially_deferred: false,
        // Unqualified function call.
        definition: Some("CHECK (compute(value) > 0)".to_string()),
        coninhcount: 0,
        is_enforced: true,
        no_inherit: false,
        nulls_not_distinct: false,
        comment: None,
    };

    let idx = TableIndex {
        schema: "test_deps".to_string(),
        table: "items".to_string(),
        name: "idx_compute".to_string(),
        catalog: Some("postgres".to_string()),
        // Unqualified function call.
        indexdef: "CREATE INDEX idx_compute ON test_deps.items USING btree (compute(value))"
            .to_string(),
        is_partition_index: false,
        comment: None,
    };

    let policy = TablePolicy {
        schema: "test_deps".to_string(),
        table: "items".to_string(),
        name: "p_items".to_string(),
        command: "all".to_string(),
        permissive: true,
        roles: vec![],
        // Unqualified function call.
        using_clause: Some("(compute(value) > 0)".to_string()),
        check_clause: None,
    };

    let mut table = Table::new(
        "test_deps".to_string(),
        "items".to_string(),
        "test_deps".to_string(),
        "items".to_string(),
        "postgres".to_string(),
        None,
        vec![value_col, def_col, gen_col],
        vec![chk_constraint],
        vec![idx],
        vec![],
        None,
    );
    table.policies = vec![policy];
    table.has_rowsecurity = true;
    table.hash();
    table
}

#[tokio::test]
async fn issue179_unqualified_function_calls_are_detected() {
    // PostgreSQL's pg_get_constraintdef / pg_get_indexdef / pg_get_expr
    // drop the schema qualifier when the function is in search_path
    // (the typical `public` case). Phase 7 must still recognise these
    // dependents — otherwise the CASCADE-drop drift goes unfixed for
    // anything the deparser deemed "in scope".
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    from_dump
        .routines
        .push(issue179_compute_routine("integer", "SELECT x * 2;"));
    to_dump.routines.push(issue179_compute_routine(
        "bigint",
        "SELECT (x * 2)::bigint;",
    ));

    from_dump.tables.push(issue179_items_table_unqualified(
        "integer",
        "compute(0)::integer",
        "integer",
    ));
    to_dump.tables.push(issue179_items_table_unqualified(
        "bigint",
        "compute(0)",
        "bigint",
    ));

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_routines_and_views().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("alter table test_deps.items add constraint chk_compute"),
        "unqualified CHECK reference must trigger recreate: {}",
        script
    );
    assert!(
        script.contains("CREATE INDEX IF NOT EXISTS idx_compute ON test_deps.items"),
        "unqualified index reference must trigger recreate: {}",
        script
    );
    assert!(
        script.contains(
            "alter table test_deps.items add column if not exists gen_col bigint generated always as (compute(value)) stored;"
        ),
        "unqualified generated-column reference must trigger recreate: {}",
        script
    );
    assert!(
        script.contains("alter table test_deps.items alter column def_col set default compute(0);"),
        "unqualified column DEFAULT reference must trigger recreate: {}",
        script
    );
    assert!(
        script.contains("create policy p_items on test_deps.items"),
        "unqualified policy reference must trigger recreate: {}",
        script
    );
}

#[test]
fn issue179_unqualified_match_rejects_substrings_and_non_calls() {
    // Direct unit test for the boundary rules: the unqualified matcher
    // must require `name(` at an identifier boundary, not match
    // partial-name suffixes, qualified `schema.name`, or non-call uses.
    let mut affected: HashSet<(String, String)> = HashSet::new();
    affected.insert(("ignored_schema".to_string(), "compute".to_string()));

    // Bare function call — should match.
    assert!(Comparer::definition_references_any(
        "check (compute(value) > 0)",
        &affected
    ));
    // Whitespace between name and `(` — still a call.
    assert!(Comparer::definition_references_any(
        "check (compute  (value) > 0)",
        &affected
    ));
    // Qualified — qualified matcher handles it via the schema, but we
    // also exercise that the unqualified matcher's left-dot exclusion
    // does not double-match `other_schema.compute(`.
    assert!(!Comparer::definition_references_any(
        "check (other_schema.compute(value) > 0)",
        &affected
    ));
    // Substring — must NOT match.
    assert!(!Comparer::definition_references_any(
        "check (compute_v2(value) > 0)",
        &affected
    ));
    assert!(!Comparer::definition_references_any(
        "check (precompute(value) > 0)",
        &affected
    ));
    // Identifier without trailing `(` — must NOT match.
    assert!(!Comparer::definition_references_any(
        "check (compute > 0)",
        &affected
    ));
}

#[test]
fn issue179_unqualified_match_handles_non_ascii_identifiers() {
    // PostgreSQL allows Unicode identifiers (quoted), and the dump's
    // `quote_ident` machinery preserves them. After `prelower_pair`
    // strips the surrounding quotes the haystack and needle both
    // contain raw multi-byte UTF-8 — the previous implementation did
    // `text[start..]` with `start = i + 1` and panicked on the next
    // iteration because byte index `i + 1` lands inside a codepoint.
    // Drive the matcher with a Cyrillic name and several haystacks to
    // ensure: (a) it returns true for a real call, (b) it returns
    // false for a non-call use without panicking, and (c) it returns
    // false for a substring without panicking.
    let mut affected: HashSet<(String, String)> = HashSet::new();
    affected.insert(("test_schema".to_string(), "функция".to_string()));

    assert!(Comparer::definition_references_any(
        "check (функция(value) > 0)",
        &affected
    ));
    assert!(!Comparer::definition_references_any(
        "check (функция > 0)",
        &affected
    ));
    // Repeated occurrences without a `(` — would have triggered the
    // panic on the post-match `start = i + 1` advance.
    assert!(!Comparer::definition_references_any(
        "check (функция функция функция > 0)",
        &affected
    ));
    // Substring (Cyrillic suffix) must not falsely match.
    assert!(!Comparer::definition_references_any(
        "check (функция_v2(value) > 0)",
        &affected
    ));
}

#[test]
fn issue179_qualified_match_requires_call_context() {
    // `pg_get_indexdef` emits `CREATE INDEX … ON schema.table …`,
    // `pg_get_expr` emits `nextval('schema.seq'::regclass)`, etc.
    // Without a call gate the qualified matcher would pick up those
    // `schema.name` references whenever a routine happens to share its
    // name with a table / view / sequence in the same schema, and
    // Phase 7 would emit spurious recreates for unrelated objects.
    let mut affected: HashSet<(String, String)> = HashSet::new();
    affected.insert(("test_schema".to_string(), "users".to_string()));

    // Match: real qualified function call.
    assert!(Comparer::definition_references_any(
        "check (test_schema.users(value) > 0)",
        &affected,
    ));
    // Match with whitespace before `(`.
    assert!(Comparer::definition_references_any(
        "check (test_schema.users  (value) > 0)",
        &affected,
    ));

    // No match: qualified reference is the table in a CREATE INDEX
    // ON clause — not a function call.
    assert!(!Comparer::definition_references_any(
        "create index idx ON test_schema.users using btree (value)",
        &affected,
    ));
    // No match: qualified reference inside a `nextval` regclass cast.
    assert!(!Comparer::definition_references_any(
        "nextval('test_schema.users'::regclass)",
        &affected,
    ));
    // No match: identifier without a `(` after.
    assert!(!Comparer::definition_references_any(
        "check (test_schema.users > 0)",
        &affected,
    ));
    // No match: qualified suffix of a longer name (boundary check).
    assert!(!Comparer::definition_references_any(
        "check (test_schema.users_v2(value) > 0)",
        &affected,
    ));
}

#[tokio::test]
async fn issue179_to_side_gate_skips_dependents_no_longer_referencing_routine() {
    // `compare_tables()` runs before `compare_routines_and_views()`, so
    // dependents that have been rewritten to no longer reference the
    // affected routine reach the CASCADE-drop step with the dependency
    // already broken — PostgreSQL leaves them alone. Phase 7 must NOT
    // re-emit recreates for those: doing so is at best wasteful and at
    // worst destructive (a `DROP COLUMN IF EXISTS` for a generated
    // column also drops every index / FK / constraint attached to the
    // column, none of which Phase 7 restores).
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    from_dump
        .routines
        .push(issue179_compute_routine("integer", "SELECT x * 2;"));
    to_dump.routines.push(issue179_compute_routine(
        "bigint",
        "SELECT (x * 2)::bigint;",
    ));

    // FROM table: every dependent references `test_deps.compute`.
    from_dump.tables.push(issue179_items_table(
        "integer",
        "test_deps.compute(0)",
        "integer",
    ));

    // TO table: dependents have been rewritten to NOT reference
    // `test_deps.compute` anymore. After `compare_tables` runs they
    // exist in this rewritten form, so the `DROP FUNCTION ... CASCADE`
    // does not touch them.
    let mut value_col = int_column("test_deps", "items", "value", 1);
    value_col.is_nullable = false;

    let mut def_col = int_column("test_deps", "items", "def_col", 2);
    def_col.data_type = "integer".to_string();
    def_col.column_default = Some("0".to_string()); // no longer references compute

    let mut gen_col = int_column("test_deps", "items", "gen_col", 3);
    gen_col.data_type = "integer".to_string();
    gen_col.is_generated = "ALWAYS".to_string();
    gen_col.generation_expression = Some("(value * 3)".to_string()); // no compute()
    gen_col.generation_type = Some("s".to_string());

    let chk = TableConstraint {
        catalog: "postgres".to_string(),
        schema: "test_deps".to_string(),
        name: "chk_compute".to_string(),
        table_name: "items".to_string(),
        constraint_type: "CHECK".to_string(),
        is_deferrable: false,
        initially_deferred: false,
        // No compute() here either — TO swapped it out.
        definition: Some("CHECK (value > 0)".to_string()),
        coninhcount: 0,
        is_enforced: true,
        no_inherit: false,
        nulls_not_distinct: false,
        comment: None,
    };

    let idx = TableIndex {
        schema: "test_deps".to_string(),
        table: "items".to_string(),
        name: "idx_compute".to_string(),
        catalog: Some("postgres".to_string()),
        // No compute() in the index expression either.
        indexdef: "CREATE INDEX idx_compute ON test_deps.items USING btree (value)".to_string(),
        is_partition_index: false,
        comment: None,
    };

    let policy = TablePolicy {
        schema: "test_deps".to_string(),
        table: "items".to_string(),
        name: "p_items".to_string(),
        command: "all".to_string(),
        permissive: true,
        roles: vec![],
        // No compute() in the policy clause either.
        using_clause: Some("(value > 0)".to_string()),
        check_clause: None,
    };

    let mut to_table = Table::new(
        "test_deps".to_string(),
        "items".to_string(),
        "test_deps".to_string(),
        "items".to_string(),
        "postgres".to_string(),
        None,
        vec![value_col, def_col, gen_col],
        vec![chk],
        vec![idx],
        vec![],
        None,
    );
    to_table.policies = vec![policy];
    to_table.has_rowsecurity = true;
    to_table.hash();
    to_dump.tables.push(to_table);

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_routines_and_views().await.unwrap();
    let script = comparer.get_script();

    // Function still gets the CASCADE drop (signature change still
    // requires DROP+CREATE).
    assert!(
        script.contains("drop function if exists test_deps.compute (x integer) cascade;"),
        "function drop must still be emitted: {}",
        script
    );

    // CRITICAL: the destructive generated-column path must stay silent.
    assert!(
        !script.contains("alter table test_deps.items drop column if exists gen_col"),
        "generated column must NOT be dropped+re-added when TO no longer references the routine — that would cascade-destroy attached indexes/FKs without restoring them: {}",
        script
    );
    assert!(
        !script.contains("alter table test_deps.items add column gen_col"),
        "generated column add must not be emitted when TO does not reference the routine: {}",
        script
    );

    // Constraint, index, and policy recreates must also be skipped to
    // avoid redundant work that would conflict with `compare_tables`.
    assert!(
        !script.contains("alter table test_deps.items add constraint chk_compute"),
        "CHECK recreate must not fire when TO definition no longer references the routine: {}",
        script
    );
    assert!(
        !script.contains("CREATE INDEX idx_compute ON test_deps.items"),
        "index recreate must not fire when TO indexdef no longer references the routine: {}",
        script
    );
    assert!(
        !script.contains("create policy p_items"),
        "policy recreate must not fire when TO clauses no longer reference the routine: {}",
        script
    );

    // Column DEFAULT must also be skipped (TO default is `0`, no
    // function reference).
    assert!(
        !script.contains("alter table test_deps.items alter column def_col set default 0;"),
        "column DEFAULT recreate must not fire when TO default no longer references the routine: {}",
        script
    );
}

#[tokio::test]
async fn issue179_to_side_gate_still_recreates_when_to_keeps_reference() {
    // Sanity check on the gate: when TO *does* still reference the
    // affected routine (e.g. the dependent definition is unchanged),
    // Phase 7 must continue to emit recreates exactly as before.
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    from_dump
        .routines
        .push(issue179_compute_routine("integer", "SELECT x * 2;"));
    to_dump.routines.push(issue179_compute_routine(
        "bigint",
        "SELECT (x * 2)::bigint;",
    ));

    from_dump.tables.push(issue179_items_table(
        "integer",
        "test_deps.compute(0)::integer",
        "integer",
    ));
    to_dump.tables.push(issue179_items_table(
        "bigint",
        "test_deps.compute(0)",
        "bigint",
    ));

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_routines_and_views().await.unwrap();
    let script = comparer.get_script();

    // Each kind of dependent must be re-emitted because TO still
    // references the routine.
    assert!(
        script.contains("alter table test_deps.items add constraint chk_compute"),
        "CHECK constraint recreate expected when TO still references the routine: {}",
        script
    );
    assert!(
        script.contains("CREATE INDEX IF NOT EXISTS idx_compute ON test_deps.items"),
        "index recreate expected when TO still references the routine: {}",
        script
    );
    assert!(
        script.contains("alter table test_deps.items add column if not exists gen_col"),
        "generated column recreate expected when TO still references the routine: {}",
        script
    );
    assert!(
        script.contains(
            "alter table test_deps.items alter column def_col set default test_deps.compute(0);"
        ),
        "column DEFAULT recreate expected when TO still references the routine: {}",
        script
    );
    assert!(
        script.contains("create policy p_items on test_deps.items"),
        "policy recreate expected when TO still references the routine: {}",
        script
    );
}

#[tokio::test]
async fn issue179_overload_collision_does_not_destroy_unrelated_column() {
    // Phase 7's `affected` set keys on `(schema, name)` and ignores the
    // argument signature, because text-based reference matching cannot
    // distinguish overloads (`compute(value)` in a CHECK or generation
    // expression carries no type info). When `compute(integer)` is
    // dropped+recreated and `compute(text)` is unchanged, a dependent
    // referencing `compute(text_value)` will text-match the affected
    // set even though CASCADE never touched it. The recreate paths
    // must therefore be non-destructive — a `DROP COLUMN IF EXISTS`
    // for a generated column would cascade through every index / FK /
    // constraint attached to the column, none of which Phase 7 knows
    // how to restore. This test pins the non-destructive contract.
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    // FROM: two overloads. `compute(integer)` will change return type
    // (forces DROP+CREATE); `compute(text)` is unchanged.
    let mut from_int = Routine::new(
        "test_deps".to_string(),
        Oid(900),
        "compute".to_string(),
        "sql".to_string(),
        "FUNCTION".to_string(),
        "integer".to_string(),
        "x integer".to_string(),
        None,
        None,
        "SELECT x * 2;".to_string(),
    );
    from_int.hash();
    let mut from_text = Routine::new(
        "test_deps".to_string(),
        Oid(901),
        "compute".to_string(),
        "sql".to_string(),
        "FUNCTION".to_string(),
        "integer".to_string(),
        "x text".to_string(),
        None,
        None,
        "SELECT length(x);".to_string(),
    );
    from_text.hash();
    from_dump.routines.push(from_int);
    from_dump.routines.push(from_text.clone());

    // TO: same overloads, but `compute(integer)` now returns BIGINT.
    let mut to_int = Routine::new(
        "test_deps".to_string(),
        Oid(900),
        "compute".to_string(),
        "sql".to_string(),
        "FUNCTION".to_string(),
        "bigint".to_string(),
        "x integer".to_string(),
        None,
        None,
        "SELECT (x * 2)::bigint;".to_string(),
    );
    to_int.hash();
    to_dump.routines.push(to_int);
    to_dump.routines.push(from_text); // text overload unchanged

    // Build a table whose generated column references `compute(text_value)`
    // — i.e. the unchanged `compute(text)` overload. CASCADE never
    // drops this column (the dropped function is `compute(integer)`),
    // so Phase 7 must NOT emit a destructive recreate.
    let make_table = || {
        let mut text_col = int_column("test_deps", "items", "text_value", 1);
        text_col.data_type = "text".to_string();
        text_col.is_nullable = false;
        let mut gen_col = int_column("test_deps", "items", "gen_col", 2);
        gen_col.is_generated = "ALWAYS".to_string();
        gen_col.generation_expression = Some("test_deps.compute(text_value)".to_string());
        gen_col.generation_type = Some("s".to_string());
        let mut table = Table::new(
            "test_deps".to_string(),
            "items".to_string(),
            "test_deps".to_string(),
            "items".to_string(),
            "postgres".to_string(),
            None,
            vec![text_col, gen_col],
            vec![],
            vec![],
            vec![],
            None,
        );
        table.hash();
        table
    };
    from_dump.tables.push(make_table());
    to_dump.tables.push(make_table());

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_routines_and_views().await.unwrap();
    let script = comparer.get_script();

    // The integer overload is still cascaded.
    assert!(
        script.contains("drop function if exists test_deps.compute (x integer) cascade;"),
        "integer overload must still be DROP+CREATEd: {}",
        script
    );

    // CRITICAL: Phase 7 must NOT emit a DROP COLUMN. The text matcher
    // false-positives on the affected set (which collapses overloads
    // by name), so without IF NOT EXISTS the unconditional drop would
    // destroy the unrelated column. Pinning this prevents regression.
    assert!(
        !script.contains("drop column if exists gen_col"),
        "overload collision must not emit DROP COLUMN — would cascade-destroy attached indexes/FKs: {}",
        script
    );
    // The recreate that *is* emitted must use IF NOT EXISTS so the
    // surviving column is left intact when the script runs.
    if let Some(add_idx) = script.find("alter table test_deps.items add column") {
        let snippet = &script[add_idx..(add_idx + 80).min(script.len())];
        assert!(
            snippet.contains("if not exists"),
            "ADD COLUMN must use IF NOT EXISTS to be non-destructive on overload false-positives: {}",
            snippet
        );
    }
}

#[tokio::test]
async fn issue179_quoted_routine_name_recreates_dependents() {
    // The dump query wraps `proname` with `quote_ident`, so a
    // mixed-case routine like `MyFunc` arrives as `"MyFunc"`. The
    // unqualified-call matcher operates on the quote-stripped haystack,
    // so the affected-routine name must also be quote-stripped or
    // dependents like `CHECK ("MyFunc"(value) > 0)` are missed.
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_routine = Routine::new(
        "\"MySchema\"".to_string(),
        Oid(950),
        "\"MyFunc\"".to_string(),
        "sql".to_string(),
        "FUNCTION".to_string(),
        "integer".to_string(),
        "x integer".to_string(),
        None,
        None,
        "SELECT x * 2;".to_string(),
    );
    from_routine.hash();
    from_dump.routines.push(from_routine);

    let mut to_routine = Routine::new(
        "\"MySchema\"".to_string(),
        Oid(950),
        "\"MyFunc\"".to_string(),
        "sql".to_string(),
        "FUNCTION".to_string(),
        "bigint".to_string(),
        "x integer".to_string(),
        None,
        None,
        "SELECT (x * 2)::bigint;".to_string(),
    );
    to_routine.hash();
    to_dump.routines.push(to_routine);

    let mut value_col = int_column("public", "items", "value", 1);
    value_col.is_nullable = false;

    let chk = TableConstraint {
        catalog: "postgres".to_string(),
        schema: "public".to_string(),
        name: "chk_my".to_string(),
        table_name: "items".to_string(),
        constraint_type: "CHECK".to_string(),
        is_deferrable: false,
        initially_deferred: false,
        definition: Some("CHECK (\"MyFunc\"(value) > 0)".to_string()),
        coninhcount: 0,
        is_enforced: true,
        no_inherit: false,
        nulls_not_distinct: false,
        comment: None,
    };

    let mut from_table = Table::new(
        "public".to_string(),
        "items".to_string(),
        "public".to_string(),
        "items".to_string(),
        "postgres".to_string(),
        None,
        vec![value_col.clone()],
        vec![chk.clone()],
        vec![],
        vec![],
        None,
    );
    from_table.hash();
    let mut to_table = Table::new(
        "public".to_string(),
        "items".to_string(),
        "public".to_string(),
        "items".to_string(),
        "postgres".to_string(),
        None,
        vec![value_col],
        vec![chk],
        vec![],
        vec![],
        None,
    );
    to_table.hash();
    from_dump.tables.push(from_table);
    to_dump.tables.push(to_table);

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_routines_and_views().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("alter table public.items add constraint chk_my"),
        "quoted-name function reference must trigger CHECK recreate: {}",
        script
    );
}

#[tokio::test]
async fn issue179_defaults_only_change_uses_create_or_replace_no_cascade() {
    // PR #187 review (C10/C15): defaults-only changes must NOT
    // trigger `DROP FUNCTION ... CASCADE`. PostgreSQL accepts
    // default-argument changes via `CREATE OR REPLACE FUNCTION`
    // when the identity argument types and return type are
    // unchanged. The earlier version of this test pinned the
    // destructive behaviour (DROP CASCADE + Phase 7 dependent
    // recreates); the correct expectation is the non-destructive
    // OR REPLACE form, with no CASCADE drop and no dependent
    // recreates (since the function was never actually dropped).
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_routine = Routine::new(
        "test_deps".to_string(),
        Oid(951),
        "compute".to_string(),
        "sql".to_string(),
        "FUNCTION".to_string(),
        "integer".to_string(),
        "x integer".to_string(),
        Some("DEFAULT 0".to_string()),
        None,
        "SELECT x * 2;".to_string(),
    );
    from_routine.hash();
    from_dump.routines.push(from_routine);

    let mut to_routine = Routine::new(
        "test_deps".to_string(),
        Oid(951),
        "compute".to_string(),
        "sql".to_string(),
        "FUNCTION".to_string(),
        "integer".to_string(),
        "x integer".to_string(),
        Some("DEFAULT 1".to_string()),
        None,
        "SELECT x * 2;".to_string(),
    );
    to_routine.hash();
    to_dump.routines.push(to_routine);

    from_dump.tables.push(issue179_items_table(
        "integer",
        "test_deps.compute(0)",
        "integer",
    ));
    to_dump.tables.push(issue179_items_table(
        "integer",
        "test_deps.compute(0)",
        "integer",
    ));

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_routines_and_views().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("create or replace function test_deps.compute(x integer DEFAULT 1)"),
        "defaults-only change must be re-emitted via CREATE OR REPLACE: {}",
        script
    );
    assert!(
        !script.contains("drop function if exists test_deps.compute"),
        "defaults-only change must NOT emit DROP FUNCTION CASCADE: {}",
        script
    );
    assert!(
        !script.contains("alter table test_deps.items add constraint chk_compute"),
        "dependents must NOT be recreated when the function was not actually dropped: {}",
        script
    );
}

/// Build a partition child of `parent_table` whose dependents (CHECK
/// constraint with `coninhcount=1`, `is_partition_index=true` index, a
/// generated column, and a CHECK with `coninhcount=0` so we can prove
/// the truly-local case is still emitted) all reference
/// `test_deps.compute`. Used by the partition-child guard tests.
fn issue179_items_partition_child(parent_qualified: &str) -> Table {
    let mut value_col = int_column("test_deps", "items_2026", "value", 1);
    value_col.is_nullable = false;

    let mut gen_col = int_column("test_deps", "items_2026", "gen_col", 2);
    gen_col.is_generated = "ALWAYS".to_string();
    gen_col.generation_expression = Some("test_deps.compute(value)".to_string());
    gen_col.generation_type = Some("s".to_string());

    let inherited_check = TableConstraint {
        catalog: "postgres".to_string(),
        schema: "test_deps".to_string(),
        name: "chk_compute".to_string(),
        table_name: "items_2026".to_string(),
        constraint_type: "CHECK".to_string(),
        is_deferrable: false,
        initially_deferred: false,
        definition: Some("CHECK (test_deps.compute(value) > 0)".to_string()),
        coninhcount: 1,
        is_enforced: true,
        no_inherit: false,
        nulls_not_distinct: false,
        comment: None,
    };
    let mut local_check = inherited_check.clone();
    local_check.name = "chk_compute_local".to_string();
    local_check.coninhcount = 0;

    let inherited_idx = TableIndex {
        schema: "test_deps".to_string(),
        table: "items_2026".to_string(),
        name: "idx_compute".to_string(),
        catalog: Some("postgres".to_string()),
        indexdef: "CREATE INDEX idx_compute ON test_deps.items_2026 USING btree (test_deps.compute(value))".to_string(),
        is_partition_index: true,
        comment: None,
    };

    let mut table = Table::new(
        "test_deps".to_string(),
        "items_2026".to_string(),
        "test_deps".to_string(),
        "items_2026".to_string(),
        "postgres".to_string(),
        None,
        vec![value_col, gen_col],
        vec![inherited_check, local_check],
        vec![inherited_idx],
        vec![],
        None,
    );
    table.partition_of = Some(parent_qualified.to_string());
    table.partition_bound = Some("FOR VALUES IN (1)".to_string());
    table.hash();
    table
}

#[tokio::test]
async fn issue179_partition_child_skips_inherited_dependents() {
    // FROM and TO each contain a partitioned parent + one partition
    // child. The function signature changes, so CASCADE drops the
    // parent-side dependents. Phase 7 must NOT emit recreates for the
    // child's inherited objects (PostgreSQL forbids `ALTER TABLE child`
    // on inherited columns/constraints/indexes), but it MUST still
    // emit the truly-local CHECK constraint (`coninhcount = 0`).
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    from_dump
        .routines
        .push(issue179_compute_routine("integer", "SELECT x * 2;"));
    to_dump.routines.push(issue179_compute_routine(
        "bigint",
        "SELECT (x * 2)::bigint;",
    ));

    // Parent (partitioned) carries the dependent definitions on its own
    // row — its recreate handles the propagation to children.
    let mut parent_from =
        issue179_items_table("integer", "test_deps.compute(0)::integer", "integer");
    parent_from.name = "items".to_string();
    parent_from.raw_name = "items".to_string();
    parent_from.partition_key = Some("LIST (value)".to_string());
    parent_from.hash();

    let mut parent_to = issue179_items_table("bigint", "test_deps.compute(0)", "bigint");
    parent_to.name = "items".to_string();
    parent_to.raw_name = "items".to_string();
    parent_to.partition_key = Some("LIST (value)".to_string());
    parent_to.hash();

    from_dump.tables.push(parent_from);
    to_dump.tables.push(parent_to);

    from_dump
        .tables
        .push(issue179_items_partition_child("test_deps.items"));
    to_dump
        .tables
        .push(issue179_items_partition_child("test_deps.items"));

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_routines_and_views().await.unwrap();
    let script = comparer.get_script();

    // Parent dependents (which Table::diff would diff normally) MUST
    // be recreated against the parent.
    assert!(
        script.contains("alter table test_deps.items add constraint chk_compute"),
        "parent CHECK must be re-added: {}",
        script
    );
    assert!(
        script.contains("CREATE INDEX IF NOT EXISTS idx_compute ON test_deps.items "),
        "parent index must be re-created: {}",
        script
    );

    // Inherited child constraint (coninhcount > 0) must NOT be re-added
    // on the child — PostgreSQL would reject `ALTER TABLE child ADD
    // CONSTRAINT` for an inherited constraint, and the parent's recreate
    // already propagates. (Use exact-suffix match so we don't accidentally
    // match `chk_compute_local` below.)
    assert!(
        !script.contains("alter table test_deps.items_2026 drop constraint if exists chk_compute;"),
        "inherited child CHECK must not be re-emitted: {}",
        script
    );
    assert!(
        !script.contains("alter table test_deps.items_2026 add constraint chk_compute check"),
        "inherited child CHECK must not be re-added: {}",
        script
    );

    // Truly-local child constraint (coninhcount == 0) IS re-emitted.
    assert!(
        script.contains(
            "alter table test_deps.items_2026 drop constraint if exists chk_compute_local;"
        ),
        "local child CHECK must be re-emitted: {}",
        script
    );
    assert!(
        script.contains("alter table test_deps.items_2026 add constraint chk_compute_local"),
        "local child CHECK must be re-added: {}",
        script
    );

    // Partition-inherited index: must not be re-emitted on the child.
    // Match by the load-bearing fragment so the assertion holds whether
    // the recreate uses `CREATE INDEX` or `CREATE INDEX IF NOT EXISTS`.
    assert!(
        !script.contains("idx_compute ON test_deps.items_2026"),
        "partition-inherited index must not be re-emitted on child: {}",
        script
    );

    // Partition child's generated column must NOT be added — PostgreSQL
    // forbids modifying inherited columns directly on a partition.
    // (Recreate paths never emit DROP COLUMN now; the add assertion
    // below is the load-bearing one for partition-child safety.)
    assert!(
        !script.contains("alter table test_deps.items_2026 add column if not exists gen_col"),
        "partition child column must not be re-added: {}",
        script
    );
}

#[tokio::test]
async fn issue179_full_drop_recreates_dependents_when_overload_survives() {
    // PR #187 review (C16): the previous version of this test built
    // an invalid PostgreSQL state — TO had dependents referencing
    // `test_deps.compute` but no function with that name at all, so
    // the recreate SQL would fail to apply. The valid scenario where
    // "function fully dropped, dependents kept" is meaningful is when
    // a *different overload* of the same name survives in TO and the
    // dependents resolve to it via PostgreSQL's name-based function
    // binding. Set that up explicitly here. FROM has both
    // `compute(integer)` (which gets dropped) and `compute(text)`
    // (the surviving overload). TO has only `compute(text)`.
    // Dependents in both sides reference `test_deps.compute` and
    // resolve via overload resolution.
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    from_dump
        .routines
        .push(issue179_compute_routine("integer", "SELECT x * 2;"));
    let mut compute_text_from = Routine::new(
        "test_deps".to_string(),
        Oid(961),
        "compute".to_string(),
        "sql".to_string(),
        "FUNCTION".to_string(),
        "integer".to_string(),
        "x text".to_string(),
        None,
        None,
        "SELECT length(x);".to_string(),
    );
    compute_text_from.hash();
    from_dump.routines.push(compute_text_from.clone());
    // TO keeps only the text overload — `compute(integer)` is gone.
    to_dump.routines.push(compute_text_from);

    from_dump.tables.push(issue179_items_table(
        "integer",
        "test_deps.compute(0)",
        "integer",
    ));
    to_dump.tables.push(issue179_items_table(
        "integer",
        "test_deps.compute(0)",
        "integer",
    ));

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_routines_and_views().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("drop function if exists test_deps.compute (x integer) cascade;"),
        "the integer overload must be dropped"
    );
    assert!(
        script.contains("alter table test_deps.items add constraint chk_compute"),
        "CHECK present in TO must be re-added after CASCADE (overload-resolves to surviving compute)"
    );
    assert!(
        script.contains("CREATE INDEX IF NOT EXISTS idx_compute"),
        "functional index present in TO must be re-created (IF NOT EXISTS) after CASCADE"
    );
    assert!(
        script.contains("create policy p_items on test_deps.items"),
        "policy present in TO must be re-created after CASCADE"
    );
}

// ─────────────────────────────────────────────────────────────────────
// Issue #180 — SET UNLOGGED / SET LOGGED statements must respect FK
// dependencies (PostgreSQL rejects an out-of-order conversion), and
// owned sequences should not redundantly re-emit the persistence flip
// the table cascade already propagates.
// ─────────────────────────────────────────────────────────────────────

/// Build a logged/unlogged-controlled table with a single FK to another
/// table in the same schema, named with a numeric `id` PK column. Used
/// by the issue-#180 ordering tests.
fn issue180_logged_table(
    schema: &str,
    name: &str,
    is_unlogged: bool,
    fk_target: Option<(&str, &str, &str)>,
) -> Table {
    let mut id_col = int_column(schema, name, "id", 1);
    id_col.is_nullable = false;

    let mut constraints: Vec<TableConstraint> = vec![TableConstraint {
        catalog: "postgres".to_string(),
        schema: schema.to_string(),
        name: format!("{name}_pkey"),
        table_name: name.to_string(),
        constraint_type: "PRIMARY KEY".to_string(),
        is_deferrable: false,
        initially_deferred: false,
        definition: Some("PRIMARY KEY (id)".to_string()),
        coninhcount: 0,
        is_enforced: true,
        no_inherit: false,
        nulls_not_distinct: false,
        comment: None,
    }];

    let mut columns = vec![id_col];
    if let Some((fk_col, fk_schema, fk_table)) = fk_target {
        let mut ref_col = int_column(schema, name, fk_col, 2);
        ref_col.is_nullable = true;
        columns.push(ref_col);
        constraints.push(TableConstraint {
            catalog: "postgres".to_string(),
            schema: schema.to_string(),
            name: format!("{name}_{fk_col}_fkey"),
            table_name: name.to_string(),
            constraint_type: "FOREIGN KEY".to_string(),
            is_deferrable: false,
            initially_deferred: false,
            definition: Some(format!(
                "FOREIGN KEY ({fk_col}) REFERENCES {fk_schema}.{fk_table}(id)"
            )),
            coninhcount: 0,
            is_enforced: true,
            no_inherit: false,
            nulls_not_distinct: false,
            comment: None,
        });
    }

    let mut table = Table::new(
        schema.to_string(),
        name.to_string(),
        schema.to_string(),
        name.to_string(),
        "postgres".to_string(),
        None,
        columns,
        constraints,
        vec![],
        vec![],
        None,
    );
    table.is_unlogged = is_unlogged;
    table.hash();
    table
}

#[tokio::test]
async fn issue180_set_unlogged_orders_dependents_before_referenced() {
    // FROM: three logged tables with FK chain
    //   child -> parent -> grandparent.
    // TO:   the same three tables, all UNLOGGED.
    // PostgreSQL refuses `SET UNLOGGED` on a table while a LOGGED table
    // still references it, so the conversion order must be leaves
    // first: child, then parent, then grandparent.
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());
    from_dump.tables.push(issue180_logged_table(
        "test_order",
        "grandparent",
        false,
        None,
    ));
    from_dump.tables.push(issue180_logged_table(
        "test_order",
        "parent",
        false,
        Some(("grandparent_id", "test_order", "grandparent")),
    ));
    from_dump.tables.push(issue180_logged_table(
        "test_order",
        "child",
        false,
        Some(("parent_id", "test_order", "parent")),
    ));
    to_dump.tables.push(issue180_logged_table(
        "test_order",
        "grandparent",
        true,
        None,
    ));
    to_dump.tables.push(issue180_logged_table(
        "test_order",
        "parent",
        true,
        Some(("grandparent_id", "test_order", "grandparent")),
    ));
    to_dump.tables.push(issue180_logged_table(
        "test_order",
        "child",
        true,
        Some(("parent_id", "test_order", "parent")),
    ));

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Ignore);
    comparer.compare_tables().await.unwrap();
    let script = comparer.get_script();

    let pos_child = script
        .find("alter table test_order.child set unlogged;")
        .expect("child SET UNLOGGED must be emitted");
    let pos_parent = script
        .find("alter table test_order.parent set unlogged;")
        .expect("parent SET UNLOGGED must be emitted");
    let pos_grand = script
        .find("alter table test_order.grandparent set unlogged;")
        .expect("grandparent SET UNLOGGED must be emitted");

    assert!(
        pos_child < pos_parent && pos_parent < pos_grand,
        "SET UNLOGGED must be ordered child -> parent -> grandparent (FK leaves first); got\n{}",
        script
    );
}

#[tokio::test]
async fn issue180_set_logged_orders_referenced_before_dependents() {
    // Reverse direction: all UNLOGGED -> all LOGGED.
    // PostgreSQL refuses `SET LOGGED` while the table still references
    // an UNLOGGED one, so order must be roots first: grandparent, then
    // parent, then child.
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());
    from_dump.tables.push(issue180_logged_table(
        "test_order",
        "grandparent",
        true,
        None,
    ));
    from_dump.tables.push(issue180_logged_table(
        "test_order",
        "parent",
        true,
        Some(("grandparent_id", "test_order", "grandparent")),
    ));
    from_dump.tables.push(issue180_logged_table(
        "test_order",
        "child",
        true,
        Some(("parent_id", "test_order", "parent")),
    ));
    to_dump.tables.push(issue180_logged_table(
        "test_order",
        "grandparent",
        false,
        None,
    ));
    to_dump.tables.push(issue180_logged_table(
        "test_order",
        "parent",
        false,
        Some(("grandparent_id", "test_order", "grandparent")),
    ));
    to_dump.tables.push(issue180_logged_table(
        "test_order",
        "child",
        false,
        Some(("parent_id", "test_order", "parent")),
    ));

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Ignore);
    comparer.compare_tables().await.unwrap();
    let script = comparer.get_script();

    let pos_grand = script
        .find("alter table test_order.grandparent set logged;")
        .expect("grandparent SET LOGGED must be emitted");
    let pos_parent = script
        .find("alter table test_order.parent set logged;")
        .expect("parent SET LOGGED must be emitted");
    let pos_child = script
        .find("alter table test_order.child set logged;")
        .expect("child SET LOGGED must be emitted");

    assert!(
        pos_grand < pos_parent && pos_parent < pos_child,
        "SET LOGGED must be ordered grandparent -> parent -> child (FK roots first); got\n{}",
        script
    );
}

#[tokio::test]
async fn issue180_persistence_change_does_not_emit_inline_inside_alter_table() {
    // A table-level ALTER (e.g. add column) MUST NOT carry a SET
    // UNLOGGED line — that would re-introduce the alphabetical ordering
    // bug. The persistence flip is owned by the dedicated phase.
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_table = Table::new(
        "test_order".to_string(),
        "items".to_string(),
        "test_order".to_string(),
        "items".to_string(),
        "postgres".to_string(),
        None,
        vec![int_column("test_order", "items", "id", 1)],
        vec![],
        vec![],
        vec![],
        None,
    );
    from_table.is_unlogged = false;
    from_table.hash();

    let mut to_table = Table::new(
        "test_order".to_string(),
        "items".to_string(),
        "test_order".to_string(),
        "items".to_string(),
        "postgres".to_string(),
        None,
        vec![
            int_column("test_order", "items", "id", 1),
            int_column("test_order", "items", "name", 2),
        ],
        vec![],
        vec![],
        vec![],
        None,
    );
    to_table.is_unlogged = true;
    to_table.hash();

    from_dump.tables.push(from_table);
    to_dump.tables.push(to_table);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Ignore);
    comparer.compare_tables().await.unwrap();
    let script = comparer.get_script();

    // SET UNLOGGED is still emitted, but only once and after the ADD
    // COLUMN — not interleaved inside the per-table ALTER block.
    let add_pos = script
        .find("alter table test_order.items add column name")
        .expect("add column must be emitted");
    let set_pos = script
        .find("alter table test_order.items set unlogged;")
        .expect("set unlogged must be emitted by the dedicated phase");
    assert!(
        add_pos < set_pos,
        "SET UNLOGGED must come from the dedicated phase, after the per-table ALTER: {}",
        script
    );
    assert_eq!(
        script.matches("set unlogged").count(),
        1,
        "SET UNLOGGED must be emitted exactly once (no inline + dedicated double-up): {}",
        script
    );
}

#[tokio::test]
async fn issue180_owned_sequence_persistence_only_diff_is_skipped() {
    // A sequence whose owning table flips persistence — and which has
    // no other diff — produces a redundant `ALTER SEQUENCE ... SET
    // UNLOGGED` followed by the full clause list. Both are noise: the
    // table's `ALTER TABLE ... SET UNLOGGED` already cascades to all
    // owned sequences. Suppress the entire ALTER SEQUENCE.
    use crate::dump::sequence::Sequence;

    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_table = Table::new(
        "test_order".to_string(),
        "items".to_string(),
        "test_order".to_string(),
        "items".to_string(),
        "postgres".to_string(),
        None,
        vec![int_column("test_order", "items", "id", 1)],
        vec![],
        vec![],
        vec![],
        None,
    );
    from_table.is_unlogged = false;
    from_table.hash();

    let mut to_table = from_table.clone();
    to_table.is_unlogged = true;
    to_table.hash();

    from_dump.tables.push(from_table);
    to_dump.tables.push(to_table);

    let make_seq = |is_unlogged: bool| {
        let mut s = Sequence::new(
            "test_order".to_string(),
            "items_id_seq".to_string(),
            "postgres".to_string(),
            "integer".to_string(),
            Some(1),
            Some(1),
            Some(2147483647),
            Some(1),
            false,
            Some(1),
            Some(1),
            Some("test_order".to_string()),
            Some("items".to_string()),
            Some("id".to_string()),
        );
        s.is_unlogged = is_unlogged;
        s.hash();
        s
    };
    from_dump.sequences.push(make_seq(false));
    to_dump.sequences.push(make_seq(true));

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Ignore);
    comparer.compare_sequences().await.unwrap();
    let script = comparer.get_script();

    assert!(
        !script.contains("alter sequence test_order.items_id_seq"),
        "owned-sequence persistence-only flip must be suppressed (table cascade handles it); got:\n{}",
        script
    );
}

#[tokio::test]
async fn issue180_owned_sequence_other_diff_skips_only_persistence_line() {
    // When the sequence has a real change (e.g. cache_size) AND the
    // owning table is also flipping persistence, we still need the
    // ALTER SEQUENCE — but not the `SET UNLOGGED|LOGGED` line, because
    // the table cascade handles that.
    use crate::dump::sequence::Sequence;

    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_table = Table::new(
        "test_order".to_string(),
        "items".to_string(),
        "test_order".to_string(),
        "items".to_string(),
        "postgres".to_string(),
        None,
        vec![int_column("test_order", "items", "id", 1)],
        vec![],
        vec![],
        vec![],
        None,
    );
    from_table.is_unlogged = false;
    from_table.hash();
    let mut to_table = from_table.clone();
    to_table.is_unlogged = true;
    to_table.hash();
    from_dump.tables.push(from_table);
    to_dump.tables.push(to_table);

    let make_seq = |is_unlogged: bool, cache: i64| {
        let mut s = Sequence::new(
            "test_order".to_string(),
            "items_id_seq".to_string(),
            "postgres".to_string(),
            "integer".to_string(),
            Some(1),
            Some(1),
            Some(2147483647),
            Some(1),
            false,
            Some(cache),
            Some(1),
            Some("test_order".to_string()),
            Some("items".to_string()),
            Some("id".to_string()),
        );
        s.is_unlogged = is_unlogged;
        s.hash();
        s
    };
    from_dump.sequences.push(make_seq(false, 1));
    to_dump.sequences.push(make_seq(true, 5));

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Ignore);
    comparer.compare_sequences().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("alter sequence test_order.items_id_seq"),
        "ALTER SEQUENCE must be emitted when non-persistence params changed: {}",
        script
    );
    assert!(
        script.contains("cache 5"),
        "the changed cache value must be in the script: {}",
        script
    );
    assert!(
        !script.contains("alter sequence test_order.items_id_seq set unlogged"),
        "SET UNLOGGED on owned sequence is redundant when the owning table is flipping persistence: {}",
        script
    );
}

#[tokio::test]
async fn issue180_standalone_sequence_persistence_change_still_emits_set() {
    // A sequence not owned by any table (or owned by a table whose
    // persistence is unchanged) must still get its own SET because no
    // table cascade applies.
    use crate::dump::sequence::Sequence;

    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let make_seq = |is_unlogged: bool| {
        let mut s = Sequence::new(
            "test_order".to_string(),
            "global_seq".to_string(),
            "postgres".to_string(),
            "integer".to_string(),
            Some(1),
            Some(1),
            Some(2147483647),
            Some(1),
            false,
            Some(1),
            Some(1),
            None,
            None,
            None,
        );
        s.is_unlogged = is_unlogged;
        s.hash();
        s
    };
    from_dump.sequences.push(make_seq(false));
    to_dump.sequences.push(make_seq(true));

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Ignore);
    comparer.compare_sequences().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("alter sequence test_order.global_seq set unlogged"),
        "standalone sequence must still emit SET UNLOGGED: {}",
        script
    );
}

#[tokio::test]
async fn issue179_quoted_routine_names_match_unqualified_calls() {
    // PGC's dump query wraps `nspname` / `proname` with `quote_ident`,
    // so a function named `MyFunc` lands here as `Routine.name = "MyFunc"`.
    // Phase 7 must strip those quotes when building its affected set —
    // otherwise the unqualified matcher (which scans quote-stripped
    // text) never lines up with the deparsed `myfunc(` in dependents.
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_routine = Routine::new(
        "\"TestDeps\"".to_string(),
        Oid(900),
        "\"MyFunc\"".to_string(),
        "sql".to_string(),
        "FUNCTION".to_string(),
        "integer".to_string(),
        "x integer".to_string(),
        None,
        None,
        "SELECT x * 2;".to_string(),
    );
    from_routine.hash();
    let mut to_routine = from_routine.clone();
    to_routine.return_type = "bigint".to_string();
    to_routine.source_code = "SELECT (x * 2)::bigint;".to_string();
    to_routine.hash();
    from_dump.routines.push(from_routine);
    to_dump.routines.push(to_routine);

    // Dependent uses the deparsed unqualified form `"MyFunc"(value)`
    // (PostgreSQL preserves the case-sensitive identifier with quotes
    // but drops the schema qualifier when the function is in
    // `search_path`).
    let chk = TableConstraint {
        catalog: "postgres".to_string(),
        schema: "public".to_string(),
        name: "chk_myfunc".to_string(),
        table_name: "items".to_string(),
        constraint_type: "CHECK".to_string(),
        is_deferrable: false,
        initially_deferred: false,
        definition: Some("CHECK (\"MyFunc\"(value) > 0)".to_string()),
        coninhcount: 0,
        is_enforced: true,
        no_inherit: false,
        nulls_not_distinct: false,
        comment: None,
    };

    let mut value_col = int_column("public", "items", "value", 1);
    value_col.is_nullable = false;

    let mut from_table = Table::new(
        "public".to_string(),
        "items".to_string(),
        "public".to_string(),
        "items".to_string(),
        "postgres".to_string(),
        None,
        vec![value_col.clone()],
        vec![chk.clone()],
        vec![],
        vec![],
        None,
    );
    from_table.hash();
    let mut to_table = Table::new(
        "public".to_string(),
        "items".to_string(),
        "public".to_string(),
        "items".to_string(),
        "postgres".to_string(),
        None,
        vec![value_col],
        vec![chk],
        vec![],
        vec![],
        None,
    );
    to_table.hash();
    from_dump.tables.push(from_table);
    to_dump.tables.push(to_table);

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_routines_and_views().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("alter table public.items add constraint chk_myfunc"),
        "quoted routine name must still match its unqualified deparsed dependent: {}",
        script
    );
}

#[tokio::test]
async fn issue179_defaults_only_change_emits_create_or_replace_no_cascade() {
    // PR #187 review (C10/C15): PostgreSQL accepts default-argument
    // changes via `CREATE OR REPLACE FUNCTION` for the same identity
    // signature/return type — there is no DROP+CREATE requirement.
    // The `arguments_defaults` field is included in `Routine::hash()`
    // so the diff is *detected*; the migration is then emitted as a
    // plain `CREATE OR REPLACE` form (no CASCADE drop, no Phase 7
    // dependent recreates).
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_routine = Routine::new(
        "test_deps".to_string(),
        Oid(900),
        "compute".to_string(),
        "sql".to_string(),
        "FUNCTION".to_string(),
        "integer".to_string(),
        "x integer".to_string(),
        Some("0".to_string()),
        None,
        "SELECT x * 2;".to_string(),
    );
    from_routine.hash();
    let mut to_routine = from_routine.clone();
    // ONLY the default value changes — every other field is identical.
    to_routine.arguments_defaults = Some("1".to_string());
    to_routine.hash();
    from_dump.routines.push(from_routine);
    to_dump.routines.push(to_routine);

    let table = issue179_items_table("integer", "test_deps.compute(0)", "integer");
    from_dump.tables.push(table.clone());
    to_dump.tables.push(table);

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_routines_and_views().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("create or replace function test_deps.compute(x integer DEFAULT 1)"),
        "defaults-only change must use CREATE OR REPLACE FUNCTION: {}",
        script
    );
    assert!(
        !script.contains("drop function if exists test_deps.compute"),
        "defaults-only change must NOT emit DROP FUNCTION CASCADE: {}",
        script
    );
    assert!(
        !script.contains("alter table test_deps.items add constraint chk_compute"),
        "Phase 7 must NOT fire on defaults-only change (no CASCADE happened): {}",
        script
    );
    assert!(
        !script.contains("CREATE INDEX IF NOT EXISTS idx_compute ON test_deps.items"),
        "no index recreate on defaults-only change: {}",
        script
    );
}

#[test]
fn issue180_parse_fk_referenced_table_word_boundary() {
    // PR #184 review: a naive `find("references ")` substring match
    // can pick up the literal text inside a quoted column name in the
    // FK column list. The matcher must be anchored to a word boundary
    // and the keyword must be followed by whitespace.
    assert_eq!(
        Comparer::parse_fk_referenced_table(
            "FOREIGN KEY (col_a) REFERENCES public.target(id)",
            "public",
        ),
        Some(("public".to_string(), "target".to_string())),
        "happy-path FK definition must parse"
    );
    // Column literally named `"references "` (with trailing space) in
    // the FK column list. Naive substring search would lock onto it
    // before the real keyword and parse garbage.
    assert_eq!(
        Comparer::parse_fk_referenced_table(
            "FOREIGN KEY (\"references \", col_b) REFERENCES public.target(id)",
            "public",
        ),
        Some(("public".to_string(), "target".to_string()))
    );
    // Column named `references_count` — substring match on
    // "references" without the right-side word-boundary check would
    // see this column first and try to parse what follows.
    assert_eq!(
        Comparer::parse_fk_referenced_table(
            "FOREIGN KEY (references_count) REFERENCES public.target(id)",
            "public",
        ),
        Some(("public".to_string(), "target".to_string()))
    );
}

#[test]
fn issue180_parse_fk_referenced_table_quoted_identifier_with_dot() {
    // PR #184 review: a quoted identifier may contain a literal `.`,
    // and the schema/name split must respect quotes — otherwise the
    // first dot inside the quoted segment is taken as the boundary
    // and the parsed pair is nonsensical.
    assert_eq!(
        Comparer::parse_fk_referenced_table(
            "FOREIGN KEY (col) REFERENCES \"weird.schema\".\"t\"(id)",
            "public",
        ),
        Some(("weird.schema".to_string(), "t".to_string()))
    );
    // Both halves quoted with embedded dots — the split must still
    // land on the dot OUTSIDE every quoted segment.
    assert_eq!(
        Comparer::parse_fk_referenced_table(
            "FOREIGN KEY (col) REFERENCES \"a.b\".\"c.d\"(id)",
            "public",
        ),
        Some(("a.b".to_string(), "c.d".to_string()))
    );
}

#[test]
fn pr187_parse_fk_skips_keyword_inside_quoted_column_name() {
    // PR #187 review (C7): a column literally named
    // `"my references col"` puts the bytes `references` between two
    // spaces, passing the naive boundary check, then returns `None`
    // from the false match without ever reaching the real keyword.
    // The scanner must skip matches that fall inside a double-quoted
    // identifier.
    assert_eq!(
        Comparer::parse_fk_referenced_table(
            "FOREIGN KEY (\"my references col\") REFERENCES public.target(id)",
            "public",
        ),
        Some(("public".to_string(), "target".to_string())),
        "FK keyword must still be located even with `references` inside a quoted column name"
    );
}

#[test]
fn pr187_parse_fk_handles_dollar_in_identifier() {
    // PR #187 review (C8): PostgreSQL identifiers may contain `$`,
    // so the unquoted-identifier scan must include it. Otherwise a
    // target like `public.parent$table` is truncated to
    // `public.parent`.
    assert_eq!(
        Comparer::parse_fk_referenced_table(
            "FOREIGN KEY (col) REFERENCES public.parent$table(id)",
            "public",
        ),
        Some(("public".to_string(), "parent$table".to_string()))
    );
}

#[test]
fn pr187_definition_references_any_skips_string_literals() {
    // PR #187 review (C4): a SQL string literal containing routine
    // text — `CHECK (msg <> 'compute(value)')` — must not trigger
    // the unqualified-call matcher. The `definition_references_any`
    // pre-pass must blank out single-quoted literals before scanning.
    let mut affected: HashSet<(String, String)> = HashSet::new();
    affected.insert(("public".to_string(), "compute".to_string()));
    assert!(
        !Comparer::definition_references_any("CHECK (msg <> 'compute(value)')", &affected),
        "literal text must not be treated as a function call"
    );
    // Sanity check: a real call outside a literal still matches.
    assert!(
        Comparer::definition_references_any(
            "CHECK (compute(value) > 0 AND msg <> 'compute(value)')",
            &affected
        ),
        "real call outside the literal must still match"
    );
}

#[tokio::test]
async fn pr187_persistence_ordering_works_with_quoted_identifiers() {
    // PR #187 review (C2): mixed-case table names round-trip into
    // `Table.schema` / `Table.name` with surrounding quotes
    // (`quote_ident` in the dump query). The FK parser strips quotes
    // from its returned `(schema, name)`. Without normalising the
    // lookup map to the same quote-stripped form, FK edges between
    // quoted-identifier tables go missing and persistence flips fall
    // back to alphabetical order, which PostgreSQL rejects for FK
    // chains. Build a parent→child chain whose names are quoted and
    // assert the SET UNLOGGED order is leaves-first.
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());
    let mk = |name: &str, is_unlogged: bool, fk: Option<(&str, &str, &str)>| {
        // Wrap the schema/name in quotes the way `quote_ident` would.
        let mut t = issue180_logged_table("\"TestOrder\"", name, is_unlogged, fk);
        t.schema = "\"TestOrder\"".to_string();
        t.raw_schema = "\"TestOrder\"".to_string();
        t
    };
    from_dump.tables.push(mk("\"Grand\"", false, None));
    from_dump.tables.push(mk(
        "\"Parent\"",
        false,
        Some(("grand_id", "\"TestOrder\"", "\"Grand\"")),
    ));
    from_dump.tables.push(mk(
        "\"Child\"",
        false,
        Some(("parent_id", "\"TestOrder\"", "\"Parent\"")),
    ));
    to_dump.tables.push(mk("\"Grand\"", true, None));
    to_dump.tables.push(mk(
        "\"Parent\"",
        true,
        Some(("grand_id", "\"TestOrder\"", "\"Grand\"")),
    ));
    to_dump.tables.push(mk(
        "\"Child\"",
        true,
        Some(("parent_id", "\"TestOrder\"", "\"Parent\"")),
    ));

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Ignore);
    comparer.compare_tables().await.unwrap();
    let script = comparer.get_script();

    let pos_child = script
        .find("alter table \"TestOrder\".\"Child\" set unlogged;")
        .expect("child SET UNLOGGED missing");
    let pos_parent = script
        .find("alter table \"TestOrder\".\"Parent\" set unlogged;")
        .expect("parent SET UNLOGGED missing");
    let pos_grand = script
        .find("alter table \"TestOrder\".\"Grand\" set unlogged;")
        .expect("grand SET UNLOGGED missing");
    assert!(
        pos_child < pos_parent && pos_parent < pos_grand,
        "FK-leaf-first order must hold for quoted identifiers too: {script}"
    );
}

#[tokio::test]
async fn pr187_persistence_ordering_includes_in_place_alterable_fks() {
    // PR #187 review (C13): an FK whose definition differs only in
    // an in-place-alterable property (deferrability, enforced,
    // no_inherit, comment) is NOT dropped by `compare_tables` — it
    // stays live until `compare_foreign_keys` ALTERs it. The live
    // FK adjacency for the SET phase must include it, otherwise
    // chains where one FK is being toggled deferrable/enforced fall
    // back to alphabetical SET order.
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());
    from_dump
        .tables
        .push(issue180_logged_table("test_order", "parent", false, None));
    from_dump.tables.push(issue180_logged_table(
        "test_order",
        "child",
        false,
        Some(("parent_id", "test_order", "parent")),
    ));
    to_dump
        .tables
        .push(issue180_logged_table("test_order", "parent", true, None));
    let mut to_child = issue180_logged_table(
        "test_order",
        "child",
        true,
        Some(("parent_id", "test_order", "parent")),
    );
    // Toggle the FK's deferrability — `can_be_altered_to` accepts
    // this, so the FK survives `compare_tables` and is still live at
    // the SET point.
    if let Some(fk) = to_child
        .constraints
        .iter_mut()
        .find(|c| c.constraint_type.eq_ignore_ascii_case("foreign key"))
    {
        fk.is_deferrable = true;
        fk.initially_deferred = true;
    }
    to_child.hash();
    to_dump.tables.push(to_child);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Ignore);
    comparer.compare_tables().await.unwrap();
    let script = comparer.get_script();

    let pos_child = script
        .find("alter table test_order.child set unlogged;")
        .expect("child SET UNLOGGED missing");
    let pos_parent = script
        .find("alter table test_order.parent set unlogged;")
        .expect("parent SET UNLOGGED missing");
    assert!(
        pos_child < pos_parent,
        "child must come before parent even when the FK between them is being in-place ALTERed: {script}"
    );
}

#[test]
fn pr187_unqualified_matcher_unicode_boundary_rejects_longer_identifier() {
    // PR #187 review (C17): the boundary check used raw ASCII byte
    // tests, which treated Cyrillic neighbours as non-identifier and
    // let `функция` match inside `мояфункция(`. The check now uses
    // character-class identifier rules, so Cyrillic-letter neighbours
    // correctly extend the identifier and reject the match.
    let mut affected: HashSet<(String, String)> = HashSet::new();
    affected.insert(("public".to_string(), "функция".to_string()));
    assert!(
        !Comparer::definition_references_any("CHECK (мояфункция(x) > 0)", &affected),
        "unicode letter to the left must extend the identifier"
    );
    assert!(
        !Comparer::definition_references_any("CHECK (функцияд(x) > 0)", &affected),
        "unicode letter to the right must extend the identifier"
    );
    // Sanity: a clean Cyrillic call still matches.
    assert!(
        Comparer::definition_references_any("CHECK (функция(x) > 0)", &affected),
        "standalone unicode call must still match"
    );
}

#[test]
fn issue180_parse_fk_referenced_table_handles_non_ascii_column_names() {
    // PR #184 follow-up review: `parse_fk_referenced_table` previously
    // built the case-insensitive haystack via `to_lowercase()`, which
    // can change byte length for some non-ASCII characters
    // (e.g. capital Turkish dotted I, `İ`, lowercases to a multi-char
    // sequence with a different UTF-8 length). The keyword position
    // came from the lowercased haystack but the slice that produces
    // the parsed identifier reaches back into `def`, so a
    // length-changing lowercasing would land mid-codepoint and panic.
    // `to_ascii_lowercase()` is byte-length-preserving — pin that
    // contract by parsing FK definitions whose column list contains
    // identifiers that trip every byte-length-changing lowercase
    // conversion in common locales.
    //
    // Quoted column with capital `İ` (U+0130). With `to_lowercase()`
    // this produces `i\u{0307}` (3 bytes total); `to_ascii_lowercase`
    // leaves the 2-byte `İ` alone, so byte offsets line up.
    assert_eq!(
        Comparer::parse_fk_referenced_table(
            "FOREIGN KEY (\"\u{0130}d\") REFERENCES public.target(id)",
            "public",
        ),
        Some(("public".to_string(), "target".to_string()))
    );
    // German sharp S (`ß`, U+00DF). `to_lowercase()` keeps it as `ß`,
    // but the inverse — uppercase `ẞ` (U+1E9E) lowercasing to `ß` —
    // is length-preserving in UTF-8 too. Use a Cyrillic lowercase
    // identifier here just to round out coverage of identifiers whose
    // bytes lie outside the ASCII range.
    assert_eq!(
        Comparer::parse_fk_referenced_table(
            "FOREIGN KEY (\"русское_имя\") REFERENCES public.target(id)",
            "public",
        ),
        Some(("public".to_string(), "target".to_string()))
    );
    // Same case in the qualified target identifier.
    assert_eq!(
        Comparer::parse_fk_referenced_table(
            "FOREIGN KEY (col) REFERENCES \"тест\".\"target\"(id)",
            "public",
        ),
        Some(("тест".to_string(), "target".to_string()))
    );
}

#[test]
fn issue190_parse_fk_unqualified_target_falls_back_to_owner_schema() {
    // Issue #190: `pg_get_constraintdef` omits the schema qualifier
    // when the target is reachable via `search_path` — typical for
    // tables in `public`. Pre-fix the parser returned `None` for these
    // and the FK edge was silently dropped from the persistence-flip
    // adjacency, leaving FK chains in `public` ordered alphabetically
    // (the order PostgreSQL rejects).
    //
    // Plain unqualified target — same schema as the FK owner.
    assert_eq!(
        Comparer::parse_fk_referenced_table(
            "FOREIGN KEY (parent_id) REFERENCES parent(id)",
            "public",
        ),
        Some(("public".to_string(), "parent".to_string())),
        "unqualified target must resolve to (owner_schema, target)"
    );
    // Quoted unqualified target — the quotes must be stripped to
    // match the comparer's normalised `to_index_by_key` keys (which
    // strip quotes on the index side too).
    assert_eq!(
        Comparer::parse_fk_referenced_table(
            "FOREIGN KEY (col) REFERENCES \"MixedCase\"(id)",
            "public",
        ),
        Some(("public".to_string(), "MixedCase".to_string()))
    );
    // Quoted owner schema (e.g. mixed-case schema names land here as
    // `"MySchema"` via `quote_ident`) — the fallback must strip the
    // surrounding quotes from the owner schema too, otherwise the
    // produced pair misses the index-side lookup keys.
    assert_eq!(
        Comparer::parse_fk_referenced_table(
            "FOREIGN KEY (col) REFERENCES parent(id)",
            "\"MySchema\"",
        ),
        Some(("MySchema".to_string(), "parent".to_string()))
    );
    // ON UPDATE / ON DELETE clauses follow the target — make sure
    // they don't confuse the boundary scan.
    assert_eq!(
        Comparer::parse_fk_referenced_table(
            "FOREIGN KEY (col) REFERENCES parent(id) ON DELETE CASCADE",
            "public",
        ),
        Some(("public".to_string(), "parent".to_string()))
    );
}

/// Issue #190 end-to-end: a FK chain in `public` whose deparsed
/// definition uses unqualified target names must still be ordered
/// leaves-first by `emit_persistence_changes`. Pre-fix the unqualified
/// targets returned `None` from the parser, the adjacency went empty,
/// and the SET UNLOGGED order fell back to alphabetical (`child`
/// emitted *after* `parent` — exactly the order PostgreSQL rejects).
#[tokio::test]
async fn issue190_set_unlogged_orders_unqualified_public_fk_chain() {
    // Builder that matches `issue180_logged_table` but emits FK
    // definitions WITHOUT the schema qualifier — the
    // `pg_get_constraintdef` output shape that exposes the issue.
    fn make_table(name: &str, is_unlogged: bool, fk_target: Option<(&str, &str)>) -> Table {
        let mut id_col = int_column("public", name, "id", 1);
        id_col.is_nullable = false;
        let mut constraints: Vec<TableConstraint> = vec![TableConstraint {
            catalog: "postgres".to_string(),
            schema: "public".to_string(),
            name: format!("{name}_pkey"),
            table_name: name.to_string(),
            constraint_type: "PRIMARY KEY".to_string(),
            is_deferrable: false,
            initially_deferred: false,
            definition: Some("PRIMARY KEY (id)".to_string()),
            coninhcount: 0,
            is_enforced: true,
            no_inherit: false,
            nulls_not_distinct: false,
            comment: None,
        }];

        let mut columns = vec![id_col];
        if let Some((fk_col, fk_table)) = fk_target {
            let mut ref_col = int_column("public", name, fk_col, 2);
            ref_col.is_nullable = true;
            columns.push(ref_col);
            // Unqualified `REFERENCES parent(id)` — no `public.`
            // qualifier. This is what `pg_get_constraintdef` returns
            // when the target is reachable via `search_path`.
            constraints.push(TableConstraint {
                catalog: "postgres".to_string(),
                schema: "public".to_string(),
                name: format!("{name}_{fk_col}_fkey"),
                table_name: name.to_string(),
                constraint_type: "FOREIGN KEY".to_string(),
                is_deferrable: false,
                initially_deferred: false,
                definition: Some(format!("FOREIGN KEY ({fk_col}) REFERENCES {fk_table}(id)")),
                coninhcount: 0,
                is_enforced: true,
                no_inherit: false,
                nulls_not_distinct: false,
                comment: None,
            });
        }

        let mut table = Table::new(
            "public".to_string(),
            name.to_string(),
            "public".to_string(),
            name.to_string(),
            "postgres".to_string(),
            None,
            columns,
            constraints,
            vec![],
            vec![],
            None,
        );
        table.is_unlogged = is_unlogged;
        table.hash();
        table
    }

    // FROM: child → parent in public, both LOGGED.
    // TO:   same chain, both UNLOGGED. The FK must survive in TO
    // unchanged (live edge) so the adjacency considers it.
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());
    from_dump.tables.push(make_table("parent", false, None));
    from_dump
        .tables
        .push(make_table("child", false, Some(("parent_id", "parent"))));
    to_dump.tables.push(make_table("parent", true, None));
    to_dump
        .tables
        .push(make_table("child", true, Some(("parent_id", "parent"))));

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Ignore);
    comparer.compare_tables().await.unwrap();
    let script = comparer.get_script();

    let child_pos = script
        .find("alter table public.child set unlogged;")
        .expect("child SET UNLOGGED must be emitted");
    let parent_pos = script
        .find("alter table public.parent set unlogged;")
        .expect("parent SET UNLOGGED must be emitted");
    assert!(
        child_pos < parent_pos,
        "child (referrer) must SET UNLOGGED BEFORE parent (referenced); \
         got child@{} parent@{} — alphabetical order would put `child` \
         after `parent` and PostgreSQL would reject the SET on `parent` \
         while `child` is still LOGGED:\n{}",
        child_pos,
        parent_pos,
        script
    );
}

#[tokio::test]
async fn issue180_set_unlogged_skips_ordering_for_new_fks_added_later() {
    // PR #184 review (FK-timing): when an FK is brand-new in TO it is
    // not yet active at the moment `emit_persistence_changes` runs —
    // `compare_foreign_keys` adds it strictly after. The adjacency
    // must therefore filter to FKs that exist UNCHANGED in both
    // FROM and TO. Without that filter, an alphabetical pair would
    // be over-ordered as if the new FK were already live.
    //
    // Setup: child references parent in TO (new FK). FROM has no FK.
    // Both flip from LOGGED to UNLOGGED.
    //
    // With the live-FK-set tightening, the adjacency is empty, so
    // ordering falls back to alphabetical (deterministic via the
    // sort_key in the topo sort). This is safe because PG won't see
    // the FK link until after the SET phase.
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());
    from_dump
        .tables
        .push(issue180_logged_table("test_order", "parent", false, None));
    // FROM child has no FK to parent — the FK is new in TO.
    from_dump
        .tables
        .push(issue180_logged_table("test_order", "child", false, None));
    to_dump
        .tables
        .push(issue180_logged_table("test_order", "parent", true, None));
    to_dump.tables.push(issue180_logged_table(
        "test_order",
        "child",
        true,
        Some(("parent_id", "test_order", "parent")),
    ));

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Ignore);
    comparer.compare_tables().await.unwrap();
    let script = comparer.get_script();

    // Both SET UNLOGGED statements must be present; ordering between
    // them is not constrained by the (not-yet-live) new FK.
    assert!(script.contains("alter table test_order.child set unlogged;"));
    assert!(script.contains("alter table test_order.parent set unlogged;"));
}

#[test]
fn issue180_sequence_only_persistence_change_uses_hash_diff() {
    // PR #184 review: `is_only_persistence_change` clones, equalises
    // `is_unlogged` to FROM, recomputes the hash, and compares.
    // That keeps the check honest if `Sequence::hash` later starts
    // covering a new field. This test pins the contract by exercising
    // both directions: identical-except-persistence returns true; a
    // hashed field different (here `cache_size`) returns false.
    use crate::dump::sequence::Sequence;

    let make = |is_unlogged: bool, cache: i64| {
        let mut s = Sequence::new(
            "public".to_string(),
            "s".to_string(),
            "postgres".to_string(),
            "integer".to_string(),
            Some(1),
            Some(1),
            Some(2147483647),
            Some(1),
            false,
            Some(cache),
            Some(1),
            None,
            None,
            None,
        );
        s.is_unlogged = is_unlogged;
        s.hash();
        s
    };
    let from = make(false, 1);
    let to_only_persistence = make(true, 1);
    let to_persistence_and_cache = make(true, 5);
    assert!(
        to_only_persistence.is_only_persistence_change(&from),
        "identical except is_unlogged must be detected as persistence-only"
    );
    assert!(
        !to_persistence_and_cache.is_only_persistence_change(&from),
        "a hashed field difference must block the persistence-only suppression"
    );
}

/// Build a view whose definition textually references `test_deps.compute`.
/// Returns a regular or materialized view depending on `is_materialized`.
fn issue189_view(name: &str, is_materialized: bool) -> View {
    let mut view = View::new(
        name.to_string(),
        " SELECT test_deps.compute(value) AS c\n   FROM test_deps.items;".to_string(),
        "test_deps".to_string(),
        vec!["test_deps.items".to_string()],
    );
    view.is_materialized = is_materialized;
    view.hash();
    view
}

#[tokio::test]
async fn issue189_signature_change_recreates_byte_identical_view() {
    // The view's hash is unchanged between FROM and TO, but the function
    // it references undergoes a signature change (integer → bigint),
    // forcing DROP FUNCTION ... CASCADE. PostgreSQL silently drops the
    // view as part of the cascade. Phase 7 must re-emit the view so the
    // migration leaves the database in a consistent state.
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    from_dump
        .routines
        .push(issue179_compute_routine("integer", "SELECT x * 2;"));
    to_dump.routines.push(issue179_compute_routine(
        "bigint",
        "SELECT (x * 2)::bigint;",
    ));

    from_dump.views.push(issue189_view("v_things", false));
    to_dump.views.push(issue189_view("v_things", false));

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_routines_and_views().await.unwrap();
    let script = comparer.get_script();

    let drop_pos = script
        .find("drop function if exists test_deps.compute (x integer) cascade;")
        .expect("CASCADE drop must be emitted for the signature change");
    let create_fn_pos = script
        .find("create or replace function test_deps.compute(x integer) returns bigint")
        .expect("function recreate must be emitted");
    assert!(drop_pos < create_fn_pos);

    let view_pos = script
        .find("CREATE OR REPLACE VIEW test_deps.v_things")
        .expect("byte-identical view must be re-emitted as CREATE OR REPLACE VIEW after CASCADE");
    assert!(
        create_fn_pos < view_pos,
        "view recreate must run after the function recreate so the new signature is in place"
    );
}

#[tokio::test]
async fn issue189_signature_change_recreates_byte_identical_materialized_view() {
    // Same scenario as the regular-view case but with a materialized
    // view. PostgreSQL CASCADE drops these via `pg_depend` the same way,
    // so Phase 7 must emit a `create materialized view if not exists`.
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    from_dump
        .routines
        .push(issue179_compute_routine("integer", "SELECT x * 2;"));
    to_dump.routines.push(issue179_compute_routine(
        "bigint",
        "SELECT (x * 2)::bigint;",
    ));

    from_dump.views.push(issue189_view("mv_things", true));
    to_dump.views.push(issue189_view("mv_things", true));

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_routines_and_views().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("create materialized view if not exists test_deps.mv_things"),
        "materialized view must be re-emitted with IF NOT EXISTS: {}",
        script
    );
}

#[tokio::test]
async fn issue189_view_not_referencing_routine_is_not_recreated() {
    // A view that doesn't textually reference the CASCADE-affected
    // routine must be left alone — re-emitting it would clutter the
    // migration and could re-introduce a stale definition if the user
    // has the same view in both dumps for unrelated reasons.
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    from_dump
        .routines
        .push(issue179_compute_routine("integer", "SELECT x * 2;"));
    to_dump.routines.push(issue179_compute_routine(
        "bigint",
        "SELECT (x * 2)::bigint;",
    ));

    let mut unrelated_from = View::new(
        "v_other".to_string(),
        " SELECT value FROM test_deps.items;".to_string(),
        "test_deps".to_string(),
        vec!["test_deps.items".to_string()],
    );
    unrelated_from.hash();
    let mut unrelated_to = View::new(
        "v_other".to_string(),
        " SELECT value FROM test_deps.items;".to_string(),
        "test_deps".to_string(),
        vec!["test_deps.items".to_string()],
    );
    unrelated_to.hash();
    from_dump.views.push(unrelated_from);
    to_dump.views.push(unrelated_to);

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_routines_and_views().await.unwrap();
    let script = comparer.get_script();

    assert!(
        !script.contains("v_other"),
        "view that doesn't reference the cascaded routine must not be re-emitted: {}",
        script
    );
}

#[tokio::test]
async fn issue189_view_recreate_skipped_when_to_definition_no_longer_references_routine() {
    // TO-side gate (PR #186): if the TO view's definition was rewritten
    // to no longer call the affected routine, the CASCADE drop never
    // touches it (no pg_depend link). `compare_routines_and_views`
    // already emits the rewrite via the normal hash-diff path; Phase 7
    // must stay silent so we don't re-emit the view twice.
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    from_dump
        .routines
        .push(issue179_compute_routine("integer", "SELECT x * 2;"));
    to_dump.routines.push(issue179_compute_routine(
        "bigint",
        "SELECT (x * 2)::bigint;",
    ));

    from_dump.views.push(issue189_view("v_things", false));
    // TO view: same name, but the definition no longer references the
    // function — different hash, so Phase 5 handles it.
    let mut to_view = View::new(
        "v_things".to_string(),
        " SELECT value AS c FROM test_deps.items;".to_string(),
        "test_deps".to_string(),
        vec!["test_deps.items".to_string()],
    );
    to_view.hash();
    to_dump.views.push(to_view);

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_routines_and_views().await.unwrap();
    let script = comparer.get_script();

    // The view must appear exactly once (the normal hash-diff path), not
    // a second time from Phase 7's recreate block.
    let recreate_section_start = script.find("Recreate dependents dropped by CASCADE: Start");
    if let Some(start) = recreate_section_start {
        let recreate_end = script[start..]
            .find("Recreate dependents dropped by CASCADE: End")
            .map(|e| start + e)
            .unwrap_or(script.len());
        let recreate_section = &script[start..recreate_end];
        assert!(
            !recreate_section.contains("v_things"),
            "Phase 7 must not re-emit a view whose TO definition no longer references the routine: {}",
            recreate_section
        );
    }
}

#[test]
fn issue189_rewrite_create_view_anchored_to_prefix() {
    // PR #195 review (Copilot): the helper must not be fooled by the
    // literal text `CREATE OR REPLACE VIEW` appearing inside the view
    // definition body that `View::get_script` appends after the
    // `create view` prefix. A whole-script `contains` early-return
    // would skip the rewrite and leave the leading `create view`
    // unchanged, which is not idempotent against a surviving view.
    let script = "create view public.v_with_literal as\n\
                  SELECT 'CREATE OR REPLACE VIEW pretend.v AS SELECT 1' AS payload;\n\n";
    let rewritten = rewrite_create_view_to_create_or_replace(script);
    assert!(
        rewritten.starts_with("CREATE OR REPLACE VIEW public.v_with_literal as\n"),
        "leading `create view` must be rewritten even when the body \
         contains the same phrase as a string literal: {}",
        rewritten
    );
    // Already in the desired form — return unchanged (no double rewrite).
    let already = "CREATE OR REPLACE VIEW public.v as\nSELECT 1;\n";
    assert_eq!(rewrite_create_view_to_create_or_replace(already), already);
}

#[tokio::test]
async fn issue189_view_definition_with_create_or_replace_literal_is_recreated() {
    // End-to-end pin for the PR #195 reviewer concern: a view whose
    // definition embeds the literal text `CREATE OR REPLACE VIEW` must
    // still emit a properly idempotent `CREATE OR REPLACE VIEW` prefix
    // when Phase 7 re-emits it after a CASCADE drop.
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    from_dump
        .routines
        .push(issue179_compute_routine("integer", "SELECT x * 2;"));
    to_dump.routines.push(issue179_compute_routine(
        "bigint",
        "SELECT (x * 2)::bigint;",
    ));

    let definition = " SELECT test_deps.compute(value) AS c,\n        \
                      'CREATE OR REPLACE VIEW evil.v AS SELECT 1' AS payload\n   \
                      FROM test_deps.items;";
    let mut from_view = View::new(
        "v_things".to_string(),
        definition.to_string(),
        "test_deps".to_string(),
        vec!["test_deps.items".to_string()],
    );
    from_view.hash();
    let mut to_view = View::new(
        "v_things".to_string(),
        definition.to_string(),
        "test_deps".to_string(),
        vec!["test_deps.items".to_string()],
    );
    to_view.hash();
    from_dump.views.push(from_view);
    to_dump.views.push(to_view);

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_routines_and_views().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("CREATE OR REPLACE VIEW test_deps.v_things"),
        "view recreate must emit `CREATE OR REPLACE VIEW` at the leading \
         statement even when the body contains the same phrase: {}",
        script
    );
    // The body's literal must survive unchanged — we do NOT want a
    // rewrite that mangles a non-prefix occurrence.
    assert!(
        script.contains("'CREATE OR REPLACE VIEW evil.v AS SELECT 1'"),
        "literal inside the view body must be left intact: {}",
        script
    );
}

#[tokio::test]
async fn issue189_view_recreate_skipped_when_routine_unchanged() {
    // No CASCADE — no recreate. Mirrors
    // `issue179_recreate_skipped_when_routine_unchanged` for views.
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let routine = issue179_compute_routine("integer", "SELECT x * 2;");
    from_dump.routines.push(routine.clone());
    to_dump.routines.push(routine);

    from_dump.views.push(issue189_view("v_things", false));
    to_dump.views.push(issue189_view("v_things", false));

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_routines_and_views().await.unwrap();
    let script = comparer.get_script();

    assert!(
        !script.contains("Recreate dependents dropped by CASCADE"),
        "no CASCADE drop happened — recreate phase must stay silent: {}",
        script
    );
    assert!(
        !script.contains("CREATE OR REPLACE VIEW test_deps.v_things"),
        "view must not be re-emitted when the function is unchanged: {}",
        script
    );
}

// ============================================================
// Issue #188 — pg_depend-driven secondary dependent restoration
// ============================================================

use crate::dump::column_dependent::{ColumnDependent, ColumnDependentKind};

/// Phase 7 / Path A: A routine signature change CASCADE-drops a
/// generated column. PostgreSQL also drops a plain index on that
/// column *because the index depends on the column, not the routine*.
/// The text-based scanner cannot see the dependency. The
/// `column_dependents` graph must drive a recreate of the index.
#[tokio::test]
async fn issue188_phase7_restores_plain_index_on_generated_column() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    from_dump
        .routines
        .push(issue179_compute_routine("integer", "SELECT x * 2;"));
    to_dump.routines.push(issue179_compute_routine(
        "bigint",
        "SELECT (x * 2)::bigint;",
    ));

    // Add a plain index ON the generated column (no function reference)
    // to both sides. PostgreSQL would CASCADE-drop it along with the
    // column; Phase 7's text scan does not detect this case.
    let plain_idx = TableIndex {
        schema: "test_deps".to_string(),
        table: "items".to_string(),
        name: "idx_gen_col".to_string(),
        catalog: Some("postgres".to_string()),
        indexdef: "CREATE INDEX idx_gen_col ON test_deps.items USING btree (gen_col)".to_string(),
        is_partition_index: false,
        comment: None,
    };

    let mut from_table =
        issue179_items_table("integer", "test_deps.compute(0)::integer", "integer");
    from_table.indexes.push(plain_idx.clone());
    from_table.hash();
    from_dump.tables.push(from_table);

    let mut to_table = issue179_items_table("bigint", "test_deps.compute(0)", "bigint");
    to_table.indexes.push(plain_idx);
    to_table.hash();
    to_dump.tables.push(to_table);

    // pg_depend at dump time recorded that `idx_gen_col` depends on
    // `gen_col`. Without this, the text scanner has no way to discover
    // the secondary dependency.
    from_dump.column_dependents.push(ColumnDependent {
        schema: "test_deps".to_string(),
        table: "items".to_string(),
        column: "gen_col".to_string(),
        kind: ColumnDependentKind::Index,
        dep_schema: "test_deps".to_string(),
        dep_table: "items".to_string(),
        dep_name: "idx_gen_col".to_string(),
    });

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_routines_and_views().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("CREATE INDEX IF NOT EXISTS idx_gen_col ON test_deps.items"),
        "plain index on generated column must be re-emitted from pg_depend graph: {}",
        script
    );
}

/// Phase 7 / Path A: same idea for a CHECK constraint that references
/// the generated column but does not name the routine.
#[tokio::test]
async fn issue188_phase7_restores_check_constraint_on_generated_column() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    from_dump
        .routines
        .push(issue179_compute_routine("integer", "SELECT x * 2;"));
    to_dump.routines.push(issue179_compute_routine(
        "bigint",
        "SELECT (x * 2)::bigint;",
    ));

    let chk_on_col = TableConstraint {
        catalog: "postgres".to_string(),
        schema: "test_deps".to_string(),
        name: "chk_gen_positive".to_string(),
        table_name: "items".to_string(),
        constraint_type: "CHECK".to_string(),
        is_deferrable: false,
        initially_deferred: false,
        // References only the generated column — no function name.
        definition: Some("CHECK (gen_col > 0)".to_string()),
        coninhcount: 0,
        is_enforced: true,
        no_inherit: false,
        nulls_not_distinct: false,
        comment: None,
    };

    let mut from_table =
        issue179_items_table("integer", "test_deps.compute(0)::integer", "integer");
    from_table.constraints.push(chk_on_col.clone());
    from_table.hash();
    from_dump.tables.push(from_table);

    let mut to_table = issue179_items_table("bigint", "test_deps.compute(0)", "bigint");
    to_table.constraints.push(chk_on_col);
    to_table.hash();
    to_dump.tables.push(to_table);

    from_dump.column_dependents.push(ColumnDependent {
        schema: "test_deps".to_string(),
        table: "items".to_string(),
        column: "gen_col".to_string(),
        kind: ColumnDependentKind::Constraint,
        dep_schema: "test_deps".to_string(),
        dep_table: "items".to_string(),
        dep_name: "chk_gen_positive".to_string(),
    });

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_routines_and_views().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("alter table test_deps.items add constraint chk_gen_positive"),
        "CHECK constraint anchored on generated column must be re-emitted: {}",
        script
    );
    assert!(
        script.contains("alter table test_deps.items drop constraint if exists chk_gen_positive;"),
        "drop-if-exists guard for column-anchored CHECK constraint missing: {}",
        script
    );
}

/// Phase 7 / Path A: TO-side gate. When the dependent is intentionally
/// removed in TO, we must NOT resurrect it via the pg_depend graph.
#[tokio::test]
async fn issue188_phase7_skips_dependent_absent_from_to() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    from_dump
        .routines
        .push(issue179_compute_routine("integer", "SELECT x * 2;"));
    to_dump.routines.push(issue179_compute_routine(
        "bigint",
        "SELECT (x * 2)::bigint;",
    ));

    // FROM has the plain index; TO deliberately omits it.
    let plain_idx = TableIndex {
        schema: "test_deps".to_string(),
        table: "items".to_string(),
        name: "idx_gen_col".to_string(),
        catalog: Some("postgres".to_string()),
        indexdef: "CREATE INDEX idx_gen_col ON test_deps.items USING btree (gen_col)".to_string(),
        is_partition_index: false,
        comment: None,
    };

    let mut from_table =
        issue179_items_table("integer", "test_deps.compute(0)::integer", "integer");
    from_table.indexes.push(plain_idx);
    from_table.hash();
    from_dump.tables.push(from_table);

    // TO-side table does NOT include `idx_gen_col`.
    to_dump.tables.push(issue179_items_table(
        "bigint",
        "test_deps.compute(0)",
        "bigint",
    ));

    from_dump.column_dependents.push(ColumnDependent {
        schema: "test_deps".to_string(),
        table: "items".to_string(),
        column: "gen_col".to_string(),
        kind: ColumnDependentKind::Index,
        dep_schema: "test_deps".to_string(),
        dep_table: "items".to_string(),
        dep_name: "idx_gen_col".to_string(),
    });

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_routines_and_views().await.unwrap();
    let script = comparer.get_script();

    assert!(
        !script.contains("idx_gen_col"),
        "index absent from TO must not be resurrected from pg_depend: {}",
        script
    );
}

/// Phase 7 / Path A: a UNIQUE/PK constraint and its backing index both
/// appear in `pg_depend`. The constraint emission already recreates the
/// index, so the dedup logic must skip the index branch when the same
/// name exists as a constraint on the TO-side table.
#[tokio::test]
async fn issue188_phase7_skips_index_backing_constraint() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    from_dump
        .routines
        .push(issue179_compute_routine("integer", "SELECT x * 2;"));
    to_dump.routines.push(issue179_compute_routine(
        "bigint",
        "SELECT (x * 2)::bigint;",
    ));

    let uniq_constraint = TableConstraint {
        catalog: "postgres".to_string(),
        schema: "test_deps".to_string(),
        name: "items_gen_col_key".to_string(),
        table_name: "items".to_string(),
        constraint_type: "UNIQUE".to_string(),
        is_deferrable: false,
        initially_deferred: false,
        definition: Some("UNIQUE (gen_col)".to_string()),
        coninhcount: 0,
        is_enforced: true,
        no_inherit: false,
        nulls_not_distinct: false,
        comment: None,
    };
    // Backing index has the same name as the constraint.
    let uniq_idx = TableIndex {
        schema: "test_deps".to_string(),
        table: "items".to_string(),
        name: "items_gen_col_key".to_string(),
        catalog: Some("postgres".to_string()),
        indexdef: "CREATE UNIQUE INDEX items_gen_col_key ON test_deps.items USING btree (gen_col)"
            .to_string(),
        is_partition_index: false,
        comment: None,
    };

    let mut from_table =
        issue179_items_table("integer", "test_deps.compute(0)::integer", "integer");
    from_table.constraints.push(uniq_constraint.clone());
    from_table.indexes.push(uniq_idx.clone());
    from_table.hash();
    from_dump.tables.push(from_table);

    let mut to_table = issue179_items_table("bigint", "test_deps.compute(0)", "bigint");
    to_table.constraints.push(uniq_constraint);
    to_table.indexes.push(uniq_idx);
    to_table.hash();
    to_dump.tables.push(to_table);

    // pg_depend records BOTH edges.
    from_dump.column_dependents.push(ColumnDependent {
        schema: "test_deps".to_string(),
        table: "items".to_string(),
        column: "gen_col".to_string(),
        kind: ColumnDependentKind::Index,
        dep_schema: "test_deps".to_string(),
        dep_table: "items".to_string(),
        dep_name: "items_gen_col_key".to_string(),
    });
    from_dump.column_dependents.push(ColumnDependent {
        schema: "test_deps".to_string(),
        table: "items".to_string(),
        column: "gen_col".to_string(),
        kind: ColumnDependentKind::Constraint,
        dep_schema: "test_deps".to_string(),
        dep_table: "items".to_string(),
        dep_name: "items_gen_col_key".to_string(),
    });

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_routines_and_views().await.unwrap();
    let script = comparer.get_script();

    // The constraint emission must fire …
    assert!(
        script.contains("alter table test_deps.items add constraint items_gen_col_key"),
        "UNIQUE constraint must be re-emitted: {}",
        script
    );
    // … and the backing-index CREATE must NOT also be emitted (the
    // constraint creates the index implicitly).
    assert!(
        !script.contains("CREATE UNIQUE INDEX IF NOT EXISTS items_gen_col_key"),
        "backing index must be skipped when a same-named constraint emission already recreates it: {}",
        script
    );
}

/// Path B: a STORED → VIRTUAL flip routes the column through the
/// `DROP COLUMN` + `ADD COLUMN` branch in `TableColumn::get_alter_script`
/// (issue #181). PostgreSQL CASCADE-drops a plain index attached to the
/// column. `compare_tables` must walk the column-dependent graph and
/// emit the recreate.
#[tokio::test]
async fn issue188_path_b_virtual_flip_restores_dependent_index() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let value_col = {
        let mut c = int_column("test_deps", "items", "value", 1);
        c.is_nullable = false;
        c
    };

    let mut from_gen_col = int_column("test_deps", "items", "gen_col", 2);
    from_gen_col.data_type = "integer".to_string();
    from_gen_col.is_generated = "ALWAYS".to_string();
    from_gen_col.generation_expression = Some("(value * 2)".to_string());
    from_gen_col.generation_type = Some("s".to_string()); // STORED in FROM

    let mut to_gen_col = from_gen_col.clone();
    to_gen_col.generation_type = Some("v".to_string()); // VIRTUAL in TO

    let plain_idx = TableIndex {
        schema: "test_deps".to_string(),
        table: "items".to_string(),
        name: "idx_gen_col".to_string(),
        catalog: Some("postgres".to_string()),
        indexdef: "CREATE INDEX idx_gen_col ON test_deps.items USING btree (gen_col)".to_string(),
        is_partition_index: false,
        comment: None,
    };

    let mut from_table = Table::new(
        "test_deps".to_string(),
        "items".to_string(),
        "test_deps".to_string(),
        "items".to_string(),
        "postgres".to_string(),
        None,
        vec![value_col.clone(), from_gen_col],
        vec![],
        vec![plain_idx.clone()],
        vec![],
        None,
    );
    from_table.hash();
    from_dump.tables.push(from_table);

    let mut to_table = Table::new(
        "test_deps".to_string(),
        "items".to_string(),
        "test_deps".to_string(),
        "items".to_string(),
        "postgres".to_string(),
        None,
        vec![value_col, to_gen_col],
        vec![],
        vec![plain_idx],
        vec![],
        None,
    );
    to_table.hash();
    to_dump.tables.push(to_table);

    from_dump.column_dependents.push(ColumnDependent {
        schema: "test_deps".to_string(),
        table: "items".to_string(),
        column: "gen_col".to_string(),
        kind: ColumnDependentKind::Index,
        dep_schema: "test_deps".to_string(),
        dep_table: "items".to_string(),
        dep_name: "idx_gen_col".to_string(),
    });

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_tables().await.unwrap();
    let script = comparer.get_script();

    // The drop+add for the column must fire (Path B trigger).
    assert!(
        script.contains("drop column"),
        "STORED→VIRTUAL flip should DROP COLUMN: {}",
        script
    );
    // The recreate block must include the dependent index.
    assert!(
        script.contains("Recreate dependents dropped by virtual-column rewrite"),
        "labeled recreate block must wrap Path B dependents: {}",
        script
    );
    assert!(
        script.contains("CREATE INDEX IF NOT EXISTS idx_gen_col ON test_deps.items"),
        "plain index on virtually-recreated column must be re-emitted: {}",
        script
    );
}

/// Phase 7 / Path A: an FK on a *different* table referencing the
/// generated column on the anchor table. The pg_depend row's
/// `refobjid` points at the parent table (where the column lives) but
/// `con.conrelid` points at the child (where the FK lives) — the
/// `dep_schema`/`dep_table` in `ColumnDependent` must be the child's,
/// not the anchor's. Locks in correct behaviour for the asymmetric
/// `conrelid` vs `refobjid` case (PR #196 review).
#[tokio::test]
async fn issue188_phase7_restores_fk_on_different_table() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    from_dump
        .routines
        .push(issue179_compute_routine("integer", "SELECT x * 2;"));
    to_dump.routines.push(issue179_compute_routine(
        "bigint",
        "SELECT (x * 2)::bigint;",
    ));

    // Parent table: the standard issue179 items table — gen_col is
    // the anchor whose CASCADE drop the test exercises.
    from_dump.tables.push(issue179_items_table(
        "integer",
        "test_deps.compute(0)::integer",
        "integer",
    ));
    to_dump.tables.push(issue179_items_table(
        "bigint",
        "test_deps.compute(0)",
        "bigint",
    ));

    // Child table: separate table whose FK references gen_col on the
    // parent. The FK's own definition contains no function name; the
    // text scanner cannot see this dependency. The pg_depend graph
    // must drive the re-emission.
    let make_child = |ref_type: &str| {
        let mut id_col = int_column("test_deps", "items_child", "id", 1);
        id_col.is_nullable = false;

        let mut ref_col = int_column("test_deps", "items_child", "ref_gen", 2);
        ref_col.data_type = ref_type.to_string();

        let fk = TableConstraint {
            catalog: "postgres".to_string(),
            schema: "test_deps".to_string(),
            name: "fk_items_child_ref_gen".to_string(),
            table_name: "items_child".to_string(),
            constraint_type: "FOREIGN KEY".to_string(),
            is_deferrable: false,
            initially_deferred: false,
            definition: Some(
                "FOREIGN KEY (ref_gen) REFERENCES test_deps.items (gen_col)".to_string(),
            ),
            coninhcount: 0,
            is_enforced: true,
            no_inherit: false,
            nulls_not_distinct: false,
            comment: None,
        };

        let mut t = Table::new(
            "test_deps".to_string(),
            "items_child".to_string(),
            "test_deps".to_string(),
            "items_child".to_string(),
            "postgres".to_string(),
            None,
            vec![id_col, ref_col],
            vec![fk],
            vec![],
            vec![],
            None,
        );
        t.hash();
        t
    };
    from_dump.tables.push(make_child("integer"));
    to_dump.tables.push(make_child("bigint"));

    // Anchor is the parent column (gen_col on items). dep_table is
    // the *child* (items_child) because the FK constraint's
    // `conrelid` points at the child, not the parent where the
    // depended-on column lives.
    from_dump.column_dependents.push(ColumnDependent {
        schema: "test_deps".to_string(),
        table: "items".to_string(),
        column: "gen_col".to_string(),
        kind: ColumnDependentKind::Constraint,
        dep_schema: "test_deps".to_string(),
        dep_table: "items_child".to_string(),
        dep_name: "fk_items_child_ref_gen".to_string(),
    });

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_routines_and_views().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("alter table test_deps.items_child add constraint fk_items_child_ref_gen"),
        "FK on a different table must be re-emitted via pg_depend graph: {}",
        script
    );
    assert!(
        script.contains(
            "alter table test_deps.items_child drop constraint if exists fk_items_child_ref_gen;"
        ),
        "drop-if-exists guard for cross-table FK missing: {}",
        script
    );
}

/// Phase 7 / Path A: when the anchor column has both a UNIQUE
/// constraint and an FK on another table referencing it, the FK must
/// be emitted *after* the UNIQUE constraint — PostgreSQL rejects
/// `ADD CONSTRAINT … FOREIGN KEY` when the referenced columns lack a
/// unique constraint. Two-pass ordering in `recreate_column_dependents`.
#[tokio::test]
async fn issue188_phase7_emits_fk_after_unique_target() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    from_dump
        .routines
        .push(issue179_compute_routine("integer", "SELECT x * 2;"));
    to_dump.routines.push(issue179_compute_routine(
        "bigint",
        "SELECT (x * 2)::bigint;",
    ));

    let uniq_constraint = TableConstraint {
        catalog: "postgres".to_string(),
        schema: "test_deps".to_string(),
        name: "items_gen_col_uniq".to_string(),
        table_name: "items".to_string(),
        constraint_type: "UNIQUE".to_string(),
        is_deferrable: false,
        initially_deferred: false,
        definition: Some("UNIQUE (gen_col)".to_string()),
        coninhcount: 0,
        is_enforced: true,
        no_inherit: false,
        nulls_not_distinct: false,
        comment: None,
    };

    let mut from_items =
        issue179_items_table("integer", "test_deps.compute(0)::integer", "integer");
    from_items.constraints.push(uniq_constraint.clone());
    from_items.hash();
    from_dump.tables.push(from_items);

    let mut to_items = issue179_items_table("bigint", "test_deps.compute(0)", "bigint");
    to_items.constraints.push(uniq_constraint);
    to_items.hash();
    to_dump.tables.push(to_items);

    let make_child = |ref_type: &str| {
        let mut id_col = int_column("test_deps", "items_child", "id", 1);
        id_col.is_nullable = false;
        let mut ref_col = int_column("test_deps", "items_child", "ref_gen", 2);
        ref_col.data_type = ref_type.to_string();

        let fk = TableConstraint {
            catalog: "postgres".to_string(),
            schema: "test_deps".to_string(),
            name: "fk_items_child_ref_gen".to_string(),
            table_name: "items_child".to_string(),
            constraint_type: "FOREIGN KEY".to_string(),
            is_deferrable: false,
            initially_deferred: false,
            definition: Some(
                "FOREIGN KEY (ref_gen) REFERENCES test_deps.items (gen_col)".to_string(),
            ),
            coninhcount: 0,
            is_enforced: true,
            no_inherit: false,
            nulls_not_distinct: false,
            comment: None,
        };

        let mut t = Table::new(
            "test_deps".to_string(),
            "items_child".to_string(),
            "test_deps".to_string(),
            "items_child".to_string(),
            "postgres".to_string(),
            None,
            vec![id_col, ref_col],
            vec![fk],
            vec![],
            vec![],
            None,
        );
        t.hash();
        t
    };
    from_dump.tables.push(make_child("integer"));
    to_dump.tables.push(make_child("bigint"));

    // FK first in the column_dependents vec — the helper must defer
    // it regardless of input order so it lands after the UNIQUE.
    from_dump.column_dependents.push(ColumnDependent {
        schema: "test_deps".to_string(),
        table: "items".to_string(),
        column: "gen_col".to_string(),
        kind: ColumnDependentKind::Constraint,
        dep_schema: "test_deps".to_string(),
        dep_table: "items_child".to_string(),
        dep_name: "fk_items_child_ref_gen".to_string(),
    });
    from_dump.column_dependents.push(ColumnDependent {
        schema: "test_deps".to_string(),
        table: "items".to_string(),
        column: "gen_col".to_string(),
        kind: ColumnDependentKind::Constraint,
        dep_schema: "test_deps".to_string(),
        dep_table: "items".to_string(),
        dep_name: "items_gen_col_uniq".to_string(),
    });

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_routines_and_views().await.unwrap();
    let script = comparer.get_script();

    let uniq_pos = script
        .find("add constraint items_gen_col_uniq")
        .expect("UNIQUE constraint must be re-emitted");
    let fk_pos = script
        .find("add constraint fk_items_child_ref_gen")
        .expect("FK constraint must be re-emitted");
    assert!(
        uniq_pos < fk_pos,
        "FK must be emitted AFTER its UNIQUE target; got uniq@{} fk@{}: {}",
        uniq_pos,
        fk_pos,
        script
    );
}

/// Regression for the inheritance_child idempotency bug — pgc was
/// dumping classical-inheritance children with `partition_of` wrongly
/// set (because `pg_inherits` records both partition and classical
/// inheritance), which made `column_type_change_forces_recreate` fire
/// and trigger a wholesale drop+recreate. After migration the
/// recreated child silently picked up the *current* default
/// privileges, leaving stray REVOKE statements in the next
/// `pgc compare` pass.
///
/// This test pins down the post-fix invariant: a classical-inheritance
/// child (`partition_of = None`, `inherits_from = [parent]`) with a
/// column type change must NOT be flagged for wholesale recreate.
/// The dump-side fix that produces this shape (filtering
/// `pg_inherits` joins by `parent.relkind = 'p'`) lives in
/// `fetch_partition_info_bulk` and cannot be unit-tested without a
/// live PostgreSQL connection, but the comparer-side gate has its own
/// expectations and those are what this test enforces.
#[tokio::test]
async fn inheritance_child_classical_inheritance_does_not_force_recreate() {
    let make_inheritance_child = |child_data_type: &str, max_len: Option<i32>| {
        let mut child_col = int_column("test_deps", "inheritance_child", "child_data", 1);
        child_col.data_type = child_data_type.to_string();
        child_col.character_maximum_length = max_len;

        let mut t = Table::new(
            "test_deps".to_string(),
            "inheritance_child".to_string(),
            "test_deps".to_string(),
            "inheritance_child".to_string(),
            "postgres".to_string(),
            None,
            vec![child_col],
            vec![],
            vec![],
            vec![],
            None,
        );
        // Classical inheritance: parent is a regular table; partition_of
        // stays None, inherits_from carries the parent reference. With
        // the pre-fix dump query, partition_of would have been
        // erroneously set here too — that mis-shape is exactly what
        // this test forbids.
        t.inherits_from = vec!["test_deps.inheritance_parent".to_string()];
        t.hash();
        t
    };

    let from_table = make_inheritance_child("text", None);
    let to_table = make_inheritance_child("character varying", Some(255));

    // The comparer-side predicate must NOT classify this column change
    // as a wholesale recreate. PostgreSQL accepts in-place
    // `ALTER TABLE … ALTER COLUMN child_data TYPE varchar(255)` on a
    // classical-inheritance child, and dropping the child wholesale
    // would leak default-privilege grants onto the recreated table.
    assert!(
        !from_table.will_be_dropped_and_recreated(&to_table),
        "classical-inheritance child with column type change must NOT \
         be flagged for wholesale recreate (partition_of: {:?}, \
         inherits_from: {:?})",
        from_table.partition_of,
        from_table.inherits_from,
    );

    // Sanity counter-test: same column change on a real partition
    // child (partition_of = Some, inherits_from = []) SHOULD force
    // wholesale recreate — PG forbids in-place type changes on
    // partition-key columns and partition-inherited columns.
    let mut from_partition_child = make_inheritance_child("text", None);
    from_partition_child.inherits_from = Vec::new();
    from_partition_child.partition_of = Some("test_deps.parent_partitioned".to_string());
    let mut to_partition_child = make_inheritance_child("character varying", Some(255));
    to_partition_child.inherits_from = Vec::new();
    to_partition_child.partition_of = Some("test_deps.parent_partitioned".to_string());
    assert!(
        from_partition_child.will_be_dropped_and_recreated(&to_partition_child),
        "real partition child with column type change MUST be flagged \
         for wholesale recreate"
    );
}
