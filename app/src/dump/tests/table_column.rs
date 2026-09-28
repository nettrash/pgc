use super::*;
use sha2::{Digest, Sha256};

// Helper function to create a basic TableColumn for testing
fn create_test_column() -> TableColumn {
    TableColumn {
        catalog: "test_catalog".to_string(),
        schema: "public".to_string(),
        table: "test_table".to_string(),
        name: "test_column".to_string(),
        ordinal_position: 1,
        column_default: None,
        is_nullable: true,
        data_type: "varchar".to_string(),
        character_maximum_length: Some(255),
        character_octet_length: Some(1020),
        numeric_precision: None,
        numeric_precision_radix: None,
        numeric_scale: None,
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
        generation_blocks_retype: false,
        generation_routine_dropped: false,
    }
}

#[test]
fn test_table_column_creation() {
    let column = create_test_column();
    assert_eq!(column.catalog, "test_catalog");
    assert_eq!(column.schema, "public");
    assert_eq!(column.table, "test_table");
    assert_eq!(column.name, "test_column");
    assert_eq!(column.ordinal_position, 1);
    assert!(column.is_nullable);
    assert_eq!(column.data_type, "varchar");
}

#[test]
fn test_table_column_clone() {
    let column = create_test_column();
    let cloned = column.clone();
    assert_eq!(column, cloned);
}

#[test]
fn test_table_column_debug_format() {
    let column = create_test_column();
    let debug_str = format!("{column:?}");
    assert!(debug_str.contains("TableColumn"));
    assert!(debug_str.contains("test_column"));
}

#[test]
fn test_add_to_hasher() {
    let column = create_test_column();
    let mut hasher = Sha256::new();
    column.add_to_hasher(&mut hasher);
    let hash = hasher.finalize();
    assert_eq!(hash.len(), 32); // SHA256 produces 32-byte hash
}

#[test]
fn test_add_to_hasher_consistency() {
    let column = create_test_column();

    let mut hasher1 = Sha256::new();
    column.add_to_hasher(&mut hasher1);
    let hash1 = hasher1.finalize();

    let mut hasher2 = Sha256::new();
    column.add_to_hasher(&mut hasher2);
    let hash2 = hasher2.finalize();

    assert_eq!(hash1, hash2);
}

#[test]
fn test_add_to_hasher_different_for_different_content() {
    let column1 = create_test_column();
    let mut column2 = create_test_column();
    column2.name = "different_column".to_string();

    let mut hasher1 = Sha256::new();
    column1.add_to_hasher(&mut hasher1);
    let hash1 = hasher1.finalize();

    let mut hasher2 = Sha256::new();
    column2.add_to_hasher(&mut hasher2);
    let hash2 = hasher2.finalize();

    assert_ne!(hash1, hash2);
}

#[test]
fn test_add_to_hasher_ignores_catalog() {
    let column1 = create_test_column();
    let mut column2 = create_test_column();
    column2.catalog = "different_catalog".to_string();

    let mut hasher1 = Sha256::new();
    column1.add_to_hasher(&mut hasher1);
    let hash1 = hasher1.finalize();

    let mut hasher2 = Sha256::new();
    column2.add_to_hasher(&mut hasher2);
    let hash2 = hasher2.finalize();

    assert_eq!(hash1, hash2);
}

#[test]
fn test_add_to_hasher_includes_all_fields() {
    let mut column = create_test_column();
    column.column_default = Some("'default_value'".to_string());
    column.numeric_precision = Some(10);
    column.numeric_scale = Some(2);
    column.collation_name = Some("en_US.UTF-8".to_string());
    column.is_identity = true;
    column.identity_generation = Some("BY DEFAULT".to_string());
    column.is_generated = "ALWAYS".to_string();
    column.generation_expression = Some("(id * 2)".to_string());

    let mut hasher = Sha256::new();
    column.add_to_hasher(&mut hasher);
    let hash = hasher.finalize();
    assert_eq!(hash.len(), 32);
}

#[test]
fn test_related_views_serde_roundtrip() {
    let mut column = create_test_column();
    column.related_views = Some(vec![
        "public.view_one".to_string(),
        "analytics.view_two".to_string(),
    ]);

    let json = serde_json::to_string(&column).expect("serialize");
    let de: TableColumn = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(de.related_views, column.related_views);
}

#[test]
fn test_get_script_varchar_column() {
    let column = create_test_column();
    let script = column.get_script();
    assert_eq!(script, "test_column varchar(255)");
}

#[test]
fn test_get_script_varchar_column_not_null() {
    let mut column = create_test_column();
    column.is_nullable = false;
    let script = column.get_script();
    assert_eq!(script, "test_column varchar(255) not null");
}

#[test]
fn test_get_script_varchar_column_with_default() {
    let mut column = create_test_column();
    column.column_default = Some("'default_value'".to_string());
    let script = column.get_script();
    assert_eq!(script, "test_column varchar(255) default 'default_value'");
}

#[test]
fn test_get_script_numeric_column() {
    let mut column = create_test_column();
    column.data_type = "numeric".to_string();
    column.character_maximum_length = None;
    column.numeric_precision = Some(10);
    column.numeric_scale = Some(2);
    let script = column.get_script();
    assert_eq!(script, "test_column numeric(10, 2)");
}

#[test]
fn test_get_script_numeric_column_precision_only() {
    let mut column = create_test_column();
    column.data_type = "numeric".to_string();
    column.character_maximum_length = None;
    column.numeric_precision = Some(10);
    column.numeric_scale = None;
    let script = column.get_script();
    assert_eq!(script, "test_column numeric(10)");
}

#[test]
fn test_get_script_with_collation() {
    let mut column = create_test_column();
    column.collation_name = Some("en_US.UTF-8".to_string());
    let script = column.get_script();
    assert_eq!(script, "test_column varchar(255) collate \"en_US.UTF-8\"");
}

#[test]
fn test_get_script_identity_column() {
    let mut column = create_test_column();
    column.data_type = "integer".to_string();
    column.character_maximum_length = None;
    column.is_identity = true;
    column.identity_generation = Some("BY DEFAULT".to_string());
    let script = column.get_script();
    assert_eq!(
        script,
        "test_column integer generated BY DEFAULT as identity"
    );
}

#[test]
fn test_get_script_identity_column_generated_always() {
    let mut column = create_test_column();
    column.data_type = "integer".to_string();
    column.character_maximum_length = None;
    column.is_identity = true;
    column.identity_generation = Some("ALWAYS".to_string());
    let script = column.get_script();
    assert_eq!(script, "test_column integer generated ALWAYS as identity");
}

#[test]
fn test_get_script_identity_column_with_options() {
    let mut column = create_test_column();
    column.data_type = "integer".to_string();
    column.character_maximum_length = None;
    column.is_identity = true;
    column.identity_generation = Some("ALWAYS".to_string());
    column.identity_start = Some("1".to_string());
    column.identity_increment = Some("1".to_string());
    column.identity_minimum = Some("1".to_string());
    column.identity_maximum = Some("1000".to_string());
    column.identity_cycle = true;
    let script = column.get_script();
    assert_eq!(
        script,
        "test_column integer generated ALWAYS as identity (start with 1 increment by 1 minvalue 1 maxvalue 1000 cycle)"
    );
}

#[test]
fn test_get_script_generated_column() {
    let mut column = create_test_column();
    column.data_type = "integer".to_string();
    column.character_maximum_length = None;
    column.is_generated = "ALWAYS".to_string();
    column.generation_expression = Some("(id * 2)".to_string());
    let script = column.get_script();
    assert_eq!(
        script,
        "test_column integer generated always as (id * 2) stored"
    );
}

#[test]
fn test_get_script_interval_column() {
    let mut column = create_test_column();
    column.data_type = "interval".to_string();
    column.character_maximum_length = None;
    column.interval_type = Some("DAY TO SECOND".to_string());
    let script = column.get_script();
    assert_eq!(script, "test_column interval DAY TO SECOND");
}

#[test]
fn test_get_script_decimal_column() {
    let mut column = create_test_column();
    column.data_type = "decimal".to_string();
    column.character_maximum_length = None;
    column.numeric_precision = Some(15);
    column.numeric_scale = Some(4);
    let script = column.get_script();
    assert_eq!(script, "test_column decimal(15, 4)");
}

#[test]
fn test_get_script_complex_column() {
    let mut column = create_test_column();
    column.data_type = "varchar".to_string();
    column.character_maximum_length = Some(100);
    column.is_nullable = false;
    column.column_default = Some("'test'".to_string());
    column.collation_name = Some("C".to_string());
    let script = column.get_script();
    assert_eq!(
        script,
        "test_column varchar(100) collate \"C\" default 'test' not null"
    );
}

#[test]
fn test_get_script_with_empty_collation() {
    let mut column = create_test_column();
    column.collation_name = Some("".to_string());
    let script = column.get_script();
    assert_eq!(script, "test_column varchar(255)");
}

#[test]
fn test_partial_eq_identical_columns() {
    let column1 = create_test_column();
    let column2 = create_test_column();
    assert_eq!(column1, column2);
}

#[test]
fn test_partial_eq_ignores_catalog() {
    let column1 = create_test_column();
    let mut column2 = create_test_column();
    column2.catalog = "different_catalog".to_string();
    assert_eq!(column1, column2);
}

#[test]
fn test_partial_eq_different_schema() {
    let column1 = create_test_column();
    let mut column2 = create_test_column();
    column2.schema = "different_schema".to_string();
    assert_ne!(column1, column2);
}

#[test]
fn test_partial_eq_different_table() {
    let column1 = create_test_column();
    let mut column2 = create_test_column();
    column2.table = "different_table".to_string();
    assert_ne!(column1, column2);
}

#[test]
fn test_partial_eq_different_name() {
    let column1 = create_test_column();
    let mut column2 = create_test_column();
    column2.name = "different_column".to_string();
    assert_ne!(column1, column2);
}

#[test]
fn test_partial_eq_different_ordinal_position() {
    let column1 = create_test_column();
    let mut column2 = create_test_column();
    column2.ordinal_position = 2;
    assert_ne!(column1, column2);
}

#[test]
fn test_partial_eq_different_column_default() {
    let column1 = create_test_column();
    let mut column2 = create_test_column();
    column2.column_default = Some("'different'".to_string());
    assert_ne!(column1, column2);
}

#[test]
fn test_partial_eq_different_is_nullable() {
    let column1 = create_test_column();
    let mut column2 = create_test_column();
    column2.is_nullable = false;
    assert_ne!(column1, column2);
}

#[test]
fn test_partial_eq_different_data_type() {
    let column1 = create_test_column();
    let mut column2 = create_test_column();
    column2.data_type = "text".to_string();
    assert_ne!(column1, column2);
}

#[test]
fn test_partial_eq_different_character_maximum_length() {
    let column1 = create_test_column();
    let mut column2 = create_test_column();
    column2.character_maximum_length = Some(500);
    assert_ne!(column1, column2);
}

#[test]
fn test_partial_eq_different_numeric_precision() {
    let column1 = create_test_column();
    let mut column2 = create_test_column();
    column2.numeric_precision = Some(10);
    assert_ne!(column1, column2);
}

#[test]
fn test_partial_eq_different_is_identity() {
    let column1 = create_test_column();
    let mut column2 = create_test_column();
    column2.is_identity = true;
    assert_ne!(column1, column2);
}

#[test]
fn test_partial_eq_is_generated_special_logic() {
    let mut column1 = create_test_column();
    let mut column2 = create_test_column();

    // Test case where both contain "ALWAYS"
    column1.is_generated = "ALWAYS".to_string();
    column2.is_generated = "GENERATED ALWAYS".to_string();
    assert_eq!(column1, column2);

    // Test case where both contain "BY DEFAULT"
    column1.is_generated = "BY DEFAULT".to_string();
    column2.is_generated = "GENERATED BY DEFAULT".to_string();
    assert_eq!(column1, column2);
}

#[test]
fn test_partial_eq_generated_differs_from_plain_column() {
    let mut column1 = create_test_column();
    column1.is_generated = "ALWAYS".to_string();
    column1.generation_expression = Some("(id * 2)".to_string());

    let column2 = create_test_column();

    assert_ne!(column1, column2);
}

#[test]
fn test_partial_eq_different_generation_expression() {
    let mut column1 = create_test_column();
    let mut column2 = create_test_column();
    column1.is_generated = "ALWAYS".to_string();
    column1.generation_expression = Some("(id * 2)".to_string());
    column2.is_generated = "ALWAYS".to_string();
    column2.generation_expression = Some("(id * 3)".to_string());
    assert_ne!(column1, column2);
}

#[test]
fn test_partial_eq_different_is_updatable() {
    let column1 = create_test_column();
    let mut column2 = create_test_column();
    column2.is_updatable = false;
    assert_ne!(column1, column2);
}

#[test]
fn test_serde_serialization() {
    let column = create_test_column();
    let serialized = serde_json::to_string(&column).expect("Failed to serialize");
    let deserialized: TableColumn =
        serde_json::from_str(&serialized).expect("Failed to deserialize");
    assert_eq!(column, deserialized);
}

#[test]
fn test_edge_cases_empty_strings() {
    let mut column = create_test_column();
    column.catalog = "".to_string();
    column.schema = "".to_string();
    column.table = "".to_string();
    column.name = "".to_string();
    column.data_type = "".to_string();

    let script = column.get_script();
    assert_eq!(script, "");

    let mut hasher = Sha256::new();
    column.add_to_hasher(&mut hasher);
    let hash = hasher.finalize();
    assert_eq!(hash.len(), 32);
}

#[test]
fn test_column_with_all_optional_fields() {
    let mut column = create_test_column();
    column.character_maximum_length = Some(1000);
    column.character_octet_length = Some(4000);
    column.numeric_precision = Some(15);
    column.numeric_precision_radix = Some(10);
    column.numeric_scale = Some(5);
    column.datetime_precision = Some(6);
    column.interval_type = Some("YEAR TO MONTH".to_string());
    column.interval_precision = Some(2);
    column.character_set_catalog = Some("catalog".to_string());
    column.character_set_schema = Some("schema".to_string());
    column.character_set_name = Some("UTF8".to_string());
    column.collation_catalog = Some("coll_catalog".to_string());
    column.collation_schema = Some("coll_schema".to_string());
    column.collation_name = Some("en_US".to_string());
    column.domain_catalog = Some("domain_cat".to_string());
    column.domain_schema = Some("domain_sch".to_string());
    column.domain_name = Some("domain_name".to_string());
    column.udt_catalog = Some("udt_cat".to_string());
    column.udt_schema = Some("udt_sch".to_string());
    column.udt_name = Some("udt_name".to_string());
    column.scope_catalog = Some("scope_cat".to_string());
    column.scope_schema = Some("scope_sch".to_string());
    column.scope_name = Some("scope_name".to_string());
    column.maximum_cardinality = Some(100);
    column.dtd_identifier = Some("dtd_id".to_string());
    column.is_self_referencing = true;

    let mut hasher = Sha256::new();
    column.add_to_hasher(&mut hasher);
    let hash = hasher.finalize();
    assert_eq!(hash.len(), 32);
}

#[test]
fn test_column_with_special_characters() {
    let mut column = create_test_column();
    column.name = "test-column_with$special@chars".to_string();
    column.column_default = Some("'value with spaces and ''quotes'''".to_string());
    column.collation_name = Some("collation-with-dashes".to_string());

    let script = column.get_script();
    assert!(script.contains("test-column_with$special@chars"));
    assert!(script.contains("collation-with-dashes"));
}

#[test]
fn test_known_sha256_hash() {
    let column = create_test_column();
    let mut hasher = Sha256::new();
    column.add_to_hasher(&mut hasher);
    let hash = hasher.finalize();
    let hash_hex = format!("{hash:x}");

    // This is a known hash for the test data - if the hashing logic changes, this will fail
    assert_eq!(
        hash_hex,
        "e71094fafb6a1f2d03c80ba04c8fea5dac0269f681cbe1c0e4afb1a8482b0db2"
    );
}

#[test]
fn test_text_data_type_without_length() {
    let mut column = create_test_column();
    column.data_type = "text".to_string();
    column.character_maximum_length = None;
    let script = column.get_script();
    assert_eq!(script, "test_column text");
}

#[test]
fn test_integer_data_type() {
    let mut column = create_test_column();
    column.data_type = "integer".to_string();
    column.character_maximum_length = None;
    let script = column.get_script();
    assert_eq!(script, "test_column integer");
}

// --- Script methods: alter/add/drop ---
#[test]
fn test_get_alter_script_type_change() {
    let existing = create_test_column();
    let mut updated = existing.clone();
    updated.data_type = "integer".to_string();
    updated.character_maximum_length = None;
    let script = updated
        .get_alter_script(&existing, true)
        .expect("expected alter statement for type change");
    assert_eq!(
        script,
        "alter table public.test_table alter column test_column type integer;\n\n"
    );
}

#[test]
fn test_get_alter_script_type_change_different_schema() {
    let existing = create_test_column();
    let mut updated = existing.clone();
    updated.schema = "app".to_string();
    updated.table = "users".to_string();
    updated.data_type = "integer".to_string();
    updated.character_maximum_length = None;
    let script = updated
        .get_alter_script(&existing, true)
        .expect("expected alter statement for type change");
    assert_eq!(
        script,
        "alter table app.users alter column test_column type integer;\n\n"
    );
}

#[test]
fn test_get_alter_script_default_change() {
    let mut existing = create_test_column();
    existing.column_default = None;
    let mut updated = existing.clone();
    updated.column_default = Some("'default_value'".to_string());

    let script = updated
        .get_alter_script(&existing, true)
        .expect("expected alter statement for default change");
    assert_eq!(
        script,
        "alter table public.test_table alter column test_column set default 'default_value';\n\n"
    );
}

#[test]
fn test_get_alter_script_nullability_change() {
    let mut existing = create_test_column();
    existing.is_nullable = true;
    let mut updated = existing.clone();
    updated.is_nullable = false;

    let script = updated
        .get_alter_script(&existing, true)
        .expect("expected alter statement for nullability change");
    assert_eq!(
        script,
        "alter table public.test_table alter column test_column set not null;\n\n"
    );
}

#[test]
fn test_get_alter_script_drop_not_null_use_drop_false() {
    let mut existing = create_test_column();
    existing.is_nullable = false;
    let mut updated = existing.clone();
    updated.is_nullable = true;

    let script = updated
        .get_alter_script(&existing, false)
        .expect("expected commented drop not null when use_drop is false");

    assert!(script.contains("use_drop=false"));
    assert!(script.contains("alter column test_column drop not null"));
    assert!(script.lines().all(|l| l.starts_with("--")));
}

#[test]
fn test_get_alter_script_returns_none_when_no_change() {
    let column = create_test_column();
    assert!(column.get_alter_script(&column, true).is_none());
}

#[test]
fn test_get_add_script_basic() {
    let column = create_test_column();
    let expected = "alter table public.test_table add column test_column varchar(255);\n\n";
    assert_eq!(column.get_add_script(), expected);
}

#[test]
fn test_get_add_script_with_constraints() {
    let mut column = create_test_column();
    column.is_nullable = false;
    column.column_default = Some("'default_value'".to_string());
    column.collation_name = Some("en_US.UTF-8".to_string());

    let expected = "alter table public.test_table add column test_column varchar(255) collate \"en_US.UTF-8\" default 'default_value' not null;\n\n";
    assert_eq!(column.get_add_script(), expected);
}

#[test]
fn test_get_add_script_identity() {
    let mut column = create_test_column();
    column.data_type = "integer".to_string();
    column.character_maximum_length = None;
    column.is_identity = true;
    column.identity_generation = Some("BY DEFAULT".to_string());
    // Remainder should include identity clause after type
    let expected = "alter table public.test_table add column test_column integer generated BY DEFAULT as identity;\n\n";
    assert_eq!(column.get_add_script(), expected);
}

#[test]
fn test_get_drop_script_basic() {
    let column = create_test_column();
    let expected = "alter table public.test_table drop column test_column;\n\n";
    assert_eq!(column.get_drop_script(), expected);
}

#[test]
fn test_get_drop_script_with_special_name() {
    let mut column = create_test_column();
    column.name = "\"weird name$\"".to_string();
    let expected = "alter table public.test_table drop column \"weird name$\";\n\n";
    assert_eq!(column.get_drop_script(), expected);
}

#[test]
fn test_get_alter_script_identity_update() {
    let mut existing = create_test_column();
    existing.is_identity = true;
    existing.identity_generation = Some("BY DEFAULT".to_string());
    existing.identity_start = Some("1".to_string());
    existing.identity_increment = Some("1".to_string());

    let mut updated = existing.clone();
    updated.identity_start = Some("100".to_string());
    updated.identity_increment = Some("5".to_string());

    let script = updated
        .get_alter_script(&existing, true)
        .expect("expected alter statement for identity update");

    assert_eq!(
        script,
        "alter table public.test_table alter column test_column set START WITH 100;\n\nalter table public.test_table alter column test_column set INCREMENT BY 5;\n\n"
    );
}

#[test]
fn test_get_alter_script_add_generated_always() {
    let existing = create_test_column();
    let mut updated = existing.clone();
    updated.is_generated = "ALWAYS".to_string();
    updated.generation_expression = Some("(id * 2)".to_string());

    let script = updated
        .get_alter_script(&existing, true)
        .expect("expected alter statement for generated column");

    assert_eq!(
        script,
        "alter table public.test_table drop column test_column;\n\n".to_owned()
            + "alter table public.test_table add column test_column varchar(255) generated always as (id * 2) stored;\n\n"
    );
}

#[test]
fn test_get_alter_script_add_generated_always_use_drop_false() {
    let existing = create_test_column();
    let mut updated = existing.clone();
    updated.is_generated = "ALWAYS".to_string();
    updated.generation_expression = Some("(id * 2)".to_string());

    let script = updated
        .get_alter_script(&existing, false)
        .expect("expected statement, even if commented when use_drop is false");

    assert!(script.contains("use_drop=false"));
    assert!(script.contains("drop column test_column"));
    assert!(
        script.contains("add column test_column varchar(255) generated always as (id * 2) stored")
    );
    assert!(script.lines().all(|l| l.starts_with("--") || l.is_empty()));
}

#[test]
fn test_get_alter_script_update_generated_expression() {
    // A STORED expression change on a server that cannot SET EXPRESSION
    // (PostgreSQL 14-16, or a dump of unknown version) is a drop + add.
    // pgc used to emit `drop expression` + `add generated always as (...)
    // stored`, and no PostgreSQL version accepts the second statement.
    let mut existing = create_test_column();
    existing.is_generated = "ALWAYS".to_string();
    existing.generation_expression = Some("(id * 2)".to_string());

    let mut updated = existing.clone();
    updated.generation_expression = Some("(id * 3)".to_string());

    let script = updated
        .get_alter_script(&existing, true)
        .expect("expected alter statement for generated expression change");

    assert_eq!(
        script,
        "alter table public.test_table drop column test_column;\n\n".to_owned()
            + "alter table public.test_table add column test_column varchar(255) generated always as (id * 3) stored;\n\n"
    );
    assert!(updated.would_drop_and_re_add(&existing));
}

#[test]
fn test_get_alter_script_drop_generated_column() {
    let mut existing = create_test_column();
    existing.is_generated = "ALWAYS".to_string();
    existing.generation_expression = Some("(id * 2)".to_string());

    let updated = create_test_column();

    let script = updated
        .get_alter_script(&existing, true)
        .expect("expected alter statement for dropping generated expression");

    assert_eq!(
        script,
        "alter table public.test_table alter column test_column drop expression;\n\n"
    );
}

#[test]
fn test_get_alter_script_drop_default_use_drop_false() {
    let mut existing = create_test_column();
    existing.column_default = Some("'old_default'".to_string());
    let mut updated = existing.clone();
    updated.column_default = None;

    let script = updated
        .get_alter_script(&existing, false)
        .expect("expected commented drop default when use_drop is false");

    assert!(script.contains("drop default"));
    assert!(script.lines().all(|l| l.starts_with("--")));
}

#[test]
fn test_get_alter_script_drop_default_use_drop_true() {
    let mut existing = create_test_column();
    existing.column_default = Some("'old_default'".to_string());
    let mut updated = existing.clone();
    updated.column_default = None;

    let script = updated
        .get_alter_script(&existing, true)
        .expect("expected drop default when use_drop is true");

    assert_eq!(
        script,
        "alter table public.test_table alter column test_column drop default;\n\n"
    );
}

#[test]
fn test_get_alter_script_drop_identity_use_drop_false() {
    let mut existing = create_test_column();
    existing.is_identity = true;
    existing.identity_generation = Some("BY DEFAULT".to_string());
    existing.identity_start = Some("1".to_string());
    existing.identity_increment = Some("1".to_string());

    let mut updated = existing.clone();
    updated.is_identity = false;
    updated.identity_generation = None;
    updated.identity_start = None;
    updated.identity_increment = None;

    let script = updated
        .get_alter_script(&existing, false)
        .expect("expected commented drop identity when use_drop is false");

    assert!(script.contains("drop identity if exists"));
    assert!(script.lines().all(|l| l.starts_with("--")));
}

#[test]
fn test_get_alter_script_drop_identity_use_drop_true() {
    let mut existing = create_test_column();
    existing.is_identity = true;
    existing.identity_generation = Some("BY DEFAULT".to_string());
    existing.identity_start = Some("1".to_string());
    existing.identity_increment = Some("1".to_string());

    let mut updated = existing.clone();
    updated.is_identity = false;
    updated.identity_generation = None;
    updated.identity_start = None;
    updated.identity_increment = None;

    let script = updated
        .get_alter_script(&existing, true)
        .expect("expected drop identity when use_drop is true");

    assert_eq!(
        script,
        "alter table public.test_table alter column test_column drop identity if exists;\n\n"
    );
}

#[test]
fn test_get_alter_script_drop_expression_use_drop_false() {
    let mut existing = create_test_column();
    existing.is_generated = "ALWAYS".to_string();
    existing.generation_expression = Some("(id * 2)".to_string());

    let updated = create_test_column();

    let script = updated
        .get_alter_script(&existing, false)
        .expect("expected commented drop expression when use_drop is false");

    assert!(script.contains("drop expression"));
    assert!(script.lines().all(|l| l.starts_with("--")));
}

#[test]
fn test_get_alter_script_update_generated_expression_use_drop_false() {
    let mut existing = create_test_column();
    existing.is_generated = "ALWAYS".to_string();
    existing.generation_expression = Some("(id * 2)".to_string());

    let mut updated = existing.clone();
    updated.generation_expression = Some("(id * 3)".to_string());

    let script = updated
        .get_alter_script(&existing, false)
        .expect("expected output when use_drop is false for expression update");

    assert_eq!(
        script,
        "-- use_drop=false: changing a stored generated column's expression needs drop/add on this server (SET EXPRESSION requires PostgreSQL 17+); statements commented out\n\
         -- alter table public.test_table drop column test_column;\n\
         -- \n\
         -- alter table public.test_table add column test_column varchar(255) generated always as (id * 3) stored;\n\
         -- \n"
    );
    assert!(
        !script.contains("add generated always"),
        "the invalid ADD GENERATED form must not be emitted: {script}"
    );
}

// ---- PG18: generation_type (virtual generated columns) tests ----

#[test]
fn test_get_script_virtual_generated_column() {
    let mut column = create_test_column();
    column.data_type = "integer".to_string();
    column.character_maximum_length = None;
    column.is_generated = "ALWAYS".to_string();
    column.generation_expression = Some("(id * 2)".to_string());
    column.generation_type = Some("v".to_string());
    let script = column.get_script();
    assert_eq!(
        script,
        "test_column integer generated always as (id * 2) virtual"
    );
}

#[test]
fn test_get_script_stored_generated_column_explicit() {
    let mut column = create_test_column();
    column.data_type = "integer".to_string();
    column.character_maximum_length = None;
    column.is_generated = "ALWAYS".to_string();
    column.generation_expression = Some("(id * 2)".to_string());
    column.generation_type = Some("s".to_string());
    let script = column.get_script();
    assert_eq!(
        script,
        "test_column integer generated always as (id * 2) stored"
    );
}

#[test]
fn test_get_add_script_virtual_generated_column() {
    let mut column = create_test_column();
    column.data_type = "integer".to_string();
    column.character_maximum_length = None;
    column.is_generated = "ALWAYS".to_string();
    column.generation_expression = Some("(price * qty)".to_string());
    column.generation_type = Some("v".to_string());
    let script = column.get_add_script();
    assert!(
        script.contains("generated always as (price * qty) virtual"),
        "expected virtual in add script: {script}"
    );
}

#[test]
fn test_get_alter_script_add_virtual_generated_column() {
    let existing = create_test_column();
    let mut updated = existing.clone();
    updated.is_generated = "ALWAYS".to_string();
    updated.generation_expression = Some("(id * 2)".to_string());
    updated.generation_type = Some("v".to_string());

    let script = updated
        .get_alter_script(&existing, true)
        .expect("expected alter statement for virtual generated column");

    assert!(
        script.contains("generated always as (id * 2) virtual"),
        "expected virtual in alter add script: {script}"
    );
}

#[test]
fn test_get_alter_script_update_virtual_generated_expression() {
    // Issue #181: PG18 rejects `ALTER COLUMN DROP EXPRESSION` for
    // VIRTUAL generated columns and has no `ALTER COLUMN ADD
    // GENERATED ALWAYS AS (...) VIRTUAL` syntax. The only valid
    // migration is a full `DROP COLUMN` + `ADD COLUMN ...
    // GENERATED ALWAYS AS (...) VIRTUAL`. Pin that here — the
    // previous test asserted the broken in-place ALTER form,
    // which produced runtime errors against PG18.
    let mut existing = create_test_column();
    existing.is_generated = "ALWAYS".to_string();
    existing.generation_expression = Some("(id * 2)".to_string());
    existing.generation_type = Some("v".to_string());

    let mut updated = existing.clone();
    updated.generation_expression = Some("(id * 3)".to_string());

    let script = updated
        .get_alter_script(&existing, true)
        .expect("expected alter statement for virtual expression change");

    assert!(
        script.contains("drop column test_column"),
        "expected DROP COLUMN (virtual columns can't be ALTERed in place): {script}"
    );
    assert!(
        !script.contains("drop expression"),
        "must NOT emit `drop expression` for virtual columns (PG18 rejects it): {script}"
    );
    assert!(
        script.contains("add column test_column"),
        "expected ADD COLUMN to re-create the column: {script}"
    );
    assert!(
        script.contains("generated always as (id * 3) virtual"),
        "expected new virtual generation expression in the re-add: {script}"
    );
}

#[test]
fn issue181_stored_to_virtual_flip_emits_drop_and_add_column() {
    // Issue #181 Bug 1: a STORED → VIRTUAL flip with the same
    // generation expression must produce a migration. Before the
    // fix the change-detection condition only inspected
    // `is_generated` and `generation_expression`, so the flip was
    // silently ignored. PG18 has no in-place ALTER for the
    // storage kind so the only valid migration is DROP COLUMN +
    // ADD COLUMN.
    let mut existing = create_test_column();
    existing.is_generated = "ALWAYS".to_string();
    existing.generation_expression = Some("(id * 2)".to_string());
    existing.generation_type = Some("s".to_string());

    let mut updated = existing.clone();
    updated.generation_type = Some("v".to_string());

    let script = updated
        .get_alter_script(&existing, true)
        .expect("STORED → VIRTUAL flip must emit a migration script");

    assert!(
        script.contains("drop column test_column"),
        "expected DROP COLUMN: {script}"
    );
    assert!(
        script.contains("add column test_column"),
        "expected ADD COLUMN: {script}"
    );
    assert!(
        script.contains("generated always as (id * 2) virtual"),
        "expected the new VIRTUAL kind in the re-add: {script}"
    );
}

#[test]
fn issue181_virtual_to_stored_flip_emits_drop_and_add_column() {
    // Mirror of the case above, the other direction. Same
    // requirement: PG18 has no in-place ALTER between VIRTUAL and
    // STORED, so a full DROP+ADD is the only valid migration.
    let mut existing = create_test_column();
    existing.is_generated = "ALWAYS".to_string();
    existing.generation_expression = Some("(id * 2)".to_string());
    existing.generation_type = Some("v".to_string());

    let mut updated = existing.clone();
    updated.generation_type = Some("s".to_string());

    let script = updated
        .get_alter_script(&existing, true)
        .expect("VIRTUAL → STORED flip must emit a migration script");

    assert!(
        script.contains("drop column test_column"),
        "expected DROP COLUMN: {script}"
    );
    assert!(
        script.contains("add column test_column"),
        "expected ADD COLUMN: {script}"
    );
    assert!(
        script.contains("generated always as (id * 2) stored"),
        "expected the new STORED kind in the re-add: {script}"
    );
    assert!(
        !script.contains("drop expression"),
        "must NOT emit `drop expression` (the VIRTUAL side rejects it): {script}"
    );
}

#[test]
fn issue181_stored_expression_change_uses_set_expression_or_drop_add() {
    // STORED -> STORED with a new expression: SET EXPRESSION where the
    // migrated server has it (PostgreSQL 17+), otherwise a drop + add. Never
    // the old `drop expression` + `add generated ... stored` pair, whose
    // second statement is a syntax error on every version.
    let mut existing = create_test_column();
    existing.is_generated = "ALWAYS".to_string();
    existing.generation_expression = Some("(id * 2)".to_string());
    existing.generation_type = Some("s".to_string());

    let mut updated = existing.clone();
    updated.generation_expression = Some("(id * 3)".to_string());

    let script = updated
        .get_alter_script_with_caps(&existing, true, PG17_CAPS)
        .expect("STORED expression change must emit an alter");
    assert_eq!(
        script,
        "alter table public.test_table alter column test_column set expression as (id * 3);\n\n"
    );

    let fallback = updated
        .get_alter_script(&existing, true)
        .expect("STORED expression change must emit an alter");
    assert!(fallback.contains("drop column test_column"), "{fallback}");
    assert!(
        fallback.contains("generated always as (id * 3) stored"),
        "{fallback}"
    );
    for script in [&script, &fallback] {
        assert!(!script.contains("drop expression"), "{script}");
        assert!(!script.contains("add generated"), "{script}");
    }
}

#[test]
fn test_partial_eq_different_generation_type() {
    let mut a = create_test_column();
    a.is_generated = "ALWAYS".to_string();
    a.generation_expression = Some("(id * 2)".to_string());
    a.generation_type = Some("s".to_string());

    let mut b = a.clone();
    b.generation_type = Some("v".to_string());

    assert_ne!(
        a, b,
        "columns with different generation_type should not be equal"
    );
}

#[test]
fn test_partial_eq_generation_type_none_vs_stored() {
    let mut a = create_test_column();
    a.is_generated = "ALWAYS".to_string();
    a.generation_expression = Some("(id * 2)".to_string());
    a.generation_type = None;

    let mut b = a.clone();
    b.generation_type = Some("s".to_string());

    // None is treated as stored ("s"), so they are semantically equal
    assert_eq!(a, b);
}

#[test]
fn test_add_to_hasher_generation_type_affects_hash() {
    let mut a = create_test_column();
    a.is_generated = "ALWAYS".to_string();
    a.generation_expression = Some("(id * 2)".to_string());
    a.generation_type = None;

    let mut b = a.clone();
    b.generation_type = Some("v".to_string());

    let mut ha = Sha256::new();
    let mut hb = Sha256::new();
    a.add_to_hasher(&mut ha);
    b.add_to_hasher(&mut hb);
    assert_ne!(
        format!("{:x}", ha.finalize()),
        format!("{:x}", hb.finalize()),
        "generation_type should affect the hash"
    );
}

#[test]
fn test_serde_default_generation_type() {
    let json = r#"{"catalog":"c","schema":"s","table":"t","name":"n","ordinal_position":1,"is_nullable":true,"data_type":"int","is_self_referencing":false,"is_identity":false,"identity_cycle":false,"is_generated":"NEVER","is_updatable":true}"#;
    let c: TableColumn = serde_json::from_str(json).unwrap();
    assert_eq!(
        c.generation_type, None,
        "missing generation_type should default to None"
    );
}

// ── issue #243: identity ordering within one column's ALTERs ───────────
// An identity column is NOT NULL, has no default, and has an integer
// type — and PostgreSQL enforces all three at the moment of each ALTER,
// not at the end of the transaction. So the identity has to come off
// before any of the three is relaxed, and go on only once all three
// already hold. The statements for one column are emitted in a single
// list, so that ordering is this function's responsibility.

/// A bigint identity column, the FROM side of every case below.
fn identity_column() -> TableColumn {
    let mut column = create_test_column();
    column.name = "id".to_string();
    column.data_type = "bigint".to_string();
    column.character_maximum_length = None;
    column.character_octet_length = None;
    column.is_nullable = false;
    column.is_identity = true;
    column.identity_generation = Some("BY DEFAULT".to_string());
    column
}

/// Byte offset of `needle` in `script`, or a failure naming what is missing.
fn position_of(script: &str, needle: &str) -> usize {
    script
        .find(needle)
        .unwrap_or_else(|| panic!("`{needle}` missing from:\n{script}"))
}

#[test]
fn identity_is_dropped_before_not_null() {
    // The reported case: `drop not null` first fails with
    // `column "id" of relation "test_table" is an identity column`.
    let from = identity_column();
    let mut to = identity_column();
    to.is_identity = false;
    to.identity_generation = None;
    to.is_nullable = true;

    let script = to.get_alter_script(&from, true).expect("no alter emitted");

    assert!(
        position_of(&script, "drop identity if exists") < position_of(&script, "drop not null"),
        "identity must come off before the column may become nullable:\n{script}"
    );
}

#[test]
fn identity_is_dropped_before_a_default_is_set() {
    // Same failure, other statement: PostgreSQL refuses `set default` on a
    // column that is still an identity column.
    let from = identity_column();
    let mut to = identity_column();
    to.is_identity = false;
    to.identity_generation = None;
    to.column_default = Some("42".to_string());

    let script = to.get_alter_script(&from, true).expect("no alter emitted");

    assert!(
        position_of(&script, "drop identity if exists") < position_of(&script, "set default 42"),
        "identity must come off before the column may take a default:\n{script}"
    );
}

#[test]
fn identity_is_dropped_before_the_type_changes() {
    // `identity column type must be smallint, integer, or bigint` — so a
    // move to a non-integer type has to wait for the identity to go.
    let from = identity_column();
    let mut to = identity_column();
    to.is_identity = false;
    to.identity_generation = None;
    to.data_type = "text".to_string();

    let script = to.get_alter_script(&from, true).expect("no alter emitted");

    assert!(
        position_of(&script, "drop identity if exists") < position_of(&script, "type text"),
        "identity must come off before the column may leave integer types:\n{script}"
    );
}

#[test]
fn identity_is_added_after_not_null_and_after_the_default_is_dropped() {
    // The mirror image, which the historical order already got right and
    // which the fix must not disturb: PostgreSQL rejects `add generated ...
    // as identity` on a nullable column, and on one that still has a
    // default.
    let mut from = identity_column();
    from.is_identity = false;
    from.identity_generation = None;
    from.is_nullable = true;
    from.column_default = Some("7".to_string());
    let to = identity_column();

    let script = to.get_alter_script(&from, true).expect("no alter emitted");

    let add = position_of(&script, "add generated");
    assert!(
        position_of(&script, "set not null") < add,
        "the column must be NOT NULL before identity is added:\n{script}"
    );
    assert!(
        position_of(&script, "drop default") < add,
        "the default must be gone before identity is added:\n{script}"
    );
}

#[test]
fn a_kept_identity_still_emits_its_parameter_changes() {
    // The third branch: identity on both sides, only its parameters moved.
    // Nothing is dropped, so nothing needs reordering — but hoisting the
    // drop must not have cost this path its statements.
    let from = identity_column();
    let mut to = identity_column();
    to.identity_generation = Some("ALWAYS".to_string());

    let script = to.get_alter_script(&from, true).expect("no alter emitted");

    assert!(script.contains("set generated ALWAYS"), "{script}");
    assert!(!script.contains("drop identity"), "{script}");
}

#[test]
fn dropping_identity_honours_use_drop_false() {
    // With `use_drop = false` the statement is still emitted, commented out,
    // and still in the leading position — moving it must not have changed
    // which side of the comment marker it lands on.
    let from = identity_column();
    let mut to = identity_column();
    to.is_identity = false;
    to.identity_generation = None;
    to.is_nullable = true;

    let script = to.get_alter_script(&from, false).expect("no alter emitted");

    assert!(
        script
            .contains("-- alter table public.test_table alter column id drop identity if exists;"),
        "{script}"
    );
    assert!(
        !script.contains("\nalter table public.test_table alter column id drop identity"),
        "the drop must stay commented out:\n{script}"
    );
}

// ---- PG17/PG19: ALTER COLUMN ... SET EXPRESSION ----

/// PostgreSQL 17/18 without the VIRTUAL arm: STORED only.
const PG17_CAPS: GeneratedAlterCaps = GeneratedAlterCaps {
    stored_set_expression: true,
    virtual_set_expression: false,
};

/// PostgreSQL 19 (or 18 with no CHECK constraints), no publication block.
const PG19_CAPS: GeneratedAlterCaps = GeneratedAlterCaps {
    stored_set_expression: true,
    virtual_set_expression: true,
};

fn generated_column(kind: &str, expr: &str) -> TableColumn {
    let mut column = create_test_column();
    column.data_type = "integer".to_string();
    column.character_maximum_length = None;
    column.character_octet_length = None;
    column.is_generated = "ALWAYS".to_string();
    column.generation_expression = Some(expr.to_string());
    column.generation_type = Some(kind.to_string());
    column
}

#[test]
fn set_expression_caps_default_is_conservative() {
    let caps = GeneratedAlterCaps::default();
    assert!(!caps.stored_set_expression && !caps.virtual_set_expression);
}

#[test]
fn set_expression_virtual_with_caps() {
    let existing = generated_column("v", "(id * 2)");
    let updated = generated_column("v", "(id * 3)");

    assert_eq!(
        updated
            .get_alter_script_with_caps(&existing, true, PG19_CAPS)
            .as_deref(),
        Some(
            "alter table public.test_table alter column test_column set expression as (id * 3);\n\n"
        )
    );
    assert!(!updated.would_drop_and_re_add_with_caps(&existing, PG19_CAPS));
    // STORED-only capability (PG17, PG18 with CHECKs): still drop + add.
    assert!(updated.would_drop_and_re_add_with_caps(&existing, PG17_CAPS));
    let script = updated
        .get_alter_script_with_caps(&existing, true, PG17_CAPS)
        .unwrap();
    assert!(script.contains("drop column test_column"), "{script}");
    assert!(
        script.contains("generated always as (id * 3) virtual"),
        "{script}"
    );
}

#[test]
fn set_expression_stored_needs_the_stored_capability() {
    let existing = generated_column("s", "(id * 2)");
    let updated = generated_column("s", "(id * 3)");
    let virtual_only = GeneratedAlterCaps {
        stored_set_expression: false,
        virtual_set_expression: true,
    };
    assert!(updated.would_drop_and_re_add_with_caps(&existing, virtual_only));
    assert!(!updated.would_drop_and_re_add_with_caps(&existing, PG17_CAPS));
}

#[test]
fn set_expression_is_emitted_even_with_use_drop_false() {
    // Non-destructive: the column keeps its attnum, grants, comment and
    // dependents, and generated values are derived data.
    let existing = generated_column("s", "(id * 2)");
    let updated = generated_column("s", "(id * 3)");
    let script = updated
        .get_alter_script_with_caps(&existing, false, PG17_CAPS)
        .unwrap();
    assert_eq!(
        script,
        "alter table public.test_table alter column test_column set expression as (id * 3);\n\n"
    );
}

#[test]
fn set_expression_never_used_for_kind_flips_or_new_generated_columns() {
    // STORED <-> VIRTUAL and plain -> generated have no in-place form.
    let stored = generated_column("s", "(id * 2)");
    let virtual_new_expr = generated_column("v", "(id * 3)");
    let virtual_same_expr = generated_column("v", "(id * 2)");
    let mut plain = generated_column("s", "(id * 2)");
    plain.is_generated = "NEVER".to_string();
    plain.generation_expression = None;
    plain.generation_type = None;

    for (to, from) in [
        (&virtual_new_expr, &stored),
        (&virtual_same_expr, &stored),
        (&stored, &virtual_same_expr),
        (&stored, &plain),
        (&virtual_same_expr, &plain),
    ] {
        assert!(to.would_drop_and_re_add_with_caps(from, PG19_CAPS));
        let script = to
            .get_alter_script_with_caps(from, true, PG19_CAPS)
            .unwrap();
        assert!(!script.contains("set expression"), "{script}");
        assert!(script.contains("drop column test_column"), "{script}");
    }
}

#[test]
fn set_expression_caps_leave_stored_to_plain_as_drop_expression() {
    let existing = generated_column("s", "(id * 2)");
    let mut updated = existing.clone();
    updated.is_generated = "NEVER".to_string();
    updated.generation_expression = None;
    updated.generation_type = None;
    for caps in [GeneratedAlterCaps::default(), PG19_CAPS] {
        assert!(!updated.would_drop_and_re_add_with_caps(&existing, caps));
        assert_eq!(
            updated
                .get_alter_script_with_caps(&existing, true, caps)
                .as_deref(),
            Some("alter table public.test_table alter column test_column drop expression;\n\n")
        );
    }
    // VIRTUAL -> plain has no DROP EXPRESSION: drop + add, whatever the caps.
    let virtual_existing = generated_column("v", "(id * 2)");
    assert!(updated.would_drop_and_re_add_with_caps(&virtual_existing, PG19_CAPS));
}

#[test]
fn a_type_change_with_the_expression_change_is_drop_and_add() {
    // Review finding: `ALTER COLUMN ... TYPE` on a generated column casts the
    // OLD expression to the new type (no USING allowed), so `int -> date`
    // fails ("generation expression ... cannot be cast automatically") and
    // the SET EXPRESSION behind it fails too ("column ... is of type integer
    // but default expression is of type date"). Verified live on 17, 18, 19.
    // A drop + add changes both at once, and the TYPE statement is left out.
    for kind in ["s", "v"] {
        let existing = generated_column(kind, "(id * 2)");
        let mut updated = generated_column(kind, "('2020-01-01'::date + id)");
        updated.data_type = "date".to_string();
        assert!(updated.would_drop_and_re_add_with_caps(&existing, PG19_CAPS));
        let script = updated
            .get_alter_script_with_caps(&existing, true, PG19_CAPS)
            .unwrap();
        let gen_kind = if kind == "v" { "virtual" } else { "stored" };
        assert_eq!(
            script,
            format!(
                "alter table public.test_table drop column test_column;\n\n\
                 alter table public.test_table add column test_column date generated always as \
                 ('2020-01-01'::date + id) {gen_kind};\n\n"
            ),
            "kind {kind}"
        );
    }
}

#[test]
fn a_live_drop_and_add_leaves_out_the_statements_it_supersedes() {
    // The re-added column carries the TO type, default and nullability, so
    // the ALTERs that would precede the drop are redundant (and a TYPE change
    // can fail). use_drop=false keeps them: the drop + add is only a comment.
    let mut existing = generated_column("s", "(id * 2)");
    existing.is_generated = "NEVER".to_string();
    existing.generation_expression = None;
    existing.generation_type = None;
    existing.data_type = "integer".to_string();
    existing.column_default = Some("0".to_string());
    let mut updated = generated_column("s", "(id * 2)");
    updated.data_type = "bigint".to_string();
    updated.is_nullable = false;
    let live = updated
        .get_alter_script_with_caps(&existing, true, PG19_CAPS)
        .unwrap();
    assert_eq!(
        live,
        "alter table public.test_table drop column test_column;\n\n\
         alter table public.test_table add column test_column bigint generated always as \
         (id * 2) stored not null;\n\n"
    );
    let commented = updated
        .get_alter_script_with_caps(&existing, false, PG19_CAPS)
        .unwrap();
    assert!(
        commented
            .starts_with("alter table public.test_table alter column test_column type bigint;"),
        "{commented}"
    );
    assert!(
        commented.contains("-- alter table public.test_table drop column test_column;"),
        "{commented}"
    );
}

#[test]
fn alter_parts_defer_the_generated_part() {
    // SET EXPRESSION and the drop + add may read a column the migration
    // adds, so the table emits them after its ADD COLUMNs: they come out of
    // `alter_parts` separately, and concatenate to the plain script.
    let existing = generated_column("s", "(id * 2)");
    let mut updated = generated_column("s", "(id * 3)");
    updated.is_nullable = false;
    updated.statistics_target = Some(500);
    updated.comment = Some("tripled".to_string());

    let parts = updated.alter_parts(&existing, true, PG17_CAPS, true);
    assert!(parts.is_deferred());
    assert_eq!(
        parts.inline,
        "alter table public.test_table alter column test_column set not null;\n\n"
    );
    assert_eq!(parts.drop, "");
    assert_eq!(
        parts.deferred,
        "alter table only public.test_table alter column test_column set expression as (id * 3);\n\n"
    );
    assert_eq!(
        parts.post,
        "alter table public.test_table alter column test_column set statistics 500;\n\n"
    );
    assert_eq!(
        parts.comment,
        "comment on column public.test_table.test_column is 'tripled';\n\n"
    );
    // Without ONLY it is exactly the column's alter script.
    let recursive = updated.alter_parts(&existing, true, PG17_CAPS, false);
    assert_eq!(
        Some(
            [
                recursive.inline,
                recursive.drop,
                recursive.deferred,
                recursive.post,
                recursive.comment
            ]
            .concat()
        ),
        updated.get_alter_script_with_caps(&existing, true, PG17_CAPS)
    );

    // Drop + add (no SET EXPRESSION before PostgreSQL 17).
    let parts = updated.alter_parts(&existing, true, GeneratedAlterCaps::default(), true);
    assert_eq!(parts.inline, "");
    assert_eq!(
        parts.drop,
        "alter table public.test_table drop column test_column;\n\n"
    );
    assert!(
        parts
            .deferred
            .starts_with("alter table public.test_table add column test_column integer generated always as (id * 3) stored not null;"),
        "{}",
        parts.deferred
    );

    // Nothing generated changes: nothing is deferred.
    let mut plain_change = existing.clone();
    plain_change.is_nullable = false;
    let parts = plain_change.alter_parts(&existing, true, PG19_CAPS, true);
    assert!(!parts.is_deferred());
    assert_eq!(
        parts.inline,
        "alter table public.test_table alter column test_column set not null;\n\n"
    );
}

#[test]
fn is_recreated_by_parent_add_compares_what_add_column_copies() {
    let parent = generated_column("s", "(qty * 3)");
    let mut child = parent.clone();
    child.table = "child_table".to_string();
    child.comment = Some("not copied, and not compared".to_string());
    assert!(child.is_recreated_by_parent_add(&parent));

    let mut own_expression = child.clone();
    own_expression.generation_expression = Some("(qty * 5)".to_string());
    let mut other_kind = child.clone();
    other_kind.generation_type = Some("v".to_string());
    let mut other_type = child.clone();
    other_type.data_type = "bigint".to_string();
    let mut not_null = child.clone();
    not_null.is_nullable = false;
    for differing in [own_expression, other_kind, other_type, not_null] {
        assert!(
            !differing.is_recreated_by_parent_add(&parent),
            "{differing:?}"
        );
    }
}

#[test]
fn equivalent_generation_expressions_are_not_a_generated_change() {
    // Two deparse forms of the same IN-list (issue #226). `PartialEq` and the
    // hash already treat them as equal; the alter path must too, or any other
    // change to the column would drag a spurious drop + add (or, before,
    // the invalid `add generated ... stored`) along with it.
    let array_level = "((label)::text = ANY ((ARRAY['FOO'::character varying, 'BAR'::character varying])::text[]))";
    let element_level = "((label)::text = ANY (ARRAY[('FOO'::character varying)::text, ('BAR'::character varying)::text]))";
    for kind in ["s", "v"] {
        let existing = generated_column(kind, array_level);
        let mut updated = generated_column(kind, element_level);
        updated.is_nullable = false;
        for caps in [GeneratedAlterCaps::default(), PG19_CAPS] {
            assert!(!updated.would_drop_and_re_add_with_caps(&existing, caps));
            assert_eq!(
                updated
                    .get_alter_script_with_caps(&existing, true, caps)
                    .as_deref(),
                Some("alter table public.test_table alter column test_column set not null;\n\n"),
                "kind {kind}"
            );
        }
    }
}

#[test]
fn only_a_pair_enclosing_the_whole_expression_is_stripped() {
    // PostgreSQL deparses `a * 2 + b * 3` as `((a * 2) + (b * 3))`. Only the
    // outer pair is redundant; `(a * 2) + (b * 3)` starts with `(` and ends
    // with `)` without being one group, and stripping it used to emit
    // `AS (a * 2) + (b * 3)` — a syntax error — from CREATE TABLE, ADD COLUMN
    // and SET EXPRESSION alike. Verified live on PostgreSQL 16 and 17.
    let deparsed = "((a * 2) + (b * 3))";
    let mut column = generated_column("s", deparsed);
    assert_eq!(
        column.get_script(),
        "test_column integer generated always as ((a * 2) + (b * 3)) stored"
    );
    assert_eq!(
        column.get_add_script(),
        "alter table public.test_table add column test_column integer \
         generated always as ((a * 2) + (b * 3)) stored;\n\n"
    );
    let existing = generated_column("s", "(a + b)");
    assert_eq!(
        column
            .get_alter_script_with_caps(&existing, true, PG17_CAPS)
            .as_deref(),
        Some(
            "alter table public.test_table alter column test_column \
             set expression as ((a * 2) + (b * 3));\n\n"
        )
    );

    // Redundant pairs still go, however many there are.
    column.generation_expression = Some("(((a * 2)))".to_string());
    assert_eq!(
        column.get_script(),
        "test_column integer generated always as (a * 2) stored"
    );
}

#[test]
fn is_enclosed_in_parens_matches_the_opening_pair() {
    assert!(is_enclosed_in_parens("(a * 2)"));
    assert!(is_enclosed_in_parens("((a * 2) + (b * 3))"));
    assert!(!is_enclosed_in_parens("(a * 2) + (b * 3)"));
    assert!(!is_enclosed_in_parens("(a) || (b)"));
    assert!(!is_enclosed_in_parens("a * 2"));
    assert!(!is_enclosed_in_parens("lower(a)"));
    // Parentheses inside literals and quoted identifiers do not count,
    // doubled quotes included.
    assert!(is_enclosed_in_parens("('(' || a)"));
    assert!(!is_enclosed_in_parens("(')' || a) || (b)"));
    assert!(is_enclosed_in_parens("(\"we(ird\" || 'it''s)')"));
    assert!(!is_enclosed_in_parens("(\"a)\") || (b)"));
    // Unbalanced input is never treated as enclosed.
    assert!(!is_enclosed_in_parens("(a"));
    assert!(!is_enclosed_in_parens("('a)"));
}

// ==== pg19:statistics tests ====

#[test]
fn rebuilds_statistics_in_place_for_a_type_or_collation_change() {
    // ALTER COLUMN ... TYPE re-creates the statistics on the column (and
    // resets their target) on every supported version.
    let existing = create_test_column();
    let mut retyped = create_test_column();
    retyped.data_type = "text".to_string();
    retyped.character_maximum_length = None;
    assert!(retyped.rebuilds_statistics_in_place(&existing, GeneratedAlterCaps::default(), true));
    let mut collated = create_test_column();
    collated.collation_name = Some("C".to_string());
    assert!(collated.rebuilds_statistics_in_place(&existing, GeneratedAlterCaps::default(), true));

    // Nullability, default, storage and a column statistics target do not.
    let mut other = create_test_column();
    other.is_nullable = false;
    other.column_default = Some("'x'::character varying".to_string());
    other.storage = Some("external".to_string());
    other.statistics_target = Some(50);
    other.comment = Some("c".to_string());
    assert!(!other.rebuilds_statistics_in_place(&existing, PG19_CAPS, true));
}

#[test]
fn rebuilds_statistics_in_place_for_set_expression_only() {
    let stored = generated_column("s", "(id * 2)");
    let stored_new = generated_column("s", "(id * 3)");
    // SET EXPRESSION (PostgreSQL 17+) rebuilds; the drop + add before it
    // does not — the statistics go with the column.
    assert!(stored_new.rebuilds_statistics_in_place(&stored, PG17_CAPS, true));
    assert!(!stored_new.rebuilds_statistics_in_place(&stored, GeneratedAlterCaps::default(), true));

    let virt = generated_column("v", "(id * 2)");
    let virt_new = generated_column("v", "(id * 3)");
    assert!(virt_new.rebuilds_statistics_in_place(&virt, PG19_CAPS, true));
    assert!(!virt_new.rebuilds_statistics_in_place(&virt, PG17_CAPS, true));

    // STORED -> plain is DROP EXPRESSION, which keeps the statistics as
    // they are.
    let mut plain = create_test_column();
    plain.data_type = "integer".to_string();
    plain.character_maximum_length = None;
    plain.character_octet_length = None;
    assert!(!plain.rebuilds_statistics_in_place(&stored, PG19_CAPS, true));
}

#[test]
fn rebuilds_statistics_in_place_when_a_commented_drop_add_leaves_the_type_change() {
    // A plain column turning generated with a new type is a drop + add.
    // Live, the type change is folded into it; with use_drop off the drop +
    // add is commented out but the ALTER COLUMN ... TYPE ahead of it runs.
    let mut existing = generated_column("s", "(id * 2)");
    existing.is_generated = "NEVER".to_string();
    existing.generation_expression = None;
    existing.generation_type = None;
    let mut updated = generated_column("s", "(id * 3)");
    updated.data_type = "bigint".to_string();
    assert!(updated.would_drop_and_re_add_with_caps(&existing, PG19_CAPS));
    assert!(!updated.rebuilds_statistics_in_place(&existing, PG19_CAPS, true));
    assert!(updated.rebuilds_statistics_in_place(&existing, PG19_CAPS, false));
    let script = updated
        .get_alter_script_with_caps(&existing, false, PG19_CAPS)
        .unwrap();
    assert!(
        script.starts_with("alter table public.test_table alter column test_column type bigint;"),
        "{script}"
    );

    // A generated column's type change is not run ahead of the commented
    // drop + add (it would be computed from the old expression), so nothing
    // is rebuilt in place.
    let existing = generated_column("s", "(id * 2)");
    assert!(updated.would_drop_and_re_add_with_caps(&existing, PG19_CAPS));
    assert!(!updated.rebuilds_statistics_in_place(&existing, PG19_CAPS, false));
    let script = updated
        .get_alter_script_with_caps(&existing, false, PG19_CAPS)
        .unwrap();
    assert!(script.lines().all(|l| l.starts_with("--")), "{script}");
}

#[test]
fn alters_type_in_place_is_the_type_half_of_rewrites_in_place() {
    // The comparer asks which members of a family an ancestor's recursing
    // statement rewrites: a partition's parent runs both ALTER COLUMN ...
    // TYPE and SET EXPRESSION without ONLY, an inheritance parent only the
    // first (its SET EXPRESSION says ONLY).
    let existing = create_test_column();
    let mut retyped = create_test_column();
    retyped.data_type = "text".to_string();
    retyped.character_maximum_length = None;
    assert!(retyped.alters_type_in_place(&existing, PG19_CAPS, true));
    assert!(retyped.rewrites_in_place(&existing, PG19_CAPS, true));

    let stored = generated_column("s", "(id * 2)");
    let stored_new = generated_column("s", "(id * 3)");
    assert!(!stored_new.alters_type_in_place(&stored, PG17_CAPS, true));
    assert!(stored_new.rewrites_in_place(&stored, PG17_CAPS, true));
    // Without SET EXPRESSION it is a drop + add, which rewrites nothing in
    // place: what read the old column went with it.
    assert!(!stored_new.rewrites_in_place(&stored, GeneratedAlterCaps::default(), true));

    // A type change folded into a live drop + add is not run. Commented out
    // (use_drop off), it is not run either when the column is generated
    // already: it would be computed from the old expression, and the drop +
    // add supersedes it. A plain column's type change still runs.
    let mut both = generated_column("s", "(id * 3)");
    both.data_type = "bigint".to_string();
    assert!(!both.alters_type_in_place(&stored, PG19_CAPS, true));
    assert!(!both.alters_type_in_place(&stored, PG19_CAPS, false));
    let mut plain = generated_column("s", "(id * 3)");
    plain.is_generated = "NEVER".to_string();
    plain.generation_expression = None;
    plain.generation_type = None;
    assert!(!both.alters_type_in_place(&plain, PG19_CAPS, true));
    assert!(both.alters_type_in_place(&plain, PG19_CAPS, false));

    // The statistics predicate is the same question.
    for (new, old, caps, use_drop) in [
        (&retyped, &existing, PG19_CAPS, true),
        (&stored_new, &stored, PG17_CAPS, true),
        (&stored_new, &stored, GeneratedAlterCaps::default(), true),
        (&both, &stored, PG19_CAPS, true),
        (&both, &stored, PG19_CAPS, false),
        (&both, &plain, PG19_CAPS, false),
    ] {
        assert_eq!(
            new.rebuilds_statistics_in_place(old, caps, use_drop),
            new.rewrites_in_place(old, caps, use_drop)
        );
    }
}

#[test]
fn pre_columns_generation_expression_reads_its_column_references() {
    let reads = |expr: &str, column: &str| {
        let mut column_def = generated_column("s", expr);
        column_def.name = "g".to_string();
        column_def.generation_expression_reads(column)
    };
    assert!(reads("(a * 2)", "a"));
    assert!(reads("((a * 2) + b)", "b"));
    assert!(reads("(\"Base Col\" * 2)", "\"Base Col\""));
    assert!(reads("COALESCE(a, 0)", "a"));
    assert!(reads("lower((a)::text)", "a"));
    // Not a reference: a longer name, a literal, a function, a type, a
    // qualified name.
    assert!(!reads("(ab * 2)", "a"));
    assert!(!reads("('a'::text || x)", "a"));
    assert!(!reads("a(x)", "a"));
    assert!(!reads("a (x)", "a"));
    assert!(!reads("(x)::a", "a"));
    assert!(!reads("s.a(x)", "a"));
    assert!(!reads("('it''s a' || x)", "a"));
    assert!(!reads("(\"Base Col\" * 2)", "Base"));
    // A column that is not generated reads nothing.
    let mut plain = generated_column("s", "(a * 2)");
    plain.is_generated = "NEVER".to_string();
    assert!(!plain.generation_expression_reads("a"));
    assert!(!plain.is_generated_column());
    assert!(generated_column("v", "(a * 2)").is_generated_column());
}

#[test]
fn pre_columns_alter_parts_recreating_forces_the_drop_and_add() {
    let existing = generated_column("s", "(id * 2)");
    // Unchanged: nothing, unless forced.
    assert_eq!(
        existing.alter_parts(&existing, true, PG19_CAPS, true),
        ColumnAlterParts::default()
    );
    let parts = existing.alter_parts_recreating(&existing, true, PG19_CAPS, true, true);
    assert_eq!(
        parts.drop,
        "alter table public.test_table drop column test_column;\n\n"
    );
    assert!(
        parts.deferred.starts_with(
            "alter table public.test_table add column test_column integer generated always as (id * 2) stored"
        ),
        "{}",
        parts.deferred
    );
    // Forced over a SET EXPRESSION the server could make.
    let updated = generated_column("s", "(id * 3)");
    let parts = updated.alter_parts_recreating(&existing, true, PG19_CAPS, true, true);
    assert!(!parts.deferred.contains("set expression"), "{parts:?}");
    assert!(!parts.drop.is_empty(), "{parts:?}");
    // Ignored without use_drop, and for a column TO has plain.
    assert_eq!(
        existing.alter_parts_recreating(&existing, false, PG19_CAPS, true, true),
        ColumnAlterParts::default()
    );
    let mut plain = existing.clone();
    plain.is_generated = "NEVER".to_string();
    plain.generation_expression = None;
    plain.generation_type = None;
    assert_eq!(
        plain.alter_parts_recreating(&existing, true, PG19_CAPS, true, true),
        plain.alter_parts(&existing, true, PG19_CAPS, true)
    );
}

// ---- pre-columns review fixes ----------------------------------------------

#[test]
fn pre_columns_with_type_of_takes_the_type_and_nothing_else() {
    let mut from = create_test_column();
    from.data_type = "integer".to_string();
    from.character_maximum_length = None;
    from.collation_name = None;
    from.udt_name = Some("int4".to_string());
    let mut to = create_test_column();
    to.data_type = "bigint".to_string();
    to.character_maximum_length = None;
    to.collation_name = None;
    to.udt_name = Some("int8".to_string());
    to.column_default = Some("7".to_string());
    to.comment = Some("new".to_string());

    let kept = to.with_type_of(&from);
    assert!(!kept.type_differs(&from));
    assert!(kept.type_differs(&to));
    assert_eq!(kept.column_default.as_deref(), Some("7"));
    assert_eq!(kept.comment.as_deref(), Some("new"));
    // So an alter from `from` to it leaves the type alone.
    let script = kept.get_alter_script(&from, true).unwrap_or_default();
    assert!(!script.contains(" type "), "{script}");
    assert!(script.contains("set default 7"), "{script}");
}

#[test]
fn pre_columns_add_script_can_name_its_not_null() {
    let mut column = create_test_column();
    column.is_nullable = false;
    let plain = column.get_add_script();
    let named = column.get_add_script_with_not_null("constraint c_nn not null");
    assert!(plain.contains(" not null;"), "{plain}");
    assert_eq!(
        named,
        plain.replace(" not null;", " constraint c_nn not null;")
    );
    // A nullable column has none to name.
    column.is_nullable = true;
    assert_eq!(
        column.get_add_script_with_not_null("constraint c_nn not null"),
        column.get_add_script()
    );
}

// ==== pre-clear tests ====

#[test]
fn use_drop_false_leaves_a_generated_columns_type_change_to_the_commented_drop_and_add() {
    // FROM `stamp integer generated always as (id * 2)`, TO `stamp date
    // generated always as ('2020-01-01'::date + id)`: a live
    // `alter column stamp type date` recomputes the OLD expression as a date
    // and fails ("cannot be cast automatically to type date"). With
    // use_drop off the whole change is the commented drop + add.
    for (kind, caps) in [
        ("s", PG19_CAPS),
        ("s", GeneratedAlterCaps::default()),
        ("v", PG19_CAPS),
    ] {
        let existing = generated_column(kind, "(id * 2)");
        let mut updated = generated_column(kind, "('2020-01-01'::date + id)");
        updated.data_type = "date".to_string();
        let script = updated
            .get_alter_script_with_caps(&existing, false, caps)
            .unwrap();
        assert!(
            script.lines().all(|l| l.starts_with("--")),
            "kind {kind}: {script}"
        );
        if kind == "s" {
            assert!(
                script.starts_with(
                    "-- use_drop=false: changing a stored generated column's type together with \
                     its expression needs drop/add (SET EXPRESSION keeps the type); statements \
                     commented out\n"
                ),
                "{script}"
            );
        }
        assert!(
            script.contains(
                "-- alter table public.test_table add column test_column date generated always as"
            ),
            "kind {kind}: {script}"
        );
        assert!(!updated.alters_type_in_place(&existing, caps, false));
    }
}

#[test]
fn use_drop_false_keeps_the_other_in_place_changes_of_a_generated_column() {
    // Only the type change goes: NOT NULL still runs in place.
    let existing = generated_column("s", "(id * 2)");
    let mut updated = generated_column("s", "(id * 3)");
    updated.data_type = "bigint".to_string();
    updated.is_nullable = false;
    let script = updated
        .get_alter_script_with_caps(&existing, false, GeneratedAlterCaps::default())
        .unwrap();
    assert!(
        script.starts_with("alter table public.test_table alter column test_column set not null;"),
        "{script}"
    );
    assert!(!script.contains("\nalter table public.test_table alter column test_column type"));
    assert!(
        script.contains("-- alter table public.test_table drop column test_column;"),
        "{script}"
    );
}

// ==== r3-gencol tests ====

/// `public.t.g <data_type> GENERATED ALWAYS AS (app.f(v)) STORED`, with the
/// PostgreSQL 14 flag as given.
fn r3g_generated(data_type: &str, blocks_retype: bool) -> TableColumn {
    let mut column = create_test_column();
    column.table = "t".to_string();
    column.name = "g".to_string();
    column.data_type = data_type.to_string();
    column.character_maximum_length = None;
    column.character_octet_length = None;
    column.is_generated = "ALWAYS".to_string();
    column.generation_expression = Some("app.f(v)".to_string());
    column.generation_type = Some("s".to_string());
    column.generation_blocks_retype = blocks_retype;
    column
}

#[test]
fn r3g_pg14_retype_blocked_by_the_expression_is_a_drop_and_add() {
    let from = r3g_generated("integer", true);
    let to = r3g_generated("bigint", false);
    assert!(to.retype_needs_drop_and_add(&from));
    assert!(to.would_drop_and_re_add_with_caps(&from, GeneratedAlterCaps::default()));
    assert!(!to.alters_type_in_place(&from, GeneratedAlterCaps::default(), true));
    let script = to
        .get_alter_script_with_caps(&from, true, GeneratedAlterCaps::default())
        .unwrap();
    assert_eq!(
        script,
        "alter table public.t drop column g;\n\nalter table public.t add column g bigint generated always as (app.f(v)) stored;\n\n"
    );
    assert!(!script.contains("alter column g type"), "{script}");
}

#[test]
fn r3g_pg14_retype_blocked_by_the_expression_is_commented_out_without_use_drop() {
    let from = r3g_generated("integer", true);
    let to = r3g_generated("bigint", false);
    let script = to
        .get_alter_script_with_caps(&from, false, GeneratedAlterCaps::default())
        .unwrap();
    assert!(
        script.starts_with(
            "-- use_drop=false: PostgreSQL 14 cannot change the type of a generated column"
        ),
        "{script}"
    );
    assert!(
        script.contains("-- alter table public.t drop column g;"),
        "{script}"
    );
    assert!(!script.contains("\nalter table"), "{script}");
}

#[test]
fn r3g_retype_stays_in_place_without_the_flag_or_the_type_change() {
    // Not flagged: PostgreSQL 15+, or 14 with only built-ins in the expression.
    let from = r3g_generated("integer", false);
    let to = r3g_generated("bigint", false);
    assert!(!to.retype_needs_drop_and_add(&from));
    assert_eq!(
        to.get_alter_script_with_caps(&from, true, GeneratedAlterCaps::default())
            .unwrap(),
        "alter table public.t alter column g type bigint;\n\n"
    );
    // Flagged but the type stays: nothing to do.
    let flagged = r3g_generated("integer", true);
    assert!(!r3g_generated("integer", false).retype_needs_drop_and_add(&flagged));
    assert!(
        r3g_generated("integer", false)
            .get_alter_script_with_caps(&flagged, true, GeneratedAlterCaps::default())
            .is_none()
    );
    // A column TO makes plain keeps its values: never a drop + add (see
    // `plain_needs_rewrite` for the rewrite it may need instead).
    let plain = r3g_plain("bigint");
    assert!(!plain.retype_needs_drop_and_add(&flagged));
}

/// The plain TO side of `r3g_generated`.
fn r3g_plain(data_type: &str) -> TableColumn {
    let mut plain = r3g_generated(data_type, false);
    plain.is_generated = "NEVER".to_string();
    plain.generation_expression = None;
    plain.generation_type = None;
    plain
}

#[test]
fn r3g_pg14_plain_with_a_new_type_is_rewritten_keeping_its_values() {
    // PostgreSQL 14 keeps the column depending on `app.f` after DROP
    // EXPRESSION: `alter column g type bigint` failed before and after it.
    let from = r3g_generated("integer", true);
    let mut to = r3g_plain("bigint");
    to.column_default = Some("7".to_string());
    to.is_nullable = false;
    to.storage = Some("PLAIN".to_string());
    to.statistics_target = Some(500);
    to.comment = Some("it's g".to_string());
    assert!(to.plain_needs_rewrite(&from));
    assert!(!to.retype_needs_drop_and_add(&from));
    assert!(to.would_drop_and_re_add_with_caps(&from, GeneratedAlterCaps::default()));
    assert!(!to.alters_type_in_place(&from, GeneratedAlterCaps::default(), true));
    let parts = to.alter_parts(&from, true, GeneratedAlterCaps::default(), false);
    assert_eq!(parts.inline, "");
    assert_eq!(
        parts.drop,
        "alter table public.t add column \"pgc_rewrite_g\" bigint;\n\n\
         alter table public.t alter column \"pgc_rewrite_g\" type bigint using (g);\n\n\
         alter table public.t drop column g;\n\n"
    );
    assert_eq!(
        parts.deferred,
        "alter table public.t rename column \"pgc_rewrite_g\" to g;\n\n\
         alter table public.t alter column g set default 7;\n\n\
         alter table public.t alter column g set not null;\n\n\
         alter table public.t alter column g set storage PLAIN;\n\n\
         alter table public.t alter column g set statistics 500;\n\n"
    );
    assert_eq!(parts.post, "");
    assert_eq!(
        parts.comment,
        "comment on column public.t.g is 'it''s g';\n\n"
    );
    let script = to
        .get_alter_script_with_caps(&from, true, GeneratedAlterCaps::default())
        .unwrap();
    assert!(!script.contains("drop expression"), "{script}");
    assert!(!script.contains("alter column g type"), "{script}");
}

#[test]
fn r3g_pg14_plain_rewrite_is_commented_out_without_use_drop() {
    let from = r3g_generated("integer", true);
    let to = r3g_plain("bigint");
    let script = to
        .get_alter_script_with_caps(&from, false, GeneratedAlterCaps::default())
        .unwrap();
    assert!(
        script.starts_with("-- use_drop=false: PostgreSQL 14 keeps a generated column"),
        "{script}"
    );
    assert!(
        script.contains("-- alter table public.t rename column \"pgc_rewrite_g\" to g;"),
        "{script}"
    );
    assert!(!script.contains("\nalter table"), "{script}");
}

#[test]
fn r3g_pg14_plain_rewrite_only_when_it_is_needed() {
    // The same type, and no routine of the expression dropped: DROP
    // EXPRESSION, as always.
    let flagged = r3g_generated("integer", true);
    let same = r3g_plain("integer");
    assert!(!same.plain_needs_rewrite(&flagged));
    assert_eq!(
        same.get_alter_script_with_caps(&flagged, true, GeneratedAlterCaps::default())
            .unwrap(),
        "alter table public.t alter column g drop expression;\n\n"
    );
    // The migration drops a routine the expression calls: rewritten.
    let mut dropped = flagged.clone();
    dropped.generation_routine_dropped = true;
    assert!(same.plain_needs_rewrite(&dropped));
    let parts = same.alter_parts(&dropped, true, GeneratedAlterCaps::default(), false);
    assert!(
        parts.drop.contains("type integer using (g);"),
        "{}",
        parts.drop
    );
    // Not flagged (PostgreSQL 15+, or 14 with built-ins only): ALTER TYPE
    // and DROP EXPRESSION.
    let unflagged = r3g_generated("integer", false);
    let bigger = r3g_plain("bigint");
    assert!(!bigger.plain_needs_rewrite(&unflagged));
    assert_eq!(
        bigger
            .get_alter_script_with_caps(&unflagged, true, GeneratedAlterCaps::default())
            .unwrap(),
        "alter table public.t alter column g type bigint;\n\nalter table public.t alter column g drop expression;\n\n"
    );
    // Never for an identity column, nor for one that stays generated.
    let mut identity = r3g_plain("bigint");
    identity.is_identity = true;
    assert!(!identity.plain_needs_rewrite(&flagged));
    assert!(!r3g_generated("bigint", false).plain_needs_rewrite(&flagged));
}

#[test]
fn r3g_rewrite_temp_name_is_quoted_and_fits_an_identifier() {
    let mut column = r3g_plain("bigint");
    column.name = "\"Mixed\"\"Case\"".to_string();
    assert_eq!(column.rewrite_temp_name(), "\"pgc_rewrite_Mixed\"\"Case\"");
    column.name = "c".repeat(63);
    let temp = column.rewrite_temp_name();
    assert_eq!(temp.len(), 63 + 2, "{temp}");
    assert!(temp.starts_with("\"pgc_rewrite_ccc"), "{temp}");
    column.name = "é".repeat(30);
    let temp = column.rewrite_temp_name();
    assert!(temp.len() <= 63 + 2, "{temp}");
}

#[test]
fn r3g_generation_routine_dropped_is_not_part_of_the_column() {
    let flagged = r3g_generated("integer", true);
    let mut marked = flagged.clone();
    marked.generation_routine_dropped = true;
    assert_eq!(flagged, marked);
    let digest = |column: &TableColumn| {
        let mut hasher = Sha256::new();
        column.add_to_hasher(&mut hasher);
        format!("{:x}", hasher.finalize())
    };
    assert_eq!(digest(&flagged), digest(&marked));
    let json = serde_json::to_string(&marked).unwrap();
    assert!(!json.contains("generation_routine_dropped"), "{json}");
    let back: TableColumn = serde_json::from_str(&json).unwrap();
    assert!(!back.generation_routine_dropped);
}

#[test]
fn r3g_generation_blocks_retype_is_not_part_of_the_column() {
    // A fact about the dumped server: equality and the hash ignore it.
    let flagged = r3g_generated("integer", true);
    let plain = r3g_generated("integer", false);
    assert_eq!(flagged, plain);
    let digest = |column: &TableColumn| {
        let mut hasher = Sha256::new();
        column.add_to_hasher(&mut hasher);
        format!("{:x}", hasher.finalize())
    };
    assert_eq!(digest(&flagged), digest(&plain));
}

// ==== r3-ddl: column collations ====

fn collated_column(schema: Option<&str>, name: &str) -> TableColumn {
    let mut column = create_test_column();
    column.collation_catalog = Some("test_catalog".to_string());
    column.collation_schema = schema.map(str::to_string);
    column.collation_name = Some(name.to_string());
    column
}

fn column_hash(column: &TableColumn) -> String {
    let mut hasher = Sha256::new();
    column.add_to_hasher(&mut hasher);
    format!("{:x}", hasher.finalize())
}

/// A `pg_catalog` collation keeps the spelling every pgc version emitted:
/// the bare name in double quotes. A collation schema the dump does not
/// record is read the same way.
#[test]
fn a_pg_catalog_collation_keeps_its_unqualified_quoted_spelling() {
    for schema in [Some("pg_catalog"), None, Some("")] {
        for (name, clause) in [
            ("C", " collate \"C\""),
            ("en_US", " collate \"en_US\""),
            ("ucs_basic", " collate \"ucs_basic\""),
        ] {
            let column = collated_column(schema, name);
            assert_eq!(
                column.get_script(),
                format!("test_column varchar(255){clause}"),
                "{schema:?}.{name}"
            );
            assert!(
                column
                    .get_add_script()
                    .contains(&format!("varchar(255){clause};")),
                "{}",
                column.get_add_script()
            );
        }
    }
}

/// Any other collation is qualified with its schema and both parts quoted as
/// `quote_ident` would: the migrated database's `search_path` may not reach
/// it, or may reach a same-named collation first.
#[test]
fn a_collation_outside_pg_catalog_is_schema_qualified() {
    for (schema, name, clause) in [
        ("app", "ci", " collate app.ci"),
        ("public", "ci", " collate public.ci"),
        ("My App", "Ci", " collate \"My App\".\"Ci\""),
        ("app", "select", " collate app.\"select\""),
        ("app", "en_US", " collate app.\"en_US\""),
    ] {
        let column = collated_column(Some(schema), name);
        assert_eq!(
            column.get_script(),
            format!("test_column varchar(255){clause}")
        );
        assert_eq!(
            column.get_add_script(),
            format!(
                "alter table public.test_table add column test_column varchar(255){clause};\n\n"
            )
        );
    }
}

/// A `"` in a collation name is doubled, so the name cannot close the
/// identifier and run the rest of itself when the migration is applied.
#[test]
fn a_collation_name_holding_a_quote_cannot_end_the_identifier() {
    let hostile = "ci\"; create table public.pwned(x int); --";
    let column = collated_column(Some("public"), hostile);
    assert_eq!(
        column.get_add_script(),
        "alter table public.test_table add column test_column varchar(255) collate \
         public.\"ci\"\"; create table public.pwned(x int); --\";\n\n"
    );
    let pg_catalog = collated_column(Some("pg_catalog"), "C\"; drop table t; --");
    assert_eq!(
        pg_catalog.get_script(),
        "test_column varchar(255) collate \"C\"\"; drop table t; --\""
    );
}

/// The collation's schema is part of the type: the same name in another
/// schema is a different collation, and the column is retyped to it.
#[test]
fn a_collation_schema_change_alone_retypes_the_column() {
    let existing = collated_column(Some("public"), "ci");
    let updated = collated_column(Some("app"), "ci");
    assert!(updated.type_differs(&existing));
    let script = updated
        .get_alter_script(&existing, true)
        .expect("a collation change is migrated");
    assert_eq!(
        script,
        "alter table public.test_table alter column test_column type varchar(255) collate app.ci;\n\n"
    );
}

/// A column without a collation of its own hashes exactly as before (the
/// pin of `test_known_sha256_hash`); one with a collation folds it in, so a
/// change of collation — its name or its schema — changes the table hash.
#[test]
fn a_column_collation_is_hashed_only_when_the_column_has_one() {
    let plain = create_test_column();
    let pinned = "e71094fafb6a1f2d03c80ba04c8fea5dac0269f681cbe1c0e4afb1a8482b0db2";
    assert_eq!(column_hash(&plain), pinned);
    let mut empty = create_test_column();
    empty.collation_name = Some(String::new());
    empty.collation_schema = Some("pg_catalog".to_string());
    assert_eq!(column_hash(&empty), pinned);

    let c = collated_column(Some("pg_catalog"), "C");
    let posix = collated_column(Some("pg_catalog"), "POSIX");
    let public_ci = collated_column(Some("public"), "ci");
    let app_ci = collated_column(Some("app"), "ci");
    let hashes = [
        column_hash(&c),
        column_hash(&posix),
        column_hash(&public_ci),
        column_hash(&app_ci),
    ];
    for (i, a) in hashes.iter().enumerate() {
        assert_ne!(a, pinned);
        for b in &hashes[i + 1..] {
            assert_ne!(a, b);
        }
    }
    // The catalog stays out, as it always has.
    let mut other_catalog = collated_column(Some("app"), "ci");
    other_catalog.collation_catalog = Some("elsewhere".to_string());
    assert_eq!(column_hash(&other_catalog), column_hash(&app_ci));
    // A pin for the folded form.
    assert_eq!(
        column_hash(&c),
        "28e7ce16ca820236e56cc1fc54c30a963b6e0c22818135efbad1421c2b9633d0"
    );
}

// ==== r3-ddl review: a collation change alone ====

/// `only_collation_differs` holds for a change of collation, schema
/// included, and for gaining or losing one; not when anything else in the
/// type changes too, nor for no change at all.
#[test]
fn only_collation_differs_tells_a_collation_change_from_a_type_change() {
    let c = collated_column(Some("pg_catalog"), "C");
    let posix = collated_column(Some("pg_catalog"), "POSIX");
    let app_ci = collated_column(Some("app"), "ci");
    let other_ci = collated_column(Some("other"), "ci");
    let mut plain = create_test_column();
    plain.collation_name = None;
    assert!(c.only_collation_differs(&posix));
    assert!(app_ci.only_collation_differs(&other_ci));
    assert!(plain.only_collation_differs(&c));
    assert!(c.only_collation_differs(&plain));
    assert!(!c.only_collation_differs(&c.clone()));

    let mut longer = posix.clone();
    longer.character_maximum_length = Some(512);
    assert!(longer.type_differs(&c));
    assert!(!c.only_collation_differs(&longer));
    let mut retyped = posix.clone();
    retyped.data_type = "text".to_string();
    retyped.character_maximum_length = None;
    assert!(!c.only_collation_differs(&retyped));
}
