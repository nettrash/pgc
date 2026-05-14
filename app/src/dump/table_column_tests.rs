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
        "alter table public.test_table alter column test_column drop expression;\n\nalter table public.test_table alter column test_column add generated always as (id * 3) stored;\n\n"
    );
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

    // Should contain a warning about manual intervention
    assert!(
        script.contains("use_drop=false") && script.contains("manual intervention needed"),
        "should contain a warning comment, script:\n{}",
        script
    );

    // Both drop expression and add generated should be commented out
    for line in script.lines() {
        if line.contains("drop expression") || line.contains("add generated always") {
            assert!(line.starts_with("--"), "should be commented out: {}", line);
        }
    }
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
fn issue181_stored_expression_change_keeps_in_place_alter() {
    // Sanity check: the in-place `DROP EXPRESSION` + `ADD
    // GENERATED ... STORED` path is still used for the case that
    // PG18 accepts — STORED → STORED with a new expression. Only
    // VIRTUAL participants get routed to the heavier DROP COLUMN
    // path.
    let mut existing = create_test_column();
    existing.is_generated = "ALWAYS".to_string();
    existing.generation_expression = Some("(id * 2)".to_string());
    existing.generation_type = Some("s".to_string());

    let mut updated = existing.clone();
    updated.generation_expression = Some("(id * 3)".to_string());

    let script = updated
        .get_alter_script(&existing, true)
        .expect("STORED expression change must emit an alter");

    assert!(
        script.contains("drop expression"),
        "STORED → STORED expression change still uses the in-place ALTER: {script}"
    );
    assert!(
        script.contains("generated always as (id * 3) stored"),
        "expected the new STORED expression to be re-added: {script}"
    );
    assert!(
        !script.contains("drop column"),
        "STORED expression change must not destroy the column: {script}"
    );
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
