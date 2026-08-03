use super::*;

fn sample(kind: ColumnDependentKind) -> ColumnDependent {
    ColumnDependent {
        schema: "public".to_string(),
        table: "orders".to_string(),
        column: "status".to_string(),
        kind,
        dep_schema: "public".to_string(),
        dep_table: "orders".to_string(),
        dep_name: "idx_orders_status".to_string(),
    }
}

#[test]
fn test_kind_equality_and_inequality() {
    assert_eq!(ColumnDependentKind::Index, ColumnDependentKind::Index);
    assert_eq!(
        ColumnDependentKind::Constraint,
        ColumnDependentKind::Constraint
    );
    assert_eq!(ColumnDependentKind::Policy, ColumnDependentKind::Policy);
    assert_ne!(ColumnDependentKind::Index, ColumnDependentKind::Constraint);
    assert_ne!(ColumnDependentKind::Constraint, ColumnDependentKind::Policy);
    assert_ne!(ColumnDependentKind::Index, ColumnDependentKind::Policy);
}

#[test]
fn test_kind_is_copy() {
    let kind = ColumnDependentKind::Index;
    let copied = kind;
    assert_eq!(kind, copied);
}

#[test]
fn test_kind_round_trips_through_serde_json() {
    for variant in [
        ColumnDependentKind::Index,
        ColumnDependentKind::Constraint,
        ColumnDependentKind::Policy,
    ] {
        let json = serde_json::to_string(&variant).expect("serialization should succeed");
        let deserialized: ColumnDependentKind =
            serde_json::from_str(&json).expect("deserialization should succeed");
        assert_eq!(deserialized, variant);
    }
}

#[test]
fn test_kind_serializes_as_variant_name() {
    assert_eq!(
        serde_json::to_string(&ColumnDependentKind::Index).unwrap(),
        "\"Index\""
    );
    assert_eq!(
        serde_json::to_string(&ColumnDependentKind::Constraint).unwrap(),
        "\"Constraint\""
    );
    assert_eq!(
        serde_json::to_string(&ColumnDependentKind::Policy).unwrap(),
        "\"Policy\""
    );
}

#[test]
fn test_kind_deserialize_unknown_variant_fails() {
    let result: Result<ColumnDependentKind, _> = serde_json::from_str("\"Trigger\"");
    assert!(result.is_err(), "unknown variant must fail to deserialize");
}

#[test]
fn test_dependent_round_trips_through_serde_json() {
    let value = sample(ColumnDependentKind::Constraint);

    let json = serde_json::to_string(&value).expect("serialization should succeed");
    let deserialized: ColumnDependent =
        serde_json::from_str(&json).expect("deserialization should succeed");

    assert_eq!(deserialized.schema, value.schema);
    assert_eq!(deserialized.table, value.table);
    assert_eq!(deserialized.column, value.column);
    assert_eq!(deserialized.kind, value.kind);
    assert_eq!(deserialized.dep_schema, value.dep_schema);
    assert_eq!(deserialized.dep_table, value.dep_table);
    assert_eq!(deserialized.dep_name, value.dep_name);
}

#[test]
fn test_dependent_serialized_keys_match_struct_fields() {
    let json = serde_json::to_string(&sample(ColumnDependentKind::Policy))
        .expect("serialization should succeed");

    for key in [
        "\"schema\":",
        "\"table\":",
        "\"column\":",
        "\"kind\":",
        "\"dep_schema\":",
        "\"dep_table\":",
        "\"dep_name\":",
    ] {
        assert!(json.contains(key), "missing key {} in {}", key, json);
    }
}

#[test]
fn test_dependent_deserialize_ignores_unknown_fields() {
    let json = r#"{
        "schema": "public",
        "table": "orders",
        "column": "status",
        "kind": "Index",
        "dep_schema": "public",
        "dep_table": "orders",
        "dep_name": "idx_orders_status",
        "extra": "ignored"
    }"#;

    let value: ColumnDependent =
        serde_json::from_str(json).expect("unknown fields must be ignored");

    assert_eq!(value.kind, ColumnDependentKind::Index);
    assert_eq!(value.dep_name, "idx_orders_status");
}

#[test]
fn test_dependent_deserialize_missing_field_fails() {
    let json = r#"{
        "schema": "public",
        "table": "orders",
        "column": "status",
        "kind": "Index",
        "dep_schema": "public",
        "dep_table": "orders"
    }"#;

    let result: Result<ColumnDependent, _> = serde_json::from_str(json);
    assert!(result.is_err(), "missing dep_name must fail to deserialize");
}

#[test]
fn test_dependent_clone_preserves_fields() {
    let original = sample(ColumnDependentKind::Index);
    let cloned = original.clone();

    assert_eq!(cloned.schema, original.schema);
    assert_eq!(cloned.table, original.table);
    assert_eq!(cloned.column, original.column);
    assert_eq!(cloned.kind, original.kind);
    assert_eq!(cloned.dep_schema, original.dep_schema);
    assert_eq!(cloned.dep_table, original.dep_table);
    assert_eq!(cloned.dep_name, original.dep_name);
}
