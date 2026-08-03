use super::*;

fn sample() -> PgEnum {
    PgEnum {
        oid: Oid(16400),
        enumtypid: Oid(16399),
        enumsortorder: 1.0,
        enumlabel: "active".to_string(),
    }
}

#[test]
fn test_fields_round_trip_through_serde_json() {
    let value = sample();

    let json = serde_json::to_string(&value).expect("serialization should succeed");
    let deserialized: PgEnum = serde_json::from_str(&json).expect("deserialization should succeed");

    assert_eq!(deserialized.oid, value.oid);
    assert_eq!(deserialized.enumtypid, value.enumtypid);
    assert_eq!(deserialized.enumsortorder, value.enumsortorder);
    assert_eq!(deserialized.enumlabel, value.enumlabel);
}

#[test]
fn test_serialized_keys_match_struct_fields() {
    let json = serde_json::to_string(&sample()).expect("serialization should succeed");

    assert!(json.contains("\"oid\":"));
    assert!(json.contains("\"enumtypid\":"));
    assert!(json.contains("\"enumsortorder\":"));
    assert!(json.contains("\"enumlabel\":\"active\""));
}

#[test]
fn test_deserialize_ignores_unknown_fields() {
    let json = r#"{
        "oid": 16400,
        "enumtypid": 16399,
        "enumsortorder": 2.5,
        "enumlabel": "pending",
        "extra": "ignored"
    }"#;

    let value: PgEnum = serde_json::from_str(json).expect("unknown fields must be ignored");

    assert_eq!(value.oid, Oid(16400));
    assert_eq!(value.enumtypid, Oid(16399));
    assert_eq!(value.enumsortorder, 2.5);
    assert_eq!(value.enumlabel, "pending");
}

#[test]
fn test_deserialize_missing_field_fails() {
    let json = r#"{"oid": 16400, "enumtypid": 16399, "enumsortorder": 1.0}"#;
    let result: Result<PgEnum, _> = serde_json::from_str(json);
    assert!(
        result.is_err(),
        "missing enumlabel should fail to deserialize"
    );
}

#[test]
fn test_clone_preserves_fields() {
    let original = sample();
    let cloned = original.clone();

    assert_eq!(cloned.oid, original.oid);
    assert_eq!(cloned.enumtypid, original.enumtypid);
    assert_eq!(cloned.enumsortorder, original.enumsortorder);
    assert_eq!(cloned.enumlabel, original.enumlabel);
}

#[test]
fn test_fractional_sort_order_round_trips() {
    let value = PgEnum {
        oid: Oid(1),
        enumtypid: Oid(2),
        enumsortorder: 1.5,
        enumlabel: "between".to_string(),
    };

    let json = serde_json::to_string(&value).expect("serialization should succeed");
    let deserialized: PgEnum = serde_json::from_str(&json).expect("deserialization should succeed");

    assert_eq!(deserialized.enumsortorder, 1.5);
}
