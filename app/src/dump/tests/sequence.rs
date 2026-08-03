use super::*;

fn build_sequence() -> Sequence {
    Sequence::new(
        "public".to_string(),
        "order_id_seq".to_string(),
        "postgres".to_string(),
        "bigint".to_string(),
        Some(1),
        Some(1),
        Some(1000),
        Some(5),
        true,
        Some(20),
        Some(42),
        Some("public".to_string()),
        Some("orders".to_string()),
        Some("id".to_string()),
    )
}

#[test]
fn test_sequence_new_initializes_hash() {
    let sequence = build_sequence();

    let mut hasher = Sha256::new();
    hasher.update("public".as_bytes());
    hasher.update("order_id_seq".as_bytes());
    hasher.update("postgres".as_bytes());
    hasher.update("bigint".as_bytes());
    hasher.update("1".as_bytes());
    hasher.update("1".as_bytes());
    hasher.update("1000".as_bytes());
    hasher.update("5".as_bytes());
    hasher.update("true".as_bytes());
    hasher.update("20".as_bytes());
    hasher.update("false".as_bytes()); // is_identity
    hasher.update("false".as_bytes()); // is_unlogged

    let expected_hash = format!("{:x}", hasher.finalize());

    assert_eq!(sequence.hash.as_deref(), Some(expected_hash.as_str()));
}

#[test]
fn test_get_script_with_all_options() {
    let sequence = build_sequence();

    let script = sequence.get_script();

    assert_eq!(
        script,
        "create sequence public.order_id_seq start with 1 increment by 5 minvalue 1 maxvalue 1000 cache 20 cycle;\n\nalter sequence public.order_id_seq owner to postgres;\n\n",
    );
}

#[test]
fn test_get_script_uses_defaults() {
    let sequence = Sequence::new(
        "public".to_string(),
        "minimal_seq".to_string(),
        "postgres".to_string(),
        "bigint".to_string(),
        None,
        None,
        None,
        None,
        false,
        None,
        None,
        None,
        None,
        None,
    );

    assert_eq!(
        sequence.get_script(),
        "create sequence public.minimal_seq no minvalue no maxvalue no cycle;\n\nalter sequence public.minimal_seq owner to postgres;\n\n",
    );
}

#[test]
fn test_get_drop_script() {
    let sequence = build_sequence();
    assert_eq!(
        sequence.get_drop_script(),
        "drop sequence if exists public.order_id_seq;\n\n"
    );
}

#[test]
fn test_get_alter_script_includes_owned_by() {
    let sequence = build_sequence();

    // start_value unchanged (from == to == 1): no RESTART WITH expected.
    assert_eq!(
        sequence.get_alter_script(&sequence.clone()),
        "alter sequence public.order_id_seq start with 1 increment by 5 minvalue 1 maxvalue 1000 cache 20 cycle owned by public.orders.id;\n\nalter sequence public.order_id_seq owner to postgres;\n\n",
    );
}

#[test]
fn test_get_alter_script_escapes_owned_by_identifiers() {
    let sequence = Sequence::new(
        "audit".to_string(),
        "event_seq".to_string(),
        "postgres".to_string(),
        "bigint".to_string(),
        Some(10),
        None,
        None,
        Some(2),
        false,
        None,
        None,
        Some("\"my\"\"schema\"".to_string()),
        Some("\"my.table\"".to_string()),
        Some("column".to_string()),
    );

    // start_value unchanged (from == to == 10): no RESTART WITH expected.
    assert_eq!(
        sequence.get_alter_script(&sequence.clone()),
        "alter sequence audit.event_seq start with 10 increment by 2 no minvalue no maxvalue no cycle owned by \"my\"\"schema\".\"my.table\".column;\n\nalter sequence audit.event_seq owner to postgres;\n\n",
    );
}

/// Regression test: when MINVALUE is raised above the sequence's previously recorded start
/// value (e.g. 1), an ALTER SEQUENCE without RESTART WITH causes PostgreSQL to implicitly
/// restart from the old start value, which violates the new MINVALUE constraint:
///
///   ERROR: RESTART value (1) cannot be less than MINVALUE (10000000)
///
/// The generated script must include an explicit RESTART WITH equal to START WITH so that
/// the current sequence position is repositioned to the new value before the constraint
/// check is applied.
#[test]
fn test_get_alter_script_restart_with_matches_start_with_when_minvalue_raised() {
    let from_sequence = Sequence::new(
        "my_schema".to_string(),
        "my_sequence_id_seq".to_string(),
        "postgres".to_string(),
        "bigint".to_string(),
        Some(1), // old start_value
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
        Some(10_000_000), // new start_value — also serves as the new MINVALUE
        Some(10_000_000), // MINVALUE raised well above the old start value of 1
        Some(999_999_999),
        Some(1),
        true,
        Some(1),
        None,
        None,
        None,
        None,
    );

    let script = to_sequence.get_alter_script(&from_sequence);

    // Both clauses must appear so that PostgreSQL does not fall back to the old start value.
    assert!(
        script.contains("start with 10000000"),
        "script must contain START WITH: {script}"
    );
    assert!(
        script.contains("restart with 10000000"),
        "script must contain RESTART WITH to avoid RESTART value < MINVALUE error: {script}"
    );
    assert_eq!(
        script,
        "alter sequence my_schema.my_sequence_id_seq start with 10000000 restart with 10000000 increment by 1 minvalue 10000000 maxvalue 999999999 cache 1 cycle;\n\nalter sequence my_schema.my_sequence_id_seq owner to postgres;\n\n",
    );
}

/// RESTART WITH must NOT be emitted when the sequence's effective current position
/// (last_value) is already above the new MINVALUE — even when start_value and MINVALUE are
/// both raised.  Emitting it would rewind the live sequence from its current position back
/// to the new start_value, risking duplicate-key violations.
#[test]
fn test_get_alter_script_no_restart_when_last_value_above_new_minvalue() {
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
        Some(15_000_000), // sequence is already well above the new MINVALUE
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
        Some(10_000_000), // MINVALUE raised — but last_value (15M) is already above it
        Some(999_999_999),
        Some(1),
        false,
        Some(1),
        None,
        None,
        None,
        None,
    );

    let script = to_sequence.get_alter_script(&from_sequence);

    assert!(
        !script.contains("restart with"),
        "script must NOT contain RESTART WITH when last_value ({}) is already above new MINVALUE (10000000): {script}",
        15_000_000
    );
    assert!(
        script.contains("start with 10000000"),
        "script must still update START WITH: {script}"
    );
}

/// When only non-start/minvalue parameters change (e.g. cycle) and the effective current
/// position is already within the new bounds, RESTART WITH must NOT be emitted.
#[test]
fn test_get_alter_script_no_restart_when_only_other_params_change() {
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
        Some(500_000), // current live position
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

    let script = to_sequence.get_alter_script(&from_sequence);

    assert!(
        !script.contains("restart with"),
        "script must NOT contain RESTART WITH when start_value is unchanged: {script}"
    );
}
