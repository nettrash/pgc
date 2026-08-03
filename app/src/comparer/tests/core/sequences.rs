//! Tests for `compare_sequences`, including the serial- and identity-owned
//! sequences that must not be emitted independently of their table.

use crate::comparer::core::*;
use super::helpers::*;
use crate::config::dump_config::DumpConfig;
use crate::config::grants_mode::GrantsMode;
use crate::dump::sequence::Sequence;
use crate::dump::table::Table;
use crate::dump::table_column::TableColumn;

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
