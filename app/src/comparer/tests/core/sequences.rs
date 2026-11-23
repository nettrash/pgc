//! Tests for `compare_sequences`, including the serial- and identity-owned
//! sequences that must not be emitted independently of their table.

use super::helpers::int_column;
use crate::comparer::core::*;
use crate::config::dump_config::DumpConfig;
use crate::config::grants_mode::GrantsMode;
use crate::dump::sequence::Sequence;
use crate::dump::table::Table;
use crate::dump::table_column::TableColumn;

#[tokio::test]
async fn compare_sequences_skips_owned_by_serial_column() {
    let from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    // Sequence owned by table column, exactly as `bigserial` creates it: every
    // option at its default (a changed one could not be reproduced by the
    // pseudo-type, so the sequence would be created on its own).
    let sequence = Sequence::new(
        "public".to_string(),
        "test_id_seq".to_string(),
        "postgres".to_string(),
        "bigint".to_string(),
        Some(1),
        Some(1),
        Some(i64::MAX),
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
        generation_blocks_retype: false,
        generation_routine_dropped: false,
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

    // The identity sequence of the column: the dump marks it `is_identity`
    // from its `pg_depend` deptype 'i'.
    let mut sequence = Sequence::new(
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
    sequence.set_is_identity(true);
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
        generation_blocks_retype: false,
        generation_routine_dropped: false,
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
        generation_blocks_retype: false,
        generation_routine_dropped: false,
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

    // The identity sequence of the column: the dump marks it `is_identity`
    // from its `pg_depend` deptype 'i'.
    let mut sequence = Sequence::new(
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
    sequence.set_is_identity(true);
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
        generation_blocks_retype: false,
        generation_routine_dropped: false,
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
        generation_blocks_retype: false,
        generation_routine_dropped: false,
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

// ---- serial detection: only a genuine serial is folded into its column ----

/// A TO-only sequence owned by `app.<table>.<column>`, every option at the
/// default for `data_type`.
fn owned_sequence(name: &str, table: &str, column: &str, data_type: &str) -> Sequence {
    let max_value = match data_type {
        "smallint" => i64::from(i16::MAX),
        "integer" => i64::from(i32::MAX),
        _ => i64::MAX,
    };
    Sequence::new(
        "app".to_string(),
        name.to_string(),
        "postgres".to_string(),
        data_type.to_string(),
        Some(1),
        Some(1),
        Some(max_value),
        Some(1),
        false,
        Some(1),
        None,
        Some("app".to_string()),
        Some(table.to_string()),
        Some(column.to_string()),
    )
}

/// A `NOT NULL` `data_type` column `app.<table>.<column>` defaulting to
/// `nextval` of `app.<sequence>`.
fn nextval_column(table: &str, column: &str, data_type: &str, sequence: &str) -> TableColumn {
    let mut col = int_column("app", table, column, 1);
    col.data_type = data_type.to_string();
    col.is_nullable = false;
    col.column_default = Some(format!("nextval('app.{sequence}'::regclass)"));
    col
}

fn app_table(name: &str, columns: Vec<TableColumn>) -> Table {
    Table::new(
        "app".to_string(),
        name.to_string(),
        "app".to_string(),
        name.to_string(),
        "postgres".to_string(),
        None,
        columns,
        vec![],
        vec![],
        vec![],
        None,
    )
}

/// Runs the full comparison and returns the script.
async fn compare_script(from_dump: Dump, to_dump: Dump) -> (String, Comparer) {
    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    (comparer.get_script(), comparer)
}

fn position(script: &str, needle: &str) -> usize {
    script
        .find(needle)
        .unwrap_or_else(|| panic!("`{needle}` missing from:\n{script}"))
}

/// The explicit path: the sequence is created in the sequences section, the
/// table keeps the column's own type and default, and `OWNED BY` follows the
/// table.
fn assert_explicit_sequence(script: &str, sequence: &str, column_clause: &str, owned_by: &str) {
    assert!(
        !script.contains(&format!("Skipping sequence app.{sequence}")),
        "{sequence} must not be left to a serial column:\n{script}"
    );
    let create = position(script, &format!("create sequence app.{sequence} "));
    let column = position(script, column_clause);
    let owned = position(
        script,
        &format!("alter sequence app.{sequence} owned by {owned_by};"),
    );
    assert!(
        create < column && column < owned,
        "expected create sequence < column < owned by:\n{script}"
    );
}

/// PostgreSQL 19's `oid8` cannot be an identity column, so an owned sequence
/// is how it auto-numbers. It used to fall into the `_ => "serial"` arm and be
/// created as an integer column with an integer sequence.
#[tokio::test]
async fn owned_sequence_of_oid8_column_is_created_explicitly() {
    let from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());
    to_dump
        .sequences
        .push(owned_sequence("sert_id_seq", "sert", "id", "bigint"));
    to_dump.tables.push(app_table(
        "sert",
        vec![nextval_column("sert", "id", "oid8", "sert_id_seq")],
    ));

    let (script, comparer) = compare_script(from_dump, to_dump).await;

    assert_explicit_sequence(
        &script,
        "sert_id_seq",
        "id oid8 default nextval('app.sert_id_seq'::regclass) not null",
        "app.sert.id",
    );
    assert!(!script.contains("serial"), "{script}");
    assert!(comparer.serial_columns.is_empty());
}

#[tokio::test]
async fn owned_sequence_of_numeric_column_is_created_explicitly() {
    let from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());
    to_dump
        .sequences
        .push(owned_sequence("num_id_seq", "num", "id", "bigint"));
    // Unconstrained `numeric`: no precision or scale.
    let mut id = nextval_column("num", "id", "numeric", "num_id_seq");
    id.numeric_precision = None;
    id.numeric_scale = None;
    to_dump.tables.push(app_table("num", vec![id]));

    let (script, _) = compare_script(from_dump, to_dump).await;

    assert_explicit_sequence(
        &script,
        "num_id_seq",
        "id numeric default nextval('app.num_id_seq'::regclass) not null",
        "app.num.id",
    );
}

/// A custom name would come out as `<table>_<column>_seq` under `bigserial`,
/// leaving the default pointing at a sequence that never exists.
#[tokio::test]
async fn custom_named_owned_sequence_is_created_explicitly() {
    let from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());
    to_dump
        .sequences
        .push(owned_sequence("order_counter", "orders", "id", "bigint"));
    to_dump.tables.push(app_table(
        "orders",
        vec![nextval_column("orders", "id", "bigint", "order_counter")],
    ));

    let (script, _) = compare_script(from_dump, to_dump).await;

    assert_explicit_sequence(
        &script,
        "order_counter",
        "id bigint default nextval('app.order_counter'::regclass) not null",
        "app.orders.id",
    );
}

/// An integer column fed by a bigint sequence: `serial` would make the
/// sequence integer.
#[tokio::test]
async fn owned_sequence_of_other_type_is_created_explicitly() {
    let from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());
    to_dump
        .sequences
        .push(owned_sequence("mis_id_seq", "mis", "id", "bigint"));
    to_dump.tables.push(app_table(
        "mis",
        vec![nextval_column("mis", "id", "integer", "mis_id_seq")],
    ));

    let (script, _) = compare_script(from_dump, to_dump).await;

    assert_explicit_sequence(
        &script,
        "mis_id_seq",
        "id integer default nextval('app.mis_id_seq'::regclass) not null",
        "app.mis.id",
    );
}

/// The pseudo-type exists only in `CREATE TABLE`: a table that is already
/// there gets `ADD COLUMN ... DEFAULT nextval(...)`, which needs the sequence
/// to exist. It used to be skipped as "created by the column" and never made.
#[tokio::test]
async fn serial_column_added_to_existing_table_creates_its_sequence() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());
    let label = int_column("app", "items", "label", 1);
    from_dump
        .tables
        .push(app_table("items", vec![label.clone()]));

    let mut id = nextval_column("items", "id", "integer", "items_id_seq");
    id.ordinal_position = 2;
    to_dump
        .sequences
        .push(owned_sequence("items_id_seq", "items", "id", "integer"));
    to_dump.tables.push(app_table("items", vec![label, id]));

    let (script, comparer) = compare_script(from_dump, to_dump).await;

    assert_explicit_sequence(
        &script,
        "items_id_seq",
        "alter table app.items add column id integer default nextval('app.items_id_seq'::regclass) not null;",
        "app.items.id",
    );
    assert!(comparer.serial_columns.is_empty());
}

/// Same for an existing column that gains the default: `SET DEFAULT
/// nextval(...)` needs the sequence first.
#[tokio::test]
async fn existing_column_becoming_serial_creates_its_sequence() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());
    let mut from_id = int_column("app", "items", "id", 1);
    from_id.is_nullable = false;
    from_dump.tables.push(app_table("items", vec![from_id]));

    to_dump
        .sequences
        .push(owned_sequence("items_id_seq", "items", "id", "integer"));
    to_dump.tables.push(app_table(
        "items",
        vec![nextval_column("items", "id", "integer", "items_id_seq")],
    ));

    let (script, _) = compare_script(from_dump, to_dump).await;

    assert_explicit_sequence(
        &script,
        "items_id_seq",
        "alter table app.items alter column id set default nextval('app.items_id_seq'::regclass);",
        "app.items.id",
    );
}

/// A serial whose sequence was changed afterwards: the pseudo-type would bring
/// back the defaults, and the next compare would alter them again.
#[tokio::test]
async fn tuned_serial_sequence_is_created_explicitly_with_its_options() {
    let from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());
    let mut sequence = owned_sequence("tuned_id_seq", "tuned", "id", "integer");
    sequence.increment_by = Some(10);
    sequence.hash();
    to_dump.sequences.push(sequence);
    to_dump.tables.push(app_table(
        "tuned",
        vec![nextval_column("tuned", "id", "integer", "tuned_id_seq")],
    ));

    let (script, _) = compare_script(from_dump, to_dump).await;

    assert_explicit_sequence(
        &script,
        "tuned_id_seq",
        "id integer default nextval('app.tuned_id_seq'::regclass) not null",
        "app.tuned.id",
    );
    assert!(script.contains(
        "create sequence app.tuned_id_seq as integer start with 1 increment by 10 minvalue 1 maxvalue 2147483647 no cycle;"
    ));
}

/// OWNED BY without a nextval default was silently dropped: the sequence was
/// created, but nothing linked it to its column (and the hash, which ignores
/// ownership, never noticed).
#[tokio::test]
async fn owned_sequence_without_nextval_default_gets_owned_by() {
    let from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());
    to_dump
        .sequences
        .push(owned_sequence("plain_seq", "plain", "id", "bigint"));
    to_dump.tables.push(app_table(
        "plain",
        vec![int_column("app", "plain", "id", 1)],
    ));

    let (script, _) = compare_script(from_dump, to_dump).await;

    assert_explicit_sequence(&script, "plain_seq", "id integer", "app.plain.id");
}

/// The ownership statements run after every table, between the sequences'
/// create section and their drop section.
#[tokio::test]
async fn owned_by_section_follows_tables_and_precedes_sequence_drops() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());
    from_dump.sequences.push(Sequence::new(
        "app".to_string(),
        "stale_seq".to_string(),
        "postgres".to_string(),
        "bigint".to_string(),
        Some(1),
        Some(1),
        Some(i64::MAX),
        Some(1),
        false,
        Some(1),
        None,
        None,
        None,
        None,
    ));
    to_dump
        .sequences
        .push(owned_sequence("order_counter", "orders", "id", "bigint"));
    to_dump.tables.push(app_table(
        "orders",
        vec![nextval_column("orders", "id", "bigint", "order_counter")],
    ));

    let (script, _) = compare_script(from_dump, to_dump).await;

    let table = position(&script, "create table app.orders");
    let section = position(
        &script,
        "Sequences: Ownership section (execute after owning tables)",
    );
    let owned = position(
        &script,
        "alter sequence app.order_counter owned by app.orders.id;",
    );
    let section_end = position(&script, "Sequences: Ownership section end");
    let drop = position(&script, "drop sequence if exists app.stale_seq;");
    assert!(table < section && section < owned && owned < section_end && section_end < drop);
}

/// Genuine serial / bigserial / smallserial columns of a new table keep the
/// pseudo-types and get no ownership statement: byte-for-byte the old output.
#[tokio::test]
async fn genuine_serial_columns_keep_pseudo_types_without_owned_by() {
    let from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());
    let mut columns = Vec::new();
    for (i, (column, data_type)) in [("a", "integer"), ("b", "bigint"), ("c", "smallint")]
        .into_iter()
        .enumerate()
    {
        let sequence = format!("gen_{column}_seq");
        to_dump
            .sequences
            .push(owned_sequence(&sequence, "gen", column, data_type));
        let mut col = nextval_column("gen", column, data_type, &sequence);
        col.ordinal_position = i as i32 + 1;
        columns.push(col);
    }
    to_dump.tables.push(app_table("gen", columns));

    let (script, comparer) = compare_script(from_dump, to_dump).await;

    assert!(
        script.contains(
            "create table app.gen (\n    a serial not null,\n    b bigserial not null,\n    c smallserial not null\n);"
        ),
        "{script}"
    );
    for column in ["a", "b", "c"] {
        assert!(script.contains(&format!(
            "/* Skipping sequence app.gen_{column}_seq as it will be created by column app.gen.{column} (identity/serial) */"
        )));
    }
    assert!(!script.contains("create sequence"), "{script}");
    assert!(!script.contains("owned by"), "{script}");
    assert!(!script.contains("Ownership section"), "{script}");
    assert_eq!(comparer.serial_columns.len(), 3);
}

/// One new table `app.ul` with a serial-shaped `id`, the table's and the
/// sequence's persistence as given, compared against a FROM server of
/// `from_version`; returns whether the pseudo-type was used.
async fn serial_used_for_persistence(
    table_unlogged: bool,
    sequence_unlogged: bool,
    from_version: Option<i32>,
) -> bool {
    let mut from_dump = Dump::new(DumpConfig::default());
    from_dump.server_version_num = from_version;
    let mut to_dump = Dump::new(DumpConfig::default());
    let mut sequence = owned_sequence("ul_id_seq", "ul", "id", "integer");
    sequence.is_unlogged = sequence_unlogged;
    sequence.hash();
    to_dump.sequences.push(sequence);
    let mut table = app_table(
        "ul",
        vec![nextval_column("ul", "id", "integer", "ul_id_seq")],
    );
    table.is_unlogged = table_unlogged;
    to_dump.tables.push(table);

    let (script, comparer) = compare_script(from_dump, to_dump).await;
    let used = script.contains("id serial not null");
    assert_eq!(used, !comparer.serial_columns.is_empty());
    assert_eq!(
        used,
        !script.contains("alter sequence app.ul_id_seq owned by app.ul.id;"),
        "{script}"
    );
    used
}

/// From PostgreSQL 15 a serial column's sequence takes its table's
/// persistence. A sequence whose persistence was changed on its own cannot
/// come from the pseudo-type, which would make the next compare flip it back.
#[tokio::test]
async fn serial_shortcut_requires_the_persistence_serial_produces() {
    // Logged table: the serial sequence is logged on every version.
    assert!(serial_used_for_persistence(false, false, Some(180000)).await);
    assert!(!serial_used_for_persistence(false, true, Some(180000)).await);
    assert!(!serial_used_for_persistence(false, true, None).await);
    // Unlogged table on PostgreSQL 15+: unlogged sequence.
    assert!(serial_used_for_persistence(true, true, Some(150000)).await);
    assert!(!serial_used_for_persistence(true, false, Some(180000)).await);
    // Unlogged table on PostgreSQL 14: sequences are always logged.
    assert!(serial_used_for_persistence(true, false, Some(140000)).await);
    // FROM dump without a server version (pgc 1.0.x): as before, not checked.
    assert!(serial_used_for_persistence(true, false, None).await);
    assert!(serial_used_for_persistence(true, true, None).await);
}

// ---- serial shortcut: only where the pseudo-type can create the sequence ----

/// `CREATE TABLE ... PARTITION OF` has no column types, so a serial column of
/// a partition is never rendered: the sequence used to be skipped as created
/// by the column, then created by the next compare, once the table existed.
#[tokio::test]
async fn serial_shaped_sequence_of_new_partition_is_created_explicitly() {
    let from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());
    let mut id = int_column("app", "p", "id", 1);
    id.data_type = "bigint".to_string();
    id.is_nullable = false;
    let mut parent = app_table("p", vec![id]);
    parent.partition_key = Some("RANGE (id)".to_string());
    to_dump.tables.push(parent);

    to_dump
        .sequences
        .push(owned_sequence("p1_id_seq", "p1", "id", "bigint"));
    let mut partition = app_table(
        "p1",
        vec![nextval_column("p1", "id", "bigint", "p1_id_seq")],
    );
    partition.partition_of = Some("app.p".to_string());
    partition.partition_bound = Some("FOR VALUES FROM (0) TO (10)".to_string());
    to_dump.tables.push(partition);

    let (script, comparer) = compare_script(from_dump, to_dump).await;

    assert_explicit_sequence(
        &script,
        "p1_id_seq",
        "create table app.p1 partition of app.p",
        "app.p1.id",
    );
    assert!(comparer.serial_columns.is_empty());
}

/// `CREATE TABLE ... OF type` takes its column types from the type, so a
/// typed table cannot declare a serial column either.
#[tokio::test]
async fn serial_shaped_sequence_of_new_typed_table_is_created_explicitly() {
    let from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());
    to_dump
        .sequences
        .push(owned_sequence("tt_id_seq", "tt", "id", "integer"));
    let mut typed = app_table(
        "tt",
        vec![nextval_column("tt", "id", "integer", "tt_id_seq")],
    );
    typed.typed_table_type = Some("app.ty".to_string());
    to_dump.tables.push(typed);

    let (script, comparer) = compare_script(from_dump, to_dump).await;

    assert!(!script.contains("serial"), "{script}");
    assert!(comparer.serial_columns.is_empty());
    let create = position(&script, "create sequence app.tt_id_seq ");
    let table = position(&script, "create table app.tt of app.ty");
    let owned = position(&script, "alter sequence app.tt_id_seq owned by app.tt.id;");
    assert!(create < table && table < owned, "{script}");
}

/// Only an identity column's own identity sequence comes with the column. A
/// second, ordinary sequence owned by that column used to be skipped as
/// "created by column (identity/serial)" and was never created.
#[tokio::test]
async fn ordinary_sequence_owned_by_identity_column_is_created_explicitly() {
    let from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());
    let mut identity_sequence = owned_sequence("t_id_seq", "t", "id", "integer");
    identity_sequence.set_is_identity(true);
    to_dump.sequences.push(identity_sequence);
    to_dump
        .sequences
        .push(owned_sequence("extra", "t", "id", "bigint"));

    let mut id = int_column("app", "t", "id", 1);
    id.is_nullable = false;
    id.is_identity = true;
    id.identity_generation = Some("ALWAYS".to_string());
    to_dump.tables.push(app_table("t", vec![id]));

    let (script, comparer) = compare_script(from_dump, to_dump).await;

    assert!(
        script.contains(
            "Skipping sequence app.t_id_seq as it will be created by column app.t.id (identity/serial)"
        ),
        "{script}"
    );
    assert!(!script.contains("create sequence app.t_id_seq"), "{script}");
    assert_explicit_sequence(&script, "extra", "create table app.t (", "app.t.id");
    assert!(comparer.serial_columns.is_empty());
}

/// A new table whose default names another new table's serial sequence and
/// that is created first (new tables go in schema and name order) would fail
/// on a sequence that does not exist yet.
#[tokio::test]
async fn serial_sequence_named_by_another_new_table_is_created_explicitly() {
    for default in [
        "nextval('app.b_t_id_seq'::regclass)",
        "nextval('b_t_id_seq'::regclass)",
        "currval('app.b_t_id_seq'::regclass)",
    ] {
        let from_dump = Dump::new(DumpConfig::default());
        let mut to_dump = Dump::new(DumpConfig::default());
        to_dump
            .sequences
            .push(owned_sequence("b_t_id_seq", "b_t", "id", "integer"));
        to_dump.tables.push(app_table(
            "b_t",
            vec![nextval_column("b_t", "id", "integer", "b_t_id_seq")],
        ));
        let mut other = int_column("app", "a_t", "id", 1);
        other.column_default = Some(default.to_string());
        to_dump.tables.push(app_table("a_t", vec![other]));

        let (script, comparer) = compare_script(from_dump, to_dump).await;

        assert_explicit_sequence(
            &script,
            "b_t_id_seq",
            "id integer default nextval('app.b_t_id_seq'::regclass) not null",
            "app.b_t.id",
        );
        assert!(
            position(&script, "create sequence app.b_t_id_seq ")
                < position(&script, "create table app.a_t"),
            "{script}"
        );
        assert!(comparer.serial_columns.is_empty(), "{default}");
    }
}

/// A new table created after the owner may name its sequence: the output
/// keeps the pseudo-type, byte for byte as before.
#[tokio::test]
async fn serial_sequence_named_by_later_new_table_keeps_pseudo_type() {
    let from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());
    to_dump
        .sequences
        .push(owned_sequence("b_t_id_seq", "b_t", "id", "integer"));
    to_dump.tables.push(app_table(
        "b_t",
        vec![nextval_column("b_t", "id", "integer", "b_t_id_seq")],
    ));
    let mut other = int_column("app", "z_t", "id", 1);
    other.column_default = Some("nextval('app.b_t_id_seq'::regclass)".to_string());
    to_dump.tables.push(app_table("z_t", vec![other]));

    let (script, comparer) = compare_script(from_dump, to_dump).await;

    assert!(
        script.contains("create table app.b_t (\n    id serial not null\n);"),
        "{script}"
    );
    assert!(!script.contains("create sequence"), "{script}");
    assert!(!script.contains("owned by"), "{script}");
    assert_eq!(comparer.serial_columns.len(), 1);
}

/// What does not stop the shortcut: another column of the owning table
/// (`CREATE TABLE` creates the serial sequence before the table), the owner's
/// partitions at any depth (created after it by partition depth, and carrying
/// its default), and a table already in FROM (altered after every new table
/// is created). The output stays the pseudo-type.
#[tokio::test]
async fn serial_sequence_named_only_by_its_family_or_existing_tables_keeps_pseudo_type() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());
    let named = |table: &str, column: &str, position: i32| {
        let mut col = nextval_column(table, column, "integer", "p_id_seq");
        col.ordinal_position = position;
        col
    };
    to_dump
        .sequences
        .push(owned_sequence("p_id_seq", "p", "id", "integer"));
    let mut copy = named("p", "copy", 2);
    copy.is_nullable = true;
    let mut parent = app_table("p", vec![named("p", "id", 1), copy]);
    parent.partition_key = Some("RANGE (id)".to_string());
    to_dump.tables.push(parent);

    // `a_*` sort before the owner by name; partition depth puts them after.
    let mut partition = app_table("a_part", vec![named("a_part", "id", 1)]);
    partition.partition_of = Some("app.p".to_string());
    partition.partition_bound = Some("FOR VALUES FROM (0) TO (10)".to_string());
    partition.partition_key = Some("RANGE (id)".to_string());
    to_dump.tables.push(partition);
    let mut sub_partition = app_table("a_sub", vec![named("a_sub", "id", 1)]);
    sub_partition.partition_of = Some("\"app\".\"a_part\"".to_string());
    sub_partition.partition_bound = Some("FOR VALUES FROM (0) TO (5)".to_string());
    to_dump.tables.push(sub_partition);

    let old = int_column("app", "a_old", "x", 1);
    from_dump.tables.push(app_table("a_old", vec![old]));
    to_dump
        .tables
        .push(app_table("a_old", vec![named("a_old", "x", 1)]));

    let (script, comparer) = compare_script(from_dump, to_dump).await;

    assert!(
        script.contains("create table app.p (\n    id serial not null,\n    copy integer default nextval('app.p_id_seq'::regclass)\n)"),
        "{script}"
    );
    assert!(!script.contains("create sequence"), "{script}");
    assert!(!script.contains("owned by"), "{script}");
    assert_eq!(comparer.serial_columns.len(), 1);
}

/// Classical inheritance: the children of a serial column's table repeat its
/// default and are created after it, whatever their names.
#[tokio::test]
async fn serial_sequence_named_by_inheritance_children_keeps_pseudo_type() {
    let from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());
    to_dump
        .sequences
        .push(owned_sequence("par_id_seq", "par", "id", "integer"));
    to_dump.tables.push(app_table(
        "par",
        vec![nextval_column("par", "id", "integer", "par_id_seq")],
    ));
    let mut child = app_table(
        "a_child",
        vec![nextval_column("a_child", "id", "integer", "par_id_seq")],
    );
    child.inherits_from = vec!["app.par".to_string()];
    to_dump.tables.push(child);
    let mut grandchild = app_table(
        "a_grandchild",
        vec![nextval_column(
            "a_grandchild",
            "id",
            "integer",
            "par_id_seq",
        )],
    );
    grandchild.inherits_from = vec!["app.a_child".to_string()];
    to_dump.tables.push(grandchild);

    let (script, comparer) = compare_script(from_dump, to_dump).await;

    assert!(script.contains("    id serial not null"), "{script}");
    assert!(!script.contains("create sequence"), "{script}");
    assert_eq!(comparer.serial_columns.len(), 1);
}

/// The drop side of the same rule: only the identity sequence goes with its
/// column's identity. An ordinary sequence owned by an identity column that
/// stays in TO used to be skipped as "owned by identity column" and was never
/// dropped.
#[tokio::test]
async fn ordinary_sequence_owned_by_identity_column_is_dropped() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());
    let mut identity_sequence = owned_sequence("t_id_seq", "t", "id", "integer");
    identity_sequence.set_is_identity(true);
    from_dump.sequences.push(identity_sequence.clone());
    to_dump.sequences.push(identity_sequence);
    from_dump
        .sequences
        .push(owned_sequence("extra", "t", "id", "bigint"));

    let mut id = int_column("app", "t", "id", 1);
    id.is_nullable = false;
    id.is_identity = true;
    id.identity_generation = Some("ALWAYS".to_string());
    from_dump.tables.push(app_table("t", vec![id.clone()]));
    to_dump.tables.push(app_table("t", vec![id]));

    let (script, _) = compare_script(from_dump, to_dump).await;

    assert!(
        script.contains("drop sequence if exists app.extra;"),
        "{script}"
    );
    assert!(
        !script.contains("Skipping drop of sequence app.extra"),
        "{script}"
    );
    assert!(!script.contains("app.t_id_seq;"), "{script}");
}

// ---- r3:types — sequence data type and linked-sequence owners ----

/// `app.t` with a `serial`-shaped `id` of `data_type` owned by `owner`, and
/// its sequence.
fn serial_table(data_type: &str, owner: &str) -> (Table, Sequence) {
    let mut table = app_table("t", vec![nextval_column("t", "id", data_type, "t_id_seq")]);
    table.owner = owner.to_string();
    table.hash();
    let mut sequence = owned_sequence("t_id_seq", "t", "id", data_type);
    sequence.owner = owner.to_string();
    sequence.hash();
    (table, sequence)
}

#[tokio::test]
async fn serial_to_bigserial_retypes_the_sequence_before_the_column() {
    let (from_table, from_sequence) = serial_table("integer", "postgres");
    let (to_table, to_sequence) = serial_table("bigint", "postgres");
    let mut from_dump = Dump::new(DumpConfig::default());
    from_dump.tables.push(from_table);
    from_dump.sequences.push(from_sequence);
    let mut to_dump = Dump::new(DumpConfig::default());
    to_dump.tables.push(to_table);
    to_dump.sequences.push(to_sequence);

    let (script, _) = compare_script(from_dump, to_dump).await;

    let sequence = position(
        &script,
        "alter sequence app.t_id_seq as bigint start with 1 increment by 1 minvalue 1 maxvalue 9223372036854775807 ",
    );
    let column = position(&script, "alter table app.t alter column id type bigint;");
    assert!(sequence < column, "{script}");
}

#[tokio::test]
async fn table_owner_change_carries_its_serial_and_identity_sequences() {
    let (from_table, from_sequence) = serial_table("integer", "postgres");
    let (to_table, to_sequence) = serial_table("integer", "app_owner");

    let mut identity_from = owned_sequence("i_id_seq", "i", "id", "integer");
    identity_from.set_is_identity(true);
    let mut identity_to = identity_from.clone();
    identity_to.owner = "app_owner".to_string();
    identity_to.hash();
    let mut id = int_column("app", "i", "id", 1);
    id.is_nullable = false;
    id.is_identity = true;
    id.identity_generation = Some("ALWAYS".to_string());
    let identity_table_from = app_table("i", vec![id]);
    let mut identity_table_to = identity_table_from.clone();
    identity_table_to.owner = "app_owner".to_string();
    identity_table_to.hash();

    let mut from_dump = Dump::new(DumpConfig::default());
    from_dump.tables.extend([from_table, identity_table_from]);
    from_dump.sequences.extend([from_sequence, identity_from]);
    let mut to_dump = Dump::new(DumpConfig::default());
    to_dump.tables.extend([to_table, identity_table_to]);
    to_dump.sequences.extend([to_sequence, identity_to]);

    let (script, _) = compare_script(from_dump, to_dump).await;

    // PostgreSQL refuses a direct owner change of a linked sequence, and the
    // table's own OWNER TO changes it along.
    assert!(
        !script.contains("alter sequence app.t_id_seq owner to"),
        "{script}"
    );
    assert!(
        !script.contains("alter sequence app.i_id_seq owner to"),
        "{script}"
    );
    position(&script, "alter table app.t owner to app_owner;");
    position(&script, "alter table app.i owner to app_owner;");
}

#[tokio::test]
async fn sequence_linked_to_a_table_whose_owner_changes_gets_owned_by_after_it() {
    let mut standalone = owned_sequence("lone", "t", "id", "bigint");
    standalone.owned_by_schema = None;
    standalone.owned_by_table = None;
    standalone.owned_by_column = None;
    standalone.hash();
    let mut linked = owned_sequence("lone", "t", "id", "bigint");
    linked.owner = "app_owner".to_string();
    linked.hash();
    let from_table = app_table("t", vec![int_column("app", "t", "id", 1)]);
    let mut to_table = from_table.clone();
    to_table.owner = "app_owner".to_string();
    to_table.hash();

    let mut from_dump = Dump::new(DumpConfig::default());
    from_dump.tables.push(from_table);
    from_dump.sequences.push(standalone);
    let mut to_dump = Dump::new(DumpConfig::default());
    to_dump.tables.push(to_table);
    to_dump.sequences.push(linked);

    let (script, _) = compare_script(from_dump, to_dump).await;

    let owner = position(&script, "alter sequence app.lone owner to app_owner;");
    let table_owner = position(&script, "alter table app.t owner to app_owner;");
    let owned_by = position(&script, "alter sequence app.lone owned by app.t.id;");
    assert!(owner < table_owner && table_owner < owned_by, "{script}");
    assert_eq!(script.matches("owned by app.t.id").count(), 1, "{script}");
}

/// Review finding: a sequence `OWNED BY app.a.id` in FROM and `OWNED BY
/// app.t.id` in TO, where `app.t` and the sequence get a new owner. It is
/// unlinked first (a direct `OWNER TO` of a sequence linked to `app.a` is
/// refused), takes its owner, and is linked to `app.t` once the table has
/// that owner too (`OWNED BY` needs one owner).
#[tokio::test]
async fn sequence_moving_to_a_table_whose_owner_changes_is_unlinked_first() {
    let mut from_sequence = owned_sequence("mv_seq", "a", "id", "bigint");
    from_sequence.hash();
    let mut to_sequence = owned_sequence("mv_seq", "t", "id", "bigint");
    to_sequence.owner = "app_owner".to_string();
    to_sequence.hash();
    let table_a = app_table("a", vec![int_column("app", "a", "id", 1)]);
    let from_t = app_table("t", vec![int_column("app", "t", "id", 1)]);
    let mut to_t = from_t.clone();
    to_t.owner = "app_owner".to_string();
    to_t.hash();

    let mut from_dump = Dump::new(DumpConfig::default());
    from_dump.tables.extend([table_a.clone(), from_t]);
    from_dump.sequences.push(from_sequence);
    let mut to_dump = Dump::new(DumpConfig::default());
    to_dump.tables.extend([table_a, to_t]);
    to_dump.sequences.push(to_sequence);

    let (script, _) = compare_script(from_dump, to_dump).await;

    let unlink = position(&script, "no cycle owned by none;");
    let owner = position(&script, "alter sequence app.mv_seq owner to app_owner;");
    let table_owner = position(&script, "alter table app.t owner to app_owner;");
    let owned_by = position(&script, "alter sequence app.mv_seq owned by app.t.id;");
    assert!(
        unlink < owner && owner < table_owner && table_owner < owned_by,
        "{script}"
    );
    assert_eq!(script.matches("owned by app.t.id").count(), 1, "{script}");
}

// ---- integration: identity sequence names that drifted ----

/// `app.t (y integer GENERATED ALWAYS AS IDENTITY)` with its identity
/// sequence `seq_name`, incrementing by `increment`, granting `acl`.
fn identity_dump(seq_name: &str, increment: i64, acl: &[&str]) -> Dump {
    let mut dump = Dump::new(DumpConfig::default());
    let mut sequence = owned_sequence(seq_name, "t", "y", "integer");
    sequence.increment_by = Some(increment);
    sequence.set_is_identity(true);
    sequence.acl = acl.iter().map(|a| a.to_string()).collect();
    sequence.hash();
    dump.sequences.push(sequence);
    let mut y = int_column("app", "t", "y", 1);
    y.is_nullable = false;
    y.is_identity = true;
    y.identity_generation = Some("ALWAYS".to_string());
    let mut table = app_table("t", vec![y]);
    table.hash();
    dump.tables.push(table);
    dump
}

/// PostgreSQL keeps an identity sequence's name when its column is renamed,
/// so FROM's `t_x_seq` and TO's `t_y_seq` are one sequence. It is renamed,
/// then altered like any sequence both sides have; before, the two were
/// never compared, and a change TO made to it was lost.
#[tokio::test]
async fn identity_sequence_whose_name_drifted_is_renamed_and_altered() {
    let from = identity_dump("t_x_seq", 1, &[]);
    let to = identity_dump("t_y_seq", 5, &[]);
    let (script, comparer) = compare_script(from, to).await;

    let rename = position(
        &script,
        "alter sequence if exists app.t_x_seq rename to t_y_seq;",
    );
    let alter = position(&script, "alter sequence app.t_y_seq");
    assert!(rename < alter, "{script}");
    assert!(script[alter..].contains("increment by 5"), "{script}");
    assert!(
        !script.contains("Skipping sequence app.t_y_seq"),
        "{script}"
    );
    assert!(
        !script.contains("Skipping drop of sequence app.t_x_seq"),
        "{script}"
    );
    assert!(!script.contains("drop sequence"), "{script}");
    assert_eq!(
        comparer
            .renamed_identity_sequences
            .get(&("app".to_string(), "t_y_seq".to_string())),
        Some(&("app".to_string(), "t_x_seq".to_string()))
    );

    // Nothing else differs: the rename alone.
    let (script, _) = compare_script(
        identity_dump("t_x_seq", 1, &[]),
        identity_dump("t_y_seq", 1, &[]),
    )
    .await;
    assert!(
        script.contains("alter sequence if exists app.t_x_seq rename to t_y_seq;"),
        "{script}"
    );
    assert!(!script.contains("alter sequence app.t_y_seq"), "{script}");
}

/// No rename when FROM has a relation of TO's name already, nor when the
/// column is no identity column on one side, nor when the names match.
#[tokio::test]
async fn identity_sequence_rename_needs_the_same_identity_column_and_a_free_name() {
    let mut from = identity_dump("t_x_seq", 1, &[]);
    from.tables.push(app_table(
        "t_y_seq",
        vec![int_column("app", "t_y_seq", "id", 1)],
    ));
    let mut to = identity_dump("t_y_seq", 1, &[]);
    to.tables.push(app_table(
        "t_y_seq",
        vec![int_column("app", "t_y_seq", "id", 1)],
    ));
    let (script, comparer) = compare_script(from, to).await;
    assert!(!script.contains("rename to t_y_seq"), "{script}");
    assert!(comparer.renamed_identity_sequences.is_empty());

    let from = identity_dump("t_x_seq", 1, &[]);
    let mut to = identity_dump("t_y_seq", 1, &[]);
    to.tables[0].columns[0].is_identity = false;
    to.tables[0].columns[0].identity_generation = None;
    to.tables[0].hash();
    let (script, _) = compare_script(from, to).await;
    assert!(!script.contains("rename to t_y_seq"), "{script}");

    let (script, comparer) = compare_script(
        identity_dump("t_y_seq", 1, &[]),
        identity_dump("t_y_seq", 1, &[]),
    )
    .await;
    assert!(!script.contains("rename to"), "{script}");
    assert!(comparer.renamed_identity_sequences.is_empty());
}

/// The renamed sequence keeps its FROM privileges, so `compare_grants`
/// compares them with TO's under the new name: full mode revokes what TO
/// no longer grants, where it used to plan the sequence as a new one.
#[tokio::test]
async fn identity_sequence_rename_carries_its_privileges_into_the_grants() {
    let from = identity_dump("t_x_seq", 1, &["app=U/postgres", "old_svc=U/postgres"]);
    let to = identity_dump("t_y_seq", 1, &["app=U/postgres"]);
    let mut comparer = Comparer::new(from, to, true, false, true, GrantsMode::Full);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();
    assert!(
        script.contains("REVOKE USAGE ON SEQUENCE app.t_y_seq FROM old_svc;"),
        "{script}"
    );
    assert!(
        !script.contains("GRANT USAGE ON SEQUENCE app.t_y_seq TO app;"),
        "{script}"
    );
}
