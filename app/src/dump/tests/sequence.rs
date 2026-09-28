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

// ---- serial detection: serial_pseudo_type / serial_sequence_name / OWNED BY ----

/// The sequence a `serial`-family column of `table.column` gets from
/// PostgreSQL: its generated name, the column's integer type, every option at
/// its default.
fn serial_sequence(table: &str, column: &str, data_type: &str, max_value: i64) -> Sequence {
    Sequence::new(
        "app".to_string(),
        format!("{table}_{column}_seq"),
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

/// A `NOT NULL` column of `table` defaulting to `nextval` of `sequence_name`
/// (schema-qualified), as `pg_get_expr` renders it.
fn nextval_column(table: &str, name: &str, data_type: &str, sequence_name: &str) -> TableColumn {
    TableColumn {
        catalog: "postgres".to_string(),
        schema: "app".to_string(),
        table: table.to_string(),
        name: name.to_string(),
        ordinal_position: 1,
        column_default: Some(format!("nextval('app.{sequence_name}'::regclass)")),
        is_nullable: false,
        data_type: data_type.to_string(),
        character_maximum_length: None,
        character_octet_length: None,
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
fn serial_sequence_name_joins_table_column_and_label() {
    assert_eq!(
        serial_sequence_name("orders", "id").as_deref(),
        Some("orders_id_seq")
    );
    assert_eq!(
        serial_sequence_name("My.Table", "Id").as_deref(),
        Some("My.Table_Id_seq")
    );
}

/// The expected names were read back from PostgreSQL 18 after
/// `CREATE TABLE ... (<column> serial)`.
#[test]
fn serial_sequence_name_truncates_like_make_object_name() {
    // Both too long: the longer is cut until the two are equal, then they are
    // cut in turn, the column first on a tie.
    assert_eq!(
        serial_sequence_name(
            "a_very_long_table_name_for_testing_serial_truncation_rules",
            "a_very_long_column_name_for_serial_truncation",
        )
        .as_deref(),
        Some("a_very_long_table_name_for_te_a_very_long_column_name_for_s_seq")
    );
    // Only the longer name loses bytes.
    assert_eq!(
        serial_sequence_name(
            "t_exactly",
            "c2345678901234567890123456789012345678901234567890123456789",
        )
        .as_deref(),
        Some("t_exactly_c234567890123456789012345678901234567890123456789_seq")
    );
    // Exactly 63 bytes: nothing is cut.
    let table = "t".repeat(29);
    let column = "c".repeat(29);
    let name = serial_sequence_name(&table, &column).unwrap();
    assert_eq!(name.len(), 63);
    assert_eq!(name, format!("{table}_{column}_seq"));
}

#[test]
fn serial_sequence_name_gives_up_on_non_ascii_truncation() {
    // PostgreSQL counts bytes in the server encoding, which the dump does not
    // record, so the cut point cannot be predicted.
    assert_eq!(
        serial_sequence_name(
            "Ünïcödé_täble_näme_thät_is_rëally_löng_indëëd",
            "cölumn_näme_älsö_löng_ënough",
        ),
        None
    );
    // Short non-ASCII names are never cut, so they are predictable.
    assert_eq!(
        serial_sequence_name("täble", "cölumn").as_deref(),
        Some("täble_cölumn_seq")
    );
}

#[test]
fn serial_pseudo_type_for_each_integer_type() {
    for (data_type, max_value, pseudo_type) in [
        ("smallint", i64::from(i16::MAX), "smallserial"),
        ("integer", i64::from(i32::MAX), "serial"),
        ("bigint", i64::MAX, "bigserial"),
    ] {
        let sequence = serial_sequence("orders", "id", data_type, max_value);
        let column = nextval_column("orders", "id", data_type, "orders_id_seq");
        assert_eq!(
            sequence.serial_pseudo_type(&column, Some(false)),
            Some(pseudo_type),
            "{data_type}"
        );
    }
}

#[test]
fn serial_pseudo_type_accepts_bare_default_and_quoted_names() {
    // `pg_get_expr` leaves out a schema on the search_path.
    let sequence = serial_sequence("orders", "id", "integer", i64::from(i32::MAX));
    let mut column = nextval_column("orders", "id", "integer", "orders_id_seq");
    column.column_default = Some("nextval('orders_id_seq'::regclass)".to_string());
    assert_eq!(
        sequence.serial_pseudo_type(&column, Some(false)),
        Some("serial")
    );

    // Mixed-case names arrive quote_ident-quoted.
    let mut sequence = serial_sequence("\"Orders\"", "\"Id\"", "integer", i64::from(i32::MAX));
    sequence.name = "\"Orders_Id_seq\"".to_string();
    let column = nextval_column("\"Orders\"", "\"Id\"", "integer", "\"Orders_Id_seq\"");
    assert_eq!(
        sequence.serial_pseudo_type(&column, Some(false)),
        Some("serial")
    );
}

#[test]
fn serial_pseudo_type_accepts_truncated_generated_name() {
    let table = "a_very_long_table_name_for_testing_serial_truncation_rules";
    let column_name = "a_very_long_column_name_for_serial_truncation";
    let seq_name = "a_very_long_table_name_for_te_a_very_long_column_name_for_s_seq";
    let mut sequence = serial_sequence(table, column_name, "integer", i64::from(i32::MAX));
    sequence.name = seq_name.to_string();
    let column = nextval_column(table, column_name, "integer", seq_name);
    assert_eq!(
        sequence.serial_pseudo_type(&column, Some(false)),
        Some("serial")
    );
}

#[test]
fn serial_pseudo_type_rejects_non_integer_columns() {
    // numeric, PostgreSQL 19's oid8 and a domain: `serial` would silently turn
    // the column into an integer (the old fallback arm did exactly that).
    for data_type in ["numeric", "oid8", "app.positive_id", "text"] {
        let sequence = serial_sequence("orders", "id", "bigint", i64::MAX);
        let column = nextval_column("orders", "id", data_type, "orders_id_seq");
        assert_eq!(
            sequence.serial_pseudo_type(&column, Some(false)),
            None,
            "{data_type}"
        );
    }
}

#[test]
fn serial_pseudo_type_rejects_sequence_of_another_type() {
    // An integer column fed by a bigint sequence: `serial` would create an
    // integer sequence.
    let sequence = serial_sequence("orders", "id", "bigint", i64::MAX);
    let column = nextval_column("orders", "id", "integer", "orders_id_seq");
    assert_eq!(sequence.serial_pseudo_type(&column, Some(false)), None);
}

#[test]
fn serial_pseudo_type_rejects_names_postgres_would_not_generate() {
    for name in [
        "order_counter",      // custom
        "orders_id_seq1",     // collision-suffixed
        "old_orders_id_seq",  // kept its name through a table rename
        "\"orders_id_seq\"x", // not a quote_ident rendering of the name
    ] {
        let mut sequence = serial_sequence("orders", "id", "integer", i64::from(i32::MAX));
        sequence.name = name.to_string();
        let column = nextval_column("orders", "id", "integer", name);
        assert_eq!(
            sequence.serial_pseudo_type(&column, Some(false)),
            None,
            "{name}"
        );
    }
}

#[test]
fn serial_pseudo_type_rejects_other_defaults() {
    let sequence = serial_sequence("orders", "id", "integer", i64::from(i32::MAX));
    for default in [
        None,
        Some("nextval('app.other_seq'::regclass)"),
        Some("(nextval('app.orders_id_seq'::regclass) * 2)"),
        Some("nextval('other.orders_id_seq'::regclass)"),
    ] {
        let mut column = nextval_column("orders", "id", "integer", "orders_id_seq");
        column.column_default = default.map(str::to_string);
        assert_eq!(
            sequence.serial_pseudo_type(&column, Some(false)),
            None,
            "{default:?}"
        );
    }
}

#[test]
fn serial_pseudo_type_rejects_what_serial_cannot_reproduce() {
    let column = nextval_column("orders", "id", "integer", "orders_id_seq");
    let base = || serial_sequence("orders", "id", "integer", i64::from(i32::MAX));
    assert_eq!(
        base().serial_pseudo_type(&column, Some(false)),
        Some("serial")
    );

    type Change = (&'static str, fn(&mut Sequence));
    let changes: [Change; 8] = [
        ("start", |s| s.start_value = Some(100)),
        ("increment", |s| s.increment_by = Some(10)),
        ("minvalue", |s| s.min_value = Some(0)),
        ("maxvalue", |s| s.max_value = Some(1000)),
        ("cache", |s| s.cache_size = Some(5)),
        ("cycle", |s| s.cycle = true),
        ("comment", |s| s.comment = Some("counter".to_string())),
        ("schema", |s| s.schema = "other".to_string()),
    ];
    for (what, change) in changes {
        let mut sequence = base();
        change(&mut sequence);
        assert_eq!(
            sequence.serial_pseudo_type(&column, Some(false)),
            None,
            "{what}"
        );
    }

    // A serial column is always NOT NULL.
    let mut nullable = column.clone();
    nullable.is_nullable = true;
    assert_eq!(base().serial_pseudo_type(&nullable, Some(false)), None);

    // Identity sequences and columns are PostgreSQL's own, not serial.
    let mut identity_sequence = base();
    identity_sequence.is_identity = true;
    assert_eq!(
        identity_sequence.serial_pseudo_type(&column, Some(false)),
        None
    );
    let mut identity_column = column.clone();
    identity_column.is_identity = true;
    assert_eq!(
        base().serial_pseudo_type(&identity_column, Some(false)),
        None
    );

    // Not owned by this column, or not owned at all.
    let other_column = nextval_column("orders", "other", "integer", "orders_id_seq");
    assert_eq!(base().serial_pseudo_type(&other_column, Some(false)), None);
    let mut unowned = base();
    unowned.owned_by_column = None;
    assert_eq!(unowned.serial_pseudo_type(&column, Some(false)), None);
}

#[test]
fn get_owned_by_script_links_the_owning_column() {
    let sequence = serial_sequence("orders", "id", "integer", i64::from(i32::MAX));
    assert_eq!(
        sequence.get_owned_by_script().as_deref(),
        Some("alter sequence app.orders_id_seq owned by app.orders.id;\n\n")
    );
}

#[test]
fn get_owned_by_script_skips_identity_and_unowned_sequences() {
    let mut identity = serial_sequence("orders", "id", "integer", i64::from(i32::MAX));
    identity.is_identity = true;
    assert_eq!(identity.get_owned_by_script(), None);

    let mut unowned = serial_sequence("orders", "id", "integer", i64::from(i32::MAX));
    unowned.owned_by_schema = None;
    unowned.owned_by_table = None;
    unowned.owned_by_column = None;
    assert_eq!(unowned.get_owned_by_script(), None);
}

#[test]
fn serial_pseudo_type_compares_persistence_when_known() {
    let column = nextval_column("orders", "id", "integer", "orders_id_seq");
    let logged = serial_sequence("orders", "id", "integer", i64::from(i32::MAX));
    let mut unlogged = logged.clone();
    unlogged.is_unlogged = true;

    // PostgreSQL 15+ unlogged table: its serial sequence is unlogged too.
    assert_eq!(
        unlogged.serial_pseudo_type(&column, Some(true)),
        Some("serial")
    );
    assert_eq!(logged.serial_pseudo_type(&column, Some(true)), None);
    // Logged table (or PostgreSQL 14): the serial sequence is logged.
    assert_eq!(
        logged.serial_pseudo_type(&column, Some(false)),
        Some("serial")
    );
    assert_eq!(unlogged.serial_pseudo_type(&column, Some(false)), None);
    // Unknown: persistence is not compared.
    assert_eq!(logged.serial_pseudo_type(&column, None), Some("serial"));
    assert_eq!(unlogged.serial_pseudo_type(&column, None), Some("serial"));
}

// ---- r3:types — data type changes and owners of linked sequences ----

/// A `serial` sequence of `app.t.id` retyped to `data_type`, with the bounds
/// of that type.
fn retyped(data_type: &str, max_value: i64) -> Sequence {
    let mut sequence = serial_sequence("t", "id", data_type, max_value);
    sequence.hash();
    sequence
}

#[test]
fn alter_script_changes_the_data_type_before_the_bounds() {
    let integer = retyped("integer", i64::from(i32::MAX));
    let bigint = retyped("bigint", i64::MAX);

    // serial -> bigserial: without `as bigint` PostgreSQL refuses the
    // MAXVALUE as out of range for integer.
    assert_eq!(
        bigint.get_alter_script(&integer),
        "alter sequence app.t_id_seq as bigint start with 1 increment by 1 minvalue 1 maxvalue 9223372036854775807 cache 1 no cycle owned by app.t.id;\n\n\
alter sequence app.t_id_seq owner to postgres;\n\n"
    );
    // bigserial -> serial: without `as integer` the sequence stays bigint.
    assert_eq!(
        integer.get_alter_script(&bigint),
        "alter sequence app.t_id_seq as integer start with 1 increment by 1 minvalue 1 maxvalue 2147483647 cache 1 no cycle owned by app.t.id;\n\n\
alter sequence app.t_id_seq owner to postgres;\n\n"
    );
    // The persistence-excluding form carries it too.
    assert!(
        bigint
            .get_alter_script_excluding_persistence(&integer)
            .starts_with("alter sequence app.t_id_seq as bigint start with 1")
    );
}

#[test]
fn alter_script_without_a_type_change_is_unchanged() {
    // Byte-identical to what pgc emitted before the `as` clause existed.
    let from = retyped("integer", i64::from(i32::MAX));
    let mut to = from.clone();
    to.cache_size = Some(10);
    to.hash();
    assert_eq!(
        to.get_alter_script(&from),
        "alter sequence app.t_id_seq start with 1 increment by 1 minvalue 1 maxvalue 2147483647 cache 10 no cycle owned by app.t.id;\n\n\
alter sequence app.t_id_seq owner to postgres;\n\n"
    );
    // An empty type (never in a live dump) adds no clause either.
    let mut untyped = to.clone();
    untyped.data_type = String::new();
    assert!(!untyped.get_alter_script(&from).contains(" as "));
}

#[test]
fn owner_of_a_linked_sequence_follows_its_table() {
    // `serial`: OWNED BY the column in both dumps, table owner changes.
    let from = retyped("integer", i64::from(i32::MAX));
    let mut to = from.clone();
    to.owner = "app_owner".to_string();
    to.hash();
    let script = to.get_alter_script(&from);
    assert!(!script.contains("owner to"), "{script}");
    assert!(script.contains("owned by app.t.id"), "{script}");
    assert!(!to.owned_by_waits_for_tables(&from));

    // Identity: linked the same way, and never rendered with OWNED BY.
    let mut from = from.clone();
    from.set_is_identity(true);
    let mut to = from.clone();
    to.owner = "app_owner".to_string();
    to.hash();
    let script = to.get_alter_script(&from);
    assert!(!script.contains("owner to"), "{script}");
    assert!(!script.contains("owned by"), "{script}");
}

#[test]
fn owner_of_a_linked_sequence_is_still_emitted_when_it_does_not_change() {
    // Unchanged owner: the (harmless) statement stays, as before.
    let from = retyped("integer", i64::from(i32::MAX));
    let mut to = from.clone();
    to.increment_by = Some(2);
    to.hash();
    assert!(
        to.get_alter_script(&from)
            .ends_with("alter sequence app.t_id_seq owner to postgres;\n\n")
    );
}

#[test]
fn a_sequence_linked_here_takes_its_new_owner_before_its_owned_by() {
    let mut from = retyped("bigint", i64::MAX);
    from.owned_by_schema = None;
    from.owned_by_table = None;
    from.owned_by_column = None;
    from.hash();
    let mut to = retyped("bigint", i64::MAX);
    to.owner = "app_owner".to_string();
    to.hash();

    assert!(to.owned_by_waits_for_tables(&from));
    assert_eq!(
        to.get_alter_script(&from),
        "alter sequence app.t_id_seq start with 1 increment by 1 minvalue 1 maxvalue 9223372036854775807 cache 1 no cycle;\n\n\
alter sequence app.t_id_seq owner to app_owner;\n\n"
    );
    assert_eq!(
        to.get_owned_by_script().as_deref(),
        Some("alter sequence app.t_id_seq owned by app.t.id;\n\n")
    );

    // Without an owner change the OWNED BY stays inline, as before.
    let mut same_owner = to.clone();
    same_owner.owner = from.owner.clone();
    same_owner.hash();
    assert!(!same_owner.owned_by_waits_for_tables(&from));
    assert!(
        same_owner
            .get_alter_script(&from)
            .contains("no cycle owned by app.t.id;")
    );
}

#[test]
fn owner_of_a_standalone_sequence_is_changed_directly() {
    let mut from = retyped("bigint", i64::MAX);
    from.owned_by_schema = None;
    from.owned_by_table = None;
    from.owned_by_column = None;
    from.hash();
    let mut to = from.clone();
    to.owner = "app_owner".to_string();
    to.hash();
    assert!(!to.owned_by_waits_for_tables(&from));
    assert!(
        to.get_alter_script(&from)
            .ends_with("no cycle;\n\nalter sequence app.t_id_seq owner to app_owner;\n\n")
    );
}

/// Review finding: `OWNED BY app.a.id` in FROM, `OWNED BY app.t.id` in TO,
/// new owner. A direct `OWNER TO` while still linked to `app.a` is refused,
/// and so is `OWNED BY app.t.id` while the owners differ; the sequence is
/// unlinked first, takes its owner, and is linked after the tables.
#[test]
fn a_sequence_moving_to_another_table_is_unlinked_before_its_owner_changes() {
    let mut from = retyped("bigint", i64::MAX);
    from.owned_by_table = Some("a".to_string());
    from.hash();
    let mut to = retyped("bigint", i64::MAX);
    to.owner = "app_owner".to_string();
    to.hash();

    assert!(to.owned_by_waits_for_tables(&from));
    assert_eq!(
        to.get_alter_script(&from),
        "alter sequence app.t_id_seq start with 1 increment by 1 minvalue 1 maxvalue 9223372036854775807 cache 1 no cycle owned by none;\n\n\
alter sequence app.t_id_seq owner to app_owner;\n\n"
    );
    assert_eq!(
        to.get_owned_by_script().as_deref(),
        Some("alter sequence app.t_id_seq owned by app.t.id;\n\n")
    );

    // Without an owner change the move stays one inline OWNED BY, as before.
    let mut same_owner = to.clone();
    same_owner.owner = from.owner.clone();
    same_owner.hash();
    assert!(!same_owner.owned_by_waits_for_tables(&from));
    let script = same_owner.get_alter_script(&from);
    assert!(script.contains("no cycle owned by app.t.id;"), "{script}");
    assert!(!script.contains("owned by none"), "{script}");
}

/// FROM `OWNED BY app.a.id`, TO standalone, new owner: unlinked first, since
/// the direct `OWNER TO` of a linked sequence is refused.
#[test]
fn a_sequence_leaving_its_table_is_unlinked_before_its_owner_changes() {
    let from = retyped("bigint", i64::MAX);
    let mut to = from.clone();
    to.owned_by_schema = None;
    to.owned_by_table = None;
    to.owned_by_column = None;
    to.owner = "app_owner".to_string();
    to.hash();

    assert!(!to.owned_by_waits_for_tables(&from));
    assert!(to.get_alter_script(&from).ends_with(
        "no cycle owned by none;\n\nalter sequence app.t_id_seq owner to app_owner;\n\n"
    ));

    // An identity sequence in FROM is never unlinked (PostgreSQL refuses it).
    let mut identity = from.clone();
    identity.set_is_identity(true);
    assert!(!to.get_alter_script(&identity).contains("owned by none"));
}
