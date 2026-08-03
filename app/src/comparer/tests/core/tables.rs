//! Tests for `compare_tables`: partition and inheritance ordering, serial
//! columns, and the pre-drop of foreign keys and triggers.

use crate::comparer::core::*;
use super::helpers::*;
use crate::config::dump_config::DumpConfig;
use crate::config::grants_mode::GrantsMode;
use crate::dump::schema::Schema;
use crate::dump::sequence::Sequence;
use crate::dump::table::Table;
use crate::dump::table_column::TableColumn;
use crate::dump::table_constraint::TableConstraint;
use crate::dump::table_trigger::TableTrigger;
use sqlx::postgres::types::Oid;

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
