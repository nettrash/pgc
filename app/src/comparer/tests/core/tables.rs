//! Tests for `compare_tables`: partition and inheritance ordering, serial
//! columns, and the pre-drop of foreign keys and triggers.

use super::helpers::*;
use crate::comparer::core::*;
use crate::config::dump_config::DumpConfig;
use crate::config::grants_mode::GrantsMode;
use crate::dump::sequence::Sequence;
use crate::dump::table::Table;
use crate::dump::table_column::TableColumn;
use crate::dump::table_constraint::TableConstraint;
use crate::dump::table_policy::TablePolicy;
use crate::dump::table_trigger::TableTrigger;
use sqlx::postgres::types::Oid;
use std::collections::{HashMap, HashSet};

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
        generation_blocks_retype: false,
        generation_routine_dropped: false,
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
        generation_blocks_retype: false,
        generation_routine_dropped: false,
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

// ---- PG19: inheritance ordering, CHECK enforceability, SET EXPRESSION ----

use crate::dump::column_dependent::{ColumnDependent, ColumnDependentKind};
use crate::dump::publication::Publication;
use crate::dump::table::TableAlterCaps;
use crate::dump::table_index::TableIndex;
use crate::dump::view::View;

fn plain_table(name: &str, columns: Vec<TableColumn>, inherits: &[&str]) -> Table {
    let mut table = Table::new(
        "public".to_string(),
        name.to_string(),
        "public".to_string(),
        name.to_string(),
        "postgres".to_string(),
        None,
        columns,
        vec![],
        vec![],
        vec![],
        None,
    );
    table.inherits_from = inherits.iter().map(|p| p.to_string()).collect();
    table.hash();
    table
}

fn names(tables: &[&Table]) -> Vec<String> {
    tables.iter().map(|t| t.name.clone()).collect()
}

#[test]
fn inheritance_parents_order_before_children_that_sort_first() {
    let tables = vec![
        plain_table("a_child", vec![], &["public.z_parent"]),
        plain_table("m_other", vec![], &[]),
        plain_table("b_grandchild", vec![], &["public.a_child"]),
        plain_table("z_parent", vec![], &[]),
    ];
    assert_eq!(
        names(&Comparer::ordered_tables(&tables)),
        ["m_other", "z_parent", "a_child", "b_grandchild"]
    );
    assert_eq!(
        names(&Comparer::ordered_tables_for_drop(&tables)),
        ["b_grandchild", "a_child", "m_other", "z_parent"]
    );
}

#[test]
fn inheritance_order_already_valid_is_unchanged() {
    // Children that already sort after their parent keep the alphabetical
    // position they always had — the output only moves where it was wrong.
    let tables = vec![
        plain_table("p_parent", vec![], &[]),
        plain_table("a_other", vec![], &[]),
        plain_table("q_child", vec![], &["public.p_parent"]),
        plain_table("r_other", vec![], &[]),
    ];
    assert_eq!(
        names(&Comparer::ordered_tables(&tables)),
        ["a_other", "p_parent", "q_child", "r_other"]
    );
    // Dropping: the parent now waits for its child.
    assert_eq!(
        names(&Comparer::ordered_tables_for_drop(&tables)),
        ["a_other", "q_child", "p_parent", "r_other"]
    );
}

#[test]
fn inheritance_order_handles_multiple_parents_and_unknown_ones() {
    let tables = vec![
        plain_table("a_both", vec![], &["public.x_one", "public.y_two"]),
        plain_table("b_outside", vec![], &["other.not_dumped"]),
        plain_table("x_one", vec![], &[]),
        plain_table("y_two", vec![], &[]),
    ];
    assert_eq!(
        names(&Comparer::ordered_tables(&tables)),
        ["b_outside", "x_one", "y_two", "a_both"]
    );
}

#[tokio::test]
async fn new_inheritance_child_is_created_after_its_parent() {
    // `CREATE TABLE a_child (...) INHERITS (z_parent)` fails before z_parent
    // exists; alphabetical order used to emit it first.
    let from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());
    to_dump.tables.push(plain_table(
        "a_child",
        vec![int_column("public", "a_child", "id", 1)],
        &["public.z_parent"],
    ));
    to_dump.tables.push(plain_table(
        "z_parent",
        vec![int_column("public", "z_parent", "id", 1)],
        &[],
    ));
    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_tables().await.unwrap();
    let script = comparer.get_script();
    let parent = script.find("/* Table: public.z_parent*/").expect(&script);
    let child = script.find("/* Table: public.a_child*/").expect(&script);
    assert!(parent < child, "parent must be created first: {script}");
}

#[tokio::test]
async fn removed_inheritance_parent_is_dropped_after_its_child() {
    // `DROP TABLE a_parent` (no CASCADE) fails while b_child exists.
    let mut from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());
    from_dump.tables.push(plain_table("a_parent", vec![], &[]));
    from_dump
        .tables
        .push(plain_table("b_child", vec![], &["public.a_parent"]));
    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_tables().await.unwrap();
    let script = comparer.get_script();
    let parent = script
        .find("drop table if exists public.a_parent;")
        .expect(&script);
    let child = script
        .find("drop table if exists public.b_child;")
        .expect(&script);
    assert!(child < parent, "child must be dropped first: {script}");
}

fn amount_check(table: &str, enforced: bool, coninhcount: i32) -> TableConstraint {
    TableConstraint {
        catalog: "postgres".to_string(),
        schema: "public".to_string(),
        name: "ck_amount".to_string(),
        table_name: table.to_string(),
        constraint_type: "CHECK".to_string(),
        is_deferrable: false,
        initially_deferred: false,
        definition: Some(if enforced {
            "CHECK (amount > 0)".to_string()
        } else {
            "CHECK (amount > 0) NOT ENFORCED".to_string()
        }),
        coninhcount,
        is_enforced: enforced,
        no_inherit: false,
        nulls_not_distinct: false,
        comment: None,
    }
}

/// An inheritance parent `ne_parent` and a child `ne_child` whose merged
/// copy of `ck_amount` is local too (conislocal, coninhcount = 1). The child
/// sorts first on purpose.
fn enforcement_dump(enforced: bool, server_version_num: Option<i32>) -> Dump {
    let mut dump = Dump::new(DumpConfig::default());
    dump.server_version_num = server_version_num;
    let mut parent = plain_table(
        "ne_parent",
        vec![int_column("public", "ne_parent", "amount", 1)],
        &[],
    );
    parent.constraints = vec![amount_check("ne_parent", enforced, 0)];
    parent.hash();
    let mut child = plain_table(
        "ne_child",
        vec![int_column("public", "ne_child", "amount", 1)],
        &["public.ne_parent"],
    );
    child.constraints = vec![amount_check("ne_child", enforced, 1)];
    child.hash();
    dump.tables.push(child);
    dump.tables.push(parent);
    dump
}

#[tokio::test]
async fn pg19_check_not_enforced_alters_parent_before_child() {
    // PG19 rejects NOT ENFORCED on a child's inherited copy while the
    // parent's is still enforced; the parent's ALTER recurses, so it must run
    // first and the child's own ALTER is then a no-op.
    let mut comparer = Comparer::new(
        enforcement_dump(true, Some(190000)),
        enforcement_dump(false, None),
        true,
        false,
        true,
        GrantsMode::Ignore,
    );
    comparer.compare_tables().await.unwrap();
    let script = comparer.get_script();
    let parent = script
        .find("alter table public.ne_parent alter constraint ck_amount not enforced;")
        .expect(&script);
    let child = script
        .find("alter table public.ne_child alter constraint ck_amount not enforced;")
        .expect(&script);
    assert!(parent < child, "{script}");
    assert!(!script.contains("drop constraint"), "{script}");
}

#[tokio::test]
async fn check_enforcement_flip_before_pg19_is_drop_and_re_add() {
    // Gated on the FROM dump: PG18 rejects the ALTER for CHECK, PG14-17 lack
    // the syntax, and a dump without a version is treated as the oldest
    // server. The TO dump's version is irrelevant.
    for from_version in [None, Some(180000)] {
        let mut comparer = Comparer::new(
            enforcement_dump(true, from_version),
            enforcement_dump(false, Some(190000)),
            true,
            false,
            true,
            GrantsMode::Ignore,
        );
        comparer.compare_tables().await.unwrap();
        let script = comparer.get_script();
        assert!(!script.contains("alter constraint"), "{script}");
        assert!(
            script.contains("alter table public.ne_parent drop constraint ck_amount;"),
            "{script}"
        );
        assert!(
            script.contains("alter table public.ne_parent add constraint ck_amount check (amount > 0) not enforced ;"),
            "{script}"
        );
    }
}

/// `public.gx` with generated column `doubled` of `kind` over `qty`, an
/// optional CHECK constraint and an index on `doubled`.
fn generated_table(kind: &str, expr: &str, with_check: bool) -> Table {
    let qty = int_column("public", "gx", "qty", 1);
    let mut doubled = int_column("public", "gx", "doubled", 2);
    doubled.is_generated = "ALWAYS".to_string();
    doubled.generation_expression = Some(expr.to_string());
    doubled.generation_type = Some(kind.to_string());
    let mut table = plain_table("gx", vec![qty, doubled], &[]);
    if with_check {
        table.constraints = vec![TableConstraint {
            catalog: "postgres".to_string(),
            schema: "public".to_string(),
            name: "ck_qty".to_string(),
            table_name: "gx".to_string(),
            constraint_type: "CHECK".to_string(),
            is_deferrable: false,
            initially_deferred: false,
            definition: Some("CHECK (qty >= 0)".to_string()),
            coninhcount: 0,
            is_enforced: true,
            no_inherit: false,
            nulls_not_distinct: false,
            comment: None,
        }];
    }
    table.indexes = vec![TableIndex {
        schema: "public".to_string(),
        table: "gx".to_string(),
        name: "gx_doubled_idx".to_string(),
        catalog: Some("postgres".to_string()),
        indexdef: "CREATE INDEX gx_doubled_idx ON public.gx USING btree (doubled)".to_string(),
        is_partition_index: false,
        comment: None,
    }];
    table.hash();
    table
}

fn generated_view() -> View {
    let mut view = View::new(
        "gx_v".to_string(),
        " SELECT doubled\n   FROM public.gx;".to_string(),
        "public".to_string(),
        vec!["public.gx".to_string()],
    );
    view.column_relation = Some(vec!["public.gx.doubled".to_string()]);
    view.hash();
    view
}

/// FROM `(qty * 2)` -> TO `(qty * 3)`; both sides carry a view reading the
/// column and pg_depend's record of the index on it.
fn generated_dumps(kind: &str, with_check: bool, from_version: Option<i32>) -> (Dump, Dump) {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());
    from_dump.server_version_num = from_version;
    from_dump
        .tables
        .push(generated_table(kind, "(qty * 2)", with_check));
    to_dump
        .tables
        .push(generated_table(kind, "(qty * 3)", with_check));
    for dump in [&mut from_dump, &mut to_dump] {
        dump.views.push(generated_view());
    }
    from_dump.column_dependents.push(ColumnDependent {
        schema: "public".to_string(),
        table: "gx".to_string(),
        column: "doubled".to_string(),
        kind: ColumnDependentKind::Index,
        dep_schema: "public".to_string(),
        dep_table: "gx".to_string(),
        dep_name: "gx_doubled_idx".to_string(),
    });
    (from_dump, to_dump)
}

async fn generated_script(from_dump: Dump, to_dump: Dump) -> String {
    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.drop_views().await.unwrap();
    comparer.compare_tables().await.unwrap();
    comparer.get_script()
}

const SET_EXPRESSION: &str =
    "alter table only public.gx alter column doubled set expression as (qty * 3);";

fn assert_set_expression(script: &str) {
    assert!(script.contains(SET_EXPRESSION), "{script}");
    assert!(!script.contains("drop column doubled"), "{script}");
    // The view stays in place, and Path B does not rebuild the index —
    // SET EXPRESSION keeps both.
    assert!(!script.contains("drop view"), "{script}");
    assert!(!script.contains("Recreate dependents"), "{script}");
    assert!(!script.contains("gx_doubled_idx"), "{script}");
}

fn assert_drop_and_add(script: &str) {
    assert!(!script.contains("set expression"), "{script}");
    assert!(
        script.contains("alter table public.gx drop column doubled;"),
        "{script}"
    );
    // DROP COLUMN fails while a view reads the column: it must be dropped
    // first (and is recreated with the views later).
    assert!(
        script.contains("drop view if exists public.gx_v"),
        "{script}"
    );
    // The index went with the column; Path B restores it.
    assert!(
        script.contains("CREATE INDEX IF NOT EXISTS gx_doubled_idx ON public.gx"),
        "{script}"
    );
}

#[tokio::test]
async fn pg19_virtual_expression_change_uses_set_expression() {
    let (from, to) = generated_dumps("v", true, Some(190000));
    assert_set_expression(&generated_script(from, to).await);
}

#[tokio::test]
async fn pg18_virtual_expression_change_with_check_is_drop_and_add() {
    // PG18: "SET EXPRESSION is not supported for virtual generated columns in
    // tables with check constraints".
    let (from, to) = generated_dumps("v", true, Some(180000));
    assert_drop_and_add(&generated_script(from, to).await);
}

#[tokio::test]
async fn pg18_virtual_expression_change_without_check_uses_set_expression() {
    let (from, to) = generated_dumps("v", false, Some(180000));
    assert_set_expression(&generated_script(from, to).await);
}

#[tokio::test]
async fn stored_expression_change_uses_set_expression_from_pg17() {
    for version in [170000, 180000, 190000] {
        let (from, to) = generated_dumps("s", true, Some(version));
        assert_set_expression(&generated_script(from, to).await);
    }
}

#[tokio::test]
async fn expression_change_without_set_expression_is_drop_and_add() {
    // PG14-16 (no SET EXPRESSION) and dumps of unknown version.
    for version in [None, Some(140000), Some(160000)] {
        for kind in ["s", "v"] {
            let (from, to) = generated_dumps(kind, false, version);
            let script = generated_script(from, to).await;
            assert_drop_and_add(&script);
            assert!(!script.contains("add generated"), "{script}");
        }
    }
}

fn publication(value: serde_json::Value) -> Publication {
    serde_json::from_value(value).expect("publication fixture")
}

#[tokio::test]
async fn pg19_virtual_set_expression_blocked_by_explicit_publication() {
    let (mut from, to) = generated_dumps("v", false, Some(190000));
    from.publications.push(publication(serde_json::json!({
        "name": "pub_gx", "owner": "postgres", "all_tables": false,
        "publish": "insert, update, delete, truncate",
        "tables": ["public.gx"], "hash": null
    })));
    assert_drop_and_add(&generated_script(from, to).await);
}

#[tokio::test]
async fn pg19_virtual_set_expression_not_blocked_by_all_tables_or_other_tables() {
    let (mut from, to) = generated_dumps("v", false, Some(190000));
    from.publications.push(publication(serde_json::json!({
        "name": "pub_all", "owner": "postgres", "all_tables": true,
        "publish": "insert", "hash": null
    })));
    from.publications.push(publication(serde_json::json!({
        "name": "pub_other", "owner": "postgres", "all_tables": false,
        "publish": "insert", "tables": ["public.gx_other", "public.g"], "hash": null
    })));
    assert_set_expression(&generated_script(from, to).await);
}

#[tokio::test]
async fn stored_set_expression_ignores_publications() {
    let (mut from, to) = generated_dumps("s", false, Some(170000));
    from.publications.push(publication(serde_json::json!({
        "name": "pub_gx", "owner": "postgres", "all_tables": false,
        "publish": "insert", "tables": ["public.gx"], "hash": null
    })));
    assert_set_expression(&generated_script(from, to).await);
}

#[test]
fn table_alter_caps_cover_the_whole_inheritance_family() {
    // SET EXPRESSION recurses, so a child's CHECK (PG18) or publication
    // membership (PG18/19) decides for the parent too, and parent and child
    // always get the same answer.
    let family = |version: i32, child_check: bool, child_published: bool| {
        let mut from = Dump::new(DumpConfig::default());
        from.server_version_num = Some(version);
        from.tables.push(plain_table("fam_parent", vec![], &[]));
        let mut child = plain_table("fam_child", vec![], &["public.fam_parent"]);
        if child_check {
            child.constraints = vec![amount_check("fam_child", true, 0)];
        }
        from.tables.push(child);
        from.tables.push(plain_table("loner", vec![], &[]));
        if child_published {
            from.publications.push(publication(serde_json::json!({
                "name": "pub_child", "owner": "postgres", "all_tables": false,
                "publish": "insert", "tables": ["public.fam_child"], "hash": null
            })));
        }
        let comparer = Comparer::new(
            from,
            Dump::new(DumpConfig::default()),
            true,
            false,
            true,
            GrantsMode::Ignore,
        );
        let caps = comparer.table_alter_caps();
        // A table missing from the map gets the default, as in compare_tables.
        let get = |name: &str| {
            caps.get(&("public".to_string(), name.to_string()))
                .copied()
                .unwrap_or_default()
        };
        (get("fam_parent"), get("fam_child"), get("loner"))
    };

    let (parent, child, loner) = family(180000, true, false);
    assert_eq!(parent, child);
    assert!(!parent.generated.virtual_set_expression);
    assert!(parent.generated.stored_set_expression);
    assert!(!parent.constraint.check_enforceability);
    assert!(loner.generated.virtual_set_expression);

    let (parent, child, loner) = family(190000, true, false);
    assert!(parent.generated.virtual_set_expression && child.generated.virtual_set_expression);
    assert!(parent.constraint.check_enforceability);
    assert!(loner.generated.virtual_set_expression);

    let (parent, child, loner) = family(190000, false, true);
    assert_eq!(parent, child);
    assert!(!parent.generated.virtual_set_expression);
    assert!(parent.generated.stored_set_expression);
    assert!(loner.generated.virtual_set_expression);

    // Before PG17 nothing is in place.
    let (parent, _, loner) = family(160000, false, false);
    assert_eq!(parent, TableAlterCaps::default());
    assert_eq!(loner, TableAlterCaps::default());
}

#[test]
fn pg18_virtual_set_expression_needs_every_parent_in_the_dump() {
    // A parent outside the dumped schemas may carry CHECKs the child
    // inherits (inherited-only constraints are not dumped).
    let mut from = Dump::new(DumpConfig::default());
    from.server_version_num = Some(180000);
    from.tables
        .push(plain_table("orphan", vec![], &["other.not_dumped"]));
    let comparer = Comparer::new(
        from,
        Dump::new(DumpConfig::default()),
        true,
        false,
        true,
        GrantsMode::Ignore,
    );
    let caps = comparer.table_alter_caps();
    assert!(
        !caps[&("public".to_string(), "orphan".to_string())]
            .generated
            .virtual_set_expression
    );
}

// ---- Review fixes: CHECK flips a child copy resists, TO-side family
// members, inheritance families sharing a generated column ----

#[tokio::test]
async fn pg19_check_flip_is_drop_and_re_add_when_a_child_copy_stays_enforced() {
    // Review finding: ALTER CONSTRAINT ... NOT ENFORCED cannot say ONLY and
    // recurses, so a merged child copy that TO keeps enforced came out
    // flipped and round 2 re-enforced it. The drop + re-add leaves the
    // child's local copy as it is (verified live on PostgreSQL 19).
    let mut to = enforcement_dump(true, None);
    for table in &mut to.tables {
        if table.name == "ne_parent" {
            table.constraints = vec![amount_check("ne_parent", false, 0)];
            table.hash();
        }
    }
    let mut comparer = Comparer::new(
        enforcement_dump(true, Some(190000)),
        to,
        true,
        false,
        true,
        GrantsMode::Ignore,
    );
    let caps = comparer.table_alter_caps();
    let key = |name: &str| ("public".to_string(), name.to_string());
    assert!(!caps[&key("ne_parent")].constraint.check_enforceability);
    assert!(caps[&key("ne_child")].constraint.check_enforceability);

    comparer.compare_tables().await.unwrap();
    let script = comparer.get_script();
    assert!(!script.contains("alter constraint"), "{script}");
    assert!(
        script.contains("alter table public.ne_parent drop constraint ck_amount;"),
        "{script}"
    );
    assert!(
        script.contains(
            "alter table public.ne_parent add constraint ck_amount check (amount > 0) not enforced ;"
        ),
        "{script}"
    );
    assert!(!script.contains("public.ne_child"), "{script}");
}

/// Partitioned `public.vp` with VIRTUAL `v` (`expr`) and partition `vp_1`.
fn virtual_partitioned(expr: &str) -> Vec<Table> {
    let mut v = int_column("public", "vp", "v", 2);
    v.is_generated = "ALWAYS".to_string();
    v.generation_expression = Some(expr.to_string());
    v.generation_type = Some("v".to_string());
    let mut parent = plain_table(
        "vp",
        vec![int_column("public", "vp", "id", 1), v.clone()],
        &[],
    );
    parent.partition_key = Some("RANGE (id)".to_string());
    parent.hash();
    let mut v1 = v;
    v1.table = "vp_1".to_string();
    let mut partition = plain_table("vp_1", vec![int_column("public", "vp_1", "id", 1), v1], &[]);
    partition.partition_of = Some("public.vp".to_string());
    partition.partition_bound = Some("FOR VALUES FROM (0) TO (10)".to_string());
    partition.hash();
    vec![parent, partition]
}

#[tokio::test]
async fn pg18_virtual_set_expression_counts_checks_of_to_only_family_members() {
    // Review finding: the create loop runs first, so a TO-only partition
    // with a CHECK already exists when the parent's SET EXPRESSION recurses
    // to it, and PostgreSQL 18 rejects it ("tables with check constraints").
    let mut from = Dump::new(DumpConfig::default());
    from.server_version_num = Some(180000);
    from.tables = virtual_partitioned("(id * 2)");
    let mut to = Dump::new(DumpConfig::default());
    to.tables = virtual_partitioned("(id * 3)");

    // Without the new partition: SET EXPRESSION (recursing — partitioned).
    let mut comparer = Comparer::new(
        from.clone(),
        to.clone(),
        true,
        false,
        true,
        GrantsMode::Ignore,
    );
    comparer.compare_tables().await.unwrap();
    let script = comparer.get_script();
    assert!(
        script.contains("alter table public.vp alter column v set expression as (id * 3);"),
        "{script}"
    );

    let mut v2 = int_column("public", "vp_2", "v", 2);
    v2.is_generated = "ALWAYS".to_string();
    v2.generation_expression = Some("(id * 3)".to_string());
    v2.generation_type = Some("v".to_string());
    let mut new_partition =
        plain_table("vp_2", vec![int_column("public", "vp_2", "id", 1), v2], &[]);
    new_partition.partition_of = Some("public.vp".to_string());
    new_partition.partition_bound = Some("FOR VALUES FROM (10) TO (20)".to_string());
    let mut check = amount_check("vp_2", true, 0);
    check.name = "vp_2_id_check".to_string();
    check.definition = Some("CHECK (id >= 10)".to_string());
    new_partition.constraints = vec![check];
    new_partition.hash();
    to.tables.push(new_partition);

    let mut comparer = Comparer::new(from, to, true, false, true, GrantsMode::Ignore);
    let caps = comparer.table_alter_caps();
    assert!(
        !caps[&("public".to_string(), "vp".to_string())]
            .generated
            .virtual_set_expression
    );
    comparer.compare_tables().await.unwrap();
    let script = comparer.get_script();
    assert!(!script.contains("set expression"), "{script}");
    assert!(
        script.contains("alter table public.vp drop column v;"),
        "{script}"
    );
}

/// `public.<name>` with `qty` and a STORED `total` of `expr`, inheriting
/// from `inherits`.
fn stored_member(name: &str, expr: &str, inherits: &[&str]) -> Table {
    let mut total = int_column("public", name, "total", 2);
    total.is_generated = "ALWAYS".to_string();
    total.generation_expression = Some(expr.to_string());
    total.generation_type = Some("s".to_string());
    plain_table(
        name,
        vec![int_column("public", name, "qty", 1), total],
        inherits,
    )
}

#[tokio::test]
async fn set_expression_on_an_inheritance_parent_leaves_a_child_expression_alone() {
    // Review finding: SET EXPRESSION recursed from the parent and overwrote
    // a child's own expression (PostgreSQL 16+), which FROM and TO share, so
    // nothing restored it. ONLY leaves the child to its own diff.
    let mut from = Dump::new(DumpConfig::default());
    from.server_version_num = Some(170000);
    from.tables = vec![
        stored_member("gp", "(qty * 2)", &[]),
        stored_member("gc", "(qty * 5)", &["public.gp"]),
        stored_member("gf", "(qty * 2)", &["public.gp"]),
    ];
    let mut to = Dump::new(DumpConfig::default());
    to.tables = vec![
        stored_member("gp", "(qty * 3)", &[]),
        stored_member("gc", "(qty * 5)", &["public.gp"]),
        stored_member("gf", "(qty * 3)", &["public.gp"]),
    ];
    let mut comparer = Comparer::new(from, to, true, false, true, GrantsMode::Ignore);
    comparer.compare_tables().await.unwrap();
    let script = comparer.get_script();
    assert!(
        script
            .contains("alter table only public.gp alter column total set expression as (qty * 3);"),
        "{script}"
    );
    // The child that follows the parent sets its own; the one with its own
    // expression gets nothing.
    assert!(
        script
            .contains("alter table only public.gf alter column total set expression as (qty * 3);"),
        "{script}"
    );
    assert!(!script.contains("public.gc"), "{script}");
}

fn family_dumps(from_version: Option<i32>) -> (Dump, Dump) {
    let mut from = Dump::new(DumpConfig::default());
    from.server_version_num = from_version;
    from.tables = vec![
        stored_member("fp", "(qty * 2)", &[]),
        stored_member("fc", "(qty * 2)", &["public.fp"]),
        stored_member("fg", "(qty * 2)", &["public.fc"]),
        // A child with an expression of its own, unchanged: the parent's
        // ADD COLUMN merges into its surviving copy.
        stored_member("fo", "(qty * 7)", &["public.fp"]),
    ];
    from.column_dependents.push(ColumnDependent {
        schema: "public".to_string(),
        table: "fc".to_string(),
        column: "total".to_string(),
        kind: ColumnDependentKind::Index,
        dep_schema: "public".to_string(),
        dep_table: "fc".to_string(),
        dep_name: "fc_total_idx".to_string(),
    });
    let mut to = Dump::new(DumpConfig::default());
    to.tables = vec![
        stored_member("fp", "(qty * 3)", &[]),
        stored_member("fc", "(qty * 3)", &["public.fp"]),
        stored_member("fg", "(qty * 3)", &["public.fc"]),
        stored_member("fo", "(qty * 7)", &["public.fp"]),
    ];
    let index = TableIndex {
        schema: "public".to_string(),
        table: "fc".to_string(),
        name: "fc_total_idx".to_string(),
        catalog: Some("postgres".to_string()),
        indexdef: "CREATE INDEX fc_total_idx ON public.fc USING btree (total)".to_string(),
        is_partition_index: false,
        comment: None,
    };
    for dump in [&mut from, &mut to] {
        let fc = dump.tables.iter_mut().find(|t| t.name == "fc").unwrap();
        fc.indexes = vec![index.clone()];
        fc.hash();
    }
    (from, to)
}

#[tokio::test]
async fn an_inheritance_family_shares_one_generated_column_drop_and_add() {
    // Review finding: every member dropped and re-added the column itself;
    // after the parent's recursing ADD COLUMN the children's copies were
    // inherited again ("cannot drop inherited column"), and a merged copy
    // kept its old expression. The root now drops the followers' copies
    // between its own drop and add, and they skip the pair.
    let (from, to) = family_dumps(Some(160000));
    let mut comparer = Comparer::new(from, to, true, false, true, GrantsMode::Ignore);
    comparer.compare_tables().await.unwrap();
    let script = comparer.get_script();
    let drop = script
        .find("alter table public.fp drop column total;")
        .expect(&script);
    let child = script
        .find("alter table public.fc drop column if exists total;")
        .expect(&script);
    let grandchild = script
        .find("alter table public.fg drop column if exists total;")
        .expect(&script);
    let add = script
        .find(
            "alter table public.fp add column total integer generated always as (qty * 3) stored;",
        )
        .expect(&script);
    assert!(
        drop < child && child < grandchild && grandchild < add,
        "{script}"
    );
    for member in ["fc", "fg", "fo"] {
        assert!(
            !script.contains(&format!("alter table public.{member} drop column total;")),
            "{script}"
        );
        assert!(
            !script.contains(&format!("alter table public.{member} add column total")),
            "{script}"
        );
    }
    // The child's index went with its copy; Path B restores it.
    assert!(
        script.contains("CREATE INDEX IF NOT EXISTS fc_total_idx ON public.fc"),
        "{script}"
    );
}

#[test]
fn inherited_recreates_need_every_parent_holding_the_column_to_follow() {
    // Multiple inheritance: a child that also inherits the column from a
    // table outside the family still holds an inherited copy after the
    // root's drop, so it cannot follow — and neither can its children.
    let (mut from, mut to) = family_dumps(None);
    for dump in [&mut from, &mut to] {
        dump.tables.push(stored_member("fx", "(qty * 2)", &[]));
        let fg = dump.tables.iter_mut().find(|t| t.name == "fg").unwrap();
        fg.inherits_from = vec!["public.fc".to_string(), "public.fx".to_string()];
        fg.hash();
    }
    let comparer = Comparer::new(from, to, true, false, true, GrantsMode::Ignore);
    let plans = comparer.inherited_recreates(&comparer.table_alter_caps());
    let key = |name: &str| ("public".to_string(), name.to_string());
    assert_eq!(
        plans[&key("fp")].descendant_drops["total"],
        vec!["public.fc".to_string()]
    );
    assert!(plans[&key("fc")].recreated_by_ancestor.contains("total"));
    assert!(!plans.contains_key(&key("fo")));
    assert!(!plans.contains_key(&key("fg")) || plans[&key("fg")].recreated_by_ancestor.is_empty());

    // use_drop=false: the drop + add is only a comment, nothing to share.
    let (from, to) = family_dumps(None);
    let comparer = Comparer::new(from, to, false, false, true, GrantsMode::Ignore);
    assert!(
        comparer
            .inherited_recreates(&comparer.table_alter_caps())
            .is_empty()
    );
}

#[tokio::test]
async fn a_follower_drops_its_changed_check_with_if_exists() {
    // The root's recursing DROP COLUMN takes the follower's local CHECK on
    // the column with it, so the follower's own drop of that CHECK — its
    // definition changes — must say IF EXISTS (it failed with "constraint
    // ... does not exist" on PostgreSQL 16). The TO CHECK is re-added.
    let (mut from, mut to) = family_dumps(Some(160000));
    for (dump, definition) in [
        (&mut from, "CHECK (total >= 0)"),
        (&mut to, "CHECK (total > '-1'::integer)"),
    ] {
        let fc = dump.tables.iter_mut().find(|t| t.name == "fc").unwrap();
        let mut check = amount_check("fc", true, 0);
        check.name = "fc_total_check".to_string();
        check.definition = Some(definition.to_string());
        fc.constraints = vec![check];
        fc.hash();
    }
    let mut comparer = Comparer::new(from, to, true, false, true, GrantsMode::Ignore);
    comparer.compare_tables().await.unwrap();
    let script = comparer.get_script();
    let root_add = script
        .find(
            "alter table public.fp add column total integer generated always as (qty * 3) stored;",
        )
        .expect(&script);
    let drop = script
        .find("alter table public.fc drop constraint if exists fc_total_check;")
        .expect(&script);
    let add = script
        .find("alter table public.fc add constraint fc_total_check check (total > '-1'::integer)")
        .expect(&script);
    assert!(root_add < drop && drop < add, "{script}");
    assert!(
        !script.contains("alter table public.fc drop constraint fc_total_check;"),
        "{script}"
    );
}

// ---- A CHECK let go before an ancestor's alter rewrites its table ----

/// `public.<name>` with `id` and a `c` of `c_type`, and, with `check`, a
/// local CHECK `<name>_c` on `c` that is enforced or not.
fn rewrite_member(name: &str, c_type: &str, check: Option<bool>) -> Table {
    let mut c = int_column("public", name, "c", 2);
    if c_type == "bigint" {
        c.data_type = "bigint".to_string();
        c.numeric_precision = Some(64);
    }
    let mut table = plain_table(name, vec![int_column("public", name, "id", 1), c], &[]);
    if let Some(enforced) = check {
        let mut constraint = amount_check(name, enforced, 0);
        constraint.name = format!("{name}_c");
        constraint.definition = Some(if enforced {
            "CHECK (c <> 1)".to_string()
        } else {
            "CHECK (c <> 1) NOT ENFORCED".to_string()
        });
        table.constraints = vec![constraint];
    }
    table
}

/// A partitioned `rp` with partition `rp1`, an inheritance parent `ip` with
/// child `ic`, and a standalone `zs`. `c` goes from integer to bigint in
/// every table; `rp1`, `ic` and `zs` carry a local CHECK on `c` that is
/// enforced in FROM and `NOT ENFORCED` in TO. Only `rp` and `ip` run the
/// `ALTER COLUMN ... TYPE` (a partition leaves it to its parent, and the
/// parent's recurses to the child), so the CHECKs of `rp1` and `ic` are
/// checked by an ancestor's rewrite.
fn rewrite_family(to: bool, server_version_num: Option<i32>) -> Dump {
    let c_type = if to { "bigint" } else { "integer" };
    let enforced = Some(!to);
    let mut dump = Dump::new(DumpConfig::default());
    dump.server_version_num = server_version_num;
    let mut rp = rewrite_member("rp", c_type, None);
    rp.partition_key = Some("RANGE (id)".to_string());
    let mut rp1 = rewrite_member("rp1", c_type, enforced);
    rp1.partition_of = Some("public.rp".to_string());
    rp1.partition_bound = Some("FOR VALUES FROM (0) TO (10)".to_string());
    let ip = rewrite_member("ip", c_type, None);
    let mut ic = rewrite_member("ic", c_type, enforced);
    ic.inherits_from = vec!["public.ip".to_string()];
    let zs = rewrite_member("zs", c_type, enforced);
    for mut table in [rp, rp1, ip, ic, zs] {
        table.hash();
        dump.tables.push(table);
    }
    dump
}

async fn rewrite_family_script(from_version: Option<i32>) -> String {
    let mut comparer = Comparer::new(
        rewrite_family(false, from_version),
        rewrite_family(true, None),
        true,
        false,
        true,
        GrantsMode::Ignore,
    );
    comparer.compare_tables().await.unwrap();
    comparer.get_script()
}

fn position_of(script: &str, statement: &str) -> usize {
    assert_eq!(
        script.matches(statement).count(),
        1,
        "expected `{statement}` exactly once in:\n{script}"
    );
    script.find(statement).unwrap()
}

#[tokio::test]
async fn pg19_check_relaxed_under_an_ancestors_rewrite_is_relaxed_before_every_alter() {
    // Review finding: a partition's (or an inheritance child's) local CHECK
    // relaxed in place ran in the child's own alter, after the parent's
    // recursing ALTER COLUMN ... TYPE had already checked the rewritten rows
    // against it ("check constraint ... is violated by some row").
    let script = rewrite_family_script(Some(190000)).await;
    let start = position_of(
        &script,
        "/* ---> CHECK constraints let go before a parent's alter rewrites their table: Start ---- */",
    );
    let end = position_of(
        &script,
        "/* ---> CHECK constraints let go before a parent's alter rewrites their table: End ------ */",
    );
    let rp1 = position_of(
        &script,
        "alter table public.rp1 alter constraint rp1_c not enforced;",
    );
    let ic = position_of(
        &script,
        "alter table public.ic alter constraint ic_c not enforced;",
    );
    assert!(
        start < rp1 && rp1 < end && start < ic && ic < end,
        "{script}"
    );
    let rp = position_of(&script, "alter table public.rp alter column c type bigint;");
    let ip = position_of(&script, "alter table public.ip alter column c type bigint;");
    assert!(end < rp && end < ip, "{script}");

    // A table that rewrites itself keeps the relax in its own alter, first.
    let zs_table = position_of(&script, "/* Table: public.zs*/");
    let zs_relax = position_of(
        &script,
        "alter table public.zs alter constraint zs_c not enforced;",
    );
    let zs_type = position_of(&script, "alter table public.zs alter column c type bigint;");
    assert!(
        end < zs_table && zs_table < zs_relax && zs_relax < zs_type,
        "{script}"
    );
}

#[tokio::test]
async fn check_dropped_under_an_ancestors_rewrite_is_dropped_before_every_alter() {
    // The same before PostgreSQL 19 (and for a pgc 1.0.x dump), where the
    // flip is a drop + re-add: the drop runs first, the re-add after the
    // parent's rewrite. A CHECK TO removes, the PG14-17 form of the case,
    // takes the same path.
    for from_version in [None, Some(140000), Some(180000)] {
        let script = rewrite_family_script(from_version).await;
        let end = position_of(
            &script,
            "/* ---> CHECK constraints let go before a parent's alter rewrites their table: End ------ */",
        );
        let rp1_drop = position_of(&script, "alter table public.rp1 drop constraint rp1_c;");
        let ic_drop = position_of(&script, "alter table public.ic drop constraint ic_c;");
        let rp = position_of(&script, "alter table public.rp alter column c type bigint;");
        let ip = position_of(&script, "alter table public.ip alter column c type bigint;");
        let rp1_add = position_of(
            &script,
            "alter table public.rp1 add constraint rp1_c check (c <> 1) not enforced ;",
        );
        let ic_add = position_of(
            &script,
            "alter table public.ic add constraint ic_c check (c <> 1) not enforced ;",
        );
        assert!(rp1_drop < end && ic_drop < end, "{script}");
        assert!(
            end < rp && rp < rp1_add && end < ip && ip < ic_add,
            "{script}"
        );
        // The standalone table's drop stays in its own alter.
        let zs_table = position_of(&script, "/* Table: public.zs*/");
        assert!(
            zs_table < position_of(&script, "alter table public.zs drop constraint zs_c;"),
            "{script}"
        );
    }

    // Removed in TO rather than relaxed.
    let mut to = rewrite_family(true, None);
    for table in to.tables.iter_mut().filter(|t| t.name == "rp1") {
        table.constraints.clear();
        table.hash();
    }
    let mut comparer = Comparer::new(
        rewrite_family(false, Some(140000)),
        to,
        true,
        false,
        true,
        GrantsMode::Ignore,
    );
    comparer.compare_tables().await.unwrap();
    let script = comparer.get_script();
    assert!(
        position_of(&script, "alter table public.rp1 drop constraint rp1_c;")
            < position_of(&script, "alter table public.rp alter column c type bigint;"),
        "{script}"
    );
}

#[test]
fn tables_rewritten_by_ancestor_are_the_members_a_recursing_rewrite_reaches() {
    let rewritten = |from: Dump, to: Dump| {
        let comparer = Comparer::new(from, to, true, false, true, GrantsMode::Ignore);
        let mut names: Vec<String> = comparer
            .tables_rewritten_by_ancestor(&comparer.table_alter_caps())
            .into_iter()
            .map(|(_, name)| name)
            .collect();
        names.sort();
        names
    };
    assert_eq!(
        rewritten(rewrite_family(false, None), rewrite_family(true, None)),
        ["ic", "rp1"]
    );

    // Nothing changes: nothing to rewrite.
    assert!(rewritten(rewrite_family(false, None), rewrite_family(false, None)).is_empty());

    // The parent's partition key changes: it is dropped and recreated, and
    // its partition with it.
    let mut to = rewrite_family(true, None);
    for table in to.tables.iter_mut().filter(|t| t.name == "rp") {
        table.partition_key = Some("RANGE (c)".to_string());
        table.hash();
    }
    assert_eq!(rewritten(rewrite_family(false, None), to), ["ic"]);

    // A column of the child's own (the parent lacks it) is its own alter's.
    let (mut from, mut to) = (rewrite_family(false, None), rewrite_family(true, None));
    for dump in [&mut from, &mut to] {
        for table in dump.tables.iter_mut().filter(|t| t.name == "ip") {
            table.columns.retain(|c| c.name != "c");
            table.hash();
        }
    }
    assert_eq!(rewritten(from, to), ["rp1"]);
}

// ---- pre-columns: a retyped column under a generated column --------------

/// `generated_dumps`, with `qty` retyped to bigint in TO and the generated
/// column's expression the same on both sides.
fn retyped_under_generated(from_version: Option<i32>) -> (Dump, Dump) {
    let (from, mut to) = generated_dumps("s", false, from_version);
    to.tables = vec![generated_table("s", "(qty * 2)", false)];
    for table in &mut to.tables {
        table.columns[0].data_type = "bigint".to_string();
        table.hash();
    }
    (from, to)
}

#[tokio::test]
async fn pre_columns_generated_column_is_re_created_around_a_retype() {
    for version in [None, Some(140000), Some(190000)] {
        let (from, to) = retyped_under_generated(version);
        let script = generated_script(from, to).await;
        let drop_view = position_of(&script, "drop view if exists public.gx_v");
        let drop = position_of(&script, "alter table public.gx drop column doubled;");
        let retype = position_of(
            &script,
            "alter table public.gx alter column qty type bigint;",
        );
        let add = position_of(
            &script,
            "alter table public.gx add column doubled integer generated always as (qty * 2) stored;",
        );
        let index = position_of(
            &script,
            "CREATE INDEX IF NOT EXISTS gx_doubled_idx ON public.gx",
        );
        assert!(
            drop_view < drop && drop < retype && retype < add && add < index,
            "{script}"
        );
    }
}

#[tokio::test]
async fn pre_columns_generated_column_re_created_around_a_retype_is_readded() {
    let (from, to) = retyped_under_generated(Some(180000));
    let mut comparer = Comparer::new(from, to, true, false, true, GrantsMode::Ignore);
    comparer.drop_views().await.unwrap();
    comparer.compare_tables().await.unwrap();
    // Its column privileges are planned from nothing, like any re-added column.
    assert!(comparer.readded_columns.contains(&(
        "public".to_string(),
        "gx".to_string(),
        "doubled".to_string()
    )));

    // Without use_drop the drop is only a comment: nothing moves, and the
    // output is what it was.
    let (from, to) = retyped_under_generated(Some(180000));
    let mut comparer = Comparer::new(from, to, false, false, true, GrantsMode::Ignore);
    comparer.drop_views().await.unwrap();
    comparer.compare_tables().await.unwrap();
    let script = comparer.get_script();
    assert!(!script.contains("drop column doubled"), "{script}");
    assert!(!script.contains("drop view"), "{script}");
    assert!(
        script.contains("alter table public.gx alter column qty type bigint;"),
        "{script}"
    );
}

// ---- pre-columns: CHECKs an inheritance family re-creates together -------

#[tokio::test]
async fn pre_columns_check_family_drops_every_copy_before_any_re_add() {
    // PG18 (or a FROM dump of unknown version): no in-place ALTER CONSTRAINT
    // for a CHECK. The parent's re-add recursed into the child's merged copy
    // before the child dropped it ("cannot drop inherited constraint"), and
    // a tightening parent re-add clashed with it ("conflicts with NOT
    // ENFORCED constraint").
    for (from_enforced, to_enforced) in [(true, false), (false, true)] {
        for from_version in [None, Some(180000)] {
            let mut comparer = Comparer::new(
                enforcement_dump(from_enforced, from_version),
                enforcement_dump(to_enforced, Some(190000)),
                true,
                false,
                true,
                GrantsMode::Ignore,
            );
            comparer.compare_tables().await.unwrap();
            let script = comparer.get_script();
            let start = position_of(
                &script,
                "/* ---> CHECK constraints dropped ahead of the table alters: Start ---- */",
            );
            let end = position_of(
                &script,
                "/* ---> CHECK constraints dropped ahead of the table alters: End ------ */",
            );
            let parent_drop = position_of(
                &script,
                "alter table public.ne_parent drop constraint ck_amount;",
            );
            let child_drop = position_of(
                &script,
                "alter table public.ne_child drop constraint ck_amount;",
            );
            let suffix = if to_enforced { "" } else { " not enforced" };
            let parent_add = position_of(
                &script,
                &format!(
                    "alter table public.ne_parent add constraint ck_amount check (amount > 0){suffix} ;"
                ),
            );
            let child_add = position_of(
                &script,
                &format!(
                    "alter table public.ne_child add constraint ck_amount check (amount > 0){suffix} ;"
                ),
            );
            assert!(
                start < parent_drop && parent_drop < child_drop && child_drop < end,
                "{script}"
            );
            assert!(end < parent_add && end < child_add, "{script}");
        }
    }
}

#[tokio::test]
async fn pre_columns_check_family_re_creates_an_unchanged_parent_copy() {
    // Only the child's merged copy flips; the parent's stays NOT ENFORCED.
    // The child cannot drop its copy while it is inherited, so the parent
    // drops and re-adds its own as it is.
    let dumps = |child_enforced: bool, version: Option<i32>| {
        let mut dump = enforcement_dump(false, version);
        for table in dump.tables.iter_mut().filter(|t| t.name == "ne_child") {
            table.constraints = vec![amount_check("ne_child", child_enforced, 1)];
            table.hash();
        }
        dump
    };
    let mut comparer = Comparer::new(
        dumps(true, Some(180000)),
        dumps(false, Some(180000)),
        true,
        false,
        true,
        GrantsMode::Ignore,
    );
    let plans = comparer.inherited_check_recreates(&comparer.table_alter_caps());
    assert_eq!(plans.len(), 2, "{plans:?}");
    comparer.compare_tables().await.unwrap();
    let script = comparer.get_script();
    let parent_drop = position_of(
        &script,
        "alter table public.ne_parent drop constraint ck_amount;",
    );
    let child_drop = position_of(
        &script,
        "alter table public.ne_child drop constraint ck_amount;",
    );
    let parent_add = position_of(
        &script,
        "alter table public.ne_parent add constraint ck_amount check (amount > 0) not enforced ;",
    );
    position_of(
        &script,
        "alter table public.ne_child add constraint ck_amount check (amount > 0) not enforced ;",
    );
    assert!(
        parent_drop < child_drop && child_drop < parent_add,
        "{script}"
    );

    // PG19 alters the child's copy in place: no family plan, as before.
    let comparer = Comparer::new(
        dumps(true, Some(190000)),
        dumps(false, Some(190000)),
        true,
        false,
        true,
        GrantsMode::Ignore,
    );
    assert!(
        comparer
            .inherited_check_recreates(&comparer.table_alter_caps())
            .is_empty()
    );
    // Nor without use_drop.
    let comparer = Comparer::new(
        dumps(true, Some(180000)),
        dumps(false, Some(180000)),
        false,
        false,
        true,
        GrantsMode::Ignore,
    );
    assert!(
        comparer
            .inherited_check_recreates(&comparer.table_alter_caps())
            .is_empty()
    );
}

#[tokio::test]
async fn pre_columns_merged_copy_that_to_only_inherits_is_dropped_under_its_parent() {
    // TO keeps the parent's CHECK and the child only inherits it: the child
    // cannot drop its merged copy on its own.
    let mut to = enforcement_dump(true, None);
    for table in to.tables.iter_mut().filter(|t| t.name == "ne_child") {
        table.constraints.clear();
        table.hash();
    }
    let mut comparer = Comparer::new(
        enforcement_dump(true, Some(140000)),
        to,
        true,
        false,
        true,
        GrantsMode::Ignore,
    );
    comparer.compare_tables().await.unwrap();
    let script = comparer.get_script();
    let parent_drop = position_of(
        &script,
        "alter table public.ne_parent drop constraint ck_amount;",
    );
    let child_drop = position_of(
        &script,
        "alter table public.ne_child drop constraint ck_amount;",
    );
    let parent_add = position_of(
        &script,
        "alter table public.ne_parent add constraint ck_amount check (amount > 0) ;",
    );
    assert!(
        parent_drop < child_drop && child_drop < parent_add,
        "{script}"
    );
    assert!(
        !script.contains("alter table public.ne_child add constraint"),
        "{script}"
    );
}

// ---- pre-columns: partition trigger clones --------------------------------

fn pc_trigger(name: &str, table: &str, definition_events: &str, enabled: &str) -> TableTrigger {
    TableTrigger {
        oid: Oid(5),
        name: name.to_string(),
        definition: format!(
            "CREATE TRIGGER {name} BEFORE {definition_events} ON public.{table} FOR EACH ROW EXECUTE FUNCTION public.trg()"
        ),
        enabled: enabled.to_string(),
        comment: None,
    }
}

/// `public.pt` partitioned, with `pt_1` (itself partitioned, holding
/// `pt_1a`) and `pt_2`. `trigger` goes on `pt` when given; `clones` are
/// the recorded clones `(partition, enabled, comment)`.
fn clone_dump(
    trigger: Option<TableTrigger>,
    clones: &[(&str, &str, Option<&str>)],
    server_version_num: Option<i32>,
) -> Dump {
    let mut dump = Dump::new(DumpConfig::default());
    dump.server_version_num = server_version_num;
    let mut parent = plain_table("pt", vec![int_column("public", "pt", "id", 1)], &[]);
    parent.partition_key = Some("RANGE (id)".to_string());
    parent.triggers = trigger.clone().into_iter().collect();
    let mut tables = vec![parent];
    for (name, parent_name) in [("pt_1", "pt"), ("pt_1a", "pt_1"), ("pt_2", "pt")] {
        let mut partition = plain_table(name, vec![int_column("public", name, "id", 1)], &[]);
        partition.partition_of = Some(format!("public.{parent_name}"));
        partition.partition_bound = Some("FOR VALUES FROM (0) TO (10)".to_string());
        if name == "pt_1" {
            partition.partition_key = Some("RANGE (id)".to_string());
        }
        if let Some(trigger) = &trigger {
            for (_, enabled, comment) in clones.iter().filter(|(p, _, _)| *p == name) {
                let mut clone = pc_trigger(&trigger.name, name, "INSERT", enabled);
                clone.comment = comment.map(str::to_string);
                partition.trigger_clones.push(clone);
            }
        }
        tables.push(partition);
    }
    for mut table in tables {
        table.hash();
        dump.tables.push(table);
    }
    dump
}

async fn clone_script(from: Dump, to: Dump) -> String {
    let mut comparer = Comparer::new(from, to, true, false, true, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    comparer.get_script()
}

#[tokio::test]
async fn pre_columns_new_partitioned_table_creates_its_trigger_once() {
    // The parent's CREATE TRIGGER clones it onto every partition; a CREATE
    // TRIGGER on a partition failed with "already exists".
    let to = clone_dump(
        Some(pc_trigger("trg", "pt", "INSERT", "O")),
        &[("pt_2", "D", Some("it's 2"))],
        Some(190000),
    );
    let script = clone_script(Dump::new(DumpConfig::default()), to).await;
    let create = position_of(
        &script,
        "CREATE TRIGGER trg BEFORE INSERT ON public.pt FOR EACH ROW",
    );
    assert_eq!(script.matches("CREATE TRIGGER").count(), 1, "{script}");
    let disable = position_of(&script, "alter table public.pt_2 disable trigger trg;");
    let comment = position_of(
        &script,
        "comment on trigger trg on public.pt_2 is 'it''s 2';",
    );
    assert!(create < disable && create < comment, "{script}");
}

#[tokio::test]
async fn pre_columns_clone_states_follow_the_parent_trigger() {
    let t = |events: &str, enabled: &str| Some(pc_trigger("trg", "pt", events, enabled));

    // Nothing changes: nothing to do.
    let same = || clone_dump(t("INSERT", "O"), &[("pt_2", "D", None)], Some(180000));
    let script = clone_script(same(), same()).await;
    assert!(!script.contains("trigger"), "{script}");

    // Only a clone's own state changes.
    let script = clone_script(
        same(),
        clone_dump(
            t("INSERT", "O"),
            &[("pt_1a", "A", Some("mine"))],
            Some(180000),
        ),
    )
    .await;
    assert!(!script.contains("CREATE TRIGGER"), "{script}");
    assert!(!script.contains("drop trigger"), "{script}");
    position_of(&script, "alter table public.pt_2 enable trigger trg;");
    position_of(
        &script,
        "alter table public.pt_1a enable always trigger trg;",
    );
    position_of(&script, "comment on trigger trg on public.pt_1a is 'mine';");

    // The parent's definition changes: its clones come back in its state,
    // so a recorded one is set again after it, and none is dropped.
    let script = clone_script(
        same(),
        clone_dump(
            t("INSERT OR UPDATE", "O"),
            &[("pt_2", "D", None)],
            Some(180000),
        ),
    )
    .await;
    let recreate = position_of(
        &script,
        "CREATE TRIGGER trg BEFORE INSERT OR UPDATE ON public.pt FOR EACH ROW",
    );
    let disable = position_of(&script, "alter table public.pt_2 disable trigger trg;");
    assert!(recreate < disable, "{script}");
    assert!(!script.contains("trg on public.pt_2;"), "{script}");
    assert_eq!(script.matches("drop trigger").count(), 1, "{script}");

    // The parent's own state changes and recurses: a partition that keeps
    // its own state (and, under it, a sub-partition following it) is set
    // again after it, parents first.
    let script = clone_script(
        clone_dump(t("INSERT", "O"), &[("pt_1", "R", None)], Some(180000)),
        clone_dump(
            t("INSERT", "D"),
            &[("pt_1", "R", None), ("pt_1a", "O", None)],
            Some(180000),
        ),
    )
    .await;
    let parent = position_of(&script, "alter table public.pt disable trigger trg;");
    let p1 = position_of(
        &script,
        "alter table public.pt_1 enable replica trigger trg;",
    );
    let p1a = position_of(&script, "alter table public.pt_1a enable trigger trg;");
    assert!(parent < p1 && p1 < p1a, "{script}");
    assert!(!script.contains("public.pt_2 "), "{script}");
}

#[tokio::test]
async fn pre_columns_dropped_partitioned_table_leaves_its_clones_to_it() {
    // The whole family goes, partitions first: only the parent's trigger is
    // dropped, never a partition's clone, which PostgreSQL refuses ("cannot
    // drop trigger ... because trigger ... requires it").
    let from = clone_dump(
        Some(pc_trigger("trg", "pt", "INSERT", "O")),
        &[("pt_2", "D", None)],
        Some(180000),
    );
    let script = clone_script(from, Dump::new(DumpConfig::default())).await;
    assert_eq!(script.matches("drop trigger").count(), 1, "{script}");
    position_of(&script, "drop trigger if exists trg on public.pt;");
}

#[tokio::test]
async fn pre_columns_check_family_is_left_alone_when_the_parent_drops_its_copy() {
    // The parent drops its CHECK for good, in its own alter, which runs
    // before the child's: the child's copy is no longer inherited when the
    // child drops (or changes) it, and the output is what it always was.
    for child_keeps in [false, true] {
        let mut to = enforcement_dump(true, None);
        for table in &mut to.tables {
            if table.name == "ne_parent" {
                table.constraints.clear();
            } else if child_keeps {
                table.constraints = vec![amount_check("ne_child", false, 0)];
            } else {
                table.constraints.clear();
            }
            table.hash();
        }
        let from = enforcement_dump(true, Some(180000));
        let comparer = Comparer::new(
            from.clone(),
            to.clone(),
            true,
            false,
            true,
            GrantsMode::Ignore,
        );
        assert!(
            comparer
                .inherited_check_recreates(&comparer.table_alter_caps())
                .is_empty()
        );
        let mut comparer = Comparer::new(from, to, true, false, true, GrantsMode::Ignore);
        comparer.compare_tables().await.unwrap();
        let script = comparer.get_script();
        assert!(
            !script.contains("dropped ahead of the table alters"),
            "{script}"
        );
        let parent_table = position_of(&script, "/* Table: public.ne_parent*/");
        let parent_drop = position_of(
            &script,
            "alter table public.ne_parent drop constraint ck_amount;",
        );
        let child_table = position_of(&script, "/* Table: public.ne_child*/");
        let child_drop = position_of(
            &script,
            "alter table public.ne_child drop constraint ck_amount;",
        );
        assert!(
            parent_table < parent_drop && parent_drop < child_table && child_table < child_drop,
            "{script}"
        );
    }
}

// ---- pre-columns review fixes: partitions of a parent outside the dump ----

/// Partitions of `outside.op`, a parent the dump does not have: `op_1`
/// (partitioned, holding `op_1a`), `op_2`, and `op_3` when `with_op_3`.
/// `clones` are the recorded clones `(partition, trigger, enabled, comment)`;
/// a current dump records every clone of `op_1`, `op_2` and `op_3`.
fn orphan_dump(
    clones: &[(&str, &str, &str, Option<&str>)],
    with_op_3: bool,
    server_version_num: Option<i32>,
) -> Dump {
    let mut dump = Dump::new(DumpConfig::default());
    dump.server_version_num = server_version_num;
    let mut partitions = vec![
        ("op_1", "outside.op"),
        ("op_1a", "public.op_1"),
        ("op_2", "outside.op"),
    ];
    if with_op_3 {
        partitions.push(("op_3", "outside.op"));
    }
    for (name, parent) in partitions {
        let mut partition = plain_table(name, vec![int_column("public", name, "id", 1)], &[]);
        partition.partition_of = Some(parent.to_string());
        partition.partition_bound = Some("FOR VALUES IN (1)".to_string());
        if name == "op_1" {
            partition.partition_key = Some("LIST (id)".to_string());
        }
        for (_, trigger, enabled, comment) in clones.iter().filter(|(p, ..)| *p == name) {
            let mut clone = pc_trigger(trigger, name, "INSERT", enabled);
            clone.comment = comment.map(str::to_string);
            partition.trigger_clones.push(clone);
        }
        partition.hash();
        dump.tables.push(partition);
    }
    dump
}

#[tokio::test]
async fn pre_columns_clone_states_of_a_parent_outside_the_dump_are_compared_directly() {
    // Review finding: only the clones of a dumped parent's triggers were
    // walked, so a partition whose parent lies outside the dumped schemas
    // lost its clone's state change (round 2 still compared clean).
    let from = orphan_dump(
        &[
            ("op_1", "trg", "O", None),
            ("op_2", "trg", "O", Some("old")),
        ],
        false,
        Some(180000),
    );
    let to = orphan_dump(
        &[
            ("op_1", "trg", "D", None),
            ("op_1a", "trg", "A", None),
            ("op_2", "trg", "O", None),
        ],
        false,
        Some(180000),
    );
    let script = clone_script(from.clone(), to).await;
    let op_1 = position_of(&script, "alter table public.op_1 disable trigger trg;");
    let op_1a = position_of(
        &script,
        "alter table public.op_1a enable always trigger trg;",
    );
    assert!(op_1 < op_1a, "{script}");
    position_of(&script, "comment on trigger trg on public.op_2 is null;");
    assert!(!script.contains("public.op_2 enable"), "{script}");
    assert!(!script.contains("CREATE TRIGGER"), "{script}");
    assert!(!script.contains("drop trigger"), "{script}");

    // Unchanged: nothing.
    let script = clone_script(from.clone(), from.clone()).await;
    assert!(!script.contains("trigger"), "{script}");

    // FROM has no such clone, so its copy of the parent has no such trigger:
    // left alone.
    let script = clone_script(
        from.clone(),
        orphan_dump(
            &[
                ("op_1", "trg", "O", None),
                ("op_2", "trg", "O", Some("old")),
                ("op_2", "trg2", "D", None),
            ],
            false,
            Some(180000),
        ),
    )
    .await;
    assert!(!script.contains("trg2"), "{script}");
}

#[tokio::test]
async fn pre_columns_new_partition_of_a_parent_outside_the_dump_gets_its_clone_states() {
    // The parent's state is not known, so a new partition's clone gets its
    // TO state set whatever it is; never a CREATE TRIGGER, which failed with
    // "already exists".
    let from = orphan_dump(
        &[("op_1", "trg", "O", None), ("op_2", "trg", "O", None)],
        false,
        Some(180000),
    );
    let to = orphan_dump(
        &[
            ("op_1", "trg", "O", None),
            ("op_2", "trg", "O", None),
            ("op_3", "trg", "O", None),
            ("op_3", "trg2", "D", Some("new")),
        ],
        true,
        Some(180000),
    );
    let script = clone_script(from, to).await;
    position_of(&script, "alter table public.op_3 enable trigger trg;");
    position_of(&script, "alter table public.op_3 disable trigger trg2;");
    position_of(&script, "comment on trigger trg2 on public.op_3 is 'new';");
    assert!(!script.contains("CREATE TRIGGER"), "{script}");
    assert!(!script.contains("public.op_1 "), "{script}");
}

#[tokio::test]
async fn pre_columns_1_0_dump_of_a_partition_outside_its_parents_schema_keeps_its_clones() {
    // Review finding: a pgc 1.0.x FROM dump lists such a partition's clones
    // among its own triggers, and the parent is not there to tell them
    // apart: comparing it with a current dump emitted a DROP TRIGGER that
    // PostgreSQL refuses.
    let legacy = |states: &[(&str, &str)]| {
        let mut dump = orphan_dump(&[], false, None);
        for table in &mut dump.tables {
            for (partition, enabled) in states {
                if table.name == *partition {
                    table
                        .triggers
                        .push(pc_trigger("trg", partition, "INSERT", enabled));
                }
            }
            table.hash();
        }
        dump
    };
    let current = orphan_dump(
        &[("op_1", "trg", "O", None), ("op_2", "trg", "D", None)],
        false,
        Some(180000),
    );
    // As `Dump::read_from_file` leaves a 1.0.x dump: the clone on `op_1a`,
    // whose parent `op_1` is in the dump, is already set apart (and, in its
    // parent's state, left out); the ones on `op_1` and `op_2` are not.
    // The same schema: nothing.
    let script = clone_script(legacy(&[("op_1", "O"), ("op_2", "D")]), current.clone()).await;
    assert!(!script.contains("drop trigger"), "{script}");
    assert!(!script.contains(" trigger trg"), "{script}");
    // A clone's state changed: set, never dropped.
    let script = clone_script(legacy(&[("op_1", "O"), ("op_2", "O")]), current.clone()).await;
    position_of(&script, "alter table public.op_2 disable trigger trg;");
    assert!(!script.contains("drop trigger"), "{script}");
    // A current FROM dump's own trigger is its own: dropped as before.
    let mut own = orphan_dump(&[], false, Some(180000));
    for table in own.tables.iter_mut().filter(|t| t.name == "op_2") {
        table
            .triggers
            .push(pc_trigger("trg", "op_2", "INSERT", "O"));
        table.hash();
    }
    let script = clone_script(own, current).await;
    position_of(&script, "drop trigger if exists trg on public.op_2;");
}

#[tokio::test]
async fn pre_columns_trigger_dropped_ahead_clones_afresh() {
    // A partitioned table's trigger dropped ahead of a column drop + add
    // (see `column_drop_blockers`) comes back cloned in its own state: a
    // clone of a state of its own is set again.
    let dump = || {
        clone_dump(
            Some(pc_trigger("trg", "pt", "INSERT", "O")),
            &[("pt_2", "D", Some("mine"))],
            Some(180000),
        )
    };
    let mut comparer = Comparer::new(dump(), dump(), true, false, true, GrantsMode::Ignore);
    assert_eq!(comparer.partition_trigger_clone_script(), "");
    comparer
        .triggers_dropped_ahead
        .insert("\"public\".\"pt\".trg".to_string());
    let script = comparer.partition_trigger_clone_script();
    position_of(&script, "alter table public.pt_2 disable trigger trg;");
    position_of(&script, "comment on trigger trg on public.pt_2 is 'mine';");
}

#[tokio::test]
async fn pre_columns_table_attached_to_a_parent_drops_its_own_trigger_first() {
    let dumps = |attached: bool| {
        let mut dump = clone_dump(
            Some(pc_trigger("trg", "pt", "INSERT", "O")),
            &[],
            Some(180000),
        );
        let mut t = plain_table("pt_9", vec![int_column("public", "pt_9", "id", 1)], &[]);
        if attached {
            t.partition_of = Some("public.pt".to_string());
            t.partition_bound = Some("FOR VALUES FROM (90) TO (100)".to_string());
        } else {
            t.triggers = vec![pc_trigger("trg", "pt_9", "INSERT", "O")];
        }
        t.hash();
        dump.tables.push(t);
        dump
    };
    let script = clone_script(dumps(false), dumps(true)).await;
    let drop = position_of(&script, "drop trigger if exists trg on public.pt_9;");
    let attach = position_of(
        &script,
        "alter table public.pt attach partition public.pt_9",
    );
    assert!(drop < attach, "{script}");

    // Under a parent the dump does not have, the clones TO records on the
    // table name the parent's row triggers.
    let orphan = |attached: bool| {
        let mut dump = Dump::new(DumpConfig::default());
        dump.server_version_num = Some(180000);
        let mut t = plain_table("px", vec![int_column("public", "px", "id", 1)], &[]);
        if attached {
            t.partition_of = Some("outside.p".to_string());
            t.partition_bound = Some("FOR VALUES IN (1)".to_string());
            t.trigger_clones = vec![pc_trigger("trg", "px", "INSERT", "O")];
        } else {
            t.triggers = vec![
                pc_trigger("trg", "px", "INSERT", "O"),
                pc_trigger("other", "px", "INSERT", "O"),
            ];
        }
        t.hash();
        dump.tables.push(t);
        dump
    };
    let comparer = Comparer::new(
        orphan(false),
        orphan(true),
        true,
        false,
        true,
        GrantsMode::Ignore,
    );
    let plans = comparer.triggers_dropped_before_attach();
    assert_eq!(
        plans[&("public".to_string(), "px".to_string())],
        HashSet::from(["trg".to_string()])
    );
    let script = clone_script(orphan(false), orphan(true)).await;
    let drop = position_of(&script, "drop trigger if exists trg on public.px;");
    let attach = position_of(&script, "alter table outside.p attach partition public.px");
    // A trigger whose name no clone takes is dropped where it always was.
    let other = position_of(&script, "drop trigger if exists other on public.px;");
    assert!(drop < attach && attach < other, "{script}");
}

// ---- pre-columns review fixes: INHERITS retype ----------------------------

/// `ip (id, a)` with children `ic (g reading a)` and `ic2 (x)`; `a` and `g`
/// integer in FROM, bigint in TO.
fn inherits_retype_dump(to: bool) -> Dump {
    let ty = if to { "bigint" } else { "integer" };
    let mut dump = Dump::new(DumpConfig::default());
    dump.server_version_num = Some(180000);
    let column = |table: &str, name: &str, ordinal: i32| {
        let mut column = int_column("public", table, name, ordinal);
        if name != "id" && name != "x" {
            column.data_type = ty.to_string();
        }
        column
    };
    let ip = plain_table("ip", vec![column("ip", "id", 1), column("ip", "a", 2)], &[]);
    let mut g = column("ic", "g", 3);
    g.is_generated = "ALWAYS".to_string();
    g.generation_expression = Some("(a * 2)".to_string());
    g.generation_type = Some("s".to_string());
    let ic = plain_table(
        "ic",
        vec![column("ic", "id", 1), column("ic", "a", 2), g],
        &["public.ip"],
    );
    let ic2 = plain_table(
        "ic2",
        vec![
            column("ic2", "id", 1),
            column("ic2", "a", 2),
            column("ic2", "x", 3),
        ],
        &["public.ip"],
    );
    dump.tables = vec![ic, ic2, ip];
    dump
}

#[tokio::test]
async fn pre_columns_inheritance_parent_retypes_for_its_children() {
    // Review finding: the parent's recursing ALTER COLUMN ... TYPE failed
    // under a child's generated column, and a child's own retype of the
    // inherited column failed ("cannot alter inherited column").
    let comparer = Comparer::new(
        inherits_retype_dump(false),
        inherits_retype_dump(true),
        true,
        false,
        true,
        GrantsMode::Ignore,
    );
    let retypes = comparer.inherited_retypes(&comparer.table_alter_caps());
    let key = |name: &str| ("public".to_string(), name.to_string());
    assert_eq!(retypes.len(), 2, "{retypes:?}");
    assert_eq!(retypes[&key("ic")], HashSet::from(["a".to_string()]));
    assert_eq!(retypes[&key("ic2")], HashSet::from(["a".to_string()]));

    let mut comparer = Comparer::new(
        inherits_retype_dump(false),
        inherits_retype_dump(true),
        true,
        false,
        true,
        GrantsMode::Ignore,
    );
    comparer.compare_tables().await.unwrap();
    let script = comparer.get_script();
    let start = position_of(
        &script,
        "/* ---> Generated columns let go before a parent's alter retypes a column they read: Start ---- */",
    );
    let drop = position_of(&script, "alter table public.ic drop column g;");
    let retype = position_of(&script, "alter table public.ip alter column a type bigint;");
    let add = position_of(
        &script,
        "alter table public.ic add column g bigint generated always as (a * 2) stored;",
    );
    assert!(start < drop && drop < retype && retype < add, "{script}");
    assert!(
        !script.contains("public.ic alter column a type"),
        "{script}"
    );
    assert!(
        !script.contains("public.ic2 alter column a type"),
        "{script}"
    );

    // A child retyping a column of its own, with no parent holding it, keeps
    // its retype.
    let mut from = inherits_retype_dump(false);
    let mut to = inherits_retype_dump(true);
    for dump in [&mut from, &mut to] {
        dump.tables.retain(|t| t.name != "ip");
        for table in &mut dump.tables {
            table.inherits_from.clear();
            table.hash();
        }
    }
    let comparer = Comparer::new(from, to, true, false, true, GrantsMode::Ignore);
    assert!(
        comparer
            .inherited_retypes(&comparer.table_alter_caps())
            .is_empty()
    );
}

// ---- pre-columns review fixes: what blocks a column drop + add ------------

#[tokio::test]
async fn pre_columns_column_drop_blockers_are_dropped_ahead_and_put_back() {
    // Review finding: a policy on the re-created column, a trigger naming
    // it (UPDATE OF / WHEN) and another table's foreign key on a key over it
    // block `DROP COLUMN` ("other objects depend on it"); they were only
    // restored after it.
    let (mut from, mut to) = retyped_under_generated(Some(180000));
    let pol = TablePolicy {
        schema: "public".to_string(),
        table: "gx".to_string(),
        name: "gx_pol".to_string(),
        command: "select".to_string(),
        permissive: true,
        roles: vec!["public".to_string()],
        using_clause: Some("(doubled > 0)".to_string()),
        check_clause: None,
    };
    let trg = TableTrigger {
        oid: Oid(9),
        name: "gx_trg".to_string(),
        definition: "CREATE TRIGGER gx_trg AFTER UPDATE OF doubled ON public.gx FOR EACH ROW EXECUTE FUNCTION public.f()".to_string(),
        enabled: "D".to_string(),
        comment: None,
    };
    let fk = TableConstraint {
        catalog: "postgres".to_string(),
        schema: "public".to_string(),
        name: "ref_fk".to_string(),
        table_name: "ref".to_string(),
        constraint_type: "FOREIGN KEY".to_string(),
        is_deferrable: false,
        initially_deferred: false,
        definition: Some("FOREIGN KEY (d) REFERENCES public.gx(doubled)".to_string()),
        coninhcount: 0,
        is_enforced: true,
        no_inherit: false,
        nulls_not_distinct: false,
        comment: None,
    };
    for dump in [&mut from, &mut to] {
        for table in &mut dump.tables {
            table.policies = vec![pol.clone()];
            table.triggers = vec![trg.clone()];
            table.hash();
        }
        let mut reference = plain_table("ref", vec![int_column("public", "ref", "d", 1)], &[]);
        reference.constraints = vec![fk.clone()];
        reference.hash();
        dump.tables.push(reference);
    }
    for (kind, dep_table, dep_name) in [
        (ColumnDependentKind::Policy, "gx", "gx_pol"),
        (ColumnDependentKind::Trigger, "gx", "gx_trg"),
        (ColumnDependentKind::Constraint, "ref", "ref_fk"),
    ] {
        from.column_dependents.push(ColumnDependent {
            schema: "public".to_string(),
            table: "gx".to_string(),
            column: "doubled".to_string(),
            kind,
            dep_schema: "public".to_string(),
            dep_table: dep_table.to_string(),
            dep_name: dep_name.to_string(),
        });
    }
    let mut comparer = Comparer::new(
        from.clone(),
        to.clone(),
        true,
        false,
        true,
        GrantsMode::Ignore,
    );
    comparer.compare().await.unwrap();
    let script = comparer.get_script();
    let start = position_of(
        &script,
        "/* ---> Dependents dropped ahead of a column drop + add: Start ---- */",
    );
    let drop_fk = script
        .find("alter table public.ref drop constraint if exists ref_fk;")
        .expect(&script);
    let drop_pol = script
        .find("drop policy if exists gx_pol on public.gx;")
        .unwrap();
    let drop_trg = script
        .find("drop trigger if exists gx_trg on public.gx;")
        .unwrap();
    let drop_column = position_of(&script, "alter table public.gx drop column doubled;");
    let add_column = position_of(
        &script,
        "alter table public.gx add column doubled integer generated always as (qty * 2) stored;",
    );
    assert!(
        start < drop_fk && start < drop_pol && start < drop_trg,
        "{script}"
    );
    assert!(
        drop_fk < drop_column && drop_pol < drop_column && drop_trg < drop_column,
        "{script}"
    );
    // Put back after the add: the policy and the foreign key with the other
    // dependents, the trigger with the other triggers, in its own state.
    let restore_pol = script
        .rfind("create policy gx_pol on public.gx")
        .expect(&script);
    let restore_fk = script
        .rfind("alter table public.ref add constraint ref_fk foreign key")
        .expect(&script);
    let restore_trg = position_of(
        &script,
        "CREATE TRIGGER gx_trg AFTER UPDATE OF doubled ON public.gx",
    );
    let disable = position_of(&script, "alter table public.gx disable trigger gx_trg;");
    assert!(
        add_column < restore_pol && add_column < restore_fk && add_column < restore_trg,
        "{script}"
    );
    assert!(restore_trg < disable, "{script}");

    // Without use_drop nothing is dropped ahead.
    let mut comparer = Comparer::new(
        from.clone(),
        to.clone(),
        false,
        false,
        true,
        GrantsMode::Ignore,
    );
    comparer.compare().await.unwrap();
    assert!(
        !comparer
            .get_script()
            .contains("Dependents dropped ahead of a column drop + add")
    );
    // Nor for a FROM dump without the graph.
    from.column_dependents.clear();
    let comparer = Comparer::new(from, to, true, false, true, GrantsMode::Ignore);
    let blockers = comparer.column_drop_blockers(&comparer.table_alter_caps());
    assert!(blockers.script.is_empty() && blockers.triggers.is_empty());
}

// ==== r3-ddl: foreign keys in the way of a dropped table ====

/// A foreign key `name` on `s.table`, as `pg_get_constraintdef` renders it.
fn r3_fk(table: &str, name: &str, definition: &str) -> TableConstraint {
    TableConstraint {
        catalog: "postgres".to_string(),
        schema: "s".to_string(),
        name: name.to_string(),
        table_name: table.to_string(),
        constraint_type: "FOREIGN KEY".to_string(),
        is_deferrable: false,
        initially_deferred: false,
        definition: Some(definition.to_string()),
        coninhcount: 0,
        is_enforced: true,
        no_inherit: false,
        nulls_not_distinct: false,
        comment: None,
    }
}

/// `s.name` with an `id` column and `constraints`.
fn r3_table(name: &str, constraints: Vec<TableConstraint>) -> Table {
    let mut table = Table::new(
        "s".to_string(),
        name.to_string(),
        "s".to_string(),
        name.to_string(),
        "postgres".to_string(),
        None,
        vec![int_column("s", name, "id", 1)],
        constraints,
        vec![],
        vec![],
        None,
    );
    table.hash();
    table
}

/// A partition of `s.parent` (itself partitioned by range when `key`).
fn r3_partition(name: &str, parent: Option<&str>, key: bool) -> Table {
    let mut table = r3_table(name, vec![]);
    if key {
        table.partition_key = Some("RANGE (id)".to_string());
    }
    if let Some(parent) = parent {
        table.partition_of = Some(format!("s.{parent}"));
        table.partition_bound = Some("FOR VALUES FROM (0) TO (100)".to_string());
    }
    table.hash();
    table
}

async fn r3_script(from: Vec<Table>, to: Vec<Table>, use_drop: bool) -> String {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());
    from_dump.tables = from;
    to_dump.tables = to;
    let mut comparer = Comparer::new(
        from_dump,
        to_dump,
        use_drop,
        false,
        true,
        GrantsMode::Ignore,
    );
    comparer.compare().await.unwrap();
    comparer.get_script()
}

/// Only a foreign key that references the dropped table is dropped ahead of
/// it. One whose definition merely holds the name — `s.account` in
/// `REFERENCES s.accounts(id)`, `log` in `catalog_id` — stays: it was
/// dropped and, unchanged in TO, never added back.
#[tokio::test]
async fn a_dropped_table_takes_only_the_foreign_keys_that_reference_it() {
    let payments_fk = r3_fk(
        "payments",
        "payments_account_fk",
        "FOREIGN KEY (account_id) REFERENCES s.accounts(id)",
    );
    let products_fk = r3_fk(
        "products",
        "products_catalog_fk",
        "FOREIGN KEY (catalog_id) REFERENCES s.catalogs(id)",
    );
    let orders_fk = r3_fk(
        "orders",
        "orders_log_fk",
        "FOREIGN KEY (log_id) REFERENCES s.log(id)",
    );
    let kept = vec![
        r3_table("accounts", vec![]),
        r3_table("catalogs", vec![]),
        r3_table("payments", vec![payments_fk]),
        r3_table("products", vec![products_fk]),
    ];
    let mut from = kept.clone();
    from.push(r3_table("account", vec![]));
    from.push(r3_table("log", vec![]));
    from.push(r3_table("orders", vec![orders_fk]));
    let mut to = kept;
    to.push(r3_table("orders", vec![]));

    let script = r3_script(from, to, true).await;
    assert!(!script.contains("payments_account_fk"), "{script}");
    assert!(!script.contains("products_catalog_fk"), "{script}");
    // The one that does reference a dropped table goes, once.
    assert_eq!(
        script
            .matches("alter table s.orders drop constraint orders_log_fk;")
            .count(),
        1,
        "{script}"
    );
    assert!(
        position_of(
            &script,
            "alter table s.orders drop constraint orders_log_fk;"
        ) < position_of(&script, "drop table if exists s.log;"),
        "{script}"
    );
}

/// A foreign key dropped ahead of a `DROP TABLE` is not dropped a second
/// time by its table's alter — which failed with "does not exist" — and
/// comes back in its TO form when TO still has it.
#[tokio::test]
async fn a_foreign_key_dropped_ahead_of_a_table_is_neither_dropped_twice_nor_lost() {
    // TO drops the foreign key along with the table it referenced.
    let from = vec![
        r3_table("account", vec![]),
        r3_table(
            "payments",
            vec![r3_fk(
                "payments",
                "payments_account_fk",
                "FOREIGN KEY (account_id) REFERENCES s.account(id)",
            )],
        ),
    ];
    let to = vec![r3_table("payments", vec![])];
    let script = r3_script(from, to, true).await;
    assert_eq!(
        script
            .matches("drop constraint payments_account_fk")
            .count(),
        1,
        "{script}"
    );

    // TO points the same foreign key at another table.
    let from = vec![
        r3_table("account", vec![]),
        r3_table("customer", vec![]),
        r3_table(
            "payments",
            vec![r3_fk(
                "payments",
                "payments_fk",
                "FOREIGN KEY (account_id) REFERENCES s.account(id)",
            )],
        ),
    ];
    let to = vec![
        r3_table("customer", vec![]),
        r3_table(
            "payments",
            vec![r3_fk(
                "payments",
                "payments_fk",
                "FOREIGN KEY (account_id) REFERENCES s.customer(id)",
            )],
        ),
    ];
    let script = r3_script(from, to, true).await;
    assert_eq!(
        script.matches("drop constraint payments_fk").count(),
        1,
        "{script}"
    );
    let drop_table = position_of(&script, "drop table if exists s.account;");
    let add = position_of(
        &script,
        "alter table s.payments add constraint payments_fk foreign key (account_id) references s.customer(id)",
    );
    assert!(
        position_of(
            &script,
            "alter table s.payments drop constraint payments_fk;"
        ) < drop_table
            && drop_table < add,
        "{script}"
    );
}

/// PostgreSQL copies a foreign key referencing a partitioned table onto each
/// partition, so dropping a partition takes the foreign key first — and it
/// is added back, since the partitioned table it references stays.
#[tokio::test]
async fn a_dropped_partition_takes_the_foreign_keys_to_its_ancestors_and_they_come_back() {
    let fk = r3_fk("r", "r_pid_fkey", "FOREIGN KEY (pid) REFERENCES s.p(id)");
    let kept = vec![
        r3_partition("p", None, true),
        r3_partition("p1", Some("p"), false),
        r3_table("r", vec![fk]),
    ];
    let mut from = kept.clone();
    from.push(r3_partition("p2", Some("p"), true));
    from.push(r3_partition("p2a", Some("p2"), false));
    let script = r3_script(from, kept, true).await;
    let drop = position_of(&script, "alter table s.r drop constraint r_pid_fkey;");
    let drop_leaf = position_of(&script, "drop table if exists s.p2a;");
    let add = position_of(
        &script,
        "alter table s.r add constraint r_pid_fkey foreign key (pid) references s.p(id)",
    );
    assert!(drop < drop_leaf && drop_leaf < add, "{script}");
    assert_eq!(script.matches("drop constraint r_pid_fkey").count(), 1);
}

/// Without `use_drop` the drops are commented out and the foreign keys stay,
/// so nothing is added back.
#[tokio::test]
async fn without_use_drop_a_foreign_key_in_the_way_stays_as_it_is() {
    let fk = r3_fk("r", "r_pid_fkey", "FOREIGN KEY (pid) REFERENCES s.p(id)");
    let kept = vec![r3_partition("p", None, true), r3_table("r", vec![fk])];
    let mut from = kept.clone();
    from.push(r3_partition("p2", Some("p"), false));
    let script = r3_script(from, kept, false).await;
    assert!(
        script.contains("-- alter table s.r drop constraint r_pid_fkey;"),
        "{script}"
    );
    assert!(!script.contains("add constraint r_pid_fkey"), "{script}");
}

/// An unqualified reference (`pg_get_constraintdef` leaves the schema out
/// when the dump's `search_path` reaches it) counts every table it may name:
/// the owner's schema and `public`. A table of that name elsewhere is not one.
#[tokio::test]
async fn an_unqualified_reference_matches_by_name_in_the_owner_schema_or_public() {
    let fk = r3_fk(
        "orders",
        "orders_log_fk",
        "FOREIGN KEY (log_id) REFERENCES log(id)",
    );
    let kept = vec![r3_table("orders", vec![fk])];
    let mut from = kept.clone();
    from.push(r3_table("log", vec![]));
    let script = r3_script(from, kept, true).await;
    let drop = position_of(
        &script,
        "alter table s.orders drop constraint orders_log_fk;",
    );
    assert!(drop < position_of(&script, "drop table if exists s.log;"));
    // TO keeps the key, whose target it no longer has — added back as TO
    // has it, as any foreign key to a missing table would be.
    assert!(script.contains("alter table s.orders add constraint orders_log_fk"));

    let mut elsewhere = r3_table("catalogs", vec![]);
    elsewhere.schema = "other".to_string();
    elsewhere.raw_schema = "other".to_string();
    elsewhere.hash();
    let fk = r3_fk(
        "products",
        "products_catalog_fk",
        "FOREIGN KEY (catalog_id) REFERENCES s.catalogs(id)",
    );
    let kept = vec![r3_table("catalogs", vec![]), r3_table("products", vec![fk])];
    let mut from = kept.clone();
    from.push(elsewhere);
    let script = r3_script(from, kept, true).await;
    assert!(!script.contains("products_catalog_fk"), "{script}");
}

// ==== r3-ddl review: collations created before the tables, and what is in
// the way of an in-place retype ====

/// A `text` column `name` of `s.table`, collated `collation` (`schema`,
/// `name`) when given.
fn r3r_text_column(
    table: &str,
    name: &str,
    ordinal: i32,
    collation: Option<(&str, &str)>,
) -> TableColumn {
    let mut column = int_column("s", table, name, ordinal);
    column.data_type = "text".to_string();
    column.udt_name = Some("text".to_string());
    column.numeric_precision = None;
    column.numeric_precision_radix = None;
    column.numeric_scale = None;
    if let Some((schema, coll)) = collation {
        column.collation_catalog = Some("postgres".to_string());
        column.collation_schema = Some(schema.to_string());
        column.collation_name = Some(coll.to_string());
    }
    column
}

/// `s.name` with `id` and a `text` column `c` collated `collation`.
fn r3r_table(name: &str, collation: Option<(&str, &str)>) -> Table {
    let mut table = Table::new(
        "s".to_string(),
        name.to_string(),
        "s".to_string(),
        name.to_string(),
        "postgres".to_string(),
        None,
        vec![
            int_column("s", name, "id", 1),
            r3r_text_column(name, "c", 2, collation),
        ],
        vec![],
        vec![],
        vec![],
        None,
    );
    table.hash();
    table
}

/// The libc collation `s.name`, hashed.
fn r3r_collation(name: &str) -> crate::dump::collation::Collation {
    let mut collation = crate::dump::collation::Collation {
        schema: "s".to_string(),
        name: name.to_string(),
        owner: "postgres".to_string(),
        provider: "c".to_string(),
        locale: Some("POSIX".to_string()),
        lc_collate: None,
        lc_ctype: None,
        icu_locale: None,
        icu_rules: None,
        deterministic: true,
        comment: None,
        hash: None,
    };
    collation.hash();
    collation
}

/// A collation only TO has is created before the types and tables: a kept
/// column moving to it (`ALTER COLUMN ... TYPE ... COLLATE`) and a new
/// table using it failed with "collation ... does not exist" when it was
/// created after them, with the other collation changes, and a
/// single-transaction migration never applied (review of r3-ddl).
#[tokio::test]
async fn a_new_collation_is_created_before_the_tables_that_use_it() {
    let mut from = Dump::new(DumpConfig::default());
    let mut to = Dump::new(DumpConfig::default());
    from.tables = vec![r3r_table("t", Some(("pg_catalog", "C")))];
    from.collations = vec![r3r_collation("gone")];
    to.tables = vec![
        r3r_table("t", Some(("s", "ci"))),
        r3r_table("t2", Some(("s", "ci"))),
    ];
    to.collations = vec![r3r_collation("ci")];
    let mut comparer = Comparer::new(from, to, true, true, true, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();

    let section = position_of(
        &script,
        "/* ---> New Collations (before types and tables) ---------- */",
    );
    let create = position_of(
        &script,
        "CREATE COLLATION s.ci (PROVIDER = libc, LOCALE = 'POSIX');",
    );
    let owner = position_of(&script, "ALTER COLLATION s.ci OWNER TO postgres;");
    let types = position_of(&script, "/* ---> User-defined types: Start");
    let retype = position_of(
        &script,
        "alter table s.t alter column c type text collate s.ci;",
    );
    let new_table = position_of(&script, "create table s.t2 (");
    assert!(
        section < create && create < owner && owner < types,
        "{script}"
    );
    assert!(owner < retype && owner < new_table, "{script}");
    // The collation TO drops goes where it always went: after the tables
    // have moved off it.
    let compare_section = position_of(&script, "/* ---> Compare Collations ------------------- */");
    let drop = position_of(&script, "DROP COLLATION IF EXISTS s.gone CASCADE;");
    assert!(
        retype < compare_section && compare_section < drop,
        "{script}"
    );
}

/// Without a new collation the script has no such section, and every other
/// collation statement stays in the Compare Collations section.
#[tokio::test]
async fn without_a_new_collation_the_collation_statements_stay_where_they_were() {
    let mut from = Dump::new(DumpConfig::default());
    let mut to = Dump::new(DumpConfig::default());
    let mut changed = r3r_collation("keep");
    changed.comment = Some("kept".to_string());
    changed.hash();
    from.collations = vec![r3r_collation("keep"), r3r_collation("gone")];
    to.collations = vec![changed];
    let mut comparer = Comparer::new(from, to, true, false, true, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();
    assert!(!script.contains("New Collations"), "{script}");
    assert!(!script.contains("CREATE COLLATION"), "{script}");
    let compare_section = position_of(&script, "/* ---> Compare Collations ------------------- */");
    assert!(compare_section < position_of(&script, "DROP COLLATION IF EXISTS s.gone CASCADE;"));
    assert!(compare_section < position_of(&script, "COMMENT ON COLLATION s.keep IS 'kept';"));
}

/// A policy on `s.<table>` named `name`, `using`.
fn r3r_policy(table: &str, name: &str, using: &str) -> TablePolicy {
    TablePolicy {
        schema: "s".to_string(),
        table: table.to_string(),
        name: name.to_string(),
        command: "all".to_string(),
        permissive: true,
        roles: vec![],
        using_clause: Some(using.to_string()),
        check_clause: None,
    }
}

/// An edge of the FROM column-dependent graph: `kind` `dep_table.dep_name`
/// depends on `s.t.c`.
fn r3r_edge(kind: ColumnDependentKind, dep_table: &str, dep_name: &str) -> ColumnDependent {
    ColumnDependent {
        schema: "s".to_string(),
        table: "t".to_string(),
        column: "c".to_string(),
        kind,
        dep_schema: "s".to_string(),
        dep_table: dep_table.to_string(),
        dep_name: dep_name.to_string(),
    }
}

/// FROM and TO of `s.t`, whose `c` changes collation only (`"C"` →
/// `"POSIX"`), with the policies and the `WHEN` trigger that depend on it,
/// a policy of `s.x` reading it in a subquery, and `s.r`'s foreign key over
/// `s.t`. TO changes `cp`, drops `gone` and keeps the rest.
fn r3r_retype_dumps() -> (Dump, Dump) {
    let trigger = TableTrigger {
        oid: Oid(7),
        name: "trg".to_string(),
        definition: "CREATE TRIGGER trg BEFORE UPDATE ON s.t FOR EACH ROW WHEN ((old.c IS DISTINCT FROM new.c)) EXECUTE FUNCTION s.f()".to_string(),
        enabled: "O".to_string(),
        comment: None,
    };
    let x_policy = r3r_policy(
        "x",
        "xp",
        "(EXISTS ( SELECT 1 FROM s.t WHERE (t.c = CURRENT_USER)))",
    );
    let fk = r3_fk("r", "r_t_fk", "FOREIGN KEY (tid) REFERENCES s.t(id)");
    let build = |collation: &str, to: bool| {
        let mut dump = Dump::new(DumpConfig::default());
        let mut t = r3r_table("t", Some(("pg_catalog", collation)));
        t.has_rowsecurity = true;
        t.policies = vec![
            r3r_policy("t", "p", "(c = CURRENT_USER)"),
            r3r_policy(
                "t",
                "cp",
                if to {
                    "(c = SESSION_USER)"
                } else {
                    "(c = CURRENT_USER)"
                },
            ),
        ];
        if !to {
            t.policies.push(r3r_policy("t", "gone", "(c <> 'x'::text)"));
        }
        t.triggers = vec![trigger.clone()];
        t.hash();
        let mut x = r3_table("x", vec![]);
        x.has_rowsecurity = true;
        x.policies = vec![x_policy.clone()];
        x.hash();
        dump.tables = vec![t, x, r3_table("r", vec![fk.clone()])];
        dump
    };
    let mut from = build("C", false);
    let to = build("POSIX", true);
    from.column_dependents = vec![
        r3r_edge(ColumnDependentKind::Policy, "t", "p"),
        r3r_edge(ColumnDependentKind::Policy, "t", "cp"),
        r3r_edge(ColumnDependentKind::Policy, "t", "gone"),
        r3r_edge(ColumnDependentKind::Trigger, "t", "trg"),
        r3r_edge(ColumnDependentKind::Policy, "x", "xp"),
        r3r_edge(ColumnDependentKind::Constraint, "r", "r_t_fk"),
    ];
    (from, to)
}

/// PostgreSQL refuses `ALTER COLUMN ... TYPE` while a policy or a trigger
/// depends on the column ("cannot alter type of a column used in a policy
/// definition" / "... in a trigger definition"), for a collation change
/// alone as for any other: the migration failed and every later compare
/// emitted the same ALTER (review of r3-ddl). Those are dropped ahead of the
/// table alters and put back from TO after them; the foreign key, which the
/// retype rebuilds itself, is left alone.
#[tokio::test]
async fn a_retype_drops_the_policies_and_triggers_in_its_way_and_puts_them_back() {
    let (from, to) = r3r_retype_dumps();
    let mut comparer = Comparer::new(
        from.clone(),
        to.clone(),
        true,
        true,
        true,
        GrantsMode::Ignore,
    );
    comparer.compare().await.unwrap();
    let script = comparer.get_script();

    let start = position_of(
        &script,
        "/* ---> Dependents dropped ahead of a column retype: Start ---- */",
    );
    let retype = position_of(
        &script,
        "alter table s.t alter column c type text collate \"POSIX\";",
    );
    for drop in [
        "drop policy if exists p on s.t;",
        "drop policy if exists cp on s.t;",
        "drop policy if exists gone on s.t;",
        "drop policy if exists xp on s.x;",
        "drop trigger if exists trg on s.t;",
    ] {
        let at = script.find(drop).expect(&script);
        assert!(start < at && at < retype, "{drop}:\n{script}");
    }
    // Dropped once: the table's alter leaves them alone.
    assert_eq!(
        script.matches("drop policy if exists gone on s.t;").count(),
        1,
        "{script}"
    );
    assert!(!script.contains("r_t_fk"), "{script}");
    assert!(!script.contains("column drop + add"), "{script}");

    // Put back from TO, once, after every table alter.
    let recreate = position_of(
        &script,
        "/* ---> Recreate policies dropped ahead of a column retype: Start ---- */",
    );
    assert!(retype < recreate, "{script}");
    for create in [
        "create policy p on s.t for all to public using (c = CURRENT_USER);",
        "create policy cp on s.t for all to public using (c = SESSION_USER);",
        "create policy xp on s.x for all to public using (EXISTS ( SELECT 1 FROM s.t WHERE (t.c = CURRENT_USER)));",
    ] {
        assert!(
            recreate < position_of(&script, create),
            "{create}:\n{script}"
        );
    }
    assert!(!script.contains("create policy gone"), "{script}");
    let trigger = position_of(
        &script,
        "CREATE TRIGGER trg BEFORE UPDATE ON s.t FOR EACH ROW WHEN ((old.c IS DISTINCT FROM new.c)) EXECUTE FUNCTION s.f()",
    );
    assert!(retype < trigger, "{script}");

    // Without use_drop nothing is dropped ahead, as for a column drop + add.
    let mut comparer = Comparer::new(
        from.clone(),
        to.clone(),
        false,
        false,
        true,
        GrantsMode::Ignore,
    );
    comparer.compare().await.unwrap();
    let script = comparer.get_script();
    assert!(!script.contains("ahead of a column retype"), "{script}");

    // Nor without a retype: a policy change alone stays in the table's alter.
    let (mut from, mut to) = r3r_retype_dumps();
    for dump in [&mut from, &mut to] {
        dump.tables[0].columns[1].collation_name = Some("C".to_string());
        dump.tables[0].hash();
    }
    let mut comparer = Comparer::new(from, to, true, false, true, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();
    assert!(!script.contains("ahead of a column retype"), "{script}");
    assert!(
        !script.contains("drop policy if exists p on s.t;"),
        "{script}"
    );
    let drop = position_of(&script, "drop policy if exists cp on s.t;");
    assert!(
        drop < position_of(&script, "create policy cp on s.t"),
        "{script}"
    );
}

/// What `column_drop_blockers` plans for a retype alone: no column drop +
/// add section, one drop per policy and trigger in the way (the foreign key
/// is not one), the policies listed for their tables' alters to leave alone
/// and the trigger for the trigger statements to put back.
#[tokio::test]
async fn column_drop_blockers_plans_the_drops_a_retype_needs() {
    let (from, to) = r3r_retype_dumps();
    let comparer = Comparer::new(from, to, true, false, true, GrantsMode::Ignore);
    let blockers = comparer.column_drop_blockers(&comparer.table_alter_caps());
    assert!(blockers.script.is_empty());
    assert_eq!(
        blockers.retype_script.matches("drop ").count(),
        5,
        "{}",
        blockers.retype_script
    );
    let mut names: Vec<&String> = blockers.policies.values().flatten().collect();
    names.sort();
    assert_eq!(names, ["cp", "gone", "p", "xp"]);
    assert_eq!(blockers.triggers.len(), 1, "{:?}", blockers.triggers);
    assert!(
        blockers.triggers.iter().all(|t| t.ends_with(".trg")),
        "{:?}",
        blockers.triggers
    );
}

/// A policy that depends on both a column the alter drops and re-adds and
/// one it retypes in place is dropped once, with the column drop + add's
/// dependents, which put it back after the table's alter; it is not left to
/// the retype's pass.
#[tokio::test]
async fn a_policy_in_the_way_of_a_rewrite_and_a_retype_is_dropped_once() {
    let (mut from, mut to) = retyped_under_generated(Some(180000));
    let pol = TablePolicy {
        schema: "public".to_string(),
        table: "gx".to_string(),
        name: "gx_pol".to_string(),
        command: "select".to_string(),
        permissive: true,
        roles: vec![],
        using_clause: Some("((doubled > 0) AND (qty > 0))".to_string()),
        check_clause: None,
    };
    for dump in [&mut from, &mut to] {
        for table in &mut dump.tables {
            table.policies = vec![pol.clone()];
            table.hash();
        }
    }
    for column in ["qty", "doubled"] {
        from.column_dependents.push(ColumnDependent {
            schema: "public".to_string(),
            table: "gx".to_string(),
            column: column.to_string(),
            kind: ColumnDependentKind::Policy,
            dep_schema: "public".to_string(),
            dep_table: "gx".to_string(),
            dep_name: "gx_pol".to_string(),
        });
    }
    let comparer = Comparer::new(
        from.clone(),
        to.clone(),
        true,
        false,
        true,
        GrantsMode::Ignore,
    );
    let blockers = comparer.column_drop_blockers(&comparer.table_alter_caps());
    assert!(
        blockers
            .script
            .contains("drop policy if exists gx_pol on public.gx;"),
        "{}",
        blockers.script
    );
    assert!(
        blockers.retype_script.is_empty(),
        "{}",
        blockers.retype_script
    );
    assert!(blockers.policies.is_empty());

    let mut comparer = Comparer::new(from, to, true, false, true, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();
    assert_eq!(
        script
            .matches("drop policy if exists gx_pol on public.gx;")
            .count(),
        2,
        "the drop ahead, and the put-back's own guard:\n{script}"
    );
    let retype = position_of(
        &script,
        "alter table public.gx alter column qty type bigint;",
    );
    let restore = position_of(&script, "create policy gx_pol on public.gx");
    assert!(retype < restore, "{script}");
    assert!(!script.contains("ahead of a column retype"), "{script}");
}

// ==== r3-constraints tests ====

fn position(script: &str, needle: &str) -> usize {
    script
        .find(needle)
        .unwrap_or_else(|| panic!("`{needle}` missing from:\n{script}"))
}

/// `public.ie_parent` holding `ck_amount` (enforced or not), and the
/// `INHERITS` chain `ie_child` / `ie_grandchild` below it, which only
/// inherit it; each name in `recorded` keeps an enforced copy
/// (`Table::enforced_inherited_checks`).
fn inherited_enforcement_dump(
    parent_enforced: bool,
    recorded: &[&str],
    server_version_num: Option<i32>,
) -> Dump {
    let mut dump = Dump::new(DumpConfig::default());
    dump.server_version_num = server_version_num;
    let mut parent = plain_table(
        "ie_parent",
        vec![int_column("public", "ie_parent", "amount", 1)],
        &[],
    );
    parent.constraints = vec![amount_check("ie_parent", parent_enforced, 0)];
    parent.hash();
    dump.tables.push(parent);
    for (name, of) in [
        ("ie_child", "public.ie_parent"),
        ("ie_grandchild", "public.ie_child"),
    ] {
        let mut table = plain_table(name, vec![int_column("public", name, "amount", 1)], &[of]);
        if recorded.contains(&name) {
            table.enforced_inherited_checks = vec!["ck_amount".to_string()];
        }
        table.hash();
        dump.tables.push(table);
    }
    dump
}

async fn tables_script(from: Dump, to: Dump) -> String {
    let mut comparer = Comparer::new(from, to, true, false, true, GrantsMode::Ignore);
    comparer.compare_tables().await.unwrap();
    comparer.get_script()
}

#[tokio::test]
async fn r3c_pg19_inherited_copy_stays_enforced_under_a_parent_going_not_enforced() {
    // Finding 1: the parent's in-place NOT ENFORCED recursed into the
    // child's copy, which TO keeps enforced, and nothing put it back.
    let script = tables_script(
        inherited_enforcement_dump(true, &[], Some(190000)),
        inherited_enforcement_dump(false, &["ie_child"], None),
    )
    .await;
    let parent = position(
        &script,
        "alter table public.ie_parent alter constraint ck_amount not enforced;",
    );
    let header = position(
        &script,
        "/* ---> Enforceability of inherited CHECK constraint copies: Start ---- */",
    );
    let child = position(
        &script,
        "alter table public.ie_child alter constraint ck_amount enforced;",
    );
    assert!(parent < header && header < child, "{script}");
    // Its descendant follows the recursing statement: enforced, like TO's.
    assert!(
        !script.contains("public.ie_grandchild alter constraint"),
        "{script}"
    );
}

#[tokio::test]
async fn r3c_pg19_inherited_copy_follows_its_parent_again() {
    // FROM keeps an enforced copy; TO's follows the NOT ENFORCED parent.
    let script = tables_script(
        inherited_enforcement_dump(false, &["ie_child"], Some(190000)),
        inherited_enforcement_dump(false, &[], None),
    )
    .await;
    assert!(
        script.contains("alter table public.ie_child alter constraint ck_amount not enforced;"),
        "{script}"
    );
    // The statement recurses: a grandchild TO keeps enforced comes after it.
    let script = tables_script(
        inherited_enforcement_dump(false, &["ie_child", "ie_grandchild"], Some(190000)),
        inherited_enforcement_dump(false, &["ie_grandchild"], None),
    )
    .await;
    let child = position(
        &script,
        "alter table public.ie_child alter constraint ck_amount not enforced;",
    );
    let grandchild = position(
        &script,
        "alter table public.ie_grandchild alter constraint ck_amount enforced;",
    );
    assert!(child < grandchild, "{script}");
}

#[tokio::test]
async fn r3c_pg19_recorded_copy_is_restored_only_when_something_resets_it() {
    // Nothing touches it: no statement, and no section.
    let unchanged = || inherited_enforcement_dump(false, &["ie_child"], Some(190000));
    let script = tables_script(unchanged(), unchanged()).await;
    assert!(!script.contains("alter constraint"), "{script}");
    assert!(!script.contains("Enforceability of inherited"), "{script}");
    // The parent's CHECK is re-created (its definition changes): the drop
    // recursed into the copy, the re-add made it NOT ENFORCED again.
    let mut to = unchanged();
    to.tables[0].constraints[0].definition = Some("CHECK (amount > 1) NOT ENFORCED".to_string());
    to.tables[0].hash();
    let script = tables_script(unchanged(), to).await;
    let re_add = position(
        &script,
        "alter table public.ie_parent add constraint ck_amount",
    );
    let child = position(
        &script,
        "alter table public.ie_child alter constraint ck_amount enforced;",
    );
    assert!(re_add < child, "{script}");
}

#[tokio::test]
async fn r3c_new_partition_gets_its_enforced_copy_after_it_is_created() {
    let mut parent = plain_table(
        "ie_pt",
        vec![int_column("public", "ie_pt", "amount", 1)],
        &[],
    );
    parent.partition_key = Some("RANGE (amount)".to_string());
    parent.constraints = vec![amount_check("ie_pt", false, 0)];
    parent.hash();
    let mut from = Dump::new(DumpConfig::default());
    from.server_version_num = Some(190000);
    from.tables.push(parent.clone());
    let mut partition = plain_table(
        "ie_pt_1",
        vec![int_column("public", "ie_pt_1", "amount", 1)],
        &[],
    );
    partition.partition_of = Some("public.ie_pt".to_string());
    partition.partition_bound = Some("FOR VALUES FROM (0) TO (10)".to_string());
    partition.enforced_inherited_checks = vec!["ck_amount".to_string()];
    partition.hash();
    let mut to = Dump::new(DumpConfig::default());
    to.tables.push(parent);
    to.tables.push(partition);
    let script = tables_script(from, to).await;
    let create = position(
        &script,
        "create table public.ie_pt_1 partition of public.ie_pt",
    );
    let enforce = position(
        &script,
        "alter table public.ie_pt_1 alter constraint ck_amount enforced;",
    );
    assert!(create < enforce, "{script}");
}

#[tokio::test]
async fn r3c_inherited_copy_before_pg19_is_a_warning_and_a_comment() {
    // PostgreSQL 18 cannot alter a CHECK's enforceability (and a dump of
    // unknown version is taken as the oldest server).
    for version in [Some(180000), None] {
        let comparer = Comparer::new(
            inherited_enforcement_dump(false, &[], version),
            inherited_enforcement_dump(false, &["ie_child"], Some(180000)),
            true,
            false,
            true,
            GrantsMode::Ignore,
        );
        let (script, warnings) =
            comparer.inherited_check_enforcement(&HashMap::new(), &HashMap::new());
        assert_eq!(
            script,
            "-- public.ie_child: its inherited copy of CHECK constraint ck_amount has to be \
             ENFORCED, which ALTER CONSTRAINT sets only on PostgreSQL 19; left as it is.\n"
        );
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        assert!(warnings[0].contains("public.ie_child"), "{warnings:?}");
    }
    // No dump records such a copy: nothing, not even the walk.
    let comparer = Comparer::new(
        inherited_enforcement_dump(true, &[], Some(190000)),
        inherited_enforcement_dump(false, &[], None),
        true,
        false,
        true,
        GrantsMode::Ignore,
    );
    assert_eq!(
        comparer.inherited_check_enforcement(&HashMap::new(), &HashMap::new()),
        (String::new(), Vec::new())
    );
}

/// `public.at_pt` partitioned with `ck_amount`, and `public.a_x` — sorting
/// before it — holding a matching copy: on its own (`attached` false) or a
/// partition of it. The parent's comment differs between the two sides.
fn attach_dump(attached: bool) -> Dump {
    let mut dump = Dump::new(DumpConfig::default());
    dump.server_version_num = Some(190000);
    let mut parent = plain_table(
        "at_pt",
        vec![int_column("public", "at_pt", "amount", 1)],
        &[],
    );
    parent.partition_key = Some("RANGE (amount)".to_string());
    parent.constraints = vec![amount_check("at_pt", true, 0)];
    parent.comment = attached.then(|| "attached".to_string());
    parent.hash();
    let mut x = plain_table("a_x", vec![int_column("public", "a_x", "amount", 1)], &[]);
    if attached {
        x.partition_of = Some("public.at_pt".to_string());
        x.partition_bound = Some("FOR VALUES FROM (0) TO (10)".to_string());
    } else {
        x.constraints = vec![amount_check("a_x", true, 0)];
    }
    x.hash();
    dump.tables.push(x);
    dump.tables.push(parent);
    dump
}

#[tokio::test]
async fn r3c_attached_table_merges_the_parent_check_after_the_parent_alter() {
    // Finding 2: the DROP of the merged copy after ATTACH PARTITION failed.
    let comparer = Comparer::new(
        attach_dump(false),
        attach_dump(true),
        true,
        false,
        true,
        GrantsMode::Ignore,
    );
    let plans = comparer.partition_move_checks();
    let plan = &plans[&("public".to_string(), "a_x".to_string())];
    let (detached, attached) = (&plan.detached, &plan.attached);
    assert!(detached.is_empty());
    assert!(plan.readds.is_empty());
    assert_eq!(attached.len(), 1);
    assert_eq!(attached[0].table_name, "a_x");
    assert_eq!(attached[0].name, "ck_amount");

    let script = tables_script(attach_dump(false), attach_dump(true)).await;
    assert!(!script.contains("drop constraint"), "{script}");
    // The table waits for its new parent's alter, although it sorts first.
    let parent = position(&script, "comment on table public.at_pt is 'attached';");
    let attach = position(
        &script,
        "alter table public.at_pt attach partition public.a_x FOR VALUES FROM (0) TO (10);",
    );
    assert!(parent < attach, "{script}");
}

#[tokio::test]
async fn r3c_detached_partition_plan_holds_the_copies_it_keeps() {
    let comparer = Comparer::new(
        attach_dump(true),
        attach_dump(false),
        true,
        false,
        true,
        GrantsMode::Ignore,
    );
    let plans = comparer.partition_move_checks();
    let plan = &plans[&("public".to_string(), "a_x".to_string())];
    let (detached, attached) = (&plan.detached, &plan.attached);
    assert!(attached.is_empty());
    assert_eq!(detached.len(), 1);
    assert_eq!(detached[0].coninhcount, 0);
    assert_eq!(detached[0].table_name, "a_x");
    // TO's standalone table keeps the same CHECK: nothing but the detach.
    let script = tables_script(attach_dump(true), attach_dump(false)).await;
    assert!(
        script.contains("alter table public.at_pt detach partition public.a_x;"),
        "{script}"
    );
    assert!(!script.contains("constraint ck_amount"), "{script}");
}

// ==== r3-constraints review fixes ====

#[tokio::test]
async fn r3c_pg19_not_enforced_pass_statement_re_enforces_a_merged_copy_beneath() {
    // Review finding: the pass's `ALTER TABLE ie_child ... NOT ENFORCED`
    // recursed into ie_grandchild's merged, enforced copy, which TO keeps
    // enforced; its own alter had run already, and nothing put it back.
    let with_merged_grandchild = |recorded: &[&str], version: Option<i32>| {
        let mut dump = inherited_enforcement_dump(false, recorded, version);
        let grandchild = dump
            .tables
            .iter_mut()
            .find(|t| t.name == "ie_grandchild")
            .unwrap();
        grandchild.constraints = vec![amount_check("ie_grandchild", true, 1)];
        grandchild.hash();
        dump
    };
    let script = tables_script(
        with_merged_grandchild(&["ie_child"], Some(190000)),
        with_merged_grandchild(&[], None),
    )
    .await;
    let child = position(
        &script,
        "alter table public.ie_child alter constraint ck_amount not enforced;",
    );
    let grandchild = position(
        &script,
        "alter table public.ie_grandchild alter constraint ck_amount enforced;",
    );
    assert!(child < grandchild, "{script}");
    // Before PostgreSQL 19 nothing runs, so nothing is set again.
    let script = tables_script(
        with_merged_grandchild(&["ie_child"], Some(180000)),
        with_merged_grandchild(&[], None),
    )
    .await;
    assert!(
        !script.contains("public.ie_grandchild alter constraint"),
        "{script}"
    );
    assert!(
        !script.contains("ie_grandchild: its inherited copy"),
        "{script}"
    );
}

/// `public.ih_g` (holding `g_ck` and `id`), `public.ih_p` inheriting it
/// (with `p_ck`), and `public.ih_c`, standalone with both CHECKs (`joined`
/// false) or an `INHERITS` child of `ih_p` — merging them (`merged`) or only
/// inheriting them.
fn inherit_dump(joined: bool, merged: bool) -> Dump {
    let mut dump = Dump::new(DumpConfig::default());
    dump.server_version_num = Some(190000);
    let check = |table: &str, name: &str, coninhcount: i32| {
        let mut c = amount_check(table, true, coninhcount);
        c.name = name.to_string();
        c
    };
    let mut g = plain_table("ih_g", vec![int_column("public", "ih_g", "amount", 1)], &[]);
    g.constraints = vec![check("ih_g", "g_ck", 0)];
    g.hash();
    let mut p = plain_table(
        "ih_p",
        vec![int_column("public", "ih_p", "amount", 1)],
        &["public.ih_g"],
    );
    p.constraints = vec![check("ih_p", "p_ck", 0)];
    p.hash();
    let mut c = plain_table(
        "ih_c",
        vec![int_column("public", "ih_c", "amount", 1)],
        if joined { &["public.ih_p"] } else { &[] },
    );
    if !joined || merged {
        c.constraints = vec![
            check("ih_c", "g_ck", joined as i32),
            check("ih_c", "p_ck", joined as i32),
        ];
    }
    c.hash();
    dump.tables.extend([c, g, p]);
    dump
}

#[tokio::test]
async fn r3c_inherit_merges_the_checks_to_lists_on_the_table() {
    let comparer = Comparer::new(
        inherit_dump(false, true),
        inherit_dump(true, true),
        true,
        false,
        true,
        GrantsMode::Ignore,
    );
    let plans = comparer.partition_move_checks();
    let plan = &plans[&("public".to_string(), "ih_c".to_string())];
    let mut names: Vec<&str> = plan.attached.iter().map(|c| c.name.as_str()).collect();
    names.sort_unstable();
    assert_eq!(names, ["g_ck", "p_ck"]);
    assert!(plan.readds.is_empty() && plan.detached.is_empty());
    let script = tables_script(inherit_dump(false, true), inherit_dump(true, true)).await;
    assert!(
        script.contains("alter table public.ih_c inherit public.ih_p;"),
        "{script}"
    );
    assert!(!script.contains("drop constraint"), "{script}");
}

#[tokio::test]
async fn r3c_inherit_re_adds_the_holders_checks_the_table_only_inherits() {
    // Review finding: TO puts the table under INHERITS; pgc dropped its copy
    // of the parent's CHECK and never joined the table to the parent.
    let comparer = Comparer::new(
        inherit_dump(false, false),
        inherit_dump(true, false),
        true,
        false,
        true,
        GrantsMode::Ignore,
    );
    let plans = comparer.partition_move_checks();
    let plan = &plans[&("public".to_string(), "ih_c".to_string())];
    assert!(plan.attached.is_empty());
    let holders: Vec<(&str, &str)> = plan
        .readds
        .iter()
        .map(|c| (c.table_name.as_str(), c.name.as_str()))
        .collect();
    // Each holder of a CHECK TO has the table only inherit; ancestors first.
    assert_eq!(holders, [("ih_g", "g_ck"), ("ih_p", "p_ck")]);
    // A holder that merges the CHECK comes after the one above it.
    let merged_on_p = |joined: bool| {
        let mut dump = inherit_dump(joined, false);
        let p = dump.tables.iter_mut().find(|t| t.name == "ih_p").unwrap();
        let mut copy = amount_check("ih_p", true, 1);
        copy.name = "g_ck".to_string();
        p.constraints.push(copy);
        p.hash();
        dump
    };
    let comparer = Comparer::new(
        merged_on_p(false),
        merged_on_p(true),
        true,
        false,
        true,
        GrantsMode::Ignore,
    );
    let plans = comparer.partition_move_checks();
    let holders: Vec<(&str, &str)> = plans[&("public".to_string(), "ih_c".to_string())]
        .readds
        .iter()
        .filter(|c| c.name == "g_ck")
        .map(|c| (c.table_name.as_str(), c.name.as_str()))
        .collect();
    assert_eq!(holders, [("ih_g", "g_ck"), ("ih_p", "g_ck")]);

    let script = tables_script(inherit_dump(false, false), inherit_dump(true, false)).await;
    let own = position(&script, "alter table public.ih_c drop constraint g_ck;");
    let drop_g = position(&script, "alter table public.ih_g drop constraint g_ck;");
    let inherit = position(&script, "alter table public.ih_c inherit public.ih_p;");
    let add_g = position(&script, "alter table public.ih_g add constraint g_ck");
    let add_p = position(&script, "alter table public.ih_p add constraint p_ck");
    assert!(
        own < inherit && drop_g < inherit && inherit < add_g && inherit < add_p,
        "{script}"
    );
    // Without use_drop the table keeps its copies, which the INHERIT merges.
    let comparer = Comparer::new(
        inherit_dump(false, false),
        inherit_dump(true, false),
        false,
        false,
        true,
        GrantsMode::Ignore,
    );
    let plans = comparer.partition_move_checks();
    let plan = &plans[&("public".to_string(), "ih_c".to_string())];
    assert!(plan.readds.is_empty());
    assert_eq!(plan.attached.len(), 2);
}

#[tokio::test]
async fn r3c_no_inherit_plans_the_copies_it_leaves_behind() {
    let comparer = Comparer::new(
        inherit_dump(true, false),
        inherit_dump(false, false),
        true,
        false,
        true,
        GrantsMode::Ignore,
    );
    let plans = comparer.partition_move_checks();
    let plan = &plans[&("public".to_string(), "ih_c".to_string())];
    let mut names: Vec<&str> = plan.detached.iter().map(|c| c.name.as_str()).collect();
    names.sort_unstable();
    assert_eq!(names, ["g_ck", "p_ck"]);
    assert!(plan.detached.iter().all(|c| c.coninhcount == 0));
    // TO's standalone table keeps both: nothing but the NO INHERIT.
    let script = tables_script(inherit_dump(true, false), inherit_dump(false, false)).await;
    assert!(
        script.contains("alter table public.ih_c no inherit public.ih_p;"),
        "{script}"
    );
    assert!(!script.contains("constraint g_ck"), "{script}");
}

/// `public.sp_pt` partitioned with a CHECK and column `c`, and `public.a_sub`
/// — sorting first — partitioned itself without `c`, with its partition
/// `public.a_sub_1`, which TO gives an index on `c`: `a_sub` on its own
/// (`attached` false) or a partition of `sp_pt`.
fn attach_sub_dump(attached: bool) -> Dump {
    let mut dump = Dump::new(DumpConfig::default());
    dump.server_version_num = Some(190000);
    let mut parent = plain_table(
        "sp_pt",
        vec![
            int_column("public", "sp_pt", "amount", 1),
            int_column("public", "sp_pt", "c", 2),
        ],
        &[],
    );
    parent.partition_key = Some("RANGE (amount)".to_string());
    parent.constraints = vec![amount_check("sp_pt", true, 0)];
    parent.hash();
    let mut columns = vec![int_column("public", "a_sub", "amount", 1)];
    if attached {
        columns.push(int_column("public", "a_sub", "c", 2));
    }
    let mut sub = plain_table("a_sub", columns.clone(), &[]);
    sub.partition_key = Some("RANGE (amount)".to_string());
    if attached {
        sub.partition_of = Some("public.sp_pt".to_string());
        sub.partition_bound = Some("FOR VALUES FROM (0) TO (10)".to_string());
    } else {
        sub.constraints = vec![amount_check("a_sub", true, 0)];
    }
    sub.hash();
    let columns = columns
        .into_iter()
        .map(|mut c| {
            c.table = "a_sub_1".to_string();
            c
        })
        .collect();
    let mut sub_1 = plain_table("a_sub_1", columns, &[]);
    sub_1.partition_of = Some("public.a_sub".to_string());
    sub_1.partition_bound = Some("FOR VALUES FROM (0) TO (5)".to_string());
    if attached {
        sub_1.indexes = vec![TableIndex {
            schema: "public".to_string(),
            table: "a_sub_1".to_string(),
            name: "a_sub_1_c".to_string(),
            catalog: None,
            indexdef: "CREATE INDEX a_sub_1_c ON public.a_sub_1 USING btree (c)".to_string(),
            is_partition_index: false,
            comment: None,
        }];
    }
    sub_1.hash();
    dump.tables.extend([sub, sub_1, parent]);
    dump
}

#[tokio::test]
async fn r3c_partitions_of_an_attached_table_are_altered_after_it() {
    // Review finding: the attaching partitioned table went after every other
    // table, its own partition's alter included, whose index on the column
    // its parent adds then failed ("column c does not exist").
    let script = tables_script(attach_sub_dump(false), attach_sub_dump(true)).await;
    let add = position(&script, "alter table public.a_sub add column c integer");
    let attach = position(
        &script,
        "alter table public.sp_pt attach partition public.a_sub FOR VALUES FROM (0) TO (10);",
    );
    let index = position(&script, "CREATE INDEX a_sub_1_c ON public.a_sub_1");
    assert!(add < attach && attach < index, "{script}");
}

#[tokio::test]
async fn r3c_no_inherit_drops_a_merged_copy_without_its_parents() {
    // A merged copy of a parent the table leaves is local once the NO
    // INHERIT has run: it is dropped on its own, not with a drop + re-add
    // across the family (whose re-add recursed into the table before it left,
    // and left a copy the next compare dropped).
    let mut to = inherit_dump(false, false);
    let c = to.tables.iter_mut().find(|t| t.name == "ih_c").unwrap();
    c.constraints.clear();
    c.hash();
    let from = inherit_dump(true, true);
    let comparer = Comparer::new(
        from.clone(),
        to.clone(),
        true,
        false,
        true,
        GrantsMode::Ignore,
    );
    let plans = comparer.inherited_check_recreates(&comparer.table_alter_caps());
    assert!(plans.is_empty(), "{plans:?}");
    let script = tables_script(from, to).await;
    let leave = position(&script, "alter table public.ih_c no inherit public.ih_p;");
    let drop_g = position(&script, "alter table public.ih_c drop constraint g_ck;");
    let drop_p = position(&script, "alter table public.ih_c drop constraint p_ck;");
    assert!(leave < drop_g && leave < drop_p, "{script}");
    assert!(!script.contains("alter table public.ih_p"), "{script}");
    assert!(!script.contains("alter table public.ih_g"), "{script}");
}
