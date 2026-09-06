//! Tests for `--output-for-production` script rewriting.

use crate::comparer::core::*;
use crate::config::dump_config::DumpConfig;
use crate::config::grants_mode::GrantsMode;
use crate::dump::table::Table;
use crate::dump::table_constraint::TableConstraint;
use crate::dump::table_index::TableIndex;

#[tokio::test]
async fn output_for_production_defers_concurrent_index_and_validates_fk() {
    // New table with one index and one foreign key. Production mode must build
    // the index CONCURRENTLY after COMMIT, add the FK NOT VALID inside the
    // transaction, and VALIDATE it after COMMIT.
    let from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let index = TableIndex {
        schema: "public".to_string(),
        table: "orders".to_string(),
        name: "idx_orders_total".to_string(),
        catalog: None,
        indexdef: "CREATE INDEX idx_orders_total ON public.orders USING btree (total)".to_string(),
        is_partition_index: false,
        comment: None,
    };
    let fk = TableConstraint {
        catalog: "postgres".to_string(),
        schema: "public".to_string(),
        name: "fk_orders_customer".to_string(),
        table_name: "orders".to_string(),
        constraint_type: "FOREIGN KEY".to_string(),
        is_deferrable: false,
        initially_deferred: false,
        definition: Some("FOREIGN KEY (customer_id) REFERENCES public.customers (id)".to_string()),
        coninhcount: 0,
        is_enforced: true,
        no_inherit: false,
        nulls_not_distinct: false,
        comment: None,
    };
    let table = Table::new(
        "public".to_string(),
        "orders".to_string(),
        "public".to_string(),
        "orders".to_string(),
        "postgres".to_string(),
        None,
        vec![],
        vec![fk],
        vec![index],
        vec![],
        None,
    );
    to_dump.tables.push(table);

    let mut comparer = Comparer::new(from_dump, to_dump, false, true, true, GrantsMode::Ignore);
    comparer.set_output_for_production(true);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();

    let commit_pos = script.find("commit;").expect("script must contain commit;");
    let concurrent_pos = script
        .find(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_orders_total ON public.orders USING btree (total);",
        )
        .expect("concurrent index build must be present");
    let validate_pos = script
        .find("validate constraint fk_orders_customer;")
        .expect("FK validation must be present");
    let not_valid_pos = script
        .find("not valid;")
        .expect("FK must be added NOT VALID");

    // Concurrent build and FK validation run after COMMIT.
    assert!(
        concurrent_pos > commit_pos,
        "concurrent index must come after commit;\n{script}"
    );
    assert!(
        validate_pos > commit_pos,
        "validate constraint must come after commit;\n{script}"
    );
    // The FK is added NOT VALID before COMMIT.
    assert!(
        not_valid_pos < commit_pos,
        "FK NOT VALID must be added before commit;\n{script}"
    );
    // The in-transaction CREATE TABLE must not carry an inline (non-concurrent)
    // index build.
    let in_txn = &script[..commit_pos];
    assert!(
        !in_txn.contains("CREATE INDEX idx_orders_total ON public.orders"),
        "index must be deferred out of the in-transaction create:\n{in_txn}"
    );
}

#[tokio::test]
async fn output_for_production_disabled_keeps_inline_index() {
    // Same shape as above but with the flag off: the index is inline in the
    // CREATE TABLE and there is no post-commit / CONCURRENTLY output.
    let from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let index = TableIndex {
        schema: "public".to_string(),
        table: "orders".to_string(),
        name: "idx_orders_total".to_string(),
        catalog: None,
        indexdef: "CREATE INDEX idx_orders_total ON public.orders USING btree (total)".to_string(),
        is_partition_index: false,
        comment: None,
    };
    let table = Table::new(
        "public".to_string(),
        "orders".to_string(),
        "public".to_string(),
        "orders".to_string(),
        "postgres".to_string(),
        None,
        vec![],
        vec![],
        vec![index],
        vec![],
        None,
    );
    to_dump.tables.push(table);

    let mut comparer = Comparer::new(from_dump, to_dump, false, true, true, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("CREATE INDEX idx_orders_total ON public.orders USING btree (total);"),
        "default mode keeps the inline index:\n{script}"
    );
    assert!(
        !script.contains("CONCURRENTLY"),
        "default mode must not emit CONCURRENTLY:\n{script}"
    );
    assert!(
        !script.contains("Production post-commit"),
        "default mode must not emit a post-commit section:\n{script}"
    );
}

#[tokio::test]
async fn production_header_mentions_commit_only_with_single_transaction() {
    // The production header comment must not claim post-transaction statements
    // run "after COMMIT" when --use-single-transaction is off (no COMMIT is
    // emitted in that valid configuration).
    let build = || {
        let mut to_dump = Dump::new(DumpConfig::default());
        to_dump.tables.push(Table::new(
            "public".to_string(),
            "orders".to_string(),
            "public".to_string(),
            "orders".to_string(),
            "postgres".to_string(),
            None,
            vec![],
            vec![],
            vec![],
            vec![],
            None,
        ));
        (Dump::new(DumpConfig::default()), to_dump)
    };

    // With single transaction: header says "after COMMIT" and a commit; exists.
    let (from_txn, to_txn) = build();
    let mut with_txn = Comparer::new(from_txn, to_txn, false, true, true, GrantsMode::Ignore);
    with_txn.set_output_for_production(true);
    with_txn.compare().await.unwrap();
    let txn_script = with_txn.get_script();
    assert!(
        txn_script.contains("are emitted after COMMIT"),
        "single-transaction header must mention COMMIT:\n{txn_script}"
    );
    assert!(txn_script.contains("commit;"), "{txn_script}");

    // Without single transaction: no COMMIT, so the header must not claim one.
    let (from_no, to_no) = build();
    let mut no_txn = Comparer::new(from_no, to_no, false, false, true, GrantsMode::Ignore);
    no_txn.set_output_for_production(true);
    no_txn.compare().await.unwrap();
    let no_txn_script = no_txn.get_script();
    assert!(
        !no_txn_script.contains("commit;"),
        "no transaction must be opened without --use-single-transaction:\n{no_txn_script}"
    );
    assert!(
        !no_txn_script.contains("after COMMIT"),
        "header must not claim 'after COMMIT' without a transaction:\n{no_txn_script}"
    );
    assert!(
        no_txn_script.contains("are emitted in a separate section at the end"),
        "header must describe the trailing section accurately:\n{no_txn_script}"
    );
}
