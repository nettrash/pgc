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

// ---- fix:replident: REPLICA IDENTITY USING INDEX under production output ----

/// `public.ri (id, code)` with a unique index on `columns` named `ri_code_uk`
/// and, when `using` is set, REPLICA IDENTITY USING INDEX on it.
fn replica_identity_table(columns: &str, using: bool) -> Table {
    let index = TableIndex {
        schema: "public".to_string(),
        table: "ri".to_string(),
        name: "ri_code_uk".to_string(),
        catalog: None,
        indexdef: format!("CREATE UNIQUE INDEX ri_code_uk ON public.ri USING btree ({columns})"),
        is_partition_index: false,
        comment: None,
    };
    let mut table = Table::new(
        "public".to_string(),
        "ri".to_string(),
        "public".to_string(),
        "ri".to_string(),
        "postgres".to_string(),
        None,
        vec![],
        vec![],
        vec![index],
        vec![],
        None,
    );
    if using {
        table.replica_identity = Some("i".to_string());
        table.replica_identity_index = Some("ri_code_uk".to_string());
    } else {
        table.replica_identity = Some("d".to_string());
    }
    table.hash();
    table
}

fn find(script: &str, needle: &str) -> usize {
    script
        .find(needle)
        .unwrap_or_else(|| panic!("`{needle}` missing from:\n{script}"))
}

const USING_RI_CODE_UK: &str = "alter table public.ri replica identity using index ri_code_uk;";

#[tokio::test]
async fn production_new_table_sets_replica_identity_after_the_concurrent_build() {
    let from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());
    to_dump.tables.push(replica_identity_table("code", true));

    let mut comparer = Comparer::new(from_dump, to_dump, true, true, true, GrantsMode::Ignore);
    comparer.set_output_for_production(true);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();

    let commit = find(&script, "commit;");
    let build = find(
        &script,
        "CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS ri_code_uk ON public.ri",
    );
    let using = find(&script, USING_RI_CODE_UK);
    assert!(commit < build && build < using, "{script}");
    assert_eq!(script.matches("replica identity").count(), 1, "{script}");
}

#[tokio::test]
async fn production_rebuild_leaves_replica_identity_in_txn_and_returns_post_commit() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());
    from_dump.tables.push(replica_identity_table("code", true));
    to_dump
        .tables
        .push(replica_identity_table("code, id", true));

    let mut comparer = Comparer::new(from_dump, to_dump, true, true, true, GrantsMode::Ignore);
    comparer.set_output_for_production(true);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();

    // DEFAULT before COMMIT, so the table is never left on a missing index
    // (which the server treats as NOTHING) while the index is rebuilt.
    let reset = find(&script, "alter table public.ri replica identity default;");
    let commit = find(&script, "commit;");
    let drop = find(
        &script,
        "drop index concurrently if exists public.ri_code_uk;",
    );
    let build = find(
        &script,
        "CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS ri_code_uk ON public.ri",
    );
    let using = find(&script, USING_RI_CODE_UK);
    assert!(
        reset < commit && commit < drop && drop < build && build < using,
        "{script}"
    );
}

#[tokio::test]
async fn default_mode_keeps_replica_identity_inline_after_the_index() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());
    from_dump.tables.push(replica_identity_table("code", false));
    to_dump
        .tables
        .push(replica_identity_table("code, id", true));

    let mut comparer = Comparer::new(from_dump, to_dump, true, true, true, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();

    let build = find(&script, "CREATE UNIQUE INDEX ri_code_uk ON public.ri");
    let using = find(&script, USING_RI_CODE_UK);
    let commit = find(&script, "commit;");
    assert!(build < using && using < commit, "{script}");
    assert!(!script.contains("replica identity default"), "{script}");
}

/// `public.pp (id, k)`, partitioned by RANGE (id), with the unique index
/// `pp_id_uk (id)` when `indexed`.
fn replica_identity_parent(indexed: bool) -> Table {
    let mut id = super::helpers::int_column("public", "pp", "id", 1);
    id.is_nullable = false;
    let indexes = if indexed {
        vec![TableIndex {
            schema: "public".to_string(),
            table: "pp".to_string(),
            name: "pp_id_uk".to_string(),
            catalog: None,
            indexdef: "CREATE UNIQUE INDEX pp_id_uk ON ONLY public.pp USING btree (id)".to_string(),
            is_partition_index: false,
            comment: None,
        }]
    } else {
        vec![]
    };
    let mut table = Table::new(
        "public".to_string(),
        "pp".to_string(),
        "public".to_string(),
        "pp".to_string(),
        "postgres".to_string(),
        None,
        vec![id],
        vec![],
        indexes,
        vec![],
        None,
    );
    table.partition_key = Some("RANGE (id)".to_string());
    table.hash();
    table
}

/// Partition `public.<name>` of `public.pp`. With `indexed` it carries
/// `<name>_id_idx`, the index PostgreSQL attaches to `pp_id_uk`, and is on
/// REPLICA IDENTITY USING INDEX of it.
fn replica_identity_partition(name: &str, bound: &str, indexed: bool) -> Table {
    let mut id = super::helpers::int_column("public", name, "id", 1);
    id.is_nullable = false;
    let index_name = format!("{name}_id_idx");
    let indexes = if indexed {
        vec![TableIndex {
            schema: "public".to_string(),
            table: name.to_string(),
            name: index_name.clone(),
            catalog: None,
            indexdef: format!("CREATE UNIQUE INDEX {index_name} ON public.{name} USING btree (id)"),
            is_partition_index: true,
            comment: None,
        }]
    } else {
        vec![]
    };
    let mut table = Table::new(
        "public".to_string(),
        name.to_string(),
        "public".to_string(),
        name.to_string(),
        "postgres".to_string(),
        None,
        vec![id],
        vec![],
        indexes,
        vec![],
        None,
    );
    table.partition_of = Some("public.pp".to_string());
    table.partition_bound = Some(bound.to_string());
    if indexed {
        table.replica_identity = Some("i".to_string());
        table.replica_identity_index = Some(index_name);
        table.replica_identity_columns = vec!["id".to_string()];
    } else {
        table.replica_identity = Some("d".to_string());
    }
    table.hash();
    table
}

/// FROM: `pp` with partition `p1`, no index. TO: `pp` gains `pp_id_uk`,
/// and `p1` and a new partition `p2` use their partition of it as replica
/// identity.
fn replica_identity_partition_dumps() -> (Dump, Dump) {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());
    from_dump.tables.push(replica_identity_parent(false));
    from_dump.tables.push(replica_identity_partition(
        "p1",
        "FOR VALUES FROM (0) TO (100)",
        false,
    ));
    to_dump.tables.push(replica_identity_parent(true));
    to_dump.tables.push(replica_identity_partition(
        "p1",
        "FOR VALUES FROM (0) TO (100)",
        true,
    ));
    to_dump.tables.push(replica_identity_partition(
        "p2",
        "FOR VALUES FROM (100) TO (200)",
        true,
    ));
    (from_dump, to_dump)
}

/// Review of fix:replident: a new partition is created before its parent
/// is altered, so USING INDEX on the index PostgreSQL creates for it from a
/// parent index added in the same migration used to run first and fail
/// ("index ... does not exist"), aborting the migration.
#[tokio::test]
async fn partition_replica_identity_follows_the_parent_index_it_comes_from() {
    let (from_dump, to_dump) = replica_identity_partition_dumps();
    let mut comparer = Comparer::new(from_dump, to_dump, true, true, true, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();

    let create_p2 = find(&script, "create table public.p2 partition of public.pp");
    let parent_index = find(&script, "CREATE UNIQUE INDEX pp_id_uk ON public.pp");
    assert!(create_p2 < parent_index, "{script}");
    for partition in ["p1", "p2"] {
        let using = format!(
            "alter table public.{partition} replica identity using index {partition}_id_idx;"
        );
        assert_eq!(script.matches(&using).count(), 1, "{script}");
        let guard = find(
            &script,
            &format!("where indexrelid = to_regclass('public.{partition}_id_idx')"),
        );
        assert!(
            parent_index < guard && guard < find(&script, &using),
            "{script}"
        );
    }
    assert!(!script.contains("replica identity default"), "{script}");
}

#[tokio::test]
async fn production_partition_replica_identity_runs_after_the_attach() {
    // The concurrent per-partition build names a new partition index
    // `<partition>_<parent index>`, so USING INDEX on the name PostgreSQL
    // chose in TO is guarded: it runs after the attach and only sets the
    // replica identity when an index of that name is there.
    let (from_dump, to_dump) = replica_identity_partition_dumps();
    let mut comparer = Comparer::new(from_dump, to_dump, true, true, true, GrantsMode::Ignore);
    comparer.set_output_for_production(true);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();

    let commit = find(&script, "commit;");
    for partition in ["p1", "p2"] {
        let attach = find(
            &script,
            &format!("alter index public.pp_id_uk attach partition public.{partition}_pp_id_uk;"),
        );
        let guard = find(
            &script,
            &format!("where indexrelid = to_regclass('public.{partition}_id_idx')"),
        );
        assert!(commit < attach && attach < guard, "{script}");
    }
}

/// Review of fix:replident: a generated-column rewrite of a column the
/// replica-identity index does not use left the table on DEFAULT from
/// COMMIT until a post-commit USING INDEX, behind every other post-commit
/// statement. The table now stays on its index.
#[tokio::test]
async fn production_rewrite_of_an_unrelated_column_keeps_the_replica_identity() {
    let make = |generated: bool| {
        let mut code = super::helpers::int_column("public", "zg", "code", 1);
        code.is_nullable = false;
        let mut g = super::helpers::int_column("public", "zg", "g", 2);
        if generated {
            g.is_generated = "ALWAYS".to_string();
            g.generation_expression = Some("(code * 2)".to_string());
            g.generation_type = Some("s".to_string());
        }
        let mut table = Table::new(
            "public".to_string(),
            "zg".to_string(),
            "public".to_string(),
            "zg".to_string(),
            "postgres".to_string(),
            None,
            vec![code, g],
            vec![],
            vec![TableIndex {
                schema: "public".to_string(),
                table: "zg".to_string(),
                name: "zg_uk".to_string(),
                catalog: None,
                indexdef: "CREATE UNIQUE INDEX zg_uk ON public.zg USING btree (code)".to_string(),
                is_partition_index: false,
                comment: None,
            }],
            vec![],
            None,
        );
        table.replica_identity = Some("i".to_string());
        table.replica_identity_index = Some("zg_uk".to_string());
        table.replica_identity_columns = vec!["code".to_string()];
        table.hash();
        table
    };
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());
    from_dump.tables.push(make(false));
    to_dump.tables.push(make(true));

    for production in [false, true] {
        let mut comparer = Comparer::new(
            from_dump.clone(),
            to_dump.clone(),
            true,
            true,
            true,
            GrantsMode::Ignore,
        );
        comparer.set_output_for_production(production);
        comparer.compare().await.unwrap();
        let script = comparer.get_script();
        find(&script, "alter table public.zg add column");
        assert!(!script.contains("replica identity"), "{script}");
    }
}
