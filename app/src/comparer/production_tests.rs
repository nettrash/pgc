use super::*;

fn index(schema: &str, table: &str, name: &str, indexdef: &str) -> TableIndex {
    TableIndex {
        schema: schema.to_string(),
        table: table.to_string(),
        name: name.to_string(),
        catalog: None,
        indexdef: indexdef.to_string(),
        is_partition_index: false,
        comment: None,
    }
}

fn fk(schema: &str, table: &str, name: &str, definition: &str) -> TableConstraint {
    TableConstraint {
        catalog: "postgres".to_string(),
        schema: schema.to_string(),
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

fn empty_ctx() -> (
    HashSet<String>,
    HashMap<String, Vec<ChildRef>>,
    HashSet<String>,
) {
    (HashSet::new(), HashMap::new(), HashSet::new())
}

#[test]
fn plain_index_becomes_concurrent_post_commit() {
    let (parents, children, part_idx) = empty_ctx();
    let ctx = PartitionContext {
        partitioned_parents: &parents,
        children: &children,
        partitioned_indexes: &part_idx,
    };
    let idx = index(
        "public",
        "orders",
        "idx_orders_total",
        "CREATE INDEX idx_orders_total ON public.orders USING btree (total)",
    );
    let split = index_create_split(&idx, &ctx, false);
    assert!(split.in_txn.is_empty());
    assert!(
        split.post_commit.contains(
            "CREATE INDEX CONCURRENTLY idx_orders_total ON public.orders USING btree (total);"
        ),
        "got: {}",
        split.post_commit
    );
}

#[test]
fn unique_index_keyword_preserved() {
    let (parents, children, part_idx) = empty_ctx();
    let ctx = PartitionContext {
        partitioned_parents: &parents,
        children: &children,
        partitioned_indexes: &part_idx,
    };
    let idx = index(
        "public",
        "orders",
        "uq_orders_ref",
        "CREATE UNIQUE INDEX uq_orders_ref ON public.orders USING btree (ref)",
    );
    let split = index_create_split(&idx, &ctx, false);
    assert!(
        split
            .post_commit
            .contains("CREATE UNIQUE INDEX CONCURRENTLY uq_orders_ref"),
        "got: {}",
        split.post_commit
    );
}

#[test]
fn index_comment_follows_concurrent_build() {
    let (parents, children, part_idx) = empty_ctx();
    let ctx = PartitionContext {
        partitioned_parents: &parents,
        children: &children,
        partitioned_indexes: &part_idx,
    };
    let mut idx = index(
        "public",
        "orders",
        "idx_total",
        "CREATE INDEX idx_total ON public.orders USING btree (total)",
    );
    idx.comment = Some("by total".to_string());
    let split = index_create_split(&idx, &ctx, false);
    assert!(split.post_commit.contains("CREATE INDEX CONCURRENTLY"));
    assert!(
        split
            .post_commit
            .contains("comment on index public.idx_total is 'by total';")
    );
}

#[test]
fn partitioned_parent_expands_to_only_plus_attach() {
    let mut parents = HashSet::new();
    parents.insert("public.orders".to_string());
    let mut children = HashMap::new();
    children.insert(
        "public.orders".to_string(),
        vec![
            ChildRef {
                schema: "public".to_string(),
                table: "orders_2023".to_string(),
            },
            ChildRef {
                schema: "public".to_string(),
                table: "orders_2024".to_string(),
            },
        ],
    );
    let part_idx = HashSet::new();
    let ctx = PartitionContext {
        partitioned_parents: &parents,
        children: &children,
        partitioned_indexes: &part_idx,
    };
    let idx = index(
        "public",
        "orders",
        "idx_orders_total",
        "CREATE INDEX idx_orders_total ON public.orders USING btree (total)",
    );
    let split = index_create_split(&idx, &ctx, false);

    // Parent index created ON ONLY, in-transaction.
    assert!(
        split
            .in_txn
            .contains("CREATE INDEX idx_orders_total ON ONLY public.orders USING btree (total);"),
        "in_txn: {}",
        split.in_txn
    );
    // Each partition gets a concurrent build + attach, post-commit.
    assert!(split.post_commit.contains(
        "CREATE INDEX CONCURRENTLY orders_2023_idx_orders_total ON public.orders_2023 USING btree (total);"
    ));
    assert!(split.post_commit.contains(
        "alter index public.idx_orders_total attach partition public.orders_2023_idx_orders_total;"
    ));
    assert!(split.post_commit.contains(
        "CREATE INDEX CONCURRENTLY orders_2024_idx_orders_total ON public.orders_2024 USING btree (total);"
    ));
    assert!(split.post_commit.contains(
        "alter index public.idx_orders_total attach partition public.orders_2024_idx_orders_total;"
    ));
}

#[test]
fn new_partitioned_parent_emits_plain_in_txn_no_attach() {
    // When the partitioned parent is itself created in this migration, its
    // partitions are new and empty: PostgreSQL auto-creates and attaches each
    // partition index at `PARTITION OF` time. The rewrite must NOT emit the
    // manual concurrent build + attach (that double-attaches), just a plain
    // in-transaction CREATE INDEX.
    let mut parents = HashSet::new();
    parents.insert("public.orders".to_string());
    let mut children = HashMap::new();
    children.insert(
        "public.orders".to_string(),
        vec![ChildRef {
            schema: "public".to_string(),
            table: "orders_2024".to_string(),
        }],
    );
    let part_idx = HashSet::new();
    let ctx = PartitionContext {
        partitioned_parents: &parents,
        children: &children,
        partitioned_indexes: &part_idx,
    };
    let idx = index(
        "public",
        "orders",
        "idx_orders_total",
        "CREATE INDEX idx_orders_total ON ONLY public.orders USING btree (total)",
    );
    let split = index_create_split(&idx, &ctx, true);

    assert!(
        split
            .in_txn
            .contains("CREATE INDEX idx_orders_total ON ONLY public.orders USING btree (total);"),
        "in_txn: {}",
        split.in_txn
    );
    assert!(
        split.post_commit.is_empty(),
        "new partitioned parent must not emit post-commit attach: {}",
        split.post_commit
    );
    assert!(
        !split.in_txn.contains("attach partition"),
        "must not attach partitions manually: {}",
        split.in_txn
    );
}

#[test]
fn partitioned_parent_indexdef_already_on_only_is_not_doubled() {
    // `pg_get_indexdef` emits `ON ONLY` for an index on a partitioned parent.
    // The rewrite must be idempotent and not produce `ON ONLY ONLY`.
    let mut parents = HashSet::new();
    parents.insert("data.tagged_items".to_string());
    let mut children = HashMap::new();
    children.insert(
        "data.tagged_items".to_string(),
        vec![ChildRef {
            schema: "data".to_string(),
            table: "tagged_items_p0".to_string(),
        }],
    );
    let part_idx = HashSet::new();
    let ctx = PartitionContext {
        partitioned_parents: &parents,
        children: &children,
        partitioned_indexes: &part_idx,
    };
    let idx = index(
        "data",
        "tagged_items",
        "idx_tagged_items_detail",
        "CREATE INDEX idx_tagged_items_detail ON ONLY data.tagged_items USING btree (detail)",
    );
    let split = index_create_split(&idx, &ctx, false);

    assert!(
        split.in_txn.contains(
            "CREATE INDEX idx_tagged_items_detail ON ONLY data.tagged_items USING btree (detail);"
        ),
        "in_txn: {}",
        split.in_txn
    );
    assert!(
        !split.in_txn.contains("ON ONLY ONLY"),
        "doubled ONLY: {}",
        split.in_txn
    );
}

#[test]
fn partitioned_parent_without_known_children_falls_back_in_txn() {
    let mut parents = HashSet::new();
    parents.insert("public.orders".to_string());
    let children = HashMap::new();
    let part_idx = HashSet::new();
    let ctx = PartitionContext {
        partitioned_parents: &parents,
        children: &children,
        partitioned_indexes: &part_idx,
    };
    let idx = index(
        "public",
        "orders",
        "idx_orders_total",
        "CREATE INDEX idx_orders_total ON public.orders USING btree (total)",
    );
    let split = index_create_split(&idx, &ctx, false);
    assert!(split.post_commit.is_empty());
    assert!(split.in_txn.contains("no partitions found"));
    assert!(
        split
            .in_txn
            .contains("CREATE INDEX idx_orders_total ON public.orders USING btree (total);")
    );
}

#[test]
fn drop_index_concurrent_post_commit_for_plain_table() {
    let (parents, children, part_idx) = empty_ctx();
    let ctx = PartitionContext {
        partitioned_parents: &parents,
        children: &children,
        partitioned_indexes: &part_idx,
    };
    let idx = index("public", "orders", "idx_total", "");
    let (stmt, post_commit) = index_drop_statement(&idx, &ctx);
    assert!(post_commit);
    assert_eq!(stmt, "drop index concurrently if exists public.idx_total;");
}

#[test]
fn drop_partitioned_index_stays_in_txn_non_concurrent() {
    let parents = HashSet::new();
    let children = HashMap::new();
    let mut part_idx = HashSet::new();
    part_idx.insert("public.idx_total".to_string());
    let ctx = PartitionContext {
        partitioned_parents: &parents,
        children: &children,
        partitioned_indexes: &part_idx,
    };
    let idx = index("public", "orders", "idx_total", "");
    let (stmt, post_commit) = index_drop_statement(&idx, &ctx);
    assert!(!post_commit);
    assert_eq!(stmt, "drop index if exists public.idx_total;");
}

#[test]
fn foreign_key_split_into_not_valid_and_validate() {
    let c = fk(
        "public",
        "orders",
        "fk_orders_customer",
        "FOREIGN KEY (customer_id) REFERENCES public.customers (id)",
    );
    let split = foreign_key_split(&c);
    assert!(
        split.in_txn.contains(
            "add constraint fk_orders_customer foreign key (customer_id) references public.customers (id) not valid;"
        ),
        "in_txn: {}",
        split.in_txn
    );
    assert_eq!(
        split.post_commit.trim_end(),
        "alter table public.orders validate constraint fk_orders_customer;"
    );
}

#[test]
fn non_enforced_fk_is_not_split() {
    let mut c = fk(
        "public",
        "orders",
        "fk_orders_customer",
        "FOREIGN KEY (customer_id) REFERENCES public.customers (id) NOT ENFORCED",
    );
    c.is_enforced = false;
    let split = foreign_key_split(&c);
    assert!(split.post_commit.is_empty());
    assert!(!split.in_txn.contains("not valid"));
}

#[test]
fn fk_comment_preserved_in_txn() {
    let mut c = fk(
        "public",
        "orders",
        "fk_orders_customer",
        "FOREIGN KEY (customer_id) REFERENCES public.customers (id)",
    );
    c.comment = Some("links to customer".to_string());
    let split = foreign_key_split(&c);
    assert!(split.in_txn.contains("not valid;"));
    assert!(
        split.in_txn.contains(
            "comment on constraint fk_orders_customer on public.orders is 'links to customer';"
        ),
        "in_txn: {}",
        split.in_txn
    );
    assert!(
        split
            .post_commit
            .contains("validate constraint fk_orders_customer;")
    );
}
