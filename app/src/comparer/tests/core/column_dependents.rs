//! Issue #188 — pg_depend-driven secondary dependent restoration.

use super::helpers::*;
use crate::comparer::core::*;
use crate::config::dump_config::DumpConfig;
use crate::config::grants_mode::GrantsMode;
use crate::dump::column_dependent::ColumnDependent;
use crate::dump::column_dependent::ColumnDependentKind;
use crate::dump::table::Table;
use crate::dump::table_constraint::TableConstraint;
use crate::dump::table_index::TableIndex;

/// Phase 7 / Path A: A routine signature change CASCADE-drops a
/// generated column. PostgreSQL also drops a plain index on that
/// column *because the index depends on the column, not the routine*.
/// The text-based scanner cannot see the dependency. The
/// `column_dependents` graph must drive a recreate of the index.
#[tokio::test]
async fn issue188_phase7_restores_plain_index_on_generated_column() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    from_dump
        .routines
        .push(issue179_compute_routine("integer", "SELECT x * 2;"));
    to_dump.routines.push(issue179_compute_routine(
        "bigint",
        "SELECT (x * 2)::bigint;",
    ));

    // Add a plain index ON the generated column (no function reference)
    // to both sides. PostgreSQL would CASCADE-drop it along with the
    // column; Phase 7's text scan does not detect this case.
    let plain_idx = TableIndex {
        schema: "test_deps".to_string(),
        table: "items".to_string(),
        name: "idx_gen_col".to_string(),
        catalog: Some("postgres".to_string()),
        indexdef: "CREATE INDEX idx_gen_col ON test_deps.items USING btree (gen_col)".to_string(),
        is_partition_index: false,
        comment: None,
    };

    let mut from_table =
        issue179_items_table("integer", "test_deps.compute(0)::integer", "integer");
    from_table.indexes.push(plain_idx.clone());
    from_table.hash();
    from_dump.tables.push(from_table);

    let mut to_table = issue179_items_table("bigint", "test_deps.compute(0)", "bigint");
    to_table.indexes.push(plain_idx);
    to_table.hash();
    to_dump.tables.push(to_table);

    // pg_depend at dump time recorded that `idx_gen_col` depends on
    // `gen_col`. Without this, the text scanner has no way to discover
    // the secondary dependency.
    from_dump.column_dependents.push(ColumnDependent {
        schema: "test_deps".to_string(),
        table: "items".to_string(),
        column: "gen_col".to_string(),
        kind: ColumnDependentKind::Index,
        dep_schema: "test_deps".to_string(),
        dep_table: "items".to_string(),
        dep_name: "idx_gen_col".to_string(),
    });

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_routines_and_views().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("CREATE INDEX IF NOT EXISTS idx_gen_col ON test_deps.items"),
        "plain index on generated column must be re-emitted from pg_depend graph: {}",
        script
    );
}

/// Phase 7 / Path A: same idea for a CHECK constraint that references
/// the generated column but does not name the routine.
#[tokio::test]
async fn issue188_phase7_restores_check_constraint_on_generated_column() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    from_dump
        .routines
        .push(issue179_compute_routine("integer", "SELECT x * 2;"));
    to_dump.routines.push(issue179_compute_routine(
        "bigint",
        "SELECT (x * 2)::bigint;",
    ));

    let chk_on_col = TableConstraint {
        catalog: "postgres".to_string(),
        schema: "test_deps".to_string(),
        name: "chk_gen_positive".to_string(),
        table_name: "items".to_string(),
        constraint_type: "CHECK".to_string(),
        is_deferrable: false,
        initially_deferred: false,
        // References only the generated column — no function name.
        definition: Some("CHECK (gen_col > 0)".to_string()),
        coninhcount: 0,
        is_enforced: true,
        no_inherit: false,
        nulls_not_distinct: false,
        comment: None,
    };

    let mut from_table =
        issue179_items_table("integer", "test_deps.compute(0)::integer", "integer");
    from_table.constraints.push(chk_on_col.clone());
    from_table.hash();
    from_dump.tables.push(from_table);

    let mut to_table = issue179_items_table("bigint", "test_deps.compute(0)", "bigint");
    to_table.constraints.push(chk_on_col);
    to_table.hash();
    to_dump.tables.push(to_table);

    from_dump.column_dependents.push(ColumnDependent {
        schema: "test_deps".to_string(),
        table: "items".to_string(),
        column: "gen_col".to_string(),
        kind: ColumnDependentKind::Constraint,
        dep_schema: "test_deps".to_string(),
        dep_table: "items".to_string(),
        dep_name: "chk_gen_positive".to_string(),
    });

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_routines_and_views().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("alter table test_deps.items add constraint chk_gen_positive"),
        "CHECK constraint anchored on generated column must be re-emitted: {}",
        script
    );
    assert!(
        script.contains("alter table test_deps.items drop constraint if exists chk_gen_positive;"),
        "drop-if-exists guard for column-anchored CHECK constraint missing: {}",
        script
    );
}

/// Phase 7 / Path A: TO-side gate. When the dependent is intentionally
/// removed in TO, we must NOT resurrect it via the pg_depend graph.
#[tokio::test]
async fn issue188_phase7_skips_dependent_absent_from_to() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    from_dump
        .routines
        .push(issue179_compute_routine("integer", "SELECT x * 2;"));
    to_dump.routines.push(issue179_compute_routine(
        "bigint",
        "SELECT (x * 2)::bigint;",
    ));

    // FROM has the plain index; TO deliberately omits it.
    let plain_idx = TableIndex {
        schema: "test_deps".to_string(),
        table: "items".to_string(),
        name: "idx_gen_col".to_string(),
        catalog: Some("postgres".to_string()),
        indexdef: "CREATE INDEX idx_gen_col ON test_deps.items USING btree (gen_col)".to_string(),
        is_partition_index: false,
        comment: None,
    };

    let mut from_table =
        issue179_items_table("integer", "test_deps.compute(0)::integer", "integer");
    from_table.indexes.push(plain_idx);
    from_table.hash();
    from_dump.tables.push(from_table);

    // TO-side table does NOT include `idx_gen_col`.
    to_dump.tables.push(issue179_items_table(
        "bigint",
        "test_deps.compute(0)",
        "bigint",
    ));

    from_dump.column_dependents.push(ColumnDependent {
        schema: "test_deps".to_string(),
        table: "items".to_string(),
        column: "gen_col".to_string(),
        kind: ColumnDependentKind::Index,
        dep_schema: "test_deps".to_string(),
        dep_table: "items".to_string(),
        dep_name: "idx_gen_col".to_string(),
    });

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_routines_and_views().await.unwrap();
    let script = comparer.get_script();

    assert!(
        !script.contains("idx_gen_col"),
        "index absent from TO must not be resurrected from pg_depend: {}",
        script
    );
}

/// Phase 7 / Path A: a UNIQUE/PK constraint and its backing index both
/// appear in `pg_depend`. The constraint emission already recreates the
/// index, so the dedup logic must skip the index branch when the same
/// name exists as a constraint on the TO-side table.
#[tokio::test]
async fn issue188_phase7_skips_index_backing_constraint() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    from_dump
        .routines
        .push(issue179_compute_routine("integer", "SELECT x * 2;"));
    to_dump.routines.push(issue179_compute_routine(
        "bigint",
        "SELECT (x * 2)::bigint;",
    ));

    let uniq_constraint = TableConstraint {
        catalog: "postgres".to_string(),
        schema: "test_deps".to_string(),
        name: "items_gen_col_key".to_string(),
        table_name: "items".to_string(),
        constraint_type: "UNIQUE".to_string(),
        is_deferrable: false,
        initially_deferred: false,
        definition: Some("UNIQUE (gen_col)".to_string()),
        coninhcount: 0,
        is_enforced: true,
        no_inherit: false,
        nulls_not_distinct: false,
        comment: None,
    };
    // Backing index has the same name as the constraint.
    let uniq_idx = TableIndex {
        schema: "test_deps".to_string(),
        table: "items".to_string(),
        name: "items_gen_col_key".to_string(),
        catalog: Some("postgres".to_string()),
        indexdef: "CREATE UNIQUE INDEX items_gen_col_key ON test_deps.items USING btree (gen_col)"
            .to_string(),
        is_partition_index: false,
        comment: None,
    };

    let mut from_table =
        issue179_items_table("integer", "test_deps.compute(0)::integer", "integer");
    from_table.constraints.push(uniq_constraint.clone());
    from_table.indexes.push(uniq_idx.clone());
    from_table.hash();
    from_dump.tables.push(from_table);

    let mut to_table = issue179_items_table("bigint", "test_deps.compute(0)", "bigint");
    to_table.constraints.push(uniq_constraint);
    to_table.indexes.push(uniq_idx);
    to_table.hash();
    to_dump.tables.push(to_table);

    // pg_depend records BOTH edges.
    from_dump.column_dependents.push(ColumnDependent {
        schema: "test_deps".to_string(),
        table: "items".to_string(),
        column: "gen_col".to_string(),
        kind: ColumnDependentKind::Index,
        dep_schema: "test_deps".to_string(),
        dep_table: "items".to_string(),
        dep_name: "items_gen_col_key".to_string(),
    });
    from_dump.column_dependents.push(ColumnDependent {
        schema: "test_deps".to_string(),
        table: "items".to_string(),
        column: "gen_col".to_string(),
        kind: ColumnDependentKind::Constraint,
        dep_schema: "test_deps".to_string(),
        dep_table: "items".to_string(),
        dep_name: "items_gen_col_key".to_string(),
    });

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_routines_and_views().await.unwrap();
    let script = comparer.get_script();

    // The constraint emission must fire …
    assert!(
        script.contains("alter table test_deps.items add constraint items_gen_col_key"),
        "UNIQUE constraint must be re-emitted: {}",
        script
    );
    // … and the backing-index CREATE must NOT also be emitted (the
    // constraint creates the index implicitly).
    assert!(
        !script.contains("CREATE UNIQUE INDEX IF NOT EXISTS items_gen_col_key"),
        "backing index must be skipped when a same-named constraint emission already recreates it: {}",
        script
    );
}

/// Path B: a STORED → VIRTUAL flip routes the column through the
/// `DROP COLUMN` + `ADD COLUMN` branch in `TableColumn::get_alter_script`
/// (issue #181). PostgreSQL CASCADE-drops a plain index attached to the
/// column. `compare_tables` must walk the column-dependent graph and
/// emit the recreate.
#[tokio::test]
async fn issue188_path_b_virtual_flip_restores_dependent_index() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let value_col = {
        let mut c = int_column("test_deps", "items", "value", 1);
        c.is_nullable = false;
        c
    };

    let mut from_gen_col = int_column("test_deps", "items", "gen_col", 2);
    from_gen_col.data_type = "integer".to_string();
    from_gen_col.is_generated = "ALWAYS".to_string();
    from_gen_col.generation_expression = Some("(value * 2)".to_string());
    from_gen_col.generation_type = Some("s".to_string()); // STORED in FROM

    let mut to_gen_col = from_gen_col.clone();
    to_gen_col.generation_type = Some("v".to_string()); // VIRTUAL in TO

    let plain_idx = TableIndex {
        schema: "test_deps".to_string(),
        table: "items".to_string(),
        name: "idx_gen_col".to_string(),
        catalog: Some("postgres".to_string()),
        indexdef: "CREATE INDEX idx_gen_col ON test_deps.items USING btree (gen_col)".to_string(),
        is_partition_index: false,
        comment: None,
    };

    let mut from_table = Table::new(
        "test_deps".to_string(),
        "items".to_string(),
        "test_deps".to_string(),
        "items".to_string(),
        "postgres".to_string(),
        None,
        vec![value_col.clone(), from_gen_col],
        vec![],
        vec![plain_idx.clone()],
        vec![],
        None,
    );
    from_table.hash();
    from_dump.tables.push(from_table);

    let mut to_table = Table::new(
        "test_deps".to_string(),
        "items".to_string(),
        "test_deps".to_string(),
        "items".to_string(),
        "postgres".to_string(),
        None,
        vec![value_col, to_gen_col],
        vec![],
        vec![plain_idx],
        vec![],
        None,
    );
    to_table.hash();
    to_dump.tables.push(to_table);

    from_dump.column_dependents.push(ColumnDependent {
        schema: "test_deps".to_string(),
        table: "items".to_string(),
        column: "gen_col".to_string(),
        kind: ColumnDependentKind::Index,
        dep_schema: "test_deps".to_string(),
        dep_table: "items".to_string(),
        dep_name: "idx_gen_col".to_string(),
    });

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_tables().await.unwrap();
    let script = comparer.get_script();

    // The drop+add for the column must fire (Path B trigger).
    assert!(
        script.contains("drop column"),
        "STORED→VIRTUAL flip should DROP COLUMN: {}",
        script
    );
    // The recreate block must include the dependent index.
    assert!(
        script.contains("Recreate dependents dropped by virtual-column rewrite"),
        "labeled recreate block must wrap Path B dependents: {}",
        script
    );
    assert!(
        script.contains("CREATE INDEX IF NOT EXISTS idx_gen_col ON test_deps.items"),
        "plain index on virtually-recreated column must be re-emitted: {}",
        script
    );
}

/// Phase 7 / Path A: an FK on a *different* table referencing the
/// generated column on the anchor table. The pg_depend row's
/// `refobjid` points at the parent table (where the column lives) but
/// `con.conrelid` points at the child (where the FK lives) — the
/// `dep_schema`/`dep_table` in `ColumnDependent` must be the child's,
/// not the anchor's. Locks in correct behaviour for the asymmetric
/// `conrelid` vs `refobjid` case (PR #196 review).
#[tokio::test]
async fn issue188_phase7_restores_fk_on_different_table() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    from_dump
        .routines
        .push(issue179_compute_routine("integer", "SELECT x * 2;"));
    to_dump.routines.push(issue179_compute_routine(
        "bigint",
        "SELECT (x * 2)::bigint;",
    ));

    // Parent table: the standard issue179 items table — gen_col is
    // the anchor whose CASCADE drop the test exercises.
    from_dump.tables.push(issue179_items_table(
        "integer",
        "test_deps.compute(0)::integer",
        "integer",
    ));
    to_dump.tables.push(issue179_items_table(
        "bigint",
        "test_deps.compute(0)",
        "bigint",
    ));

    // Child table: separate table whose FK references gen_col on the
    // parent. The FK's own definition contains no function name; the
    // text scanner cannot see this dependency. The pg_depend graph
    // must drive the re-emission.
    let make_child = |ref_type: &str| {
        let mut id_col = int_column("test_deps", "items_child", "id", 1);
        id_col.is_nullable = false;

        let mut ref_col = int_column("test_deps", "items_child", "ref_gen", 2);
        ref_col.data_type = ref_type.to_string();

        let fk = TableConstraint {
            catalog: "postgres".to_string(),
            schema: "test_deps".to_string(),
            name: "fk_items_child_ref_gen".to_string(),
            table_name: "items_child".to_string(),
            constraint_type: "FOREIGN KEY".to_string(),
            is_deferrable: false,
            initially_deferred: false,
            definition: Some(
                "FOREIGN KEY (ref_gen) REFERENCES test_deps.items (gen_col)".to_string(),
            ),
            coninhcount: 0,
            is_enforced: true,
            no_inherit: false,
            nulls_not_distinct: false,
            comment: None,
        };

        let mut t = Table::new(
            "test_deps".to_string(),
            "items_child".to_string(),
            "test_deps".to_string(),
            "items_child".to_string(),
            "postgres".to_string(),
            None,
            vec![id_col, ref_col],
            vec![fk],
            vec![],
            vec![],
            None,
        );
        t.hash();
        t
    };
    from_dump.tables.push(make_child("integer"));
    to_dump.tables.push(make_child("bigint"));

    // Anchor is the parent column (gen_col on items). dep_table is
    // the *child* (items_child) because the FK constraint's
    // `conrelid` points at the child, not the parent where the
    // depended-on column lives.
    from_dump.column_dependents.push(ColumnDependent {
        schema: "test_deps".to_string(),
        table: "items".to_string(),
        column: "gen_col".to_string(),
        kind: ColumnDependentKind::Constraint,
        dep_schema: "test_deps".to_string(),
        dep_table: "items_child".to_string(),
        dep_name: "fk_items_child_ref_gen".to_string(),
    });

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_routines_and_views().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("alter table test_deps.items_child add constraint fk_items_child_ref_gen"),
        "FK on a different table must be re-emitted via pg_depend graph: {}",
        script
    );
    assert!(
        script.contains(
            "alter table test_deps.items_child drop constraint if exists fk_items_child_ref_gen;"
        ),
        "drop-if-exists guard for cross-table FK missing: {}",
        script
    );
}

/// Phase 7 / Path A: when the anchor column has both a UNIQUE
/// constraint and an FK on another table referencing it, the FK must
/// be emitted *after* the UNIQUE constraint — PostgreSQL rejects
/// `ADD CONSTRAINT … FOREIGN KEY` when the referenced columns lack a
/// unique constraint. Two-pass ordering in `recreate_column_dependents`.
#[tokio::test]
async fn issue188_phase7_emits_fk_after_unique_target() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    from_dump
        .routines
        .push(issue179_compute_routine("integer", "SELECT x * 2;"));
    to_dump.routines.push(issue179_compute_routine(
        "bigint",
        "SELECT (x * 2)::bigint;",
    ));

    let uniq_constraint = TableConstraint {
        catalog: "postgres".to_string(),
        schema: "test_deps".to_string(),
        name: "items_gen_col_uniq".to_string(),
        table_name: "items".to_string(),
        constraint_type: "UNIQUE".to_string(),
        is_deferrable: false,
        initially_deferred: false,
        definition: Some("UNIQUE (gen_col)".to_string()),
        coninhcount: 0,
        is_enforced: true,
        no_inherit: false,
        nulls_not_distinct: false,
        comment: None,
    };

    let mut from_items =
        issue179_items_table("integer", "test_deps.compute(0)::integer", "integer");
    from_items.constraints.push(uniq_constraint.clone());
    from_items.hash();
    from_dump.tables.push(from_items);

    let mut to_items = issue179_items_table("bigint", "test_deps.compute(0)", "bigint");
    to_items.constraints.push(uniq_constraint);
    to_items.hash();
    to_dump.tables.push(to_items);

    let make_child = |ref_type: &str| {
        let mut id_col = int_column("test_deps", "items_child", "id", 1);
        id_col.is_nullable = false;
        let mut ref_col = int_column("test_deps", "items_child", "ref_gen", 2);
        ref_col.data_type = ref_type.to_string();

        let fk = TableConstraint {
            catalog: "postgres".to_string(),
            schema: "test_deps".to_string(),
            name: "fk_items_child_ref_gen".to_string(),
            table_name: "items_child".to_string(),
            constraint_type: "FOREIGN KEY".to_string(),
            is_deferrable: false,
            initially_deferred: false,
            definition: Some(
                "FOREIGN KEY (ref_gen) REFERENCES test_deps.items (gen_col)".to_string(),
            ),
            coninhcount: 0,
            is_enforced: true,
            no_inherit: false,
            nulls_not_distinct: false,
            comment: None,
        };

        let mut t = Table::new(
            "test_deps".to_string(),
            "items_child".to_string(),
            "test_deps".to_string(),
            "items_child".to_string(),
            "postgres".to_string(),
            None,
            vec![id_col, ref_col],
            vec![fk],
            vec![],
            vec![],
            None,
        );
        t.hash();
        t
    };
    from_dump.tables.push(make_child("integer"));
    to_dump.tables.push(make_child("bigint"));

    // FK first in the column_dependents vec — the helper must defer
    // it regardless of input order so it lands after the UNIQUE.
    from_dump.column_dependents.push(ColumnDependent {
        schema: "test_deps".to_string(),
        table: "items".to_string(),
        column: "gen_col".to_string(),
        kind: ColumnDependentKind::Constraint,
        dep_schema: "test_deps".to_string(),
        dep_table: "items_child".to_string(),
        dep_name: "fk_items_child_ref_gen".to_string(),
    });
    from_dump.column_dependents.push(ColumnDependent {
        schema: "test_deps".to_string(),
        table: "items".to_string(),
        column: "gen_col".to_string(),
        kind: ColumnDependentKind::Constraint,
        dep_schema: "test_deps".to_string(),
        dep_table: "items".to_string(),
        dep_name: "items_gen_col_uniq".to_string(),
    });

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_routines_and_views().await.unwrap();
    let script = comparer.get_script();

    let uniq_pos = script
        .find("add constraint items_gen_col_uniq")
        .expect("UNIQUE constraint must be re-emitted");
    let fk_pos = script
        .find("add constraint fk_items_child_ref_gen")
        .expect("FK constraint must be re-emitted");
    assert!(
        uniq_pos < fk_pos,
        "FK must be emitted AFTER its UNIQUE target; got uniq@{} fk@{}: {}",
        uniq_pos,
        fk_pos,
        script
    );
}

/// fix:replident — a key column of a REPLICA IDENTITY USING INDEX turns into
/// a stored generated column, which pgc does as DROP COLUMN + ADD COLUMN.
/// The drop takes the index with it, so USING INDEX has to follow the
/// re-created index: after the recreate block by default, and after its
/// post-commit concurrent build under --output-for-production.
#[tokio::test]
async fn replica_identity_follows_the_index_re_created_after_a_column_rewrite() {
    let build = || {
        let mut from_dump = Dump::new(DumpConfig::default());
        let mut to_dump = Dump::new(DumpConfig::default());
        let index = TableIndex {
            schema: "public".to_string(),
            table: "ri".to_string(),
            name: "ri_code_uk".to_string(),
            catalog: None,
            indexdef: "CREATE UNIQUE INDEX ri_code_uk ON public.ri USING btree (code)".to_string(),
            is_partition_index: false,
            comment: None,
        };
        let make = |generated: bool| {
            let mut code = int_column("public", "ri", "code", 2);
            code.is_nullable = false;
            if generated {
                code.is_generated = "ALWAYS".to_string();
                code.generation_expression = Some("(id * 2)".to_string());
                code.generation_type = Some("s".to_string());
            }
            let mut id = int_column("public", "ri", "id", 1);
            id.is_nullable = false;
            let mut table = Table::new(
                "public".to_string(),
                "ri".to_string(),
                "public".to_string(),
                "ri".to_string(),
                "postgres".to_string(),
                None,
                vec![id, code],
                vec![],
                vec![index.clone()],
                vec![],
                None,
            );
            table.replica_identity = Some("i".to_string());
            table.replica_identity_index = Some("ri_code_uk".to_string());
            table.hash();
            table
        };
        from_dump.tables.push(make(false));
        to_dump.tables.push(make(true));
        from_dump.column_dependents.push(ColumnDependent {
            schema: "public".to_string(),
            table: "ri".to_string(),
            column: "code".to_string(),
            kind: ColumnDependentKind::Index,
            dep_schema: "public".to_string(),
            dep_table: "ri".to_string(),
            dep_name: "ri_code_uk".to_string(),
        });
        (from_dump, to_dump)
    };
    let pos = |script: &str, needle: &str| {
        script
            .find(needle)
            .unwrap_or_else(|| panic!("`{needle}` missing from:\n{script}"))
    };
    let using = "alter table public.ri replica identity using index ri_code_uk;";

    let (from_dump, to_dump) = build();
    let mut comparer = Comparer::new(from_dump, to_dump, true, true, true, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();
    let reset = pos(&script, "alter table public.ri replica identity default;");
    let drop = pos(&script, "alter table public.ri drop column code;");
    let recreate = pos(
        &script,
        "CREATE UNIQUE INDEX IF NOT EXISTS ri_code_uk ON public.ri",
    );
    assert!(
        reset < drop && drop < recreate && recreate < pos(&script, using),
        "{script}"
    );
    assert_eq!(script.matches(using).count(), 1, "{script}");

    let (from_dump, to_dump) = build();
    let mut comparer = Comparer::new(from_dump, to_dump, true, true, true, GrantsMode::Ignore);
    comparer.set_output_for_production(true);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();
    let commit = pos(&script, "commit;");
    let recreate = pos(
        &script,
        "CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS ri_code_uk ON public.ri",
    );
    assert!(
        commit < recreate && recreate < pos(&script, using),
        "{script}"
    );
    assert_eq!(script.matches(using).count(), 1, "{script}");
}

// ==== r3-gencol tests ====

use crate::dump::table_column::TableColumn;
use crate::dump::table_policy::TablePolicy;
use crate::dump::table_trigger::TableTrigger;
use sqlx::postgres::types::Oid;

/// `s.<name>` with `columns`, partitioned by `k` or a partition of
/// `s.<parent>`.
fn r3g_table(
    name: &str,
    columns: Vec<TableColumn>,
    partition_of: Option<&str>,
    partitioned: bool,
) -> Table {
    let mut table = Table::new(
        "s".to_string(),
        name.to_string(),
        "s".to_string(),
        name.to_string(),
        "postgres".to_string(),
        None,
        columns,
        vec![],
        vec![],
        vec![],
        None,
    );
    if let Some(parent) = partition_of {
        table.partition_of = Some(format!("s.{parent}"));
        table.partition_bound = Some("FOR VALUES IN (1)".to_string());
    }
    if partitioned {
        table.partition_key = Some("LIST (k)".to_string());
    }
    table.hash();
    table
}

fn r3g_column(table: &str, name: &str, ordinal: i32, data_type: &str) -> TableColumn {
    let mut column = int_column("s", table, name, ordinal);
    column.data_type = data_type.to_string();
    column
}

/// `g integer GENERATED ALWAYS AS (a + 1) STORED`.
fn r3g_generated(table: &str, ordinal: i32) -> TableColumn {
    let mut column = int_column("s", table, "g", ordinal);
    column.is_generated = "ALWAYS".to_string();
    column.generation_expression = Some("(a + 1)".to_string());
    column.generation_type = Some("s".to_string());
    column
}

fn r3g_policy(table: &str, name: &str, using: &str) -> TablePolicy {
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

fn r3g_edge(table: &str, column: &str, kind: ColumnDependentKind, dep: &str) -> ColumnDependent {
    ColumnDependent {
        schema: "s".to_string(),
        table: table.to_string(),
        column: column.to_string(),
        kind,
        dep_schema: "s".to_string(),
        dep_table: table.to_string(),
        dep_name: dep.to_string(),
    }
}

fn r3g_at(script: &str, needle: &str) -> usize {
    script
        .find(needle)
        .unwrap_or_else(|| panic!("expected {needle:?} in:\n{script}"))
}

/// A partitioned `s.p (id, k, a <a_type>, g)` whose partition `s.p1` has its
/// own index, CHECK and policy on `g`.
fn r3g_partition_dump(a_type: &str) -> Dump {
    let mut dump = Dump::new(DumpConfig::default());
    dump.server_version_num = Some(170000);
    let columns = |table: &str| {
        vec![
            r3g_column(table, "id", 1, "integer"),
            r3g_column(table, "k", 2, "integer"),
            r3g_column(table, "a", 3, a_type),
            r3g_generated(table, 4),
        ]
    };
    dump.tables.push(r3g_table("p", columns("p"), None, true));
    let mut p1 = r3g_table("p1", columns("p1"), Some("p"), false);
    p1.indexes.push(TableIndex {
        schema: "s".to_string(),
        table: "p1".to_string(),
        name: "p1_g".to_string(),
        catalog: None,
        indexdef: "CREATE INDEX p1_g ON s.p1 USING btree (g)".to_string(),
        is_partition_index: false,
        comment: None,
    });
    p1.constraints.push(TableConstraint {
        catalog: "postgres".to_string(),
        schema: "s".to_string(),
        name: "p1_chk".to_string(),
        table_name: "p1".to_string(),
        constraint_type: "CHECK".to_string(),
        is_deferrable: false,
        initially_deferred: false,
        definition: Some("CHECK ((g > 0))".to_string()),
        coninhcount: 0,
        is_enforced: true,
        no_inherit: false,
        nulls_not_distinct: false,
        comment: None,
    });
    p1.policies.push(r3g_policy("p1", "p1_pol", "(g > 0)"));
    p1.has_rowsecurity = true;
    p1.hash();
    dump.tables.push(p1);
    dump.column_dependents = vec![
        r3g_edge("p1", "g", ColumnDependentKind::Index, "p1_g"),
        r3g_edge("p1", "g", ColumnDependentKind::Constraint, "p1_chk"),
        r3g_edge("p1", "g", ColumnDependentKind::Policy, "p1_pol"),
    ];
    dump
}

#[tokio::test]
async fn r3g_partition_objects_on_a_generated_column_recreated_around_a_retype_come_back() {
    // The parent retypes `a`, which `g` reads: its recursing drop + add of
    // `g` takes the partition's own objects on its copy. The partition is
    // `table_recreated` (its column type changes through its parent), but no
    // DROP TABLE runs for it, so they are restored after the alter.
    let from = r3g_partition_dump("integer");
    let to = r3g_partition_dump("bigint");
    let mut comparer = Comparer::new(from, to, true, false, true, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();

    let predrop = r3g_at(&script, "drop policy if exists p1_pol on s.p1;");
    let drop = r3g_at(&script, "alter table s.p drop column g;");
    let add = r3g_at(&script, "alter table s.p add column g integer");
    assert!(predrop < drop && drop < add, "{script}");
    for restored in [
        "CREATE INDEX IF NOT EXISTS p1_g ON s.p1 USING btree (g);",
        "alter table s.p1 add constraint p1_chk check ((g > 0))",
        "create policy p1_pol on s.p1 for all to public using (g > 0);",
    ] {
        assert!(add < r3g_at(&script, restored), "{restored}:\n{script}");
    }
    assert!(!script.contains("drop table"), "{script}");
}

/// `s.docs (id, tenant <tenant_type>)` under the RLS policy `docs_pol`
/// (`using`), and `s.orders (id, status <status_type>)` with a `BEFORE UPDATE
/// OF status` trigger.
fn r3g_retype_dump(tenant_type: &str, status_type: &str, using: &str) -> Dump {
    let mut dump = Dump::new(DumpConfig::default());
    dump.server_version_num = Some(170000);
    let mut docs = r3g_table(
        "docs",
        vec![
            r3g_column("docs", "id", 1, "integer"),
            r3g_column("docs", "tenant", 2, tenant_type),
        ],
        None,
        false,
    );
    docs.policies.push(r3g_policy("docs", "docs_pol", using));
    docs.has_rowsecurity = true;
    docs.hash();
    let mut orders = r3g_table(
        "orders",
        vec![
            r3g_column("orders", "id", 1, "integer"),
            r3g_column("orders", "status", 2, status_type),
        ],
        None,
        false,
    );
    orders.triggers.push(TableTrigger {
        oid: Oid(1),
        name: "orders_upd".to_string(),
        definition: "CREATE TRIGGER orders_upd BEFORE UPDATE OF status ON s.orders FOR EACH ROW EXECUTE FUNCTION s.touch()".to_string(),
        enabled: "O".to_string(),
        comment: None,
    });
    orders.hash();
    dump.tables.push(docs);
    dump.tables.push(orders);
    dump.column_dependents = vec![
        r3g_edge("docs", "tenant", ColumnDependentKind::Policy, "docs_pol"),
        r3g_edge(
            "orders",
            "status",
            ColumnDependentKind::Trigger,
            "orders_upd",
        ),
    ];
    dump
}

#[tokio::test]
async fn r3g_policy_and_trigger_on_a_column_retyped_in_place_are_dropped_first() {
    // PostgreSQL refuses ALTER COLUMN ... TYPE of a column a policy or a
    // trigger (UPDATE OF, WHEN) uses.
    let from = r3g_retype_dump("integer", "text", "(tenant > 0)");
    let to = r3g_retype_dump("bigint", "character varying", "(tenant > 0)");
    let mut comparer = Comparer::new(from, to, true, false, true, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();

    let header = r3g_at(
        &script,
        "Dependents dropped ahead of a column retype: Start",
    );
    let drop_policy = r3g_at(&script, "drop policy if exists docs_pol on s.docs;");
    let drop_trigger = r3g_at(&script, "drop trigger if exists orders_upd on s.orders;");
    let retype_docs = r3g_at(
        &script,
        "alter table s.docs alter column tenant type bigint;",
    );
    let retype_orders = r3g_at(
        &script,
        "alter table s.orders alter column status type character varying;",
    );
    assert!(
        header < drop_policy && drop_policy < retype_docs,
        "{script}"
    );
    assert!(drop_trigger < retype_orders, "{script}");
    let restore_policy = r3g_at(
        &script,
        "drop policy if exists docs_pol on s.docs;\n\ncreate policy docs_pol on s.docs for all to public using (tenant > 0);",
    );
    assert!(retype_docs < restore_policy, "{script}");
    let restore_trigger = r3g_at(&script, "CREATE TRIGGER orders_upd BEFORE UPDATE OF status");
    assert!(retype_orders < restore_trigger, "{script}");
    assert!(!script.contains("drop column"), "{script}");
    // Only the blocking kinds: nothing else about the columns is re-created.
    assert_eq!(
        script.matches("create policy docs_pol").count(),
        1,
        "{script}"
    );
    assert_eq!(
        script.matches("CREATE TRIGGER orders_upd").count(),
        1,
        "{script}"
    );
}

#[tokio::test]
async fn r3g_changed_policy_on_a_retyped_column_is_created_once_after_the_retype() {
    // The table's alter leaves a changed policy in the way of its retype to
    // the retype restore, which creates its TO form once, after the retype.
    let from = r3g_retype_dump("integer", "text", "(tenant > 0)");
    let to = r3g_retype_dump("bigint", "text", "(tenant > 1)");
    let mut comparer = Comparer::new(from, to, true, false, true, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();
    let drop_policy = r3g_at(&script, "drop policy if exists docs_pol on s.docs;");
    let retype = r3g_at(
        &script,
        "alter table s.docs alter column tenant type bigint;",
    );
    let create = r3g_at(
        &script,
        "create policy docs_pol on s.docs for all to public using (tenant > 1);",
    );
    assert!(drop_policy < retype && retype < create, "{script}");
    assert_eq!(
        script.matches("create policy docs_pol").count(),
        1,
        "{script}"
    );
}

#[tokio::test]
async fn r3g_retype_blockers_are_left_alone_without_use_drop() {
    let from = r3g_retype_dump("integer", "text", "(tenant > 0)");
    let to = r3g_retype_dump("bigint", "character varying", "(tenant > 0)");
    let mut comparer = Comparer::new(from, to, false, false, true, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();
    assert!(
        !script.contains("Dependents dropped ahead of a column retype"),
        "{script}"
    );
    assert!(
        !script.contains("drop policy if exists docs_pol"),
        "{script}"
    );
    assert!(!script.contains("create policy docs_pol"), "{script}");
}

/// `s.<name> (id, x)` under RLS with the policy `<policy>` reading
/// `s.docs.tenant` through a subquery, `extra` added to its condition.
fn r3g_reader_table(name: &str, policy: &str, extra: &str) -> Table {
    let mut table = r3g_table(
        name,
        vec![
            r3g_column(name, "id", 1, "integer"),
            r3g_column(name, "x", 2, "integer"),
        ],
        None,
        false,
    );
    table.policies.push(r3g_policy(
        name,
        policy,
        &format!("(EXISTS ( SELECT 1 FROM s.docs d WHERE ((d.tenant = {name}.x){extra})))"),
    ));
    table.has_rowsecurity = true;
    table.hash();
    table
}

/// The edge of the policy `<policy>` of `s.<table>` on `s.docs.tenant`.
fn r3g_cross_edge(table: &str, policy: &str) -> ColumnDependent {
    ColumnDependent {
        schema: "s".to_string(),
        table: "docs".to_string(),
        column: "tenant".to_string(),
        kind: ColumnDependentKind::Policy,
        dep_schema: "s".to_string(),
        dep_table: table.to_string(),
        dep_name: policy.to_string(),
    }
}

/// `s.docs.tenant` goes from integer to bigint. `s.a_other`, altered before
/// it, changes its policy reading it; `s.a_new` is new, with such a policy;
/// `s.z_other`, altered after it, gains one.
fn r3g_cross_policy_dumps() -> (Dump, Dump) {
    let mut from = r3g_retype_dump("integer", "text", "(tenant > 0)");
    let mut to = r3g_retype_dump("bigint", "text", "(tenant > 0)");
    from.tables.push(r3g_reader_table("a_other", "o_pol", ""));
    from.tables.push(r3g_table(
        "z_other",
        vec![
            r3g_column("z_other", "id", 1, "integer"),
            r3g_column("z_other", "x", 2, "integer"),
        ],
        None,
        false,
    ));
    from.column_dependents
        .push(r3g_cross_edge("a_other", "o_pol"));
    to.tables
        .push(r3g_reader_table("a_other", "o_pol", " AND (d.id > 0)"));
    to.tables.push(r3g_reader_table("a_new", "n_pol", ""));
    to.tables.push(r3g_reader_table("z_other", "z_pol", ""));
    to.column_dependents.extend([
        r3g_cross_edge("a_other", "o_pol"),
        r3g_cross_edge("a_new", "n_pol"),
        r3g_cross_edge("z_other", "z_pol"),
    ]);
    (from, to)
}

#[tokio::test]
async fn r3g_policies_of_other_tables_reading_a_retyped_column_wait_for_the_retype() {
    // PostgreSQL refuses the retype while any policy reads the column,
    // through a subquery on another table too. The changed one of a table
    // altered earlier and the one of a new table were created ahead of it.
    let (from, to) = r3g_cross_policy_dumps();
    let mut comparer = Comparer::new(from, to, true, false, true, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();

    let predrop = r3g_at(&script, "drop policy if exists o_pol on s.a_other;");
    let create_new = r3g_at(&script, "create table s.a_new");
    let retype = r3g_at(
        &script,
        "alter table s.docs alter column tenant type bigint;",
    );
    assert!(predrop < retype && create_new < retype, "{script}");
    for policy in [
        "create policy o_pol on s.a_other",
        "create policy n_pol on s.a_new",
    ] {
        assert!(retype < r3g_at(&script, policy), "{policy}:\n{script}");
        assert_eq!(script.matches(policy).count(), 1, "{policy}:\n{script}");
    }
    assert!(
        script.contains(
            "using (EXISTS ( SELECT 1 FROM s.docs d WHERE ((d.tenant = a_other.x) AND (d.id > 0))))"
        ),
        "{script}"
    );
    // A table altered after the retype creates its own, where it did.
    let z_table = r3g_at(&script, "/* Table: s.z_other*/");
    let z_policy = r3g_at(&script, "create policy z_pol on s.z_other");
    assert!(retype < z_table && z_table < z_policy, "{script}");
    assert_eq!(script.matches("create policy z_pol").count(), 1, "{script}");
}

#[tokio::test]
async fn r3g_policies_of_other_tables_stay_put_without_use_drop() {
    let (from, to) = r3g_cross_policy_dumps();
    let mut comparer = Comparer::new(from, to, false, false, true, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();
    // The new table is created with its policy, ahead of the alters.
    let create_new = r3g_at(&script, "create table s.a_new");
    let policy = r3g_at(&script, "create policy n_pol on s.a_new");
    let retype = r3g_at(
        &script,
        "alter table s.docs alter column tenant type bigint;",
    );
    assert!(create_new < policy && policy < retype, "{script}");
}

#[test]
fn r3g_policies_reading_retyped_columns_come_from_the_to_graph() {
    let (from, to) = r3g_cross_policy_dumps();
    let comparer = Comparer::new(from, to, true, false, true, GrantsMode::Ignore);
    let retyped: HashSet<(String, String, String)> =
        [("s".to_string(), "docs".to_string(), "tenant".to_string())]
            .into_iter()
            .collect();
    let waiting = comparer.policies_reading_retyped_columns(&retyped);
    let mut found: Vec<(String, String, String)> = waiting
        .iter()
        .flat_map(|(owner, entries)| {
            entries
                .iter()
                .map(move |(policy, table)| (owner.clone(), policy.clone(), table.clone()))
        })
        .collect();
    found.sort();
    let docs = "\"s\".\"docs\"".to_string();
    assert_eq!(
        found,
        vec![
            (
                "\"s\".\"a_new\"".to_string(),
                "n_pol".to_string(),
                docs.clone()
            ),
            (
                "\"s\".\"a_other\"".to_string(),
                "o_pol".to_string(),
                docs.clone()
            ),
            ("\"s\".\"z_other\"".to_string(), "z_pol".to_string(), docs),
        ]
    );
    // A policy of the retyped table itself is left to its own alter.
    assert!(!waiting.contains_key("\"s\".\"docs\""));
    assert!(
        comparer
            .policies_reading_retyped_columns(&HashSet::new())
            .is_empty()
    );
}

// ==== r4-typed tests: one policy blocking a drop + add and a retype ====

/// `s.a_t (id, n <n_type>, g <n_type> GENERATED ALWAYS AS (n * 2) STORED)`,
/// `s.z (id, n <n_type>)` and `s.o (id, x)`, whose policy `o_pol` reads
/// `a_t.g` and `z.n` (`policy`), with the FROM graph's edges. From integer
/// to bigint, `a_t.g` is dropped and re-added around `a_t.n`'s retype, and
/// `z.n` retyped in place by `s.z`'s alter, which comes after `s.a_t`'s.
/// With `own`, `s.a_t` carries a policy `a_pol` reading both of its columns.
fn r4t_drop_and_retype_dump(n_type: &str, own: bool) -> Dump {
    let mut dump = Dump::new(DumpConfig::default());
    dump.server_version_num = Some(170000);
    let mut g = r3g_column("a_t", "g", 3, n_type);
    g.is_generated = "ALWAYS".to_string();
    g.generation_expression = Some("(n * 2)".to_string());
    g.generation_type = Some("s".to_string());
    let mut a_t = r3g_table(
        "a_t",
        vec![
            r3g_column("a_t", "id", 1, "integer"),
            r3g_column("a_t", "n", 2, n_type),
            g,
        ],
        None,
        false,
    );
    if own {
        a_t.policies
            .push(r3g_policy("a_t", "a_pol", "((g > 0) AND (n > 0))"));
        a_t.has_rowsecurity = true;
        a_t.hash();
    }
    let z = r3g_table(
        "z",
        vec![
            r3g_column("z", "id", 1, "integer"),
            r3g_column("z", "n", 2, n_type),
        ],
        None,
        false,
    );
    let mut o = r3g_table(
        "o",
        vec![
            r3g_column("o", "id", 1, "integer"),
            r3g_column("o", "x", 2, "bigint"),
        ],
        None,
        false,
    );
    o.policies.push(r3g_policy(
        "o",
        "o_pol",
        "((x IN ( SELECT a_t.g FROM s.a_t)) AND (x IN ( SELECT z.n FROM s.z)))",
    ));
    o.has_rowsecurity = true;
    o.hash();
    dump.tables.extend([a_t, z, o]);
    let edge = |table: &str, column: &str, dep_table: &str, dep: &str| ColumnDependent {
        schema: "s".to_string(),
        table: table.to_string(),
        column: column.to_string(),
        kind: ColumnDependentKind::Policy,
        dep_schema: "s".to_string(),
        dep_table: dep_table.to_string(),
        dep_name: dep.to_string(),
    };
    dump.column_dependents = vec![edge("a_t", "g", "o", "o_pol"), edge("z", "n", "o", "o_pol")];
    if own {
        dump.column_dependents.extend([
            edge("a_t", "g", "a_t", "a_pol"),
            edge("a_t", "n", "a_t", "a_pol"),
        ]);
    }
    dump
}

#[tokio::test]
async fn r4t_policy_blocking_a_drop_and_a_later_retype_is_created_after_every_alter() {
    // The drop + add restore put o_pol back right after s.a_t's alter,
    // before s.z's ALTER COLUMN ... TYPE, which PostgreSQL then refused.
    let mut comparer = Comparer::new(
        r4t_drop_and_retype_dump("integer", false),
        r4t_drop_and_retype_dump("bigint", false),
        true,
        false,
        true,
        GrantsMode::Ignore,
    );
    comparer.compare().await.unwrap();
    let script = comparer.get_script();

    let predrop = r3g_at(&script, "drop policy if exists o_pol on s.o;");
    let drop_g = r3g_at(&script, "alter table s.a_t drop column g;");
    let retype_z = r3g_at(&script, "alter table s.z alter column n type bigint;");
    let create = r3g_at(&script, "create policy o_pol on s.o");
    assert!(predrop < drop_g && retype_z < create, "{script}");
    assert_eq!(script.matches("create policy o_pol").count(), 1, "{script}");
    // Dropped once ahead, and once more by its restore block.
    assert_eq!(
        script
            .matches("drop policy if exists o_pol on s.o;")
            .count(),
        2,
        "{script}"
    );
    let section = r3g_at(
        &script,
        "Recreate policies dropped ahead of a column retype: Start",
    );
    assert!(section < create, "{script}");
}

#[tokio::test]
async fn r4t_policy_blocking_a_drop_and_a_retype_of_its_own_table_is_restored_with_it() {
    // Both columns are s.a_t's: its alter retypes n between the drop and the
    // add of g, so the restore right after it is late enough, as before.
    let mut comparer = Comparer::new(
        r4t_drop_and_retype_dump("integer", true),
        r4t_drop_and_retype_dump("bigint", true),
        true,
        false,
        true,
        GrantsMode::Ignore,
    );
    comparer.compare().await.unwrap();
    let script = comparer.get_script();

    let add_g = r3g_at(&script, "alter table s.a_t add column g bigint");
    let create_own = r3g_at(&script, "create policy a_pol on s.a_t");
    let retype_z = r3g_at(&script, "alter table s.z alter column n type bigint;");
    assert!(add_g < create_own && create_own < retype_z, "{script}");
    assert_eq!(script.matches("create policy a_pol").count(), 1, "{script}");
    // o_pol reads s.z too, and still waits for every alter.
    assert!(
        retype_z < r3g_at(&script, "create policy o_pol on s.o"),
        "{script}"
    );
}
