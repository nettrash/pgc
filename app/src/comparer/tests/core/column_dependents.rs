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
