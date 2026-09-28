//! Issue #179 — `DROP FUNCTION ... CASCADE` silently drops dependent
//! objects (functional indexes, CHECK constraints, generated columns,
//! column DEFAULT expressions, RLS policies). Phase 7 of
//! `compare_routines_and_views` re-emits them; these tests cover it.
//!
//! The `reg_cascade_*` tests at the end cover what the `CASCADE` takes
//! beyond the table: domain CHECKs and defaults, triggers, extended
//! statistics and routines whose argument defaults call the routine — and
//! the routines `drop_views` keeps back because their `CASCADE` would take
//! something pgc cannot re-create.
//!
//! The `pre_cascade_*` tests cover what reads a view the routine phase's
//! `CASCADE` takes: other views and materialized views, routines with a
//! SQL-standard body and rules, which `drop_views` drops ahead of everything
//! together with the view and `compare_routines_and_views` re-creates in
//! dependency order.

use super::helpers::*;
use crate::comparer::core::*;
use crate::config::dump_config::DumpConfig;
use crate::config::grants_mode::GrantsMode;
use crate::dump::cast::Cast;
use crate::dump::column_dependent::{ColumnDependent, ColumnDependentKind};
use crate::dump::foreign_table::{ForeignTable, ForeignTableColumn};
use crate::dump::operator::Operator;
use crate::dump::pg_type::{DomainConstraint, PgType};
use crate::dump::routine::{Routine, SqlBodyDependencies};
use crate::dump::rule::{Rule, RuleDependencies};
use crate::dump::statistic::Statistic;
use crate::dump::table::Table;
use crate::dump::table_column::TableColumn;
use crate::dump::table_constraint::TableConstraint;
use crate::dump::table_index::TableIndex;
use crate::dump::table_policy::TablePolicy;
use crate::dump::table_trigger::TableTrigger;
use crate::dump::view::View;
use sqlx::postgres::types::Oid;
use std::collections::HashSet;

#[tokio::test]
async fn issue179_signature_change_recreates_all_cascade_dependents() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    from_dump
        .routines
        .push(issue179_compute_routine("integer", "SELECT x * 2;"));
    to_dump.routines.push(issue179_compute_routine(
        "bigint",
        "SELECT (x * 2)::bigint;",
    ));

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

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_routines_and_views().await.unwrap();
    let script = comparer.get_script();

    let drop_pos = script
        .find("drop function if exists test_deps.compute (x integer) cascade;")
        .expect("CASCADE drop must be emitted for the signature change");
    let create_pos = script
        .find("create or replace function test_deps.compute(x integer) returns bigint")
        .expect("function recreate must be emitted");
    assert!(drop_pos < create_pos);

    // The recreate phase must run AFTER the function is recreated so the
    // dependent objects can be created against the new function.
    let chk_pos = script
        .find("alter table test_deps.items add constraint chk_compute")
        .expect("CHECK constraint must be re-added after CASCADE");
    assert!(
        create_pos < chk_pos,
        "CHECK must be re-added after function recreate"
    );
    assert!(
        script.contains("alter table test_deps.items drop constraint if exists chk_compute;"),
        "drop-if-exists guard for CHECK constraint missing: {}",
        script
    );

    let idx_pos = script
        .find("CREATE INDEX IF NOT EXISTS idx_compute ON test_deps.items")
        .expect("functional index must be re-created (CREATE INDEX IF NOT EXISTS) after CASCADE");
    assert!(create_pos < idx_pos);
    // Index recreate is now non-destructive: no separate DROP INDEX is
    // emitted (a false-positive match must not silently invalidate a
    // surviving index — see Phase 7 / issue #179 review thread).
    assert!(
        !script.contains("drop index if exists test_deps.idx_compute;"),
        "DROP INDEX must not be emitted; recreate uses CREATE INDEX IF NOT EXISTS"
    );

    // Generated column recreate is non-destructive: ADD COLUMN IF NOT
    // EXISTS, no DROP COLUMN. A drop here would cascade to attached
    // indexes / FKs / constraints that Phase 7 cannot restore.
    assert!(
        !script.contains("alter table test_deps.items drop column if exists gen_col"),
        "DROP COLUMN must not be emitted for generated column recreate"
    );
    assert!(
        script.contains("alter table test_deps.items add column if not exists gen_col bigint generated always as (test_deps.compute(value)) stored;"),
        "generated column must be re-added (IF NOT EXISTS) with TO type/expression"
    );

    // Column DEFAULT: column survives, only the default is gone.
    assert!(
        script.contains(
            "alter table test_deps.items alter column def_col set default test_deps.compute(0);"
        ),
        "column default must be restored from TO"
    );
    // We must NOT drop+re-add a non-generated column whose default was cascaded:
    assert!(
        !script.contains("drop column if exists def_col"),
        "non-generated column must survive — only its DEFAULT clause was cascaded"
    );

    let policy_pos = script
        .find("create policy p_items on test_deps.items")
        .expect("policy must be re-created after CASCADE");
    assert!(create_pos < policy_pos);
    assert!(
        script.contains("drop policy if exists p_items on test_deps.items;"),
        "drop-if-exists guard for policy missing"
    );
}

#[tokio::test]
async fn issue179_recreate_skipped_when_routine_unchanged() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let routine = issue179_compute_routine("integer", "SELECT x * 2;");
    from_dump.routines.push(routine.clone());
    to_dump.routines.push(routine);

    from_dump.tables.push(issue179_items_table(
        "integer",
        "test_deps.compute(0)",
        "integer",
    ));
    to_dump.tables.push(issue179_items_table(
        "integer",
        "test_deps.compute(0)",
        "integer",
    ));

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_routines_and_views().await.unwrap();
    let script = comparer.get_script();

    assert!(
        !script.contains("Recreate dependents dropped by CASCADE"),
        "no CASCADE drop happened — recreate phase must stay silent: {}",
        script
    );
    assert!(!script.contains("alter table test_deps.items add constraint chk_compute"));
    assert!(!script.contains("CREATE INDEX idx_compute"));
    assert!(!script.contains("create policy p_items"));
}

#[tokio::test]
async fn issue179_recreate_skipped_when_to_dependent_missing() {
    // Function is dropped entirely. The dependent objects are also gone
    // in TO (user removed both function and dependents). We must NOT
    // resurrect the dependents — they're intentionally absent.
    let mut from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());

    from_dump
        .routines
        .push(issue179_compute_routine("integer", "SELECT x * 2;"));
    from_dump.tables.push(issue179_items_table(
        "integer",
        "test_deps.compute(0)",
        "integer",
    ));

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_routines_and_views().await.unwrap();
    let script = comparer.get_script();

    assert!(
        !script.contains("alter table test_deps.items add constraint chk_compute"),
        "CHECK must not be resurrected when neither it nor the function exist in TO"
    );
    assert!(
        !script.contains("CREATE INDEX idx_compute"),
        "index must not be resurrected when neither it nor the function exist in TO"
    );
    assert!(
        !script.contains("create policy p_items"),
        "policy must not be resurrected when neither it nor the function exist in TO"
    );
}

/// Variant of [`issue179_items_table`] that mirrors PostgreSQL's
/// deparser output when the function is reachable via `search_path`:
/// the dependent texts use *unqualified* `compute(value)` instead of
/// `test_deps.compute(value)`. The `pg_get_*` family routinely drops
/// the schema qualifier in this case.
fn issue179_items_table_unqualified(value_type: &str, def_default: &str, gen_type: &str) -> Table {
    let mut def_col = int_column("test_deps", "items", "def_col", 2);
    def_col.data_type = value_type.to_string();
    def_col.column_default = Some(def_default.to_string());

    let mut gen_col = int_column("test_deps", "items", "gen_col", 3);
    gen_col.data_type = gen_type.to_string();
    gen_col.is_generated = "ALWAYS".to_string();
    // Unqualified function call.
    gen_col.generation_expression = Some("compute(value)".to_string());
    gen_col.generation_type = Some("s".to_string());

    let mut value_col = int_column("test_deps", "items", "value", 1);
    value_col.is_nullable = false;

    let chk_constraint = TableConstraint {
        catalog: "postgres".to_string(),
        schema: "test_deps".to_string(),
        name: "chk_compute".to_string(),
        table_name: "items".to_string(),
        constraint_type: "CHECK".to_string(),
        is_deferrable: false,
        initially_deferred: false,
        // Unqualified function call.
        definition: Some("CHECK (compute(value) > 0)".to_string()),
        coninhcount: 0,
        is_enforced: true,
        no_inherit: false,
        nulls_not_distinct: false,
        comment: None,
    };

    let idx = TableIndex {
        schema: "test_deps".to_string(),
        table: "items".to_string(),
        name: "idx_compute".to_string(),
        catalog: Some("postgres".to_string()),
        // Unqualified function call.
        indexdef: "CREATE INDEX idx_compute ON test_deps.items USING btree (compute(value))"
            .to_string(),
        is_partition_index: false,
        comment: None,
    };

    let policy = TablePolicy {
        schema: "test_deps".to_string(),
        table: "items".to_string(),
        name: "p_items".to_string(),
        command: "all".to_string(),
        permissive: true,
        roles: vec![],
        // Unqualified function call.
        using_clause: Some("(compute(value) > 0)".to_string()),
        check_clause: None,
    };

    let mut table = Table::new(
        "test_deps".to_string(),
        "items".to_string(),
        "test_deps".to_string(),
        "items".to_string(),
        "postgres".to_string(),
        None,
        vec![value_col, def_col, gen_col],
        vec![chk_constraint],
        vec![idx],
        vec![],
        None,
    );
    table.policies = vec![policy];
    table.has_rowsecurity = true;
    table.hash();
    table
}

#[tokio::test]
async fn issue179_unqualified_function_calls_are_detected() {
    // PostgreSQL's pg_get_constraintdef / pg_get_indexdef / pg_get_expr
    // drop the schema qualifier when the function is in search_path
    // (the typical `public` case). Phase 7 must still recognise these
    // dependents — otherwise the CASCADE-drop drift goes unfixed for
    // anything the deparser deemed "in scope".
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    from_dump
        .routines
        .push(issue179_compute_routine("integer", "SELECT x * 2;"));
    to_dump.routines.push(issue179_compute_routine(
        "bigint",
        "SELECT (x * 2)::bigint;",
    ));

    from_dump.tables.push(issue179_items_table_unqualified(
        "integer",
        "compute(0)::integer",
        "integer",
    ));
    to_dump.tables.push(issue179_items_table_unqualified(
        "bigint",
        "compute(0)",
        "bigint",
    ));

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_routines_and_views().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("alter table test_deps.items add constraint chk_compute"),
        "unqualified CHECK reference must trigger recreate: {}",
        script
    );
    assert!(
        script.contains("CREATE INDEX IF NOT EXISTS idx_compute ON test_deps.items"),
        "unqualified index reference must trigger recreate: {}",
        script
    );
    assert!(
        script.contains(
            "alter table test_deps.items add column if not exists gen_col bigint generated always as (compute(value)) stored;"
        ),
        "unqualified generated-column reference must trigger recreate: {}",
        script
    );
    assert!(
        script.contains("alter table test_deps.items alter column def_col set default compute(0);"),
        "unqualified column DEFAULT reference must trigger recreate: {}",
        script
    );
    assert!(
        script.contains("create policy p_items on test_deps.items"),
        "unqualified policy reference must trigger recreate: {}",
        script
    );
}

#[test]
fn issue179_unqualified_match_rejects_substrings_and_non_calls() {
    // Direct unit test for the boundary rules: the unqualified matcher
    // must require `name(` at an identifier boundary, not match
    // partial-name suffixes, qualified `schema.name`, or non-call uses.
    let mut affected: HashSet<(String, String)> = HashSet::new();
    affected.insert(("ignored_schema".to_string(), "compute".to_string()));

    // Bare function call — should match.
    assert!(Comparer::definition_references_any(
        "check (compute(value) > 0)",
        &affected
    ));
    // Whitespace between name and `(` — still a call.
    assert!(Comparer::definition_references_any(
        "check (compute  (value) > 0)",
        &affected
    ));
    // Qualified — qualified matcher handles it via the schema, but we
    // also exercise that the unqualified matcher's left-dot exclusion
    // does not double-match `other_schema.compute(`.
    assert!(!Comparer::definition_references_any(
        "check (other_schema.compute(value) > 0)",
        &affected
    ));
    // Substring — must NOT match.
    assert!(!Comparer::definition_references_any(
        "check (compute_v2(value) > 0)",
        &affected
    ));
    assert!(!Comparer::definition_references_any(
        "check (precompute(value) > 0)",
        &affected
    ));
    // Identifier without trailing `(` — must NOT match.
    assert!(!Comparer::definition_references_any(
        "check (compute > 0)",
        &affected
    ));
}

#[test]
fn issue179_unqualified_match_handles_non_ascii_identifiers() {
    // PostgreSQL allows Unicode identifiers (quoted), and the dump's
    // `quote_ident` machinery preserves them. After `prelower_pair`
    // strips the surrounding quotes the haystack and needle both
    // contain raw multi-byte UTF-8 — the previous implementation did
    // `text[start..]` with `start = i + 1` and panicked on the next
    // iteration because byte index `i + 1` lands inside a codepoint.
    // Drive the matcher with a Cyrillic name and several haystacks to
    // ensure: (a) it returns true for a real call, (b) it returns
    // false for a non-call use without panicking, and (c) it returns
    // false for a substring without panicking.
    let mut affected: HashSet<(String, String)> = HashSet::new();
    affected.insert(("test_schema".to_string(), "функция".to_string()));

    assert!(Comparer::definition_references_any(
        "check (функция(value) > 0)",
        &affected
    ));
    assert!(!Comparer::definition_references_any(
        "check (функция > 0)",
        &affected
    ));
    // Repeated occurrences without a `(` — would have triggered the
    // panic on the post-match `start = i + 1` advance.
    assert!(!Comparer::definition_references_any(
        "check (функция функция функция > 0)",
        &affected
    ));
    // Substring (Cyrillic suffix) must not falsely match.
    assert!(!Comparer::definition_references_any(
        "check (функция_v2(value) > 0)",
        &affected
    ));
}

#[test]
fn issue179_qualified_match_requires_call_context() {
    // `pg_get_indexdef` emits `CREATE INDEX … ON schema.table …`,
    // `pg_get_expr` emits `nextval('schema.seq'::regclass)`, etc.
    // Without a call gate the qualified matcher would pick up those
    // `schema.name` references whenever a routine happens to share its
    // name with a table / view / sequence in the same schema, and
    // Phase 7 would emit spurious recreates for unrelated objects.
    let mut affected: HashSet<(String, String)> = HashSet::new();
    affected.insert(("test_schema".to_string(), "users".to_string()));

    // Match: real qualified function call.
    assert!(Comparer::definition_references_any(
        "check (test_schema.users(value) > 0)",
        &affected,
    ));
    // Match with whitespace before `(`.
    assert!(Comparer::definition_references_any(
        "check (test_schema.users  (value) > 0)",
        &affected,
    ));

    // No match: qualified reference is the table in a CREATE INDEX
    // ON clause — not a function call.
    assert!(!Comparer::definition_references_any(
        "create index idx ON test_schema.users using btree (value)",
        &affected,
    ));
    // No match: qualified reference inside a `nextval` regclass cast.
    assert!(!Comparer::definition_references_any(
        "nextval('test_schema.users'::regclass)",
        &affected,
    ));
    // No match: identifier without a `(` after.
    assert!(!Comparer::definition_references_any(
        "check (test_schema.users > 0)",
        &affected,
    ));
    // No match: qualified suffix of a longer name (boundary check).
    assert!(!Comparer::definition_references_any(
        "check (test_schema.users_v2(value) > 0)",
        &affected,
    ));
}

#[tokio::test]
async fn issue179_to_side_gate_skips_dependents_no_longer_referencing_routine() {
    // `compare_tables()` runs before `compare_routines_and_views()`, so
    // dependents that have been rewritten to no longer reference the
    // affected routine reach the CASCADE-drop step with the dependency
    // already broken — PostgreSQL leaves them alone. Phase 7 must NOT
    // re-emit recreates for those: doing so is at best wasteful and at
    // worst destructive (a `DROP COLUMN IF EXISTS` for a generated
    // column also drops every index / FK / constraint attached to the
    // column, none of which Phase 7 restores).
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    from_dump
        .routines
        .push(issue179_compute_routine("integer", "SELECT x * 2;"));
    to_dump.routines.push(issue179_compute_routine(
        "bigint",
        "SELECT (x * 2)::bigint;",
    ));

    // FROM table: every dependent references `test_deps.compute`.
    from_dump.tables.push(issue179_items_table(
        "integer",
        "test_deps.compute(0)",
        "integer",
    ));

    // TO table: dependents have been rewritten to NOT reference
    // `test_deps.compute` anymore. After `compare_tables` runs they
    // exist in this rewritten form, so the `DROP FUNCTION ... CASCADE`
    // does not touch them.
    let mut value_col = int_column("test_deps", "items", "value", 1);
    value_col.is_nullable = false;

    let mut def_col = int_column("test_deps", "items", "def_col", 2);
    def_col.data_type = "integer".to_string();
    def_col.column_default = Some("0".to_string()); // no longer references compute

    let mut gen_col = int_column("test_deps", "items", "gen_col", 3);
    gen_col.data_type = "integer".to_string();
    gen_col.is_generated = "ALWAYS".to_string();
    gen_col.generation_expression = Some("(value * 3)".to_string()); // no compute()
    gen_col.generation_type = Some("s".to_string());

    let chk = TableConstraint {
        catalog: "postgres".to_string(),
        schema: "test_deps".to_string(),
        name: "chk_compute".to_string(),
        table_name: "items".to_string(),
        constraint_type: "CHECK".to_string(),
        is_deferrable: false,
        initially_deferred: false,
        // No compute() here either — TO swapped it out.
        definition: Some("CHECK (value > 0)".to_string()),
        coninhcount: 0,
        is_enforced: true,
        no_inherit: false,
        nulls_not_distinct: false,
        comment: None,
    };

    let idx = TableIndex {
        schema: "test_deps".to_string(),
        table: "items".to_string(),
        name: "idx_compute".to_string(),
        catalog: Some("postgres".to_string()),
        // No compute() in the index expression either.
        indexdef: "CREATE INDEX idx_compute ON test_deps.items USING btree (value)".to_string(),
        is_partition_index: false,
        comment: None,
    };

    let policy = TablePolicy {
        schema: "test_deps".to_string(),
        table: "items".to_string(),
        name: "p_items".to_string(),
        command: "all".to_string(),
        permissive: true,
        roles: vec![],
        // No compute() in the policy clause either.
        using_clause: Some("(value > 0)".to_string()),
        check_clause: None,
    };

    let mut to_table = Table::new(
        "test_deps".to_string(),
        "items".to_string(),
        "test_deps".to_string(),
        "items".to_string(),
        "postgres".to_string(),
        None,
        vec![value_col, def_col, gen_col],
        vec![chk],
        vec![idx],
        vec![],
        None,
    );
    to_table.policies = vec![policy];
    to_table.has_rowsecurity = true;
    to_table.hash();
    to_dump.tables.push(to_table);

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_routines_and_views().await.unwrap();
    let script = comparer.get_script();

    // Function still gets the CASCADE drop (signature change still
    // requires DROP+CREATE).
    assert!(
        script.contains("drop function if exists test_deps.compute (x integer) cascade;"),
        "function drop must still be emitted: {}",
        script
    );

    // CRITICAL: the destructive generated-column path must stay silent.
    assert!(
        !script.contains("alter table test_deps.items drop column if exists gen_col"),
        "generated column must NOT be dropped+re-added when TO no longer references the routine — that would cascade-destroy attached indexes/FKs without restoring them: {}",
        script
    );
    assert!(
        !script.contains("alter table test_deps.items add column gen_col"),
        "generated column add must not be emitted when TO does not reference the routine: {}",
        script
    );

    // Constraint, index, and policy recreates must also be skipped to
    // avoid redundant work that would conflict with `compare_tables`.
    assert!(
        !script.contains("alter table test_deps.items add constraint chk_compute"),
        "CHECK recreate must not fire when TO definition no longer references the routine: {}",
        script
    );
    assert!(
        !script.contains("CREATE INDEX idx_compute ON test_deps.items"),
        "index recreate must not fire when TO indexdef no longer references the routine: {}",
        script
    );
    assert!(
        !script.contains("create policy p_items"),
        "policy recreate must not fire when TO clauses no longer reference the routine: {}",
        script
    );

    // Column DEFAULT must also be skipped (TO default is `0`, no
    // function reference).
    assert!(
        !script.contains("alter table test_deps.items alter column def_col set default 0;"),
        "column DEFAULT recreate must not fire when TO default no longer references the routine: {}",
        script
    );
}

#[tokio::test]
async fn issue179_to_side_gate_still_recreates_when_to_keeps_reference() {
    // Sanity check on the gate: when TO *does* still reference the
    // affected routine (e.g. the dependent definition is unchanged),
    // Phase 7 must continue to emit recreates exactly as before.
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    from_dump
        .routines
        .push(issue179_compute_routine("integer", "SELECT x * 2;"));
    to_dump.routines.push(issue179_compute_routine(
        "bigint",
        "SELECT (x * 2)::bigint;",
    ));

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

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_routines_and_views().await.unwrap();
    let script = comparer.get_script();

    // Each kind of dependent must be re-emitted because TO still
    // references the routine.
    assert!(
        script.contains("alter table test_deps.items add constraint chk_compute"),
        "CHECK constraint recreate expected when TO still references the routine: {}",
        script
    );
    assert!(
        script.contains("CREATE INDEX IF NOT EXISTS idx_compute ON test_deps.items"),
        "index recreate expected when TO still references the routine: {}",
        script
    );
    assert!(
        script.contains("alter table test_deps.items add column if not exists gen_col"),
        "generated column recreate expected when TO still references the routine: {}",
        script
    );
    assert!(
        script.contains(
            "alter table test_deps.items alter column def_col set default test_deps.compute(0);"
        ),
        "column DEFAULT recreate expected when TO still references the routine: {}",
        script
    );
    assert!(
        script.contains("create policy p_items on test_deps.items"),
        "policy recreate expected when TO still references the routine: {}",
        script
    );
}

#[tokio::test]
async fn issue179_overload_collision_does_not_destroy_unrelated_column() {
    // Phase 7's `affected` set keys on `(schema, name)` and ignores the
    // argument signature, because text-based reference matching cannot
    // distinguish overloads (`compute(value)` in a CHECK or generation
    // expression carries no type info). When `compute(integer)` is
    // dropped+recreated and `compute(text)` is unchanged, a dependent
    // referencing `compute(text_value)` will text-match the affected
    // set even though CASCADE never touched it. The recreate paths
    // must therefore be non-destructive — a `DROP COLUMN IF EXISTS`
    // for a generated column would cascade through every index / FK /
    // constraint attached to the column, none of which Phase 7 knows
    // how to restore. This test pins the non-destructive contract.
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    // FROM: two overloads. `compute(integer)` will change return type
    // (forces DROP+CREATE); `compute(text)` is unchanged.
    let mut from_int = Routine::new(
        "test_deps".to_string(),
        Oid(900),
        "compute".to_string(),
        "sql".to_string(),
        "FUNCTION".to_string(),
        "integer".to_string(),
        "x integer".to_string(),
        None,
        None,
        "SELECT x * 2;".to_string(),
    );
    from_int.hash();
    let mut from_text = Routine::new(
        "test_deps".to_string(),
        Oid(901),
        "compute".to_string(),
        "sql".to_string(),
        "FUNCTION".to_string(),
        "integer".to_string(),
        "x text".to_string(),
        None,
        None,
        "SELECT length(x);".to_string(),
    );
    from_text.hash();
    from_dump.routines.push(from_int);
    from_dump.routines.push(from_text.clone());

    // TO: same overloads, but `compute(integer)` now returns BIGINT.
    let mut to_int = Routine::new(
        "test_deps".to_string(),
        Oid(900),
        "compute".to_string(),
        "sql".to_string(),
        "FUNCTION".to_string(),
        "bigint".to_string(),
        "x integer".to_string(),
        None,
        None,
        "SELECT (x * 2)::bigint;".to_string(),
    );
    to_int.hash();
    to_dump.routines.push(to_int);
    to_dump.routines.push(from_text); // text overload unchanged

    // Build a table whose generated column references `compute(text_value)`
    // — i.e. the unchanged `compute(text)` overload. CASCADE never
    // drops this column (the dropped function is `compute(integer)`),
    // so Phase 7 must NOT emit a destructive recreate.
    let make_table = || {
        let mut text_col = int_column("test_deps", "items", "text_value", 1);
        text_col.data_type = "text".to_string();
        text_col.is_nullable = false;
        let mut gen_col = int_column("test_deps", "items", "gen_col", 2);
        gen_col.is_generated = "ALWAYS".to_string();
        gen_col.generation_expression = Some("test_deps.compute(text_value)".to_string());
        gen_col.generation_type = Some("s".to_string());
        let mut table = Table::new(
            "test_deps".to_string(),
            "items".to_string(),
            "test_deps".to_string(),
            "items".to_string(),
            "postgres".to_string(),
            None,
            vec![text_col, gen_col],
            vec![],
            vec![],
            vec![],
            None,
        );
        table.hash();
        table
    };
    from_dump.tables.push(make_table());
    to_dump.tables.push(make_table());

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_routines_and_views().await.unwrap();
    let script = comparer.get_script();

    // The integer overload is still cascaded.
    assert!(
        script.contains("drop function if exists test_deps.compute (x integer) cascade;"),
        "integer overload must still be DROP+CREATEd: {}",
        script
    );

    // CRITICAL: Phase 7 must NOT emit a DROP COLUMN. The text matcher
    // false-positives on the affected set (which collapses overloads
    // by name), so without IF NOT EXISTS the unconditional drop would
    // destroy the unrelated column. Pinning this prevents regression.
    assert!(
        !script.contains("drop column if exists gen_col"),
        "overload collision must not emit DROP COLUMN — would cascade-destroy attached indexes/FKs: {}",
        script
    );
    // The recreate that *is* emitted must use IF NOT EXISTS so the
    // surviving column is left intact when the script runs.
    if let Some(add_idx) = script.find("alter table test_deps.items add column") {
        let snippet = &script[add_idx..(add_idx + 80).min(script.len())];
        assert!(
            snippet.contains("if not exists"),
            "ADD COLUMN must use IF NOT EXISTS to be non-destructive on overload false-positives: {}",
            snippet
        );
    }
}

#[tokio::test]
async fn issue179_quoted_routine_name_recreates_dependents() {
    // The dump query wraps `proname` with `quote_ident`, so a
    // mixed-case routine like `MyFunc` arrives as `"MyFunc"`. The
    // unqualified-call matcher operates on the quote-stripped haystack,
    // so the affected-routine name must also be quote-stripped or
    // dependents like `CHECK ("MyFunc"(value) > 0)` are missed.
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_routine = Routine::new(
        "\"MySchema\"".to_string(),
        Oid(950),
        "\"MyFunc\"".to_string(),
        "sql".to_string(),
        "FUNCTION".to_string(),
        "integer".to_string(),
        "x integer".to_string(),
        None,
        None,
        "SELECT x * 2;".to_string(),
    );
    from_routine.hash();
    from_dump.routines.push(from_routine);

    let mut to_routine = Routine::new(
        "\"MySchema\"".to_string(),
        Oid(950),
        "\"MyFunc\"".to_string(),
        "sql".to_string(),
        "FUNCTION".to_string(),
        "bigint".to_string(),
        "x integer".to_string(),
        None,
        None,
        "SELECT (x * 2)::bigint;".to_string(),
    );
    to_routine.hash();
    to_dump.routines.push(to_routine);

    let mut value_col = int_column("public", "items", "value", 1);
    value_col.is_nullable = false;

    let chk = TableConstraint {
        catalog: "postgres".to_string(),
        schema: "public".to_string(),
        name: "chk_my".to_string(),
        table_name: "items".to_string(),
        constraint_type: "CHECK".to_string(),
        is_deferrable: false,
        initially_deferred: false,
        definition: Some("CHECK (\"MyFunc\"(value) > 0)".to_string()),
        coninhcount: 0,
        is_enforced: true,
        no_inherit: false,
        nulls_not_distinct: false,
        comment: None,
    };

    let mut from_table = Table::new(
        "public".to_string(),
        "items".to_string(),
        "public".to_string(),
        "items".to_string(),
        "postgres".to_string(),
        None,
        vec![value_col.clone()],
        vec![chk.clone()],
        vec![],
        vec![],
        None,
    );
    from_table.hash();
    let mut to_table = Table::new(
        "public".to_string(),
        "items".to_string(),
        "public".to_string(),
        "items".to_string(),
        "postgres".to_string(),
        None,
        vec![value_col],
        vec![chk],
        vec![],
        vec![],
        None,
    );
    to_table.hash();
    from_dump.tables.push(from_table);
    to_dump.tables.push(to_table);

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_routines_and_views().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("alter table public.items add constraint chk_my"),
        "quoted-name function reference must trigger CHECK recreate: {}",
        script
    );
}

#[tokio::test]
async fn issue179_defaults_only_change_uses_create_or_replace_no_cascade() {
    // PR #187 review (C10/C15): defaults-only changes must NOT
    // trigger `DROP FUNCTION ... CASCADE`. PostgreSQL accepts
    // default-argument changes via `CREATE OR REPLACE FUNCTION`
    // when the identity argument types and return type are
    // unchanged. The earlier version of this test pinned the
    // destructive behaviour (DROP CASCADE + Phase 7 dependent
    // recreates); the correct expectation is the non-destructive
    // OR REPLACE form, with no CASCADE drop and no dependent
    // recreates (since the function was never actually dropped).
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_routine = Routine::new(
        "test_deps".to_string(),
        Oid(951),
        "compute".to_string(),
        "sql".to_string(),
        "FUNCTION".to_string(),
        "integer".to_string(),
        "x integer".to_string(),
        Some("DEFAULT 0".to_string()),
        None,
        "SELECT x * 2;".to_string(),
    );
    from_routine.hash();
    from_dump.routines.push(from_routine);

    let mut to_routine = Routine::new(
        "test_deps".to_string(),
        Oid(951),
        "compute".to_string(),
        "sql".to_string(),
        "FUNCTION".to_string(),
        "integer".to_string(),
        "x integer".to_string(),
        Some("DEFAULT 1".to_string()),
        None,
        "SELECT x * 2;".to_string(),
    );
    to_routine.hash();
    to_dump.routines.push(to_routine);

    from_dump.tables.push(issue179_items_table(
        "integer",
        "test_deps.compute(0)",
        "integer",
    ));
    to_dump.tables.push(issue179_items_table(
        "integer",
        "test_deps.compute(0)",
        "integer",
    ));

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_routines_and_views().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("create or replace function test_deps.compute(x integer DEFAULT 1)"),
        "defaults-only change must be re-emitted via CREATE OR REPLACE: {}",
        script
    );
    assert!(
        !script.contains("drop function if exists test_deps.compute"),
        "defaults-only change must NOT emit DROP FUNCTION CASCADE: {}",
        script
    );
    assert!(
        !script.contains("alter table test_deps.items add constraint chk_compute"),
        "dependents must NOT be recreated when the function was not actually dropped: {}",
        script
    );
}

/// Build a partition child of `parent_table` whose dependents (CHECK
/// constraint with `coninhcount=1`, `is_partition_index=true` index, a
/// generated column, and a CHECK with `coninhcount=0` so we can prove
/// the truly-local case is still emitted) all reference
/// `test_deps.compute`. Used by the partition-child guard tests.
fn issue179_items_partition_child(parent_qualified: &str) -> Table {
    let mut value_col = int_column("test_deps", "items_2026", "value", 1);
    value_col.is_nullable = false;

    let mut gen_col = int_column("test_deps", "items_2026", "gen_col", 2);
    gen_col.is_generated = "ALWAYS".to_string();
    gen_col.generation_expression = Some("test_deps.compute(value)".to_string());
    gen_col.generation_type = Some("s".to_string());

    let inherited_check = TableConstraint {
        catalog: "postgres".to_string(),
        schema: "test_deps".to_string(),
        name: "chk_compute".to_string(),
        table_name: "items_2026".to_string(),
        constraint_type: "CHECK".to_string(),
        is_deferrable: false,
        initially_deferred: false,
        definition: Some("CHECK (test_deps.compute(value) > 0)".to_string()),
        coninhcount: 1,
        is_enforced: true,
        no_inherit: false,
        nulls_not_distinct: false,
        comment: None,
    };
    let mut local_check = inherited_check.clone();
    local_check.name = "chk_compute_local".to_string();
    local_check.coninhcount = 0;

    let inherited_idx = TableIndex {
        schema: "test_deps".to_string(),
        table: "items_2026".to_string(),
        name: "idx_compute".to_string(),
        catalog: Some("postgres".to_string()),
        indexdef: "CREATE INDEX idx_compute ON test_deps.items_2026 USING btree (test_deps.compute(value))".to_string(),
        is_partition_index: true,
        comment: None,
    };

    let mut table = Table::new(
        "test_deps".to_string(),
        "items_2026".to_string(),
        "test_deps".to_string(),
        "items_2026".to_string(),
        "postgres".to_string(),
        None,
        vec![value_col, gen_col],
        vec![inherited_check, local_check],
        vec![inherited_idx],
        vec![],
        None,
    );
    table.partition_of = Some(parent_qualified.to_string());
    table.partition_bound = Some("FOR VALUES IN (1)".to_string());
    table.hash();
    table
}

#[tokio::test]
async fn issue179_partition_child_skips_inherited_dependents() {
    // FROM and TO each contain a partitioned parent + one partition
    // child. The function signature changes, so CASCADE drops the
    // parent-side dependents. Phase 7 must NOT emit recreates for the
    // child's inherited objects (PostgreSQL forbids `ALTER TABLE child`
    // on inherited columns/constraints/indexes), but it MUST still
    // emit the truly-local CHECK constraint (`coninhcount = 0`).
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    from_dump
        .routines
        .push(issue179_compute_routine("integer", "SELECT x * 2;"));
    to_dump.routines.push(issue179_compute_routine(
        "bigint",
        "SELECT (x * 2)::bigint;",
    ));

    // Parent (partitioned) carries the dependent definitions on its own
    // row — its recreate handles the propagation to children.
    let mut parent_from =
        issue179_items_table("integer", "test_deps.compute(0)::integer", "integer");
    parent_from.name = "items".to_string();
    parent_from.raw_name = "items".to_string();
    parent_from.partition_key = Some("LIST (value)".to_string());
    parent_from.hash();

    let mut parent_to = issue179_items_table("bigint", "test_deps.compute(0)", "bigint");
    parent_to.name = "items".to_string();
    parent_to.raw_name = "items".to_string();
    parent_to.partition_key = Some("LIST (value)".to_string());
    parent_to.hash();

    from_dump.tables.push(parent_from);
    to_dump.tables.push(parent_to);

    from_dump
        .tables
        .push(issue179_items_partition_child("test_deps.items"));
    to_dump
        .tables
        .push(issue179_items_partition_child("test_deps.items"));

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_routines_and_views().await.unwrap();
    let script = comparer.get_script();

    // Parent dependents (which Table::diff would diff normally) MUST
    // be recreated against the parent.
    assert!(
        script.contains("alter table test_deps.items add constraint chk_compute"),
        "parent CHECK must be re-added: {}",
        script
    );
    assert!(
        script.contains("CREATE INDEX IF NOT EXISTS idx_compute ON test_deps.items "),
        "parent index must be re-created: {}",
        script
    );

    // Inherited child constraint (coninhcount > 0) must NOT be re-added
    // on the child — PostgreSQL would reject `ALTER TABLE child ADD
    // CONSTRAINT` for an inherited constraint, and the parent's recreate
    // already propagates. (Use exact-suffix match so we don't accidentally
    // match `chk_compute_local` below.)
    assert!(
        !script.contains("alter table test_deps.items_2026 drop constraint if exists chk_compute;"),
        "inherited child CHECK must not be re-emitted: {}",
        script
    );
    assert!(
        !script.contains("alter table test_deps.items_2026 add constraint chk_compute check"),
        "inherited child CHECK must not be re-added: {}",
        script
    );

    // Truly-local child constraint (coninhcount == 0) IS re-emitted.
    assert!(
        script.contains(
            "alter table test_deps.items_2026 drop constraint if exists chk_compute_local;"
        ),
        "local child CHECK must be re-emitted: {}",
        script
    );
    assert!(
        script.contains("alter table test_deps.items_2026 add constraint chk_compute_local"),
        "local child CHECK must be re-added: {}",
        script
    );

    // Partition-inherited index: must not be re-emitted on the child.
    // Match by the load-bearing fragment so the assertion holds whether
    // the recreate uses `CREATE INDEX` or `CREATE INDEX IF NOT EXISTS`.
    assert!(
        !script.contains("idx_compute ON test_deps.items_2026"),
        "partition-inherited index must not be re-emitted on child: {}",
        script
    );

    // Partition child's generated column must NOT be added — PostgreSQL
    // forbids modifying inherited columns directly on a partition.
    // (Recreate paths never emit DROP COLUMN now; the add assertion
    // below is the load-bearing one for partition-child safety.)
    assert!(
        !script.contains("alter table test_deps.items_2026 add column if not exists gen_col"),
        "partition child column must not be re-added: {}",
        script
    );
}

#[tokio::test]
async fn issue179_full_drop_recreates_dependents_when_overload_survives() {
    // PR #187 review (C16): the previous version of this test built
    // an invalid PostgreSQL state — TO had dependents referencing
    // `test_deps.compute` but no function with that name at all, so
    // the recreate SQL would fail to apply. The valid scenario where
    // "function fully dropped, dependents kept" is meaningful is when
    // a *different overload* of the same name survives in TO and the
    // dependents resolve to it via PostgreSQL's name-based function
    // binding. Set that up explicitly here. FROM has both
    // `compute(integer)` (which gets dropped) and `compute(text)`
    // (the surviving overload). TO has only `compute(text)`.
    // Dependents in both sides reference `test_deps.compute` and
    // resolve via overload resolution.
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    from_dump
        .routines
        .push(issue179_compute_routine("integer", "SELECT x * 2;"));
    let mut compute_text_from = Routine::new(
        "test_deps".to_string(),
        Oid(961),
        "compute".to_string(),
        "sql".to_string(),
        "FUNCTION".to_string(),
        "integer".to_string(),
        "x text".to_string(),
        None,
        None,
        "SELECT length(x);".to_string(),
    );
    compute_text_from.hash();
    from_dump.routines.push(compute_text_from.clone());
    // TO keeps only the text overload — `compute(integer)` is gone.
    to_dump.routines.push(compute_text_from);

    from_dump.tables.push(issue179_items_table(
        "integer",
        "test_deps.compute(0)",
        "integer",
    ));
    to_dump.tables.push(issue179_items_table(
        "integer",
        "test_deps.compute(0)",
        "integer",
    ));

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_routines_and_views().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("drop function if exists test_deps.compute (x integer) cascade;"),
        "the integer overload must be dropped"
    );
    assert!(
        script.contains("alter table test_deps.items add constraint chk_compute"),
        "CHECK present in TO must be re-added after CASCADE (overload-resolves to surviving compute)"
    );
    assert!(
        script.contains("CREATE INDEX IF NOT EXISTS idx_compute"),
        "functional index present in TO must be re-created (IF NOT EXISTS) after CASCADE"
    );
    assert!(
        script.contains("create policy p_items on test_deps.items"),
        "policy present in TO must be re-created after CASCADE"
    );
}

#[tokio::test]
async fn issue179_quoted_routine_names_match_unqualified_calls() {
    // PGC's dump query wraps `nspname` / `proname` with `quote_ident`,
    // so a function named `MyFunc` lands here as `Routine.name = "MyFunc"`.
    // Phase 7 must strip those quotes when building its affected set —
    // otherwise the unqualified matcher (which scans quote-stripped
    // text) never lines up with the deparsed `myfunc(` in dependents.
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_routine = Routine::new(
        "\"TestDeps\"".to_string(),
        Oid(900),
        "\"MyFunc\"".to_string(),
        "sql".to_string(),
        "FUNCTION".to_string(),
        "integer".to_string(),
        "x integer".to_string(),
        None,
        None,
        "SELECT x * 2;".to_string(),
    );
    from_routine.hash();
    let mut to_routine = from_routine.clone();
    to_routine.return_type = "bigint".to_string();
    to_routine.source_code = "SELECT (x * 2)::bigint;".to_string();
    to_routine.hash();
    from_dump.routines.push(from_routine);
    to_dump.routines.push(to_routine);

    // Dependent uses the deparsed unqualified form `"MyFunc"(value)`
    // (PostgreSQL preserves the case-sensitive identifier with quotes
    // but drops the schema qualifier when the function is in
    // `search_path`).
    let chk = TableConstraint {
        catalog: "postgres".to_string(),
        schema: "public".to_string(),
        name: "chk_myfunc".to_string(),
        table_name: "items".to_string(),
        constraint_type: "CHECK".to_string(),
        is_deferrable: false,
        initially_deferred: false,
        definition: Some("CHECK (\"MyFunc\"(value) > 0)".to_string()),
        coninhcount: 0,
        is_enforced: true,
        no_inherit: false,
        nulls_not_distinct: false,
        comment: None,
    };

    let mut value_col = int_column("public", "items", "value", 1);
    value_col.is_nullable = false;

    let mut from_table = Table::new(
        "public".to_string(),
        "items".to_string(),
        "public".to_string(),
        "items".to_string(),
        "postgres".to_string(),
        None,
        vec![value_col.clone()],
        vec![chk.clone()],
        vec![],
        vec![],
        None,
    );
    from_table.hash();
    let mut to_table = Table::new(
        "public".to_string(),
        "items".to_string(),
        "public".to_string(),
        "items".to_string(),
        "postgres".to_string(),
        None,
        vec![value_col],
        vec![chk],
        vec![],
        vec![],
        None,
    );
    to_table.hash();
    from_dump.tables.push(from_table);
    to_dump.tables.push(to_table);

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_routines_and_views().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("alter table public.items add constraint chk_myfunc"),
        "quoted routine name must still match its unqualified deparsed dependent: {}",
        script
    );
}

#[tokio::test]
async fn issue179_defaults_only_change_emits_create_or_replace_no_cascade() {
    // PR #187 review (C10/C15): PostgreSQL accepts default-argument
    // changes via `CREATE OR REPLACE FUNCTION` for the same identity
    // signature/return type — there is no DROP+CREATE requirement.
    // The `arguments_defaults` field is included in `Routine::hash()`
    // so the diff is *detected*; the migration is then emitted as a
    // plain `CREATE OR REPLACE` form (no CASCADE drop, no Phase 7
    // dependent recreates).
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_routine = Routine::new(
        "test_deps".to_string(),
        Oid(900),
        "compute".to_string(),
        "sql".to_string(),
        "FUNCTION".to_string(),
        "integer".to_string(),
        "x integer".to_string(),
        Some("0".to_string()),
        None,
        "SELECT x * 2;".to_string(),
    );
    from_routine.hash();
    let mut to_routine = from_routine.clone();
    // ONLY the default value changes — every other field is identical.
    to_routine.arguments_defaults = Some("1".to_string());
    to_routine.hash();
    from_dump.routines.push(from_routine);
    to_dump.routines.push(to_routine);

    let table = issue179_items_table("integer", "test_deps.compute(0)", "integer");
    from_dump.tables.push(table.clone());
    to_dump.tables.push(table);

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_routines_and_views().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("create or replace function test_deps.compute(x integer DEFAULT 1)"),
        "defaults-only change must use CREATE OR REPLACE FUNCTION: {}",
        script
    );
    assert!(
        !script.contains("drop function if exists test_deps.compute"),
        "defaults-only change must NOT emit DROP FUNCTION CASCADE: {}",
        script
    );
    assert!(
        !script.contains("alter table test_deps.items add constraint chk_compute"),
        "Phase 7 must NOT fire on defaults-only change (no CASCADE happened): {}",
        script
    );
    assert!(
        !script.contains("CREATE INDEX IF NOT EXISTS idx_compute ON test_deps.items"),
        "no index recreate on defaults-only change: {}",
        script
    );
}

// ---- reg-cascade: what a routine's CASCADE takes beyond the table --------

/// `s1.<name>(<arguments>)` in plpgsql, optionally with argument defaults.
fn rc_plpgsql(name: &str, arguments: &str, return_type: &str, defaults: Option<&str>) -> Routine {
    let mut routine = Routine::new(
        "s1".to_string(),
        Oid(700),
        name.to_string(),
        "plpgsql".to_string(),
        "function".to_string(),
        return_type.to_string(),
        arguments.to_string(),
        defaults.map(str::to_string),
        None,
        " begin return null; end ".to_string(),
    );
    routine.owner = "postgres".to_string();
    routine.hash();
    routine
}

/// `s1.<name>(<arguments>)` with a SQL-standard body reading `s1.codes.code`.
fn rc_codes_reader(name: &str, arguments: &str, return_type: &str) -> Routine {
    let mut routine = Routine::new(
        "s1".to_string(),
        Oid(701),
        name.to_string(),
        "sql".to_string(),
        "function".to_string(),
        return_type.to_string(),
        arguments.to_string(),
        None,
        None,
        String::new(),
    );
    routine.owner = "postgres".to_string();
    routine.source_code =
        "BEGIN ATOMIC\n SELECT min(codes.code) AS min FROM s1.codes;\nEND".to_string();
    routine.is_sql_body = true;
    routine.sql_body_dependencies = SqlBodyDependencies {
        relations: vec!["s1.codes".to_string()],
        columns: vec!["s1.codes.code".to_string()],
        ..SqlBodyDependencies::default()
    };
    routine.hash();
    routine
}

fn rc_table(name: &str, columns: Vec<TableColumn>, triggers: Vec<TableTrigger>) -> Table {
    let mut table = Table::new(
        "s1".to_string(),
        name.to_string(),
        "s1".to_string(),
        name.to_string(),
        "postgres".to_string(),
        None,
        columns,
        vec![],
        vec![],
        triggers,
        None,
    );
    table.hash();
    table
}

/// `s1.codes (code integer)`, or `code bigint`: the retyped column the
/// SQL-standard readers are in the way of.
fn rc_codes(bigint: bool) -> Table {
    let mut code = int_column("s1", "codes", "code", 1);
    if bigint {
        code.data_type = "bigint".to_string();
        code.numeric_precision = Some(64);
    }
    rc_table("codes", vec![code], vec![])
}

fn rc_trigger(name: &str, table: &str, when: &str) -> TableTrigger {
    TableTrigger {
        oid: Oid(1),
        name: name.to_string(),
        definition: format!(
            "CREATE TRIGGER {name} BEFORE INSERT ON s1.{table} FOR EACH ROW WHEN ({when}) EXECUTE FUNCTION s1.reject()"
        ),
        enabled: "O".to_string(),
        comment: None,
    }
}

fn rc_domain(name: &str, default: Option<&str>, checks: &[(&str, &str)]) -> PgType {
    let mut domain = make_domain_type("s1", name, 900);
    domain.typdefault = default.map(str::to_string);
    domain.domain_constraints = checks
        .iter()
        .map(|(name, definition)| DomainConstraint {
            name: name.to_string(),
            definition: definition.to_string(),
        })
        .collect();
    domain.hash();
    domain
}

fn rc_statistic(name: &str, table: &str, on: &str) -> Statistic {
    let mut stat = Statistic::new(
        "s1".to_string(),
        name.to_string(),
        "postgres".to_string(),
        "s1".to_string(),
        table.to_string(),
        vec!["ndistinct".to_string(), "expressions".to_string()],
        on.split(',').map(|c| c.trim().to_string()).collect(),
        format!("CREATE STATISTICS s1.{name} (ndistinct) ON {on} FROM s1.{table}"),
    );
    stat.columns_def = Some(on.to_string());
    stat.hash();
    stat
}

fn rc_orders() -> Table {
    rc_table(
        "orders",
        vec![
            int_column("s1", "orders", "id", 1),
            int_column("s1", "orders", "code", 2),
        ],
        vec![rc_trigger(
            "orders_chk",
            "orders",
            "(NOT s1.is_valid(new.code))",
        )],
    )
}

/// Both sides of the reviewer's scenario, extended: `s1.is_valid` and
/// `s1.first_code` read `s1.codes.code`, which TO retypes, so `drop_views`
/// drops them with CASCADE ahead of the retype. `is_valid` is called by a
/// domain CHECK, a trigger's WHEN clause, an extended statistics expression
/// and an argument default of `s1.flag`; `first_code` is a domain's default.
fn rc_predrop_dumps() -> (Dump, Dump) {
    let mut dumps = (
        Dump::new(DumpConfig::default()),
        Dump::new(DumpConfig::default()),
    );
    for (dump, bigint) in [(&mut dumps.0, false), (&mut dumps.1, true)] {
        dump.tables.push(rc_codes(bigint));
        dump.tables.push(rc_orders());
        dump.routines
            .push(rc_codes_reader("is_valid", "x integer", "boolean"));
        dump.routines
            .push(rc_codes_reader("first_code", "", "integer"));
        dump.routines.push(rc_plpgsql(
            "flag",
            "ok boolean",
            "boolean",
            Some("s1.is_valid(1)"),
        ));
        dump.types.push(rc_domain(
            "code_d",
            None,
            &[("code_d_check", "CHECK (s1.is_valid(VALUE))")],
        ));
        dump.types
            .push(rc_domain("code_def", Some("s1.first_code()"), &[]));
        dump.statistics
            .push(rc_statistic("orders_st", "orders", "s1.is_valid(code), id"));
    }
    dumps
}

async fn rc_compare(from: Dump, to: Dump, use_drop: bool, grants: GrantsMode) -> String {
    let mut comparer = Comparer::new(from, to, use_drop, false, true, grants);
    comparer.compare().await.unwrap();
    comparer.get_script()
}

fn rc_position(script: &str, needle: &str) -> usize {
    script
        .find(needle)
        .unwrap_or_else(|| panic!("expected {needle:?} in:\n{script}"))
}

#[tokio::test]
async fn reg_cascade_predrop_restores_domain_checks_triggers_statistics_and_routines() {
    let (from, to) = rc_predrop_dumps();
    let script = rc_compare(from, to, true, GrantsMode::Ignore).await;

    let drop_is_valid = rc_position(
        &script,
        "drop function if exists s1.is_valid (x integer) cascade;",
    );
    let retype = rc_position(&script, "alter column code type bigint");
    let create_is_valid = rc_position(&script, "create or replace function s1.is_valid(x integer)");
    assert!(drop_is_valid < retype && retype < create_is_valid);

    // The trigger, re-created from TO once the routine is back.
    let trigger = rc_position(
        &script,
        "drop trigger if exists orders_chk on s1.orders;\n\nCREATE TRIGGER orders_chk BEFORE INSERT ON s1.orders FOR EACH ROW WHEN ((NOT s1.is_valid(new.code))) EXECUTE FUNCTION s1.reject();",
    );
    assert!(create_is_valid < trigger);

    // The statistics object: restored after the routine, and only there —
    // `compare_statistics` leaves it alone.
    let stat = rc_position(
        &script,
        "drop statistics if exists s1.orders_st;\n\ncreate statistics s1.orders_st (ndistinct) on s1.is_valid(code), id from s1.orders;",
    );
    assert!(create_is_valid < stat);
    assert_eq!(script.matches("create statistics s1.orders_st").count(), 1);

    // The domain CHECK, with the domain constraint statements that run once
    // the tables and routines are done.
    let check = rc_position(
        &script,
        "alter domain s1.code_d drop constraint if exists \"code_d_check\";\n\nalter domain s1.code_d add constraint \"code_d_check\" CHECK (s1.is_valid(VALUE));",
    );
    assert!(create_is_valid < check);
    assert!(script.contains("Recreate domain constraints dropped by CASCADE: Start"));

    // A routine whose argument default calls it is dropped with it, and
    // re-created after it.
    let drop_flag = rc_position(
        &script,
        "drop function if exists s1.flag (ok boolean) cascade;",
    );
    assert!(drop_flag < retype);
    let create_flag = rc_position(
        &script,
        "create or replace function s1.flag(ok boolean DEFAULT s1.is_valid(1))",
    );
    assert!(create_is_valid < create_flag);
}

#[tokio::test]
async fn reg_cascade_predrop_detaches_and_restores_a_domain_default() {
    let (from, to) = rc_predrop_dumps();
    let script = rc_compare(from, to, true, GrantsMode::Ignore).await;

    // Detached right before the routine it calls is dropped: the domain
    // depends on its default, and the CASCADE would drop it with every
    // column of it.
    let detach = rc_position(&script, "alter domain s1.code_def drop default;");
    let drop_first_code = rc_position(&script, "drop function if exists s1.first_code () cascade;");
    assert!(detach < drop_first_code);
    assert_eq!(
        script
            .matches("alter domain s1.code_def drop default;")
            .count(),
        1
    );
    let create_first_code = rc_position(&script, "create or replace function s1.first_code()");
    let restore = rc_position(
        &script,
        "alter domain s1.code_def set default s1.first_code();",
    );
    assert!(create_first_code < restore);
}

#[tokio::test]
async fn reg_cascade_return_type_change_restores_everything_after_its_drop() {
    // `s1.score` is plpgsql; its return type changes, so the routine phase
    // drops it with CASCADE and creates it again.
    let mut from = Dump::new(DumpConfig::default());
    let mut to = Dump::new(DumpConfig::default());
    for (dump, return_type) in [(&mut from, "integer"), (&mut to, "bigint")] {
        dump.routines
            .push(rc_plpgsql("score", "x integer", return_type, None));
        dump.routines.push(rc_plpgsql(
            "score_or",
            "x bigint",
            "bigint",
            Some("s1.score(1)"),
        ));
        dump.types.push(rc_domain(
            "pos_d",
            None,
            &[("pos_d_check", "CHECK ((s1.score(VALUE) > 0))")],
        ));
        dump.types
            .push(rc_domain("preset_d", Some("s1.score(7)"), &[]));
        dump.tables.push(rc_table(
            "items",
            vec![
                int_column("s1", "items", "id", 1),
                int_column("s1", "items", "qty", 2),
            ],
            vec![rc_trigger(
                "items_guard",
                "items",
                "(s1.score(new.qty) < 0)",
            )],
        ));
        dump.statistics
            .push(rc_statistic("items_st", "items", "s1.score(qty), id"));
    }
    let script = rc_compare(from, to, true, GrantsMode::Ignore).await;

    // Not dropped ahead of the tables: nothing there is in its way.
    let views_drop_end = rc_position(&script, "Views DROP: End section");
    let detach = rc_position(&script, "alter domain s1.preset_d drop default;");
    let drop_score = rc_position(
        &script,
        "drop function if exists s1.score (x integer) cascade;",
    );
    assert!(views_drop_end < detach && detach < drop_score);
    let create_score = rc_position(
        &script,
        "create or replace function s1.score(x integer) returns bigint",
    );
    for restored in [
        "alter domain s1.preset_d set default s1.score(7);",
        "CREATE TRIGGER items_guard",
        "create statistics s1.items_st",
        "alter domain s1.pos_d add constraint \"pos_d_check\" CHECK ((s1.score(VALUE) > 0));",
    ] {
        assert!(create_score < rc_position(&script, restored), "{restored}");
    }
    // `compare_statistics`, which runs before the routine phase, leaves the
    // object to the restore: creating it there would only be undone.
    assert_eq!(script.matches("create statistics s1.items_st").count(), 1);

    // The argument-default user is not dropped ahead of the tables — the
    // table phase may need it — but taken by the routine's CASCADE, and it
    // comes back right after the routine it calls.
    assert!(
        !script.contains("s1.score_or (x bigint) cascade"),
        "{script}"
    );
    let create_score_or = rc_position(
        &script,
        "create or replace function s1.score_or(x bigint DEFAULT s1.score(1))",
    );
    assert!(create_score < create_score_or);
}

#[tokio::test]
async fn reg_cascade_restores_the_grants_of_a_routine_dropped_through_its_default() {
    let (mut from, mut to) = rc_predrop_dumps();
    for dump in [&mut from, &mut to] {
        let flag = dump.routines.iter_mut().find(|r| r.name == "flag").unwrap();
        flag.acl = vec![
            "postgres=X/postgres".to_string(),
            "reader=X/postgres".to_string(),
        ];
        flag.hash();
    }
    let script = rc_compare(from, to, true, GrantsMode::Full).await;
    assert!(
        script.contains("GRANT EXECUTE ON FUNCTION s1.flag(ok boolean) TO reader;"),
        "{script}"
    );
    assert!(script.contains("REVOKE EXECUTE ON FUNCTION s1.flag(ok boolean) FROM PUBLIC;"));
}

#[tokio::test]
async fn reg_cascade_changed_objects_are_left_to_their_own_diff() {
    let (from, mut to) = rc_predrop_dumps();
    // TO changes the trigger's WHEN clause and the domain CHECK: the trigger
    // diff (drop if exists + create, at the end) and the domain alter (drop
    // early, add late) re-create them anyway.
    let orders = to.tables.iter_mut().find(|t| t.name == "orders").unwrap();
    orders.triggers = vec![rc_trigger(
        "orders_chk",
        "orders",
        "(NOT s1.is_valid(new.id))",
    )];
    orders.hash();
    let code_d = to.types.iter_mut().find(|t| t.typname == "code_d").unwrap();
    code_d.domain_constraints[0].definition =
        "CHECK (s1.is_valid(VALUE) OR VALUE IS NULL)".to_string();
    code_d.hash();
    let script = rc_compare(from, to, true, GrantsMode::Ignore).await;

    assert_eq!(
        script.matches("CREATE TRIGGER orders_chk").count(),
        1,
        "{script}"
    );
    assert_eq!(
        script.matches("add constraint \"code_d_check\"").count(),
        1,
        "{script}"
    );
    assert!(!script.contains("Recreate domain constraints dropped by CASCADE"));
}

#[tokio::test]
async fn reg_cascade_restores_a_not_valid_domain_check_before_validating_it() {
    let (mut from, to) = rc_predrop_dumps();
    let code_d = from
        .types
        .iter_mut()
        .find(|t| t.typname == "code_d")
        .unwrap();
    code_d.domain_constraints[0].definition = "CHECK (s1.is_valid(VALUE)) NOT VALID".to_string();
    code_d.hash();
    let script = rc_compare(from, to, true, GrantsMode::Ignore).await;

    let restore = rc_position(
        &script,
        "alter domain s1.code_d add constraint \"code_d_check\" CHECK (s1.is_valid(VALUE));",
    );
    let validate = rc_position(
        &script,
        "alter domain s1.code_d validate constraint \"code_d_check\";",
    );
    assert!(restore < validate);
}

#[tokio::test]
async fn reg_cascade_restores_the_checks_and_default_of_a_domain_only_to_has() {
    // `compare_types` creates a new domain before any routine is dropped, so
    // the CASCADE takes its CHECK and — through its default — the domain.
    let (mut from, to) = rc_predrop_dumps();
    from.types.clear();
    let script = rc_compare(from, to, true, GrantsMode::Ignore).await;

    let create_domain = rc_position(
        &script,
        "create domain s1.code_def as text default s1.first_code();",
    );
    let detach = rc_position(&script, "alter domain s1.code_def drop default;");
    assert!(create_domain < detach);
    assert!(script.contains("alter domain s1.code_def set default s1.first_code();"));
    assert!(script.contains(
        "alter domain s1.code_d drop constraint if exists \"code_d_check\";\n\nalter domain s1.code_d add constraint \"code_d_check\" CHECK (s1.is_valid(VALUE));"
    ));
}

#[tokio::test]
async fn reg_cascade_restores_are_commented_out_without_use_drop() {
    // Nothing is dropped with `use_drop` off: the routine phase comments its
    // `DROP … CASCADE` out, and so every restore after it.
    let mut from = Dump::new(DumpConfig::default());
    let mut to = Dump::new(DumpConfig::default());
    for (dump, return_type) in [(&mut from, "integer"), (&mut to, "bigint")] {
        dump.routines
            .push(rc_plpgsql("score", "x integer", return_type, None));
        dump.types.push(rc_domain(
            "pos_d",
            Some("s1.score(7)"),
            &[("pos_d_check", "CHECK ((s1.score(VALUE) > 0))")],
        ));
        dump.tables.push(rc_table(
            "items",
            vec![
                int_column("s1", "items", "id", 1),
                int_column("s1", "items", "qty", 2),
            ],
            vec![rc_trigger(
                "items_guard",
                "items",
                "(s1.score(new.qty) < 0)",
            )],
        ));
        dump.statistics
            .push(rc_statistic("items_st", "items", "s1.score(qty), id"));
    }
    let script = rc_compare(from, to, false, GrantsMode::Ignore).await;
    for commented in [
        "-- alter domain s1.pos_d drop default;",
        "-- alter domain s1.pos_d set default s1.score(7);",
        "-- CREATE TRIGGER items_guard",
        "-- create statistics s1.items_st",
        "-- alter domain s1.pos_d add constraint \"pos_d_check\"",
    ] {
        assert!(script.contains(commented), "{commented} in:\n{script}");
    }
    for live in [
        "\nalter domain s1.pos_d drop default;",
        "\nCREATE TRIGGER items_guard",
        "\ncreate statistics s1.items_st",
    ] {
        assert!(!script.contains(live), "{live} in:\n{script}");
    }
}

#[tokio::test]
async fn reg_cascade_skips_partition_trigger_clones_and_recreated_tables() {
    let mut from = Dump::new(DumpConfig::default());
    let mut to = Dump::new(DumpConfig::default());
    for (dump, return_type) in [(&mut from, "integer"), (&mut to, "bigint")] {
        dump.routines
            .push(rc_plpgsql("score", "x integer", return_type, None));
        let when = "(s1.score(new.qty) < 0)";
        let mut parent = rc_table(
            "parted",
            vec![int_column("s1", "parted", "qty", 1)],
            vec![rc_trigger("guard", "parted", when)],
        );
        parent.partition_key = Some("RANGE (qty)".to_string());
        parent.hash();
        // A clone the dump records (disabled on its own) is not a trigger
        // of the partition: it is never dropped or created there.
        let mut clone = rc_trigger("guard", "parted_1", when);
        clone.enabled = "D".to_string();
        let mut child = rc_table(
            "parted_1",
            vec![int_column("s1", "parted_1", "qty", 1)],
            vec![],
        );
        child.trigger_clones = vec![clone];
        child.partition_of = Some("s1.parted".to_string());
        child.hash();
        dump.tables.push(parent);
        dump.tables.push(child);
    }
    let script = rc_compare(from, to, true, GrantsMode::Ignore).await;
    assert!(
        script.contains("drop trigger if exists guard on s1.parted;"),
        "{script}"
    );
    // The clone comes back with the parent's trigger, and only its own
    // state is set again.
    assert!(
        !script.contains("trigger if exists guard on s1.parted_1"),
        "{script}"
    );
    assert!(!script.contains("ON s1.parted_1"), "{script}");
    assert_eq!(
        script
            .matches("alter table s1.parted_1 disable trigger guard;")
            .count(),
        1,
        "{script}"
    );
}

#[tokio::test]
async fn reg_cascade_restores_the_triggers_of_a_table_the_alter_path_recreates() {
    // The partition key changes, so `compare_tables` drops and re-creates
    // the table with its triggers — before the routine phase drops `score`
    // with CASCADE, which takes the trigger again.
    let mut from = Dump::new(DumpConfig::default());
    let mut to = Dump::new(DumpConfig::default());
    for (dump, return_type, key) in [
        (&mut from, "integer", "RANGE (qty)"),
        (&mut to, "bigint", "LIST (qty)"),
    ] {
        dump.routines
            .push(rc_plpgsql("score", "x integer", return_type, None));
        let mut rebuilt = rc_table(
            "rebuilt",
            vec![int_column("s1", "rebuilt", "qty", 1)],
            vec![rc_trigger("guard", "rebuilt", "(s1.score(new.qty) < 0)")],
        );
        rebuilt.partition_key = Some(key.to_string());
        rebuilt.hash();
        dump.tables.push(rebuilt);
    }
    let script = rc_compare(from, to, true, GrantsMode::Ignore).await;
    let recreate = rc_position(&script, "drop table if exists s1.rebuilt;");
    let drop_score = rc_position(
        &script,
        "drop function if exists s1.score (x integer) cascade;",
    );
    let restore = rc_position(&script, "drop trigger if exists guard on s1.rebuilt;");
    assert!(recreate < drop_score && drop_score < restore, "{script}");
    assert_eq!(
        script.matches("CREATE TRIGGER guard").count(),
        2,
        "{script}"
    );
}

#[tokio::test]
async fn reg_cascade_leaves_the_triggers_of_a_partition_recreated_with_its_parent() {
    // The parent's partition key changes: the partition is re-created with
    // it, and its triggers wait in `trigger_post_script`, after every
    // routine. A restore of its own would create one twice.
    let mut from = Dump::new(DumpConfig::default());
    let mut to = Dump::new(DumpConfig::default());
    for (dump, return_type, key) in [
        (&mut from, "integer", "RANGE (qty)"),
        (&mut to, "bigint", "LIST (qty)"),
    ] {
        dump.routines
            .push(rc_plpgsql("score", "x integer", return_type, None));
        let mut parent = rc_table("parted", vec![int_column("s1", "parted", "qty", 1)], vec![]);
        parent.partition_key = Some(key.to_string());
        parent.hash();
        let mut child = rc_table(
            "parted_1",
            vec![int_column("s1", "parted_1", "qty", 1)],
            vec![rc_trigger(
                "own_guard",
                "parted_1",
                "(s1.score(new.qty) < 0)",
            )],
        );
        child.partition_of = Some("s1.parted".to_string());
        // A stand-in change so the child is compared at all.
        child.comment = Some(format!("{return_type} era"));
        child.hash();
        dump.tables.push(parent);
        dump.tables.push(child);
    }
    let script = rc_compare(from, to, true, GrantsMode::Ignore).await;
    assert_eq!(
        script.matches("CREATE TRIGGER own_guard").count(),
        1,
        "{script}"
    );
    assert!(
        !script.contains("drop trigger if exists own_guard"),
        "{script}"
    );
    let drop_score = rc_position(
        &script,
        "drop function if exists s1.score (x integer) cascade;",
    );
    assert!(
        drop_score < rc_position(&script, "CREATE TRIGGER own_guard"),
        "{script}"
    );
}

#[tokio::test]
async fn reg_cascade_keeps_back_a_routine_an_operator_is_built_on() {
    // `s1.opf` reads the retyped column, but operator `s1.===` is built on
    // it: its CASCADE would take the operator and whatever uses it, which pgc
    // cannot re-create. So it stays, and the retype fails loudly on
    // PostgreSQL's dependency check, as with pgc 1.0.x.
    let (mut from, mut to) = rc_predrop_dumps();
    for dump in [&mut from, &mut to] {
        dump.routines
            .push(rc_codes_reader("opf", "a integer, b integer", "boolean"));
        let mut op = Operator {
            schema: "s1".to_string(),
            name: "===".to_string(),
            owner: "postgres".to_string(),
            left_type: Some("integer".to_string()),
            right_type: Some("integer".to_string()),
            result_type: "boolean".to_string(),
            procedure: "s1.opf".to_string(),
            commutator: None,
            negator: None,
            restrict: None,
            join: None,
            is_hashes: false,
            is_merges: false,
            comment: None,
            hash: None,
        };
        op.hash();
        dump.operators.push(op);
    }
    let script = rc_compare(from, to, true, GrantsMode::Ignore).await;
    assert!(
        !script.contains("drop function if exists s1.opf"),
        "{script}"
    );
    assert!(script.contains(
        "-- Routine s1.opf(a integer, b integer) is in the way of this migration but is not dropped ahead of it: operator s1.===(integer, integer) depends on it, which a DROP ... CASCADE would take along and pgc cannot re-create (manual intervention needed)."
    ));
    // The other readers are dropped and restored as before.
    assert!(script.contains("drop function if exists s1.is_valid (x integer) cascade;"));
    assert!(script.contains("CREATE TRIGGER orders_chk"));
}

#[tokio::test]
async fn reg_cascade_keeps_back_what_an_unrestorable_dependent_reaches_through() {
    // `s1.bucket` is a SQL-standard routine calling `s1.first_code`, and a
    // partition key calls `s1.bucket`. Dropping `first_code` with CASCADE
    // would take `bucket` and, with it, the partitioned table and its data:
    // neither is dropped ahead of the retype.
    let (mut from, mut to) = rc_predrop_dumps();
    for dump in [&mut from, &mut to] {
        let mut bucket = rc_codes_reader("bucket", "k integer", "integer");
        bucket.sql_body_dependencies = SqlBodyDependencies {
            routines: vec!["s1.first_code()".to_string()],
            ..SqlBodyDependencies::default()
        };
        bucket.hash();
        dump.routines.push(bucket);
        let mut parted = rc_table("parted", vec![int_column("s1", "parted", "k", 1)], vec![]);
        parted.partition_key = Some("RANGE (s1.bucket(k))".to_string());
        parted.hash();
        dump.tables.push(parted);
    }
    let script = rc_compare(from, to, true, GrantsMode::Ignore).await;
    assert!(
        !script.contains("drop function if exists s1.bucket"),
        "{script}"
    );
    assert!(
        !script.contains("drop function if exists s1.first_code"),
        "{script}"
    );
    assert!(script.contains("Routine s1.bucket(k integer) is in the way"));
    assert!(script.contains(
        "Routine s1.first_code() is in the way of this migration but is not dropped ahead of it: the partition key of table s1.parted, through routine s1.bucket(k integer), depends on it"
    ), "{script}");
    // Nothing detaches the default of a domain whose routine stays.
    assert!(!script.contains("alter domain s1.code_def drop default;"));
    // `is_valid` has nothing unrestorable behind it and still goes.
    assert!(script.contains("drop function if exists s1.is_valid (x integer) cascade;"));
}

#[tokio::test]
async fn reg_cascade_does_not_keep_back_a_routine_a_later_cascade_takes_anyway() {
    // `s1.opf` backs an operator, but it calls `s1.score`, whose return type
    // changes: the routine phase's CASCADE takes `opf` whatever happens, so
    // keeping it back would only lose it. It is dropped ahead and re-created,
    // as without the operator.
    let mut from = Dump::new(DumpConfig::default());
    let mut to = Dump::new(DumpConfig::default());
    for (dump, return_type) in [(&mut from, "integer"), (&mut to, "bigint")] {
        dump.routines
            .push(rc_plpgsql("score", "x integer", return_type, None));
        let mut opf = rc_codes_reader("opf", "a integer, b integer", "boolean");
        opf.sql_body_dependencies = SqlBodyDependencies {
            routines: vec!["s1.score(x integer)".to_string()],
            ..SqlBodyDependencies::default()
        };
        opf.hash();
        dump.routines.push(opf);
        let mut op = Operator {
            schema: "s1".to_string(),
            name: "===".to_string(),
            owner: "postgres".to_string(),
            left_type: Some("integer".to_string()),
            right_type: Some("integer".to_string()),
            result_type: "boolean".to_string(),
            procedure: "s1.opf".to_string(),
            commutator: None,
            negator: None,
            restrict: None,
            join: None,
            is_hashes: false,
            is_merges: false,
            comment: None,
            hash: None,
        };
        op.hash();
        dump.operators.push(op);
    }
    let script = rc_compare(from, to, true, GrantsMode::Ignore).await;
    assert!(
        !script.contains("is in the way of this migration"),
        "{script}"
    );
    let drop_opf = rc_position(
        &script,
        "drop function if exists s1.opf (a integer, b integer) cascade;",
    );
    let create_score = rc_position(
        &script,
        "create or replace function s1.score(x integer) returns bigint",
    );
    let create_opf = rc_position(
        &script,
        "create or replace function s1.opf(a integer, b integer)",
    );
    assert!(drop_opf < create_score && create_score < create_opf);
}

#[test]
fn reg_cascade_unrestorable_dependents_are_found_by_kind() {
    let mut from = Dump::new(DumpConfig::default());
    let routine = rc_codes_reader("f", "x integer", "integer");
    let mut range = make_domain_type("s1", "frange", 901);
    range.typtype = 'r' as i8;
    range.range_subdiff = Some("s1.f".to_string());
    from.types.push(range);
    let mut parted = rc_table("parted", vec![int_column("s1", "parted", "k", 1)], vec![]);
    parted.partition_key = Some("LIST (s1.f(k))".to_string());
    from.tables.push(parted);
    let mut cast = Cast {
        source_type: "s1.pair".to_string(),
        target_type: "integer".to_string(),
        cast_method: "f".to_string(),
        function_name: Some("s1.f(x integer)".to_string()),
        cast_context: "e".to_string(),
        comment: None,
        hash: None,
    };
    cast.hash();
    from.casts.push(cast);
    from.routines.push(routine.clone());
    let comparer = Comparer::new(
        from,
        Dump::new(DumpConfig::default()),
        true,
        false,
        true,
        GrantsMode::Ignore,
    );
    assert_eq!(
        comparer.unrestorable_cascade_dependents(&routine),
        vec![
            "the partition key of table s1.parted".to_string(),
            "range type s1.frange".to_string(),
            "cast (s1.pair as integer)".to_string(),
        ]
    );
    // Another overload of the cast's function is not the cast's function.
    let other = rc_codes_reader("f", "x bigint", "integer");
    assert_eq!(
        comparer.unrestorable_cascade_dependents(&other),
        vec![
            "the partition key of table s1.parted".to_string(),
            "range type s1.frange".to_string(),
        ]
    );
}

#[test]
fn reg_cascade_reference_and_default_matchers() {
    assert!(Comparer::reference_names_routine("s1.f", "s1", "f"));
    assert!(Comparer::reference_names_routine(
        "\"S1\".\"F\"(integer)",
        "s1",
        "f"
    ));
    assert!(Comparer::reference_names_routine("f", "s1", "f"));
    assert!(!Comparer::reference_names_routine("s1.f2", "s1", "f"));
    assert!(!Comparer::reference_names_routine("s2.f", "s1", "f"));

    let names = HashSet::from([("s1".to_string(), "chk".to_string())]);
    let with_default = |defaults: &str| rc_plpgsql("g", "x boolean", "boolean", Some(defaults));
    assert!(Comparer::defaults_call_any(
        &with_default("s1.chk('a'::text)"),
        &names
    ));
    assert!(Comparer::defaults_call_any(
        &with_default("1, chk(2)"),
        &names
    ));
    assert!(!Comparer::defaults_call_any(
        &with_default("'s1.chk(1)'::text"),
        &names
    ));
    assert!(!Comparer::defaults_call_any(
        &with_default("s1.chk_other(1)"),
        &names
    ));
    assert!(!Comparer::defaults_call_any(
        &rc_plpgsql("g", "x boolean", "boolean", None),
        &names
    ));
    assert!(!Comparer::defaults_call_any(
        &with_default("s1.chk(1)"),
        &HashSet::new()
    ));
}

#[test]
fn reg_cascade_domain_default_detach_reads_the_default_in_place() {
    let routine = rc_plpgsql("f", "", "integer", None);
    let detach_for = |from_default: Option<&str>, to_default: Option<&str>, use_drop: bool| {
        let mut from = Dump::new(DumpConfig::default());
        let mut to = Dump::new(DumpConfig::default());
        from.types.push(rc_domain("d", from_default, &[]));
        to.types.push(rc_domain("d", to_default, &[]));
        let comparer = Comparer::new(from, to, use_drop, false, true, GrantsMode::Ignore);
        comparer.domain_default_detach(&routine)
    };
    let detached = (
        "alter domain s1.d drop default;\n\n".to_string(),
        vec![("s1".to_string(), "d".to_string())],
    );
    let untouched = (String::new(), Vec::new());
    // Unchanged: the FROM default is in place.
    assert_eq!(detach_for(Some("s1.f()"), Some("s1.f()"), true), detached);
    // `compare_types` already set a TO default that no longer calls it.
    assert_eq!(detach_for(Some("s1.f()"), Some("0"), true), untouched);
    // ... or one that now does.
    assert_eq!(detach_for(Some("0"), Some("s1.f() + 1"), true), detached);
    // It dropped the default, unless that drop is commented out.
    assert_eq!(detach_for(Some("s1.f()"), None, true), untouched);
    assert_eq!(detach_for(Some("s1.f()"), None, false), detached);
    assert_eq!(detach_for(None, None, true), untouched);
}

#[test]
fn reg_cascade_statistics_on_a_created_matview_are_left_to_it() {
    let mut from = Dump::new(DumpConfig::default());
    let mut to = Dump::new(DumpConfig::default());
    to.statistics
        .push(rc_statistic("mv_st", "mv", "s1.f(a), b"));
    to.statistics.push(rc_statistic("t_st", "t", "s1.f(a), b"));
    to.statistics.push(rc_statistic("plain_st", "t", "a, b"));
    let mut mv = View::new(
        "mv".to_string(),
        " SELECT 1 AS a, 2 AS b;".to_string(),
        "s1".to_string(),
        vec![],
    );
    mv.is_materialized = true;
    mv.hash();
    to.views.push(mv);
    from.routines
        .push(rc_plpgsql("f", "a integer", "integer", None));
    let comparer = Comparer::new(from, to, true, false, true, GrantsMode::Ignore);
    let affected = HashSet::from([("s1".to_string(), "f".to_string())]);
    assert_eq!(
        comparer.statistics_restored_after_cascade(&affected),
        vec![("s1".to_string(), "t_st".to_string())]
    );
    assert!(
        comparer
            .statistics_restored_after_cascade(&HashSet::new())
            .is_empty()
    );
}

#[tokio::test]
async fn reg_cascade_argument_default_orders_routine_creation() {
    // Both new: `s1.a_user`'s default calls `s1.z_callee`, which PostgreSQL
    // resolves when `a_user` is created, so the callee goes first although
    // it sorts after it.
    let mut to = Dump::new(DumpConfig::default());
    to.routines.push(rc_plpgsql(
        "a_user",
        "x integer",
        "integer",
        Some("s1.z_callee()"),
    ));
    to.routines
        .push(rc_plpgsql("z_callee", "", "integer", None));
    let script = rc_compare(
        Dump::new(DumpConfig::default()),
        to,
        true,
        GrantsMode::Ignore,
    )
    .await;
    let callee = rc_position(&script, "create or replace function s1.z_callee()");
    let user = rc_position(&script, "create or replace function s1.a_user(");
    assert!(callee < user, "{script}");
}

fn rc_check(table: &str, name: &str, definition: &str, coninhcount: i32) -> TableConstraint {
    TableConstraint {
        catalog: "postgres".to_string(),
        schema: "s1".to_string(),
        name: name.to_string(),
        table_name: table.to_string(),
        constraint_type: "CHECK".to_string(),
        is_deferrable: false,
        initially_deferred: false,
        definition: Some(definition.to_string()),
        coninhcount,
        is_enforced: true,
        no_inherit: false,
        nulls_not_distinct: false,
        comment: None,
    }
}

fn rc_index(table: &str, name: &str, on: &str) -> TableIndex {
    TableIndex {
        schema: "s1".to_string(),
        table: table.to_string(),
        name: name.to_string(),
        catalog: Some("postgres".to_string()),
        indexdef: format!("CREATE INDEX {name} ON s1.{table} USING btree ({on})"),
        is_partition_index: false,
        comment: None,
    }
}

/// FROM `s1.items (id, qty)` and nothing calling `s1.score`; TO has
/// `s1.score` returning bigint, and on `items` a new CHECK, expression index,
/// column default, generated column (with an index of its own) and policy
/// calling it, plus a new table `s1.fresh` with a CHECK calling it.
fn rc_table_phase_dumps() -> (Dump, Dump) {
    let mut from = Dump::new(DumpConfig::default());
    let mut to = Dump::new(DumpConfig::default());
    from.routines
        .push(rc_plpgsql("score", "x integer", "integer", None));
    to.routines
        .push(rc_plpgsql("score", "x integer", "bigint", None));
    from.tables.push(rc_table(
        "items",
        vec![
            int_column("s1", "items", "id", 1),
            int_column("s1", "items", "qty", 2),
        ],
        vec![],
    ));

    let mut note = int_column("s1", "items", "note", 3);
    note.column_default = Some("s1.score(3)".to_string());
    let mut doubled = int_column("s1", "items", "doubled", 4);
    doubled.is_generated = "ALWAYS".to_string();
    doubled.generation_expression = Some("(s1.score(qty) * 2)".to_string());
    doubled.generation_type = Some("s".to_string());
    let mut items = rc_table(
        "items",
        vec![
            int_column("s1", "items", "id", 1),
            int_column("s1", "items", "qty", 2),
            note,
            doubled,
        ],
        vec![],
    );
    items.constraints = vec![rc_check(
        "items",
        "items_qty_chk",
        "CHECK ((s1.score(qty) > 0))",
        0,
    )];
    items.indexes = vec![
        rc_index("items", "items_score_idx", "s1.score(qty)"),
        rc_index("items", "items_doubled_idx", "doubled"),
    ];
    items.policies = vec![TablePolicy {
        schema: "s1".to_string(),
        table: "items".to_string(),
        name: "items_pol".to_string(),
        command: "all".to_string(),
        permissive: true,
        roles: vec![],
        using_clause: Some("(s1.score(qty) > 0)".to_string()),
        check_clause: None,
    }];
    items.hash();
    to.tables.push(items);
    let mut fresh = rc_table("fresh", vec![int_column("s1", "fresh", "qty", 1)], vec![]);
    fresh.constraints = vec![rc_check(
        "fresh",
        "fresh_chk",
        "CHECK ((s1.score(qty) > 0))",
        0,
    )];
    fresh.hash();
    to.tables.push(fresh);
    to.column_dependents.push(ColumnDependent {
        schema: "s1".to_string(),
        table: "items".to_string(),
        column: "doubled".to_string(),
        kind: ColumnDependentKind::Index,
        dep_schema: "s1".to_string(),
        dep_table: "items".to_string(),
        dep_name: "items_doubled_idx".to_string(),
    });
    (from, to)
}

#[tokio::test]
async fn reg_cascade_restores_what_the_table_phase_creates_before_the_routine_phase_drop() {
    let (from, to) = rc_table_phase_dumps();
    let script = rc_compare(from, to, true, GrantsMode::Ignore).await;

    let drop_score = rc_position(
        &script,
        "drop function if exists s1.score (x integer) cascade;",
    );
    let create_score = rc_position(
        &script,
        "create or replace function s1.score(x integer) returns bigint",
    );
    // Created by the table phase first...
    assert!(rc_position(&script, "items_qty_chk") < drop_score);
    // ... and put back once the routine is back.
    for restored in [
        "alter table s1.items drop constraint if exists items_qty_chk;\n\nalter table s1.items add constraint items_qty_chk",
        "CREATE INDEX IF NOT EXISTS items_score_idx ON s1.items",
        "alter table s1.items alter column note set default s1.score(3);",
        "alter table s1.items add column if not exists doubled integer generated always as (s1.score(qty) * 2) stored;",
        // The generated column's own index, from the TO dump's column edges.
        "CREATE INDEX IF NOT EXISTS items_doubled_idx ON s1.items",
        "drop policy if exists items_pol on s1.items;",
        "alter table s1.fresh drop constraint if exists fresh_chk;",
    ] {
        assert!(create_score < rc_position(&script, restored), "{restored}");
    }
}

/// The `Tables` section of `script`.
fn rc_table_section(script: &str) -> &str {
    let start = rc_position(script, "Tables: Start section");
    let end = rc_position(script, "Tables: End section");
    &script[start..end]
}

#[tokio::test]
async fn reg_cascade_creates_what_calls_a_predropped_routine_after_it() {
    // `drop_views` drops `s1.is_valid` before the table phase, so the CHECK,
    // index, policy and column default TO gives `orders` calling it — and
    // the CHECK and default of a new table — cannot be created there: the
    // table phase leaves them out, and they are created once it is back.
    let (mut from, mut to) = rc_predrop_dumps();
    for dump in [&mut from, &mut to] {
        let orders = dump.tables.iter_mut().find(|t| t.name == "orders").unwrap();
        orders.columns.push(int_column("s1", "orders", "note", 3));
        orders.hash();
    }
    let orders = to.tables.iter_mut().find(|t| t.name == "orders").unwrap();
    orders.constraints = vec![rc_check(
        "orders",
        "orders_code_chk",
        "CHECK (s1.is_valid(code))",
        0,
    )];
    orders.indexes = vec![rc_index("orders", "orders_code_idx", "s1.is_valid(code)")];
    orders.policies = vec![TablePolicy {
        schema: "s1".to_string(),
        table: "orders".to_string(),
        name: "orders_pol".to_string(),
        command: "all".to_string(),
        permissive: true,
        roles: vec![],
        using_clause: Some("s1.is_valid(code)".to_string()),
        check_clause: None,
    }];
    orders.columns[2].column_default = Some("s1.is_valid(1)".to_string());
    orders.hash();
    let mut fresh_qty = int_column("s1", "fresh", "qty", 1);
    fresh_qty.column_default = Some("s1.is_valid(2)".to_string());
    let mut fresh = rc_table("fresh", vec![fresh_qty], vec![]);
    fresh.constraints = vec![rc_check(
        "fresh",
        "fresh_chk",
        "CHECK (s1.is_valid(qty))",
        0,
    )];
    fresh.hash();
    to.tables.push(fresh);
    let script = rc_compare(from, to, true, GrantsMode::Ignore).await;

    // The table phase creates the new table, and none of what calls the
    // routine.
    let tables = rc_table_section(&script);
    assert!(tables.contains("create table s1.fresh"), "{script}");
    assert!(!tables.contains("is_valid"), "{tables}");

    let create_is_valid = rc_position(&script, "create or replace function s1.is_valid(x integer)");
    for created in [
        "alter table s1.orders add constraint orders_code_chk check (s1.is_valid(code))",
        "CREATE INDEX IF NOT EXISTS orders_code_idx ON s1.orders USING btree (s1.is_valid(code));",
        "create policy orders_pol on s1.orders",
        "alter table s1.orders alter column note set default s1.is_valid(1);",
        "alter table s1.fresh add constraint fresh_chk check (s1.is_valid(qty))",
        "alter table s1.fresh alter column qty set default s1.is_valid(2);",
    ] {
        assert!(
            create_is_valid < rc_position(&script, created),
            "{created}\n{script}"
        );
        assert_eq!(script.matches(created).count(), 1, "{created}\n{script}");
    }
}

#[tokio::test]
async fn reg_cascade_table_phase_finds_gone_what_called_a_predropped_routine() {
    // FROM's CHECK and index on `orders` call `s1.is_valid`, TO's same-named
    // ones no longer do. The predrop's CASCADE took FROM's: the table phase
    // creates TO's without dropping anything first, and nothing is restored.
    let (mut from, mut to) = rc_predrop_dumps();
    let from_orders = from.tables.iter_mut().find(|t| t.name == "orders").unwrap();
    from_orders.constraints = vec![rc_check(
        "orders",
        "orders_code_chk",
        "CHECK (s1.is_valid(code))",
        0,
    )];
    from_orders.indexes = vec![rc_index("orders", "orders_code_idx", "s1.is_valid(code)")];
    from_orders.hash();
    let to_orders = to.tables.iter_mut().find(|t| t.name == "orders").unwrap();
    to_orders.constraints = vec![rc_check(
        "orders",
        "orders_code_chk",
        "CHECK ((code > 0))",
        0,
    )];
    to_orders.indexes = vec![rc_index("orders", "orders_code_idx", "code")];
    to_orders.hash();
    let script = rc_compare(from, to, true, GrantsMode::Ignore).await;

    let tables = rc_table_section(&script);
    assert!(
        tables.contains("alter table s1.orders add constraint orders_code_chk check ((code > 0))"),
        "{tables}"
    );
    assert!(
        tables.contains("CREATE INDEX orders_code_idx ON s1.orders USING btree (code);"),
        "{tables}"
    );
    assert!(!tables.contains("drop constraint"), "{tables}");
    assert!(!tables.contains("drop index"), "{tables}");
    assert!(!script.contains("drop constraint if exists orders_code_chk"));
    assert!(!script.contains("CREATE INDEX IF NOT EXISTS orders_code_idx"));
}

#[tokio::test]
async fn reg_cascade_keeps_back_a_routine_a_new_column_is_filled_with() {
    // TO adds to `orders` — which may hold rows — a column whose default
    // calls `s1.is_valid`: `ADD COLUMN` fills the rows with it, so it can
    // neither run without the routine nor wait for it. The routine stays,
    // and the retype fails on PostgreSQL's dependency check, as with pgc
    // 1.0.x, instead of the rows getting NULL.
    let (from, mut to) = rc_predrop_dumps();
    let orders = to.tables.iter_mut().find(|t| t.name == "orders").unwrap();
    let mut filled = int_column("s1", "orders", "filled", 3);
    filled.column_default = Some("s1.is_valid(0)".to_string());
    orders.columns.push(filled);
    orders.hash();
    let script = rc_compare(from, to, true, GrantsMode::Ignore).await;
    assert!(
        !script.contains("drop function if exists s1.is_valid"),
        "{script}"
    );
    assert!(script.contains(
        "-- Routine s1.is_valid(x integer) is in the way of this migration but is not dropped ahead of it: the default of new column s1.orders.filled needs it in place while the tables change (manual intervention needed)."
    ), "{script}");
    assert!(script.contains("add column filled integer default s1.is_valid(0)"));
    // The routine its default calls stays, so `s1.flag` does too.
    assert!(
        !script.contains("drop function if exists s1.flag"),
        "{script}"
    );
    // `first_code` has nothing of the kind and still goes.
    assert!(script.contains("drop function if exists s1.first_code () cascade;"));
}

#[tokio::test]
async fn reg_cascade_keeps_back_a_routine_a_new_generated_column_calls() {
    let (from, mut to) = rc_predrop_dumps();
    let orders = to.tables.iter_mut().find(|t| t.name == "orders").unwrap();
    let mut valid = int_column("s1", "orders", "valid", 3);
    valid.is_generated = "ALWAYS".to_string();
    valid.generation_expression = Some("s1.is_valid(code)".to_string());
    valid.generation_type = Some("s".to_string());
    orders.columns.push(valid);
    orders.hash();
    let script = rc_compare(from, to, true, GrantsMode::Ignore).await;
    assert!(script.contains(
        "is not dropped ahead of it: generated column s1.orders.valid needs it in place while the tables change"
    ), "{script}");
    assert!(!script.contains("drop function if exists s1.is_valid"));
}

#[tokio::test]
async fn reg_cascade_keeps_back_the_routine_a_new_domain_column_is_filled_by() {
    // `s1.code_def`'s default calls `s1.first_code`, and TO adds a column of
    // that domain to `orders`: detaching the default for the drop would fill
    // the existing rows with NULL instead.
    let (from, mut to) = rc_predrop_dumps();
    let orders = to.tables.iter_mut().find(|t| t.name == "orders").unwrap();
    let mut fallback = int_column("s1", "orders", "fallback", 3);
    fallback.domain_schema = Some("s1".to_string());
    fallback.domain_name = Some("code_def".to_string());
    orders.columns.push(fallback);
    orders.hash();
    let script = rc_compare(from, to, true, GrantsMode::Ignore).await;
    assert!(script.contains(
        "-- Routine s1.first_code() is in the way of this migration but is not dropped ahead of it: the default of domain s1.code_def for new column s1.orders.fallback needs it in place while the tables change (manual intervention needed)."
    ), "{script}");
    assert!(!script.contains("drop function if exists s1.first_code"));
    assert!(!script.contains("alter domain s1.code_def drop default;"));
    // A column of the domain that FROM already has is left alone: its rows
    // hold their values.
    let (mut from, mut to) = rc_predrop_dumps();
    for dump in [&mut from, &mut to] {
        let orders = dump.tables.iter_mut().find(|t| t.name == "orders").unwrap();
        let mut fallback = int_column("s1", "orders", "fallback", 3);
        fallback.domain_schema = Some("s1".to_string());
        fallback.domain_name = Some("code_def".to_string());
        orders.columns.push(fallback);
        orders.hash();
    }
    let script = rc_compare(from, to, true, GrantsMode::Ignore).await;
    assert!(script.contains("drop function if exists s1.first_code () cascade;"));
}

#[tokio::test]
async fn reg_cascade_argument_default_user_waits_for_the_routine_phase() {
    // The reviewer's scenario: `s1.score`'s return type changes, and TO gives
    // table `t` a column whose default calls `s1.score_or`, whose argument
    // default calls `s1.score`. The table phase needs `score_or`, so it is
    // not dropped ahead of the tables; `score`'s CASCADE takes it, and the
    // column's default with it, in the routine phase, and both come back.
    let mut from = Dump::new(DumpConfig::default());
    let mut to = Dump::new(DumpConfig::default());
    for (dump, return_type) in [(&mut from, "integer"), (&mut to, "bigint")] {
        dump.routines
            .push(rc_plpgsql("score", "x integer", return_type, None));
        dump.routines.push(rc_plpgsql(
            "score_or",
            "x bigint",
            "bigint",
            Some("s1.score(1)"),
        ));
    }
    from.tables
        .push(rc_table("t", vec![int_column("s1", "t", "id", 1)], vec![]));
    let mut c = int_column("s1", "t", "c", 2);
    c.column_default = Some("s1.score_or()".to_string());
    to.tables.push(rc_table(
        "t",
        vec![int_column("s1", "t", "id", 1), c],
        vec![],
    ));
    let script = rc_compare(from, to, true, GrantsMode::Ignore).await;

    assert!(
        !script.contains("drop function if exists s1.score_or"),
        "{script}"
    );
    let add = rc_position(
        &script,
        "alter table s1.t add column c integer default s1.score_or();",
    );
    let drop_score = rc_position(
        &script,
        "drop function if exists s1.score (x integer) cascade;",
    );
    let create_score = rc_position(
        &script,
        "create or replace function s1.score(x integer) returns bigint",
    );
    let create_score_or = rc_position(
        &script,
        "create or replace function s1.score_or(x bigint DEFAULT s1.score(1))",
    );
    let restore = rc_position(
        &script,
        "alter table s1.t alter column c set default s1.score_or();",
    );
    assert!(
        add < drop_score
            && drop_score < create_score
            && create_score < create_score_or
            && create_score_or < restore,
        "{script}"
    );
}

#[tokio::test]
async fn reg_cascade_routine_phase_recreates_what_its_cascade_takes_and_their_grants() {
    // `s1.score_or`'s default calls `s1.score`, whose return type changes;
    // `s1.reader`'s SQL-standard body calls `score_or`, and `s1.via` is a
    // view over `reader`. The routine phase's CASCADE takes all three: both
    // routines come back after `score`, with their privileges, and so does
    // the view.
    let mut from = Dump::new(DumpConfig::default());
    let mut to = Dump::new(DumpConfig::default());
    for (dump, return_type) in [(&mut from, "integer"), (&mut to, "bigint")] {
        dump.routines
            .push(rc_plpgsql("score", "x integer", return_type, None));
        let mut score_or = rc_plpgsql("score_or", "x bigint", "bigint", Some("s1.score(1)"));
        score_or.acl = vec![
            "postgres=X/postgres".to_string(),
            "reader=X/postgres".to_string(),
        ];
        score_or.hash();
        dump.routines.push(score_or);
        let mut reader = rc_codes_reader("reader", "", "bigint");
        reader.source_code = "RETURN s1.score_or()".to_string();
        reader.sql_body_dependencies = SqlBodyDependencies {
            routines: vec!["s1.score_or(x bigint)".to_string()],
            ..SqlBodyDependencies::default()
        };
        reader.hash();
        dump.routines.push(reader);
        let mut via = View::new(
            "via".to_string(),
            " SELECT s1.reader() AS r;".to_string(),
            "s1".to_string(),
            vec![],
        );
        via.hash();
        dump.views.push(via);
    }
    let script = rc_compare(from, to, true, GrantsMode::Full).await;
    let create_score = rc_position(
        &script,
        "create or replace function s1.score(x integer) returns bigint",
    );
    let create_score_or = rc_position(&script, "create or replace function s1.score_or(");
    let create_reader = rc_position(&script, "create or replace function s1.reader(");
    let view = rc_position(&script, "CREATE OR REPLACE VIEW s1.via");
    assert!(
        create_score < create_score_or && create_score_or < create_reader && create_reader < view,
        "{script}"
    );
    assert!(
        script.contains("GRANT EXECUTE ON FUNCTION s1.score_or(x bigint) TO reader;"),
        "{script}"
    );
    assert!(script.contains("REVOKE EXECUTE ON FUNCTION s1.score_or(x bigint) FROM PUBLIC;"));
}

#[tokio::test]
async fn reg_cascade_routine_phase_drop_without_cascade_when_a_partition_key_calls_it() {
    // The reviewer's scenario: `s1.bucket`'s return type changes, and a
    // partition key calls it. Its CASCADE would take the partitioned table
    // and every row: it is dropped without CASCADE instead, and the
    // migration stops there.
    let mut from = Dump::new(DumpConfig::default());
    let mut to = Dump::new(DumpConfig::default());
    for (dump, return_type) in [(&mut from, "integer"), (&mut to, "bigint")] {
        dump.routines
            .push(rc_plpgsql("bucket", "x integer", return_type, None));
        dump.types
            .push(rc_domain("bucket_d", Some("s1.bucket(1)"), &[]));
        let mut parted = rc_table("parted", vec![int_column("s1", "parted", "id", 1)], vec![]);
        parted.partition_key = Some("RANGE (s1.bucket(id))".to_string());
        parted.hash();
        dump.tables.push(parted);
    }
    let script = rc_compare(from, to, true, GrantsMode::Ignore).await;
    let note = rc_position(
        &script,
        "-- Routine s1.bucket(x integer) is dropped without CASCADE: the partition key of table s1.parted depends on it, which a DROP ... CASCADE would take along and pgc cannot re-create (manual intervention needed).",
    );
    let drop = rc_position(&script, "drop function if exists s1.bucket (x integer);\n");
    assert!(note < drop, "{script}");
    assert!(
        !script.contains("s1.bucket (x integer) cascade"),
        "{script}"
    );
    // Nothing is taken, so nothing is detached.
    assert!(
        !script.contains("alter domain s1.bucket_d drop default;"),
        "{script}"
    );

    // A FROM-only overload the key does not call: the drop keeps its
    // CASCADE only where nothing is in the way — here the name matches, so
    // it goes without, and fails only if something really depends on it.
    let mut from = Dump::new(DumpConfig::default());
    let mut to = Dump::new(DumpConfig::default());
    from.routines
        .push(rc_plpgsql("bucket", "x integer", "integer", None));
    to.routines
        .push(rc_plpgsql("bucket", "x bigint", "integer", None));
    for dump in [&mut from, &mut to] {
        let mut parted = rc_table("parted", vec![int_column("s1", "parted", "id", 1)], vec![]);
        parted.partition_key = Some("RANGE (s1.bucket(id))".to_string());
        parted.hash();
        dump.tables.push(parted);
    }
    let script = rc_compare(from, to, true, GrantsMode::Ignore).await;
    assert!(
        script.contains("drop function if exists s1.bucket (x integer);\n"),
        "{script}"
    );
    assert!(script.contains("-- Routine s1.bucket(x integer) is dropped without CASCADE"));

    // With `use_drop` off the drop is commented out anyway.
    let mut from = Dump::new(DumpConfig::default());
    let mut to = Dump::new(DumpConfig::default());
    for (dump, return_type) in [(&mut from, "integer"), (&mut to, "bigint")] {
        dump.routines
            .push(rc_plpgsql("bucket", "x integer", return_type, None));
        let mut parted = rc_table("parted", vec![int_column("s1", "parted", "id", 1)], vec![]);
        parted.partition_key = Some("RANGE (s1.bucket(id))".to_string());
        parted.hash();
        dump.tables.push(parted);
    }
    let script = rc_compare(from, to, false, GrantsMode::Ignore).await;
    assert!(!script.contains("is dropped without CASCADE"), "{script}");
    assert!(script.contains("-- drop function if exists s1.bucket (x integer) cascade;"));
}

#[test]
fn reg_cascade_routine_phase_blockers_are_what_is_in_place_then() {
    let routine = rc_plpgsql("f", "x integer", "integer", None);
    let mut from = Dump::new(DumpConfig::default());
    let mut to = Dump::new(DumpConfig::default());
    // A partitioned table only FROM has is dropped by the table phase first;
    // one only TO has was created by it.
    let mut gone = rc_table("gone", vec![int_column("s1", "gone", "k", 1)], vec![]);
    gone.partition_key = Some("LIST (s1.f(k))".to_string());
    from.tables.push(gone);
    let mut fresh = rc_table("fresh", vec![int_column("s1", "fresh", "k", 1)], vec![]);
    fresh.partition_key = Some("LIST (s1.f(k))".to_string());
    to.tables.push(fresh);
    // An operator TO keeps unchanged would not come back; a changed one is
    // rebuilt by `compare_operators`.
    let operator = |name: &str, result_type: &str| {
        let mut op = Operator {
            schema: "s1".to_string(),
            name: name.to_string(),
            owner: "postgres".to_string(),
            left_type: Some("integer".to_string()),
            right_type: Some("integer".to_string()),
            result_type: result_type.to_string(),
            procedure: "s1.f".to_string(),
            commutator: None,
            negator: None,
            restrict: None,
            join: None,
            is_hashes: false,
            is_merges: false,
            comment: None,
            hash: None,
        };
        op.hash();
        op
    };
    from.operators.push(operator("===", "boolean"));
    to.operators.push(operator("===", "boolean"));
    from.operators.push(operator("!==", "boolean"));
    to.operators.push(operator("!==", "integer"));
    let comparer = Comparer::new(from, to, true, false, true, GrantsMode::Ignore);
    assert_eq!(
        comparer.cascade_blockers(&[&routine], CascadeMoment::RoutinePhase),
        vec![
            "the partition key of table s1.fresh".to_string(),
            "operator s1.===(integer, integer)".to_string(),
        ]
    );
    assert_eq!(
        comparer.cascade_blockers(&[&routine], CascadeMoment::AheadOfTables),
        vec![
            "the partition key of table s1.gone".to_string(),
            "operator s1.===(integer, integer)".to_string(),
            "operator s1.!==(integer, integer)".to_string(),
        ]
    );
}

#[test]
fn reg_cascade_without_cascade_drops_only_the_cascade() {
    assert_eq!(
        Comparer::without_cascade("drop function if exists s1.f (x integer) cascade;\n\n"),
        "drop function if exists s1.f (x integer);\n\n"
    );
    assert_eq!(
        Comparer::without_cascade("drop routine if exists s1.\"a cascade;\" () cascade;\n\n"),
        "drop routine if exists s1.\"a cascade;\" ();\n\n"
    );
    assert_eq!(Comparer::without_cascade("drop view v;"), "drop view v;");
}

#[tokio::test]
async fn reg_cascade_restores_the_state_and_comment_of_partition_trigger_clones() {
    // The dump records a partition's clone of its parent's trigger when its
    // state is its own (`Table::trigger_clones`). When the parent's trigger
    // is created again, every clone comes back in the parent's state and
    // without a comment: a clone disabled on its own, or carrying its own
    // comment, is set back.
    let mut from = Dump::new(DumpConfig::default());
    let mut to = Dump::new(DumpConfig::default());
    for (dump, return_type) in [(&mut from, "integer"), (&mut to, "bigint")] {
        dump.routines
            .push(rc_plpgsql("score", "x integer", return_type, None));
        let when = "(s1.score(new.qty) < 0)";
        let mut parent = rc_table(
            "parted",
            vec![int_column("s1", "parted", "qty", 1)],
            vec![rc_trigger("guard", "parted", when)],
        );
        parent.partition_key = Some("RANGE (qty)".to_string());
        parent.hash();
        dump.tables.push(parent);
        for (name, enabled, comment) in [
            ("parted_1", "D", None),
            ("parted_2", "O", Some("it's the second")),
            ("parted_3", "O", None),
        ] {
            let mut clone = rc_trigger("guard", name, when);
            clone.enabled = enabled.to_string();
            clone.comment = comment.map(str::to_string);
            let mut child = rc_table(name, vec![int_column("s1", name, "qty", 1)], vec![]);
            // A clone in the parent's state is not recorded.
            if enabled != "O" || comment.is_some() {
                child.trigger_clones = vec![clone];
            }
            child.partition_of = Some("s1.parted".to_string());
            child.hash();
            dump.tables.push(child);
        }
    }
    let script = rc_compare(from, to, true, GrantsMode::Ignore).await;
    let parent = rc_position(&script, "drop trigger if exists guard on s1.parted;");
    let disabled = rc_position(&script, "alter table s1.parted_1 disable trigger guard;");
    let comment = rc_position(
        &script,
        "comment on trigger guard on s1.parted_2 is 'it''s the second';",
    );
    assert!(parent < disabled && parent < comment, "{script}");
    assert!(!script.contains("guard on s1.parted_3"), "{script}");
    assert!(!script.contains("s1.parted_3 enable"), "{script}");
    assert!(!script.contains("drop trigger if exists guard on s1.parted_1"));
}

/// `s1.mv`, a materialized view over `s1.items` that calls nothing, with an
/// index calling `s1.score`.
fn rc_scored_matview() -> View {
    let mut mv = View::new(
        "mv".to_string(),
        " SELECT items.id, items.qty FROM s1.items;".to_string(),
        "s1".to_string(),
        vec!["s1.items".to_string()],
    );
    mv.is_materialized = true;
    mv.indexes = vec![TableIndex {
        schema: "s1".to_string(),
        table: "mv".to_string(),
        name: "mv_idx".to_string(),
        catalog: Some("postgres".to_string()),
        indexdef: "CREATE INDEX mv_idx ON s1.mv USING btree (s1.score(qty))".to_string(),
        is_partition_index: false,
        comment: None,
    }];
    mv.hash();
    mv
}

#[tokio::test]
async fn reg_cascade_restores_an_index_on_a_kept_materialized_view() {
    // The view itself calls nothing, but its index calls `s1.score`, whose
    // return type changes: the CASCADE takes the index alone.
    let mut from = Dump::new(DumpConfig::default());
    let mut to = Dump::new(DumpConfig::default());
    for (dump, return_type) in [(&mut from, "integer"), (&mut to, "bigint")] {
        dump.routines
            .push(rc_plpgsql("score", "x integer", return_type, None));
        dump.tables.push(rc_table(
            "items",
            vec![
                int_column("s1", "items", "id", 1),
                int_column("s1", "items", "qty", 2),
            ],
            vec![],
        ));
        dump.views.push(rc_scored_matview());
    }
    let script = rc_compare(from, to, true, GrantsMode::Ignore).await;
    let create_score = rc_position(
        &script,
        "create or replace function s1.score(x integer) returns bigint",
    );
    let restore = rc_position(
        &script,
        "CREATE INDEX IF NOT EXISTS mv_idx ON s1.mv USING btree (s1.score(qty));",
    );
    assert!(create_score < restore, "{script}");
    assert_eq!(script.matches("mv_idx ON s1.mv").count(), 1, "{script}");
    // The view stays.
    assert!(!script.contains("materialized view"), "{script}");
}

#[tokio::test]
async fn reg_cascade_leaves_what_its_cascade_takes_out_of_the_view_index_diff() {
    // `s1.score`'s return type changes. TO changes the comment of `mv_idx`,
    // which calls it, and gives `s1.mv2`, whose definition calls it, a new
    // index. The CASCADE has taken both by the time the index diff of
    // unchanged views runs: that diff leaves them alone, and the restore
    // creates them from TO, comment and index included.
    let mut from = Dump::new(DumpConfig::default());
    let mut to = Dump::new(DumpConfig::default());
    for (dump, return_type, comment) in [(&mut from, "integer", "old"), (&mut to, "bigint", "new")]
    {
        dump.routines
            .push(rc_plpgsql("score", "x integer", return_type, None));
        let mut mv = rc_scored_matview();
        mv.indexes[0].comment = Some(comment.to_string());
        mv.hash();
        dump.views.push(mv);
        let mut mv2 = View::new(
            "mv2".to_string(),
            " SELECT items.id, s1.score(items.qty) AS s FROM s1.items;".to_string(),
            "s1".to_string(),
            vec!["s1.items".to_string()],
        );
        mv2.is_materialized = true;
        mv2.owner = "postgres".to_string();
        if return_type == "bigint" {
            mv2.indexes = vec![TableIndex {
                schema: "s1".to_string(),
                table: "mv2".to_string(),
                name: "mv2_s_idx".to_string(),
                catalog: Some("postgres".to_string()),
                indexdef: "CREATE INDEX mv2_s_idx ON s1.mv2 USING btree (s)".to_string(),
                is_partition_index: false,
                comment: None,
            }];
        }
        mv2.hash();
        dump.views.push(mv2);
    }
    let script = rc_compare(from, to, true, GrantsMode::Ignore).await;
    let restore_start = rc_position(&script, "Recreate dependents dropped by CASCADE: Start");
    assert_eq!(
        script
            .matches("comment on index s1.mv_idx is 'new';")
            .count(),
        1,
        "{script}"
    );
    assert!(rc_position(&script, "comment on index s1.mv_idx is 'new';") > restore_start);
    assert_eq!(script.matches("mv2_s_idx ON s1.mv2").count(), 1, "{script}");
    assert!(
        rc_position(&script, "CREATE INDEX IF NOT EXISTS mv2_s_idx ON s1.mv2") > restore_start,
        "{script}"
    );
    assert!(script[restore_start..].contains("create materialized view if not exists s1.mv2"));

    // With `use_drop` off nothing is taken, and the index diff runs as ever.
    let mut from = Dump::new(DumpConfig::default());
    let mut to = Dump::new(DumpConfig::default());
    for (dump, return_type, comment) in [(&mut from, "integer", "old"), (&mut to, "bigint", "new")]
    {
        dump.routines
            .push(rc_plpgsql("score", "x integer", return_type, None));
        let mut mv = rc_scored_matview();
        mv.indexes[0].comment = Some(comment.to_string());
        mv.hash();
        dump.views.push(mv);
    }
    let script = rc_compare(from, to, false, GrantsMode::Ignore).await;
    assert!(
        script.contains("\ncomment on index s1.mv_idx is 'new';"),
        "{script}"
    );
}

#[tokio::test]
async fn reg_cascade_creates_a_materialized_view_after_the_routines_its_indexes_call() {
    // Both new. `s1.a_mv`'s index calls `s1.z_norm`, whose body names view
    // `s1.b_view`, so `z_norm` only becomes available after `b_view` — and
    // `a_mv`, which sorts before `b_view`, has to wait for it too.
    let mut to = Dump::new(DumpConfig::default());
    let mut z_norm = rc_plpgsql("z_norm", "x integer", "integer", None);
    z_norm.source_code = " begin perform 1 from s1.b_view; return x; end ".to_string();
    z_norm.hash();
    to.routines.push(z_norm);
    let mut b_view = View::new(
        "b_view".to_string(),
        " SELECT 1 AS one;".to_string(),
        "s1".to_string(),
        vec![],
    );
    b_view.hash();
    to.views.push(b_view);
    let mut a_mv = View::new(
        "a_mv".to_string(),
        " SELECT 1 AS qty;".to_string(),
        "s1".to_string(),
        vec![],
    );
    a_mv.is_materialized = true;
    a_mv.indexes = vec![TableIndex {
        schema: "s1".to_string(),
        table: "a_mv".to_string(),
        name: "a_mv_idx".to_string(),
        catalog: Some("postgres".to_string()),
        indexdef: "CREATE INDEX a_mv_idx ON s1.a_mv USING btree (s1.z_norm(qty))".to_string(),
        is_partition_index: false,
        comment: None,
    }];
    a_mv.hash();
    to.views.push(a_mv);
    let script = rc_compare(
        Dump::new(DumpConfig::default()),
        to,
        true,
        GrantsMode::Ignore,
    )
    .await;
    let z_norm = rc_position(&script, "create or replace function s1.z_norm(");
    let a_mv = rc_position(&script, "create materialized view s1.a_mv");
    assert!(z_norm < a_mv, "{script}");
}

/// `s1.<name>` on server `srv`, one column `c bigint` defaulting to
/// `default`.
fn rc_foreign_table(name: &str, default: &str) -> ForeignTable {
    ForeignTable::new(
        "s1".to_string(),
        name.to_string(),
        "srv".to_string(),
        "postgres".to_string(),
        vec![],
        vec![ForeignTableColumn {
            name: "c".to_string(),
            data_type: "bigint".to_string(),
            is_nullable: true,
            column_default: Some(default.to_string()),
            options: vec![],
            acl: Vec::new(),
        }],
    )
}

#[tokio::test]
async fn reg_cascade_restores_foreign_table_column_defaults() {
    // `s1.score`'s return type changes. The default of `s1.ft`, which both
    // have, and of `s1.fresh`, which `compare_foreign_tables` creates before
    // the routine phase, call it: the CASCADE takes both.
    let mut from = Dump::new(DumpConfig::default());
    let mut to = Dump::new(DumpConfig::default());
    for (dump, return_type) in [(&mut from, "integer"), (&mut to, "bigint")] {
        dump.routines
            .push(rc_plpgsql("score", "x integer", return_type, None));
        dump.foreign_tables
            .push(rc_foreign_table("ft", "s1.score(7)"));
    }
    to.foreign_tables
        .push(rc_foreign_table("fresh", "s1.score(8)"));
    let script = rc_compare(from, to, true, GrantsMode::Ignore).await;
    let create_score = rc_position(
        &script,
        "create or replace function s1.score(x integer) returns bigint",
    );
    for restored in [
        "alter foreign table s1.ft alter column c set default s1.score(7);",
        "alter foreign table s1.fresh alter column c set default s1.score(8);",
    ] {
        assert!(
            create_score < rc_position(&script, restored),
            "{restored}\n{script}"
        );
        assert_eq!(script.matches(restored).count(), 1, "{restored}");
    }
}

#[tokio::test]
async fn reg_cascade_foreign_table_default_waits_for_a_predropped_routine() {
    // `s1.is_valid` is dropped ahead of the tables. TO changes a foreign
    // table's default to call it, and adds one whose default does: neither
    // can be set before it is back.
    let (mut from, mut to) = rc_predrop_dumps();
    from.foreign_tables.push(rc_foreign_table("ft", "0"));
    to.foreign_tables
        .push(rc_foreign_table("ft", "s1.is_valid(3)"));
    to.foreign_tables
        .push(rc_foreign_table("fresh", "s1.is_valid(4)"));
    let script = rc_compare(from, to, true, GrantsMode::Ignore).await;
    let foreign_start = rc_position(&script, "Compare Foreign Tables");
    let foreign_end = rc_position(&script, "Compare Statistics");
    assert!(
        !script[foreign_start..foreign_end].contains("is_valid"),
        "{script}"
    );
    let create_is_valid = rc_position(&script, "create or replace function s1.is_valid(x integer)");
    assert!(script.contains("create foreign table s1.fresh"));
    for set in [
        "alter foreign table s1.ft alter column c set default s1.is_valid(3);",
        "alter foreign table s1.fresh alter column c set default s1.is_valid(4);",
    ] {
        assert!(
            create_is_valid < rc_position(&script, set),
            "{set}\n{script}"
        );
    }
}

#[tokio::test]
async fn reg_cascade_restores_a_new_partitioned_check_on_the_parent_only() {
    let mut from = Dump::new(DumpConfig::default());
    let mut to = Dump::new(DumpConfig::default());
    for (dump, return_type, with_check) in
        [(&mut from, "integer", false), (&mut to, "bigint", true)]
    {
        dump.routines
            .push(rc_plpgsql("score", "x integer", return_type, None));
        let mut parent = rc_table("parted", vec![int_column("s1", "parted", "qty", 1)], vec![]);
        parent.partition_key = Some("RANGE (qty)".to_string());
        let mut child = rc_table(
            "parted_1",
            vec![int_column("s1", "parted_1", "qty", 1)],
            vec![],
        );
        child.partition_of = Some("s1.parted".to_string());
        if with_check {
            parent.constraints = vec![rc_check(
                "parted",
                "parted_chk",
                "CHECK ((s1.score(qty) > 0))",
                0,
            )];
            child.constraints = vec![rc_check(
                "parted_1",
                "parted_chk",
                "CHECK ((s1.score(qty) > 0))",
                1,
            )];
        }
        parent.hash();
        child.hash();
        dump.tables.push(parent);
        dump.tables.push(child);
    }
    let script = rc_compare(from, to, true, GrantsMode::Ignore).await;
    assert!(
        script.contains("alter table s1.parted drop constraint if exists parted_chk;"),
        "{script}"
    );
    assert!(
        !script.contains("alter table s1.parted_1 drop constraint if exists parted_chk;"),
        "{script}"
    );
}

// ==== pre-recreate: a partition trigger is a clone only when it repeats a
// row trigger of its parent.

#[tokio::test]
async fn reg_cascade_restores_a_partitions_own_trigger_named_like_the_parents() {
    // The parent's `guard` is a statement trigger, which PostgreSQL does not
    // clone; the partition's `guard` is a row trigger of its own. The
    // routine's CASCADE takes both, and both come back.
    let mut from = Dump::new(DumpConfig::default());
    let mut to = Dump::new(DumpConfig::default());
    for (dump, return_type) in [(&mut from, "integer"), (&mut to, "bigint")] {
        dump.routines
            .push(rc_plpgsql("score", "x integer", return_type, None));
        let mut statement = rc_trigger("guard", "parted", "(s1.score(1) < 0)");
        statement.definition = statement
            .definition
            .replace("FOR EACH ROW", "FOR EACH STATEMENT");
        let mut parent = rc_table(
            "parted",
            vec![int_column("s1", "parted", "qty", 1)],
            vec![statement],
        );
        parent.partition_key = Some("RANGE (qty)".to_string());
        parent.hash();
        let mut child = rc_table(
            "parted_1",
            vec![int_column("s1", "parted_1", "qty", 1)],
            vec![rc_trigger("guard", "parted_1", "(s1.score(new.qty) < 0)")],
        );
        child.partition_of = Some("s1.parted".to_string());
        child.hash();
        dump.tables.push(parent);
        dump.tables.push(child);
    }
    let script = rc_compare(from, to, true, GrantsMode::Ignore).await;
    let drop_score = rc_position(
        &script,
        "drop function if exists s1.score (x integer) cascade;",
    );
    let own = rc_position(
        &script,
        "CREATE TRIGGER guard BEFORE INSERT ON s1.parted_1 FOR EACH ROW",
    );
    assert!(drop_score < own, "{script}");
    assert!(
        script.contains("CREATE TRIGGER guard BEFORE INSERT ON s1.parted FOR EACH STATEMENT"),
        "{script}"
    );
}

// ---- pre-cascade: what reads a view the routine phase's CASCADE takes ------

/// `s1.norm(t text)`, a plain SQL function returning `return_type`.
fn pc_norm(return_type: &str) -> Routine {
    let mut routine = Routine::new(
        "s1".to_string(),
        Oid(710),
        "norm".to_string(),
        "sql".to_string(),
        "function".to_string(),
        return_type.to_string(),
        "t text".to_string(),
        None,
        None,
        format!(" select lower(t)::{return_type} "),
    );
    routine.owner = "postgres".to_string();
    routine.hash();
    routine
}

/// `s1.<name>()` with a SQL-standard body reading `relation`.
fn pc_body_reader(name: &str, relation: &str) -> Routine {
    let mut routine = Routine::new(
        "s1".to_string(),
        Oid(711),
        name.to_string(),
        "sql".to_string(),
        "function".to_string(),
        "bigint".to_string(),
        String::new(),
        None,
        None,
        String::new(),
    );
    routine.owner = "postgres".to_string();
    routine.source_code =
        format!("BEGIN ATOMIC\n SELECT count(*) AS count\n    FROM {relation};\nEND");
    routine.is_sql_body = true;
    routine.sql_body_dependencies = SqlBodyDependencies {
        relations: vec![relation.to_string()],
        ..SqlBodyDependencies::default()
    };
    routine.hash();
    routine
}

/// `s1.<name>`, calling no routine as a current dump records it
/// (`View::routine_dependencies`); see `pc_calling`.
fn pc_view(name: &str, definition: &str, reads: &[&str], materialized: bool) -> View {
    let mut view = View::new(
        name.to_string(),
        definition.to_string(),
        "s1".to_string(),
        reads.iter().map(|r| r.to_string()).collect(),
    );
    view.owner = "postgres".to_string();
    view.is_materialized = materialized;
    view.routine_dependencies = Some(Vec::new());
    view.hash();
    view
}

/// `view`, recorded as calling `routines` (`Routine::dependency_key` form).
fn pc_calling(mut view: View, routines: &[&str]) -> View {
    view.routine_dependencies = Some(routines.iter().map(|r| r.to_string()).collect());
    view
}

/// `s1.v_items`, which calls `s1.norm`.
fn pc_items_view() -> View {
    pc_calling(
        pc_view(
            "v_items",
            " SELECT items.id,\n    s1.norm(items.code) AS n\n   FROM s1.items;",
            &["s1.items"],
            false,
        ),
        &["s1.norm(t text)"],
    )
}

fn pc_items_table() -> Table {
    rc_table(
        "items",
        vec![
            int_column("s1", "items", "id", 1),
            int_column("s1", "items", "code", 2),
        ],
        vec![],
    )
}

/// Both sides of the reviewer's scenario: `s1.norm` changes its return type,
/// `s1.v_items` calls it, and — with `readers` — `s1.v_items2` and the
/// materialized view `s1.mv_items` (with an index and extended statistics)
/// read `s1.v_items`, `s1.cnt()` reads `s1.v_items2` in a SQL-standard body,
/// and a rule on `s1.v_items2` reads `s1.v_items`.
fn pc_dumps(readers: bool) -> (Dump, Dump) {
    let mut dumps = (
        Dump::new(DumpConfig::default()),
        Dump::new(DumpConfig::default()),
    );
    for (dump, return_type) in [(&mut dumps.0, "text"), (&mut dumps.1, "character varying")] {
        dump.tables.push(pc_items_table());
        dump.routines.push(pc_norm(return_type));
        dump.views.push(pc_items_view());
        if !readers {
            continue;
        }
        dump.views.push(pc_view(
            "v_items2",
            " SELECT v_items.id,\n    v_items.n\n   FROM s1.v_items;",
            &["s1.v_items"],
            false,
        ));
        let mut mv = pc_view(
            "mv_items",
            " SELECT v_items.id,\n    v_items.n\n   FROM s1.v_items;",
            &["s1.v_items"],
            true,
        );
        mv.indexes = vec![TableIndex {
            schema: "s1".to_string(),
            table: "mv_items".to_string(),
            name: "mv_items_id".to_string(),
            catalog: None,
            indexdef: "CREATE INDEX mv_items_id ON s1.mv_items USING btree (id)".to_string(),
            is_partition_index: false,
            comment: None,
        }];
        dump.views.push(mv);
        dump.statistics
            .push(rc_statistic("mv_items_st", "mv_items", "id, n"));
        dump.routines.push(pc_body_reader("cnt", "s1.v_items2"));
        let mut rule = Rule::new(
            "s1".to_string(),
            "v_items2".to_string(),
            "v_items2_del".to_string(),
            "CREATE RULE v_items2_del AS\n    ON DELETE TO s1.v_items2 DO INSTEAD  SELECT v_items.id\n   FROM s1.v_items\n  WHERE (v_items.id = old.id);".to_string(),
            None,
        );
        rule.depends_on = Some(RuleDependencies {
            relations: vec!["s1.v_items".to_string(), "s1.v_items2".to_string()],
            ..RuleDependencies::default()
        });
        dump.rules.push(rule);
    }
    dumps
}

fn pc_section<'a>(script: &'a str, start: &str, end: &str) -> &'a str {
    let from = rc_position(script, start);
    let to = from + rc_position(&script[from..], end);
    &script[from..to]
}

#[tokio::test]
async fn pre_cascade_view_with_readers_is_dropped_ahead_with_them() {
    let (from, to) = pc_dumps(true);
    let script = rc_compare(from, to, true, GrantsMode::Ignore).await;

    let drops = pc_section(
        &script,
        "/* ---> Views DROP: Start section",
        "/* ---> Views DROP: End section",
    );
    let drop_cnt = rc_position(drops, "drop function if exists s1.cnt () cascade;");
    let drop_v2 = rc_position(drops, "drop view if exists s1.v_items2;");
    let drop_mv = rc_position(drops, "drop materialized view if exists s1.mv_items;");
    let drop_v = rc_position(drops, "drop view if exists s1.v_items;");
    assert!(drop_cnt < drop_v2, "{drops}");
    assert!(drop_v2 < drop_v && drop_mv < drop_v, "{drops}");
    // The rule reading s1.v_items goes first, before any view.
    assert!(
        rc_position(
            &script,
            "drop rule if exists \"v_items2_del\" on s1.\"v_items2\" cascade;"
        ) < rc_position(&script, "/* ---> Views DROP: Start section"),
        "{script}"
    );

    // Re-created after the routine, each after what it reads.
    let create_norm = rc_position(&script, "create or replace function s1.norm(t text)");
    let create_v = rc_position(&script, "CREATE OR REPLACE VIEW s1.v_items as");
    let create_v2 = rc_position(&script, "CREATE OR REPLACE VIEW s1.v_items2 as");
    let create_mv = rc_position(&script, "create materialized view s1.mv_items as");
    let create_cnt = rc_position(&script, "create or replace function s1.cnt()");
    assert!(
        rc_position(&script, "drop function if exists s1.norm (t text) cascade;") < create_norm
    );
    assert!(create_norm < create_v, "{script}");
    assert!(create_v < create_v2 && create_v < create_mv, "{script}");
    assert!(create_v2 < create_cnt, "{script}");
    assert!(create_mv < rc_position(&script, "CREATE INDEX mv_items_id ON s1.mv_items"));
    assert!(create_mv < rc_position(&script, "create statistics s1.mv_items_st"));
    assert!(
        create_v2 < rc_position(&script, "CREATE OR REPLACE RULE v_items2_del AS"),
        "{script}"
    );
    // Once each: Phase 7 leaves the view to the routine phase.
    assert_eq!(
        script
            .matches("CREATE OR REPLACE VIEW s1.v_items as")
            .count(),
        1,
        "{script}"
    );
    assert!(
        !script.contains("Recreate dependents dropped by CASCADE"),
        "{script}"
    );
}

#[tokio::test]
async fn pre_cascade_view_nothing_reads_stays_with_phase_7() {
    let (from, to) = pc_dumps(false);
    let script = rc_compare(from, to, true, GrantsMode::Ignore).await;
    assert!(
        !script.contains("drop view if exists s1.v_items;"),
        "{script}"
    );
    let phase7 = rc_position(&script, "Recreate dependents dropped by CASCADE: Start");
    let create_v = rc_position(&script, "CREATE OR REPLACE VIEW s1.v_items as");
    assert!(phase7 < create_v, "{script}");
}

#[tokio::test]
async fn pre_cascade_a_new_reader_is_created_after_the_view() {
    let (from, mut to) = pc_dumps(false);
    to.views.push(pc_view(
        "v_new",
        " SELECT v_items.id\n   FROM s1.v_items;",
        &["s1.v_items"],
        false,
    ));
    let script = rc_compare(from, to, true, GrantsMode::Ignore).await;
    assert!(
        script.contains("drop view if exists s1.v_items;"),
        "{script}"
    );
    let create_v = rc_position(&script, "CREATE OR REPLACE VIEW s1.v_items as");
    let create_new = rc_position(&script, "CREATE OR REPLACE VIEW s1.v_new as");
    assert!(
        rc_position(&script, "create or replace function s1.norm(t text)") < create_v,
        "{script}"
    );
    assert!(create_v < create_new, "{script}");
}

#[tokio::test]
async fn pre_cascade_a_sql_body_reader_alone_is_enough() {
    let (mut from, mut to) = pc_dumps(false);
    from.routines.push(pc_body_reader("cnt", "s1.v_items"));
    to.routines.push(pc_body_reader("cnt", "s1.v_items"));
    let script = rc_compare(from, to, true, GrantsMode::Ignore).await;
    let drop_cnt = rc_position(&script, "drop function if exists s1.cnt () cascade;");
    let drop_v = rc_position(&script, "drop view if exists s1.v_items;");
    assert!(drop_cnt < drop_v, "{script}");
    assert!(
        rc_position(&script, "CREATE OR REPLACE VIEW s1.v_items as")
            < rc_position(&script, "create or replace function s1.cnt()"),
        "{script}"
    );
}

#[test]
fn pre_cascade_which_views_go_ahead() {
    let changes_of = |comparer: &Comparer| comparer.relation_changes();

    // Readers only FROM has are dropped anyway and do not count.
    let (mut from, to) = pc_dumps(false);
    from.views.push(pc_view(
        "v_old",
        " SELECT v_items.id\n   FROM s1.v_items;",
        &["s1.v_items"],
        false,
    ));
    let comparer = Comparer::new(from, to, true, false, true, GrantsMode::Ignore);
    assert!(
        comparer
            .views_cascaded_with_readers(&changes_of(&comparer))
            .is_empty()
    );

    // Without use_drop the routine drop is commented out.
    let (from, to) = pc_dumps(true);
    let comparer = Comparer::new(from, to, false, false, true, GrantsMode::Ignore);
    assert!(
        comparer
            .views_cascaded_with_readers(&changes_of(&comparer))
            .is_empty()
    );

    // An unchanged routine takes nothing.
    let (from, _) = pc_dumps(true);
    let (same, _) = pc_dumps(true);
    let comparer = Comparer::new(from, same, true, false, true, GrantsMode::Ignore);
    assert!(
        comparer
            .views_cascaded_with_readers(&changes_of(&comparer))
            .is_empty()
    );

    // The reviewer's case: only s1.v_items, the view calling the routine.
    let (from, to) = pc_dumps(true);
    let comparer = Comparer::new(from, to, true, false, true, GrantsMode::Ignore);
    let picked = comparer.views_cascaded_with_readers(&changes_of(&comparer));
    let names: Vec<&str> = picked
        .iter()
        .map(|&i| comparer.from.views[i].name.as_str())
        .collect();
    assert_eq!(names, ["v_items"]);

    // A routine the routine phase drops without CASCADE (a partition key
    // calls it) takes nothing along.
    let (mut from, mut to) = pc_dumps(true);
    for dump in [&mut from, &mut to] {
        let mut parted = rc_table(
            "parted",
            vec![int_column("s1", "parted", "code", 1)],
            vec![],
        );
        parted.partition_key = Some("RANGE (s1.norm(code))".to_string());
        parted.hash();
        dump.tables.push(parted);
    }
    let comparer = Comparer::new(from, to, true, false, true, GrantsMode::Ignore);
    assert!(
        comparer
            .views_cascaded_with_readers(&changes_of(&comparer))
            .is_empty()
    );
}

#[test]
fn pre_cascade_a_view_calling_a_routine_the_cascade_takes_goes_too() {
    // s1.pick's argument default calls s1.norm, so the routine phase's
    // CASCADE of s1.norm takes s1.pick, and the views calling it.
    let (mut from, mut to) = pc_dumps(false);
    for dump in [&mut from, &mut to] {
        dump.routines.push(rc_plpgsql(
            "pick",
            "x text",
            "text",
            Some("s1.norm('a'::text)"),
        ));
        dump.views.push(pc_calling(
            pc_view("v_pick", " SELECT s1.pick() AS p;", &[], false),
            &["s1.pick(x text)"],
        ));
        dump.views.push(pc_view(
            "v_pick2",
            " SELECT v_pick.p\n   FROM s1.v_pick;",
            &["s1.v_pick"],
            false,
        ));
    }
    let comparer = Comparer::new(from, to, true, false, true, GrantsMode::Ignore);
    let picked = comparer.views_cascaded_with_readers(&comparer.relation_changes());
    let names: Vec<&str> = picked
        .iter()
        .map(|&i| comparer.from.views[i].name.as_str())
        .collect();
    assert_eq!(names, ["v_pick"]);
}

/// The names of the FROM views `views_cascaded_with_readers` picks, sorted.
fn pc_picked(from: Dump, to: Dump) -> Vec<String> {
    let comparer = Comparer::new(from, to, true, false, true, GrantsMode::Ignore);
    let picked = comparer.views_cascaded_with_readers(&comparer.relation_changes());
    let mut names: Vec<String> = picked
        .iter()
        .map(|&i| comparer.from.views[i].name.clone())
        .collect();
    names.sort();
    names
}

/// `s1.<name>`, a materialized view reading `s1.<reads>`, with an index.
fn pc_reading_matview(name: &str, reads: &str) -> View {
    let mut mv = pc_view(
        name,
        &format!(" SELECT {reads}.id\n   FROM s1.{reads};"),
        &[&format!("s1.{reads}")],
        true,
    );
    mv.indexes = vec![TableIndex {
        schema: "s1".to_string(),
        table: name.to_string(),
        name: format!("{name}_id"),
        catalog: None,
        indexdef: format!("CREATE INDEX {name}_id ON s1.{name} USING btree (id)"),
        is_partition_index: false,
        comment: None,
    }];
    mv
}

#[tokio::test]
async fn pre_cascade_a_view_calling_an_unchanged_overload_stays_in_place() {
    // s1.v_int calls s1.norm(t integer), which does not change; its name
    // matches the s1.norm(t text) the routine phase drops, but the CASCADE
    // does not take it, so neither it nor the materialized view reading it
    // is dropped and rebuilt.
    let (mut from, mut to) = pc_dumps(false);
    for dump in [&mut from, &mut to] {
        let mut overload = pc_norm("text");
        overload.arguments = "t integer".to_string();
        overload.hash();
        dump.routines.push(overload);
        dump.views.push(pc_calling(
            pc_view(
                "v_int",
                " SELECT items.id,\n    s1.norm(items.id) AS n\n   FROM s1.items;",
                &["s1.items"],
                false,
            ),
            &["s1.norm(t integer)"],
        ));
        dump.views.push(pc_reading_matview("mv_int", "v_int"));
    }
    assert_eq!(pc_picked(from.clone(), to.clone()), Vec::<String>::new());
    let script = rc_compare(from, to, true, GrantsMode::Ignore).await;
    assert!(
        !script.contains("drop view if exists s1.v_int;"),
        "{script}"
    );
    assert!(!script.contains("s1.mv_int"), "{script}");
}

#[tokio::test]
async fn pre_cascade_a_view_calling_a_built_in_of_the_same_name_stays_in_place() {
    // s2.round(numeric) changes its return type; s1.v_round calls the
    // built-in round(), printed unqualified, which the dump does not record.
    let (mut from, mut to) = (
        Dump::new(DumpConfig::default()),
        Dump::new(DumpConfig::default()),
    );
    for (dump, return_type) in [(&mut from, "numeric"), (&mut to, "bigint")] {
        let mut round = pc_norm(return_type);
        round.schema = "s2".to_string();
        round.name = "round".to_string();
        round.arguments = "x numeric".to_string();
        round.hash();
        dump.routines.push(round);
        dump.tables.push(pc_items_table());
        dump.views.push(pc_view(
            "v_round",
            " SELECT items.id,\n    round((items.code)::numeric) AS r\n   FROM s1.items;",
            &["s1.items"],
            false,
        ));
        dump.views.push(pc_reading_matview("mv_round", "v_round"));
    }
    assert_eq!(pc_picked(from.clone(), to.clone()), Vec::<String>::new());
    let script = rc_compare(from, to, true, GrantsMode::Ignore).await;
    assert!(!script.contains("s1.mv_round"), "{script}");
    assert!(
        !script.contains("drop view if exists s1.v_round;"),
        "{script}"
    );
}

#[tokio::test]
async fn pre_cascade_a_dump_without_routine_dependencies_keeps_phase_7() {
    // pgc 1.0.x recorded no routine dependencies: the view is left to
    // Phase 7, as that release did.
    let (mut from, mut to) = pc_dumps(true);
    for dump in [&mut from, &mut to] {
        for view in &mut dump.views {
            view.routine_dependencies = None;
        }
    }
    assert_eq!(pc_picked(from.clone(), to.clone()), Vec::<String>::new());
    let script = rc_compare(from, to, true, GrantsMode::Ignore).await;
    assert!(
        !script.contains("drop view if exists s1.v_items;"),
        "{script}"
    );
    let phase7 = rc_position(&script, "Recreate dependents dropped by CASCADE: Start");
    assert!(
        phase7 < rc_position(&script, "CREATE OR REPLACE VIEW s1.v_items as"),
        "{script}"
    );
}

#[tokio::test]
async fn pre_cascade_a_view_its_drop_cannot_take_stays_with_phase_7() {
    // `drop_views` drops without CASCADE, which fails while something pgc
    // neither drops nor re-creates first depends on the view, or on a view
    // reading it: the routine's CASCADE is left to take them, as before.
    let setof =
        |name: &str, relation: &str| rc_plpgsql(name, "", &format!("SETOF {relation}"), None);
    type AddBlocker = Box<dyn Fn(&mut Dump)>;
    let blockers: Vec<(&str, AddBlocker)> = vec![
        (
            "a routine returning its rows",
            Box::new(move |dump: &mut Dump| dump.routines.push(setof("all_v", "s1.v_items"))),
        ),
        (
            "a routine returning the rows of a view reading it",
            Box::new(move |dump: &mut Dump| dump.routines.push(setof("all_v2", "s1.v_items2"))),
        ),
        (
            "a routine taking its row type",
            Box::new(|dump: &mut Dump| {
                dump.routines
                    .push(rc_plpgsql("take", "r s1.v_items", "integer", None))
            }),
        ),
        (
            "a policy reading it",
            Box::new(|dump: &mut Dump| {
                let table = dump.tables.iter_mut().find(|t| t.name == "items").unwrap();
                table.policies.push(TablePolicy {
                    schema: "s1".to_string(),
                    table: "items".to_string(),
                    name: "items_seen".to_string(),
                    command: "select".to_string(),
                    permissive: true,
                    roles: Vec::new(),
                    using_clause: Some(
                        "(EXISTS ( SELECT 1\n   FROM s1.v_items v\n  WHERE (v.id = items.id)))"
                            .to_string(),
                    ),
                    check_clause: None,
                });
                table.hash();
            }),
        ),
        (
            "a column of its row type",
            Box::new(|dump: &mut Dump| {
                let mut column = int_column("s1", "snapshots", "row", 1);
                column.data_type = "USER-DEFINED".to_string();
                column.udt_schema = Some("s1".to_string());
                column.udt_name = Some("v_items".to_string());
                dump.tables
                    .push(rc_table("snapshots", vec![column], vec![]));
            }),
        ),
    ];
    for (what, add) in &blockers {
        let (mut from, mut to) = pc_dumps(true);
        add(&mut from);
        add(&mut to);
        assert_eq!(
            pc_picked(from.clone(), to.clone()),
            Vec::<String>::new(),
            "{what}"
        );
        let script = rc_compare(from, to, true, GrantsMode::Ignore).await;
        assert!(
            !script.contains("drop view if exists s1.v_items;"),
            "{what}: {script}"
        );
        assert!(
            !script.contains("drop view if exists s1.v_items2;"),
            "{what}: {script}"
        );
    }

    // A SQL-standard routine reading the view goes first, so its result type
    // does not stand in the way.
    let (mut from, mut to) = pc_dumps(true);
    for dump in [&mut from, &mut to] {
        let mut reader = pc_body_reader("rows_v", "s1.v_items");
        reader.return_type = "SETOF s1.v_items".to_string();
        reader.hash();
        dump.routines.push(reader);
    }
    assert_eq!(pc_picked(from, to), ["v_items"]);
}

#[test]
fn pre_cascade_view_drop_blockers_name_what_stands_in_the_way() {
    let (mut from, _) = pc_dumps(false);
    from.routines
        .push(rc_plpgsql("all_v", "", "SETOF s1.v_items", None));
    from.routines.push(rc_plpgsql(
        "unrelated",
        "v_items_id integer",
        "integer",
        None,
    ));
    let view = from.views[0].clone();
    let comparer = Comparer::new(
        from,
        Dump::new(DumpConfig::default()),
        true,
        false,
        true,
        GrantsMode::Ignore,
    );
    assert_eq!(comparer.view_drop_blockers(&view), ["function s1.all_v()"]);
}

#[test]
fn pre_cascade_text_mentions_relation() {
    let mentions = Comparer::text_mentions_relation;
    assert!(mentions("SETOF s1.v_items", "s1", "v_items"));
    assert!(mentions("SETOF v_items", "s1", "v_items"));
    assert!(mentions("r v_items[]", "s1", "v_items"));
    assert!(mentions("SETOF \"S\".\"Mixed\"", "\"S\"", "\"Mixed\""));
    assert!(mentions("SETOF s.\"Mixed\"", "s", "\"Mixed\""));
    assert!(!mentions("SETOF other.v_items", "s1", "v_items"));
    assert!(!mentions("SETOF s1.v_items2", "s1", "v_items"));
    assert!(!mentions("v_items_id integer", "s1", "v_items"));
    assert!(!mentions("integer", "s1", "v_items"));
}

// ==== r3-gencol tests ====
//
// A routine's `DROP ... CASCADE` takes a stored generated column calling it,
// and with it whatever depends on the column: the views reading it (and the
// views over those), SQL-standard routines reading it, its privileges, its
// `UPDATE OF` / `WHEN` triggers, the publication rows using it and, on a
// partition, the partition's own objects on its copy. Phase 7 re-adds the
// column; everything else has to come back too, after it.

use crate::dump::publication::{Publication, PublicationTableSpec};

/// `s1.<name>` with columns `id` and `a <a_type>`, the generated column
/// `g <g_type> GENERATED ALWAYS AS (s1.f(a)) STORED`, and the given triggers.
fn r3g_table(name: &str, a_type: &str, g_type: &str, triggers: Vec<TableTrigger>) -> Table {
    let mut a = int_column("s1", name, "a", 2);
    a.data_type = a_type.to_string();
    let mut g = int_column("s1", name, "g", 3);
    g.data_type = g_type.to_string();
    g.is_generated = "ALWAYS".to_string();
    g.generation_expression = Some("s1.f(a)".to_string());
    g.generation_type = Some("s".to_string());
    g.acl = vec![
        "postgres=r/postgres".to_string(),
        "reader=r/postgres".to_string(),
    ];
    rc_table(name, vec![int_column("s1", name, "id", 1), a, g], triggers)
}

fn r3g_trigger(name: &str, table: &str) -> TableTrigger {
    TableTrigger {
        oid: Oid(1),
        name: name.to_string(),
        definition: format!(
            "CREATE TRIGGER {name} AFTER UPDATE OF g ON s1.{table} FOR EACH ROW EXECUTE FUNCTION s1.trg()"
        ),
        enabled: "O".to_string(),
        comment: None,
    }
}

/// `s1.<name>` reading `columns` of `relation`.
fn r3g_view(name: &str, relation: &str, columns: &[&str]) -> View {
    let mut view = View::new(
        name.to_string(),
        format!(" SELECT {} FROM {relation};", columns.join(", ")),
        "s1".to_string(),
        vec![relation.to_string()],
    );
    view.column_relation = Some(columns.iter().map(|c| format!("{relation}.{c}")).collect());
    view.owner = "postgres".to_string();
    view.hash();
    view
}

fn r3g_publication(member: &str) -> Publication {
    let mut publication = Publication {
        name: "p".into(),
        owner: "postgres".into(),
        publish: "insert,update,delete,truncate".into(),
        tables: vec![member.to_string()],
        ..Default::default()
    };
    publication.table_specs.insert(
        member.to_string(),
        PublicationTableSpec {
            columns: vec![],
            row_filter: Some("(g > 0)".to_string()),
        },
    );
    publication.hash();
    publication
}

/// FROM `s1.f(x integer)` returns `integer`, TO `bigint`, as does `s1.t.g`,
/// which calls it: `s1.tv` reads `g` and `s1.tvv` reads `s1.tv`,
/// `s1.g_total` (SQL-standard) reads `g`, the trigger `t_g` fires on
/// `UPDATE OF g`, `reader` may select `g`, and publication `p` filters on
/// it.
fn r3g_dumps(server: i32) -> (Dump, Dump) {
    let mut dumps = (
        Dump::new(DumpConfig::default()),
        Dump::new(DumpConfig::default()),
    );
    for (dump, g_type) in [(&mut dumps.0, "integer"), (&mut dumps.1, "bigint")] {
        dump.server_version_num = Some(server);
        dump.routines
            .push(rc_plpgsql("f", "x integer", g_type, None));
        let mut total = rc_codes_reader("g_total", "", "numeric");
        total.source_code = "BEGIN ATOMIC\n SELECT sum(t.g) AS sum FROM s1.t;\nEND".to_string();
        total.sql_body_dependencies = SqlBodyDependencies {
            relations: vec!["s1.t".to_string()],
            columns: vec!["s1.t.g".to_string()],
            ..SqlBodyDependencies::default()
        };
        total.hash();
        dump.routines.push(total);
        dump.tables.push(r3g_table(
            "t",
            "integer",
            g_type,
            vec![r3g_trigger("t_g", "t")],
        ));
        dump.views.push(r3g_view("tv", "s1.t", &["id", "g"]));
        dump.views.push(r3g_view("tvv", "s1.tv", &["id"]));
        dump.publications.push(r3g_publication("s1.t"));
        dump.column_dependents = vec![ColumnDependent {
            schema: "s1".to_string(),
            table: "t".to_string(),
            column: "g".to_string(),
            kind: ColumnDependentKind::Trigger,
            dep_schema: "s1".to_string(),
            dep_table: "t".to_string(),
            dep_name: "t_g".to_string(),
        }];
    }
    dumps
}

#[tokio::test]
async fn r3g_cascade_restores_what_went_with_a_generated_column_after_re_adding_it() {
    for server in [140000, 180000] {
        let (from, to) = r3g_dumps(server);
        let script = rc_compare(from, to, true, GrantsMode::Full).await;

        // What reads the column goes ahead of everything.
        let views_end = rc_position(&script, "Views DROP: End section");
        for dropped in [
            "drop view if exists s1.tvv;",
            "drop view if exists s1.tv;",
            "drop function if exists s1.g_total () cascade;",
        ] {
            assert!(
                rc_position(&script, dropped) < views_end,
                "{dropped}:\n{script}"
            );
        }
        // The table phase leaves the type to the re-add: redundant on every
        // server, refused by PostgreSQL 14. Nor does it release the
        // publication row for a retype it does not run: the CASCADE takes
        // the row, and the restore below puts it back.
        assert!(!script.contains("alter column g type"), "{script}");
        assert!(
            !script.contains("ALTER PUBLICATION p DROP TABLE"),
            "{script}"
        );

        let create_f = rc_position(
            &script,
            "create or replace function s1.f(x integer) returns bigint",
        );
        let readd = rc_position(
            &script,
            "alter table s1.t add column if not exists g bigint generated always as (s1.f(a)) stored;",
        );
        let trigger = rc_position(
            &script,
            "drop trigger if exists t_g on s1.t;\n\nCREATE TRIGGER t_g AFTER UPDATE OF g ON s1.t",
        );
        let publication = rc_position(
            &script,
            "ALTER PUBLICATION p SET TABLE s1.t WHERE ((g > 0));",
        );
        let readers = rc_position(
            &script,
            "Views and routines reading generated columns re-added above: Start",
        );
        let total = rc_position(&script, "create or replace function s1.g_total()");
        let tv = rc_position(&script, "CREATE OR REPLACE VIEW s1.tv ");
        let tvv = rc_position(&script, "CREATE OR REPLACE VIEW s1.tvv ");
        assert!(create_f < readd, "{script}");
        assert!(
            readd < trigger && readd < publication && readd < readers,
            "{script}"
        );
        assert!(readers < total && readers < tv && tv < tvv, "{script}");
        // The re-added column starts without privileges.
        let grant = rc_position(&script, "GRANT SELECT (g) ON TABLE s1.t TO reader;");
        assert!(tvv < grant, "{script}");
        assert_eq!(script.matches("CREATE TRIGGER t_g").count(), 1, "{script}");
    }
}

#[tokio::test]
async fn r3g_cascade_re_add_is_left_as_it_was_without_use_drop() {
    // Nothing is dropped, so nothing is taken: the output keeps the
    // retype, and neither drops the readers nor restores anything.
    let (from, to) = r3g_dumps(180000);
    let script = rc_compare(from, to, false, GrantsMode::Ignore).await;
    assert!(
        script.contains("alter table s1.t alter column g type bigint;"),
        "{script}"
    );
    assert!(
        !script.contains("reading generated columns re-added"),
        "{script}"
    );
    // The release of the retyped column's member is put back once, as
    // before; Phase 7 adds nothing.
    assert_eq!(
        script
            .matches("ALTER PUBLICATION p SET TABLE s1.t WHERE ((g > 0));")
            .count(),
        1,
        "{script}"
    );
    assert!(!script.contains("CREATE TRIGGER t_g"), "{script}");
}

#[tokio::test]
async fn r3g_cascade_re_add_waits_for_a_retype_of_the_column_it_reads() {
    // TO also retypes `a`, which `g` reads: the table phase drops and
    // re-adds `g` around that retype, `drop_views` drops the view for both,
    // and the view comes back only after the routine phase re-adds `g`.
    let (mut from, mut to) = r3g_dumps(170000);
    for (dump, a_type, g_type) in [
        (&mut from, "integer", "integer"),
        (&mut to, "bigint", "bigint"),
    ] {
        dump.tables = vec![r3g_table("t", a_type, g_type, vec![])];
        dump.column_dependents.clear();
        dump.publications.clear();
        dump.routines.retain(|r| r.name == "f");
        dump.views.retain(|v| v.name == "tv");
    }
    let script = rc_compare(from, to, true, GrantsMode::Ignore).await;
    let drop_view = rc_position(&script, "drop view if exists s1.tv;");
    let drop_g = rc_position(&script, "alter table s1.t drop column g;");
    let retype = rc_position(&script, "alter table s1.t alter column a type bigint;");
    let add_g = rc_position(&script, "alter table s1.t add column g bigint");
    let drop_f = rc_position(&script, "drop function if exists s1.f (x integer) cascade;");
    let readd = rc_position(
        &script,
        "alter table s1.t add column if not exists g bigint",
    );
    let view = rc_position(&script, "CREATE OR REPLACE VIEW s1.tv ");
    assert!(
        drop_view < drop_g && drop_g < retype && retype < add_g && add_g < drop_f,
        "{script}"
    );
    assert!(drop_f < readd && readd < view, "{script}");
}

#[tokio::test]
async fn r3g_cascade_restores_a_partitions_own_objects_on_its_copy() {
    // Phase 7 re-adds the column on the partitioned parent; the partition's
    // own index, policy, trigger, privileges, view and publication row on its
    // copy went with the CASCADE too.
    let mut dumps = (
        Dump::new(DumpConfig::default()),
        Dump::new(DumpConfig::default()),
    );
    for (dump, g_type) in [(&mut dumps.0, "integer"), (&mut dumps.1, "bigint")] {
        dump.server_version_num = Some(180000);
        dump.routines
            .push(rc_plpgsql("f", "x integer", g_type, None));
        let mut parent = r3g_table("pt", "integer", g_type, vec![]);
        parent.partition_key = Some("LIST (id)".to_string());
        parent.hash();
        let mut partition = r3g_table("pt1", "integer", g_type, vec![r3g_trigger("pt1_g", "pt1")]);
        partition.partition_of = Some("s1.pt".to_string());
        partition.partition_bound = Some("FOR VALUES IN (1)".to_string());
        partition.indexes.push(TableIndex {
            schema: "s1".to_string(),
            table: "pt1".to_string(),
            name: "pt1_g".to_string(),
            catalog: None,
            indexdef: "CREATE INDEX pt1_g ON s1.pt1 USING btree (g)".to_string(),
            is_partition_index: false,
            comment: None,
        });
        partition.policies.push(TablePolicy {
            schema: "s1".to_string(),
            table: "pt1".to_string(),
            name: "pt1_pol".to_string(),
            command: "all".to_string(),
            permissive: true,
            roles: vec![],
            using_clause: Some("(g > 0)".to_string()),
            check_clause: None,
        });
        partition.has_rowsecurity = true;
        partition.hash();
        dump.tables.push(parent);
        dump.tables.push(partition);
        dump.views.push(r3g_view("pt1v", "s1.pt1", &["id", "g"]));
        dump.publications.push(r3g_publication("s1.pt1"));
        let edge = |kind, dep: &str| ColumnDependent {
            schema: "s1".to_string(),
            table: "pt1".to_string(),
            column: "g".to_string(),
            kind,
            dep_schema: "s1".to_string(),
            dep_table: "pt1".to_string(),
            dep_name: dep.to_string(),
        };
        dump.column_dependents = vec![
            edge(ColumnDependentKind::Index, "pt1_g"),
            edge(ColumnDependentKind::Policy, "pt1_pol"),
            edge(ColumnDependentKind::Trigger, "pt1_g"),
        ];
    }
    let script = rc_compare(dumps.0, dumps.1, true, GrantsMode::Full).await;
    let readd = rc_position(
        &script,
        "alter table s1.pt add column if not exists g bigint",
    );
    assert!(
        !script.contains("alter table s1.pt1 add column"),
        "the parent's ADD COLUMN re-creates the partition's copy:\n{script}"
    );
    for restored in [
        "CREATE INDEX IF NOT EXISTS pt1_g ON s1.pt1 USING btree (g);",
        "create policy pt1_pol on s1.pt1 for all to public using (g > 0);",
        "CREATE TRIGGER pt1_g AFTER UPDATE OF g ON s1.pt1",
        "ALTER PUBLICATION p SET TABLE s1.pt1 WHERE ((g > 0));",
        "CREATE OR REPLACE VIEW s1.pt1v ",
        "GRANT SELECT (g) ON TABLE s1.pt1 TO reader;",
    ] {
        assert!(
            readd < rc_position(&script, restored),
            "{restored}:\n{script}"
        );
    }
}

#[test]
fn r3g_cascade_readded_columns_follow_phase_7_and_the_partitions() {
    let (mut from, mut to) = r3g_dumps(180000);
    for (dump, g_type) in [(&mut from, "integer"), (&mut to, "bigint")] {
        let mut parent = r3g_table("pt", "integer", g_type, vec![]);
        parent.partition_key = Some("LIST (id)".to_string());
        parent.hash();
        let mut partition = r3g_table("pt1", "integer", g_type, vec![]);
        partition.partition_of = Some("s1.pt".to_string());
        partition.hash();
        dump.tables.push(parent);
        dump.tables.push(partition);
        // Calls nothing the migration drops.
        let mut plain = r3g_table("plain", "integer", "integer", vec![]);
        plain.columns[2].generation_expression = Some("(a * 2)".to_string());
        plain.hash();
        dump.tables.push(plain);
    }
    let comparer = Comparer::new(from, to, true, false, true, GrantsMode::Ignore);
    let readded = comparer.cascade_readded_columns();
    let found: Vec<(&str, &str, bool)> = readded
        .iter()
        .map(|c| (c.table.as_str(), c.column.as_str(), c.in_partition))
        .collect();
    assert_eq!(
        found,
        vec![("t", "g", false), ("pt", "g", false), ("pt1", "g", true)]
    );
    let keys = comparer.cascade_taken_column_keys(&readded);
    let expected: HashSet<String> = ["s1.t.g", "s1.pt.g", "s1.pt1.g"]
        .into_iter()
        .map(str::to_string)
        .collect();
    assert_eq!(keys, expected);

    // Nothing without use_drop.
    let (from, to) = r3g_dumps(180000);
    let comparer = Comparer::new(from, to, false, false, true, GrantsMode::Ignore);
    assert!(comparer.cascade_readded_columns().is_empty());
}

#[test]
fn r3g_readers_of_re_added_columns_wait_but_the_routines_they_call_do_not() {
    let (from, mut to) = r3g_dumps(180000);
    // Items: 0 = view reading g, 1 = view reading id only, 2 = view over 0,
    // 3 = SQL string-body routine naming s1.t and g, 4 = s1.f (called by g)
    // with an edge to 0.
    to.views = vec![
        r3g_view("tv", "s1.t", &["id", "g"]),
        r3g_view("tid", "s1.t", &["id"]),
        r3g_view("tvv", "s1.tv", &["id"]),
    ];
    let mut reader = rc_plpgsql("reads_g", "", "bigint", None);
    reader.lang = "sql".to_string();
    reader.source_code = " SELECT max(g) FROM s1.t ".to_string();
    to.routines = vec![reader, rc_plpgsql("f", "x integer", "bigint", None)];
    let mut comparer = Comparer::new(from, to, true, false, true, GrantsMode::Ignore);
    comparer.cascade_readded = comparer.cascade_readded_columns();
    let items = [(true, 0), (true, 1), (true, 2), (false, 0), (false, 1)];
    let mut depends_on: Vec<HashSet<usize>> = vec![HashSet::new(); items.len()];
    depends_on[2].insert(0);
    depends_on[4].insert(0);
    let waiting = comparer.items_reading_cascade_readded_columns(items.into_iter(), &depends_on);
    let expected: HashSet<usize> = [0, 2, 3].into_iter().collect();
    assert_eq!(waiting, expected);

    // Nothing waits when nothing is re-added.
    comparer.cascade_readded.clear();
    assert!(
        comparer
            .items_reading_cascade_readded_columns(items.into_iter(), &depends_on)
            .is_empty()
    );
}

// ---- r3-gencol review fixes ----

/// `s1.u (id, v integer DEFAULT <call>, w integer)` with `u_chk CHECK ((w <=
/// <call>))`: a column default and a CHECK calling a routine that reads a
/// generated column Phase 7 re-adds.
fn r3g_caller_table(call: &str) -> Table {
    let mut v = int_column("s1", "u", "v", 2);
    v.column_default = Some(call.to_string());
    let mut table = rc_table(
        "u",
        vec![
            int_column("s1", "u", "id", 1),
            v,
            int_column("s1", "u", "w", 3),
        ],
        vec![],
    );
    table
        .constraints
        .push(rc_check("u", "u_chk", &format!("CHECK ((w <= {call}))"), 0));
    table.hash();
    table
}

#[tokio::test]
async fn r3g_what_calls_a_routine_waiting_for_the_re_add_comes_back_after_it() {
    // `s1.g_total()` (SQL-standard) reads `s1.t.g`, which `s1.f`'s CASCADE
    // takes: `drop_views` drops it ahead, and the routine phase creates it
    // only once Phase 7 has re-added `g`. The default and the CHECK calling
    // it were restored by Phase 7 before that: "function does not exist".
    let (mut from, mut to) = r3g_dumps(180000);
    for dump in [&mut from, &mut to] {
        dump.tables.push(r3g_caller_table("s1.g_total()"));
    }
    let script = rc_compare(from, to, true, GrantsMode::Ignore).await;
    let readd = rc_position(
        &script,
        "alter table s1.t add column if not exists g bigint generated always as (s1.f(a)) stored;",
    );
    let create_total = rc_position(&script, "create or replace function s1.g_total()");
    let late = rc_position(
        &script,
        "Recreate dependents dropped by CASCADE that call the routines created above: Start",
    );
    let check = rc_position(&script, "alter table s1.u add constraint u_chk");
    let default = rc_position(
        &script,
        "alter table s1.u alter column v set default s1.g_total();",
    );
    assert!(readd < create_total, "{script}");
    assert!(
        create_total < late && late < check && late < default,
        "{script}"
    );
    assert_eq!(
        script.matches("add constraint u_chk").count(),
        1,
        "{script}"
    );
}

#[tokio::test]
async fn r3g_what_calls_a_recreated_string_body_reader_is_not_taken_again() {
    // `s1.r()` has a string body reading `s1.t.g` and its return type
    // changes, so it is dropped with CASCADE and re-created after the re-add.
    // Phase 7 used to restore the CHECK and the default calling it before
    // that drop, which took them again, silently.
    let (mut from, mut to) = r3g_dumps(180000);
    for (dump, return_type) in [(&mut from, "integer"), (&mut to, "bigint")] {
        let mut reader = rc_plpgsql("r", "", return_type, None);
        reader.lang = "sql".to_string();
        reader.source_code = " select coalesce(max(g), 0) from s1.t ".to_string();
        reader.hash();
        dump.routines.push(reader);
        dump.tables.push(r3g_caller_table("s1.r()"));
    }
    let script = rc_compare(from, to, true, GrantsMode::Ignore).await;
    let readd = rc_position(
        &script,
        "alter table s1.t add column if not exists g bigint",
    );
    let drop_r = rc_position(&script, "drop function if exists s1.r () cascade;");
    let create_r = rc_position(&script, "create or replace function s1.r() returns bigint");
    let check = rc_position(&script, "alter table s1.u add constraint u_chk");
    let default = rc_position(
        &script,
        "alter table s1.u alter column v set default s1.r();",
    );
    assert!(readd < drop_r && drop_r < create_r, "{script}");
    assert!(create_r < check && create_r < default, "{script}");
}

#[tokio::test]
async fn r3g_nothing_moves_after_the_readers_when_none_is_called() {
    // A caller of `s1.f` itself (not of a reader of `g`) stays in Phase 7,
    // and no late block appears.
    let (mut from, mut to) = r3g_dumps(180000);
    for dump in [&mut from, &mut to] {
        dump.tables.push(r3g_caller_table("s1.f(1)"));
    }
    let script = rc_compare(from, to, true, GrantsMode::Ignore).await;
    assert!(
        !script.contains("that call the routines created above"),
        "{script}"
    );
    let check = rc_position(&script, "alter table s1.u add constraint u_chk");
    let readers = rc_position(
        &script,
        "Views and routines reading generated columns re-added above: Start",
    );
    assert!(check < readers, "{script}");
}

#[tokio::test]
async fn r3g_to_only_dependents_of_a_re_added_column_come_back() {
    // TO adds an index, a CHECK and a policy on `g`: the table phase creates
    // them before `s1.f`'s CASCADE, which takes them with the column. The
    // TO dump's column edges bring them back with the re-add.
    let (mut from, mut to) = r3g_dumps(180000);
    let t = to.tables.iter_mut().find(|t| t.name == "t").unwrap();
    t.indexes.push(rc_index("t", "t_g", "g"));
    t.constraints
        .push(rc_check("t", "t_g_chk", "CHECK ((g > 0))", 0));
    t.policies.push(TablePolicy {
        schema: "s1".to_string(),
        table: "t".to_string(),
        name: "t_pol".to_string(),
        command: "all".to_string(),
        permissive: false,
        roles: vec![],
        using_clause: Some("(g > 0)".to_string()),
        check_clause: None,
    });
    t.has_rowsecurity = true;
    t.hash();
    let edge = |kind, name: &str| ColumnDependent {
        schema: "s1".to_string(),
        table: "t".to_string(),
        column: "g".to_string(),
        kind,
        dep_schema: "s1".to_string(),
        dep_table: "t".to_string(),
        dep_name: name.to_string(),
    };
    to.column_dependents.extend([
        edge(ColumnDependentKind::Index, "t_g"),
        edge(ColumnDependentKind::Constraint, "t_g_chk"),
        edge(ColumnDependentKind::Policy, "t_pol"),
    ]);
    from.column_dependents.clear();
    let script = rc_compare(from, to, true, GrantsMode::Ignore).await;
    let drop_f = rc_position(&script, "drop function if exists s1.f (x integer) cascade;");
    let readd = rc_position(
        &script,
        "alter table s1.t add column if not exists g bigint",
    );
    for restored in [
        "CREATE INDEX IF NOT EXISTS t_g ON s1.t USING btree (g);",
        "alter table s1.t add constraint t_g_chk check ((g > 0))",
        "create policy t_pol on s1.t as restrictive for all to public using (g > 0);",
    ] {
        let at = script
            .rfind(restored)
            .unwrap_or_else(|| panic!("{restored}:\n{script}"));
        assert!(drop_f < readd && readd < at, "{restored}:\n{script}");
    }
}

#[tokio::test]
async fn r3g_statistics_on_a_matview_follow_it_into_the_held_back_routines() {
    // `s1.tv` is materialized, with extended statistics, and reads `g`: it
    // is re-created after Phase 7's re-add. A new SQL-standard routine using
    // a new operator sends that block after the operators; its statistics
    // used to stay ahead of them, before the view existed.
    let (mut from, mut to) = r3g_dumps(180000);
    for dump in [&mut from, &mut to] {
        dump.views.retain(|v| v.name == "tv");
        let view = &mut dump.views[0];
        view.is_materialized = true;
        view.hash();
        dump.statistics.push(rc_statistic("tv_st", "tv", "id, g"));
    }
    let plus = rc_plpgsql("plus2", "a integer, b integer", "integer", None);
    to.routines.push(plus);
    let mut op = Operator {
        schema: "s1".to_string(),
        name: "###".to_string(),
        owner: "postgres".to_string(),
        left_type: Some("integer".to_string()),
        right_type: Some("integer".to_string()),
        result_type: "integer".to_string(),
        procedure: "s1.plus2".to_string(),
        commutator: None,
        negator: None,
        restrict: None,
        join: None,
        is_hashes: false,
        is_merges: false,
        comment: None,
        hash: None,
    };
    op.hash();
    let mut uses_op = rc_plpgsql("h", "", "integer", None);
    uses_op.lang = "sql".to_string();
    uses_op.is_sql_body = true;
    uses_op.source_code = "BEGIN ATOMIC\n SELECT (1 OPERATOR(s1.###) 2);\nEND".to_string();
    uses_op.sql_body_dependencies = SqlBodyDependencies {
        operators: vec![op.key()],
        ..SqlBodyDependencies::default()
    };
    uses_op.hash();
    to.routines.push(uses_op);
    to.operators.push(op);

    let script = rc_compare(from, to, true, GrantsMode::Ignore).await;
    let operator = rc_position(&script, "CREATE OPERATOR s1.###");
    let view = rc_position(&script, "create materialized view s1.tv");
    let stat = script
        .rfind("create statistics s1.tv_st")
        .unwrap_or_else(|| panic!("{script}"));
    assert!(operator < view && view < stat, "{script}");
    assert_eq!(
        script.matches("create statistics s1.tv_st").count(),
        1,
        "{script}"
    );
}

/// `s1.pg14 (id, v, g <g_type> …)`: `g` generated by `s1.f(v)` on the FROM
/// side, flagged as PostgreSQL 14 records it, and plain in TO.
fn r3g_pg14_plain_dumps(to_type: &str, to_keeps_f: bool) -> (Dump, Dump) {
    let mut from = Dump::new(DumpConfig::default());
    let mut to = Dump::new(DumpConfig::default());
    from.server_version_num = Some(140000);
    to.server_version_num = Some(140000);
    let mut g = int_column("s1", "pg14", "g", 3);
    g.is_generated = "ALWAYS".to_string();
    g.generation_expression = Some("s1.f(v)".to_string());
    g.generation_type = Some("s".to_string());
    g.generation_blocks_retype = true;
    from.tables.push(rc_table(
        "pg14",
        vec![
            int_column("s1", "pg14", "id", 1),
            int_column("s1", "pg14", "v", 2),
            g,
        ],
        vec![],
    ));
    let mut plain = int_column("s1", "pg14", "g", 3);
    plain.data_type = to_type.to_string();
    to.tables.push(rc_table(
        "pg14",
        vec![
            int_column("s1", "pg14", "id", 1),
            int_column("s1", "pg14", "v", 2),
            plain,
        ],
        vec![],
    ));
    from.routines
        .push(rc_plpgsql("f", "x integer", "integer", None));
    if to_keeps_f {
        to.routines
            .push(rc_plpgsql("f", "x integer", "integer", None));
    }
    (from, to)
}

#[tokio::test]
async fn r3g_pg14_a_plain_column_keeps_its_values_when_its_routine_is_dropped() {
    // PostgreSQL 14 keeps the column depending on `s1.f` after DROP
    // EXPRESSION, so the routine's CASCADE used to take the plain column
    // with its data. The column is rewritten first instead.
    let (from, to) = r3g_pg14_plain_dumps("integer", false);
    let script = rc_compare(from, to, true, GrantsMode::Ignore).await;
    let copy = rc_position(
        &script,
        "alter table s1.pg14 alter column \"pgc_rewrite_g\" type integer using (g);",
    );
    let rename = rc_position(
        &script,
        "alter table s1.pg14 rename column \"pgc_rewrite_g\" to g;",
    );
    let drop_f = rc_position(&script, "drop function if exists s1.f (x integer) cascade;");
    assert!(copy < rename && rename < drop_f, "{script}");
    assert!(!script.contains("drop expression"), "{script}");

    // Without use_drop nothing is dropped: DROP EXPRESSION, as before.
    let (from, to) = r3g_pg14_plain_dumps("integer", false);
    let script = rc_compare(from, to, false, GrantsMode::Ignore).await;
    assert!(
        script.contains("alter table s1.pg14 alter column g drop expression;"),
        "{script}"
    );
    assert!(!script.contains("pgc_rewrite_g"), "{script}");

    // The routine stays and the type does not change: DROP EXPRESSION.
    let (from, to) = r3g_pg14_plain_dumps("integer", true);
    let script = rc_compare(from, to, true, GrantsMode::Ignore).await;
    assert!(
        script.contains("alter table s1.pg14 alter column g drop expression;"),
        "{script}"
    );
    assert!(!script.contains("pgc_rewrite_g"), "{script}");
}

#[tokio::test]
async fn r3g_pg14_a_plain_column_with_a_new_type_is_rewritten() {
    let (from, to) = r3g_pg14_plain_dumps("bigint", true);
    let script = rc_compare(from, to, true, GrantsMode::Ignore).await;
    assert!(
        script.contains(
            "alter table s1.pg14 add column \"pgc_rewrite_g\" bigint;\n\nalter table s1.pg14 alter column \"pgc_rewrite_g\" type bigint using (g);\n\nalter table s1.pg14 drop column g;\n\nalter table s1.pg14 rename column \"pgc_rewrite_g\" to g;"
        ),
        "{script}"
    );
    assert!(!script.contains("alter column g type"), "{script}");
    assert!(!script.contains("drop expression"), "{script}");
}
