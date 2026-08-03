//! Issue #179 — `DROP FUNCTION ... CASCADE` silently drops dependent
//! objects (functional indexes, CHECK constraints, generated columns,
//! column DEFAULT expressions, RLS policies). Phase 7 of
//! `compare_routines_and_views` re-emits them; these tests cover it.

use super::helpers::*;
use crate::comparer::core::*;
use crate::config::dump_config::DumpConfig;
use crate::config::grants_mode::GrantsMode;
use crate::dump::routine::Routine;
use crate::dump::table::Table;
use crate::dump::table_constraint::TableConstraint;
use crate::dump::table_index::TableIndex;
use crate::dump::table_policy::TablePolicy;
use sqlx::postgres::types::Oid;

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
