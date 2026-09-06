//! Tests for view handling: drop/create ordering, regular ↔ materialized
//! kind transitions, the `CREATE OR REPLACE` compatibility rules from
//! issue #227, and the byte-identical recreation of issue #189.

use super::helpers::*;
use crate::comparer::core::*;
use crate::config::dump_config::DumpConfig;
use crate::config::grants_mode::GrantsMode;
use crate::dump::view::View;

#[tokio::test]
async fn create_views_emits_owner_change_for_existing_view() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_view = View::new(
        "active_users".to_string(),
        "select id from public.users".to_string(),
        "public".to_string(),
        vec!["public.users".to_string()],
    );
    from_view.owner = "old_owner".to_string();
    from_view.hash();

    let mut to_view = View::new(
        "active_users".to_string(),
        "select id from public.users".to_string(),
        "public".to_string(),
        vec!["public.users".to_string()],
    );
    to_view.owner = "new_owner".to_string();
    to_view.hash();

    from_dump.views.push(from_view);
    to_dump.views.push(to_view);

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.create_views().await.unwrap();
    let script = comparer.get_script();

    assert!(script.contains("alter view public.active_users owner to new_owner;"));
}

#[tokio::test]
async fn kind_transition_regular_to_materialized_use_drop_true() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_view = View::new(
        "my_view".to_string(),
        "SELECT 1".to_string(),
        "public".to_string(),
        vec![],
    );
    from_view.is_materialized = false;
    from_view.hash();
    from_dump.views.push(from_view);

    let mut to_view = View::new(
        "my_view".to_string(),
        "SELECT 1".to_string(),
        "public".to_string(),
        vec![],
    );
    to_view.is_materialized = true;
    to_view.hash();
    to_dump.views.push(to_view);

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.drop_views().await.unwrap();
    comparer.create_views().await.unwrap();
    let script = comparer.get_script();

    // DROP VIEW (regular) should be active
    let has_active_drop = script
        .lines()
        .any(|l| !l.starts_with("--") && l.to_lowercase().contains("drop view"));
    assert!(
        has_active_drop,
        "Regular→mat: DROP VIEW should be active when use_drop=true, script:\n{}",
        script
    );
    // CREATE MATERIALIZED VIEW should be active
    let has_active_create = script
        .lines()
        .any(|l| !l.starts_with("--") && l.to_lowercase().contains("create materialized view"));
    assert!(
        has_active_create,
        "Regular→mat: CREATE MATERIALIZED VIEW should be active when use_drop=true, script:\n{}",
        script
    );
}

#[tokio::test]
async fn kind_transition_regular_to_materialized_use_drop_false() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_view = View::new(
        "my_view".to_string(),
        "SELECT 1".to_string(),
        "public".to_string(),
        vec![],
    );
    from_view.is_materialized = false;
    from_view.hash();
    from_dump.views.push(from_view);

    let mut to_view = View::new(
        "my_view".to_string(),
        "SELECT 1".to_string(),
        "public".to_string(),
        vec![],
    );
    to_view.is_materialized = true;
    to_view.hash();
    to_dump.views.push(to_view);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Ignore);
    comparer.drop_views().await.unwrap();
    comparer.create_views().await.unwrap();
    let script = comparer.get_script();

    // DROP should be commented
    let has_active_drop = script
        .lines()
        .any(|l| !l.starts_with("--") && l.to_lowercase().contains("drop view"));
    assert!(
        !has_active_drop,
        "Regular→mat: DROP VIEW should be commented when use_drop=false, script:\n{}",
        script
    );
    // CREATE should be commented (manual intervention needed)
    let has_active_create = script
        .lines()
        .any(|l| !l.starts_with("--") && l.to_lowercase().contains("create materialized view"));
    assert!(
        !has_active_create,
        "Regular→mat: CREATE MATERIALIZED VIEW should be commented when use_drop=false, script:\n{}",
        script
    );
    assert!(
        script.contains("manual intervention needed"),
        "Should contain manual intervention warning, script:\n{}",
        script
    );
}

#[tokio::test]
async fn kind_transition_materialized_to_regular_use_drop_true() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_view = View::new(
        "my_view".to_string(),
        "SELECT 1".to_string(),
        "public".to_string(),
        vec![],
    );
    from_view.is_materialized = true;
    from_view.hash();
    from_dump.views.push(from_view);

    let mut to_view = View::new(
        "my_view".to_string(),
        "SELECT 1".to_string(),
        "public".to_string(),
        vec![],
    );
    to_view.is_materialized = false;
    to_view.hash();
    to_dump.views.push(to_view);

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.drop_views().await.unwrap();
    comparer.create_views().await.unwrap();
    let script = comparer.get_script();

    // DROP MATERIALIZED VIEW should be active
    let has_active_drop = script
        .lines()
        .any(|l| !l.starts_with("--") && l.to_lowercase().contains("drop materialized view"));
    assert!(
        has_active_drop,
        "Mat→regular: DROP MATERIALIZED VIEW should be active when use_drop=true, script:\n{}",
        script
    );
    // CREATE OR REPLACE VIEW should be active
    let has_active_create = script
        .lines()
        .any(|l| !l.starts_with("--") && l.to_lowercase().contains("create or replace view"));
    assert!(
        has_active_create,
        "Mat→regular: CREATE OR REPLACE VIEW should be active when use_drop=true, script:\n{}",
        script
    );
}

#[tokio::test]
async fn kind_transition_materialized_to_regular_use_drop_false() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_view = View::new(
        "my_view".to_string(),
        "SELECT 1".to_string(),
        "public".to_string(),
        vec![],
    );
    from_view.is_materialized = true;
    from_view.hash();
    from_dump.views.push(from_view);

    let mut to_view = View::new(
        "my_view".to_string(),
        "SELECT 1".to_string(),
        "public".to_string(),
        vec![],
    );
    to_view.is_materialized = false;
    to_view.hash();
    to_dump.views.push(to_view);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Ignore);
    comparer.drop_views().await.unwrap();
    comparer.create_views().await.unwrap();
    let script = comparer.get_script();

    // DROP should be commented
    let has_active_drop = script
        .lines()
        .any(|l| !l.starts_with("--") && l.to_lowercase().contains("drop materialized view"));
    assert!(
        !has_active_drop,
        "Mat→regular: DROP MATERIALIZED VIEW should be commented when use_drop=false, script:\n{}",
        script
    );
    // CREATE OR REPLACE VIEW should also be commented (kind transition)
    let has_active_create = script
        .lines()
        .any(|l| !l.starts_with("--") && l.to_lowercase().contains("create or replace view"));
    assert!(
        !has_active_create,
        "Mat→regular: CREATE OR REPLACE VIEW should be commented when use_drop=false, script:\n{}",
        script
    );
    assert!(
        script.contains("manual intervention needed"),
        "Should contain manual intervention warning, script:\n{}",
        script
    );
}

/// FROM-only views (present in FROM, absent in TO) must still appear in the output
/// when use_drop=false — as a commented-out DROP statement so the user is aware the
/// view should be removed.  Previously `should_drop` gated `is_from_only` behind
/// `self.use_drop`, which suppressed the DROP entirely.
#[tokio::test]
async fn from_only_view_commented_drop_when_use_drop_false() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());

    let mut view = View::new(
        "obsolete_view".to_string(),
        "SELECT 1".to_string(),
        "public".to_string(),
        vec![],
    );
    view.is_materialized = false;
    view.hash();
    from_dump.views.push(view);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Ignore);
    comparer.drop_views().await.unwrap();
    let script = comparer.get_script();

    // The DROP must appear in the output …
    assert!(
        script.to_lowercase().contains("drop view"),
        "FROM-only view must produce a DROP statement even with use_drop=false, script:\n{}",
        script
    );
    // … but it must be commented out, not active SQL.
    let has_active_drop = script
        .lines()
        .any(|l| !l.starts_with("--") && l.to_lowercase().contains("drop view"));
    assert!(
        !has_active_drop,
        "FROM-only view DROP should be commented when use_drop=false, script:\n{}",
        script
    );
}

/// Counterpart: with use_drop=true the DROP for a FROM-only view must be active SQL.
#[tokio::test]
async fn from_only_view_active_drop_when_use_drop_true() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());

    let mut view = View::new(
        "obsolete_view".to_string(),
        "SELECT 1".to_string(),
        "public".to_string(),
        vec![],
    );
    view.is_materialized = false;
    view.hash();
    from_dump.views.push(view);

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.drop_views().await.unwrap();
    let script = comparer.get_script();

    let has_active_drop = script
        .lines()
        .any(|l| !l.starts_with("--") && l.to_lowercase().contains("drop view"));
    assert!(
        has_active_drop,
        "FROM-only view DROP should be active SQL when use_drop=true, script:\n{}",
        script
    );
}

#[tokio::test]
async fn issue189_signature_change_recreates_byte_identical_view() {
    // The view's hash is unchanged between FROM and TO, but the function
    // it references undergoes a signature change (integer → bigint),
    // forcing DROP FUNCTION ... CASCADE. PostgreSQL silently drops the
    // view as part of the cascade. Phase 7 must re-emit the view so the
    // migration leaves the database in a consistent state.
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    from_dump
        .routines
        .push(issue179_compute_routine("integer", "SELECT x * 2;"));
    to_dump.routines.push(issue179_compute_routine(
        "bigint",
        "SELECT (x * 2)::bigint;",
    ));

    from_dump.views.push(issue189_view("v_things", false));
    to_dump.views.push(issue189_view("v_things", false));

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_routines_and_views().await.unwrap();
    let script = comparer.get_script();

    let drop_pos = script
        .find("drop function if exists test_deps.compute (x integer) cascade;")
        .expect("CASCADE drop must be emitted for the signature change");
    let create_fn_pos = script
        .find("create or replace function test_deps.compute(x integer) returns bigint")
        .expect("function recreate must be emitted");
    assert!(drop_pos < create_fn_pos);

    let view_pos = script
        .find("CREATE OR REPLACE VIEW test_deps.v_things")
        .expect("byte-identical view must be re-emitted as CREATE OR REPLACE VIEW after CASCADE");
    assert!(
        create_fn_pos < view_pos,
        "view recreate must run after the function recreate so the new signature is in place"
    );
}

#[tokio::test]
async fn issue189_signature_change_recreates_byte_identical_materialized_view() {
    // Same scenario as the regular-view case but with a materialized
    // view. PostgreSQL CASCADE drops these via `pg_depend` the same way,
    // so Phase 7 must emit a `create materialized view if not exists`.
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    from_dump
        .routines
        .push(issue179_compute_routine("integer", "SELECT x * 2;"));
    to_dump.routines.push(issue179_compute_routine(
        "bigint",
        "SELECT (x * 2)::bigint;",
    ));

    from_dump.views.push(issue189_view("mv_things", true));
    to_dump.views.push(issue189_view("mv_things", true));

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_routines_and_views().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("create materialized view if not exists test_deps.mv_things"),
        "materialized view must be re-emitted with IF NOT EXISTS: {}",
        script
    );
}

#[tokio::test]
async fn issue189_view_not_referencing_routine_is_not_recreated() {
    // A view that doesn't textually reference the CASCADE-affected
    // routine must be left alone — re-emitting it would clutter the
    // migration and could re-introduce a stale definition if the user
    // has the same view in both dumps for unrelated reasons.
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    from_dump
        .routines
        .push(issue179_compute_routine("integer", "SELECT x * 2;"));
    to_dump.routines.push(issue179_compute_routine(
        "bigint",
        "SELECT (x * 2)::bigint;",
    ));

    let mut unrelated_from = View::new(
        "v_other".to_string(),
        " SELECT value FROM test_deps.items;".to_string(),
        "test_deps".to_string(),
        vec!["test_deps.items".to_string()],
    );
    unrelated_from.hash();
    let mut unrelated_to = View::new(
        "v_other".to_string(),
        " SELECT value FROM test_deps.items;".to_string(),
        "test_deps".to_string(),
        vec!["test_deps.items".to_string()],
    );
    unrelated_to.hash();
    from_dump.views.push(unrelated_from);
    to_dump.views.push(unrelated_to);

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_routines_and_views().await.unwrap();
    let script = comparer.get_script();

    assert!(
        !script.contains("v_other"),
        "view that doesn't reference the cascaded routine must not be re-emitted: {}",
        script
    );
}

#[tokio::test]
async fn issue189_view_recreate_skipped_when_to_definition_no_longer_references_routine() {
    // TO-side gate (PR #186): if the TO view's definition was rewritten
    // to no longer call the affected routine, the CASCADE drop never
    // touches it (no pg_depend link). `compare_routines_and_views`
    // already emits the rewrite via the normal hash-diff path; Phase 7
    // must stay silent so we don't re-emit the view twice.
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    from_dump
        .routines
        .push(issue179_compute_routine("integer", "SELECT x * 2;"));
    to_dump.routines.push(issue179_compute_routine(
        "bigint",
        "SELECT (x * 2)::bigint;",
    ));

    from_dump.views.push(issue189_view("v_things", false));
    // TO view: same name, but the definition no longer references the
    // function — different hash, so Phase 5 handles it.
    let mut to_view = View::new(
        "v_things".to_string(),
        " SELECT value AS c FROM test_deps.items;".to_string(),
        "test_deps".to_string(),
        vec!["test_deps.items".to_string()],
    );
    to_view.hash();
    to_dump.views.push(to_view);

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_routines_and_views().await.unwrap();
    let script = comparer.get_script();

    // The view must appear exactly once (the normal hash-diff path), not
    // a second time from Phase 7's recreate block.
    let recreate_section_start = script.find("Recreate dependents dropped by CASCADE: Start");
    if let Some(start) = recreate_section_start {
        let recreate_end = script[start..]
            .find("Recreate dependents dropped by CASCADE: End")
            .map(|e| start + e)
            .unwrap_or(script.len());
        let recreate_section = &script[start..recreate_end];
        assert!(
            !recreate_section.contains("v_things"),
            "Phase 7 must not re-emit a view whose TO definition no longer references the routine: {}",
            recreate_section
        );
    }
}

#[test]
fn issue189_rewrite_create_view_anchored_to_prefix() {
    // PR #195 review (Copilot): the helper must not be fooled by the
    // literal text `CREATE OR REPLACE VIEW` appearing inside the view
    // definition body that `View::get_script` appends after the
    // `create view` prefix. A whole-script `contains` early-return
    // would skip the rewrite and leave the leading `create view`
    // unchanged, which is not idempotent against a surviving view.
    let script = "create view public.v_with_literal as\n\
                  SELECT 'CREATE OR REPLACE VIEW pretend.v AS SELECT 1' AS payload;\n\n";
    let rewritten = rewrite_create_view_to_create_or_replace(script);
    assert!(
        rewritten.starts_with("CREATE OR REPLACE VIEW public.v_with_literal as\n"),
        "leading `create view` must be rewritten even when the body \
         contains the same phrase as a string literal: {}",
        rewritten
    );
    // Already in the desired form — return unchanged (no double rewrite).
    let already = "CREATE OR REPLACE VIEW public.v as\nSELECT 1;\n";
    assert_eq!(rewrite_create_view_to_create_or_replace(already), already);
}

#[tokio::test]
async fn issue189_view_definition_with_create_or_replace_literal_is_recreated() {
    // End-to-end pin for the PR #195 reviewer concern: a view whose
    // definition embeds the literal text `CREATE OR REPLACE VIEW` must
    // still emit a properly idempotent `CREATE OR REPLACE VIEW` prefix
    // when Phase 7 re-emits it after a CASCADE drop.
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    from_dump
        .routines
        .push(issue179_compute_routine("integer", "SELECT x * 2;"));
    to_dump.routines.push(issue179_compute_routine(
        "bigint",
        "SELECT (x * 2)::bigint;",
    ));

    let definition = " SELECT test_deps.compute(value) AS c,\n        \
                      'CREATE OR REPLACE VIEW evil.v AS SELECT 1' AS payload\n   \
                      FROM test_deps.items;";
    let mut from_view = View::new(
        "v_things".to_string(),
        definition.to_string(),
        "test_deps".to_string(),
        vec!["test_deps.items".to_string()],
    );
    from_view.hash();
    let mut to_view = View::new(
        "v_things".to_string(),
        definition.to_string(),
        "test_deps".to_string(),
        vec!["test_deps.items".to_string()],
    );
    to_view.hash();
    from_dump.views.push(from_view);
    to_dump.views.push(to_view);

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_routines_and_views().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("CREATE OR REPLACE VIEW test_deps.v_things"),
        "view recreate must emit `CREATE OR REPLACE VIEW` at the leading \
         statement even when the body contains the same phrase: {}",
        script
    );
    // The body's literal must survive unchanged — we do NOT want a
    // rewrite that mangles a non-prefix occurrence.
    assert!(
        script.contains("'CREATE OR REPLACE VIEW evil.v AS SELECT 1'"),
        "literal inside the view body must be left intact: {}",
        script
    );
}

#[tokio::test]
async fn issue189_view_recreate_skipped_when_routine_unchanged() {
    // No CASCADE — no recreate. Mirrors
    // `issue179_recreate_skipped_when_routine_unchanged` for views.
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let routine = issue179_compute_routine("integer", "SELECT x * 2;");
    from_dump.routines.push(routine.clone());
    to_dump.routines.push(routine);

    from_dump.views.push(issue189_view("v_things", false));
    to_dump.views.push(issue189_view("v_things", false));

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_routines_and_views().await.unwrap();
    let script = comparer.get_script();

    assert!(
        !script.contains("Recreate dependents dropped by CASCADE"),
        "no CASCADE drop happened — recreate phase must stay silent: {}",
        script
    );
    assert!(
        !script.contains("CREATE OR REPLACE VIEW test_deps.v_things"),
        "view must not be re-emitted when the function is unchanged: {}",
        script
    );
}

fn view_227(name: &str, definition: &str, cols: &[(&str, &str)]) -> View {
    let mut v = View::new(
        name.to_string(),
        definition.to_string(),
        "public".to_string(),
        vec![],
    );
    v.columns = cols
        .iter()
        .map(|(n, t)| crate::dump::view::ViewColumn {
            name: n.to_string(),
            data_type: t.to_string(),
            collation: None,
        })
        .collect();
    v.hash();
    v
}

#[tokio::test]
async fn incompatible_view_column_change_drops_and_recreates() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());
    from_dump.views.push(view_227(
        "item_v",
        "select id, profile_id from public.item",
        &[("id", "integer"), ("profile_id", "integer")],
    ));
    to_dump.views.push(view_227(
        "item_v",
        "select id, kind, profile_id from public.item",
        &[
            ("id", "integer"),
            ("kind", "text"),
            ("profile_id", "integer"),
        ],
    ));

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.drop_views().await.unwrap();
    let script = comparer.get_script();
    let has_active_drop = script
        .lines()
        .any(|l| !l.starts_with("--") && l.contains("drop view if exists public.item_v"));
    assert!(
        has_active_drop,
        "incompatible column change must emit an active DROP VIEW:\n{script}"
    );
}

#[tokio::test]
async fn compatible_view_column_append_keeps_or_replace_without_drop() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());
    from_dump.views.push(view_227(
        "item_v",
        "select id from public.item",
        &[("id", "integer")],
    ));
    to_dump.views.push(view_227(
        "item_v",
        "select id, kind from public.item",
        &[("id", "integer"), ("kind", "text")],
    ));

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.drop_views().await.unwrap();
    let script = comparer.get_script();
    assert!(
        !script
            .lines()
            .any(|l| !l.starts_with("--") && l.to_lowercase().contains("drop view")),
        "appending a column at the end must not drop the view:\n{script}"
    );
}

#[tokio::test]
async fn changed_views_without_column_data_keep_or_replace() {
    // Dumps written by an older pgc carry no column data; the historical
    // CREATE OR REPLACE behavior must be preserved for them.
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());
    from_dump.views.push(view_227(
        "item_v",
        "select id, profile_id from public.item",
        &[],
    ));
    to_dump.views.push(view_227(
        "item_v",
        "select id, kind, profile_id from public.item",
        &[],
    ));

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.drop_views().await.unwrap();
    let script = comparer.get_script();
    assert!(
        !script
            .lines()
            .any(|l| !l.starts_with("--") && l.to_lowercase().contains("drop view")),
        "without column data the view must not be dropped:\n{script}"
    );
}

#[tokio::test]
async fn dependent_view_is_dropped_before_incompatible_base_view() {
    // v2 reads item_v and is unchanged; item_v changes incompatibly. DROP VIEW runs
    // without CASCADE, so v2 must be pulled into the drop set and dropped first.
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    from_dump.views.push(view_227(
        "item_v",
        "select id, profile_id from public.item",
        &[("id", "integer"), ("profile_id", "integer")],
    ));
    let mut from_dep = view_227("v2", "select id from public.item_v", &[("id", "integer")]);
    from_dep.table_relation = vec!["public.item_v".to_string()];
    from_dep.hash();
    from_dump.views.push(from_dep);

    to_dump.views.push(view_227(
        "item_v",
        "select id, kind, profile_id from public.item",
        &[
            ("id", "integer"),
            ("kind", "text"),
            ("profile_id", "integer"),
        ],
    ));
    let mut to_dep = view_227("v2", "select id from public.item_v", &[("id", "integer")]);
    to_dep.table_relation = vec!["public.item_v".to_string()];
    to_dep.hash();
    to_dump.views.push(to_dep);

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.drop_views().await.unwrap();
    let script = comparer.get_script();

    let drop_dep = script
        .find("drop view if exists public.v2;")
        .expect("dependent view must be dropped too");
    let drop_base = script
        .find("drop view if exists public.item_v;")
        .expect("base view must be dropped");
    assert!(
        drop_dep < drop_base,
        "dependent must drop before the view it reads:\n{script}"
    );
}
