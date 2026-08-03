//! Tests for `compare_grants` and `compare_column_grants` across all three
//! [`GrantsMode`] settings (`ignore`, `addonly`, `full`), including owner
//! changes and the default-ACL handling for recreated or dropped objects.

use crate::comparer::core::*;
use super::helpers::*;
use crate::config::dump_config::DumpConfig;
use crate::config::grants_mode::GrantsMode;
use crate::dump::default_privilege::DefaultPrivilege;
use crate::dump::foreign_table::ForeignTable;
use crate::dump::routine::Routine;
use crate::dump::schema::Schema;
use crate::dump::sequence::Sequence;
use crate::dump::table::Table;
use crate::dump::view::View;
use sqlx::postgres::types::Oid;

#[tokio::test]
async fn compare_grants_ignore_mode_produces_no_output() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_schema = Schema::new("public".to_string(), "public".to_string(), None);
    from_schema.acl = vec!["reader=U/owner".to_string()];
    let mut to_schema = Schema::new("public".to_string(), "public".to_string(), None);
    to_schema.acl = vec!["reader=U/owner".to_string(), "writer=UC/owner".to_string()];

    from_dump.schemas.push(from_schema);
    to_dump.schemas.push(to_schema);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Ignore);
    comparer.compare_grants().await.unwrap();
    let script = comparer.get_script();

    assert!(
        !script.contains("GRANT"),
        "Ignore mode must not emit GRANT, got: {script}"
    );
    assert!(
        !script.contains("REVOKE"),
        "Ignore mode must not emit REVOKE, got: {script}"
    );
}

#[tokio::test]
async fn compare_grants_addonly_adds_missing_schema_grant() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_schema = Schema::new("myschema".to_string(), "myschema".to_string(), None);
    from_schema.acl = vec!["reader=U/owner".to_string()];
    let mut to_schema = Schema::new("myschema".to_string(), "myschema".to_string(), None);
    to_schema.acl = vec!["reader=U/owner".to_string(), "writer=UC/owner".to_string()];

    from_dump.schemas.push(from_schema);
    to_dump.schemas.push(to_schema);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::AddOnly);
    comparer.compare_grants().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("GRANT CREATE, USAGE ON SCHEMA myschema TO writer;"),
        "AddOnly must add missing grant, got: {script}"
    );
    assert!(
        !script.contains("REVOKE"),
        "AddOnly must not emit REVOKE, got: {script}"
    );
}

#[tokio::test]
async fn compare_grants_addonly_does_not_revoke_removed_grant() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_schema = Schema::new("myschema".to_string(), "myschema".to_string(), None);
    from_schema.acl = vec!["reader=U/owner".to_string(), "writer=UC/owner".to_string()];
    let mut to_schema = Schema::new("myschema".to_string(), "myschema".to_string(), None);
    to_schema.acl = vec!["reader=U/owner".to_string()];

    from_dump.schemas.push(from_schema);
    to_dump.schemas.push(to_schema);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::AddOnly);
    comparer.compare_grants().await.unwrap();
    let script = comparer.get_script();

    assert!(
        !script.contains("REVOKE"),
        "AddOnly must not revoke, got: {script}"
    );
}

#[tokio::test]
async fn compare_grants_full_revokes_removed_schema_grant() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_schema = Schema::new("myschema".to_string(), "myschema".to_string(), None);
    from_schema.acl = vec!["reader=U/owner".to_string(), "writer=UC/owner".to_string()];
    let mut to_schema = Schema::new("myschema".to_string(), "myschema".to_string(), None);
    to_schema.acl = vec!["reader=U/owner".to_string()];

    from_dump.schemas.push(from_schema);
    to_dump.schemas.push(to_schema);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Full);
    comparer.compare_grants().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("REVOKE CREATE, USAGE ON SCHEMA myschema FROM writer;"),
        "Full mode must revoke removed grant, got: {script}"
    );
}

#[tokio::test]
async fn compare_grants_full_table_add_and_revoke() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_table = Table::new(
        "public".to_string(),
        "users".to_string(),
        "public".to_string(),
        "users".to_string(),
        "owner".to_string(),
        None,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        None,
    );
    from_table.acl = vec!["reader=r/owner".to_string()];

    let mut to_table = Table::new(
        "public".to_string(),
        "users".to_string(),
        "public".to_string(),
        "users".to_string(),
        "owner".to_string(),
        None,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        None,
    );
    to_table.acl = vec!["writer=rw/owner".to_string()];

    from_dump.tables.push(from_table);
    to_dump.tables.push(to_table);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Full);
    comparer.compare_grants().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("GRANT SELECT, UPDATE ON TABLE public.users TO writer;"),
        "Full must add new grant, got: {script}"
    );
    assert!(
        script.contains("REVOKE SELECT ON TABLE public.users FROM reader;"),
        "Full must revoke removed grant, got: {script}"
    );
}

#[tokio::test]
async fn compare_grants_sequence() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_seq = Sequence::new(
        "public".to_string(),
        "my_seq".to_string(),
        "owner".to_string(),
        "bigint".to_string(),
        Some(1),
        Some(1),
        Some(9223372036854775807),
        Some(1),
        false,
        Some(1),
        Some(1),
        None,
        None,
        None,
    );
    from_seq.acl = vec![];

    let mut to_seq = Sequence::new(
        "public".to_string(),
        "my_seq".to_string(),
        "owner".to_string(),
        "bigint".to_string(),
        Some(1),
        Some(1),
        Some(9223372036854775807),
        Some(1),
        false,
        Some(1),
        Some(1),
        None,
        None,
        None,
    );
    to_seq.acl = vec!["reader=U/owner".to_string()];

    from_dump.sequences.push(from_seq);
    to_dump.sequences.push(to_seq);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::AddOnly);
    comparer.compare_grants().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("GRANT USAGE ON SEQUENCE public.my_seq TO reader;"),
        "Must add sequence grant, got: {script}"
    );
}

#[tokio::test]
async fn compare_grants_view() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_view = View::new(
        "my_view".to_string(),
        "SELECT 1".to_string(),
        "public".to_string(),
        Vec::new(),
    );
    from_view.acl = vec!["reader=r/owner".to_string()];

    let mut to_view = View::new(
        "my_view".to_string(),
        "SELECT 1".to_string(),
        "public".to_string(),
        Vec::new(),
    );
    to_view.acl = vec!["reader=r/owner".to_string(), "writer=rw/owner".to_string()];

    from_dump.views.push(from_view);
    to_dump.views.push(to_view);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Full);
    comparer.compare_grants().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("GRANT SELECT, UPDATE ON TABLE public.my_view TO writer;"),
        "Must add view grant, got: {script}"
    );
    assert!(
        !script.contains("REVOKE"),
        "No revoke expected when unchanged grant remains, got: {script}"
    );
}

#[tokio::test]
async fn compare_grants_foreign_table_add_and_revoke() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_ft = ForeignTable::new(
        "public".to_string(),
        "ft_orders".to_string(),
        "fdw_server".to_string(),
        "owner".to_string(),
        Vec::new(),
        Vec::new(),
    );
    from_ft.acl = vec!["reader=r/owner".to_string()];

    let mut to_ft = ForeignTable::new(
        "public".to_string(),
        "ft_orders".to_string(),
        "fdw_server".to_string(),
        "owner".to_string(),
        Vec::new(),
        Vec::new(),
    );
    to_ft.acl = vec!["writer=rw/owner".to_string()];

    from_dump.foreign_tables.push(from_ft);
    to_dump.foreign_tables.push(to_ft);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Full);
    comparer.compare_grants().await.unwrap();
    let script = comparer.get_script();

    // PostgreSQL's GRANT syntax has no `ON FOREIGN TABLE` form — foreign
    // tables share the regular `ON TABLE` grant syntax. Pre-fix the
    // comparer emitted `ON FOREIGN TABLE`, which produced invalid SQL
    // (`syntax error at or near "TABLE"`) when the diff was applied.
    assert!(
        script.contains("GRANT SELECT, UPDATE ON TABLE public.ft_orders TO writer;"),
        "Full must add foreign table grant via ON TABLE syntax, got: {script}"
    );
    assert!(
        script.contains("REVOKE SELECT ON TABLE public.ft_orders FROM reader;"),
        "Full must revoke removed foreign table grant via ON TABLE syntax, got: {script}"
    );
    assert!(
        !script.contains("ON FOREIGN TABLE"),
        "Foreign table grants must not use `ON FOREIGN TABLE` (invalid SQL), got: {script}"
    );
}

/// User-reported regression: when ownership changes AND TO has an explicit
/// grant to the former owner, the migration must emit exactly one GRANT
/// (for the explicit privilege in TO) and zero REVOKEs (the implicit-owner
/// ACL row is stripped by ALTER OWNER alone). Replays the exact ACL shape
/// you'd see in the schema_a → schema_b owner-change scenario after both
/// FROM and TO have run their explicit GRANTs and PG has materialised the
/// implicit-owner row.
#[tokio::test]
async fn compare_grants_owner_change_with_explicit_grant_to_former_owner_is_idempotent() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    // FROM table: owned by pgc_owner_from. relacl carries the implicit-
    // owner row (pg materialises it once any GRANT exists) plus the two
    // explicit grants to reader/writer.
    let mut from_table = Table::new(
        "test_schema".to_string(),
        "users".to_string(),
        "test_schema".to_string(),
        "users".to_string(),
        "pgc_owner_from".to_string(),
        None,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        None,
    );
    from_table.acl = vec![
        "pgc_owner_from=arwdDxt/pgc_owner_from".to_string(),
        "pgc_grant_reader=r/pgc_owner_from".to_string(),
        "pgc_grant_writer=arw/pgc_owner_from".to_string(),
    ];

    // TO table: owned by pgc_owner_to. relacl has the new implicit-owner
    // row, the same reader grant, the writer with UPDATE removed, and an
    // explicit grant to the former owner pgc_owner_from.
    let mut to_table = Table::new(
        "test_schema".to_string(),
        "users".to_string(),
        "test_schema".to_string(),
        "users".to_string(),
        "pgc_owner_to".to_string(),
        None,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        None,
    );
    to_table.acl = vec![
        "pgc_owner_to=arwdDxt/pgc_owner_to".to_string(),
        "pgc_owner_from=r/pgc_owner_to".to_string(),
        "pgc_grant_reader=r/pgc_owner_to".to_string(),
        "pgc_grant_writer=ar/pgc_owner_to".to_string(),
    ];

    from_dump.tables.push(from_table);
    to_dump.tables.push(to_table);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Full);
    comparer.compare_grants().await.unwrap();
    let script = comparer.get_script();

    // Exactly two statements expected:
    //   - GRANT SELECT TO pgc_owner_from (the new explicit grant in TO)
    //   - REVOKE UPDATE FROM pgc_grant_writer (UPDATE removed in TO)
    assert!(
        script.contains("GRANT SELECT ON TABLE test_schema.users TO pgc_owner_from;"),
        "Must emit explicit grant to former owner, got: {script}"
    );
    assert!(
        script.contains("REVOKE UPDATE ON TABLE test_schema.users FROM pgc_grant_writer;"),
        "Must revoke writer's UPDATE removed in TO, got: {script}"
    );
    // No REVOKE/GRANT for pgc_owner_to (TO owner — implicit privileges).
    // No REVOKE for pgc_owner_from's old implicit-owner row — ALTER OWNER
    // strips it. Specifically NO REVOKE on pgc_owner_from for the 7 other
    // privileges, which is the bug this regression test guards against.
    assert!(
        !script.contains("FROM pgc_owner_from"),
        "Must not REVOKE anything from former owner — ALTER OWNER strips the implicit row, got: {script}"
    );
    assert!(
        !script.contains("pgc_owner_to"),
        "Current owner must not appear in grants output, got: {script}"
    );
}

/// Regression: a TO-only foreign table must inherit the FROM database's
/// default-table privileges as the effective `from_acl` under `full` mode,
/// because PostgreSQL auto-applies them on CREATE. Without this, the diff
/// is non-idempotent — re-running compare after applying it would emit
/// `REVOKE` statements for the auto-granted privileges that the migration
/// itself is responsible for cleaning up.
#[tokio::test]
async fn compare_grants_new_foreign_table_revokes_default_priv_grants() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    // FROM has a default-privilege rule that grants SELECT to `reader` on
    // any new table in `public`. No explicit grants in TO → after CREATE,
    // the auto-applied SELECT must be revoked in this same diff.
    let dp = DefaultPrivilege {
        role_name: String::new(),
        schema_name: "public".to_string(),
        object_type: "r".to_string(),
        acl: vec!["reader=r/owner".to_string()],
        hash: Some("dp".to_string()),
    };
    from_dump.default_privileges.push(dp);

    // TO-only foreign table (no FROM counterpart, no explicit ACL).
    let to_ft = ForeignTable::new(
        "public".to_string(),
        "ft_new".to_string(),
        "fdw_server".to_string(),
        "owner".to_string(),
        Vec::new(),
        Vec::new(),
    );
    to_dump.foreign_tables.push(to_ft);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Full);
    comparer.compare_grants().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("REVOKE SELECT ON TABLE public.ft_new FROM reader;"),
        "New foreign table must revoke auto-applied default-privilege grants under full mode, got: {script}"
    );
    assert!(
        !script.contains("ON FOREIGN TABLE"),
        "Foreign table grants must use `ON TABLE` syntax, got: {script}"
    );
}

#[tokio::test]
async fn compare_grants_dropped_view_restores_all_grants() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    // Both FROM and TO have the same grant on the view.
    let mut from_view = View::new(
        "my_view".to_string(),
        "SELECT 1".to_string(),
        "public".to_string(),
        Vec::new(),
    );
    from_view.acl = vec!["reader=r/owner".to_string()];

    let mut to_view = View::new(
        "my_view".to_string(),
        "SELECT 1".to_string(),
        "public".to_string(),
        Vec::new(),
    );
    to_view.acl = vec!["reader=r/owner".to_string()];

    from_dump.views.push(from_view);
    to_dump.views.push(to_view);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Full);
    // Simulate that the view was dropped earlier in the script
    // (e.g. as a dependency of an altered table).
    comparer
        .dropped_views
        .insert(Comparer::normalized_view_key("public", "my_view"), true);
    comparer.compare_grants().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("GRANT SELECT ON TABLE public.my_view TO reader;"),
        "Dropped view must restore grants even when FROM has the same ACL, got: {script}"
    );
}

/// When a view's DROP was only commented out (use_drop=false), the view still
/// exists in the database.  compare_grants must keep the original from_acl so
/// that identical ACLs produce no diff (no redundant GRANTs/REVOKEs).
#[tokio::test]
async fn compare_grants_commented_drop_keeps_from_acl() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_view = View::new(
        "my_view".to_string(),
        "SELECT 1".to_string(),
        "public".to_string(),
        Vec::new(),
    );
    from_view.acl = vec!["reader=r/owner".to_string()];

    let mut to_view = View::new(
        "my_view".to_string(),
        "SELECT 1".to_string(),
        "public".to_string(),
        Vec::new(),
    );
    to_view.acl = vec!["reader=r/owner".to_string()];

    from_dump.views.push(from_view);
    to_dump.views.push(to_view);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Full);
    // Simulate a commented-out drop (use_drop=false → stored as false).
    comparer
        .dropped_views
        .insert(Comparer::normalized_view_key("public", "my_view"), false);
    comparer.compare_grants().await.unwrap();
    let script = comparer.get_script();

    // ACLs are identical and the view was NOT actually dropped,
    // so no GRANT/REVOKE should appear.
    let has_grant_stmt = script
        .lines()
        .any(|l| l.trim_start().to_lowercase().starts_with("grant "));
    assert!(
        !has_grant_stmt,
        "Commented-out drop must not cause redundant GRANTs, got: {script}"
    );
    let has_revoke_stmt = script
        .lines()
        .any(|l| l.trim_start().to_lowercase().starts_with("revoke "));
    assert!(
        !has_revoke_stmt,
        "Commented-out drop must not cause redundant REVOKEs, got: {script}"
    );
}

#[tokio::test]
async fn compare_grants_routine_function() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_routine = Routine::new(
        "public".to_string(),
        Oid(1),
        "my_func".to_string(),
        "plpgsql".to_string(),
        "function".to_string(),
        "void".to_string(),
        "".to_string(),
        None,
        None,
        "BEGIN END".to_string(),
    );
    from_routine.acl = vec![];

    let mut to_routine = Routine::new(
        "public".to_string(),
        Oid(1),
        "my_func".to_string(),
        "plpgsql".to_string(),
        "function".to_string(),
        "void".to_string(),
        "".to_string(),
        None,
        None,
        "BEGIN END".to_string(),
    );
    to_routine.acl = vec!["app=X/owner".to_string()];

    from_dump.routines.push(from_routine);
    to_dump.routines.push(to_routine);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::AddOnly);
    comparer.compare_grants().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("GRANT EXECUTE ON FUNCTION public.my_func() TO app;"),
        "Must add function grant, got: {script}"
    );
}

#[tokio::test]
async fn compare_grants_routine_procedure() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_routine = Routine::new(
        "public".to_string(),
        Oid(2),
        "my_proc".to_string(),
        "plpgsql".to_string(),
        "procedure".to_string(),
        "void".to_string(),
        "days integer".to_string(),
        None,
        None,
        "BEGIN END".to_string(),
    );
    from_routine.acl = vec!["app=X/owner".to_string()];

    let mut to_routine = Routine::new(
        "public".to_string(),
        Oid(2),
        "my_proc".to_string(),
        "plpgsql".to_string(),
        "procedure".to_string(),
        "void".to_string(),
        "days integer".to_string(),
        None,
        None,
        "BEGIN END".to_string(),
    );
    to_routine.acl = vec![];

    from_dump.routines.push(from_routine);
    to_dump.routines.push(to_routine);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Full);
    comparer.compare_grants().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("REVOKE EXECUTE ON PROCEDURE public.my_proc(days integer) FROM app;"),
        "Full must revoke removed procedure grant, got: {script}"
    );
}

#[tokio::test]
async fn compare_grants_with_grant_option() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_schema = Schema::new("myschema".to_string(), "myschema".to_string(), None);
    from_schema.acl = vec!["reader=U/owner".to_string()];
    let mut to_schema = Schema::new("myschema".to_string(), "myschema".to_string(), None);
    to_schema.acl = vec!["reader=U*/owner".to_string()];

    from_dump.schemas.push(from_schema);
    to_dump.schemas.push(to_schema);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Full);
    comparer.compare_grants().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("GRANT USAGE ON SCHEMA myschema TO reader WITH GRANT OPTION;"),
        "Must add grant with grant option, got: {script}"
    );
    assert!(
        !script.contains("REVOKE"),
        "Upgrading to WITH GRANT OPTION must not REVOKE, got: {script}"
    );
}

#[tokio::test]
async fn compare_grants_no_comments_mode() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_schema = Schema::new("myschema".to_string(), "myschema".to_string(), None);
    from_schema.acl = vec![];
    let mut to_schema = Schema::new("myschema".to_string(), "myschema".to_string(), None);
    to_schema.acl = vec!["reader=U/owner".to_string()];

    from_dump.schemas.push(from_schema);
    to_dump.schemas.push(to_schema);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, false, GrantsMode::AddOnly);
    comparer.compare_grants().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("GRANT USAGE ON SCHEMA myschema TO reader;"),
        "Grant must still be emitted, got: {script}"
    );
    assert!(
        !script.contains("/* Grants for schema"),
        "Comments must be suppressed, got: {script}"
    );
}

#[tokio::test]
async fn compare_grants_new_object_no_from_acl() {
    let from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    // Table exists only in TO
    let mut to_table = Table::new(
        "public".to_string(),
        "new_tbl".to_string(),
        "public".to_string(),
        "new_tbl".to_string(),
        "owner".to_string(),
        None,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        None,
    );
    to_table.acl = vec!["reader=r/owner".to_string()];

    to_dump.tables.push(to_table);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::AddOnly);
    comparer.compare_grants().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("GRANT SELECT ON TABLE public.new_tbl TO reader;"),
        "Must grant on new object with empty FROM acl, got: {script}"
    );
}

#[tokio::test]
async fn compare_grants_table_addonly_no_revoke() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_table = Table::new(
        "public".to_string(),
        "orders".to_string(),
        "public".to_string(),
        "orders".to_string(),
        "owner".to_string(),
        None,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        None,
    );
    from_table.acl = vec!["reader=r/owner".to_string(), "old_app=rw/owner".to_string()];

    let mut to_table = Table::new(
        "public".to_string(),
        "orders".to_string(),
        "public".to_string(),
        "orders".to_string(),
        "owner".to_string(),
        None,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        None,
    );
    to_table.acl = vec![
        "reader=r/owner".to_string(),
        "new_app=rwd/owner".to_string(),
    ];

    from_dump.tables.push(from_table);
    to_dump.tables.push(to_table);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::AddOnly);
    comparer.compare_grants().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("GRANT DELETE, SELECT, UPDATE ON TABLE public.orders TO new_app;"),
        "AddOnly must add new_app grant, got: {script}"
    );
    assert!(
        !script.contains("REVOKE"),
        "AddOnly must not revoke old_app, got: {script}"
    );
}

#[tokio::test]
async fn compare_grants_sequence_full_revoke() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_seq = Sequence::new(
        "public".to_string(),
        "order_id_seq".to_string(),
        "owner".to_string(),
        "bigint".to_string(),
        Some(1),
        Some(1),
        Some(9223372036854775807),
        Some(1),
        false,
        Some(1),
        Some(1),
        None,
        None,
        None,
    );
    from_seq.acl = vec!["app=U/owner".to_string(), "old_svc=U/owner".to_string()];

    let mut to_seq = Sequence::new(
        "public".to_string(),
        "order_id_seq".to_string(),
        "owner".to_string(),
        "bigint".to_string(),
        Some(1),
        Some(1),
        Some(9223372036854775807),
        Some(1),
        false,
        Some(1),
        Some(1),
        None,
        None,
        None,
    );
    to_seq.acl = vec!["app=U/owner".to_string()];

    from_dump.sequences.push(from_seq);
    to_dump.sequences.push(to_seq);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Full);
    comparer.compare_grants().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("REVOKE USAGE ON SEQUENCE public.order_id_seq FROM old_svc;"),
        "Full must revoke removed sequence grant, got: {script}"
    );
    assert!(
        !script.contains("GRANT"),
        "No new grants expected, got: {script}"
    );
}

#[tokio::test]
async fn compare_grants_sequence_addonly_no_revoke() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_seq = Sequence::new(
        "public".to_string(),
        "s1".to_string(),
        "owner".to_string(),
        "bigint".to_string(),
        Some(1),
        Some(1),
        Some(9223372036854775807),
        Some(1),
        false,
        Some(1),
        Some(1),
        None,
        None,
        None,
    );
    from_seq.acl = vec!["old_svc=U/owner".to_string()];

    let mut to_seq = Sequence::new(
        "public".to_string(),
        "s1".to_string(),
        "owner".to_string(),
        "bigint".to_string(),
        Some(1),
        Some(1),
        Some(9223372036854775807),
        Some(1),
        false,
        Some(1),
        Some(1),
        None,
        None,
        None,
    );
    to_seq.acl = vec![];

    from_dump.sequences.push(from_seq);
    to_dump.sequences.push(to_seq);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::AddOnly);
    comparer.compare_grants().await.unwrap();
    let script = comparer.get_script();

    assert!(
        !script.contains("REVOKE"),
        "AddOnly must not revoke sequence grants, got: {script}"
    );
    assert!(
        !script.contains("GRANT"),
        "No grants expected, got: {script}"
    );
}

#[tokio::test]
async fn compare_grants_view_full_revoke() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_view = View::new(
        "report_v".to_string(),
        "SELECT 1".to_string(),
        "reports".to_string(),
        Vec::new(),
    );
    from_view.acl = vec!["analyst=r/owner".to_string(), "intern=r/owner".to_string()];

    let mut to_view = View::new(
        "report_v".to_string(),
        "SELECT 1".to_string(),
        "reports".to_string(),
        Vec::new(),
    );
    to_view.acl = vec!["analyst=r/owner".to_string()];

    from_dump.views.push(from_view);
    to_dump.views.push(to_view);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Full);
    comparer.compare_grants().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("REVOKE SELECT ON TABLE reports.report_v FROM intern;"),
        "Full must revoke removed view grant, got: {script}"
    );
    assert!(
        !script.contains("GRANT"),
        "No new grants expected, got: {script}"
    );
}

#[tokio::test]
async fn compare_grants_routine_function_full_revoke() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_routine = Routine::new(
        "public".to_string(),
        Oid(10),
        "calc".to_string(),
        "plpgsql".to_string(),
        "function".to_string(),
        "integer".to_string(),
        "x integer".to_string(),
        None,
        None,
        "BEGIN RETURN x; END".to_string(),
    );
    from_routine.acl = vec!["app=X/owner".to_string(), "old_svc=X/owner".to_string()];

    let mut to_routine = Routine::new(
        "public".to_string(),
        Oid(10),
        "calc".to_string(),
        "plpgsql".to_string(),
        "function".to_string(),
        "integer".to_string(),
        "x integer".to_string(),
        None,
        None,
        "BEGIN RETURN x; END".to_string(),
    );
    to_routine.acl = vec!["app=X/owner".to_string()];

    from_dump.routines.push(from_routine);
    to_dump.routines.push(to_routine);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Full);
    comparer.compare_grants().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("REVOKE EXECUTE ON FUNCTION public.calc(x integer) FROM old_svc;"),
        "Full must revoke removed function grant, got: {script}"
    );
    assert!(
        !script.contains("GRANT"),
        "No new grants expected, got: {script}"
    );
}

#[tokio::test]
async fn compare_grants_grantor_only_diff_produces_no_output() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    // Schema: same grantee + privileges, different grantor
    let mut from_schema = Schema::new("app".to_string(), "app".to_string(), None);
    from_schema.acl = vec!["reader=UC/old_owner".to_string()];
    let mut to_schema = Schema::new("app".to_string(), "app".to_string(), None);
    to_schema.acl = vec!["reader=UC/new_owner".to_string()];
    from_dump.schemas.push(from_schema);
    to_dump.schemas.push(to_schema);

    // Table: same grantee + privileges, different grantor
    let mut from_table = Table::new(
        "app".to_string(),
        "t1".to_string(),
        "app".to_string(),
        "t1".to_string(),
        "owner".to_string(),
        None,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        None,
    );
    from_table.acl = vec!["reader=rw/old_owner".to_string()];
    let mut to_table = Table::new(
        "app".to_string(),
        "t1".to_string(),
        "app".to_string(),
        "t1".to_string(),
        "owner".to_string(),
        None,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        None,
    );
    to_table.acl = vec!["reader=rw/new_owner".to_string()];
    from_dump.tables.push(from_table);
    to_dump.tables.push(to_table);

    // Sequence: same grantee + privileges, different grantor
    let mut from_seq = Sequence::new(
        "app".to_string(),
        "s1".to_string(),
        "owner".to_string(),
        "bigint".to_string(),
        Some(1),
        Some(1),
        Some(9223372036854775807),
        Some(1),
        false,
        Some(1),
        Some(1),
        None,
        None,
        None,
    );
    from_seq.acl = vec!["reader=U/old_owner".to_string()];
    let mut to_seq = Sequence::new(
        "app".to_string(),
        "s1".to_string(),
        "owner".to_string(),
        "bigint".to_string(),
        Some(1),
        Some(1),
        Some(9223372036854775807),
        Some(1),
        false,
        Some(1),
        Some(1),
        None,
        None,
        None,
    );
    to_seq.acl = vec!["reader=U/new_owner".to_string()];
    from_dump.sequences.push(from_seq);
    to_dump.sequences.push(to_seq);

    // View: same grantee + privileges, different grantor
    let mut from_view = View::new(
        "v1".to_string(),
        "SELECT 1".to_string(),
        "app".to_string(),
        Vec::new(),
    );
    from_view.acl = vec!["reader=r/old_owner".to_string()];
    let mut to_view = View::new(
        "v1".to_string(),
        "SELECT 1".to_string(),
        "app".to_string(),
        Vec::new(),
    );
    to_view.acl = vec!["reader=r/new_owner".to_string()];
    from_dump.views.push(from_view);
    to_dump.views.push(to_view);

    // Routine: same grantee + privileges, different grantor
    let mut from_routine = Routine::new(
        "app".to_string(),
        Oid(99),
        "do_it".to_string(),
        "plpgsql".to_string(),
        "function".to_string(),
        "void".to_string(),
        "".to_string(),
        None,
        None,
        "BEGIN END".to_string(),
    );
    from_routine.acl = vec!["runner=X/old_owner".to_string()];
    let mut to_routine = Routine::new(
        "app".to_string(),
        Oid(99),
        "do_it".to_string(),
        "plpgsql".to_string(),
        "function".to_string(),
        "void".to_string(),
        "".to_string(),
        None,
        None,
        "BEGIN END".to_string(),
    );
    to_routine.acl = vec!["runner=X/new_owner".to_string()];
    from_dump.routines.push(from_routine);
    to_dump.routines.push(to_routine);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Full);
    comparer.compare_grants().await.unwrap();
    let script = comparer.get_script();

    assert!(
        !script.contains("GRANT"),
        "Grantor-only diff must not emit GRANT, got: {script}"
    );
    assert!(
        !script.contains("REVOKE"),
        "Grantor-only diff must not emit REVOKE, got: {script}"
    );
}

#[tokio::test]
async fn compare_grants_full_grant_option_downgrade() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_table = Table::new(
        "public".to_string(),
        "items".to_string(),
        "public".to_string(),
        "items".to_string(),
        "owner".to_string(),
        None,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        None,
    );
    from_table.acl = vec!["admin=r*/owner".to_string()];

    let mut to_table = Table::new(
        "public".to_string(),
        "items".to_string(),
        "public".to_string(),
        "items".to_string(),
        "owner".to_string(),
        None,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        None,
    );
    to_table.acl = vec!["admin=r/owner".to_string()];

    from_dump.tables.push(from_table);
    to_dump.tables.push(to_table);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Full);
    comparer.compare_grants().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("REVOKE GRANT OPTION FOR SELECT ON TABLE public.items FROM admin;"),
        "Full must revoke grant option when downgrading, got: {script}"
    );
    assert!(
        !script.contains("GRANT SELECT"),
        "No new grant expected for downgrade, got: {script}"
    );
    // Should only contain REVOKE GRANT OPTION FOR, not a bare REVOKE SELECT
    assert!(
        !script.contains("REVOKE SELECT ON TABLE"),
        "Must not fully revoke the privilege on downgrade, got: {script}"
    );
}

#[tokio::test]
async fn compare_grants_addonly_ignores_grant_option_downgrade() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_table = Table::new(
        "public".to_string(),
        "items".to_string(),
        "public".to_string(),
        "items".to_string(),
        "owner".to_string(),
        None,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        None,
    );
    from_table.acl = vec!["admin=r*/owner".to_string()];

    let mut to_table = Table::new(
        "public".to_string(),
        "items".to_string(),
        "public".to_string(),
        "items".to_string(),
        "owner".to_string(),
        None,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        None,
    );
    to_table.acl = vec!["admin=r/owner".to_string()];

    from_dump.tables.push(from_table);
    to_dump.tables.push(to_table);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::AddOnly);
    comparer.compare_grants().await.unwrap();
    let script = comparer.get_script();

    assert!(
        !script.contains("REVOKE"),
        "AddOnly must not revoke grant option, got: {script}"
    );
    assert!(
        !script.contains("GRANT"),
        "No new grant expected, got: {script}"
    );
}

#[tokio::test]
async fn compare_grants_excludes_owner_acl_entries() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    // Table with ownership change: old_owner → new_owner
    let mut from_table = Table::new(
        "public".to_string(),
        "data".to_string(),
        "public".to_string(),
        "data".to_string(),
        "old_owner".to_string(),
        None,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        None,
    );
    from_table.acl = vec![
        "old_owner=arwdDxt/old_owner".to_string(),
        "reader=r/old_owner".to_string(),
    ];

    let mut to_table = Table::new(
        "public".to_string(),
        "data".to_string(),
        "public".to_string(),
        "data".to_string(),
        "new_owner".to_string(),
        None,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        None,
    );
    to_table.acl = vec![
        "new_owner=arwdDxt/new_owner".to_string(),
        "reader=r/new_owner".to_string(),
    ];

    from_dump.tables.push(from_table);
    to_dump.tables.push(to_table);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Full);
    comparer.compare_grants().await.unwrap();
    let script = comparer.get_script();

    // The `old_owner=arwdDxt/old_owner` and `new_owner=arwdDxt/new_owner`
    // entries are PostgreSQL's implicit-owner ACL rows (materialised once
    // any GRANT exists). `ALTER TABLE ... OWNER TO new_owner` removes
    // old_owner's implicit row and adds new_owner's automatically — no
    // REVOKE/GRANT is needed for those rows. Reader is unchanged on both
    // sides. Net diff: empty. Pre-fix the comparer treated the implicit
    // FROM-owner row as if it would persist post-migration and emitted a
    // long REVOKE, then on the next compare run had nothing to compare
    // against and emitted GRANTs — a non-idempotent oscillation.
    assert!(
        !script.contains("REVOKE"),
        "ALTER OWNER alone strips the implicit-owner entry; no REVOKE should be emitted, got: {script}"
    );
    assert!(
        !script.contains("GRANT"),
        "No grants expected — reader is unchanged and new_owner gets implicit privileges via ALTER OWNER, got: {script}"
    );
    assert!(
        !script.contains("new_owner"),
        "Must not reference the new owner explicitly, got: {script}"
    );
    assert!(
        !script.contains("old_owner"),
        "Must not reference the former owner explicitly when only the implicit-owner ACL row needs to migrate, got: {script}"
    );
}

#[tokio::test]
async fn compare_grants_full_revokes_explicit_grants_from_former_owner() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_schema = Schema::new("billing".to_string(), "billing".to_string(), None);
    from_schema.owner = "old_owner".to_string();
    from_schema.acl = vec!["old_owner=UC/old_owner".to_string()];

    let mut to_schema = Schema::new("billing".to_string(), "billing".to_string(), None);
    to_schema.owner = "new_owner".to_string();

    let mut from_table = Table::new(
        "billing".to_string(),
        "invoice".to_string(),
        "billing".to_string(),
        "invoice".to_string(),
        "old_owner".to_string(),
        None,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        None,
    );
    from_table.acl = vec!["old_owner=ar/old_owner".to_string()];

    let to_table = Table::new(
        "billing".to_string(),
        "invoice".to_string(),
        "billing".to_string(),
        "invoice".to_string(),
        "new_owner".to_string(),
        None,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        None,
    );

    from_dump.schemas.push(from_schema);
    from_dump.tables.push(from_table);
    to_dump.schemas.push(to_schema);
    to_dump.tables.push(to_table);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Full);
    comparer.compare_grants().await.unwrap();
    let script = comparer.get_script();

    // Same reasoning as `compare_grants_excludes_owner_acl_entries`:
    // `old_owner=UC/old_owner` and `old_owner=ar/old_owner` are
    // implicit-owner ACL entries that `ALTER ... OWNER TO new_owner`
    // strips automatically. Comparing against TO (which has no entries
    // at all) should produce an empty diff, not REVOKE statements.
    assert!(
        !script.contains("REVOKE"),
        "Implicit-owner ACL entries are removed by ALTER OWNER alone; no REVOKE should be emitted, got: {script}"
    );
    assert!(
        !script.contains("new_owner"),
        "Current owner must not appear in grant/revoke output, got: {script}"
    );
    assert!(
        !script.contains("old_owner"),
        "Former owner must not appear in grant/revoke output for the implicit-owner ACL row, got: {script}"
    );
}

#[tokio::test]
async fn compare_grants_emits_explicit_grants_to_former_owner() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_schema = Schema::new("billing".to_string(), "billing".to_string(), None);
    from_schema.owner = "old_owner".to_string();

    let mut to_schema = Schema::new("billing".to_string(), "billing".to_string(), None);
    to_schema.owner = "new_owner".to_string();
    to_schema.acl = vec![
        "old_owner=UC/new_owner".to_string(),
        "app_user=U/new_owner".to_string(),
    ];

    let from_table = Table::new(
        "billing".to_string(),
        "invoice".to_string(),
        "billing".to_string(),
        "invoice".to_string(),
        "old_owner".to_string(),
        None,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        None,
    );

    let mut to_table = Table::new(
        "billing".to_string(),
        "invoice".to_string(),
        "billing".to_string(),
        "invoice".to_string(),
        "new_owner".to_string(),
        None,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        None,
    );
    to_table.acl = vec![
        "old_owner=ar/new_owner".to_string(),
        "app_user=r/new_owner".to_string(),
    ];

    from_dump.schemas.push(from_schema);
    from_dump.tables.push(from_table);
    to_dump.schemas.push(to_schema);
    to_dump.tables.push(to_table);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::AddOnly);
    comparer.compare_grants().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("GRANT CREATE, USAGE ON SCHEMA billing TO old_owner;"),
        "Former schema owner must receive explicit TO grant, got: {script}"
    );
    assert!(
        script.contains("GRANT USAGE ON SCHEMA billing TO app_user;"),
        "Non-owner schema grant must still be emitted, got: {script}"
    );
    assert!(
        script.contains("GRANT INSERT, SELECT ON TABLE billing.invoice TO old_owner;"),
        "Former table owner must receive explicit TO grant, got: {script}"
    );
    assert!(
        script.contains("GRANT SELECT ON TABLE billing.invoice TO app_user;"),
        "Non-owner table grant must still be emitted, got: {script}"
    );
    assert!(
        !script.contains("TO new_owner"),
        "Current owner must not receive explicit grants, got: {script}"
    );
}

#[tokio::test]
async fn compare_grants_owner_excluded_nonowner_still_diffed() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_routine = Routine::new(
        "public".to_string(),
        Oid(50),
        "process".to_string(),
        "plpgsql".to_string(),
        "function".to_string(),
        "void".to_string(),
        "".to_string(),
        None,
        None,
        "BEGIN END".to_string(),
    );
    from_routine.owner = "the_owner".to_string();
    from_routine.acl = vec![
        "the_owner=X/the_owner".to_string(),
        "old_app=X/the_owner".to_string(),
    ];

    let mut to_routine = Routine::new(
        "public".to_string(),
        Oid(50),
        "process".to_string(),
        "plpgsql".to_string(),
        "function".to_string(),
        "void".to_string(),
        "".to_string(),
        None,
        None,
        "BEGIN END".to_string(),
    );
    to_routine.owner = "the_owner".to_string();
    to_routine.acl = vec![
        "the_owner=X/the_owner".to_string(),
        "new_app=X/the_owner".to_string(),
    ];

    from_dump.routines.push(from_routine);
    to_dump.routines.push(to_routine);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Full);
    comparer.compare_grants().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("GRANT EXECUTE ON FUNCTION public.process() TO new_app;"),
        "Must grant to non-owner, got: {script}"
    );
    assert!(
        script.contains("REVOKE EXECUTE ON FUNCTION public.process() FROM old_app;"),
        "Must revoke from non-owner, got: {script}"
    );
    assert!(
        !script.contains("the_owner"),
        "Must not reference owner in grants/revokes, got: {script}"
    );
}

/// A table tracked in `recreated_tables` (e.g. due to partition key change)
/// must use the FROM default privilege ACL as its effective from_acl in full
/// grants mode, so that no spurious REVOKEs appear on repeated runs.
#[tokio::test]
async fn compare_grants_recreated_table_uses_default_acl() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    // Both FROM and TO have the same table with the same ACL.
    let mut from_table = Table::new(
        "public".to_string(),
        "orders".to_string(),
        "public".to_string(),
        "orders".to_string(),
        "owner".to_string(),
        None,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        None,
    );
    from_table.acl = vec!["reader=r/owner".to_string()];

    let mut to_table = Table::new(
        "public".to_string(),
        "orders".to_string(),
        "public".to_string(),
        "orders".to_string(),
        "owner".to_string(),
        None,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        None,
    );
    to_table.acl = vec!["reader=r/owner".to_string()];

    // Default privilege that auto-grants SELECT to reader on new tables.
    from_dump.default_privileges.push(DefaultPrivilege {
        role_name: "owner".to_string(),
        schema_name: "public".to_string(),
        object_type: "r".to_string(),
        acl: vec!["reader=r/owner".to_string()],
        hash: None,
    });

    from_dump.tables.push(from_table);
    to_dump.tables.push(to_table);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Full);
    // Simulate that the table was recreated (e.g. partition key change).
    comparer
        .recreated_tables
        .insert(Comparer::table_key("public", "orders"));
    comparer.compare_grants().await.unwrap();
    let script = comparer.get_script();

    // The default privilege matches the TO ACL, so no GRANT or REVOKE needed.
    let has_grant = script
        .lines()
        .any(|l| l.trim_start().to_lowercase().starts_with("grant "));
    let has_revoke = script
        .lines()
        .any(|l| l.trim_start().to_lowercase().starts_with("revoke "));
    assert!(
        !has_grant && !has_revoke,
        "Recreated table with matching default ACL must produce no GRANT/REVOKE, got: {script}"
    );
}

/// A recreated table whose TO ACL differs from the default privilege ACL
/// must produce the correct GRANT to bridge the gap.
#[tokio::test]
async fn compare_grants_recreated_table_grants_extra_over_default() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_table = Table::new(
        "public".to_string(),
        "orders".to_string(),
        "public".to_string(),
        "orders".to_string(),
        "owner".to_string(),
        None,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        None,
    );
    from_table.acl = vec!["reader=r/owner".to_string(), "writer=rw/owner".to_string()];

    let mut to_table = Table::new(
        "public".to_string(),
        "orders".to_string(),
        "public".to_string(),
        "orders".to_string(),
        "owner".to_string(),
        None,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        None,
    );
    to_table.acl = vec!["reader=r/owner".to_string(), "writer=rw/owner".to_string()];

    // Default privilege only grants SELECT to reader (no writer grant).
    from_dump.default_privileges.push(DefaultPrivilege {
        role_name: "owner".to_string(),
        schema_name: "public".to_string(),
        object_type: "r".to_string(),
        acl: vec!["reader=r/owner".to_string()],
        hash: None,
    });

    from_dump.tables.push(from_table);
    to_dump.tables.push(to_table);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Full);
    comparer
        .recreated_tables
        .insert(Comparer::table_key("public", "orders"));
    comparer.compare_grants().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("GRANT SELECT, UPDATE ON TABLE public.orders TO writer;"),
        "Must grant writer privileges beyond default ACL, got: {script}"
    );
    // reader already has SELECT via default, so no GRANT for reader.
    let reader_grant = script.lines().any(|l| l.contains("TO reader"));
    assert!(
        !reader_grant,
        "reader grant already covered by default ACL, got: {script}"
    );
}

/// A non-recreated table that exists in both FROM and TO must use the
/// original FROM ACL, not the default privilege ACL.
#[tokio::test]
async fn compare_grants_non_recreated_table_uses_from_acl() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_table = Table::new(
        "public".to_string(),
        "orders".to_string(),
        "public".to_string(),
        "orders".to_string(),
        "owner".to_string(),
        None,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        None,
    );
    from_table.acl = vec!["reader=r/owner".to_string()];

    let mut to_table = Table::new(
        "public".to_string(),
        "orders".to_string(),
        "public".to_string(),
        "orders".to_string(),
        "owner".to_string(),
        None,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        None,
    );
    to_table.acl = vec!["reader=r/owner".to_string()];

    // Even though default privilege differs, we must use FROM ACL.
    from_dump.default_privileges.push(DefaultPrivilege {
        role_name: "owner".to_string(),
        schema_name: "public".to_string(),
        object_type: "r".to_string(),
        acl: vec![],
        hash: None,
    });

    from_dump.tables.push(from_table);
    to_dump.tables.push(to_table);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Full);
    // Do NOT insert into recreated_tables — table is not recreated.
    comparer.compare_grants().await.unwrap();
    let script = comparer.get_script();

    // FROM and TO ACLs match, so no diff should be produced.
    let has_grant = script
        .lines()
        .any(|l| l.trim_start().to_lowercase().starts_with("grant "));
    let has_revoke = script
        .lines()
        .any(|l| l.trim_start().to_lowercase().starts_with("revoke "));
    assert!(
        !has_grant && !has_revoke,
        "Non-recreated table with identical ACLs must produce no GRANT/REVOKE, got: {script}"
    );
}

/// A dropped+recreated view (use_drop=true) in full grants mode must use
/// the default privilege ACL as the effective from_acl, matching the table
/// recreated-object logic.
#[tokio::test]
async fn compare_grants_dropped_view_uses_default_acl_full_mode() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_view = View::new(
        "my_view".to_string(),
        "SELECT 1".to_string(),
        "public".to_string(),
        Vec::new(),
    );
    from_view.acl = vec!["reader=r/owner".to_string()];

    let mut to_view = View::new(
        "my_view".to_string(),
        "SELECT 1".to_string(),
        "public".to_string(),
        Vec::new(),
    );
    to_view.acl = vec!["reader=r/owner".to_string()];

    // Default privilege auto-grants SELECT to reader on new tables/views.
    from_dump.default_privileges.push(DefaultPrivilege {
        role_name: "owner".to_string(),
        schema_name: "public".to_string(),
        object_type: "r".to_string(),
        acl: vec!["reader=r/owner".to_string()],
        hash: None,
    });

    from_dump.views.push(from_view);
    to_dump.views.push(to_view);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Full);
    // Simulate that the view was actually dropped (use_drop=true).
    comparer
        .dropped_views
        .insert(Comparer::normalized_view_key("public", "my_view"), true);
    comparer.compare_grants().await.unwrap();
    let script = comparer.get_script();

    // Default ACL matches TO ACL, so no diff needed.
    let has_grant = script
        .lines()
        .any(|l| l.trim_start().to_lowercase().starts_with("grant "));
    let has_revoke = script
        .lines()
        .any(|l| l.trim_start().to_lowercase().starts_with("revoke "));
    assert!(
        !has_grant && !has_revoke,
        "Dropped view with matching default ACL must produce no GRANT/REVOKE, got: {script}"
    );
}

/// A dropped view (use_drop=true) in full mode whose TO ACL has more
/// privileges than the default must produce GRANTs to bridge the gap.
#[tokio::test]
async fn compare_grants_dropped_view_grants_extra_over_default() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_view = View::new(
        "my_view".to_string(),
        "SELECT 1".to_string(),
        "public".to_string(),
        Vec::new(),
    );
    from_view.acl = vec!["reader=r/owner".to_string(), "writer=rw/owner".to_string()];

    let mut to_view = View::new(
        "my_view".to_string(),
        "SELECT 1".to_string(),
        "public".to_string(),
        Vec::new(),
    );
    to_view.acl = vec!["reader=r/owner".to_string(), "writer=rw/owner".to_string()];

    // Default only gives reader SELECT.
    from_dump.default_privileges.push(DefaultPrivilege {
        role_name: "owner".to_string(),
        schema_name: "public".to_string(),
        object_type: "r".to_string(),
        acl: vec!["reader=r/owner".to_string()],
        hash: None,
    });

    from_dump.views.push(from_view);
    to_dump.views.push(to_view);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Full);
    comparer
        .dropped_views
        .insert(Comparer::normalized_view_key("public", "my_view"), true);
    comparer.compare_grants().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("GRANT SELECT, UPDATE ON TABLE public.my_view TO writer;"),
        "Must grant writer privileges beyond default ACL for dropped view, got: {script}"
    );
}

/// When table ownership changes between FROM and TO, column-level ACL
/// diffing must keep former-owner entries diffable while suppressing
/// current-owner implicit privilege entries.
#[tokio::test]
async fn compare_column_grants_revokes_former_owner_and_excludes_current_owner() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    // FROM table owned by old_owner with column ACL for old_owner
    let mut from_col = int_column("public", "users", "secret", 1);
    from_col.acl = vec!["old_owner=r/old_owner".to_string()];
    let from_table = Table::new(
        "public".to_string(),
        "users".to_string(),
        "public".to_string(),
        "users".to_string(),
        "old_owner".to_string(),
        None,
        vec![from_col],
        Vec::new(),
        Vec::new(),
        Vec::new(),
        None,
    );

    // TO table owned by new_owner with column ACL for new_owner
    let mut to_col = int_column("public", "users", "secret", 1);
    to_col.acl = vec!["new_owner=r/new_owner".to_string()];
    let to_table = Table::new(
        "public".to_string(),
        "users".to_string(),
        "public".to_string(),
        "users".to_string(),
        "new_owner".to_string(),
        None,
        vec![to_col],
        Vec::new(),
        Vec::new(),
        Vec::new(),
        None,
    );

    from_dump.tables.push(from_table);
    to_dump.tables.push(to_table);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Full);
    comparer.compare_grants().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("REVOKE SELECT (secret) ON TABLE public.users FROM old_owner;"),
        "Former owner column ACL must remain diffable in full mode, got: {script}"
    );
    assert!(
        !script.contains("new_owner"),
        "Current owner column ACL entries must be suppressed, got: {script}"
    );
    assert!(
        !script
            .lines()
            .any(|l| l.contains("secret") && l.trim_start().to_lowercase().starts_with("grant ")),
        "Unexpected column GRANT for owner ACL entries, got: {script}"
    );
}

/// Counterpart to `compare_column_grants_revokes_former_owner_and_excludes_current_owner`:
/// when ownership changes and the new TO has *no* explicit column ACL at all
/// (only the implicit owner privileges), a former owner's column grant in
/// FROM must still be revoked under `full` mode. Without this we would leak
/// the old owner's column-level access into the post-migration database.
#[tokio::test]
async fn compare_column_grants_revokes_former_owner_when_to_has_no_column_acl() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_col = int_column("public", "users", "secret", 1);
    from_col.acl = vec!["old_owner=r/old_owner".to_string()];
    let from_table = Table::new(
        "public".to_string(),
        "users".to_string(),
        "public".to_string(),
        "users".to_string(),
        "old_owner".to_string(),
        None,
        vec![from_col],
        Vec::new(),
        Vec::new(),
        Vec::new(),
        None,
    );

    // No column ACL in TO.
    let to_col = int_column("public", "users", "secret", 1);
    let to_table = Table::new(
        "public".to_string(),
        "users".to_string(),
        "public".to_string(),
        "users".to_string(),
        "new_owner".to_string(),
        None,
        vec![to_col],
        Vec::new(),
        Vec::new(),
        Vec::new(),
        None,
    );

    from_dump.tables.push(from_table);
    to_dump.tables.push(to_table);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Full);
    comparer.compare_grants().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("REVOKE SELECT (secret) ON TABLE public.users FROM old_owner;"),
        "Former owner column ACL must be revoked even without ACL in TO, got: {script}"
    );
    assert!(
        !script.contains("new_owner"),
        "Current owner must never appear in column grant output, got: {script}"
    );
}

/// Regression test for the per-table column-ACL HashMap rewrite. Previously
/// each TO column did a linear scan over `from_cols`; the rewrite indexes
/// `from_cols` by name once per table. This test exercises a table with
/// multiple columns where each column's effective `from_acl` differs, to
/// catch off-by-one mistakes that a single-column test would miss.
#[tokio::test]
async fn compare_column_grants_dispatches_per_column_acl_correctly() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    // FROM: three columns with distinct ACL states.
    let mut from_a = int_column("public", "t", "a", 1);
    from_a.acl = vec!["reader=r/owner".to_string()];
    let mut from_b = int_column("public", "t", "b", 2);
    from_b.acl = vec!["reader=r/owner".to_string()];
    let from_c = int_column("public", "t", "c", 3); // no ACL in FROM

    let from_table = Table::new(
        "public".to_string(),
        "t".to_string(),
        "public".to_string(),
        "t".to_string(),
        "owner".to_string(),
        None,
        vec![from_a, from_b, from_c],
        Vec::new(),
        Vec::new(),
        Vec::new(),
        None,
    );

    // TO: a kept, b loses its grant, c gains a grant.
    let mut to_a = int_column("public", "t", "a", 1);
    to_a.acl = vec!["reader=r/owner".to_string()];
    let to_b = int_column("public", "t", "b", 2); // grant should be revoked
    let mut to_c = int_column("public", "t", "c", 3);
    to_c.acl = vec!["writer=a/owner".to_string()]; // INSERT grant added

    let to_table = Table::new(
        "public".to_string(),
        "t".to_string(),
        "public".to_string(),
        "t".to_string(),
        "owner".to_string(),
        None,
        vec![to_a, to_b, to_c],
        Vec::new(),
        Vec::new(),
        Vec::new(),
        None,
    );

    from_dump.tables.push(from_table);
    to_dump.tables.push(to_table);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Full);
    comparer.compare_grants().await.unwrap();
    let script = comparer.get_script();

    // a: identical → nothing emitted for column a.
    assert!(
        !script.contains("(a)"),
        "column a is unchanged and must not appear, got: {script}"
    );
    // b: REVOKE for the dropped grant.
    assert!(
        script.contains("REVOKE SELECT (b) ON TABLE public.t FROM reader;"),
        "expected REVOKE for column b, got: {script}"
    );
    // c: GRANT for the added INSERT privilege.
    assert!(
        script.contains("GRANT INSERT (c) ON TABLE public.t TO writer;"),
        "expected GRANT INSERT on column c, got: {script}"
    );
    // Sanity: no cross-talk where column b's REVOKE refers to writer/c, etc.
    assert!(
        !script.contains("REVOKE SELECT (c)"),
        "column c had no FROM grant and must not be revoked, got: {script}"
    );
    assert!(
        !script.contains("GRANT INSERT (a)") && !script.contains("GRANT INSERT (b)"),
        "INSERT grant must be scoped to column c only, got: {script}"
    );
}
