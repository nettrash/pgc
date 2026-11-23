//! Tests for `compare_grants` and `compare_column_grants` across all three
//! [`GrantsMode`] settings (`ignore`, `addonly`, `full`), including owner
//! changes and the default-ACL handling for recreated or dropped objects.

use super::helpers::*;
use crate::comparer::core::*;
use crate::config::dump_config::DumpConfig;
use crate::config::grants_mode::GrantsMode;
use crate::dump::default_privilege::DefaultPrivilege;
use crate::dump::foreign_table::ForeignTable;
use crate::dump::pg_type::PgType;
use crate::dump::routine::Routine;
use crate::dump::schema::Schema;
use crate::dump::sequence::Sequence;
use crate::dump::table::Table;
use crate::dump::table_column::TableColumn;
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

    // use_drop: without it the DROP TABLE is commented out and the table
    // keeps its privileges.
    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Full);
    // Simulate that the table was recreated (e.g. partition key change): the
    // table section lists it in both sets.
    comparer
        .recreated_tables
        .insert(Comparer::table_key("public", "orders"));
    comparer
        .dropped_and_recreated_tables
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

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Full);
    comparer
        .recreated_tables
        .insert(Comparer::table_key("public", "orders"));
    comparer
        .dropped_and_recreated_tables
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

// --- Entries granted by a role other than the owner (fix:grants) ---

fn owned_table(name: &str, acl: &[&str], columns: Vec<TableColumn>) -> Table {
    let mut table = Table::new(
        "app".to_string(),
        name.to_string(),
        "app".to_string(),
        name.to_string(),
        "owner".to_string(),
        None,
        columns,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        None,
    );
    table.acl = acl.iter().map(|s| s.to_string()).collect();
    table
}

fn column_with_acl(name: &str, ordinal: i32, acl: &[&str]) -> TableColumn {
    let mut column = int_column("app", "t", name, ordinal);
    column.acl = acl.iter().map(|s| s.to_string()).collect();
    column
}

fn owned_schema(acl: &[&str]) -> Schema {
    let mut schema = Schema::new("app".to_string(), "app".to_string(), None);
    schema.owner = "owner".to_string();
    schema.acl = acl.iter().map(|s| s.to_string()).collect();
    schema
}

fn owned_function(acl: &[&str]) -> Routine {
    let mut routine = Routine::new(
        "app".to_string(),
        Oid(7),
        "f".to_string(),
        "sql".to_string(),
        "function".to_string(),
        "integer".to_string(),
        "".to_string(),
        None,
        None,
        "SELECT 1".to_string(),
    );
    routine.owner = "owner".to_string();
    routine.acl = acl.iter().map(|s| s.to_string()).collect();
    routine
}

/// FROM: a delegate holding grant options passed SELECT on app.t, SELECT on
/// its note column, USAGE on schema app and EXECUTE on app.f() to other
/// roles; TO keeps the delegate's options and drops what it granted, and adds
/// an owner-level schema grant.
fn delegated_dumps(from_version: Option<i32>) -> (Dump, Dump) {
    let mut from = Dump::new(DumpConfig::default());
    let mut to = Dump::new(DumpConfig::default());
    from.server_version_num = from_version;
    to.server_version_num = Some(190000);

    from.schemas
        .push(owned_schema(&["delegate=U*/owner", "reader=U/delegate"]));
    to.schemas
        .push(owned_schema(&["delegate=U*/owner", "other=U/owner"]));

    from.tables.push(owned_table(
        "t",
        &["delegate=r*/owner", "reader=r/delegate"],
        vec![column_with_acl("note", 1, &["writer=r/delegate"])],
    ));
    to.tables.push(owned_table(
        "t",
        &["delegate=r*/owner"],
        vec![column_with_acl("note", 1, &[])],
    ));

    from.routines
        .push(owned_function(&["delegate=X*/owner", "reader=X/delegate"]));
    to.routines.push(owned_function(&["delegate=X*/owner"]));
    (from, to)
}

async fn full_grants_script(from: Dump, to: Dump, use_comments: bool) -> String {
    let mut comparer = Comparer::new(from, to, false, false, use_comments, GrantsMode::Full);
    comparer.compare_grants().await.unwrap();
    comparer.get_script()
}

fn position(script: &str, needle: &str) -> usize {
    script
        .find(needle)
        .unwrap_or_else(|| panic!("missing {needle:?} in:\n{script}"))
}

#[tokio::test]
async fn compare_grants_revokes_delegated_entries_granted_by_on_pg19() {
    let (from, to) = delegated_dumps(Some(190000));
    let script = full_grants_script(from, to, true).await;

    let start = position(&script, "/* ---> Grants: Start section");
    let column = position(
        &script,
        "/* Grantor revokes for column: app.t.note */\n\
         REVOKE SELECT (note) ON TABLE app.t FROM writer GRANTED BY delegate;\n\n",
    );
    let table = position(
        &script,
        "/* Grantor revokes for table: app.t */\n\
         REVOKE SELECT ON TABLE app.t FROM reader GRANTED BY delegate;\n\n",
    );
    let function = position(
        &script,
        "/* Grantor revokes for function: app.f() */\n\
         REVOKE EXECUTE ON FUNCTION app.f() FROM reader GRANTED BY delegate;\n\n",
    );
    let schema = position(
        &script,
        "/* Grantor revokes for schema: app */\n\
         REVOKE USAGE ON SCHEMA app FROM reader GRANTED BY delegate;\n\n",
    );
    let owner_level = position(
        &script,
        "/* Grants for schema: app */\nGRANT USAGE ON SCHEMA app TO other;\n\n",
    );
    // Columns first, schemas last, all before any owner-level statement.
    assert!(start < column && column < table && table < function && function < schema);
    assert!(schema < owner_level);
    assert!(!script.contains("SET ROLE"), "got: {script}");
    // No plain (ineffective) REVOKE is left behind for the delegated entries.
    assert!(!script.contains("FROM reader;"), "got: {script}");
    assert!(!script.contains("FROM writer;"), "got: {script}");
}

#[tokio::test]
async fn compare_grants_revokes_delegated_entries_with_set_role_before_pg19() {
    for from_version in [None, Some(140000), Some(180000)] {
        let (from, to) = delegated_dumps(from_version);
        let script = full_grants_script(from, to, false).await;
        assert!(
            script.contains(
                "SET ROLE delegate;\n\n\
                 REVOKE SELECT (note) ON TABLE app.t FROM writer;\n\n\
                 RESET ROLE;\n\n\
                 SET ROLE delegate;\n\n\
                 REVOKE SELECT ON TABLE app.t FROM reader;\n\n\
                 RESET ROLE;\n\n\
                 SET ROLE delegate;\n\n\
                 REVOKE EXECUTE ON FUNCTION app.f() FROM reader;\n\n\
                 RESET ROLE;\n\n\
                 SET ROLE delegate;\n\n\
                 REVOKE USAGE ON SCHEMA app FROM reader;\n\n\
                 RESET ROLE;\n\n\
                 GRANT USAGE ON SCHEMA app TO other;"
            ),
            "{from_version:?}: got: {script}"
        );
        assert!(
            !script.contains("GRANTED BY"),
            "{from_version:?}: got: {script}"
        );
    }
}

#[tokio::test]
async fn compare_grants_owner_granted_entries_do_not_depend_on_the_server_version() {
    // Only owner-granted entries: the output is the pre-1.1 one whatever the
    // migrated server, byte for byte.
    let build = |version: Option<i32>| {
        let mut from = Dump::new(DumpConfig::default());
        let mut to = Dump::new(DumpConfig::default());
        from.server_version_num = version;
        from.schemas.push(owned_schema(&["reader=U*/owner"]));
        to.schemas.push(owned_schema(&["writer=U/owner"]));
        from.tables.push(owned_table(
            "t",
            &["reader=r*w/owner"],
            vec![column_with_acl("note", 1, &["reader=r/owner"])],
        ));
        to.tables.push(owned_table(
            "t",
            &["reader=r/owner"],
            vec![column_with_acl("note", 1, &[])],
        ));
        (from, to)
    };
    let (from, to) = build(Some(190000));
    let pg19 = full_grants_script(from, to, true).await;
    let (from, to) = build(None);
    let unknown = full_grants_script(from, to, true).await;
    assert_eq!(pg19, unknown);
    assert!(!pg19.contains("GRANTED BY") && !pg19.contains("SET ROLE"));
    assert!(pg19.contains(
        "/* Grants for schema: app */\n\
         REVOKE GRANT OPTION FOR USAGE ON SCHEMA app FROM reader;\n\n\
         REVOKE USAGE ON SCHEMA app FROM reader;\n\n\
         GRANT USAGE ON SCHEMA app TO writer;\n\n"
    ));
    assert!(pg19.contains(
        "/* Grants for table: app.t */\n\
         REVOKE GRANT OPTION FOR SELECT ON TABLE app.t FROM reader;\n\n\
         REVOKE UPDATE ON TABLE app.t FROM reader;\n\n"
    ));
    assert!(pg19.contains(
        "/* Column grants for app.t.note */\nREVOKE SELECT (note) ON TABLE app.t FROM reader;\n\n"
    ));
}

#[tokio::test]
async fn compare_grants_grant_option_downgrade_revokes_dependents_first() {
    let mut from = Dump::new(DumpConfig::default());
    let mut to = Dump::new(DumpConfig::default());
    from.server_version_num = Some(190000);
    from.tables.push(owned_table(
        "t",
        &["delegate=r*/owner", "sub=r*/delegate", "reader=r/sub"],
        Vec::new(),
    ));
    to.tables.push(owned_table(
        "t",
        &["delegate=r/owner", "reader=r/owner"],
        Vec::new(),
    ));
    let script = full_grants_script(from, to, false).await;
    let leaf = position(
        &script,
        "REVOKE SELECT ON TABLE app.t FROM reader GRANTED BY sub;",
    );
    let middle = position(
        &script,
        "REVOKE SELECT ON TABLE app.t FROM sub GRANTED BY delegate;",
    );
    let option = position(
        &script,
        "REVOKE GRANT OPTION FOR SELECT ON TABLE app.t FROM delegate;",
    );
    let regrant = position(&script, "GRANT SELECT ON TABLE app.t TO reader;");
    assert!(
        leaf < middle && middle < option && option < regrant,
        "got: {script}"
    );
}

#[tokio::test]
async fn compare_grants_recreated_table_has_no_grantor_revokes() {
    // A recreated table starts from its default ACL, so the FROM entries the
    // delegate granted are gone with the old table.
    let (from, to) = delegated_dumps(Some(190000));
    let mut comparer = Comparer::new(from, to, true, false, false, GrantsMode::Full);
    comparer
        .recreated_tables
        .insert(Comparer::table_key("app", "t"));
    comparer
        .dropped_and_recreated_tables
        .insert(Comparer::table_key("app", "t"));
    comparer.compare_grants().await.unwrap();
    let script = comparer.get_script();
    assert!(
        !script
            .lines()
            .any(|l| l.contains("app.t") && l.contains("GRANTED BY")),
        "got: {script}"
    );
    assert!(
        !script.contains("ON TABLE app.t FROM reader"),
        "got: {script}"
    );
    assert!(
        script.contains("REVOKE EXECUTE ON FUNCTION app.f() FROM reader GRANTED BY delegate;"),
        "got: {script}"
    );
}

#[tokio::test]
async fn compare_grants_addonly_ignores_delegated_entries() {
    let (from, to) = delegated_dumps(Some(190000));
    let mut comparer = Comparer::new(from, to, false, false, false, GrantsMode::AddOnly);
    comparer.compare_grants().await.unwrap();
    let script = comparer.get_script();
    assert!(!script.contains("REVOKE"), "got: {script}");
    assert!(
        script.contains("GRANT USAGE ON SCHEMA app TO other;"),
        "got: {script}"
    );
}

// --- Review fixes (fix:grants): replay guards and table revokes on columns ---

async fn production_grants_script(from: Dump, to: Dump) -> String {
    let mut comparer = Comparer::new(from, to, false, false, false, GrantsMode::Full);
    comparer.set_output_for_production(true);
    comparer.compare_grants().await.unwrap();
    comparer.get_script()
}

#[tokio::test]
async fn compare_grants_production_guards_granted_by_revokes_for_replay() {
    // A grantor that no longer holds any privilege on the object makes a
    // replayed REVOKE fail with "permission denied", so production output
    // runs each grantor-named revoke only while its entry still exists.
    let (from, to) = delegated_dumps(Some(190000));
    let script = production_grants_script(from, to).await;
    assert!(
        script.contains(
            "DO $pgc$\n\
             BEGIN\n    \
             IF EXISTS (SELECT 1 FROM pg_catalog.aclexplode((SELECT c.relacl FROM pg_catalog.pg_class c WHERE c.oid = pg_catalog.to_regclass('app.t'))) AS a\n               \
             WHERE pg_catalog.pg_get_userbyid(a.grantor) = 'delegate'\n                 \
             AND pg_catalog.pg_get_userbyid(a.grantee) = 'reader'\n                 \
             AND a.privilege_type IN ('SELECT')) THEN\n        \
             REVOKE SELECT ON TABLE app.t FROM reader GRANTED BY delegate;\n    \
             END IF;\n\
             END\n\
             $pgc$;\n\n"
        ),
        "got: {script}"
    );
    for lookup in [
        "(SELECT c.attacl FROM pg_catalog.pg_attribute c WHERE c.attrelid = pg_catalog.to_regclass('app.t') AND pg_catalog.quote_ident(c.attname) = 'note')",
        "(SELECT p.proacl FROM pg_catalog.pg_proc p WHERE p.pronamespace = pg_catalog.to_regnamespace('app') AND pg_catalog.format('%I(%s)', p.proname, pg_catalog.pg_get_function_identity_arguments(p.oid)) = 'f()')",
        "(SELECT n.nspacl FROM pg_catalog.pg_namespace n WHERE n.oid = pg_catalog.to_regnamespace('app'))",
    ] {
        assert!(script.contains(lookup), "missing {lookup}; got: {script}");
    }
    // Every grantor-named revoke is guarded; none is left bare.
    assert!(
        !script
            .lines()
            .any(|l| l.starts_with("REVOKE") && l.contains("GRANTED BY")),
        "got: {script}"
    );
}

#[tokio::test]
async fn compare_grants_production_guards_set_role_revokes_for_replay() {
    let (from, to) = delegated_dumps(None);
    let script = production_grants_script(from, to).await;
    assert!(
        script.contains(
            "AND a.privilege_type IN ('SELECT')) THEN\n        \
             SET ROLE delegate;\n        \
             REVOKE SELECT ON TABLE app.t FROM reader;\n        \
             RESET ROLE;\n    \
             END IF;\n"
        ),
        "got: {script}"
    );
    // The role switch lives inside the guarded block, so a failing REVOKE
    // cannot leave the session running as the grantor.
    assert!(
        !script.lines().any(|l| l.starts_with("SET ROLE")),
        "got: {script}"
    );
    assert!(!script.contains("GRANTED BY"), "got: {script}");
}

#[tokio::test]
async fn compare_grants_default_output_has_no_replay_guard() {
    let (from, to) = delegated_dumps(Some(190000));
    let script = full_grants_script(from, to, false).await;
    assert!(!script.contains("DO $"), "got: {script}");
}

#[tokio::test]
async fn compare_grants_regrants_column_privileges_a_table_revoke_strips() {
    // PostgreSQL applies a table-level REVOKE to every column's ACL as well,
    // removing the column entries the same grantor gave the same grantee.
    // TO keeps reader's column grant, so it must come back in the same run.
    for (label, from_table, from_note) in [
        ("owner", vec!["reader=r/owner"], vec!["reader=r/owner"]),
        (
            "delegate",
            vec!["delegate=r*/owner", "reader=r/delegate"],
            vec!["reader=r/delegate"],
        ),
    ] {
        let mut from = Dump::new(DumpConfig::default());
        let mut to = Dump::new(DumpConfig::default());
        from.server_version_num = Some(190000);
        let to_table: Vec<&str> = from_table
            .iter()
            .copied()
            .filter(|i| i.starts_with("delegate"))
            .collect();
        from.tables.push(owned_table(
            "t",
            &from_table,
            vec![column_with_acl("note", 1, &from_note)],
        ));
        to.tables.push(owned_table(
            "t",
            &to_table,
            vec![column_with_acl("note", 1, &["reader=r/owner"])],
        ));
        let script = full_grants_script(from, to, false).await;
        let revoke = position(&script, "REVOKE SELECT ON TABLE app.t FROM reader");
        let regrant = position(&script, "GRANT SELECT (note) ON TABLE app.t TO reader;");
        assert!(revoke < regrant, "{label}: got: {script}");
    }
}

#[tokio::test]
async fn compare_grants_table_option_revoke_clears_column_dependents_first() {
    // Revoking the delegate's table-level grant option also takes its
    // column-level option (granted by the owner); the column entry it granted
    // under that option would make the table statement fail with "dependent
    // privileges exist", so it goes first, and what TO keeps comes back.
    let mut from = Dump::new(DumpConfig::default());
    let mut to = Dump::new(DumpConfig::default());
    from.server_version_num = Some(190000);
    from.tables.push(owned_table(
        "t",
        &["delegate=r*/owner"],
        vec![column_with_acl(
            "note",
            1,
            &["delegate=r*/owner", "reader=r/delegate"],
        )],
    ));
    to.tables.push(owned_table(
        "t",
        &["delegate=r/owner"],
        vec![column_with_acl(
            "note",
            1,
            &["delegate=r*/owner", "reader=r/owner"],
        )],
    ));
    let script = full_grants_script(from, to, false).await;
    let dependent = position(
        &script,
        "REVOKE SELECT (note) ON TABLE app.t FROM reader GRANTED BY delegate;",
    );
    let option = position(
        &script,
        "REVOKE GRANT OPTION FOR SELECT ON TABLE app.t FROM delegate;",
    );
    let reader = position(&script, "GRANT SELECT (note) ON TABLE app.t TO reader;");
    let delegate = position(
        &script,
        "GRANT SELECT (note) ON TABLE app.t TO delegate WITH GRANT OPTION;",
    );
    assert!(
        dependent < option && option < reader && option < delegate,
        "got: {script}"
    );
}

// --- Grants planned against the state the table DDL leaves (reg-misc) ---

/// `app.<table>` owned by `owner`, with `columns`, hashed so that
/// `compare_tables` diffs it; `partition` makes it a partitioned parent
/// (`None`) or a partition of `app.<parent>` (`Some(parent)`).
fn hashed_table(
    table: &str,
    partition: Option<Option<&str>>,
    acl: &[&str],
    columns: Vec<TableColumn>,
) -> Table {
    let mut t = owned_table(table, acl, columns);
    match partition {
        Some(Some(parent)) => {
            t.partition_of = Some(format!("app.{parent}"));
            t.partition_bound = Some("FOR VALUES FROM (0) TO (100)".to_string());
        }
        Some(None) => t.partition_key = Some("RANGE (id)".to_string()),
        None => {}
    }
    t.hash();
    t
}

/// Column `name` of `app.<table>`: `generated` is `None` for a plain
/// integer column, or the STORED generation expression.
fn table_column(
    table: &str,
    name: &str,
    ordinal: i32,
    data_type: &str,
    generated: Option<&str>,
    acl: &[&str],
) -> TableColumn {
    let mut column = int_column("app", table, name, ordinal);
    column.data_type = data_type.to_string();
    if let Some(expression) = generated {
        column.is_generated = "ALWAYS".to_string();
        column.generation_expression = Some(expression.to_string());
        column.generation_type = Some("s".to_string());
    }
    column.acl = acl.iter().map(|s| s.to_string()).collect();
    column
}

async fn compare_all(from: Dump, to: Dump, use_drop: bool) -> (Comparer, String) {
    let mut comparer = Comparer::new(from, to, use_drop, false, false, GrantsMode::Full);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();
    (comparer, script)
}

/// FROM: app.t.g is a plain column that reader (from the owner), delegate
/// (WITH GRANT OPTION) and sub (from the delegate) may read. TO: g is
/// generated, which only a drop + add can make, and sub's grant is gone.
fn readded_column_dumps() -> (Dump, Dump) {
    let mut from = Dump::new(DumpConfig::default());
    let mut to = Dump::new(DumpConfig::default());
    from.server_version_num = Some(190000);
    from.tables.push(hashed_table(
        "t",
        None,
        &[],
        vec![
            table_column("t", "id", 1, "integer", None, &[]),
            table_column(
                "t",
                "g",
                2,
                "integer",
                None,
                &["reader=r/owner", "delegate=r*/owner", "sub=r/delegate"],
            ),
        ],
    ));
    to.tables.push(hashed_table(
        "t",
        None,
        &[],
        vec![
            table_column("t", "id", 1, "integer", None, &[]),
            table_column(
                "t",
                "g",
                2,
                "integer",
                Some("(id * 2)"),
                &["reader=r/owner", "delegate=r*/owner"],
            ),
        ],
    ));
    (from, to)
}

#[tokio::test]
async fn compare_grants_readded_column_is_planned_from_an_empty_acl() {
    // The drop + add leaves g without privileges: what TO has is granted
    // again, and sub's entry, gone with the old column, is not revoked —
    // `REVOKE ... GRANTED BY delegate` would fail with "permission denied
    // for column".
    let (from, to) = readded_column_dumps();
    let (comparer, script) = compare_all(from, to, true).await;
    assert!(comparer.readded_columns.contains(&(
        "app".to_string(),
        "t".to_string(),
        "g".to_string()
    )));
    let drop = position(&script, "alter table app.t drop column g;");
    let reader = position(&script, "GRANT SELECT (g) ON TABLE app.t TO reader;");
    let delegate = position(
        &script,
        "GRANT SELECT (g) ON TABLE app.t TO delegate WITH GRANT OPTION;",
    );
    assert!(drop < reader && drop < delegate, "got: {script}");
    assert!(!script.contains("REVOKE"), "got: {script}");
    assert!(!script.contains("GRANTED BY"), "got: {script}");
}

#[tokio::test]
async fn compare_grants_column_kept_without_use_drop_keeps_its_acl() {
    // Without use_drop the drop + add is commented out, so g keeps its FROM
    // privileges and sub's entry is revoked in the delegate's name.
    let (from, to) = readded_column_dumps();
    let (comparer, script) = compare_all(from, to, false).await;
    assert!(comparer.readded_columns.is_empty());
    assert!(
        script.contains("REVOKE SELECT (g) ON TABLE app.t FROM sub GRANTED BY delegate;"),
        "got: {script}"
    );
    assert!(!script.contains("GRANT SELECT (g)"), "got: {script}");
}

#[tokio::test]
async fn compare_grants_recreated_table_columns_are_planned_from_an_empty_acl() {
    // A table whose DROP TABLE runs comes back without column privileges.
    let (from, to) = delegated_dumps(Some(190000));
    let mut from = from;
    let mut to = to;
    from.tables[0].columns[0].acl = vec!["reader=r/owner".into(), "writer=r/delegate".into()];
    to.tables[0].columns[0].acl = vec!["reader=r/owner".into()];
    for use_drop in [true, false] {
        let mut comparer = Comparer::new(
            from.clone(),
            to.clone(),
            use_drop,
            false,
            false,
            GrantsMode::Full,
        );
        for set in [
            &mut comparer.recreated_tables,
            &mut comparer.dropped_and_recreated_tables,
        ] {
            set.insert(Comparer::table_key("app", "t"));
        }
        comparer.compare_grants().await.unwrap();
        let script = comparer.get_script();
        if use_drop {
            assert!(
                script.contains("GRANT SELECT (note) ON TABLE app.t TO reader;"),
                "got: {script}"
            );
            assert!(
                !script.contains("(note) ON TABLE app.t FROM"),
                "got: {script}"
            );
        } else {
            // The DROP TABLE is only commented out: the FROM entries stay.
            assert!(
                script.contains(
                    "REVOKE SELECT (note) ON TABLE app.t FROM writer GRANTED BY delegate;"
                ),
                "got: {script}"
            );
            assert!(!script.contains("GRANT SELECT (note)"), "got: {script}");
        }
    }
}

#[tokio::test]
async fn compare_grants_partition_retyped_through_its_parent_keeps_its_acl() {
    // The parent's ALTER COLUMN ... TYPE reaches the partition, which is
    // never dropped: `recreated_tables` lists it, but its table and column
    // privileges are still the FROM ones and are diffed as such.
    let parent = |v_type: &str| {
        hashed_table(
            "p",
            Some(None),
            &[],
            vec![
                table_column("p", "id", 1, "integer", None, &[]),
                table_column("p", "v", 2, v_type, None, &[]),
            ],
        )
    };
    let child = |v_type: &str, acl: &[&str], v_acl: &[&str]| {
        hashed_table(
            "p1",
            Some(Some("p")),
            acl,
            vec![
                table_column("p1", "id", 1, "integer", None, &[]),
                table_column("p1", "v", 2, v_type, None, v_acl),
            ],
        )
    };
    let mut from = Dump::new(DumpConfig::default());
    let mut to = Dump::new(DumpConfig::default());
    from.server_version_num = Some(190000);
    from.tables = vec![
        parent("integer"),
        child(
            "integer",
            &["delegate=r*/owner", "reader=r/delegate", "other=r/owner"],
            &["colr=r/owner"],
        ),
    ];
    to.tables = vec![
        parent("bigint"),
        child("bigint", &["delegate=r*/owner"], &[]),
    ];
    let (comparer, script) = compare_all(from, to, true).await;
    let key = Comparer::table_key("app", "p1");
    assert!(comparer.recreated_tables.contains(&key));
    assert!(!comparer.dropped_and_recreated_tables.contains(&key));
    assert!(!script.contains("drop table"), "got: {script}");
    for expected in [
        "REVOKE SELECT ON TABLE app.p1 FROM reader GRANTED BY delegate;",
        "REVOKE SELECT ON TABLE app.p1 FROM other;",
        "REVOKE SELECT (v) ON TABLE app.p1 FROM colr;",
    ] {
        assert!(script.contains(expected), "{expected}: got: {script}");
    }
    assert!(
        !script.contains("GRANT SELECT ON TABLE app.p1"),
        "got: {script}"
    );
}

#[tokio::test]
async fn compare_grants_partition_column_readded_by_its_parent_is_planned_from_an_empty_acl() {
    // A type and expression change is a drop + add on the parent, which
    // recurses to the partition: the partition's column comes back without
    // privileges, although the partition itself is never dropped.
    let columns = |table: &str, g_type: &str, expression: &str, g_acl: &[&str]| {
        vec![
            table_column(table, "id", 1, "integer", None, &[]),
            table_column(table, "qty", 2, "integer", None, &[]),
            table_column(table, "g", 3, g_type, Some(expression), g_acl),
        ]
    };
    let mut from = Dump::new(DumpConfig::default());
    let mut to = Dump::new(DumpConfig::default());
    from.server_version_num = Some(190000);
    from.tables = vec![
        hashed_table(
            "gp",
            Some(None),
            &[],
            columns("gp", "integer", "(qty * 2)", &[]),
        ),
        hashed_table(
            "gp1",
            Some(Some("gp")),
            &[],
            columns(
                "gp1",
                "integer",
                "(qty * 2)",
                &["reader=r/owner", "delegate=r*/owner", "sub=r/delegate"],
            ),
        ),
    ];
    to.tables = vec![
        hashed_table(
            "gp",
            Some(None),
            &[],
            columns("gp", "bigint", "(qty * 3)", &[]),
        ),
        hashed_table(
            "gp1",
            Some(Some("gp")),
            &[],
            columns(
                "gp1",
                "bigint",
                "(qty * 3)",
                &["reader=r/owner", "delegate=r*/owner"],
            ),
        ),
    ];
    let (comparer, script) = compare_all(from, to, true).await;
    assert!(comparer.readded_columns.contains(&(
        "app".to_string(),
        "gp1".to_string(),
        "g".to_string()
    )));
    let drop = position(&script, "alter table app.gp drop column g;");
    let reader = position(&script, "GRANT SELECT (g) ON TABLE app.gp1 TO reader;");
    let delegate = position(
        &script,
        "GRANT SELECT (g) ON TABLE app.gp1 TO delegate WITH GRANT OPTION;",
    );
    assert!(drop < reader && drop < delegate, "got: {script}");
    assert!(!script.contains("REVOKE"), "got: {script}");
}

/// FROM: app.rb partitioned by `RANGE (id)`, readable by reader (table and
/// column `code`) and by other (table). TO: partitioned by `RANGE (k)` — a
/// drop + create — and other's grant is gone.
fn rebuilt_table_dumps() -> (Dump, Dump) {
    let rebuilt = |key: &str, acl: &[&str]| {
        let mut table = hashed_table(
            "rb",
            Some(None),
            acl,
            vec![
                table_column("rb", "id", 1, "integer", None, &[]),
                table_column("rb", "k", 2, "integer", None, &[]),
                table_column("rb", "code", 3, "text", None, &["reader=r/owner"]),
            ],
        );
        table.partition_key = Some(format!("RANGE ({key})"));
        table.hash();
        table
    };
    let mut from = Dump::new(DumpConfig::default());
    let mut to = Dump::new(DumpConfig::default());
    from.server_version_num = Some(190000);
    from.tables = vec![rebuilt("id", &["reader=r/owner", "other=r/owner"])];
    to.tables = vec![rebuilt("k", &["reader=r/owner"])];
    (from, to)
}

async fn compare_all_in(mode: GrantsMode, from: Dump, to: Dump, use_drop: bool) -> String {
    let mut comparer = Comparer::new(from, to, use_drop, false, false, mode);
    comparer.compare().await.unwrap();
    comparer.get_script()
}

#[tokio::test]
async fn compare_grants_addonly_regrants_a_recreated_table() {
    // Review finding: addonly diffed a table whose DROP TABLE runs against
    // its old ACL, so TO's table-level grants looked present and the
    // recreated table lost them (round 2 granted them). It re-issued the
    // column grants of the same table. A recreated table is now planned from
    // an empty ACL, like a new one.
    let (from, to) = rebuilt_table_dumps();
    let script = compare_all_in(GrantsMode::AddOnly, from, to, true).await;
    let create = position(&script, "create table app.rb");
    let table = position(&script, "GRANT SELECT ON TABLE app.rb TO reader;");
    let column = position(&script, "GRANT SELECT (code) ON TABLE app.rb TO reader;");
    assert!(create < table && create < column, "got: {script}");
    assert!(!script.contains("REVOKE"), "got: {script}");
}

#[tokio::test]
async fn compare_grants_table_kept_without_use_drop_keeps_its_acl() {
    // Without use_drop the DROP TABLE is only commented out: the table keeps
    // its privileges, and full mode revokes the FROM-only entry (planning it
    // from the default privileges never did).
    let (from, to) = rebuilt_table_dumps();
    let script = compare_all_in(GrantsMode::Full, from.clone(), to.clone(), false).await;
    assert!(
        script.contains("REVOKE SELECT ON TABLE app.rb FROM other;"),
        "got: {script}"
    );
    assert!(!script.contains("GRANT SELECT"), "got: {script}");
    let script = compare_all_in(GrantsMode::AddOnly, from, to, false).await;
    assert!(!script.contains("GRANT SELECT"), "got: {script}");

    // With use_drop the full mode starts from the default privileges (none
    // here) and grants reader's entries back.
    let (from, to) = rebuilt_table_dumps();
    let script = compare_all_in(GrantsMode::Full, from, to, true).await;
    for expected in [
        "GRANT SELECT ON TABLE app.rb TO reader;",
        "GRANT SELECT (code) ON TABLE app.rb TO reader;",
    ] {
        assert!(script.contains(expected), "{expected}: got: {script}");
    }
    assert!(!script.contains("REVOKE"), "got: {script}");
}

// ---- pre-cascade: privileges of what the migration creates afresh --------
//
// A view re-created after a routine's `DROP ... CASCADE` took it, and a
// routine created while default privileges for functions may apply, start
// from ACLs the migration does not know exactly; their statements have to
// reach TO from each of them.

/// `s1.<name>(<arguments>)`, a SQL function owned by `postgres`.
fn pc_function(name: &str, arguments: &str, return_type: &str, acl: &[&str]) -> Routine {
    let mut routine = Routine::new(
        "s1".to_string(),
        Oid(720),
        name.to_string(),
        "sql".to_string(),
        "function".to_string(),
        return_type.to_string(),
        arguments.to_string(),
        None,
        None,
        " select null ".to_string(),
    );
    routine.owner = "postgres".to_string();
    routine.acl = acl.iter().map(|a| a.to_string()).collect();
    routine.hash();
    routine
}

/// `s1.<name>`, a view owned by `postgres` calling `call`.
fn pc_calling_view(name: &str, call: &str, acl: &[&str]) -> View {
    let mut view = View::new(
        name.to_string(),
        format!(" SELECT {call} AS n;"),
        "s1".to_string(),
        Vec::new(),
    );
    view.owner = "postgres".to_string();
    view.acl = acl.iter().map(|a| a.to_string()).collect();
    view.hash();
    view
}

fn pc_default(role: &str, schema: &str, object_type: &str, acl: &[&str]) -> DefaultPrivilege {
    let mut dp = DefaultPrivilege {
        role_name: role.to_string(),
        schema_name: schema.to_string(),
        object_type: object_type.to_string(),
        acl: acl.iter().map(|a| a.to_string()).collect(),
        hash: None,
    };
    dp.hash();
    dp
}

/// The grants section of a full comparison.
async fn pc_grants_section(from: Dump, to: Dump, use_drop: bool, mode: GrantsMode) -> String {
    let mut comparer = Comparer::new(from, to, use_drop, false, true, mode);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();
    let start = script
        .find("/* ---> Grants: Start section")
        .unwrap_or_else(|| panic!("no grants section in:\n{script}"));
    let end = script[start..]
        .find("/* ---> Grants: End section")
        .map(|e| start + e)
        .unwrap();
    script[start..end].to_string()
}

/// FROM and TO with `s1.norm(t text)` changing its return type, and `views`
/// (the same on both sides) calling it or its unchanged overload
/// `s1.norm(t integer)`.
fn pc_cascade_dumps(views: &[View], from_defaults: Vec<DefaultPrivilege>) -> (Dump, Dump) {
    let mut from = Dump::new(DumpConfig::default());
    let mut to = Dump::new(DumpConfig::default());
    from.routines
        .push(pc_function("norm", "t text", "text", &[]));
    to.routines
        .push(pc_function("norm", "t text", "character varying", &[]));
    for dump in [&mut from, &mut to] {
        dump.routines
            .push(pc_function("norm", "t integer", "text", &[]));
        dump.views.extend(views.iter().cloned());
    }
    from.default_privileges = from_defaults.clone();
    to.default_privileges = from_defaults;
    (from, to)
}

#[tokio::test]
async fn pre_cascade_view_recreated_after_cascade_gets_its_grants_back() {
    let view = pc_calling_view(
        "v_items",
        "s1.norm('a'::text)",
        &["postgres=arwdDxtm/postgres", "reader=r/postgres"],
    );
    for mode in [GrantsMode::Full, GrantsMode::AddOnly] {
        let (from, to) = pc_cascade_dumps(std::slice::from_ref(&view), Vec::new());
        let grants = pc_grants_section(from, to, true, mode).await;
        assert!(
            grants.contains("GRANT SELECT ON TABLE s1.v_items TO reader;"),
            "{mode}: {grants}"
        );
        assert!(!grants.contains("REVOKE"), "{mode}: {grants}");
    }
    // Without use_drop the view is never dropped and keeps its privileges.
    let (from, to) = pc_cascade_dumps(std::slice::from_ref(&view), Vec::new());
    let grants = pc_grants_section(from, to, false, GrantsMode::Full).await;
    assert!(!grants.contains("s1.v_items"), "{grants}");
}

#[tokio::test]
async fn pre_cascade_view_recreated_after_cascade_loses_default_privileges_it_lacks() {
    // The default privileges give snoop SELECT on a new view in s1; both
    // sides revoked it from this one.
    let view = pc_calling_view(
        "v_items",
        "s1.norm('a'::text)",
        &["postgres=arwdDxtm/postgres", "reader=r/postgres"],
    );
    let (from, to) = pc_cascade_dumps(
        std::slice::from_ref(&view),
        vec![pc_default("postgres", "s1", "r", &["snoop=r/postgres"])],
    );
    let grants = pc_grants_section(from, to, true, GrantsMode::Full).await;
    assert!(
        grants.contains("REVOKE SELECT ON TABLE s1.v_items FROM snoop;"),
        "{grants}"
    );
    assert!(
        grants.contains("GRANT SELECT ON TABLE s1.v_items TO reader;"),
        "{grants}"
    );
}

#[tokio::test]
async fn pre_cascade_view_matched_through_an_overload_keeps_its_revokes() {
    // s1.v_int calls the overload s1.norm(integer), which does not change:
    // the name match re-emits the view, but it is never dropped, and the
    // privilege TO no longer has is still revoked.
    let mut from_view = pc_calling_view(
        "v_int",
        "s1.norm(1)",
        &["postgres=arwdDxtm/postgres", "reader=ar/postgres"],
    );
    let (mut from, mut to) = pc_cascade_dumps(&[], Vec::new());
    from.views.push(from_view.clone());
    from_view.acl = vec![
        "postgres=arwdDxtm/postgres".to_string(),
        "reader=r/postgres".to_string(),
    ];
    to.views.push(from_view);
    let grants = pc_grants_section(from, to, true, GrantsMode::Full).await;
    assert!(
        grants.contains("REVOKE INSERT ON TABLE s1.v_int FROM reader;"),
        "{grants}"
    );
    // Granted again in case the view was re-created; a no-op otherwise.
    assert!(
        grants.contains("GRANT SELECT ON TABLE s1.v_int TO reader;"),
        "{grants}"
    );
}

/// FROM and TO with `s1.g()` changing its return type from integer to
/// bigint, with `acl` on both sides, and `defaults` in FROM.
fn pc_recreated_routine_dumps(acl: &[&str], defaults: Vec<DefaultPrivilege>) -> (Dump, Dump) {
    let mut from = Dump::new(DumpConfig::default());
    let mut to = Dump::new(DumpConfig::default());
    from.routines.push(pc_function("g", "", "integer", acl));
    to.routines.push(pc_function("g", "", "bigint", acl));
    from.default_privileges = defaults.clone();
    to.default_privileges = defaults;
    (from, to)
}

#[tokio::test]
async fn pre_cascade_recreated_routine_revokes_what_schema_defaults_grant() {
    // ALTER DEFAULT PRIVILEGES IN SCHEMA s1 GRANT EXECUTE ON FUNCTIONS TO
    // deleg, and REVOKE ... FROM deleg on the routine itself.
    let (from, to) = pc_recreated_routine_dumps(
        &["=X/postgres", "postgres=X/postgres", "reader=X/postgres"],
        vec![pc_default("postgres", "s1", "f", &["deleg=X/postgres"])],
    );
    let grants = pc_grants_section(from, to, true, GrantsMode::Full).await;
    assert!(
        grants.contains("REVOKE EXECUTE ON FUNCTION s1.g() FROM deleg;"),
        "{grants}"
    );
    assert!(
        grants.contains("GRANT EXECUTE ON FUNCTION s1.g() TO reader;"),
        "{grants}"
    );
    assert!(!grants.contains("PUBLIC"), "{grants}");
    // Addonly revokes nothing.
    let (from, to) = pc_recreated_routine_dumps(
        &["=X/postgres", "postgres=X/postgres", "reader=X/postgres"],
        vec![pc_default("postgres", "s1", "f", &["deleg=X/postgres"])],
    );
    let grants = pc_grants_section(from, to, true, GrantsMode::AddOnly).await;
    assert!(!grants.contains("REVOKE"), "{grants}");
    assert!(
        grants.contains("GRANT EXECUTE ON FUNCTION s1.g() TO reader;"),
        "{grants}"
    );
}

#[tokio::test]
async fn pre_cascade_recreated_routine_regrants_public_a_global_default_revokes() {
    // ALTER DEFAULT PRIVILEGES REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC, and
    // GRANT EXECUTE ... TO PUBLIC on the routine itself.
    for mode in [GrantsMode::Full, GrantsMode::AddOnly] {
        let (from, to) = pc_recreated_routine_dumps(
            &["postgres=X/postgres", "=X/postgres"],
            vec![pc_default("postgres", "", "f", &["postgres=X/postgres"])],
        );
        let grants = pc_grants_section(from, to, true, mode).await;
        assert!(
            grants.contains("GRANT EXECUTE ON FUNCTION s1.g() TO PUBLIC;"),
            "{mode}: {grants}"
        );
        assert!(!grants.contains("REVOKE"), "{mode}: {grants}");
    }
}

#[tokio::test]
async fn pre_cascade_new_routine_revokes_what_schema_defaults_grant() {
    let mut from = Dump::new(DumpConfig::default());
    let mut to = Dump::new(DumpConfig::default());
    to.routines.push(pc_function(
        "h",
        "",
        "integer",
        &["=X/postgres", "postgres=X/postgres"],
    ));
    from.default_privileges = vec![pc_default("postgres", "s1", "f", &["deleg=X/postgres"])];
    to.default_privileges = from.default_privileges.clone();
    let grants = pc_grants_section(from, to, true, GrantsMode::Full).await;
    assert!(
        grants.contains("REVOKE EXECUTE ON FUNCTION s1.h() FROM deleg;"),
        "{grants}"
    );
    // PUBLIC already has EXECUTE whichever role creates it.
    assert!(!grants.contains("PUBLIC"), "{grants}");
}

#[tokio::test]
async fn pre_cascade_routine_with_a_null_acl_keeps_public_execute() {
    // TO's NULL ACL is the built-in default: only the default grant goes.
    let defaults = vec![pc_default("postgres", "s1", "f", &["deleg=X/postgres"])];
    let (from, to) = pc_recreated_routine_dumps(&[], defaults.clone());
    let grants = pc_grants_section(from, to, true, GrantsMode::Full).await;
    assert!(
        grants.contains("REVOKE EXECUTE ON FUNCTION s1.g() FROM deleg;"),
        "{grants}"
    );
    assert!(!grants.contains("PUBLIC"), "{grants}");

    // The REVOKE spells the default out, `{=X/postgres,postgres=X/postgres}`,
    // on the migrated routine, while TO's stays NULL: the next compare finds
    // nothing to do (it used to revoke PUBLIC's EXECUTE).
    let (mut from, mut to) = pc_recreated_routine_dumps(&[], defaults);
    from.routines[0] = pc_function("g", "", "bigint", &["=X/postgres", "postgres=X/postgres"]);
    to.routines[0] = pc_function("g", "", "bigint", &[]);
    for mode in [GrantsMode::Full, GrantsMode::AddOnly] {
        let grants = pc_grants_section(from.clone(), to.clone(), true, mode).await;
        assert!(
            !grants.contains("GRANT") && !grants.contains("REVOKE"),
            "{mode}: {grants}"
        );
    }
}

#[tokio::test]
async fn pre_cascade_routine_null_acl_under_another_roles_defaults_converges() {
    // Default privileges FOR ROLE other: the routine is created by some role,
    // so the migration covers `other` creating it too — and what it leaves
    // is the default spelled out, which the next compare accepts.
    for (defaults, statement) in [
        (
            vec![pc_default("other", "s1", "f", &["x=X/other"])],
            "REVOKE EXECUTE ON FUNCTION s1.h() FROM x;",
        ),
        (
            vec![pc_default("other", "", "f", &["other=X/other"])],
            "GRANT EXECUTE ON FUNCTION s1.h() TO PUBLIC;",
        ),
    ] {
        // Round 1: a new routine with a NULL ACL.
        let mut from = Dump::new(DumpConfig::default());
        let mut to = Dump::new(DumpConfig::default());
        to.routines.push(pc_function("h", "", "integer", &[]));
        from.default_privileges = defaults.clone();
        to.default_privileges = defaults.clone();
        let grants = pc_grants_section(from, to.clone(), true, GrantsMode::Full).await;
        assert!(grants.contains(statement), "{grants}");
        // Round 2: the routine as the statement left it.
        let mut migrated = Dump::new(DumpConfig::default());
        migrated.routines.push(pc_function(
            "h",
            "",
            "integer",
            &["=X/postgres", "postgres=X/postgres"],
        ));
        migrated.default_privileges = defaults;
        let grants = pc_grants_section(migrated, to, true, GrantsMode::Full).await;
        assert!(
            !grants.contains("GRANT") && !grants.contains("REVOKE"),
            "{grants}"
        );
    }
}

#[tokio::test]
async fn pre_cascade_null_to_routine_acl_is_the_default_against_an_explicit_one() {
    // No drop and no default privileges: the FROM routine's own ACL against
    // TO's NULL one, which stands for PUBLIC's EXECUTE.
    let run = |from_acl: &'static [&'static str], to_acl: &'static [&'static str], mode| async move {
        let mut from = Dump::new(DumpConfig::default());
        let mut to = Dump::new(DumpConfig::default());
        from.routines
            .push(pc_function("k", "", "integer", from_acl));
        to.routines.push(pc_function("k", "", "integer", to_acl));
        let grants = pc_grants_section(from, to, true, mode).await;
        grants
            .lines()
            .filter(|l| l.starts_with("GRANT") || l.starts_with("REVOKE"))
            .map(str::to_string)
            .collect::<Vec<String>>()
    };
    let none: Vec<String> = Vec::new();
    // The default spelled out is the default.
    assert_eq!(
        run(
            &["=X/postgres", "postgres=X/postgres"],
            &[],
            GrantsMode::Full
        )
        .await,
        none
    );
    // A grant TO lacks is revoked, PUBLIC's EXECUTE kept.
    assert_eq!(
        run(
            &["=X/postgres", "postgres=X/postgres", "reader=X/postgres"],
            &[],
            GrantsMode::Full
        )
        .await,
        ["REVOKE EXECUTE ON FUNCTION s1.k() FROM reader;"]
    );
    // PUBLIC's EXECUTE revoked in FROM comes back, addonly included.
    for mode in [GrantsMode::Full, GrantsMode::AddOnly] {
        assert_eq!(
            run(&["postgres=X/postgres"], &[], mode).await,
            ["GRANT EXECUTE ON FUNCTION s1.k() TO PUBLIC;"]
        );
    }
    // Both NULL: nothing. A NULL FROM ACL is the default too, so PUBLIC's
    // EXECUTE is not granted again (pre-grants: `acl::effective_acl`).
    assert_eq!(run(&[], &[], GrantsMode::Full).await, none);
    assert_eq!(
        run(
            &[],
            &["=X/postgres", "postgres=X/postgres", "reader=X/postgres"],
            GrantsMode::Full
        )
        .await,
        ["GRANT EXECUTE ON FUNCTION s1.k() TO reader;"]
    );
}

#[tokio::test]
async fn pre_cascade_routine_grants_unchanged_without_function_defaults() {
    // Default privileges for tables, or for functions in another schema, do
    // not apply: a recreated routine and a new one are both planned from
    // acldefault(), PUBLIC's EXECUTE, so the new one keeping it needs no
    // GRANT ... TO PUBLIC (pre-grants: `created_object_acl`).
    for defaults in [
        Vec::new(),
        vec![pc_default("postgres", "s1", "r", &["deleg=r/postgres"])],
        vec![pc_default("postgres", "other", "f", &["deleg=X/postgres"])],
    ] {
        let (from, mut to) = pc_recreated_routine_dumps(
            &["=X/postgres", "postgres=X/postgres", "reader=X/postgres"],
            defaults,
        );
        to.routines.push(pc_function(
            "h",
            "",
            "integer",
            &["=X/postgres", "postgres=X/postgres"],
        ));
        let grants = pc_grants_section(from, to, true, GrantsMode::Full).await;
        let statements: Vec<&str> = grants
            .lines()
            .filter(|l| l.starts_with("GRANT") || l.starts_with("REVOKE"))
            .collect();
        assert_eq!(
            statements,
            ["GRANT EXECUTE ON FUNCTION s1.g() TO reader;"],
            "{grants}"
        );
    }
}

#[test]
fn pre_cascade_created_object_acl_per_creating_role() {
    // One state per role with default privileges for functions that apply
    // in s1, plus acldefault() for a role without: `certain` is what all of
    // them hold, `possible` what any of them does.
    let mut from = Dump::new(DumpConfig::default());
    from.default_privileges = vec![
        pc_default("app", "", "f", &["app=X/app"]),
        pc_default("app", "s1", "f", &["deleg=X/app"]),
        pc_default("postgres", "s1", "f", &["other=X/postgres"]),
        pc_default("postgres", "s2", "f", &["ignored=X/postgres"]),
        pc_default("postgres", "s1", "r", &["ignored=r/postgres"]),
    ];
    let own = |v: &[&str]| v.iter().map(|s| s.to_string()).collect::<Vec<String>>();
    let created = |from: Dump, mode: GrantsMode, default_type: &str, kind: &str| {
        Comparer::new(
            from,
            Dump::new(DumpConfig::default()),
            true,
            false,
            true,
            mode,
        )
        .created_object_acl(default_type, "s1", kind, "postgres")
    };
    // app's database-wide row takes PUBLIC's EXECUTE away.
    assert_eq!(
        created(from.clone(), GrantsMode::Full, "f", "FUNCTION"),
        crate::dump::acl::CreatedAcl {
            certain: own(&[]),
            possible: own(&["=X/postgres", "deleg=X/app", "other=X/postgres"]),
        }
    );
    // Unless one of app's rows for the schema gives it back.
    let mut given_back = from.clone();
    given_back.default_privileges[1]
        .acl
        .push("=X/app".to_string());
    assert_eq!(
        created(given_back, GrantsMode::Full, "f", "FUNCTION"),
        crate::dump::acl::CreatedAcl {
            certain: own(&["=X/postgres"]),
            possible: own(&["=X/postgres", "deleg=X/app", "=X/app", "other=X/postgres"]),
        }
    );
    // A relation holds nothing by default; in full mode the rows for its
    // schema are taken as applied, database-wide ones only as possible.
    let mut relations = from.clone();
    relations.default_privileges.push(pc_default(
        "app",
        "",
        "r",
        &["app=arwdDxt/app", "wide=r/app"],
    ));
    assert_eq!(
        created(relations.clone(), GrantsMode::Full, "r", "TABLE"),
        crate::dump::acl::CreatedAcl {
            certain: own(&["ignored=r/postgres"]),
            possible: own(&["ignored=r/postgres", "wide=r/app"]),
        }
    );
    assert_eq!(
        created(relations, GrantsMode::AddOnly, "r", "TABLE"),
        crate::dump::acl::CreatedAcl {
            certain: own(&[]),
            possible: own(&["ignored=r/postgres", "wide=r/app"]),
        }
    );
    // Without default privileges: acldefault() alone.
    assert_eq!(
        created(
            Dump::new(DumpConfig::default()),
            GrantsMode::Full,
            "f",
            "FUNCTION"
        ),
        crate::dump::acl::CreatedAcl {
            certain: own(&["=X/postgres"]),
            possible: own(&["=X/postgres"]),
        }
    );
}

/// `view` recorded as calling `routines` (`View::routine_dependencies`).
fn pc_with_calls(mut view: View, routines: &[&str]) -> View {
    view.routine_dependencies = Some(routines.iter().map(|r| r.to_string()).collect());
    view
}

#[tokio::test]
async fn pre_cascade_view_the_cascade_takes_is_planned_like_a_dropped_one() {
    // The view calls s1.norm(t text), which the routine phase drops with
    // CASCADE: it starts from the schema's default privileges alone, so
    // what only FROM granted needs no REVOKE, and no grantor is named.
    let from_view = pc_with_calls(
        pc_calling_view(
            "v_items",
            "s1.norm('a'::text)",
            &[
                "postgres=arwdDxtm/postgres",
                "reader=r/postgres",
                "old=r/postgres",
                "delegate=r*/postgres",
                "friend=r/delegate",
            ],
        ),
        &["s1.norm(t text)"],
    );
    let mut to_view = from_view.clone();
    to_view.acl = vec![
        "postgres=arwdDxtm/postgres".to_string(),
        "reader=r/postgres".to_string(),
    ];
    let (mut from, mut to) = pc_cascade_dumps(
        &[],
        vec![pc_default("postgres", "s1", "r", &["snoop=r/postgres"])],
    );
    from.views.push(from_view);
    to.views.push(to_view);
    let grants = pc_grants_section(from, to, true, GrantsMode::Full).await;
    let statements: Vec<&str> = grants
        .lines()
        .filter(|l| l.starts_with("GRANT") || l.starts_with("REVOKE") || l.contains("GRANTED BY"))
        .collect();
    assert_eq!(
        statements,
        [
            "GRANT SELECT ON TABLE s1.v_items TO reader;",
            "REVOKE SELECT ON TABLE s1.v_items FROM snoop;",
        ],
        "{grants}"
    );
}

#[tokio::test]
async fn pre_cascade_view_calling_an_unchanged_overload_keeps_its_grantor_revokes() {
    // The name match re-emits s1.v_int, which calls the unchanged overload
    // s1.norm(t integer): the view is never dropped, so its FROM privileges
    // are the baseline, and the entry a delegate granted is revoked in its
    // name.
    let from_view = pc_with_calls(
        pc_calling_view(
            "v_int",
            "s1.norm(1)",
            &[
                "postgres=arwdDxtm/postgres",
                "delegate=r*/postgres",
                "reader=r/delegate",
            ],
        ),
        &["s1.norm(t integer)"],
    );
    let mut to_view = from_view.clone();
    to_view.acl = vec![
        "postgres=arwdDxtm/postgres".to_string(),
        "delegate=r*/postgres".to_string(),
    ];
    let (mut from, mut to) = pc_cascade_dumps(&[], Vec::new());
    from.views.push(from_view);
    to.views.push(to_view);
    // PostgreSQL 19 names the grantor with GRANTED BY.
    from.server_version_num = Some(190000);
    to.server_version_num = Some(190000);
    let grants = pc_grants_section(from, to, true, GrantsMode::Full).await;
    let statements: Vec<&str> = grants
        .lines()
        .filter(|l| l.starts_with("GRANT") || l.starts_with("REVOKE"))
        .collect();
    assert_eq!(
        statements,
        ["REVOKE SELECT ON TABLE s1.v_int FROM reader GRANTED BY delegate;"],
        "{grants}"
    );
}

#[tokio::test]
async fn pre_cascade_view_of_an_older_dump_revokes_a_grantors_entry_only_while_it_is_there() {
    // A dump without routine dependencies cannot tell whether the CASCADE
    // took the view: the revoke naming the delegate runs only while its
    // entry is still recorded.
    let from_view = pc_calling_view(
        "v_int",
        "s1.norm(1)",
        &[
            "postgres=arwdDxtm/postgres",
            "delegate=r*/postgres",
            "reader=r/delegate",
        ],
    );
    let mut to_view = from_view.clone();
    to_view.acl = vec![
        "postgres=arwdDxtm/postgres".to_string(),
        "delegate=r*/postgres".to_string(),
    ];
    let (mut from, mut to) = pc_cascade_dumps(&[], Vec::new());
    from.views.push(from_view);
    to.views.push(to_view);
    let grants = pc_grants_section(from, to, true, GrantsMode::Full).await;
    // pgc 1.0.x recorded no server version either: the grantor is taken on
    // with SET ROLE.
    let guarded = rc_guarded_block(&grants);
    assert!(
        guarded.contains("pg_catalog.pg_get_userbyid(a.grantor) = 'delegate'")
            && guarded.contains("SET ROLE delegate;")
            && guarded.contains("REVOKE SELECT ON TABLE s1.v_int FROM reader;"),
        "{grants}"
    );
    // Granted again in case the view was re-created, and revoked in case it
    // was not; each a no-op in the other case.
    assert!(
        grants.contains("GRANT SELECT ON TABLE s1.v_int TO delegate WITH GRANT OPTION;"),
        "{grants}"
    );
    assert!(
        grants.contains("REVOKE SELECT ON TABLE s1.v_int FROM reader;"),
        "{grants}"
    );
}

/// The first `DO` block of `grants`.
fn rc_guarded_block(grants: &str) -> &str {
    let start = grants
        .find("DO $")
        .unwrap_or_else(|| panic!("no guarded revoke in:\n{grants}"));
    let end = start
        + grants[start..]
            .find("END")
            .unwrap_or_else(|| panic!("unterminated guarded revoke in:\n{grants}"));
    &grants[start..end]
}

// ==== fix:pre-grants tests ====

/// `app.<name>()` owned by `postgres`, with `acl` as its `proacl` (empty and
/// `explicitly_empty` false: NULL).
fn pregrants_fn(name: &str, acl: &[&str], explicitly_empty: bool) -> Routine {
    let mut r = Routine::new(
        "app".to_string(),
        Oid(7),
        name.to_string(),
        "sql".to_string(),
        "function".to_string(),
        "integer".to_string(),
        "".to_string(),
        None,
        None,
        "select 1".to_string(),
    );
    r.owner = "postgres".to_string();
    r.acl = acl.iter().map(|a| a.to_string()).collect();
    r.acl_explicitly_empty = explicitly_empty;
    r.hash();
    r
}

fn pregrants_domain(acl: &[&str]) -> crate::dump::pg_type::PgType {
    let mut t = make_domain_type("app", "dm", 16400);
    t.acl = acl.iter().map(|a| a.to_string()).collect();
    t.hash();
    t
}

async fn pregrants_compare(from: Dump, to: Dump, mode: GrantsMode, use_drop: bool) -> String {
    let mut comparer = Comparer::new(from, to, use_drop, false, false, mode);
    comparer.compare_grants().await.unwrap();
    comparer.get_script()
}

fn routine_dumps(from: Option<Routine>, to: Routine) -> (Dump, Dump) {
    let mut from_dump = Dump::new(DumpConfig::default());
    from_dump.routines.extend(from);
    let mut to_dump = Dump::new(DumpConfig::default());
    to_dump.routines.push(to);
    (from_dump, to_dump)
}

#[tokio::test]
async fn null_routine_acl_against_a_public_revoke_revokes_it() {
    // NULL proacl gives PUBLIC EXECUTE; it used to compare equal to
    // `{postgres=X/postgres}` and the hardening was never migrated.
    let (from, to) = routine_dumps(
        Some(pregrants_fn("secret", &[], false)),
        pregrants_fn("secret", &["postgres=X/postgres"], false),
    );
    let script = pregrants_compare(from.clone(), to.clone(), GrantsMode::Full, true).await;
    assert_eq!(
        script,
        "REVOKE EXECUTE ON FUNCTION app.secret() FROM PUBLIC;\n"
    );
    // Add-only never revokes.
    assert_eq!(
        pregrants_compare(from, to, GrantsMode::AddOnly, true).await,
        ""
    );
}

#[tokio::test]
async fn routine_back_to_a_null_acl_gets_public_execute_back() {
    let (from, to) = routine_dumps(
        Some(pregrants_fn(
            "f",
            &["postgres=X/postgres", "app=X/postgres"],
            false,
        )),
        pregrants_fn("f", &[], false),
    );
    // It used to revoke PUBLIC's EXECUTE here instead, which TO grants.
    assert_eq!(
        pregrants_compare(from.clone(), to.clone(), GrantsMode::Full, true).await,
        "GRANT EXECUTE ON FUNCTION app.f() TO PUBLIC;\n\nREVOKE EXECUTE ON FUNCTION app.f() FROM app;\n"
    );
    assert_eq!(
        pregrants_compare(from, to, GrantsMode::AddOnly, true).await,
        "GRANT EXECUTE ON FUNCTION app.f() TO PUBLIC;\n"
    );
}

#[tokio::test]
async fn routine_acls_that_agree_emit_nothing() {
    for acl in [&[][..], &["postgres=X/postgres", "app=X/postgres"][..]] {
        let (from, to) = routine_dumps(
            Some(pregrants_fn("f", acl, false)),
            pregrants_fn("f", acl, false),
        );
        assert_eq!(
            pregrants_compare(from, to, GrantsMode::Full, true).await,
            ""
        );
    }
    // `{}` on both sides, and `{}` against an ACL without PUBLIC.
    let (from, to) = routine_dumps(
        Some(pregrants_fn("f", &[], true)),
        pregrants_fn("f", &[], true),
    );
    assert_eq!(
        pregrants_compare(from, to, GrantsMode::Full, true).await,
        ""
    );
}

#[tokio::test]
async fn explicitly_empty_routine_acl_is_not_the_default() {
    // `{}` (everything revoked, the owner's too) against NULL: PUBLIC gets
    // EXECUTE back. Read as NULL, the two compared equal.
    let (from, to) = routine_dumps(
        Some(pregrants_fn("f", &[], true)),
        pregrants_fn("f", &[], false),
    );
    assert_eq!(
        pregrants_compare(from, to, GrantsMode::Full, true).await,
        "GRANT EXECUTE ON FUNCTION app.f() TO PUBLIC;\n"
    );
}

#[tokio::test]
async fn new_routine_created_with_public_revoked_is_revoked_in_the_same_run() {
    // The CREATE gives PUBLIC EXECUTE. It used to take a second run (or never
    // happen when nothing else was granted).
    let (from, to) = routine_dumps(
        None,
        pregrants_fn(
            "f_new",
            &["postgres=X/postgres", "Reader=X/postgres"],
            false,
        ),
    );
    assert_eq!(
        pregrants_compare(from, to, GrantsMode::Full, true).await,
        "REVOKE EXECUTE ON FUNCTION app.f_new() FROM PUBLIC;\n\n\
         GRANT EXECUTE ON FUNCTION app.f_new() TO \"Reader\";\n"
    );
    // Keeping PUBLIC's default needs no GRANT ... TO PUBLIC any more.
    let (from, to) = routine_dumps(
        None,
        pregrants_fn(
            "f_new",
            &["=X/postgres", "postgres=X/postgres", "app=X/postgres"],
            false,
        ),
    );
    assert_eq!(
        pregrants_compare(from, to, GrantsMode::Full, true).await,
        "GRANT EXECUTE ON FUNCTION app.f_new() TO app;\n"
    );
    // A new routine with a NULL ACL: nothing, as before.
    let (from, to) = routine_dumps(None, pregrants_fn("f_new", &[], false));
    assert_eq!(
        pregrants_compare(from, to, GrantsMode::Full, true).await,
        ""
    );
}

fn pregrants_default_row(role: &str, schema: &str, kind: &str, acl: &[&str]) -> DefaultPrivilege {
    DefaultPrivilege {
        role_name: role.to_string(),
        schema_name: schema.to_string(),
        object_type: kind.to_string(),
        acl: acl.iter().map(|a| a.to_string()).collect(),
        hash: None,
    }
}

#[tokio::test]
async fn new_routine_under_from_default_privileges_of_an_unknown_creator() {
    // Only the creating role's pg_default_acl rows apply, and who runs the
    // migration is unknown. A database-wide row that drops PUBLIC may or may
    // not apply, so PUBLIC is granted when TO keeps it and revoked when TO
    // does not — one of the two is a no-op.
    let global = pregrants_default_row("postgres", "", "f", &["postgres=X/postgres"]);
    let (mut from, to) = routine_dumps(None, pregrants_fn("f_new", &[], false));
    from.default_privileges = vec![global.clone()];
    assert_eq!(
        pregrants_compare(from.clone(), to.clone(), GrantsMode::Full, true).await,
        "GRANT EXECUTE ON FUNCTION app.f_new() TO PUBLIC;\n"
    );
    assert_eq!(
        pregrants_compare(from.clone(), to, GrantsMode::AddOnly, true).await,
        "GRANT EXECUTE ON FUNCTION app.f_new() TO PUBLIC;\n"
    );
    let (_, revoked) = routine_dumps(None, pregrants_fn("f_new", &["postgres=X/postgres"], false));
    // Taking the row for granted emitted nothing here, and a routine created
    // by any other role kept PUBLIC's EXECUTE.
    assert_eq!(
        pregrants_compare(from.clone(), revoked.clone(), GrantsMode::Full, true).await,
        "REVOKE EXECUTE ON FUNCTION app.f_new() FROM PUBLIC;\n"
    );
    assert_eq!(
        pregrants_compare(from, revoked, GrantsMode::AddOnly, true).await,
        ""
    );

    // A schema row adds to acldefault: full mode revokes what TO lacks, and
    // what TO holds is granted even though the row may give it already.
    let schema_row = pregrants_default_row("postgres", "app", "f", &["app_reader=X/postgres"]);
    let (mut from, to) = routine_dumps(None, pregrants_fn("f_new", &[], false));
    from.default_privileges = vec![schema_row];
    assert_eq!(
        pregrants_compare(from.clone(), to.clone(), GrantsMode::Full, true).await,
        "REVOKE EXECUTE ON FUNCTION app.f_new() FROM app_reader;\n"
    );
    assert_eq!(
        pregrants_compare(from.clone(), to, GrantsMode::AddOnly, true).await,
        ""
    );
    let (_, granted) = routine_dumps(
        None,
        pregrants_fn(
            "f_new",
            &[
                "=X/postgres",
                "postgres=X/postgres",
                "app_reader=X/postgres",
            ],
            false,
        ),
    );
    for mode in [GrantsMode::Full, GrantsMode::AddOnly] {
        assert_eq!(
            pregrants_compare(from.clone(), granted.clone(), mode, true).await,
            "GRANT EXECUTE ON FUNCTION app.f_new() TO app_reader;\n"
        );
    }

    // A row granting the routine's owner is not revoked from the owner.
    let owner_row = pregrants_default_row("app", "app", "f", &["postgres=X/app"]);
    let (mut from, to) = routine_dumps(None, pregrants_fn("f_new", &[], false));
    from.default_privileges = vec![owner_row];
    assert_eq!(
        pregrants_compare(from, to, GrantsMode::Full, true).await,
        ""
    );
}

/// The hardening pattern `ALTER DEFAULT PRIVILEGES FOR ROLE app REVOKE
/// EXECUTE ON FUNCTIONS FROM PUBLIC` plus a schema-level grant, with the
/// migration run by a superuser: a new and a recreated routine owned by
/// `app` get PUBLIC revoked and the reader granted in the first run, as
/// pg19-integration already did for the recreated one.
#[tokio::test]
async fn created_routines_under_another_roles_default_privileges() {
    let rows = vec![
        pregrants_default_row("app", "", "f", &["app=X/app"]),
        pregrants_default_row("app", "app", "f", &["reader=X/app"]),
    ];
    let acl = ["app=X/app", "reader=X/app"];
    let owned_by_app = |mut r: Routine| {
        r.owner = "app".to_string();
        r.hash();
        r
    };
    let from_fn = owned_by_app(pregrants_fn("fr", &acl, false));
    let mut to_fn = pregrants_fn("fr", &acl, false);
    to_fn.return_type = "bigint".to_string();
    let to_fn = owned_by_app(to_fn);
    let new_fn = owned_by_app(pregrants_fn("fnew", &acl, false));
    let mut from = Dump::new(DumpConfig::default());
    from.routines.push(from_fn);
    from.default_privileges = rows.clone();
    let mut to = Dump::new(DumpConfig::default());
    to.routines.push(to_fn);
    to.routines.push(new_fn);
    to.default_privileges = rows;
    let expected = "REVOKE EXECUTE ON FUNCTION app.fr() FROM PUBLIC;\n\n\
                    GRANT EXECUTE ON FUNCTION app.fr() TO reader;\n\n\
                    REVOKE EXECUTE ON FUNCTION app.fnew() FROM PUBLIC;\n\n\
                    GRANT EXECUTE ON FUNCTION app.fnew() TO reader;\n";
    assert_eq!(
        pregrants_compare(from.clone(), to.clone(), GrantsMode::Full, true).await,
        expected
    );
    // Without use_drop the changed routine is not recreated and keeps its
    // FROM ACL, which already matches.
    assert_eq!(
        pregrants_compare(from, to, GrantsMode::Full, false).await,
        "REVOKE EXECUTE ON FUNCTION app.fnew() FROM PUBLIC;\n\n\
         GRANT EXECUTE ON FUNCTION app.fnew() TO reader;\n"
    );
}

#[tokio::test]
async fn new_types_under_from_default_privileges_of_an_unknown_creator() {
    // `FOR ROLE app REVOKE USAGE ON TYPES FROM PUBLIC` and `GRANT USAGE ON
    // TYPES TO reader`, database-wide: a domain `app` owns with that ACL gets
    // both statements; one the superuser owns with a NULL ACL gets PUBLIC's
    // USAGE (a no-op unless `app` created it) and loses the reader's.
    let row = pregrants_default_row("app", "", "T", &["app=U/app", "reader=U/app"]);
    let mut owned = pregrants_domain(&["app=U/app", "reader=U/app"]);
    owned.owner = "app".to_string();
    owned.hash();
    let mut from = Dump::new(DumpConfig::default());
    from.default_privileges = vec![row];
    let mut to = Dump::new(DumpConfig::default());
    to.types.push(owned);
    assert_eq!(
        pregrants_compare(from.clone(), to, GrantsMode::Full, true).await,
        "REVOKE USAGE ON TYPE app.dm FROM PUBLIC;\n\n\
         GRANT USAGE ON TYPE app.dm TO reader;\n"
    );
    let mut to = Dump::new(DumpConfig::default());
    to.types.push(pregrants_domain(&[]));
    assert_eq!(
        pregrants_compare(from, to, GrantsMode::Full, true).await,
        "GRANT USAGE ON TYPE app.dm TO PUBLIC;\n\n\
         REVOKE USAGE ON TYPE app.dm FROM reader;\n"
    );
}

/// `compare_types` drops and recreates a changed range type, which comes back
/// with `acldefault()`: its TO ACL is re-applied from there, not from the ACL
/// the `DROP` removed.
#[tokio::test]
async fn recreated_range_type_gets_its_acl_again() {
    let range = |diff: Option<&str>| {
        let mut t = make_domain_type("app", "rng", 16500);
        t.typtype = 'r' as i8;
        t.range_subtype = Some("integer".to_string());
        t.range_subdiff = diff.map(str::to_string);
        t.acl = vec![
            "postgres=U/postgres".to_string(),
            "reader=U/postgres".to_string(),
        ];
        t.hash();
        t
    };
    let mut from = Dump::new(DumpConfig::default());
    from.types.push(range(Some("int4range_subdiff")));
    let mut to = Dump::new(DumpConfig::default());
    to.types.push(range(None));
    assert_eq!(
        pregrants_compare(from.clone(), to.clone(), GrantsMode::Full, true).await,
        "REVOKE USAGE ON TYPE app.rng FROM PUBLIC;\n\n\
         GRANT USAGE ON TYPE app.rng TO reader;\n"
    );
    assert_eq!(
        pregrants_compare(from.clone(), to.clone(), GrantsMode::AddOnly, true).await,
        "GRANT USAGE ON TYPE app.rng TO reader;\n"
    );
    // Without use_drop the DROP is commented out and the type keeps its ACL.
    assert_eq!(
        pregrants_compare(from.clone(), to, GrantsMode::Full, false).await,
        ""
    );
    // An unchanged range type is not recreated.
    assert_eq!(
        pregrants_compare(from.clone(), from, GrantsMode::Full, true).await,
        ""
    );
}

#[tokio::test]
async fn mixed_case_routine_owner_entries_are_not_grantees() {
    // The dump spells the owner `"Owner"`, the ACL `Owner`.
    let mut from_fn = pregrants_fn("f", &[], false);
    from_fn.owner = "\"Owner\"".to_string();
    let mut to_fn = pregrants_fn("f", &["=X/Owner", "Owner=X/Owner", "app=X/Owner"], false);
    to_fn.owner = "\"Owner\"".to_string();
    let (from, to) = routine_dumps(Some(from_fn), to_fn);
    assert_eq!(
        pregrants_compare(from, to, GrantsMode::Full, true).await,
        "GRANT EXECUTE ON FUNCTION app.f() TO app;\n"
    );
}

#[tokio::test]
async fn null_type_acl_against_a_public_revoke_revokes_usage() {
    let mut from = Dump::new(DumpConfig::default());
    from.types.push(pregrants_domain(&[]));
    let mut to = Dump::new(DumpConfig::default());
    to.types.push(pregrants_domain(&["postgres=U/postgres"]));
    assert_eq!(
        pregrants_compare(from.clone(), to.clone(), GrantsMode::Full, true).await,
        "REVOKE USAGE ON TYPE app.dm FROM PUBLIC;\n"
    );
    // The reverse direction grants it back.
    assert_eq!(
        pregrants_compare(to, from, GrantsMode::Full, true).await,
        "GRANT USAGE ON TYPE app.dm TO PUBLIC;\n"
    );
    // A new domain created with PUBLIC revoked.
    let mut to = Dump::new(DumpConfig::default());
    to.types.push(pregrants_domain(&["postgres=U/postgres"]));
    assert_eq!(
        pregrants_compare(Dump::new(DumpConfig::default()), to, GrantsMode::Full, true).await,
        "REVOKE USAGE ON TYPE app.dm FROM PUBLIC;\n"
    );
    // NULL on both sides: nothing.
    let mut both = Dump::new(DumpConfig::default());
    both.types.push(pregrants_domain(&[]));
    assert_eq!(
        pregrants_compare(both.clone(), both, GrantsMode::Full, true).await,
        ""
    );
}

#[tokio::test]
async fn table_grants_to_roles_that_need_quotes_are_quoted() {
    let mut from = Dump::new(DumpConfig::default());
    let mut to = Dump::new(DumpConfig::default());
    let mut from_t = Table::new(
        "app".into(),
        "t".into(),
        "app".into(),
        "t".into(),
        "postgres".into(),
        None,
        vec![],
        vec![],
        vec![],
        vec![],
        None,
    );
    from_t.acl = vec![
        "postgres=arwdDxt/postgres".into(),
        "Reader=r/postgres".into(),
    ];
    from_t.hash();
    let mut to_t = from_t.clone();
    to_t.acl = vec![
        "postgres=arwdDxt/postgres".into(),
        "\"a b\"=a/postgres".into(),
        "user=w/postgres".into(),
    ];
    to_t.hash();
    from.tables.push(from_t);
    to.tables.push(to_t);
    assert_eq!(
        pregrants_compare(from, to, GrantsMode::Full, true).await,
        "GRANT INSERT ON TABLE app.t TO \"a b\";\n\n\
         REVOKE SELECT ON TABLE app.t FROM \"Reader\";\n\n\
         GRANT UPDATE ON TABLE app.t TO \"user\";\n"
    );
}

/// PostgreSQL refuses any GRANT or REVOKE on a multirange ("cannot set
/// privileges of multirange types"), so none is emitted — not even the
/// GRANT ... TO PUBLIC a new one would get under FROM default privileges that
/// may have taken PUBLIC's USAGE away.
#[tokio::test]
async fn multirange_types_get_no_grants() {
    let multirange = |acl: &[&str]| {
        let mut t = make_domain_type("app", "rng_multirange", 16501);
        t.typtype = 'm' as i8;
        t.acl = acl.iter().map(|a| a.to_string()).collect();
        t.hash();
        t
    };
    let mut from = Dump::new(DumpConfig::default());
    from.default_privileges = vec![pregrants_default_row("app", "", "T", &["app=U/app"])];
    let mut to = Dump::new(DumpConfig::default());
    to.types.push(multirange(&[]));
    assert_eq!(
        pregrants_compare(from.clone(), to, GrantsMode::Full, true).await,
        ""
    );
    // Nor for one whose ACL differs between the dumps.
    from.types.push(multirange(&["postgres=U/postgres"]));
    let mut to = Dump::new(DumpConfig::default());
    to.types.push(multirange(&[]));
    assert_eq!(
        pregrants_compare(from, to, GrantsMode::Full, true).await,
        ""
    );
}

/// A range type whose ACL alone differs is not dropped and recreated — the
/// `CASCADE` used to take every column of that type with it — and neither is
/// one whose NULL ACL became an equivalent explicit one after a
/// `GRANT ... TO PUBLIC`: only the grants are migrated.
#[tokio::test]
async fn range_type_whose_acl_alone_differs_is_not_recreated() {
    let range = |acl: &[&str]| {
        let mut t = make_domain_type("app", "rng", 16500);
        t.typtype = 'r' as i8;
        t.range_subtype = Some("integer".to_string());
        t.acl = acl.iter().map(|a| a.to_string()).collect();
        t.hash();
        t
    };
    let compare = |from: PgType, to: PgType| async move {
        let mut from_dump = Dump::new(DumpConfig::default());
        from_dump.types.push(from);
        let mut to_dump = Dump::new(DumpConfig::default());
        to_dump.types.push(to);
        let mut comparer = Comparer::new(from_dump, to_dump, true, false, false, GrantsMode::Full);
        comparer.compare().await.unwrap();
        comparer.get_script()
    };
    let script = compare(
        range(&[]),
        range(&["=U/postgres", "postgres=U/postgres", "reader=U/postgres"]),
    )
    .await;
    assert!(!script.contains("drop type"), "{script}");
    assert!(
        script.contains("GRANT USAGE ON TYPE app.rng TO reader;"),
        "{script}"
    );
    let script = compare(range(&["=U/postgres", "postgres=U/postgres"]), range(&[])).await;
    assert!(!script.contains("drop type"), "{script}");
    assert!(
        !script.contains("GRANT") && !script.contains("REVOKE"),
        "{script}"
    );
}
