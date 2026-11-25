//! Dumps written by different pgc versions compared with each other.
//!
//! A dump written by pgc 1.1 or later records `server_version_num`; one
//! written by pgc 1.0.x does not. A pgc 1.0.x TO dump against a pgc 1.1+
//! FROM dump is refused (`Comparer::check_dump_versions`), because every gap
//! in the old dump would become a destructive change. A pgc 1.0.x FROM dump
//! against a pgc 1.1+ TO dump is compared, with a warning, a comment at the
//! top of the script and the `standard_conforming_strings` guard, and every
//! TO subscription is created, because the old dump lists the subscriptions
//! of the whole cluster. Two dumps of the same generation compare exactly as
//! before.

use super::helpers::*;
use crate::comparer::core::*;
use crate::config::dump_config::DumpConfig;
use crate::config::grants_mode::GrantsMode;
use crate::dump::publication::Subscription;
use crate::dump::table::Table;

const SCS_GUARD: &str = "set standard_conforming_strings = on;";

/// The first line of the comment a mixed-version migration opens with.
const MIXED_COMMENT_START: &str =
    "/* Warning: the FROM dump was written by pgc 1.0.x and the TO dump by pgc 1.1 or later.";

/// A dump written by pgc 1.1+ when `version` is set, by pgc 1.0.x otherwise.
fn dump_written_by(version: Option<i32>) -> Dump {
    let mut dump = Dump::new(DumpConfig::default());
    dump.server_version_num = version;
    dump
}

/// A one-integer-column table, hashed.
fn rv_mixed_table(schema: &str, name: &str) -> Table {
    let mut table = Table::new(
        schema.to_string(),
        name.to_string(),
        schema.to_string(),
        name.to_string(),
        "postgres".to_string(),
        None,
        vec![int_column(schema, name, "id", 1)],
        vec![],
        vec![],
        vec![],
        None,
    );
    table.hash();
    table
}

/// A migration that creates one table, `public.widgets`, from a FROM dump
/// written with `from_version` to a TO dump written with `to_version`.
fn rv_mixed_comparer(
    from_version: Option<i32>,
    to_version: Option<i32>,
    use_single_transaction: bool,
    use_comments: bool,
) -> Comparer {
    let from = dump_written_by(from_version);
    let mut to = dump_written_by(to_version);
    to.tables.push(rv_mixed_table("public", "widgets"));
    Comparer::new(
        from,
        to,
        false,
        use_single_transaction,
        use_comments,
        GrantsMode::Ignore,
    )
}

// ── check_dump_versions ─────────────────────────────────────────────────

#[test]
fn check_dump_versions_refuses_a_1_1_from_dump_against_a_1_0_to_dump() {
    let err = Comparer::check_dump_versions(&dump_written_by(Some(180000)), &dump_written_by(None))
        .expect_err("a pgc 1.0.x TO dump against a pgc 1.1+ FROM dump must be refused");

    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    let message = err.to_string();
    assert!(
        message.contains("Re-dump 'to' with pgc 1.1 or later"),
        "the error must say how to fix it: {message}"
    );
    assert!(message.contains("pgc 1.0.x"), "{message}");
}

#[test]
fn check_dump_versions_accepts_every_other_pairing() {
    for (from, to) in [
        (None, None),
        (None, Some(180000)),
        (Some(140000), Some(190000)),
        (Some(190000), Some(140000)),
    ] {
        assert!(
            Comparer::check_dump_versions(&dump_written_by(from), &dump_written_by(to)).is_ok(),
            "from {from:?}, to {to:?}"
        );
    }
}

/// The check reads the versions only: neither the objects a dump holds nor
/// the FROM server's `standard_conforming_strings` default changes it.
#[test]
fn check_dump_versions_ignores_everything_but_the_versions() {
    let mut to = dump_written_by(None);
    to.tables.push(rv_mixed_table("public", "pgbench_accounts"));
    let mut from = dump_written_by(Some(160000));
    from.standard_conforming_strings_off = true;

    assert!(Comparer::check_dump_versions(&from, &to).is_err());
    assert!(Comparer::check_dump_versions(&to, &from).is_ok());
}

// ── legacy_from_against_current_to ──────────────────────────────────────

#[test]
fn legacy_from_against_current_to_holds_only_for_a_1_0_from_and_a_1_1_to() {
    for (from, to, expected) in [
        (None, None, false),
        (None, Some(190000), true),
        (Some(190000), None, false),
        (Some(180000), Some(190000), false),
    ] {
        let comparer = rv_mixed_comparer(from, to, false, false);
        assert_eq!(
            comparer.legacy_from_against_current_to(),
            expected,
            "from {from:?}, to {to:?}"
        );
    }
}

// ── the comment at the top of the script ────────────────────────────────

#[tokio::test]
async fn a_1_0_from_dump_opens_the_script_with_a_warning_comment() {
    let mut comparer = rv_mixed_comparer(None, Some(180000), true, true);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();

    assert_eq!(script.matches(MIXED_COMMENT_START).count(), 1, "{script}");
    // Right after the header block, ahead of every statement.
    let header_end = script.find("*/").expect("header block");
    let comment = script.find(MIXED_COMMENT_START).unwrap();
    assert!(
        script[header_end..comment].trim() == "*/",
        "the warning must directly follow the header:\n{script}"
    );
    let guard = script.find(SCS_GUARD).expect("guard missing");
    let begin = script.find("begin;").expect("begin; missing");
    assert!(comment < guard && guard < begin, "{script}");
    assert!(
        script.contains("Re-dump FROM with pgc 1.1 or later for an\n   exact migration. */"),
        "{script}"
    );
}

/// Pinned so a reworded comment is a deliberate change.
#[tokio::test]
async fn the_warning_comment_text_is_pinned() {
    let mut comparer = rv_mixed_comparer(None, Some(180000), false, true);
    comparer.compare().await.unwrap();

    assert!(
        comparer.get_script().contains(
            "/* Warning: the FROM dump was written by pgc 1.0.x and the TO dump by pgc 1.1 or later.
   pgc 1.0.x did not read everything pgc 1.1 does, so this migration re-applies objects
   FROM may already have: SQL-standard routines, publication filters and options, replica
   identities and literals holding a backslash. It also creates every table whose name
   starts with pg but not pg_, and every subscription: where FROM has one, its CREATE
   fails with \"already exists\". Production output skips such a CREATE TABLE, but not the
   constraints added to the table separately, which fail the same way, and any other
   difference in that table is left unapplied. Re-dump FROM with pgc 1.1 or later for an
   exact migration. */\n\n"
        ),
        "{}",
        comparer.get_script()
    );
}

/// A comment like every other: `--use-comments false` strips it, and the
/// warning is then on stderr only.
#[tokio::test]
async fn the_warning_comment_goes_with_use_comments_off() {
    let mut comparer = rv_mixed_comparer(None, Some(180000), false, false);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();

    assert!(!script.contains("pgc 1.0.x"), "{script}");
    assert!(script.starts_with(&format!("{SCS_GUARD}\n\n")), "{script}");
}

#[tokio::test]
async fn the_warning_comment_is_absent_for_dumps_of_one_generation() {
    for (from, to) in [(None, None), (Some(180000), Some(180000))] {
        let mut comparer = rv_mixed_comparer(from, to, true, true);
        comparer.compare().await.unwrap();
        assert!(
            !comparer.get_script().contains("pgc 1.0.x"),
            "from {from:?}, to {to:?}:\n{}",
            comparer.get_script()
        );
    }
}

/// The production rewrite is comment-aware, so the warning survives it, and
/// it still comes first.
#[tokio::test]
async fn the_warning_comment_precedes_the_production_note() {
    let mut comparer = rv_mixed_comparer(None, Some(180000), true, true);
    comparer.set_output_for_production(true);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();

    let comment = script.find(MIXED_COMMENT_START).expect("comment missing");
    let note = script
        .find("/* Output generated for production")
        .expect("production note missing");
    assert!(comment < note, "{script}");
}

// ── the standard_conforming_strings guard ──────────────────────────────

/// A pgc 1.0.x FROM dump does not record the server's default, and of an
/// `off` server it holds every backslash literal doubled, so each one is
/// rewritten from the TO dump's standard form; the guard makes psql read
/// those as written.
#[tokio::test]
async fn scs_guard_leads_a_migration_from_a_1_0_from_dump() {
    for use_single_transaction in [false, true] {
        let mut comparer = rv_mixed_comparer(None, Some(180000), use_single_transaction, false);
        comparer.compare().await.unwrap();
        let script = comparer.get_script();

        let expected_start = if use_single_transaction {
            format!("{SCS_GUARD}\n\nbegin;\n\n")
        } else {
            format!("{SCS_GUARD}\n\n")
        };
        assert!(script.starts_with(&expected_start), "{script}");
        assert_eq!(script.matches(SCS_GUARD).count(), 1, "{script}");
    }
}

/// Like the other guards, withdrawn when it is all the migration would do.
#[tokio::test]
async fn scs_guard_is_withdrawn_from_an_empty_mixed_migration() {
    for use_single_transaction in [false, true] {
        let mut plain = Comparer::new(
            dump_written_by(None),
            dump_written_by(None),
            false,
            use_single_transaction,
            false,
            GrantsMode::Ignore,
        );
        plain.compare().await.unwrap();
        let mut mixed = Comparer::new(
            dump_written_by(None),
            dump_written_by(Some(180000)),
            false,
            use_single_transaction,
            false,
            GrantsMode::Ignore,
        );
        mixed.compare().await.unwrap();

        assert_eq!(mixed.get_script(), plain.get_script());
    }
    let mut empty = Comparer::new(
        dump_written_by(None),
        dump_written_by(Some(180000)),
        false,
        false,
        false,
        GrantsMode::Ignore,
    );
    empty.compare().await.unwrap();
    assert_eq!(empty.get_script(), "", "an empty migration must stay empty");
}

/// Two pgc 1.0.x dumps: exactly the pgc 1.0.x output, no guard, no comment.
#[tokio::test]
async fn two_1_0_dumps_get_neither_the_guard_nor_the_comment() {
    for use_comments in [false, true] {
        let mut comparer = rv_mixed_comparer(None, None, true, use_comments);
        comparer.compare().await.unwrap();
        let script = comparer.get_script();

        assert!(!script.contains("standard_conforming_strings"), "{script}");
        assert!(!script.contains("pgc 1.0.x"), "{script}");
        assert!(script.contains("create table public.widgets"), "{script}");
    }
}

/// Two pgc 1.1+ dumps of a server at the default: unchanged, and in
/// particular no guard (the FROM dump says the server is `on`).
#[tokio::test]
async fn two_1_1_dumps_of_an_on_server_get_no_guard() {
    let mut comparer = rv_mixed_comparer(Some(180000), Some(180000), true, false);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();

    assert!(script.starts_with("begin;\n\n"), "{script}");
    assert!(!script.contains("standard_conforming_strings"), "{script}");
}

// ── tables pgc 1.0.x could not see ─────────────────────────────────────

/// pgc 1.0.x filtered `tablename not like 'pg_%'`, where `_` matches any
/// character: every name of three or more characters starting with `pg`
/// was left out of its dumps.
#[test]
fn tables_hidden_from_pgc_1_0_lists_the_to_only_pg_wildcard_tables() {
    let mut from = dump_written_by(None);
    from.tables.push(rv_mixed_table("public", "pgx_in_both"));
    let mut to = dump_written_by(Some(180000));
    for (schema, name) in [
        ("app", "pgbench_accounts"),
        ("public", "pg1_x"),
        ("public", "pgx_in_both"),
        // Two characters: `pg_%` needs three, so pgc 1.0.x did dump it.
        ("public", "pg"),
        ("public", "orders"),
        ("public", "Pgx"),
    ] {
        to.tables.push(rv_mixed_table(schema, name));
    }
    let comparer = Comparer::new(from, to, false, false, false, GrantsMode::Ignore);

    assert_eq!(
        comparer.tables_hidden_from_pgc_1_0(),
        vec![
            "app.pgbench_accounts".to_string(),
            "public.pg1_x".to_string()
        ]
    );
}

#[test]
fn tables_hidden_from_pgc_1_0_is_empty_unless_the_from_dump_predates_the_to_dump() {
    for (from, to) in [(None, None), (Some(180000), Some(180000))] {
        let mut to_dump = dump_written_by(to);
        to_dump
            .tables
            .push(rv_mixed_table("app", "pgbench_accounts"));
        let comparer = Comparer::new(
            dump_written_by(from),
            to_dump,
            false,
            false,
            false,
            GrantsMode::Ignore,
        );
        assert!(
            comparer.tables_hidden_from_pgc_1_0().is_empty(),
            "from {from:?}, to {to:?}"
        );
    }
}

/// A table matched by its raw catalog name, which is what the LIKE saw: a
/// quoted name that begins with `pg` counts too.
#[test]
fn tables_hidden_from_pgc_1_0_reads_the_raw_name() {
    let mut to = dump_written_by(Some(180000));
    let mut quoted = Table::new(
        "public".to_string(),
        "\"pg Space\"".to_string(),
        "public".to_string(),
        "pg Space".to_string(),
        "postgres".to_string(),
        None,
        vec![int_column("public", "\"pg Space\"", "id", 1)],
        vec![],
        vec![],
        vec![],
        None,
    );
    quoted.hash();
    to.tables.push(quoted);
    let comparer = Comparer::new(
        dump_written_by(None),
        to,
        false,
        false,
        false,
        GrantsMode::Ignore,
    );

    assert_eq!(
        comparer.tables_hidden_from_pgc_1_0(),
        vec!["public.\"pg Space\"".to_string()]
    );
}

// ── the stderr warnings ─────────────────────────────────────────────────

/// The first stderr warning of a mixed-version compare.
const MIXED_WARNING_START: &str = "Warning: the 'from' dump was written by pgc 1.0.x";

/// Pinned so a reworded warning is a deliberate change. In particular it
/// must not promise that production output makes an existing hidden table
/// harmless: it skips the `CREATE TABLE` only, while the table's separately
/// added constraints still fail and its other differences are dropped.
#[test]
fn the_general_warning_text_is_pinned() {
    let comparer = rv_mixed_comparer(None, Some(180000), false, false);
    let warnings = comparer.legacy_from_warnings();

    assert_eq!(
        warnings[0],
        "Warning: the 'from' dump was written by pgc 1.0.x and the 'to' dump by pgc 1.1 or \
         later. The migration re-applies what pgc 1.0.x could not read: SQL-standard routines \
         are re-created, publications and replica identities are set again, and literals \
         holding a backslash are rewritten (so the migration starts with set \
         standard_conforming_strings = on;). It also creates every 'to' table whose name \
         starts with pg but not pg_, which pgc 1.0.x could not see, and every 'to' \
         subscription, because pgc 1.0.x dumped the subscriptions of every database in the \
         cluster; where 'from' already has one, its CREATE fails with 'already exists'. \
         Production output skips the CREATE TABLE of such a table instead, but not the \
         constraints added to it separately, which fail with 'already exists' as well, and \
         any other difference in that table is silently left unapplied. Re-dump 'from' with \
         pgc 1.1 or later to compare without these."
    );
    // Just the general warning: `widgets` does not start with `pg`, and
    // neither dump has a subscription.
    assert_eq!(warnings.len(), 1, "{warnings:?}");
}

#[test]
fn the_warnings_are_empty_for_every_other_pairing() {
    for (from, to) in [
        (None, None),
        (Some(180000), Some(190000)),
        (Some(190000), None),
    ] {
        let mut from_dump = dump_written_by(from);
        from_dump
            .subscriptions
            .push(rv_mixed_subscription("s_shared", true));
        let mut to_dump = dump_written_by(to);
        to_dump
            .tables
            .push(rv_mixed_table("app", "pgbench_accounts"));
        to_dump
            .subscriptions
            .push(rv_mixed_subscription("s_shared", true));
        let comparer = Comparer::new(from_dump, to_dump, false, false, false, GrantsMode::Ignore);

        assert!(
            comparer.legacy_from_warnings().is_empty(),
            "from {from:?}, to {to:?}"
        );
    }
}

/// Besides the general warning, one line names the tables pgc 1.0.x could
/// not see and one the subscriptions it could not place.
#[test]
fn the_warnings_name_the_hidden_tables_and_the_unplaced_subscriptions() {
    let (from, mut to) = rv_mixed_subscription_dumps();
    to.tables.push(rv_mixed_table("app", "pgbench_accounts"));
    let comparer = Comparer::new(from, to, true, false, false, GrantsMode::Ignore);
    let warnings = comparer.legacy_from_warnings();

    assert_eq!(warnings.len(), 3, "{warnings:?}");
    assert!(warnings[0].starts_with(MIXED_WARNING_START), "{warnings:?}");
    assert_eq!(
        warnings[1],
        "Warning: the migration creates these tables, which a pgc 1.0.x dump cannot see, so \
         'from' may already have them: app.pgbench_accounts."
    );
    assert_eq!(
        warnings[2],
        "Warning: the migration creates these subscriptions, which the pgc 1.0.x 'from' dump \
         lists without saying which database has them, so 'from' may already have them: \
         s_shared, s_changed."
    );
}

// ── subscriptions ──────────────────────────────────────────────────────

/// A subscription, hashed; `enabled` stands for its whole definition.
fn rv_mixed_subscription(name: &str, enabled: bool) -> Subscription {
    let mut sub = Subscription {
        name: name.into(),
        owner: "postgres".into(),
        connection: "host=127.0.0.1 port=1 dbname=nowhere".into(),
        server: None,
        publications: vec!["p".into()],
        enabled,
        slot_name: Default::default(),
        retain_dead_tuples: false,
        max_retention_duration: 0,
        wal_receiver_timeout: None,
        comment: None,
        hash: None,
    };
    sub.hash();
    sub
}

/// A pgc 1.0.x FROM dump listing the subscriptions of a whole cluster, and a
/// pgc 1.1+ TO dump of one database:
/// - `s_shared`: in TO, and listed by FROM with the same definition — by
///   another database, for all the FROM dump can say;
/// - `s_changed`: in TO, listed by FROM with another definition, and
///   `retain_dead_tuples` would bounce an enabled worker;
/// - `s_new`: in TO only;
/// - `s_twice`: listed by FROM twice (two databases), not in TO.
fn rv_mixed_subscription_dumps() -> (Dump, Dump) {
    let mut from = dump_written_by(None);
    from.subscriptions
        .push(rv_mixed_subscription("s_shared", true));
    from.subscriptions
        .push(rv_mixed_subscription("s_changed", true));
    from.subscriptions
        .push(rv_mixed_subscription("s_twice", false));
    from.subscriptions
        .push(rv_mixed_subscription("s_twice", true));
    let mut to = dump_written_by(Some(180000));
    to.subscriptions
        .push(rv_mixed_subscription("s_shared", true));
    let mut changed = rv_mixed_subscription("s_changed", true);
    changed.retain_dead_tuples = true;
    changed.hash();
    to.subscriptions.push(changed);
    to.subscriptions.push(rv_mixed_subscription("s_new", true));
    (from, to)
}

/// A pgc 1.0.x FROM dump holds the subscriptions of every database in the
/// cluster, so matching TO against it by name would take another database's
/// subscription for FROM's own: an identical one would never be created, a
/// different one altered from the wrong definition. Every TO subscription
/// is created instead, which fails loudly ("already exists") where FROM has
/// it; a FROM-only name is dropped once, with `IF EXISTS`.
#[tokio::test]
async fn a_1_0_from_dump_gets_every_to_subscription_created() {
    let (from, to) = rv_mixed_subscription_dumps();
    let mut comparer = Comparer::new(from, to, true, false, false, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();

    for name in ["s_shared", "s_changed", "s_new"] {
        assert_eq!(
            script
                .matches(&format!("CREATE SUBSCRIPTION {name} "))
                .count(),
            1,
            "{name} must be created:\n{script}"
        );
    }
    assert!(
        !script.contains("ALTER SUBSCRIPTION s_changed DISABLE"),
        "{script}"
    );
    assert!(!script.contains("SET (retain_dead_tuples"), "{script}");
    assert_eq!(
        script
            .matches("DROP SUBSCRIPTION IF EXISTS s_twice;")
            .count(),
        1,
        "{script}"
    );
    assert!(
        !script.contains("DROP SUBSCRIPTION IF EXISTS s_shared"),
        "{script}"
    );
}

/// Nothing is altered, so nothing bounces an apply worker.
#[test]
fn a_1_0_from_dump_raises_no_worker_restart_warning() {
    let (from, to) = rv_mixed_subscription_dumps();
    let comparer = Comparer::new(from, to, true, true, false, GrantsMode::Ignore);

    assert!(comparer.worker_restart_warning().is_none());
}

/// Dumps of one generation still match subscriptions by name, and two pgc
/// 1.0.x dumps keep dropping a name once per listed subscription, exactly as
/// pgc 1.0.x did.
#[tokio::test]
async fn subscriptions_of_one_generation_are_matched_by_name_as_before() {
    for version in [None, Some(180000)] {
        let (mut from, mut to) = rv_mixed_subscription_dumps();
        from.server_version_num = version;
        to.server_version_num = version;
        let mut comparer = Comparer::new(from, to, true, true, false, GrantsMode::Ignore);
        comparer.compare().await.unwrap();
        let script = comparer.get_script();

        assert!(!script.contains("CREATE SUBSCRIPTION s_shared"), "{script}");
        assert!(
            !script.contains("CREATE SUBSCRIPTION s_changed"),
            "{script}"
        );
        assert!(
            script.contains("ALTER SUBSCRIPTION s_changed DISABLE;"),
            "{script}"
        );
        assert!(script.contains("CREATE SUBSCRIPTION s_new "), "{script}");
        assert_eq!(
            script
                .matches("DROP SUBSCRIPTION IF EXISTS s_twice;")
                .count(),
            2,
            "{version:?}:\n{script}"
        );
        assert!(comparer.worker_restart_warning().is_some(), "{version:?}");
    }
}

// ==== r3-ddl tests ====

/// A materialized view `app.<name>` over `app.src`, as a dump stores it
/// (`name` already in the dump's spelling), hashed.
fn rv_r3d_matview(name: &str) -> crate::dump::view::View {
    let mut view = crate::dump::view::View::new(
        name.to_string(),
        " SELECT 1 AS x;".to_string(),
        "app".to_string(),
        vec![],
    );
    view.is_materialized = true;
    view.owner = "postgres".to_string();
    view.hash();
    view
}

/// A pgc 1.0.x FROM dump, loaded (its matview names spelled for
/// PostgreSQL 14), holding the views `names` raw.
fn rv_r3d_legacy_matviews(names: &[&str]) -> Dump {
    let mut legacy = dump_written_by(None);
    legacy.views = names.iter().map(|name| rv_r3d_matview(name)).collect();
    legacy
}

/// A 1.0.x dump does not say its server's version, so a view named after a
/// keyword only PostgreSQL 16 or 17 quotes is matched by the TO dump's
/// version: bare against PostgreSQL 15, which stores `json_object` bare,
/// and quoted against 17, which stores `"json_object"`. Either way nothing
/// is dropped or recreated. Loading used to quote it for PostgreSQL 19
/// whatever the server, so a 1.0.x dump of PostgreSQL 14–16 had it dropped
/// and recreated (review of r3-ddl).
#[tokio::test]
async fn a_1_0_matview_named_after_a_newer_keyword_matches_the_to_servers_spelling() {
    for (version, to_names) in [
        (150000, ["json_object", "merge_action", "json", "\"left\""]),
        (
            160000,
            ["\"json_object\"", "merge_action", "json", "\"left\""],
        ),
        (
            170000,
            [
                "\"json_object\"",
                "\"merge_action\"",
                "\"json\"",
                "\"left\"",
            ],
        ),
    ] {
        // As `Dump::read_from_file` leaves a 1.0.x dump: `left` is quoted on
        // load, the newer keywords are not.
        let from = rv_r3d_legacy_matviews(&["json_object", "merge_action", "json", "\"left\""]);
        let mut to = dump_written_by(Some(version));
        to.views = to_names.iter().map(|name| rv_r3d_matview(name)).collect();

        let mut comparer = Comparer::new(from, to, true, false, false, GrantsMode::Ignore);
        comparer.compare().await.unwrap();
        let script = comparer.get_script();
        assert!(
            !script.contains("materialized view"),
            "PostgreSQL {version}: nothing to migrate:\n{script}"
        );
    }
}

/// Two 1.0.x dumps: neither version is known, both keep PostgreSQL 14's
/// spelling, and they still match each other.
#[tokio::test]
async fn two_1_0_dumps_keep_the_newer_keywords_bare() {
    let from = rv_r3d_legacy_matviews(&["json_object", "json"]);
    let to = rv_r3d_legacy_matviews(&["json_object", "json"]);
    let mut comparer = Comparer::new(from, to, true, false, false, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();
    assert!(!script.contains("materialized view"), "{script}");
}
