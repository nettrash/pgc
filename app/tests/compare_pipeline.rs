//! End-to-end tests for the `compare` command's public path.
//!
//! These drive the same sequence `main.rs` does — build two [`Dump`]s, write
//! them out, read them back, hand them to [`Comparer`], and save the script —
//! so they cover the dump→file→dump hop that the in-memory unit tests skip.

mod common;

use common::{ScratchDir, assert_no_ddl, index, populated_dump, schema, table, view};
use pgc::comparer::core::Comparer;
use pgc::config::grants_mode::GrantsMode;
use pgc::dump::core::Dump;

/// Round-trip both dumps through the filesystem, then compare them, exactly as
/// `pgc --command compare --from … --to …` does.
async fn compare_via_files(from: Dump, to: Dump, use_drop: bool, label: &str) -> String {
    let dir = ScratchDir::new(label);
    let from_path = dir.path_str("dump.from");
    let to_path = dir.path_str("dump.to");
    let out_path = dir.path_str("output.sql");

    from.write_to_file(&from_path).expect("write FROM dump");
    to.write_to_file(&to_path).expect("write TO dump");

    let from = Dump::read_from_file(&from_path)
        .await
        .expect("read FROM dump");
    let to = Dump::read_from_file(&to_path).await.expect("read TO dump");
    Comparer::check_dump_versions(&from, &to).expect("dumps of comparable pgc versions");

    let mut comparer = Comparer::new(from, to, use_drop, false, true, GrantsMode::Ignore);
    comparer.compare().await.expect("compare");
    comparer.save_script(&out_path).await.expect("save script");

    std::fs::read_to_string(&out_path).expect("read generated script")
}

/// The core correctness property, at the level this test can reach without a
/// live server: a schema compared against itself needs no migration.
#[tokio::test]
async fn comparing_a_dump_against_itself_emits_no_ddl() {
    let script = compare_via_files(
        populated_dump("shop"),
        populated_dump("shop"),
        true,
        "self-compare",
    )
    .await;

    assert_no_ddl(&script, "self-comparison");
}

#[tokio::test]
async fn a_new_table_is_created_and_a_removed_one_is_dropped() {
    let from = populated_dump("shop");
    let mut to = populated_dump("shop");
    to.tables.push(table("app", "invoices", &["id", "total"]));
    to.tables.retain(|t| t.name != "orders");
    // `order_summary` reads `app.orders`; drop it too so the TO side is
    // self-consistent.
    to.views.retain(|v| v.name != "order_summary");

    let script = compare_via_files(from, to, true, "add-drop-table").await;

    assert!(
        script.contains("create table") || script.contains("CREATE TABLE"),
        "expected a CREATE TABLE for app.invoices:\n{script}"
    );
    assert!(script.contains("invoices"), "new table missing:\n{script}");
    assert!(
        script.to_lowercase().contains("drop table"),
        "expected a DROP TABLE for app.orders:\n{script}"
    );
}

/// With `use_drop = false` the destructive statements must still be *present*,
/// but commented out, so a reviewer can see and opt into them.
#[tokio::test]
async fn destructive_statements_are_commented_out_when_use_drop_is_false() {
    let from = populated_dump("shop");
    let mut to = populated_dump("shop");
    to.tables.retain(|t| t.name != "orders");
    to.views.retain(|v| v.name != "order_summary");

    let script = compare_via_files(from, to, false, "no-drop").await;

    let uncommented_drop = script.lines().any(|l| {
        let l = l.trim().to_lowercase();
        l.starts_with("drop table")
    });
    assert!(
        !uncommented_drop,
        "use_drop=false must not emit an active DROP TABLE:\n{script}"
    );
}

#[tokio::test]
async fn a_changed_view_definition_is_re_emitted() {
    let from = populated_dump("shop");
    let mut to = populated_dump("shop");
    to.views.retain(|v| v.name != "order_summary");
    to.views.push(view(
        "app",
        "order_summary",
        " SELECT o.id\n   FROM app.orders o;",
        &["app.orders"],
    ));

    let script = compare_via_files(from, to, true, "view-change").await;
    let lower = script.to_lowercase();
    assert!(
        lower.contains("view app.order_summary") || lower.contains("view order_summary"),
        "expected app.order_summary to be re-emitted:\n{script}"
    );
}

#[tokio::test]
async fn single_transaction_wraps_the_script_in_begin_and_commit() {
    let dir = ScratchDir::new("single-tx");
    let out_path = dir.path_str("output.sql");

    let mut to = populated_dump("shop");
    to.tables.push(table("app", "invoices", &["id"]));

    let mut comparer = Comparer::new(
        populated_dump("shop"),
        to,
        true,
        true, // use_single_transaction
        true,
        GrantsMode::Ignore,
    );
    comparer.compare().await.expect("compare");
    comparer.save_script(&out_path).await.expect("save script");

    let script = std::fs::read_to_string(&out_path).expect("read script");
    let lower = script.to_lowercase();
    let begin = lower
        .find("begin;")
        .expect("script must open a transaction");
    let commit = lower.rfind("commit;").expect("script must commit");
    assert!(begin < commit, "begin must precede commit:\n{script}");
}

/// `--output-for-production` adds idempotency guards and moves statements that
/// cannot run inside a transaction into a post-commit section. Off by default.
///
/// The assertions below deliberately name whole statements rather than looking
/// for a bare `if not exists`. That substring is **not** production-specific:
/// `Schema::get_script` and `Extension::get_script` emit `create schema if not
/// exists` / `create extension if not exists` on the default path too, so the
/// fixture below — which adds a schema — contains it either way. A test
/// matching only the substring would keep passing if the rewrite stopped
/// working entirely.
#[tokio::test]
async fn production_mode_is_opt_in() {
    let build = |production: bool| async move {
        let mut to = populated_dump("shop");
        // A new schema: emits `create schema if not exists` on BOTH paths, and
        // is what makes the naive substring check useless.
        to.schemas.push(schema("reporting"));
        // A new table with an index: `create table` gains a guard and the index
        // build becomes CONCURRENTLY, only under production mode.
        let mut invoices = table("app", "invoices", &["id"]);
        invoices
            .indexes
            .push(index("app", "invoices", "ix_invoices_id", "id"));
        invoices.hash();
        to.tables.push(invoices);

        let mut comparer = Comparer::new(
            populated_dump("shop"),
            to,
            true,
            true,
            true,
            GrantsMode::Ignore,
        );
        comparer.set_output_for_production(production);
        comparer.compare().await.expect("compare");
        let dir = ScratchDir::new(if production { "prod-on" } else { "prod-off" });
        let out = dir.path_str("output.sql");
        comparer.save_script(&out).await.expect("save script");
        std::fs::read_to_string(&out)
            .expect("read script")
            .to_lowercase()
    };

    let default_script = build(false).await;
    let production_script = build(true).await;

    // The confound, pinned: present on both paths, so it discriminates nothing.
    for (label, script) in [
        ("default", &default_script),
        ("production", &production_script),
    ] {
        assert!(
            script.contains("create schema if not exists reporting;"),
            "{label} output should carry the unguarded-by-production schema create:\n{script}"
        );
    }

    // Default path: no guard on CREATE TABLE, index built inline, no
    // post-commit section.
    assert!(
        default_script.contains("create table app.invoices"),
        "default output must create the table without a guard:\n{default_script}"
    );
    assert!(
        !default_script.contains("create table if not exists app.invoices"),
        "the CREATE TABLE guard is production-only:\n{default_script}"
    );
    assert!(
        !default_script.contains("concurrently"),
        "default output must build indexes inline:\n{default_script}"
    );
    assert!(
        !default_script.contains("post-commit"),
        "default output must not carry a post-commit section:\n{default_script}"
    );

    // Production path: guarded CREATE TABLE, CONCURRENTLY index, and the
    // statement that cannot run in a transaction emitted after the commit.
    assert!(
        production_script.contains("create table if not exists app.invoices"),
        "production output must guard CREATE TABLE:\n{production_script}"
    );
    assert!(
        production_script.contains("create index concurrently if not exists ix_invoices_id"),
        "production output must build the index concurrently:\n{production_script}"
    );

    let commit = production_script
        .rfind("commit;")
        .expect("production script commits");
    let concurrent_index = production_script
        .find("create index concurrently")
        .expect("production script builds the index concurrently");
    assert!(
        concurrent_index > commit,
        "CREATE INDEX CONCURRENTLY cannot run inside a transaction, so it must \
         be emitted after commit:\n{production_script}"
    );
}

// ==== reg:mixed tests ====

// Dumps written by different pgc versions. A dump written by pgc 1.1 or later
// records `server_version_num`; one written by pgc 1.0.x does not, which is
// all these fixtures need to differ in. `pgc --command compare` refuses a
// pgc 1.0.x TO dump against a pgc 1.1+ FROM dump, and compares the opposite
// pairing with a warning, a comment and the standard_conforming_strings guard.

/// Where `cargo test` built the `pgc` binary.
const RV_MIXED_PGC: &str = env!("CARGO_BIN_EXE_pgc");

/// Write `dump` to `dir/name` as the given pgc generation wrote it: with a
/// `server_version_num` for pgc 1.1+, without one for pgc 1.0.x.
fn rv_mixed_write(dir: &ScratchDir, name: &str, mut dump: Dump, pgc_1_1: bool) -> String {
    dump.server_version_num = pgc_1_1.then_some(180000);
    let path = dir.path_str(name);
    dump.write_to_file(&path).expect("write dump");
    path
}

/// The JSON document inside a dump file.
fn rv_mixed_json(path: &str) -> serde_json::Value {
    use std::io::Read;
    let mut archive =
        zip::ZipArchive::new(std::fs::File::open(path).expect("open dump")).expect("read zip");
    let mut json = String::new();
    archive
        .by_name("dump.io")
        .expect("dump.io entry")
        .read_to_string(&mut json)
        .expect("utf-8 json");
    serde_json::from_str(&json).expect("valid json")
}

/// `pgc --command compare --from <from> --to <to> --output <output>` plus
/// `extra`, as the user runs it: (exit status, stderr).
fn rv_mixed_run_compare(
    from: &str,
    to: &str,
    output: &str,
    extra: &[&str],
) -> (std::process::ExitStatus, String) {
    let result = std::process::Command::new(RV_MIXED_PGC)
        .args([
            "--command",
            "compare",
            "--from",
            from,
            "--to",
            to,
            "--output",
            output,
        ])
        .args(extra)
        .output()
        .expect("run pgc");
    (
        result.status,
        String::from_utf8_lossy(&result.stderr).into_owned(),
    )
}

/// The TO side of the mixed-version tests: `populated_dump` plus a table
/// named `pgbench_accounts`, which pgc 1.0.x could not see, and a table with
/// a backslash in a default.
fn rv_mixed_to_dump() -> Dump {
    let mut to = populated_dump("shop");
    to.tables.push(table("app", "pgbench_accounts", &["aid"]));
    let mut files = table("app", "rv_mixed_files", &["id", "path"]);
    let path = &mut files.columns[1];
    path.data_type = "text".to_string();
    path.numeric_precision = None;
    path.numeric_precision_radix = None;
    path.numeric_scale = None;
    path.column_default = Some(r"'C:\temp'::text".to_string());
    files.hash();
    to.tables.push(files);
    to
}

#[tokio::test]
async fn mixed_fixture_files_differ_only_in_the_recorded_version() {
    let dir = ScratchDir::new("mixed-shape");
    let legacy = rv_mixed_write(&dir, "legacy.dump", populated_dump("shop"), false);
    let current = rv_mixed_write(&dir, "current.dump", populated_dump("shop"), true);

    assert!(rv_mixed_json(&legacy).get("server_version_num").is_none());
    assert_eq!(rv_mixed_json(&current)["server_version_num"], 180000);
    assert_eq!(
        Dump::read_from_file(&legacy)
            .await
            .expect("read")
            .server_version_num,
        None
    );
}

#[tokio::test]
async fn the_library_refuses_a_1_1_from_file_against_a_1_0_to_file() {
    let dir = ScratchDir::new("mixed-refuse-lib");
    let from = rv_mixed_write(&dir, "from.dump", populated_dump("shop"), true);
    let to = rv_mixed_write(&dir, "to.dump", populated_dump("shop"), false);
    let from = Dump::read_from_file(&from).await.expect("read FROM dump");
    let to = Dump::read_from_file(&to).await.expect("read TO dump");

    let err = Comparer::check_dump_versions(&from, &to).expect_err("must refuse");
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    assert!(Comparer::check_dump_versions(&to, &from).is_ok());
}

/// The same database dumped by both versions: pgc 1.0.x could not see
/// `app.pgbench_accounts`, so a migration toward its dump would drop it.
#[test]
fn pgc_compare_refuses_a_1_1_from_dump_against_a_1_0_to_dump() {
    let dir = ScratchDir::new("mixed-refuse-bin");
    let from = rv_mixed_write(&dir, "from.dump", rv_mixed_to_dump(), true);
    let mut legacy = rv_mixed_to_dump();
    legacy.tables.retain(|t| t.name != "pgbench_accounts");
    let to = rv_mixed_write(&dir, "to.dump", legacy, false);
    let output = dir.path_str("output.sql");

    let (status, stderr) = rv_mixed_run_compare(&from, &to, &output, &["--use-drop", "true"]);

    assert!(!status.success(), "pgc must exit non-zero:\n{stderr}");
    assert!(
        stderr.contains("the 'to' dump was written by pgc 1.0.x")
            && stderr.contains("Re-dump 'to' with pgc 1.1 or later and compare again."),
        "the refusal must say why and what to do:\n{stderr}"
    );
    assert!(
        !std::path::Path::new(&output).exists(),
        "no migration may be written"
    );
}

#[test]
fn pgc_compare_warns_and_guards_a_1_0_from_dump_against_a_1_1_to_dump() {
    let dir = ScratchDir::new("mixed-warn-bin");
    let from = rv_mixed_write(&dir, "from.dump", populated_dump("shop"), false);
    let to = rv_mixed_write(&dir, "to.dump", rv_mixed_to_dump(), true);
    let output = dir.path_str("output.sql");

    let (status, stderr) = rv_mixed_run_compare(&from, &to, &output, &[]);

    assert!(status.success(), "{stderr}");
    assert!(
        stderr.contains("Warning: the 'from' dump was written by pgc 1.0.x"),
        "{stderr}"
    );
    assert!(
        stderr.contains("'from' may already have them: app.pgbench_accounts."),
        "the tables pgc 1.0.x could not see are named:\n{stderr}"
    );
    let script = std::fs::read_to_string(&output).expect("read script");
    let comment = script
        .find("/* Warning: the FROM dump was written by pgc 1.0.x")
        .unwrap_or_else(|| panic!("no warning comment:\n{script}"));
    let guard = script
        .find("set standard_conforming_strings = on;")
        .unwrap_or_else(|| panic!("no guard:\n{script}"));
    let create = script
        .find("create table app.rv_mixed_files")
        .unwrap_or_else(|| panic!("no create:\n{script}"));
    assert!(comment < guard && guard < create, "{script}");
    assert!(script.contains(r"default 'C:\temp'::text"), "{script}");
}

/// Without comments the warning is on stderr only, and the guard is the
/// script's first statement.
#[test]
fn pgc_compare_of_a_1_0_from_dump_without_comments_starts_with_the_guard() {
    let dir = ScratchDir::new("mixed-warn-nocomments");
    let from = rv_mixed_write(&dir, "from.dump", populated_dump("shop"), false);
    let to = rv_mixed_write(&dir, "to.dump", rv_mixed_to_dump(), true);
    let output = dir.path_str("output.sql");

    let (status, stderr) = rv_mixed_run_compare(&from, &to, &output, &["--use-comments", "false"]);

    assert!(status.success(), "{stderr}");
    assert!(
        stderr.contains("Warning: the 'from' dump was written by pgc 1.0.x"),
        "{stderr}"
    );
    let script = std::fs::read_to_string(&output).expect("read script");
    assert!(
        script.starts_with("set standard_conforming_strings = on;\n\n"),
        "{script}"
    );
    assert!(!script.contains("pgc 1.0.x"), "{script}");
}

/// Two dumps of one generation compare as before: no refusal, no warning,
/// no comment and no guard.
#[test]
fn pgc_compare_of_two_dumps_of_one_generation_is_unchanged() {
    for pgc_1_1 in [false, true] {
        let dir = ScratchDir::new(if pgc_1_1 {
            "mixed-same-11"
        } else {
            "mixed-same-10"
        });
        let from = rv_mixed_write(&dir, "from.dump", populated_dump("shop"), pgc_1_1);
        let to = rv_mixed_write(&dir, "to.dump", rv_mixed_to_dump(), pgc_1_1);
        let output = dir.path_str("output.sql");

        let (status, stderr) = rv_mixed_run_compare(&from, &to, &output, &[]);

        assert!(status.success(), "{stderr}");
        assert!(!stderr.contains("pgc 1.0.x"), "{stderr}");
        let script = std::fs::read_to_string(&output).expect("read script");
        assert!(!script.contains("pgc 1.0.x"), "{script}");
        assert!(!script.contains("standard_conforming_strings"), "{script}");
        assert!(
            script.contains("create table app.pgbench_accounts"),
            "{script}"
        );
    }
}

/// A subscription as `pgc --command dump` records it, hashed.
fn rv_mixed_subscription(name: &str) -> pgc::dump::publication::Subscription {
    let mut sub = pgc::dump::publication::Subscription {
        name: name.to_string(),
        owner: "postgres".to_string(),
        connection: "host=127.0.0.1 port=1 dbname=nowhere".to_string(),
        server: None,
        publications: vec!["p".to_string()],
        enabled: false,
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

/// A pgc 1.0.x FROM dump lists the subscriptions of every database in the
/// cluster, so it cannot say whether FROM has `rv_mixed_sub`, which another
/// database may have: pgc creates it anyway, where it would fail loudly if
/// FROM had it, instead of silently leaving it out, and names it on stderr.
/// `rv_mixed_gone`, listed twice (two databases), is dropped once. The
/// warning does not claim production output makes an existing hidden table
/// harmless.
#[test]
fn pgc_compare_of_a_1_0_from_dump_creates_every_subscription_and_says_so() {
    let dir = ScratchDir::new("mixed-subscriptions");
    let mut from = populated_dump("shop");
    from.subscriptions
        .push(rv_mixed_subscription("rv_mixed_sub"));
    from.subscriptions
        .push(rv_mixed_subscription("rv_mixed_gone"));
    from.subscriptions
        .push(rv_mixed_subscription("rv_mixed_gone"));
    let mut to = rv_mixed_to_dump();
    to.subscriptions.push(rv_mixed_subscription("rv_mixed_sub"));
    let from = rv_mixed_write(&dir, "from.dump", from, false);
    let to = rv_mixed_write(&dir, "to.dump", to, true);

    for production in ["false", "true"] {
        let output = dir.path_str(&format!("output-{production}.sql"));
        let (status, stderr) = rv_mixed_run_compare(
            &from,
            &to,
            &output,
            &["--use-drop", "true", "--output-for-production", production],
        );

        assert!(status.success(), "{stderr}");
        assert!(
            stderr.contains("'from' may already have them: rv_mixed_sub."),
            "the subscription pgc 1.0.x could not place is named:\n{stderr}"
        );
        assert!(
            stderr.contains("but not the constraints added to it separately")
                && !stderr.contains("production output skips it"),
            "production output must not be described as safe:\n{stderr}"
        );
        let script = std::fs::read_to_string(&output).expect("read script");
        assert_eq!(
            script
                .matches("CREATE SUBSCRIPTION rv_mixed_sub CONNECTION")
                .count(),
            1,
            "{script}"
        );
        assert_eq!(
            script
                .matches("DROP SUBSCRIPTION IF EXISTS rv_mixed_gone;")
                .count(),
            1,
            "{script}"
        );
    }

    // The same pair written by one generation matches by name, as before.
    let same_from = rv_mixed_write(
        &dir,
        "same-from.dump",
        {
            let mut d = populated_dump("shop");
            d.subscriptions.push(rv_mixed_subscription("rv_mixed_sub"));
            d
        },
        true,
    );
    let output = dir.path_str("output-same.sql");
    let (status, stderr) = rv_mixed_run_compare(&same_from, &to, &output, &[]);
    assert!(status.success(), "{stderr}");
    let script = std::fs::read_to_string(&output).expect("read script");
    assert!(!script.contains("SUBSCRIPTION rv_mixed_sub"), "{script}");
}

// ==== r3-compat tests ====

// A dump whose `--scheme` pattern matches no accessible schema — the FROM of
// a first deployment into a fresh database, or a TO from which the whole
// schema is gone — used to be written without `server_version_num`, as if by
// pgc 1.0.x: as TO it made the compare refuse, as FROM it got the 1.0.x
// warning, the standard_conforming_strings guard and pre-PostgreSQL 19 DDL.
// It also held none of the database-wide objects (event triggers,
// publications, subscriptions, foreign-data wrappers, servers, user mappings,
// extensions in `public`) that a dump of any schema of the same database
// holds, so a compare against it dropped every one of them. `Dump::fill` now
// runs in full for such a dump; these pin how one is compared.

/// An empty dump as `pgc --command dump` writes it for a pattern matching no
/// schema on a server of `version`.
fn rv_compat_empty_dump(version: i32) -> Dump {
    let mut dump = common::empty_dump("fresh");
    dump.server_version_num = Some(version);
    dump
}

/// Database-wide objects, as `pgc --command dump` reads them whatever the
/// `--scheme` pattern: `populated_dump`'s `pgcrypto` in `public`, plus a
/// publication and an event trigger.
fn rv_compat_add_database_wide(dump: &mut Dump) {
    use pgc::dump::event_trigger::EventTrigger;
    use pgc::dump::publication::Publication;

    dump.extensions = vec![common::extension("pgcrypto", "1.3", "public")];
    let mut publication = Publication {
        name: "rv_compat_pub".into(),
        owner: "postgres".into(),
        all_tables: true,
        publish: "insert, update, delete, truncate".into(),
        ..Default::default()
    };
    publication.hash();
    dump.publications = vec![publication];
    dump.event_triggers = vec![EventTrigger::new(
        "rv_compat_et".into(),
        "ddl_command_start".into(),
        "public.rv_compat_et()".into(),
        Vec::new(),
        "O".into(),
        "postgres".into(),
        None,
    )];
}

/// A statement, in any case, that touches one of the database-wide objects
/// of [`rv_compat_add_database_wide`].
fn rv_compat_database_wide_ddl(script: &str) -> Vec<String> {
    let lower = common::strip_sql_comments(script).to_lowercase();
    ["extension", "publication", "event trigger"]
        .iter()
        .flat_map(|kind| ["create", "drop", "alter"].map(|verb| format!("{verb} {kind}")))
        .filter(|statement| lower.contains(statement.as_str()))
        .collect()
}

#[test]
fn pgc_compare_against_an_empty_to_dump_drops_instead_of_refusing() {
    let dir = ScratchDir::new("compat-empty-to");
    let mut from = populated_dump("shop");
    rv_compat_add_database_wide(&mut from);
    let from = rv_mixed_write(&dir, "from.dump", from, true);
    // What `pgc --command dump` writes for a pattern matching no schema of a
    // database that shares FROM's database-wide objects.
    let to = dir.path_str("to.dump");
    let mut empty = rv_compat_empty_dump(180000);
    rv_compat_add_database_wide(&mut empty);
    empty.write_to_file(&to).expect("write dump");
    let output = dir.path_str("output.sql");

    let (status, stderr) = rv_mixed_run_compare(&from, &to, &output, &["--use-drop", "true"]);

    assert!(status.success(), "an empty TO must be compared:\n{stderr}");
    assert!(!stderr.contains("pgc 1.0.x"), "{stderr}");
    let script = std::fs::read_to_string(&output).expect("read script");
    for table in ["app.orders", "app.customers"] {
        assert!(
            script.contains(&format!("drop table if exists {table}")),
            "{table} must be dropped:\n{script}"
        );
    }
    assert!(!script.contains("pgc 1.0.x"), "{script}");
    assert_eq!(
        rv_compat_database_wide_ddl(&script),
        Vec::<String>::new(),
        "both databases hold the database-wide objects:\n{script}"
    );
}

/// A first deployment into a database that already holds the database-wide
/// objects TO has: the empty FROM dump lists them, so the migration does not
/// create them again (which failed on the first `CREATE` under
/// `ON_ERROR_STOP`).
#[test]
fn pgc_compare_from_an_empty_from_dump_keeps_the_database_wide_objects_it_shares() {
    let dir = ScratchDir::new("compat-empty-from-wide");
    let from = dir.path_str("from.dump");
    let mut empty = rv_compat_empty_dump(180000);
    rv_compat_add_database_wide(&mut empty);
    empty.write_to_file(&from).expect("write dump");
    let mut to = populated_dump("shop");
    rv_compat_add_database_wide(&mut to);
    let to = rv_mixed_write(&dir, "to.dump", to, true);
    let output = dir.path_str("output.sql");

    let (status, stderr) = rv_mixed_run_compare(&from, &to, &output, &["--use-drop", "true"]);

    assert!(status.success(), "{stderr}");
    let script = std::fs::read_to_string(&output).expect("read script");
    assert!(script.contains("create table app.orders"), "{script}");
    assert_eq!(
        rv_compat_database_wide_ddl(&script),
        Vec::<String>::new(),
        "both databases hold the database-wide objects:\n{script}"
    );
}

#[test]
fn pgc_compare_from_an_empty_from_dump_has_no_pgc_1_0_handling() {
    let dir = ScratchDir::new("compat-empty-from");
    let from = dir.path_str("from.dump");
    rv_compat_empty_dump(180000)
        .write_to_file(&from)
        .expect("write dump");
    let to = rv_mixed_write(&dir, "to.dump", rv_mixed_to_dump(), true);
    let output = dir.path_str("output.sql");

    let (status, stderr) = rv_mixed_run_compare(&from, &to, &output, &[]);

    assert!(status.success(), "{stderr}");
    assert!(!stderr.contains("pgc 1.0.x"), "{stderr}");
    let script = std::fs::read_to_string(&output).expect("read script");
    assert!(!script.contains("pgc 1.0.x"), "{script}");
    assert!(
        !script.contains("standard_conforming_strings"),
        "the FROM server's setting is known, and on:\n{script}"
    );
    assert!(
        script.contains("create table app.rv_mixed_files"),
        "{script}"
    );
}

/// The PostgreSQL 19 case of the finding: an empty FROM on 19 must keep the
/// forms only 19 accepts, or the migration does not converge — `SET
/// search_path = ''` stores `""`, and the next compare re-creates the routine
/// with `SET search_path TO NULL`.
#[tokio::test]
async fn an_empty_from_dump_on_postgresql_19_keeps_postgresql_19_forms() {
    let to_dump = || {
        let mut dump = populated_dump("shop");
        let mut routine = common::routine("app", 901, "guarded", "integer", "SELECT 1");
        routine.config = vec!["search_path=".to_string()];
        routine.hash();
        dump.routines.push(routine);
        dump.server_version_num = Some(190000);
        dump
    };

    let script = compare_via_files(
        rv_compat_empty_dump(190000),
        to_dump(),
        true,
        "compat-empty-from-19",
    )
    .await;
    assert!(script.contains(" SET search_path TO NULL "), "{script}");
    assert!(!script.contains("standard_conforming_strings"), "{script}");

    // What the same empty FROM got while it looked like a pgc 1.0.x dump.
    let mut legacy = rv_compat_empty_dump(190000);
    legacy.server_version_num = None;
    let script = compare_via_files(legacy, to_dump(), true, "compat-empty-from-legacy").await;
    assert!(script.contains(" SET search_path = '' "), "{script}");
}
