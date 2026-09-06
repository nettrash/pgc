//! Round-trip tests against a real PostgreSQL server.
//!
//! Ignored by default — they need a reachable database, so `cargo test` on a
//! bare checkout must not try them. Run explicitly:
//!
//! ```text
//! PGHOST=localhost PGDATABASE=postgres PGUSER=postgres PGPASSWORD=secret \
//!   cargo test --test live_database -- --ignored
//! ```
//!
//! CI's `integration` job covers the full contract (apply both fixture schemas,
//! compare, apply the diff, compare again → empty) across PostgreSQL 14–18 in
//! `.github/workflows/rust.yml`. These are the equivalent local smoke tests.

mod common;

use common::{ScratchDir, assert_no_ddl};
use pgc::comparer::core::Comparer;
use pgc::config::dump_config::DumpConfig;
use pgc::config::grants_mode::GrantsMode;
use pgc::dump::core::Dump;

/// Connection details from the standard `PG*` environment variables, with the
/// same defaults as the `pgc` CLI.
fn env_config(file: &str) -> DumpConfig {
    let var =
        |name: &str, fallback: &str| std::env::var(name).unwrap_or_else(|_| fallback.to_string());
    DumpConfig {
        host: var("PGHOST", "localhost"),
        port: var("PGPORT", "5432"),
        user: var("PGUSER", "postgres"),
        password: var("PGPASSWORD", ""),
        database: var("PGDATABASE", "postgres"),
        scheme: var("PGC_TEST_SCHEME", "public"),
        ssl: false,
        file: file.to_string(),
    }
}

#[tokio::test]
#[ignore = "needs a reachable PostgreSQL server; run with --ignored"]
async fn dump_of_a_live_database_round_trips_through_a_file() {
    let dir = ScratchDir::new("live-roundtrip");
    let path = dir.path_str("live.dump");

    let mut dump = Dump::new(env_config(&path));
    dump.process(8).await.expect("dump the live database");

    let reloaded = Dump::read_from_file(&path)
        .await
        .expect("read back the dump pgc just wrote");
    assert_eq!(dump.get_info(), reloaded.get_info());
}

/// The idempotency contract at the database level: a schema compared against
/// itself needs no migration.
#[tokio::test]
#[ignore = "needs a reachable PostgreSQL server; run with --ignored"]
async fn live_database_compared_against_itself_emits_no_ddl() {
    let dir = ScratchDir::new("live-self");
    let path = dir.path_str("live.dump");

    let mut dump = Dump::new(env_config(&path));
    dump.process(8).await.expect("dump the live database");

    let from = Dump::read_from_file(&path).await.expect("read FROM");
    let to = Dump::read_from_file(&path).await.expect("read TO");

    let mut comparer = Comparer::new(from, to, true, false, true, GrantsMode::Full);
    comparer.compare().await.expect("compare");

    let out = dir.path_str("output.sql");
    comparer.save_script(&out).await.expect("save script");
    let script = std::fs::read_to_string(&out).expect("read script");

    assert_no_ddl(&script, "self-comparison of a live database");
}

#[tokio::test]
#[ignore = "needs a reachable PostgreSQL server; run with --ignored"]
async fn inspect_populates_a_dump_without_writing_a_file() {
    let dir = ScratchDir::new("live-inspect");
    let path = dir.path_str("never-written.dump");

    let mut dump = Dump::new(env_config(&path));
    dump.inspect(8).await.expect("inspect the live database");

    assert!(
        !dump.schemas.is_empty(),
        "inspect must find at least one schema"
    );
    assert!(
        !std::path::Path::new(&path).exists(),
        "inspect must not write the dump file"
    );
    // The clear script is generated from an inspected dump, so it must be
    // buildable straight after.
    assert!(!dump.generate_clear_script(true, true, false).is_empty());
}

/// Issue #244: a password full of URL-reserved characters must reach the
/// server as typed.
///
/// The unit tests can only show that the credential is not rewritten on its
/// way into `PgConnectOptions`. Whether the server accepts it is a question
/// only the server can answer, and it is the question that was actually
/// wrong: `#`, `/` and `?` used to fail as `invalid port number` before any
/// socket was opened, and `pass%41word` used to authenticate as `passAword`.
///
/// Needs a role-creating connection (the `PG*` user must have CREATEROLE or
/// be a superuser), which is what the documented local and CI setups use.
#[tokio::test]
#[ignore = "needs a reachable PostgreSQL server; run with --ignored"]
async fn a_password_of_reserved_characters_authenticates() {
    // Every character the URL form mangled, in one password: the three that
    // ended the authority section, the escape that was silently decoded, and
    // the two that survived by luck.
    const PASSWORD: &str = "p#ss/w?rd%41@x%";
    const ROLE: &str = "pgc_issue244_role";

    let admin = env_config("");
    let mut admin_dump = Dump::new(admin.clone());
    admin_dump
        .inspect(4)
        .await
        .expect("connect to the live database as the configured user");

    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect_with(admin.get_connect_options().expect("admin configuration"))
        .await
        .expect("open an admin connection");

    // `CREATE ROLE` takes the password as a plain SQL literal, so only the
    // single quotes need escaping — this is not a URL either.
    sqlx::query(&format!(r#"drop role if exists {ROLE}"#))
        .execute(&pool)
        .await
        .expect("clean up any leftover role");
    sqlx::query(&format!(
        r#"create role {ROLE} login password '{}'"#,
        PASSWORD.replace('\'', "''")
    ))
    .execute(&pool)
    .await
    .expect("create the test role");
    let dir = ScratchDir::new("live-issue244");
    let mut as_role = env_config(&dir.path_str("issue244.dump"));
    as_role.user = ROLE.to_string();
    as_role.password = PASSWORD.to_string();

    let result = Dump::new(as_role).inspect(4).await;

    // Drop the role before asserting, so a failure does not leave it behind.
    let _ = sqlx::query(&format!(r#"drop role if exists {ROLE}"#))
        .execute(&pool)
        .await;
    pool.close().await;

    result.expect("a password of URL-reserved characters must authenticate");
}
