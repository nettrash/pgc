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

use common::ScratchDir;
use pgc::comparer::core::Comparer;
use pgc::config::dump_config::DumpConfig;
use pgc::config::grants_mode::GrantsMode;
use pgc::dump::core::Dump;

/// Connection details from the standard `PG*` environment variables, with the
/// same defaults as the `pgc` CLI.
fn env_config(file: &str) -> DumpConfig {
    let var = |name: &str, fallback: &str| {
        std::env::var(name).unwrap_or_else(|_| fallback.to_string())
    };
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

    for line in script.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with("--") || line.starts_with("/*") {
            continue;
        }
        panic!("self-comparison of a live database emitted DDL:\n{script}");
    }
}

#[tokio::test]
#[ignore = "needs a reachable PostgreSQL server; run with --ignored"]
async fn inspect_populates_a_dump_without_writing_a_file() {
    let dir = ScratchDir::new("live-inspect");
    let path = dir.path_str("never-written.dump");

    let mut dump = Dump::new(env_config(&path));
    dump.inspect(8).await.expect("inspect the live database");

    assert!(!dump.schemas.is_empty(), "inspect must find at least one schema");
    assert!(
        !std::path::Path::new(&path).exists(),
        "inspect must not write the dump file"
    );
    // The clear script is generated from an inspected dump, so it must be
    // buildable straight after.
    assert!(!dump.generate_clear_script(true, true, false).is_empty());
}
