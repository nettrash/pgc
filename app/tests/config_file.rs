//! Integration tests for `pgc.conf` loading.
//!
//! [`Config::load`] takes a path, so unlike the unit tests these read real
//! files — including the configuration samples shipped under `data/`, which
//! must keep parsing as keys are added.

mod common;

use common::{ScratchDir, data_path};
use pgc::config::core::Config;
use pgc::config::grants_mode::GrantsMode;

fn write_config(dir: &ScratchDir, body: &str) -> String {
    let path = dir.path_str("pgc.conf");
    std::fs::write(&path, body).expect("write config");
    path
}

#[test]
fn shipped_sample_config_parses() {
    let path = data_path("pgc.conf");
    let config = Config::load(path.to_str().expect("utf-8 path"))
        .unwrap_or_else(|e| panic!("data/pgc.conf must stay loadable: {e}"));

    assert_eq!(config.from.host, "localhost");
    assert_eq!(config.from.database, "service");
    assert!(config.from.ssl, "FROM_SSL=true");
    assert!(!config.to.ssl, "TO_SSL=false");
    assert_eq!(config.output, "delta.sql");
    assert!(!config.use_drop);
    assert!(!config.output_for_production);
    // Not set in the sample — must fall back to the documented defaults.
    assert_eq!(config.grants_mode, GrantsMode::Ignore);
    assert_eq!(config.max_connections, 16);
    assert!(config.use_comments);
}

// Note: `data/test.conf` is deliberately *not* covered here. It is listed in
// `.gitignore` — a developer-local file holding real credentials and absolute
// paths — so a test reading it passes only on the machine that has it. The
// keys it exercises are covered by `every_key_round_trips_from_a_file` below,
// which builds its own fixture.

/// The `|` alternation in a scheme pattern has to survive parsing verbatim:
/// `--scheme` is matched against `nspname` with SQL `SIMILAR TO`, not equality,
/// so mangling it would silently change which schemas are dumped.
#[test]
fn multi_schema_patterns_survive_parsing() {
    let dir = ScratchDir::new("scheme-pattern");
    let path = write_config(
        &dir,
        "FROM_HOST=a.example\nFROM_SCHEME=public|app\nTO_HOST=b.example\nTO_SCHEME=public|app\n",
    );

    let config = Config::load(&path).expect("load config");
    assert_eq!(config.from.scheme, "public|app");
    assert_eq!(config.from.scheme, config.to.scheme);
}

#[test]
fn every_key_round_trips_from_a_file() {
    let dir = ScratchDir::new("full");
    let path = write_config(
        &dir,
        "\
# a comment, and the blank line below, are both ignored

FROM_HOST=from.example
FROM_PORT=5433
FROM_USER=alice
FROM_PASSWORD=secret
FROM_DATABASE=old
FROM_SCHEME=app_.*
FROM_SSL=true
FROM_DUMP=a.dump
TO_HOST=to.example
TO_PORT=5434
TO_USER=bob
TO_DATABASE=new
TO_SCHEME=public|app
TO_SSL=false
TO_DUMP=b.dump
OUTPUT=migration.sql
USE_DROP=true
USE_SINGLE_TRANSACTION=true
USE_COMMENTS=false
GRANTS_MODE=addonly
MAX_CONNECTIONS=4
OUTPUT_FOR_PRODUCTION=true
",
    );

    let config = Config::load(&path).expect("load config");

    assert_eq!(config.from.host, "from.example");
    assert_eq!(config.from.port, "5433");
    assert_eq!(config.from.user, "alice");
    assert_eq!(config.from.database, "old");
    assert_eq!(config.from.scheme, "app_.*");
    assert!(config.from.ssl);
    assert_eq!(config.from.file, "a.dump");

    assert_eq!(config.to.host, "to.example");
    assert_eq!(config.to.port, "5434");
    assert_eq!(config.to.user, "bob");
    assert_eq!(config.to.database, "new");
    assert_eq!(config.to.scheme, "public|app");
    assert!(!config.to.ssl);
    assert_eq!(config.to.file, "b.dump");

    assert_eq!(config.output, "migration.sql");
    assert!(config.use_drop);
    assert!(config.use_single_transaction);
    assert!(!config.use_comments);
    assert_eq!(config.grants_mode, GrantsMode::AddOnly);
    assert_eq!(config.max_connections, 4);
    assert!(config.output_for_production);
}

#[test]
fn omitted_keys_fall_back_to_defaults() {
    let dir = ScratchDir::new("defaults");
    let path = write_config(&dir, "FROM_HOST=a.example\nTO_HOST=b.example\n");

    let config = Config::load(&path).expect("load config");

    assert_eq!(config.from.port, "5432");
    assert_eq!(config.to.port, "5432");
    assert_eq!(config.from.file, "dump.from");
    assert_eq!(config.to.file, "dump.to");
    assert_eq!(config.output, "data.out");
    assert!(!config.use_drop);
    assert!(!config.use_single_transaction);
    assert!(config.use_comments);
    assert_eq!(config.grants_mode, GrantsMode::Ignore);
    assert_eq!(config.max_connections, 16);
    assert!(!config.output_for_production);
}

#[test]
fn a_missing_file_is_an_error_naming_the_path() {
    let dir = ScratchDir::new("missing");
    let path = dir.path_str("nope.conf");

    let err = Config::load(&path).expect_err("a missing config must not load");
    assert!(err.contains(&path), "error should name the file: {err}");
}

#[test]
fn an_unknown_key_is_rejected() {
    let dir = ScratchDir::new("unknown-key");
    let path = write_config(&dir, "FROM_HOST=a.example\nFROM_FLAVOUR=vanilla\n");

    let err = Config::load(&path).expect_err("unknown keys must not be ignored");
    assert!(err.contains("FROM_FLAVOUR"), "unexpected error: {err}");
}

#[test]
fn a_line_without_a_value_is_rejected() {
    let dir = ScratchDir::new("no-value");
    let path = write_config(&dir, "FROM_HOST=a.example\nTO_HOST=\n");

    assert!(
        Config::load(&path).is_err(),
        "a key with an empty value must not load"
    );
}

#[test]
fn a_non_boolean_ssl_value_is_rejected() {
    let dir = ScratchDir::new("bad-ssl");
    let path = write_config(&dir, "FROM_HOST=a.example\nFROM_SSL=yes\n");

    let err = Config::load(&path).expect_err("FROM_SSL must be true or false");
    assert!(err.contains("FROM_SSL"), "unexpected error: {err}");
}

#[test]
fn a_non_numeric_max_connections_is_rejected() {
    let dir = ScratchDir::new("bad-max-conn");
    let path = write_config(&dir, "FROM_HOST=a.example\nMAX_CONNECTIONS=many\n");

    let err = Config::load(&path).expect_err("MAX_CONNECTIONS must be a number");
    assert!(err.contains("MAX_CONNECTIONS"), "unexpected error: {err}");
}

/// Values keep their original case even though keys are matched
/// case-insensitively — schema patterns and passwords are case-sensitive.
#[test]
fn keys_are_case_insensitive_but_values_are_not() {
    let dir = ScratchDir::new("case");
    let path = write_config(
        &dir,
        "from_host=MixedCase.Example\nFrOm_ScHeMe=App_Schema\n",
    );

    let config = Config::load(&path).expect("load config");
    assert_eq!(config.from.host, "MixedCase.Example");
    assert_eq!(config.from.scheme, "App_Schema");
}
