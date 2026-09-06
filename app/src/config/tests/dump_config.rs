use super::*;
use sqlx::postgres::{PgConnectOptions, PgSslMode};

#[test]
fn test_dump_config_new() {
    let config = DumpConfig {
        host: "testhost".to_string(),
        port: "9999".to_string(),
        user: "testuser".to_string(),
        password: "testpass".to_string(),
        database: "testdb".to_string(),
        scheme: "testschema".to_string(),
        ssl: true,
        file: "test.dump".to_string(),
    };

    assert_eq!(config.host, "testhost");
    assert_eq!(config.port, "9999");
    assert_eq!(config.user, "testuser");
    assert_eq!(config.password, "testpass");
    assert_eq!(config.database, "testdb");
    assert_eq!(config.scheme, "testschema");
    assert!(config.ssl);
    assert_eq!(config.file, "test.dump");
}

#[test]
fn test_dump_config_default() {
    let config = DumpConfig::default();

    assert_eq!(config.host, "localhost");
    assert_eq!(config.port, "5432");
    assert_eq!(config.user, "postgres");
    assert_eq!(config.password, "postgres");
    assert_eq!(config.database, "postgres");
    assert_eq!(config.scheme, "public");
    assert!(!config.ssl);
    assert_eq!(config.file, "dump.io");
}

#[test]
fn test_get_connect_options_with_ssl_disabled() {
    let config = DumpConfig {
        host: "localhost".to_string(),
        port: "5432".to_string(),
        user: "testuser".to_string(),
        password: "testpass".to_string(),
        database: "testdb".to_string(),
        scheme: "public".to_string(),
        ssl: false,
        file: "test.dump".to_string(),
    };

    let options = config.get_connect_options().expect("valid configuration");
    assert_eq!(options.get_host(), "localhost");
    assert_eq!(options.get_port(), 5432);
    assert_eq!(options.get_username(), "testuser");
    assert_eq!(options.get_database(), Some("testdb"));
    assert!(matches!(options.get_ssl_mode(), PgSslMode::Disable));
}

#[test]
fn test_get_connect_options_with_ssl_enabled() {
    let config = DumpConfig {
        host: "remotehost".to_string(),
        port: "5433".to_string(),
        user: "produser".to_string(),
        password: "securepass".to_string(),
        database: "proddb".to_string(),
        scheme: "app_schema".to_string(),
        ssl: true,
        file: "prod.dump".to_string(),
    };

    let options = config.get_connect_options().expect("valid configuration");
    assert_eq!(options.get_host(), "remotehost");
    assert_eq!(options.get_port(), 5433);
    assert_eq!(options.get_username(), "produser");
    assert_eq!(options.get_database(), Some("proddb"));
    assert!(matches!(options.get_ssl_mode(), PgSslMode::Require));
}

#[test]
fn test_get_masked_connection_string_with_ssl_disabled() {
    let config = DumpConfig {
        host: "localhost".to_string(),
        port: "5432".to_string(),
        user: "testuser".to_string(),
        password: "testpass".to_string(),
        database: "testdb".to_string(),
        scheme: "public".to_string(),
        ssl: false,
        file: "test.dump".to_string(),
    };

    let masked_string = config.get_masked_connection_string();
    let expected = "postgres://*:*@localhost:5432/testdb?sslmode=disable";
    assert_eq!(masked_string, expected);
}

#[test]
fn test_get_masked_connection_string_with_ssl_enabled() {
    let config = DumpConfig {
        host: "remotehost".to_string(),
        port: "5433".to_string(),
        user: "produser".to_string(),
        password: "securepass".to_string(),
        database: "proddb".to_string(),
        scheme: "app_schema".to_string(),
        ssl: true,
        file: "prod.dump".to_string(),
    };

    let masked_string = config.get_masked_connection_string();
    let expected = "postgres://*:*@remotehost:5433/proddb?sslmode=require";
    assert_eq!(masked_string, expected);
}

/// The test this replaces asserted only that `format!` had produced the
/// string it was told to produce, and never parsed the result — so it passed
/// while every connection using that string failed (issue #244). What matters
/// is that the credentials survive, so that is what is asserted here.
#[test]
fn test_connect_options_with_special_characters() {
    let config = DumpConfig {
        host: "test-host.example.com".to_string(),
        port: "5432".to_string(),
        user: "user@domain".to_string(),
        password: "pass!@#$%".to_string(),
        database: "test_db-name".to_string(),
        scheme: "schema_name".to_string(),
        ssl: false,
        file: "special.dump".to_string(),
    };

    let options = config.get_connect_options().expect("valid configuration");
    assert_eq!(options.get_host(), "test-host.example.com");
    assert_eq!(options.get_port(), 5432);
    assert_eq!(options.get_username(), "user@domain");
    assert_eq!(options.get_database(), Some("test_db-name"));

    // The masked form is log output only and stays a plain string.
    let masked_string = config.get_masked_connection_string();
    let expected_masked = "postgres://*:*@test-host.example.com:5432/test_db-name?sslmode=disable";
    assert_eq!(masked_string, expected_masked);
}

#[test]
fn test_dump_config_clone() {
    let original = DumpConfig {
        host: "localhost".to_string(),
        port: "5432".to_string(),
        user: "testuser".to_string(),
        password: "testpass".to_string(),
        database: "testdb".to_string(),
        scheme: "public".to_string(),
        ssl: true,
        file: "test.dump".to_string(),
    };

    let cloned = original.clone();

    assert_eq!(original.host, cloned.host);
    assert_eq!(original.port, cloned.port);
    assert_eq!(original.user, cloned.user);
    assert_eq!(original.password, cloned.password);
    assert_eq!(original.database, cloned.database);
    assert_eq!(original.scheme, cloned.scheme);
    assert_eq!(original.ssl, cloned.ssl);
    assert_eq!(original.file, cloned.file);
}

#[test]
fn test_dump_config_debug_format() {
    let config = DumpConfig::default();
    let debug_string = format!("{config:?}");

    // Verify that the debug string contains all fields
    assert!(debug_string.contains("DumpConfig"));
    assert!(debug_string.contains("host"));
    assert!(debug_string.contains("port"));
    assert!(debug_string.contains("user"));
    assert!(debug_string.contains("password"));
    assert!(debug_string.contains("database"));
    assert!(debug_string.contains("scheme"));
    assert!(debug_string.contains("ssl"));
    assert!(debug_string.contains("file"));
}

#[test]
fn test_serde_serialization() {
    let config = DumpConfig {
        host: "testhost".to_string(),
        port: "9999".to_string(),
        user: "testuser".to_string(),
        password: "testpass".to_string(),
        database: "testdb".to_string(),
        scheme: "testschema".to_string(),
        ssl: true,
        file: "test.dump".to_string(),
    };

    // Test serialization
    let json = serde_json::to_string(&config).expect("Failed to serialize");
    assert!(json.contains("testhost"));
    assert!(json.contains("9999"));
    assert!(json.contains("testuser"));
    assert!(json.contains("testpass"));
    assert!(json.contains("testdb"));
    assert!(json.contains("testschema"));
    assert!(json.contains("true"));
    assert!(json.contains("test.dump"));

    // Test deserialization
    let deserialized: DumpConfig = serde_json::from_str(&json).expect("Failed to deserialize");
    assert_eq!(config.host, deserialized.host);
    assert_eq!(config.port, deserialized.port);
    assert_eq!(config.user, deserialized.user);
    assert_eq!(config.password, deserialized.password);
    assert_eq!(config.database, deserialized.database);
    assert_eq!(config.scheme, deserialized.scheme);
    assert_eq!(config.ssl, deserialized.ssl);
    assert_eq!(config.file, deserialized.file);
}

#[test]
fn test_edge_cases_empty_strings() {
    let config = DumpConfig {
        host: "".to_string(),
        port: "".to_string(),
        user: "".to_string(),
        password: "".to_string(),
        database: "".to_string(),
        scheme: "".to_string(),
        ssl: false,
        file: "".to_string(),
    };

    // An empty port is not a port. The URL path used to carry it as far as
    // the parser and come back with `invalid port number`; now it is refused
    // where it can still be described as what it is.
    assert!(config.get_connect_options().is_err());

    let masked_string = config.get_masked_connection_string();
    let expected_masked = "postgres://*:*@:/?sslmode=disable";
    assert_eq!(masked_string, expected_masked);
}

// ── issue #244: credentials are data, not URL syntax ───────────────────
// The connection details used to be interpolated into a
// `postgres://user:password@host:port/db` string and parsed back by sqlx.
// Every RFC 3986 reserved character in a password was then read as syntax.
// These tests pin both halves: that the URL round-trip really is lossy,
// and that `get_connect_options` does not take part in it.

use std::str::FromStr;

fn config_with_password(password: &str) -> DumpConfig {
    DumpConfig {
        host: "localhost".to_string(),
        port: "5432".to_string(),
        user: "alice".to_string(),
        password: password.to_string(),
        database: "shop".to_string(),
        scheme: "public".to_string(),
        ssl: false,
        file: "test.dump".to_string(),
    }
}

/// The URL the old code would have built for this configuration.
fn legacy_url(config: &DumpConfig) -> String {
    format!(
        "postgres://{}:{}@{}:{}/{}?sslmode=disable",
        config.user, config.password, config.host, config.port, config.database
    )
}

#[test]
fn reserved_characters_that_broke_the_url_leave_the_options_intact() {
    // `#`, `/` and `?` each end the authority section early, so the parser
    // reads the port out of something that is not a number — the reported
    // `invalid port number`, naming the one part that was never wrong.
    for password in ["repro#pass", "repro/pass", "repro?pass"] {
        let config = config_with_password(password);

        assert!(
            PgConnectOptions::from_str(&legacy_url(&config)).is_err(),
            "`{password}` was expected to break the URL form"
        );

        let options = config
            .get_connect_options()
            .unwrap_or_else(|e| panic!("`{password}` must still configure cleanly: {e}"));
        assert_eq!(options.get_host(), "localhost");
        assert_eq!(options.get_port(), 5432);
        assert_eq!(options.get_username(), "alice");
        assert_eq!(options.get_database(), Some("shop"));
    }
}

#[test]
fn a_percent_escape_in_a_password_is_no_longer_decoded() {
    // The quiet one. `%41` is a valid percent-escape, so the URL parser
    // decoded it and `pass%41word` authenticated as `passAword` — verified
    // live against PostgreSQL 16 in both directions: the wrong password was
    // accepted, and the real one could not be used at all.
    //
    // There is no password getter on `PgConnectOptions`, so this reads it out
    // of the `Debug` rendering, which prints it. That couples the test to
    // sqlx's formatting; if sqlx ever redacts it the assertion fails loudly,
    // which is the right way round for a test whose whole subject is a
    // credential being rewritten in transit.
    let config = config_with_password("pass%41word");

    let from_url = PgConnectOptions::from_str(&legacy_url(&config))
        .expect("this one parses — that is exactly the problem");
    assert!(
        format!("{from_url:?}").contains(r#"password: Some("passAword")"#),
        "the URL form is supposed to decode %41; if it no longer does, this \
         test has stopped describing the bug: {from_url:?}"
    );

    let from_options = config.get_connect_options().expect("valid configuration");
    assert!(
        format!("{from_options:?}").contains(r#"password: Some("pass%41word")"#),
        "the password must reach sqlx exactly as it was typed: {from_options:?}"
    );
}

#[test]
fn a_port_that_is_not_a_number_is_reported_as_a_port() {
    let mut config = config_with_password("plain");
    config.port = "not-a-port".to_string();

    let error = config
        .get_connect_options()
        .expect_err("a non-numeric port must be refused");

    assert!(
        matches!(error, sqlx::Error::Configuration(_)),
        "expected a configuration error, got: {error:?}"
    );
}

#[test]
fn a_port_with_surrounding_whitespace_is_accepted() {
    // Config files are hand-edited; `PORT = 5432 ` is a typo, not an outage.
    let mut config = config_with_password("plain");
    config.port = " 5432 ".to_string();

    assert_eq!(
        config
            .get_connect_options()
            .expect("whitespace around a port is not an error")
            .get_port(),
        5432
    );
}
