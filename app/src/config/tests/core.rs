use super::*;
use std::env;
use std::fs::File;
use std::io::Write;

fn write_temp_config(contents: &str, file_name: &str) -> String {
    let dir = env::temp_dir();
    let file_path = dir.join(file_name);
    if file_path.exists() {
        std::fs::remove_file(&file_path).unwrap();
    }
    let mut file = File::create(&file_path).unwrap();
    file.write_all(contents.as_bytes()).unwrap();
    file_path.to_str().unwrap().to_string()
}

#[test]
fn test_valid_config_parsing() {
    let config_content = r#"
        FROM_HOST=localhost
        FROM_DATABASE=testdb
        FROM_SCHEME=postgres
        FROM_SSL=true
        FROM_DUMP=from.dump
        TO_HOST=remotehost
        TO_DATABASE=remotedb
        TO_SCHEME=postgres
        TO_SSL=false
        TO_DUMP=to.dump
        OUTPUT=result.out
        USE_DROP=true
        USE_SINGLE_TRANSACTION=true
        OUTPUT_FOR_PRODUCTION=true
    "#;
    let file = write_temp_config(config_content, "test_valid_config_parsing.cfg");
    let config = Config::new(file.clone());
    assert_eq!(config.from.host, "localhost");
    assert_eq!(config.from.database, "testdb");
    assert_eq!(config.from.scheme, "postgres");
    assert!(config.from.ssl);
    assert_eq!(config.from.file, "from.dump");
    assert_eq!(config.to.host, "remotehost");
    assert_eq!(config.to.database, "remotedb");
    assert_eq!(config.to.scheme, "postgres");
    assert!(!config.to.ssl);
    assert_eq!(config.to.file, "to.dump");
    assert_eq!(config.output, "result.out");
    assert!(config.use_drop);
    assert!(config.use_single_transaction);
    assert!(config.output_for_production);
    let _ = std::fs::remove_file(file);
}

#[test]
fn test_output_for_production_defaults_false() {
    let config_content = "FROM_HOST=localhost\nTO_HOST=remotehost";
    let file = write_temp_config(config_content, "test_output_for_production_default.cfg");
    let config = Config::new(file.clone());
    assert!(!config.output_for_production);
    let _ = std::fs::remove_file(file);
}

#[test]
#[should_panic]
fn test_invalid_output_for_production_value_panics() {
    let config_content = "FROM_HOST=localhost\nOUTPUT_FOR_PRODUCTION=maybe";
    let file = write_temp_config(config_content, "test_invalid_ofp_value.cfg");
    let _ = Config::new(file.clone());
    let _ = std::fs::remove_file(file);
}

#[test]
#[should_panic]
fn test_invalid_config_line_panics() {
    let config_content = "FROM_HOST=localhost\nINVALID_LINE";
    let file = write_temp_config(config_content, "test_invalid_config_line_panics.cfg");
    // This should exit due to invalid line
    let _ = Config::new(file.clone());
    let _ = std::fs::remove_file(file);
}

#[test]
#[should_panic]
fn test_unknown_key_panics() {
    let config_content = "FROM_HOST=localhost\nUNKNOWN_KEY=value";
    let file = write_temp_config(config_content, "test_unknown_key_panics.cfg");
    let _ = Config::new(file.clone());
    let _ = std::fs::remove_file(file);
}

#[test]
#[should_panic]
fn test_invalid_ssl_value_panics() {
    let config_content = "FROM_HOST=localhost\nFROM_SSL=maybe";
    let file = write_temp_config(config_content, "test_invalid_ssl_value_panics.cfg");
    let _ = Config::new(file.clone());
    let _ = std::fs::remove_file(file);
}

#[test]
fn test_comments_and_empty_lines_are_ignored() {
    let config_content = r#"
        # This is a comment
        FROM_HOST=localhost

        FROM_DATABASE=testdb
        # Another comment
        FROM_SCHEME=postgres
        FROM_SSL=true
        FROM_DUMP=from.dump

        TO_HOST=remotehost
        TO_DATABASE=remotedb
        TO_SCHEME=postgres
        TO_SSL=false
        TO_DUMP=to.dump
        OUTPUT=result.out
        # Comment about USE_DROP
        USE_DROP=true
        USE_SINGLE_TRANSACTION=true
    "#;
    let file = write_temp_config(
        config_content,
        "test_comments_and_empty_lines_are_ignored.cfg",
    );
    let config = Config::new(file.clone());
    assert_eq!(config.from.host, "localhost");
    assert_eq!(config.to.host, "remotehost");
    assert!(config.use_drop);
    assert!(config.use_single_transaction);
    let _ = std::fs::remove_file(file);
}

#[test]
fn test_default_values_are_used() {
    let config_content = "";
    let file = write_temp_config(config_content, "test_default_values_are_used.cfg");
    let config = Config::new(file.clone());
    assert_eq!(config.from.file, "dump.from");
    assert_eq!(config.to.file, "dump.to");
    assert_eq!(config.output, "data.out");
    assert!(!config.use_drop);
    assert!(!config.use_single_transaction);
    let _ = std::fs::remove_file(file);
}

#[test]
#[should_panic]
fn test_missing_file_panics() {
    let file = "/tmp/non_existent_config_file.cfg".to_string();
    let _ = Config::new(file);
}

#[test]
fn test_use_drop_true_value() {
    let config_content = r#"
        FROM_HOST=localhost
        FROM_DATABASE=testdb
        FROM_DUMP=from.dump
        TO_HOST=localhost
        TO_DATABASE=testdb
        TO_DUMP=to.dump
        OUTPUT=result.out
        USE_DROP=true
    "#;
    let file = write_temp_config(config_content, "test_use_drop_true_value.cfg");
    let config = Config::new(file.clone());
    assert!(config.use_drop);
    let _ = std::fs::remove_file(file);
}

#[test]
fn test_use_drop_false_value() {
    let config_content = r#"
        FROM_HOST=localhost
        FROM_DATABASE=testdb
        FROM_DUMP=from.dump
        TO_HOST=localhost
        TO_DATABASE=testdb
        TO_DUMP=to.dump
        OUTPUT=result.out
        USE_DROP=false
    "#;
    let file = write_temp_config(config_content, "test_use_drop_false_value.cfg");
    let config = Config::new(file.clone());
    assert!(!config.use_drop);
    let _ = std::fs::remove_file(file);
}

#[test]
fn test_use_drop_case_insensitive() {
    let config_content = r#"
        FROM_HOST=localhost
        FROM_DATABASE=testdb
        FROM_DUMP=from.dump
        TO_HOST=localhost
        TO_DATABASE=testdb
        TO_DUMP=to.dump
        OUTPUT=result.out
        USE_DROP=TRUE
    "#;
    let file = write_temp_config(config_content, "test_use_drop_case_insensitive.cfg");
    let config = Config::new(file.clone());
    assert!(config.use_drop);
    let _ = std::fs::remove_file(file);
}

#[test]
fn test_use_single_transaction_true_value() {
    let config_content = r#"
        USE_SINGLE_TRANSACTION=true
    "#;

    let file = write_temp_config(config_content, "test_use_single_transaction_true_value.cfg");

    let config = Config::new(file.clone());

    assert!(config.use_single_transaction);

    let _ = std::fs::remove_file(file);
}

#[test]
fn test_use_single_transaction_false_value() {
    let config_content = r#"
        USE_SINGLE_TRANSACTION=false
    "#;

    let file = write_temp_config(
        config_content,
        "test_use_single_transaction_false_value.cfg",
    );

    let config = Config::new(file.clone());

    assert!(!config.use_single_transaction);

    let _ = std::fs::remove_file(file);
}

#[test]
fn test_use_single_transaction_case_insensitive() {
    let config_content = r#"
        USE_SINGLE_TRANSACTION=TRUE
    "#;

    let file = write_temp_config(
        config_content,
        "test_use_single_transaction_case_insensitive.cfg",
    );

    let config = Config::new(file.clone());

    assert!(config.use_single_transaction);

    let _ = std::fs::remove_file(file);
}

// --- Key normalisation (case-insensitive key names) ---

#[test]
fn test_lowercase_key_use_comments_accepted() {
    // Previously `use_comments=false` was rejected by the allowlist even though
    // the match branch would have accepted it after to_uppercase(); now both use
    // the same normalised string so lowercase keys must work.
    let config_content = "use_comments=false\n";
    let file = write_temp_config(config_content, "test_lowercase_key_use_comments.cfg");
    let config = Config::new(file.clone());
    assert!(!config.use_comments);
    let _ = std::fs::remove_file(file);
}

#[test]
fn test_mixed_case_key_use_comments_accepted() {
    let config_content = "Use_Comments=true\n";
    let file = write_temp_config(config_content, "test_mixed_case_key_use_comments.cfg");
    let config = Config::new(file.clone());
    assert!(config.use_comments);
    let _ = std::fs::remove_file(file);
}

#[test]
fn test_lowercase_key_use_drop_accepted() {
    let config_content = "use_drop=true\n";
    let file = write_temp_config(config_content, "test_lowercase_key_use_drop.cfg");
    let config = Config::new(file.clone());
    assert!(config.use_drop);
    let _ = std::fs::remove_file(file);
}

#[test]
fn test_lowercase_key_use_single_transaction_accepted() {
    let config_content = "use_single_transaction=true\n";
    let file = write_temp_config(
        config_content,
        "test_lowercase_key_use_single_transaction.cfg",
    );
    let config = Config::new(file.clone());
    assert!(config.use_single_transaction);
    let _ = std::fs::remove_file(file);
}

#[test]
fn test_lowercase_key_from_host_accepted() {
    let config_content = "from_host=myhost\n";
    let file = write_temp_config(config_content, "test_lowercase_key_from_host.cfg");
    let config = Config::new(file.clone());
    assert_eq!(config.from.host, "myhost");
    let _ = std::fs::remove_file(file);
}

#[test]
fn test_mixed_case_key_to_database_accepted() {
    let config_content = "To_Database=mydb\n";
    let file = write_temp_config(config_content, "test_mixed_case_key_to_database.cfg");
    let config = Config::new(file.clone());
    assert_eq!(config.to.database, "mydb");
    let _ = std::fs::remove_file(file);
}

// --- Value normalisation (case-insensitive boolean values) ---

#[test]
fn test_use_comments_false_lowercase_value() {
    let config_content = "USE_COMMENTS=false\n";
    let file = write_temp_config(
        config_content,
        "test_use_comments_false_lowercase_value.cfg",
    );
    let config = Config::new(file.clone());
    assert!(!config.use_comments);
    let _ = std::fs::remove_file(file);
}

#[test]
fn test_use_comments_true_mixed_case_value() {
    let config_content = "USE_COMMENTS=True\n";
    let file = write_temp_config(
        config_content,
        "test_use_comments_true_mixed_case_value.cfg",
    );
    let config = Config::new(file.clone());
    assert!(config.use_comments);
    let _ = std::fs::remove_file(file);
}

#[test]
#[should_panic]
fn test_use_comments_invalid_value_panics() {
    let config_content = "USE_COMMENTS=yes\n";
    let file = write_temp_config(config_content, "test_use_comments_invalid_value_panics.cfg");
    let _ = Config::new(file.clone());
    let _ = std::fs::remove_file(file);
}

#[test]
fn test_from_ssl_lowercase_true_value() {
    let config_content = "FROM_SSL=true\n";
    let file = write_temp_config(config_content, "test_from_ssl_lowercase_true_value.cfg");
    let config = Config::new(file.clone());
    assert!(config.from.ssl);
    let _ = std::fs::remove_file(file);
}

#[test]
fn test_to_ssl_mixed_case_false_value() {
    let config_content = "TO_SSL=False\n";
    let file = write_temp_config(config_content, "test_to_ssl_mixed_case_false_value.cfg");
    let config = Config::new(file.clone());
    assert!(!config.to.ssl);
    let _ = std::fs::remove_file(file);
}

#[test]
#[should_panic]
fn test_from_ssl_invalid_value_still_panics() {
    // Ensure the value guard still fires after normalisation.
    let config_content = "FROM_SSL=yes\n";
    let file = write_temp_config(
        config_content,
        "test_from_ssl_invalid_value_still_panics.cfg",
    );
    let _ = Config::new(file.clone());
    let _ = std::fs::remove_file(file);
}

// --- Values that legitimately contain `=` (e.g. passwords) ---

#[test]
fn test_password_with_equals_sign_is_preserved() {
    let config_content = "FROM_PASSWORD=abc=def=ghi\n";
    let file = write_temp_config(config_content, "test_password_with_equals.cfg");
    let config = Config::new(file.clone());
    assert_eq!(config.from.password, "abc=def=ghi");
    let _ = std::fs::remove_file(file);
}

#[test]
fn test_password_with_trailing_equals_is_preserved() {
    // base64-style trailing padding
    let config_content = "TO_PASSWORD=c29tZXBhc3M=\n";
    let file = write_temp_config(config_content, "test_password_trailing_equals.cfg");
    let config = Config::new(file.clone());
    assert_eq!(config.to.password, "c29tZXBhc3M=");
    let _ = std::fs::remove_file(file);
}

// --- MAX_CONNECTIONS validation ---

#[test]
fn test_max_connections_valid_value() {
    let config_content = "MAX_CONNECTIONS=16\n";
    let file = write_temp_config(config_content, "test_max_connections_valid.cfg");
    let config = Config::new(file.clone());
    assert_eq!(config.max_connections, 16);
    let _ = std::fs::remove_file(file);
}

#[test]
fn test_max_connections_default_value() {
    let config_content = "FROM_HOST=localhost\n";
    let file = write_temp_config(config_content, "test_max_connections_default.cfg");
    let config = Config::new(file.clone());
    assert_eq!(config.max_connections, 16);
    let _ = std::fs::remove_file(file);
}

#[test]
fn test_max_connections_minimum_value() {
    let config_content = "MAX_CONNECTIONS=1\n";
    let file = write_temp_config(config_content, "test_max_connections_min.cfg");
    let config = Config::new(file.clone());
    assert_eq!(config.max_connections, 1);
    let _ = std::fs::remove_file(file);
}

#[test]
#[should_panic(expected = "MAX_CONNECTIONS must be at least 1")]
fn test_max_connections_zero_panics() {
    let config_content = "MAX_CONNECTIONS=0\n";
    let file = write_temp_config(config_content, "test_max_connections_zero.cfg");
    let _ = Config::new(file.clone());
    let _ = std::fs::remove_file(file);
}

#[test]
#[should_panic(expected = "Invalid value for MAX_CONNECTIONS")]
fn test_max_connections_non_numeric_panics() {
    let config_content = "MAX_CONNECTIONS=abc\n";
    let file = write_temp_config(config_content, "test_max_connections_non_numeric.cfg");
    let _ = Config::new(file.clone());
    let _ = std::fs::remove_file(file);
}

#[test]
#[should_panic(expected = "Invalid value for MAX_CONNECTIONS")]
fn test_max_connections_negative_panics() {
    let config_content = "MAX_CONNECTIONS=-1\n";
    let file = write_temp_config(config_content, "test_max_connections_negative.cfg");
    let _ = Config::new(file.clone());
    let _ = std::fs::remove_file(file);
}
