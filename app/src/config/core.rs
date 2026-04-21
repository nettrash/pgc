use crate::config::dump_config::DumpConfig;
use crate::config::grants_mode::GrantsMode;

// Configuration file representation.
#[derive(Debug, Clone)]
pub struct Config {
    // From Dump Configuration
    pub from: DumpConfig,
    // To Dump Configuration
    pub to: DumpConfig,
    // Output file name for the comparison result
    pub output: String,
    // Whether to use DROP statements in the output
    pub use_drop: bool,
    // True - if explicit begin...commit statement has to be added into resulting diff file; False - otherwise
    pub use_single_transaction: bool,
    // Whether to include comments in the output script
    pub use_comments: bool,
    // How to handle grants (privileges) during comparison
    pub grants_mode: GrantsMode,
    // Maximum number of connections in the PostgreSQL connection pool
    pub max_connections: u32,
}

impl Config {
    /// Load configuration from `file`. Returns a descriptive error instead of
    /// panicking on malformed input so callers can format it however they want.
    pub fn load(file: &str) -> Result<Self, String> {
        let binding = std::fs::read_to_string(file)
            .map_err(|e| format!("Error reading configuration file {file}: {e}"))?;
        let config_data = binding
            .split('\n')
            .filter(|line| !line.trim().is_empty() && !line.trim().starts_with('#'))
            .collect::<Vec<&str>>();

        let mut from_host = "".to_string();
        let mut from_port = "5432".to_string();
        let mut from_user = "".to_string();
        let mut from_password = "".to_string();
        let mut from_database = "".to_string();
        let mut from_scheme = "".to_string();
        let mut from_ssl = false;
        let mut from_dump = "dump.from".to_string();
        let mut to_host = "".to_string();
        let mut to_port = "5432".to_string();
        let mut to_user = "".to_string();
        let mut to_password = "".to_string();
        let mut to_database = "".to_string();
        let mut to_scheme = "".to_string();
        let mut to_ssl = false;
        let mut to_dump = "dump.to".to_string();
        let mut output = "data.out".to_string();
        let mut use_drop = false;
        let mut use_single_transaction = false;
        let mut use_comments = true;
        let mut grants_mode = GrantsMode::Ignore;
        let mut max_connections: u32 = 16;

        for line in &config_data {
            if line.trim().is_empty() || line.starts_with('#') {
                continue; // Skip empty lines and comments
            }
            let parts: Vec<&str> = line.splitn(2, '=').collect();
            if parts.len() != 2 || parts[0].trim().is_empty() || parts[1].trim().is_empty() {
                return Err(format!("Invalid configuration line: {line}"));
            }
            let key = parts[0].trim().to_uppercase();
            let value = parts[1].trim().to_uppercase();
            let raw_value = parts[1].trim();
            if key != "FROM_HOST"
                && key != "FROM_PORT"
                && key != "FROM_USER"
                && key != "FROM_PASSWORD"
                && key != "FROM_DATABASE"
                && key != "FROM_SCHEME"
                && key != "FROM_SSL"
                && key != "FROM_DUMP"
                && key != "TO_HOST"
                && key != "TO_PORT"
                && key != "TO_USER"
                && key != "TO_PASSWORD"
                && key != "TO_DATABASE"
                && key != "TO_SCHEME"
                && key != "TO_SSL"
                && key != "TO_DUMP"
                && key != "OUTPUT"
                && key != "USE_DROP"
                && key != "USE_SINGLE_TRANSACTION"
                && key != "USE_COMMENTS"
                && key != "GRANTS_MODE"
                && key != "MAX_CONNECTIONS"
            {
                return Err(format!("Unknown configuration key: {}", parts[0]));
            }
            if key == "FROM_SSL" && value != "TRUE" && value != "FALSE" {
                return Err(format!("Invalid value for FROM_SSL: {raw_value}"));
            }
            if key == "TO_SSL" && value != "TRUE" && value != "FALSE" {
                return Err(format!("Invalid value for TO_SSL: {raw_value}"));
            }

            match key.as_str() {
                "FROM_HOST" => from_host = raw_value.to_string(),
                "FROM_PORT" => from_port = raw_value.to_string(),
                "FROM_USER" => from_user = raw_value.to_string(),
                "FROM_PASSWORD" => from_password = raw_value.to_string(),
                "FROM_DATABASE" => from_database = raw_value.to_string(),
                "FROM_SCHEME" => from_scheme = raw_value.to_string(),
                "FROM_SSL" => from_ssl = value == "TRUE",
                "FROM_DUMP" => from_dump = raw_value.to_string(),
                "TO_HOST" => to_host = raw_value.to_string(),
                "TO_PORT" => to_port = raw_value.to_string(),
                "TO_USER" => to_user = raw_value.to_string(),
                "TO_PASSWORD" => to_password = raw_value.to_string(),
                "TO_DATABASE" => to_database = raw_value.to_string(),
                "TO_SCHEME" => to_scheme = raw_value.to_string(),
                "TO_SSL" => to_ssl = value == "TRUE",
                "TO_DUMP" => to_dump = raw_value.to_string(),
                "OUTPUT" => output = raw_value.to_string(),
                "USE_DROP" => use_drop = value == "TRUE",
                "USE_SINGLE_TRANSACTION" => use_single_transaction = value == "TRUE",
                "USE_COMMENTS" => {
                    use_comments = match value.as_str() {
                        "TRUE" => true,
                        "FALSE" => false,
                        _ => return Err(format!("Invalid value for USE_COMMENTS: {raw_value}")),
                    };
                }
                "GRANTS_MODE" => {
                    grants_mode = raw_value.parse::<GrantsMode>().map_err(|e| e.to_string())?;
                }
                "MAX_CONNECTIONS" => {
                    let v = raw_value
                        .parse::<u32>()
                        .map_err(|e| format!("Invalid value for MAX_CONNECTIONS: {e}"))?;
                    if v < 1 {
                        return Err(format!("MAX_CONNECTIONS must be at least 1, got {v}"));
                    }
                    max_connections = v;
                }
                _ => {}
            }
        }
        let from = DumpConfig {
            host: from_host,
            port: from_port,
            user: from_user,
            password: from_password,
            database: from_database,
            scheme: from_scheme,
            ssl: from_ssl,
            file: from_dump,
        };
        let to = DumpConfig {
            host: to_host,
            port: to_port,
            user: to_user,
            password: to_password,
            database: to_database,
            scheme: to_scheme,
            ssl: to_ssl,
            file: to_dump,
        };
        Ok(Config {
            from,
            to,
            output,
            use_drop,
            use_single_transaction,
            use_comments,
            grants_mode,
            max_connections,
        })
    }

    /// Back-compat shim for existing call sites that expect a panicking constructor.
    /// New code should prefer `Config::load`.
    pub fn new(file: String) -> Self {
        match Self::load(&file) {
            Ok(cfg) => cfg,
            Err(e) => panic!("{e}"),
        }
    }
}

#[cfg(test)]
mod tests {
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
}
