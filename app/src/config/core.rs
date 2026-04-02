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
}

impl Config {
    // Create a new configuration instance.
    pub fn new(file: String) -> Self {
        // Load the configuration from the file.
        let config_data = std::fs::read_to_string(file);
        if config_data.is_err() {
            panic!(
                "Error reading configuration file: {}",
                config_data.err().unwrap()
            );
        }
        let binding = config_data.unwrap();
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

        for line in &config_data {
            if line.trim().is_empty() || line.starts_with('#') {
                continue; // Skip empty lines and comments
            }
            if line.split('=').count() != 2 {
                panic!("Invalid configuration line: {line}");
            }
            let parts: Vec<&str> = line.split('=').collect();
            if parts[0].trim().is_empty() || parts[1].trim().is_empty() {
                panic!("Invalid configuration line: {line}");
            }
            let key = parts[0].trim().to_uppercase();
            let value = parts[1].trim().to_uppercase();
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
            {
                panic!("Unknown configuration key: {}", parts[0]);
            }
            if key == "FROM_SSL" && value != "TRUE" && value != "FALSE" {
                panic!("Invalid value for FROM_SSL: {}", parts[1]);
            }
            if key == "TO_SSL" && value != "TRUE" && value != "FALSE" {
                panic!("Invalid value for TO_SSL: {}", parts[1]);
            }

            match key.as_str() {
                "FROM_HOST" => from_host = parts[1].trim().to_string(),
                "FROM_PORT" => from_port = parts[1].trim().to_string(),
                "FROM_USER" => from_user = parts[1].trim().to_string(),
                "FROM_PASSWORD" => from_password = parts[1].trim().to_string(),
                "FROM_DATABASE" => from_database = parts[1].trim().to_string(),
                "FROM_SCHEME" => from_scheme = parts[1].trim().to_string(),
                "FROM_SSL" => from_ssl = value == "TRUE",
                "FROM_DUMP" => from_dump = parts[1].trim().to_string(),
                "TO_HOST" => to_host = parts[1].trim().to_string(),
                "TO_PORT" => to_port = parts[1].trim().to_string(),
                "TO_USER" => to_user = parts[1].trim().to_string(),
                "TO_PASSWORD" => to_password = parts[1].trim().to_string(),
                "TO_DATABASE" => to_database = parts[1].trim().to_string(),
                "TO_SCHEME" => to_scheme = parts[1].trim().to_string(),
                "TO_SSL" => to_ssl = value == "TRUE",
                "TO_DUMP" => to_dump = parts[1].trim().to_string(),
                "OUTPUT" => output = parts[1].trim().to_string(),
                "USE_DROP" => use_drop = value == "TRUE",
                "USE_SINGLE_TRANSACTION" => use_single_transaction = value == "TRUE",
                "USE_COMMENTS" => {
                    use_comments = match value.as_str() {
                        "TRUE" => true,
                        "FALSE" => false,
                        _ => panic!("Invalid value for USE_COMMENTS: {}", parts[1].trim()),
                    };
                }
                "GRANTS_MODE" => {
                    grants_mode = parts[1]
                        .trim()
                        .parse::<GrantsMode>()
                        .unwrap_or_else(|e| panic!("{e}"));
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
        Config {
            from,
            to,
            output,
            use_drop,
            use_single_transaction,
            use_comments,
            grants_mode,
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
}
