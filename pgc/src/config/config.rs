use crate::config::dump_config::DumpConfig;

// Configuration file representation.
#[derive(Debug, Clone)]
pub struct Config {
    // From Dump Configuration
    pub from: DumpConfig,
    // To Dump Configuration
    pub to: DumpConfig,
    // Output file name for the comparison result
    pub output: String,
}

impl Config {
    // Create a new configuration instance.
    pub fn new(file: String) -> Self {
        // Load the configuration from the file.
        let config_data = std::fs::read_to_string(file);
        if config_data.is_err() {
            panic!("Error reading configuration file: {}", config_data.err().unwrap());
        }
        let binding = config_data.unwrap();
        let config_data = binding.split('\n')
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

        for line in &config_data {
            if line.trim().is_empty() || line.starts_with('#') {
                continue; // Skip empty lines and comments
            }
            if line.split('=').count() != 2 {
                panic!("Invalid configuration line: {}", line);
            }
            let parts: Vec<&str> = line.split('=').collect();
            if parts[0].trim().is_empty() || parts[1].trim().is_empty() {
                panic!("Invalid configuration line: {}", line);
            }
            if parts[0].trim().to_uppercase() != "FROM_HOST" && parts[0].trim() != "FROM_PORT" &&
               parts[0].trim().to_uppercase() != "FROM_USER" && parts[0].trim() != "FROM_PASSWORD" &&
               parts[0].trim() != "FROM_DATABASE" &&
               parts[0].trim() != "FROM_SCHEME" && parts[0].trim() != "FROM_SSL" &&
               parts[0].trim() != "FROM_DUMP" &&
               parts[0].trim().to_uppercase() != "TO_HOST" && parts[0].trim() != "TO_PORT" &&
               parts[0].trim().to_uppercase() != "TO_USER" && parts[0].trim() != "TO_PASSWORD" &&
               parts[0].trim() != "TO_DATABASE" &&
               parts[0].trim() != "TO_SCHEME" && parts[0].trim() != "TO_SSL" &&
               parts[0].trim() != "TO_DUMP" &&
               parts[0].trim() != "OUTPUT" {
                panic!("Unknown configuration key: {}", parts[0]);
            }
            if parts[0].trim().to_uppercase() == "FROM_SSL" && parts[1].trim().to_uppercase() != "TRUE" && parts[1].trim().to_uppercase() != "FALSE" {
                panic!("Invalid value for FROM_SSL: {}", parts[1]);
            }
            if parts[0].trim().to_uppercase() == "TO_SSL" && parts[1].trim().to_uppercase() != "TRUE" && parts[1].trim().to_uppercase() != "FALSE" {
                panic!("Invalid value for TO_SSL: {}", parts[1]);
            }

            match parts[0].trim().to_uppercase().as_str() {
                "FROM_HOST" => from_host = parts[1].trim().to_string(),
                "FROM_PORT" => from_port = parts[1].trim().to_string(),
                "FROM_USER" => from_user = parts[1].trim().to_string(),
                "FROM_PASSWORD" => from_password = parts[1].trim().to_string(),
                "FROM_DATABASE" => from_database = parts[1].trim().to_string(),
                "FROM_SCHEME" => from_scheme = parts[1].trim().to_string(),
                "FROM_SSL" => from_ssl = parts[1].trim().to_uppercase() == "TRUE",
                "FROM_DUMP" => from_dump = parts[1].trim().to_string(),
                "TO_HOST" => to_host = parts[1].trim().to_string(),
                "TO_PORT" => to_port = parts[1].trim().to_string(),
                "TO_USER" => to_user = parts[1].trim().to_string(),
                "TO_PASSWORD" => to_password = parts[1].trim().to_string(),
                "TO_DATABASE" => to_database = parts[1].trim().to_string(),
                "TO_SCHEME" => to_scheme = parts[1].trim().to_string(),
                "TO_SSL" => to_ssl = parts[1].trim().to_uppercase() == "TRUE",
                "TO_DUMP" => to_dump = parts[1].trim().to_string(),
                "OUTPUT" => output = parts[1].trim().to_string(),
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
        Config { from, to, output }
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::Write;
    use std::env;
    use super::*;

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
        "#;
        let file = write_temp_config(config_content, "test_valid_config_parsing.cfg");
        let config = Config::new(file.clone());
        assert_eq!(config.from.host, "localhost");
        assert_eq!(config.from.database, "testdb");
        assert_eq!(config.from.scheme, "postgres");
        assert_eq!(config.from.ssl, true);
        assert_eq!(config.from.file, "from.dump");
        assert_eq!(config.to.host, "remotehost");
        assert_eq!(config.to.database, "remotedb");
        assert_eq!(config.to.scheme, "postgres");
        assert_eq!(config.to.ssl, false);
        assert_eq!(config.to.file, "to.dump");
        assert_eq!(config.output, "result.out");
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
        "#;
        let file = write_temp_config(config_content, "test_comments_and_empty_lines_are_ignored.cfg");
        let config = Config::new(file.clone());
        assert_eq!(config.from.host, "localhost");
        assert_eq!(config.to.host, "remotehost");
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
        let _ = std::fs::remove_file(file);
    }

    #[test]
    #[should_panic]
    fn test_missing_file_panics() {
        let file = "/tmp/non_existent_config_file.cfg".to_string();
        let _ = Config::new(file);
    }
}
