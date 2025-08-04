use serde::{Deserialize, Serialize};

// This is a database dump configuration structure.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DumpConfig {
    // Database host
    pub host: String,
    // Database port
    pub port: String,
    // Database user name
    pub user: String,
    // Database user password
    pub password: String,
    // Database name
    pub database: String,
    // Schema name. Mask allowed. For example: sche*
    pub scheme: String,
    // Flag of SSL usage
    pub ssl: bool,
    // Dump file name
    pub file: String,
}

impl DumpConfig {
    // Returns the connection string for the database.
    pub fn get_connection_string(&self) -> String {
        format!(
            "postgres://{}:{}@{}:{}/{}?sslmode={}",
            self.user,
            self.password,
            self.host,
            self.port,
            self.database,
            if self.ssl { "require" } else { "disable" }
        )
    }

    // Returns a masked connection string for the database.
    pub fn get_masked_connection_string(&self) -> String {
        format!(
            "postgres://*:*@{}:{}/{}?sslmode={}",
            self.host,
            self.port,
            self.database,
            if self.ssl { "require" } else { "disable" }
        )
    }
}
impl Default for DumpConfig {
    fn default() -> Self {
        DumpConfig {
            host: "localhost".to_string(),
            port: "5432".to_string(),
            user: "postgres".to_string(),
            password: "postgres".to_string(),
            database: "postgres".to_string(),
            scheme: "public".to_string(),
            ssl: false,
            file: "dump.io".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn test_get_connection_string_with_ssl_disabled() {
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

        let connection_string = config.get_connection_string();
        let expected = "postgres://testuser:testpass@localhost:5432/testdb?sslmode=disable";
        assert_eq!(connection_string, expected);
    }

    #[test]
    fn test_get_connection_string_with_ssl_enabled() {
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

        let connection_string = config.get_connection_string();
        let expected = "postgres://produser:securepass@remotehost:5433/proddb?sslmode=require";
        assert_eq!(connection_string, expected);
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

    #[test]
    fn test_connection_string_with_special_characters() {
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

        let connection_string = config.get_connection_string();
        let expected = "postgres://user@domain:pass!@#$%@test-host.example.com:5432/test_db-name?sslmode=disable";
        assert_eq!(connection_string, expected);

        let masked_string = config.get_masked_connection_string();
        let expected_masked =
            "postgres://*:*@test-host.example.com:5432/test_db-name?sslmode=disable";
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

        let connection_string = config.get_connection_string();
        let expected = "postgres://:@:/?sslmode=disable";
        assert_eq!(connection_string, expected);

        let masked_string = config.get_masked_connection_string();
        let expected_masked = "postgres://*:*@:/?sslmode=disable";
        assert_eq!(masked_string, expected_masked);
    }
}
