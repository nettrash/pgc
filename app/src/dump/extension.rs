use serde::{Deserialize, Serialize};

// This is an information about a PostgreSQL extension.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Extension {
    /// Name of the extension
    pub name: String,
    /// Version of the extension
    pub version: String,
    /// Schema where the extension is installed
    pub schema: String,
}

impl Extension {
    /// Creates a new Extension with the given name, version, and schema
    pub fn new(name: String, version: String, schema: String) -> Self {
        Self {
            name,
            version,
            schema,
        }
    }

    /// Hash
    pub fn hash(&self) -> String {
        format!(
            "{:x}",
            md5::compute(format!("{}.{}", self.schema, self.name))
        )
    }

    /// Returns a string to create the extension.
    pub fn get_script(&self) -> String {
        let script = format!(
            "create extension if not exists {} with schema {};\n",
            self.name, self.schema
        );

        script
    }

    /// Returns a string to drop the extension.
    pub fn get_drop_script(&self) -> String {
        format!("drop extension if exists {};\n", self.name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extension_new() {
        let extension = Extension::new(
            "uuid-ossp".to_string(),
            "1.1".to_string(),
            "public".to_string(),
        );

        assert_eq!(extension.name, "uuid-ossp");
        assert_eq!(extension.version, "1.1");
        assert_eq!(extension.schema, "public");
    }

    #[test]
    fn test_extension_creation_with_custom_schema() {
        let extension = Extension::new(
            "postgis".to_string(),
            "3.3.2".to_string(),
            "extensions".to_string(),
        );

        assert_eq!(extension.name, "postgis");
        assert_eq!(extension.version, "3.3.2");
        assert_eq!(extension.schema, "extensions");
    }

    #[test]
    fn test_extension_hash() {
        let extension = Extension::new(
            "uuid-ossp".to_string(),
            "1.1".to_string(),
            "public".to_string(),
        );

        let hash = extension.hash();

        // Hash should be consistent for the same input
        assert_eq!(hash, extension.hash());

        // Hash should be a valid hex string
        assert!(hash.chars().all(|c| c.is_ascii_hexdigit()));

        // Hash should be 32 characters (MD5 hex representation)
        assert_eq!(hash.len(), 32);
    }

    #[test]
    fn test_extension_hash_consistency() {
        let extension1 = Extension::new(
            "test_ext".to_string(),
            "1.0".to_string(),
            "test_schema".to_string(),
        );

        let extension2 = Extension::new(
            "test_ext".to_string(),
            "2.0".to_string(), // Different version
            "test_schema".to_string(),
        );

        // Hash should be the same for same name and schema, regardless of version
        assert_eq!(extension1.hash(), extension2.hash());
    }

    #[test]
    fn test_extension_hash_different_for_different_name() {
        let extension1 =
            Extension::new("ext1".to_string(), "1.0".to_string(), "public".to_string());

        let extension2 =
            Extension::new("ext2".to_string(), "1.0".to_string(), "public".to_string());

        // Hash should be different for different extension names
        assert_ne!(extension1.hash(), extension2.hash());
    }

    #[test]
    fn test_extension_hash_different_for_different_schema() {
        let extension1 = Extension::new(
            "test_ext".to_string(),
            "1.0".to_string(),
            "schema1".to_string(),
        );

        let extension2 = Extension::new(
            "test_ext".to_string(),
            "1.0".to_string(),
            "schema2".to_string(),
        );

        // Hash should be different for different schemas
        assert_ne!(extension1.hash(), extension2.hash());
    }

    #[test]
    fn test_get_script() {
        let extension = Extension::new(
            "uuid-ossp".to_string(),
            "1.1".to_string(),
            "public".to_string(),
        );

        let script = extension.get_script();
        let expected = "create extension if not exists uuid-ossp with schema public;\n";

        assert_eq!(script, expected);
    }

    #[test]
    fn test_get_script_with_custom_schema() {
        let extension = Extension::new(
            "postgis".to_string(),
            "3.3.2".to_string(),
            "extensions".to_string(),
        );

        let script = extension.get_script();
        let expected = "create extension if not exists postgis with schema extensions;\n";

        assert_eq!(script, expected);
    }

    #[test]
    fn test_get_script_with_special_characters() {
        let extension = Extension::new(
            "test-ext_name".to_string(),
            "1.0.0".to_string(),
            "custom_schema".to_string(),
        );

        let script = extension.get_script();
        let expected = "create extension if not exists test-ext_name with schema custom_schema;\n";

        assert_eq!(script, expected);
    }

    #[test]
    fn test_get_drop_script() {
        let extension = Extension::new(
            "uuid-ossp".to_string(),
            "1.1".to_string(),
            "public".to_string(),
        );

        let drop_script = extension.get_drop_script();
        let expected = "drop extension if exists uuid-ossp;\n";

        assert_eq!(drop_script, expected);
    }

    #[test]
    fn test_get_drop_script_with_different_names() {
        let extension = Extension::new(
            "postgis".to_string(),
            "3.3.2".to_string(),
            "extensions".to_string(),
        );

        let drop_script = extension.get_drop_script();
        let expected = "drop extension if exists postgis;\n";

        assert_eq!(drop_script, expected);
    }

    #[test]
    fn test_get_drop_script_with_special_characters() {
        let extension = Extension::new(
            "test-ext_name".to_string(),
            "1.0.0".to_string(),
            "custom_schema".to_string(),
        );

        let drop_script = extension.get_drop_script();
        let expected = "drop extension if exists test-ext_name;\n";

        assert_eq!(drop_script, expected);
    }

    #[test]
    fn test_extension_clone() {
        let original = Extension::new(
            "uuid-ossp".to_string(),
            "1.1".to_string(),
            "public".to_string(),
        );

        let cloned = original.clone();

        assert_eq!(original.name, cloned.name);
        assert_eq!(original.version, cloned.version);
        assert_eq!(original.schema, cloned.schema);
        assert_eq!(original.hash(), cloned.hash());
    }

    #[test]
    fn test_extension_debug_format() {
        let extension = Extension::new(
            "test_ext".to_string(),
            "1.0".to_string(),
            "test_schema".to_string(),
        );

        let debug_string = format!("{extension:?}");

        // Verify that the debug string contains all fields
        assert!(debug_string.contains("Extension"));
        assert!(debug_string.contains("name"));
        assert!(debug_string.contains("test_ext"));
        assert!(debug_string.contains("version"));
        assert!(debug_string.contains("1.0"));
        assert!(debug_string.contains("schema"));
        assert!(debug_string.contains("test_schema"));
    }

    #[test]
    fn test_serde_serialization() {
        let extension = Extension::new(
            "uuid-ossp".to_string(),
            "1.1".to_string(),
            "public".to_string(),
        );

        // Test serialization
        let json = serde_json::to_string(&extension).expect("Failed to serialize");
        assert!(json.contains("uuid-ossp"));
        assert!(json.contains("1.1"));
        assert!(json.contains("public"));

        // Test deserialization
        let deserialized: Extension = serde_json::from_str(&json).expect("Failed to deserialize");
        assert_eq!(extension.name, deserialized.name);
        assert_eq!(extension.version, deserialized.version);
        assert_eq!(extension.schema, deserialized.schema);
    }

    #[test]
    fn test_edge_cases_empty_strings() {
        let extension = Extension::new("".to_string(), "".to_string(), "".to_string());

        // Should handle empty strings gracefully
        assert_eq!(extension.name, "");
        assert_eq!(extension.version, "");
        assert_eq!(extension.schema, "");

        // Hash should still work with empty strings
        let hash = extension.hash();
        assert_eq!(hash.len(), 32);

        // Scripts should work with empty strings
        let script = extension.get_script();
        assert_eq!(script, "create extension if not exists  with schema ;\n");

        let drop_script = extension.get_drop_script();
        assert_eq!(drop_script, "drop extension if exists ;\n");
    }

    #[test]
    fn test_extension_with_very_long_names() {
        let long_name = "a".repeat(100);
        let long_version = "1.".repeat(50);
        let long_schema = "schema_".repeat(20);

        let extension =
            Extension::new(long_name.clone(), long_version.clone(), long_schema.clone());

        assert_eq!(extension.name, long_name);
        assert_eq!(extension.version, long_version);
        assert_eq!(extension.schema, long_schema);

        // Should still generate valid hash and scripts
        assert_eq!(extension.hash().len(), 32);
        assert!(extension.get_script().contains(&long_name));
        assert!(extension.get_script().contains(&long_schema));
        assert!(extension.get_drop_script().contains(&long_name));
    }

    #[test]
    fn test_known_md5_hash() {
        let extension = Extension::new("test".to_string(), "1.0".to_string(), "public".to_string());

        // The hash is computed from "public.test" (schema.name)
        // We can verify this produces a consistent MD5 hash
        let expected_hash = format!("{:x}", md5::compute("public.test"));
        assert_eq!(extension.hash(), expected_hash);
    }
}
