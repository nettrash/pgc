use serde::{Deserialize, Serialize};
use sqlx::postgres::types::Oid;

// This is an information about a PostgreSQL routine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Routine {
    /// The schema name of the routine.
    pub schema: String,
    /// The object identifier of the routine.
    pub oid: Oid,
    /// The name of the routine.
    pub name: String,
    /// The language of the routine (e.g., 'plpgsql', 'sql').
    pub lang: String,
    /// The kind of the routine (e.g., 'function', 'procedure').
    pub kind: String,
    /// The return type of the routine (e.g., 'void', 'integer').
    pub return_type: String,
    /// The arguments of the routine, formatted as a string.
    pub arguments: String,
    /// The default values for the arguments, formatted as a string.
    pub arguments_defaults: Option<String>,
    /// The description of the routine.
    pub source_code: String,
}

impl Routine {
    /// Hash
    pub fn hash(&self) -> String {
        format!(
            "{:x}",
            md5::compute(format!(
                "{}.{}.{}.{}.{}.{}.{}.{}",
                self.schema,
                self.name,
                self.lang,
                self.kind,
                self.return_type,
                self.arguments,
                self.arguments_defaults.as_deref().unwrap_or(""),
                self.source_code
            ))
        )
    }

    /// Returns a string to create the routine.
    pub fn get_script(&self) -> String {
        let mut script = format!(
            "create or replace {} {}.{}({}) returns {} as $${}$$ language {};\n",
            self.kind.to_lowercase(),
            self.schema,
            self.name,
            self.arguments,
            self.return_type,
            self.source_code,
            self.lang
        );

        if let Some(defaults) = &self.arguments_defaults {
            script.push_str(&format!("-- Defaults: {defaults}\n"));
        }

        script
    }

    /// Returns a string to drop the routine.
    pub fn get_drop_script(&self) -> String {
        format!(
            "drop {} if exists {}.{} ({});\n",
            self.kind.to_lowercase(),
            self.schema,
            self.name,
            self.arguments
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::postgres::types::Oid;

    fn create_test_routine() -> Routine {
        Routine {
            schema: "public".to_string(),
            oid: Oid(12345),
            name: "test_function".to_string(),
            lang: "plpgsql".to_string(),
            kind: "FUNCTION".to_string(),
            return_type: "integer".to_string(),
            arguments: "param1 integer, param2 text".to_string(),
            arguments_defaults: Some("param1 default 0".to_string()),
            source_code: "BEGIN RETURN param1 + length(param2); END;".to_string(),
        }
    }

    fn create_test_procedure() -> Routine {
        Routine {
            schema: "app".to_string(),
            oid: Oid(67890),
            name: "test_procedure".to_string(),
            lang: "sql".to_string(),
            kind: "PROCEDURE".to_string(),
            return_type: "void".to_string(),
            arguments: "".to_string(),
            arguments_defaults: None,
            source_code: "INSERT INTO test_table VALUES (1, 'test');".to_string(),
        }
    }

    #[test]
    fn test_routine_creation() {
        let routine = create_test_routine();

        assert_eq!(routine.schema, "public");
        assert_eq!(routine.oid, Oid(12345));
        assert_eq!(routine.name, "test_function");
        assert_eq!(routine.lang, "plpgsql");
        assert_eq!(routine.kind, "FUNCTION");
        assert_eq!(routine.return_type, "integer");
        assert_eq!(routine.arguments, "param1 integer, param2 text");
        assert_eq!(
            routine.arguments_defaults,
            Some("param1 default 0".to_string())
        );
        assert_eq!(
            routine.source_code,
            "BEGIN RETURN param1 + length(param2); END;"
        );
    }

    #[test]
    fn test_routine_creation_without_defaults() {
        let routine = create_test_procedure();

        assert_eq!(routine.schema, "app");
        assert_eq!(routine.oid, Oid(67890));
        assert_eq!(routine.name, "test_procedure");
        assert_eq!(routine.lang, "sql");
        assert_eq!(routine.kind, "PROCEDURE");
        assert_eq!(routine.return_type, "void");
        assert_eq!(routine.arguments, "");
        assert_eq!(routine.arguments_defaults, None);
        assert_eq!(
            routine.source_code,
            "INSERT INTO test_table VALUES (1, 'test');"
        );
    }

    #[test]
    fn test_routine_hash() {
        let routine = create_test_routine();
        let hash = routine.hash();

        // Hash should be consistent for the same input
        assert_eq!(hash, routine.hash());

        // Hash should be a valid hex string
        assert!(hash.chars().all(|c| c.is_ascii_hexdigit()));

        // Hash should be 32 characters (MD5 hex representation)
        assert_eq!(hash.len(), 32);
    }

    #[test]
    fn test_routine_hash_consistency() {
        let routine1 = create_test_routine();
        let routine2 = create_test_routine();

        // Hash should be the same for identical routines
        assert_eq!(routine1.hash(), routine2.hash());
    }

    #[test]
    fn test_routine_hash_different_for_different_content() {
        let routine1 = create_test_routine();
        let mut routine2 = create_test_routine();
        routine2.name = "different_function".to_string();

        // Hash should be different for different routines
        assert_ne!(routine1.hash(), routine2.hash());
    }

    #[test]
    fn test_routine_hash_includes_all_fields() {
        let base_routine = create_test_routine();

        // Test that changing each field affects the hash
        let mut test_routine = base_routine.clone();
        test_routine.schema = "different_schema".to_string();
        assert_ne!(base_routine.hash(), test_routine.hash());

        let mut test_routine = base_routine.clone();
        test_routine.name = "different_name".to_string();
        assert_ne!(base_routine.hash(), test_routine.hash());

        let mut test_routine = base_routine.clone();
        test_routine.lang = "sql".to_string();
        assert_ne!(base_routine.hash(), test_routine.hash());

        let mut test_routine = base_routine.clone();
        test_routine.kind = "PROCEDURE".to_string();
        assert_ne!(base_routine.hash(), test_routine.hash());

        let mut test_routine = base_routine.clone();
        test_routine.return_type = "text".to_string();
        assert_ne!(base_routine.hash(), test_routine.hash());

        let mut test_routine = base_routine.clone();
        test_routine.arguments = "param1 text".to_string();
        assert_ne!(base_routine.hash(), test_routine.hash());

        let mut test_routine = base_routine.clone();
        test_routine.arguments_defaults = None;
        assert_ne!(base_routine.hash(), test_routine.hash());

        let mut test_routine = base_routine.clone();
        test_routine.source_code = "BEGIN RETURN 42; END;".to_string();
        assert_ne!(base_routine.hash(), test_routine.hash());
    }

    #[test]
    fn test_get_script_function_with_defaults() {
        let routine = create_test_routine();
        let script = routine.get_script();

        let expected = "create or replace function public.test_function(param1 integer, param2 text) returns integer as $$BEGIN RETURN param1 + length(param2); END;$$ language plpgsql;\n-- Defaults: param1 default 0\n";
        assert_eq!(script, expected);
    }

    #[test]
    fn test_get_script_procedure_without_defaults() {
        let routine = create_test_procedure();
        let script = routine.get_script();

        let expected = "create or replace procedure app.test_procedure() returns void as $$INSERT INTO test_table VALUES (1, 'test');$$ language sql;\n";
        assert_eq!(script, expected);
    }

    #[test]
    fn test_get_script_with_complex_source_code() {
        let routine = Routine {
            schema: "utils".to_string(),
            oid: Oid(11111),
            name: "complex_function".to_string(),
            lang: "plpgsql".to_string(),
            kind: "FUNCTION".to_string(),
            return_type: "table(id integer, name text)".to_string(),
            arguments: "search_term text".to_string(),
            arguments_defaults: None,
            source_code: "BEGIN\n    RETURN QUERY\n    SELECT t.id, t.name\n    FROM test_table t\n    WHERE t.name ILIKE '%' || search_term || '%';\nEND;".to_string(),
        };

        let script = routine.get_script();
        let expected = "create or replace function utils.complex_function(search_term text) returns table(id integer, name text) as $$BEGIN\n    RETURN QUERY\n    SELECT t.id, t.name\n    FROM test_table t\n    WHERE t.name ILIKE '%' || search_term || '%';\nEND;$$ language plpgsql;\n";
        assert_eq!(script, expected);
    }

    #[test]
    fn test_get_drop_script_function() {
        let routine = create_test_routine();
        let drop_script = routine.get_drop_script();

        let expected =
            "drop function if exists public.test_function (param1 integer, param2 text);\n";
        assert_eq!(drop_script, expected);
    }

    #[test]
    fn test_get_drop_script_procedure() {
        let routine = create_test_procedure();
        let drop_script = routine.get_drop_script();

        let expected = "drop procedure if exists app.test_procedure ();\n";
        assert_eq!(drop_script, expected);
    }

    #[test]
    fn test_get_drop_script_with_complex_arguments() {
        let routine = Routine {
            schema: "public".to_string(),
            oid: Oid(22222),
            name: "complex_function".to_string(),
            lang: "plpgsql".to_string(),
            kind: "FUNCTION".to_string(),
            return_type: "json".to_string(),
            arguments: "param1 integer[], param2 jsonb, param3 timestamp with time zone"
                .to_string(),
            arguments_defaults: None,
            source_code: "BEGIN RETURN '{}'; END;".to_string(),
        };

        let drop_script = routine.get_drop_script();
        let expected = "drop function if exists public.complex_function (param1 integer[], param2 jsonb, param3 timestamp with time zone);\n";
        assert_eq!(drop_script, expected);
    }

    #[test]
    fn test_routine_clone() {
        let original = create_test_routine();
        let cloned = original.clone();

        assert_eq!(original.schema, cloned.schema);
        assert_eq!(original.oid, cloned.oid);
        assert_eq!(original.name, cloned.name);
        assert_eq!(original.lang, cloned.lang);
        assert_eq!(original.kind, cloned.kind);
        assert_eq!(original.return_type, cloned.return_type);
        assert_eq!(original.arguments, cloned.arguments);
        assert_eq!(original.arguments_defaults, cloned.arguments_defaults);
        assert_eq!(original.source_code, cloned.source_code);
        assert_eq!(original.hash(), cloned.hash());
    }

    #[test]
    fn test_routine_debug_format() {
        let routine = create_test_routine();
        let debug_string = format!("{routine:?}");

        // Verify that the debug string contains all fields
        assert!(debug_string.contains("Routine"));
        assert!(debug_string.contains("schema"));
        assert!(debug_string.contains("public"));
        assert!(debug_string.contains("oid"));
        assert!(debug_string.contains("12345"));
        assert!(debug_string.contains("name"));
        assert!(debug_string.contains("test_function"));
        assert!(debug_string.contains("lang"));
        assert!(debug_string.contains("plpgsql"));
        assert!(debug_string.contains("kind"));
        assert!(debug_string.contains("FUNCTION"));
        assert!(debug_string.contains("return_type"));
        assert!(debug_string.contains("integer"));
        assert!(debug_string.contains("arguments"));
        assert!(debug_string.contains("source_code"));
    }

    #[test]
    fn test_serde_serialization() {
        let routine = create_test_routine();

        // Test serialization
        let json = serde_json::to_string(&routine).expect("Failed to serialize");
        assert!(json.contains("public"));
        assert!(json.contains("12345"));
        assert!(json.contains("test_function"));
        assert!(json.contains("plpgsql"));
        assert!(json.contains("FUNCTION"));
        assert!(json.contains("integer"));
        assert!(json.contains("param1 integer, param2 text"));
        assert!(json.contains("param1 default 0"));
        assert!(json.contains("BEGIN RETURN param1 + length(param2); END;"));

        // Test deserialization
        let deserialized: Routine = serde_json::from_str(&json).expect("Failed to deserialize");
        assert_eq!(routine.schema, deserialized.schema);
        assert_eq!(routine.oid, deserialized.oid);
        assert_eq!(routine.name, deserialized.name);
        assert_eq!(routine.lang, deserialized.lang);
        assert_eq!(routine.kind, deserialized.kind);
        assert_eq!(routine.return_type, deserialized.return_type);
        assert_eq!(routine.arguments, deserialized.arguments);
        assert_eq!(routine.arguments_defaults, deserialized.arguments_defaults);
        assert_eq!(routine.source_code, deserialized.source_code);
    }

    #[test]
    fn test_edge_cases_empty_strings() {
        let routine = Routine {
            schema: "".to_string(),
            oid: Oid(0),
            name: "".to_string(),
            lang: "".to_string(),
            kind: "".to_string(),
            return_type: "".to_string(),
            arguments: "".to_string(),
            arguments_defaults: None,
            source_code: "".to_string(),
        };

        // Should handle empty strings gracefully
        assert_eq!(routine.schema, "");
        assert_eq!(routine.name, "");
        assert_eq!(routine.lang, "");
        assert_eq!(routine.kind, "");
        assert_eq!(routine.return_type, "");
        assert_eq!(routine.arguments, "");
        assert_eq!(routine.source_code, "");

        // Hash should still work with empty strings
        let hash = routine.hash();
        assert_eq!(hash.len(), 32);

        // Scripts should work with empty strings
        let script = routine.get_script();
        assert_eq!(
            script,
            "create or replace  .() returns  as $$$$ language ;\n"
        );

        let drop_script = routine.get_drop_script();
        assert_eq!(drop_script, "drop  if exists . ();\n");
    }

    #[test]
    fn test_routine_with_special_characters() {
        let routine = Routine {
            schema: "test-schema".to_string(),
            oid: Oid(99999),
            name: "test_func$ion".to_string(),
            lang: "plpgsql".to_string(),
            kind: "FUNCTION".to_string(),
            return_type: "character varying(255)".to_string(),
            arguments: "param1 \"special name\" text, param2 'quoted' integer".to_string(),
            arguments_defaults: Some("param2 default 42".to_string()),
            source_code:
                "BEGIN\n    -- Comment with special chars: !@#$%^&*()\n    RETURN 'test'; \nEND;"
                    .to_string(),
        };

        // Should handle special characters in all fields
        let hash = routine.hash();
        assert_eq!(hash.len(), 32);

        let script = routine.get_script();
        assert!(script.contains("test-schema"));
        assert!(script.contains("test_func$ion"));
        assert!(script.contains("special name"));
        assert!(script.contains("!@#$%^&*()"));

        let drop_script = routine.get_drop_script();
        assert!(drop_script.contains("test-schema.test_func$ion"));
    }

    #[test]
    fn test_known_md5_hash() {
        let routine = Routine {
            schema: "test".to_string(),
            oid: Oid(1),
            name: "func".to_string(),
            lang: "sql".to_string(),
            kind: "FUNCTION".to_string(),
            return_type: "void".to_string(),
            arguments: "".to_string(),
            arguments_defaults: None,
            source_code: "SELECT 1;".to_string(),
        };

        // Create the same hash string as the implementation
        let hash_input = format!(
            "{}.{}.{}.{}.{}.{}.{}.{}",
            routine.schema,
            routine.name,
            routine.lang,
            routine.kind,
            routine.return_type,
            routine.arguments,
            routine.arguments_defaults.as_deref().unwrap_or(""),
            routine.source_code
        );

        let expected_hash = format!("{:x}", md5::compute(&hash_input));
        assert_eq!(routine.hash(), expected_hash);

        // Verify the actual hash input string: schema.name.lang.kind.return_type.arguments.arguments_defaults.source_code
        // With empty arguments and None arguments_defaults, we get: "test.func.sql.FUNCTION.void...SELECT 1;"
        assert_eq!(hash_input, "test.func.sql.FUNCTION.void...SELECT 1;");
    }

    #[test]
    fn test_arguments_defaults_handling() {
        let mut routine = create_test_routine();

        // Test with Some defaults
        let script_with_defaults = routine.get_script();
        assert!(script_with_defaults.contains("-- Defaults: param1 default 0"));

        // Test with None defaults
        routine.arguments_defaults = None;
        let script_without_defaults = routine.get_script();
        assert!(!script_without_defaults.contains("-- Defaults:"));
        assert!(!script_without_defaults.contains("param1 default 0"));
    }
}
