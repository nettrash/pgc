use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::postgres::types::Oid;

// This is an information about a PostgreSQL table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableTrigger {
    pub oid: Oid,           // Object identifier of the trigger
    pub name: String,       // Name of the trigger
    pub definition: String, // Definition of the trigger
}

impl TableTrigger {
    /// Hash
    pub fn add_to_hasher(&self, hasher: &mut Sha256) {
        hasher.update(self.oid.0.to_string().as_bytes());
        hasher.update(self.name.as_bytes());
        hasher.update(self.definition.as_bytes());
    }

    /// Returns a string representation of the trigger
    pub fn get_script(&self) -> String {
        let mut script = String::new();
        script.push_str(&self.definition);
        script.push(';');
        script
    }
}

impl PartialEq for TableTrigger {
    fn eq(&self, other: &Self) -> bool {
        self.oid == other.oid && self.name == other.name && self.definition == other.definition
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};

    fn create_test_trigger() -> TableTrigger {
        TableTrigger {
            oid: Oid(12345),
            name: "test_trigger".to_string(),
            definition: "before insert or update on test_table for each row execute function test_function()".to_string(),
        }
    }

    fn create_simple_trigger() -> TableTrigger {
        TableTrigger {
            oid: Oid(67890),
            name: "simple_trigger".to_string(),
            definition: "after delete on users for each row execute function audit_delete()"
                .to_string(),
        }
    }

    fn create_complex_trigger() -> TableTrigger {
        TableTrigger {
            oid: Oid(11111),
            name: "complex_trigger".to_string(),
            definition: "before insert or update of name, email on users for each row when (new.active = true) execute function validate_user()".to_string(),
        }
    }

    #[test]
    fn test_table_trigger_creation() {
        let trigger = create_test_trigger();

        assert_eq!(trigger.oid, Oid(12345));
        assert_eq!(trigger.name, "test_trigger");
        assert_eq!(
            trigger.definition,
            "before insert or update on test_table for each row execute function test_function()"
        );
    }

    #[test]
    fn test_table_trigger_creation_with_different_values() {
        let trigger = create_simple_trigger();

        assert_eq!(trigger.oid, Oid(67890));
        assert_eq!(trigger.name, "simple_trigger");
        assert_eq!(
            trigger.definition,
            "after delete on users for each row execute function audit_delete()"
        );
    }

    #[test]
    fn test_table_trigger_creation_complex() {
        let trigger = create_complex_trigger();

        assert_eq!(trigger.oid, Oid(11111));
        assert_eq!(trigger.name, "complex_trigger");
        assert_eq!(
            trigger.definition,
            "before insert or update of name, email on users for each row when (new.active = true) execute function validate_user()"
        );
    }

    #[test]
    fn test_add_to_hasher() {
        let trigger = create_test_trigger();
        let mut hasher1 = Sha256::new();
        let mut hasher2 = Sha256::new();

        // Add the same trigger to both hashers
        trigger.add_to_hasher(&mut hasher1);
        trigger.add_to_hasher(&mut hasher2);

        // Should produce the same hash
        let hash1 = format!("{:x}", hasher1.finalize());
        let hash2 = format!("{:x}", hasher2.finalize());
        assert_eq!(hash1, hash2);

        // Hash should be 64 characters (SHA256)
        assert_eq!(hash1.len(), 64);
        assert!(hash1.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_add_to_hasher_different_triggers() {
        let trigger1 = create_test_trigger();
        let trigger2 = create_simple_trigger();

        let mut hasher1 = Sha256::new();
        let mut hasher2 = Sha256::new();

        trigger1.add_to_hasher(&mut hasher1);
        trigger2.add_to_hasher(&mut hasher2);

        let hash1 = format!("{:x}", hasher1.finalize());
        let hash2 = format!("{:x}", hasher2.finalize());

        // Different triggers should produce different hashes
        assert_ne!(hash1, hash2);
    }

    #[test]
    fn test_add_to_hasher_includes_all_fields() {
        let base_trigger = create_test_trigger();

        // Test that changing each field affects the hash
        let mut trigger_diff_oid = base_trigger.clone();
        trigger_diff_oid.oid = Oid(99999);

        let mut trigger_diff_name = base_trigger.clone();
        trigger_diff_name.name = "different_name".to_string();

        let mut trigger_diff_definition = base_trigger.clone();
        trigger_diff_definition.definition =
            "after insert on different_table for each row execute function different_function()"
                .to_string();

        // Get hashes for all variations
        let mut hasher_base = Sha256::new();
        base_trigger.add_to_hasher(&mut hasher_base);
        let hash_base = format!("{:x}", hasher_base.finalize());

        let mut hasher_oid = Sha256::new();
        trigger_diff_oid.add_to_hasher(&mut hasher_oid);
        let hash_oid = format!("{:x}", hasher_oid.finalize());

        let mut hasher_name = Sha256::new();
        trigger_diff_name.add_to_hasher(&mut hasher_name);
        let hash_name = format!("{:x}", hasher_name.finalize());

        let mut hasher_definition = Sha256::new();
        trigger_diff_definition.add_to_hasher(&mut hasher_definition);
        let hash_definition = format!("{:x}", hasher_definition.finalize());

        // All hashes should be different
        assert_ne!(hash_base, hash_oid);
        assert_ne!(hash_base, hash_name);
        assert_ne!(hash_base, hash_definition);
        assert_ne!(hash_oid, hash_name);
        assert_ne!(hash_oid, hash_definition);
        assert_ne!(hash_name, hash_definition);
    }

    #[test]
    fn test_get_script_simple() {
        let trigger = create_test_trigger();
        let script = trigger.get_script();

        let expected = "create trigger test_trigger before insert or update on test_table for each row execute function test_function();";
        assert_eq!(script, expected);
    }

    #[test]
    fn test_get_script_after_delete() {
        let trigger = create_simple_trigger();
        let script = trigger.get_script();

        let expected = "create trigger simple_trigger after delete on users for each row execute function audit_delete();";
        assert_eq!(script, expected);
    }

    #[test]
    fn test_get_script_complex() {
        let trigger = create_complex_trigger();
        let script = trigger.get_script();

        let expected = "create trigger complex_trigger before insert or update of name, email on users for each row when (new.active = true) execute function validate_user();";
        assert_eq!(script, expected);
    }

    #[test]
    fn test_get_script_with_special_characters() {
        let trigger = TableTrigger {
            oid: Oid(22222),
            name: "test_trigger$name".to_string(),
            definition: "before insert on \"special-table\" for each row execute function \"validate_data\"()".to_string(),
        };

        let script = trigger.get_script();
        let expected = "create trigger test_trigger$name before insert on \"special-table\" for each row execute function \"validate_data\"();";
        assert_eq!(script, expected);
    }

    #[test]
    fn test_get_script_with_empty_definition() {
        let trigger = TableTrigger {
            oid: Oid(33333),
            name: "empty_trigger".to_string(),
            definition: "".to_string(),
        };

        let script = trigger.get_script();
        let expected = "create trigger empty_trigger ;";
        assert_eq!(script, expected);
    }

    #[test]
    fn test_partial_eq_identical_triggers() {
        let trigger1 = create_test_trigger();
        let trigger2 = create_test_trigger();

        assert_eq!(trigger1, trigger2);
        assert!(trigger1.eq(&trigger2));
    }

    #[test]
    fn test_partial_eq_different_oid() {
        let trigger1 = create_test_trigger();
        let mut trigger2 = create_test_trigger();
        trigger2.oid = Oid(99999);

        assert_ne!(trigger1, trigger2);
        assert!(!trigger1.eq(&trigger2));
    }

    #[test]
    fn test_partial_eq_different_name() {
        let trigger1 = create_test_trigger();
        let mut trigger2 = create_test_trigger();
        trigger2.name = "different_name".to_string();

        assert_ne!(trigger1, trigger2);
        assert!(!trigger1.eq(&trigger2));
    }

    #[test]
    fn test_partial_eq_different_definition() {
        let trigger1 = create_test_trigger();
        let mut trigger2 = create_test_trigger();
        trigger2.definition =
            "after insert on different_table for each row execute function different_function()"
                .to_string();

        assert_ne!(trigger1, trigger2);
        assert!(!trigger1.eq(&trigger2));
    }

    #[test]
    fn test_table_trigger_clone() {
        let original = create_test_trigger();
        let cloned = original.clone();

        assert_eq!(original.oid, cloned.oid);
        assert_eq!(original.name, cloned.name);
        assert_eq!(original.definition, cloned.definition);
        assert_eq!(original, cloned);

        // Verify hash consistency
        let mut hasher_original = Sha256::new();
        let mut hasher_cloned = Sha256::new();
        original.add_to_hasher(&mut hasher_original);
        cloned.add_to_hasher(&mut hasher_cloned);

        let hash_original = format!("{:x}", hasher_original.finalize());
        let hash_cloned = format!("{:x}", hasher_cloned.finalize());
        assert_eq!(hash_original, hash_cloned);
    }

    #[test]
    fn test_table_trigger_debug_format() {
        let trigger = create_test_trigger();
        let debug_string = format!("{trigger:?}");

        // Verify that the debug string contains all fields
        assert!(debug_string.contains("TableTrigger"));
        assert!(debug_string.contains("oid"));
        assert!(debug_string.contains("12345"));
        assert!(debug_string.contains("name"));
        assert!(debug_string.contains("test_trigger"));
        assert!(debug_string.contains("definition"));
        assert!(debug_string.contains("before insert or update"));
    }

    #[test]
    fn test_serde_serialization() {
        let trigger = create_test_trigger();

        // Test serialization
        let json = serde_json::to_string(&trigger).expect("Failed to serialize");
        assert!(json.contains("12345"));
        assert!(json.contains("test_trigger"));
        assert!(json.contains("before insert or update on test_table"));

        // Test deserialization
        let deserialized: TableTrigger =
            serde_json::from_str(&json).expect("Failed to deserialize");
        assert_eq!(trigger.oid, deserialized.oid);
        assert_eq!(trigger.name, deserialized.name);
        assert_eq!(trigger.definition, deserialized.definition);
        assert_eq!(trigger, deserialized);
    }

    #[test]
    fn test_edge_cases_empty_strings() {
        let trigger = TableTrigger {
            oid: Oid(0),
            name: "".to_string(),
            definition: "".to_string(),
        };

        // Should handle empty strings gracefully
        assert_eq!(trigger.oid, Oid(0));
        assert_eq!(trigger.name, "");
        assert_eq!(trigger.definition, "");

        // Hash should still work with empty strings
        let mut hasher = Sha256::new();
        trigger.add_to_hasher(&mut hasher);
        let hash = format!("{:x}", hasher.finalize());
        assert_eq!(hash.len(), 64);

        // Script should work with empty strings
        let script = trigger.get_script();
        assert_eq!(script, "create trigger  ;");

        // Equality should work
        let trigger2 = TableTrigger {
            oid: Oid(0),
            name: "".to_string(),
            definition: "".to_string(),
        };
        assert_eq!(trigger, trigger2);
    }

    #[test]
    fn test_trigger_with_multiline_definition() {
        let trigger = TableTrigger {
            oid: Oid(44444),
            name: "multiline_trigger".to_string(),
            definition: "before insert or update on users\n    for each row\n    when (new.email is not null)\n    execute function validate_email()".to_string(),
        };

        let script = trigger.get_script();
        let expected = "create trigger multiline_trigger before insert or update on users\n    for each row\n    when (new.email is not null)\n    execute function validate_email();";
        assert_eq!(script, expected);

        // Hash should work with multiline definitions
        let mut hasher = Sha256::new();
        trigger.add_to_hasher(&mut hasher);
        let hash = format!("{:x}", hasher.finalize());
        assert_eq!(hash.len(), 64);
    }

    #[test]
    fn test_trigger_with_very_long_definition() {
        let long_definition = "before insert or update on ".to_string()
            + &"very_long_table_name_".repeat(10)
            + " for each row execute function "
            + &"very_long_function_name_".repeat(5)
            + "()";

        let trigger = TableTrigger {
            oid: Oid(55555),
            name: "long_trigger".to_string(),
            definition: long_definition.clone(),
        };

        assert_eq!(trigger.definition, long_definition);

        let script = trigger.get_script();
        assert!(script.contains("create trigger long_trigger"));
        assert!(script.contains(&long_definition));
        assert!(script.ends_with(";"));

        // Hash should work with very long definitions
        let mut hasher = Sha256::new();
        trigger.add_to_hasher(&mut hasher);
        let hash = format!("{:x}", hasher.finalize());
        assert_eq!(hash.len(), 64);
    }

    #[test]
    fn test_known_sha256_hash() {
        let trigger = TableTrigger {
            oid: Oid(1),
            name: "test".to_string(),
            definition: "definition".to_string(),
        };

        // Create the same hash as the implementation
        let mut hasher = Sha256::new();
        hasher.update("1".as_bytes()); // oid.0.to_string()
        hasher.update("test".as_bytes()); // name
        hasher.update("definition".as_bytes()); // definition

        let expected_hash = format!("{:x}", hasher.finalize());

        let mut test_hasher = Sha256::new();
        trigger.add_to_hasher(&mut test_hasher);
        let actual_hash = format!("{:x}", test_hasher.finalize());

        assert_eq!(actual_hash, expected_hash);
    }

    #[test]
    fn test_trigger_types_coverage() {
        // Test different trigger types and events
        let triggers = vec![
            TableTrigger {
                oid: Oid(1001),
                name: "before_insert_trigger".to_string(),
                definition: "before insert on table1 for each row execute function func1()"
                    .to_string(),
            },
            TableTrigger {
                oid: Oid(1002),
                name: "after_update_trigger".to_string(),
                definition: "after update on table2 for each row execute function func2()"
                    .to_string(),
            },
            TableTrigger {
                oid: Oid(1003),
                name: "instead_of_trigger".to_string(),
                definition: "instead of delete on view1 for each row execute function func3()"
                    .to_string(),
            },
            TableTrigger {
                oid: Oid(1004),
                name: "statement_trigger".to_string(),
                definition: "after truncate on table3 for each statement execute function func4()"
                    .to_string(),
            },
        ];

        for trigger in triggers {
            // Each should produce a valid script
            let script = trigger.get_script();
            assert!(script.starts_with(&format!("create trigger {}", trigger.name)));
            assert!(script.ends_with(";"));
            assert!(script.contains(&trigger.definition));

            // Each should produce a valid hash
            let mut hasher = Sha256::new();
            trigger.add_to_hasher(&mut hasher);
            let hash = format!("{:x}", hasher.finalize());
            assert_eq!(hash.len(), 64);
        }
    }
}
