use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

// This is an information about a PostgreSQL table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableConstraint {
    pub catalog: String,            // Catalog name
    pub schema: String,             // Schema name
    pub name: String,               // Constraint name
    pub table_name: String,         // Table name
    pub constraint_type: String, // Type of the constraint (e.g., PRIMARY KEY, FOREIGN KEY, UNIQUE)
    pub is_deferrable: bool,     // Whether the constraint is deferrable
    pub initially_deferred: bool, // Whether the constraint is initially deferred
    pub definition: Option<String>, // Definition of the constraint (e.g., check expression)
}

impl TableConstraint {
    /// Hash
    pub fn add_to_hasher(&self, hasher: &mut Sha256) {
        hasher.update(self.catalog.as_bytes());
        hasher.update(self.schema.as_bytes());
        hasher.update(self.name.as_bytes());
        hasher.update(self.table_name.as_bytes());
        hasher.update(self.constraint_type.as_bytes());
        hasher.update(self.is_deferrable.to_string().as_bytes());
        hasher.update(self.initially_deferred.to_string().as_bytes());
        if let Some(definition) = &self.definition {
            hasher.update(definition.as_bytes());
        }
    }

    /// Returns a string representation of the constraint
    pub fn get_script(&self) -> String {
        let mut script = String::new();
        script.push_str(&format!(
            "alter table {}.{} add constraint {} ",
            self.schema, self.table_name, self.name
        ));

        // If a definition is provided, start from that (lowercased) and optionally append flags.
        // Otherwise, build from constraint_type and attribute flags.
        let clause = if let Some(def) = &self.definition {
            let mut base = def.to_lowercase();
            // Append deferrable flags for foreign key or unique if flags set
            if self.constraint_type.eq_ignore_ascii_case("FOREIGN KEY")
                || self.constraint_type.eq_ignore_ascii_case("UNIQUE")
            {
                if self.is_deferrable && !base.contains("deferrable") {
                    base.push_str(" deferrable");
                }
                if self.initially_deferred && !base.contains("initially deferred") {
                    base.push_str(" initially deferred");
                }
            }
            base
        } else {
            let mut parts: Vec<String> = Vec::new();
            match self.constraint_type.to_uppercase().as_str() {
                "PRIMARY KEY" => parts.push("primary key".to_string()),
                "FOREIGN KEY" => {
                    parts.push("foreign key".to_string());
                    if self.is_deferrable {
                        parts.push("deferrable".to_string());
                    }
                    if self.initially_deferred {
                        parts.push("initially deferred".to_string());
                    }
                }
                "UNIQUE" => parts.push("unique".to_string()),
                "CHECK" => parts.push("check".to_string()),
                _ => {}
            }
            parts.join(" ")
        };

        script.push_str(&format!("{} ", clause));
        script.push_str(";\n");
        script
    }
}

impl PartialEq for TableConstraint {
    fn eq(&self, other: &Self) -> bool {
        self.catalog == other.catalog
            && self.schema == other.schema
            && self.name == other.name
            && self.table_name == other.table_name
            && self.constraint_type == other.constraint_type
            && self.is_deferrable == other.is_deferrable
            && self.initially_deferred == other.initially_deferred
            && self.definition == other.definition
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};

    fn create_primary_key_constraint() -> TableConstraint {
        TableConstraint {
            catalog: "postgres".to_string(),
            schema: "public".to_string(),
            name: "pk_users_id".to_string(),
            table_name: "users".to_string(),
            constraint_type: "PRIMARY KEY".to_string(),
            is_deferrable: false,
            initially_deferred: false,
            definition: None,
        }
    }

    fn create_foreign_key_constraint() -> TableConstraint {
        TableConstraint {
            catalog: "postgres".to_string(),
            schema: "app".to_string(),
            name: "fk_orders_user_id".to_string(),
            table_name: "orders".to_string(),
            constraint_type: "FOREIGN KEY".to_string(),
            is_deferrable: true,
            initially_deferred: true,
            definition: None,
        }
    }

    fn create_unique_constraint() -> TableConstraint {
        TableConstraint {
            catalog: "analytics".to_string(),
            schema: "analytics".to_string(),
            name: "uk_products_sku".to_string(),
            table_name: "products".to_string(),
            constraint_type: "UNIQUE".to_string(),
            is_deferrable: false,
            initially_deferred: false,
            definition: None,
        }
    }

    fn create_check_constraint() -> TableConstraint {
        TableConstraint {
            catalog: "test".to_string(),
            schema: "test".to_string(),
            name: "chk_age_positive".to_string(),
            table_name: "persons".to_string(),
            constraint_type: "CHECK".to_string(),
            is_deferrable: false,
            initially_deferred: false,
            definition: None,
        }
    }

    #[test]
    fn test_table_constraint_creation_primary_key() {
        let constraint = create_primary_key_constraint();

        assert_eq!(constraint.catalog, "postgres");
        assert_eq!(constraint.schema, "public");
        assert_eq!(constraint.name, "pk_users_id");
        assert_eq!(constraint.table_name, "users");
        assert_eq!(constraint.constraint_type, "PRIMARY KEY");
        assert!(!constraint.is_deferrable);
        assert!(!constraint.initially_deferred);
        assert_eq!(constraint.definition, None);
    }

    #[test]
    fn test_table_constraint_creation_foreign_key() {
        let constraint = create_foreign_key_constraint();

        assert_eq!(constraint.catalog, "postgres");
        assert_eq!(constraint.schema, "app");
        assert_eq!(constraint.name, "fk_orders_user_id");
        assert_eq!(constraint.table_name, "orders");
        assert_eq!(constraint.constraint_type, "FOREIGN KEY");
        assert!(constraint.is_deferrable);
        assert!(constraint.initially_deferred);
        assert_eq!(constraint.definition, None);
    }

    #[test]
    fn test_table_constraint_creation_unique() {
        let constraint = create_unique_constraint();

        assert_eq!(constraint.catalog, "analytics");
        assert_eq!(constraint.schema, "analytics");
        assert_eq!(constraint.name, "uk_products_sku");
        assert_eq!(constraint.table_name, "products");
        assert_eq!(constraint.constraint_type, "UNIQUE");
        assert!(!constraint.is_deferrable);
        assert!(!constraint.initially_deferred);
        assert_eq!(constraint.definition, None);
    }

    #[test]
    fn test_table_constraint_creation_check() {
        let constraint = create_check_constraint();

        assert_eq!(constraint.catalog, "test");
        assert_eq!(constraint.schema, "test");
        assert_eq!(constraint.name, "chk_age_positive");
        assert_eq!(constraint.table_name, "persons");
        assert_eq!(constraint.constraint_type, "CHECK");
        assert!(!constraint.is_deferrable);
        assert!(!constraint.initially_deferred);
        assert_eq!(constraint.definition, None);
    }

    #[test]
    fn test_add_to_hasher() {
        let constraint = create_primary_key_constraint();
        let mut hasher1 = Sha256::new();
        let mut hasher2 = Sha256::new();

        // Add the same constraint to both hashers
        constraint.add_to_hasher(&mut hasher1);
        constraint.add_to_hasher(&mut hasher2);

        // Should produce the same hash
        let hash1 = format!("{:x}", hasher1.finalize());
        let hash2 = format!("{:x}", hasher2.finalize());
        assert_eq!(hash1, hash2);

        // Hash should be 64 characters (SHA256)
        assert_eq!(hash1.len(), 64);
        assert!(hash1.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_add_to_hasher_different_constraints() {
        let constraint1 = create_primary_key_constraint();
        let constraint2 = create_foreign_key_constraint();

        let mut hasher1 = Sha256::new();
        let mut hasher2 = Sha256::new();

        constraint1.add_to_hasher(&mut hasher1);
        constraint2.add_to_hasher(&mut hasher2);

        let hash1 = format!("{:x}", hasher1.finalize());
        let hash2 = format!("{:x}", hasher2.finalize());

        // Different constraints should produce different hashes
        assert_ne!(hash1, hash2);
    }

    #[test]
    fn test_add_to_hasher_includes_all_fields() {
        let base_constraint = create_primary_key_constraint();

        // Test that changing each field affects the hash
        let mut constraint_diff_catalog = base_constraint.clone();
        constraint_diff_catalog.catalog = "different_catalog".to_string();

        let mut constraint_diff_schema = base_constraint.clone();
        constraint_diff_schema.schema = "different_schema".to_string();

        let mut constraint_diff_name = base_constraint.clone();
        constraint_diff_name.name = "different_name".to_string();

        let mut constraint_diff_table_name = base_constraint.clone();
        constraint_diff_table_name.table_name = "different_table_name".to_string();

        let mut constraint_diff_type = base_constraint.clone();
        constraint_diff_type.constraint_type = "UNIQUE".to_string();

        let mut constraint_diff_deferrable = base_constraint.clone();
        constraint_diff_deferrable.is_deferrable = true;

        let mut constraint_diff_deferred = base_constraint.clone();
        constraint_diff_deferred.initially_deferred = true;

        // Get base hash
        let mut hasher_base = Sha256::new();
        base_constraint.add_to_hasher(&mut hasher_base);
        let hash_base = format!("{:x}", hasher_base.finalize());

        // Test each variation produces different hash
        let constraints = vec![
            constraint_diff_catalog,
            constraint_diff_schema,
            constraint_diff_name,
            constraint_diff_table_name,
            constraint_diff_type,
            constraint_diff_deferrable,
            constraint_diff_deferred,
        ];

        for constraint in constraints {
            let mut hasher = Sha256::new();
            constraint.add_to_hasher(&mut hasher);
            let hash = format!("{:x}", hasher.finalize());
            assert_ne!(hash_base, hash);
        }
    }

    #[test]
    fn test_get_script_primary_key() {
        let constraint = create_primary_key_constraint();
        let script = constraint.get_script();

        let expected = "alter table public.users add constraint pk_users_id primary key ;\n";
        assert_eq!(script, expected);
    }

    #[test]
    fn test_get_script_foreign_key_deferrable() {
        let constraint = create_foreign_key_constraint();
        let script = constraint.get_script();

        let expected = "alter table app.orders add constraint fk_orders_user_id foreign key deferrable initially deferred ;\n";
        assert_eq!(script, expected);
    }

    #[test]
    fn test_get_script_unique() {
        let constraint = create_unique_constraint();
        let script = constraint.get_script();
        // With reduced fields/behavior we no longer append null handling
        let expected = "alter table analytics.products add constraint uk_products_sku unique ;\n";
        assert_eq!(script, expected);
    }

    #[test]
    fn test_get_script_check() {
        let constraint = create_check_constraint();
        let script = constraint.get_script();
        // Simplified behavior: just the base type
        let expected = "alter table test.persons add constraint chk_age_positive check ;\n";
        assert_eq!(script, expected);
    }

    #[test]
    fn test_get_script_with_all_options() {
        let constraint = TableConstraint {
            catalog: "test".to_string(),
            schema: "test".to_string(),
            name: "test_constraint".to_string(),
            table_name: "test_table".to_string(),
            constraint_type: "UNIQUE".to_string(),
            is_deferrable: true,
            initially_deferred: true,
            definition: Some("UNIQUE (id)".to_string()),
        };

        let script = constraint.get_script();
        let expected = "alter table test.test_table add constraint test_constraint unique (id) deferrable initially deferred ;\n";
        assert_eq!(script, expected);
    }

    #[test]
    fn test_get_script_case_conversion() {
        let constraint = TableConstraint {
            catalog: "TEST".to_string(),
            schema: "PUBLIC".to_string(),
            name: "CONSTRAINT_NAME".to_string(),
            table_name: "USERS".to_string(),
            constraint_type: "PRIMARY KEY".to_string(),
            is_deferrable: false,
            initially_deferred: false,
            definition: Some("PRIMARY KEY (id)".to_string()),
        };

        let script = constraint.get_script();
        let expected =
            "alter table PUBLIC.USERS add constraint CONSTRAINT_NAME primary key (id) ;\n";
        assert_eq!(script, expected);
    }

    #[test]
    fn test_get_script_with_empty_strings() {
        let constraint = TableConstraint {
            catalog: "".to_string(),
            schema: "".to_string(),
            name: "".to_string(),
            table_name: "".to_string(),
            constraint_type: "".to_string(),
            is_deferrable: false,
            initially_deferred: false,
            definition: None,
        };

        let script = constraint.get_script();
        // Note: constraint_type.to_lowercase() produces empty string, but format!("{} ", "") produces " "
        let expected = "alter table . add constraint   ;\n";
        assert_eq!(script, expected);
    }

    #[test]
    fn test_partial_eq_identical_constraints() {
        let constraint1 = create_primary_key_constraint();
        let constraint2 = create_primary_key_constraint();

        assert_eq!(constraint1, constraint2);
        assert!(constraint1.eq(&constraint2));
    }

    #[test]
    fn test_partial_eq_different_catalog() {
        let constraint1 = create_primary_key_constraint();
        let mut constraint2 = create_primary_key_constraint();
        constraint2.catalog = "different_catalog".to_string();

        assert_ne!(constraint1, constraint2);
        assert!(!constraint1.eq(&constraint2));
    }

    #[test]
    fn test_partial_eq_different_schema() {
        let constraint1 = create_primary_key_constraint();
        let mut constraint2 = create_primary_key_constraint();
        constraint2.schema = "different_schema".to_string();

        assert_ne!(constraint1, constraint2);
        assert!(!constraint1.eq(&constraint2));
    }

    #[test]
    fn test_partial_eq_different_name() {
        let constraint1 = create_primary_key_constraint();
        let mut constraint2 = create_primary_key_constraint();
        constraint2.name = "different_name".to_string();

        assert_ne!(constraint1, constraint2);
        assert!(!constraint1.eq(&constraint2));
    }

    #[test]
    fn test_partial_eq_different_constraint_type() {
        let constraint1 = create_primary_key_constraint();
        let mut constraint2 = create_primary_key_constraint();
        constraint2.constraint_type = "UNIQUE".to_string();

        assert_ne!(constraint1, constraint2);
        assert!(!constraint1.eq(&constraint2));
    }

    #[test]
    fn test_partial_eq_different_is_deferrable() {
        let constraint1 = create_primary_key_constraint();
        let mut constraint2 = create_primary_key_constraint();
        constraint2.is_deferrable = true;

        assert_ne!(constraint1, constraint2);
        assert!(!constraint1.eq(&constraint2));
    }

    #[test]
    fn test_partial_eq_different_initially_deferred() {
        let constraint1 = create_primary_key_constraint();
        let mut constraint2 = create_primary_key_constraint();
        constraint2.initially_deferred = true;

        assert_ne!(constraint1, constraint2);
        assert!(!constraint1.eq(&constraint2));
    }

    #[test]
    fn test_table_constraint_debug_format() {
        let constraint = create_primary_key_constraint();
        let debug_string = format!("{constraint:?}");

        // Verify that the debug string contains all fields
        assert!(debug_string.contains("TableConstraint"));
        assert!(debug_string.contains("catalog"));
        assert!(debug_string.contains("postgres"));
        assert!(debug_string.contains("schema"));
        assert!(debug_string.contains("public"));
        assert!(debug_string.contains("name"));
        assert!(debug_string.contains("pk_users_id"));
        assert!(debug_string.contains("table_name"));
        assert!(debug_string.contains("users"));
        assert!(debug_string.contains("constraint_type"));
        assert!(debug_string.contains("PRIMARY KEY"));
        assert!(debug_string.contains("is_deferrable"));
        assert!(debug_string.contains("initially_deferred"));
    }

    #[test]
    fn test_serde_serialization() {
        let constraint = create_primary_key_constraint();

        // Test serialization
        let json = serde_json::to_string(&constraint).expect("Failed to serialize");
        assert!(json.contains("postgres"));
        assert!(json.contains("public"));
        assert!(json.contains("pk_users_id"));
        assert!(json.contains("users"));
        assert!(json.contains("PRIMARY KEY"));

        // Test deserialization
        let deserialized: TableConstraint =
            serde_json::from_str(&json).expect("Failed to deserialize");
        assert_eq!(constraint.catalog, deserialized.catalog);
        assert_eq!(constraint.schema, deserialized.schema);
        assert_eq!(constraint.name, deserialized.name);
        assert_eq!(constraint.table_name, deserialized.table_name);
        assert_eq!(constraint.constraint_type, deserialized.constraint_type);
        assert_eq!(constraint.is_deferrable, deserialized.is_deferrable);
        assert_eq!(
            constraint.initially_deferred,
            deserialized.initially_deferred
        );
        assert_eq!(constraint, deserialized);
    }

    #[test]
    fn test_constraint_with_special_characters() {
        let constraint = TableConstraint {
            catalog: "test-db".to_string(),
            schema: "app$schema".to_string(),
            name: "constraint@name".to_string(),
            table_name: "table#name".to_string(),
            constraint_type: "UNIQUE".to_string(),
            is_deferrable: false,
            initially_deferred: false,
            definition: Some("UNIQUE (column1, column2)".to_string()),
        };

        // Should handle special characters in all fields
        let mut hasher = Sha256::new();
        constraint.add_to_hasher(&mut hasher);
        let hash = format!("{:x}", hasher.finalize());
        assert_eq!(hash.len(), 64);

        let script = constraint.get_script();
        assert!(script.contains("app$schema.table#name"));
        assert!(script.contains("constraint@name"));
        assert!(script.contains("unique"));
        assert!(script.ends_with(";\n"));
    }

    #[test]
    fn test_different_constraint_types() {
        let constraints = vec![
            TableConstraint {
                catalog: "db".to_string(),
                schema: "public".to_string(),
                name: "pk_test".to_string(),
                table_name: "test".to_string(),
                constraint_type: "PRIMARY KEY".to_string(),
                is_deferrable: false,
                initially_deferred: false,
                definition: Some("PRIMARY KEY (id)".to_string()),
            },
            TableConstraint {
                catalog: "db".to_string(),
                schema: "public".to_string(),
                name: "fk_test".to_string(),
                table_name: "test".to_string(),
                constraint_type: "FOREIGN KEY".to_string(),
                is_deferrable: true,
                initially_deferred: false,
                definition: Some("FOREIGN KEY (user_id) REFERENCES users(id)".to_string()),
            },
            TableConstraint {
                catalog: "db".to_string(),
                schema: "public".to_string(),
                name: "uk_test".to_string(),
                table_name: "test".to_string(),
                constraint_type: "UNIQUE".to_string(),
                is_deferrable: false,
                initially_deferred: false,
                definition: Some("UNIQUE (column1, column2)".to_string()),
            },
            TableConstraint {
                catalog: "db".to_string(),
                schema: "public".to_string(),
                name: "chk_test".to_string(),
                table_name: "test".to_string(),
                constraint_type: "CHECK".to_string(),
                is_deferrable: false,
                initially_deferred: false,
                definition: Some("CHECK (age > 0)".to_string()),
            },
        ];

        for constraint in constraints {
            // Each should produce a valid script
            let script = constraint.get_script();
            assert!(script.contains(&format!("add constraint {}", constraint.name)));
            assert!(script.contains(&constraint.constraint_type.to_lowercase()));
            assert!(script.ends_with(";\n"));

            // Each should produce a valid hash
            let mut hasher = Sha256::new();
            constraint.add_to_hasher(&mut hasher);
            let hash = format!("{:x}", hasher.finalize());
            assert_eq!(hash.len(), 64);
        }
    }

    #[test]
    fn test_known_sha256_hash() {
        let constraint = TableConstraint {
            catalog: "cat".to_string(),
            schema: "sch".to_string(),
            name: "name".to_string(),
            table_name: "table".to_string(),
            constraint_type: "PK".to_string(),
            is_deferrable: false,
            initially_deferred: false,
            definition: None,
        };

        // Create the same hash as the implementation
        let mut hasher = Sha256::new();
        hasher.update("cat".as_bytes()); // catalog
        hasher.update("sch".as_bytes()); // schema
        hasher.update("name".as_bytes()); // name
        hasher.update("table".as_bytes()); // table_name
        hasher.update("PK".as_bytes()); // constraint_type
        hasher.update("false".as_bytes()); // is_deferrable
        hasher.update("false".as_bytes()); // initially_deferred

        let expected_hash = format!("{:x}", hasher.finalize());

        let mut test_hasher = Sha256::new();
        constraint.add_to_hasher(&mut test_hasher);
        let actual_hash = format!("{:x}", test_hasher.finalize());

        assert_eq!(actual_hash, expected_hash);
    }

    #[test]
    fn test_known_sha256_hash_without_nulls_distinct() {
        let constraint = TableConstraint {
            catalog: "cat".to_string(),
            schema: "sch".to_string(),
            name: "name".to_string(),
            table_name: "table".to_string(),
            constraint_type: "PK".to_string(),
            is_deferrable: false,
            initially_deferred: false,
            definition: None,
        };

        // Create the same hash as the implementation (nulls_distinct=None means no update)
        let mut hasher = Sha256::new();
        hasher.update("cat".as_bytes()); // catalog
        hasher.update("sch".as_bytes()); // schema
        hasher.update("name".as_bytes()); // name
        hasher.update("table".as_bytes()); // table_name
        hasher.update("PK".as_bytes()); // constraint_type
        hasher.update("false".as_bytes()); // is_deferrable
        hasher.update("false".as_bytes()); // initially_deferred
        // No nulls_distinct update for None

        let expected_hash = format!("{:x}", hasher.finalize());

        let mut test_hasher = Sha256::new();
        constraint.add_to_hasher(&mut test_hasher);
        let actual_hash = format!("{:x}", test_hasher.finalize());

        assert_eq!(actual_hash, expected_hash);
    }
}
