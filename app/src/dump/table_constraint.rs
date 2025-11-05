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
    /// ALTER TABLE ... ADD CONSTRAINT ...
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

    /// Get alter script to change this constraint to match the target constraint
    /// Returns None if the constraint needs to be dropped and recreated
    pub fn get_alter_script(&self, target: &TableConstraint) -> Option<String> {
        // Only FOREIGN KEY constraints can have their deferrable properties altered
        // All other changes require drop/recreate
        if self.constraint_type.eq_ignore_ascii_case("FOREIGN KEY")
            && target.constraint_type.eq_ignore_ascii_case("FOREIGN KEY")
            && self.can_be_altered_to(target)
        {
            let mut script = String::new();

            // Handle FOREIGN KEY deferrable property changes
            if self.is_deferrable != target.is_deferrable
                || self.initially_deferred != target.initially_deferred
            {
                if target.is_deferrable {
                    if target.initially_deferred {
                        script.push_str(&format!(
                            "alter table {}.{} alter constraint \"{}\" deferrable initially deferred;\n",
                            self.schema, self.table_name, target.name
                        ));
                    } else {
                        script.push_str(&format!(
                            "alter table {}.{} alter constraint \"{}\" deferrable initially immediate;\n",
                            self.schema, self.table_name, target.name
                        ));
                    }
                } else {
                    script.push_str(&format!(
                        "alter table {}.{} alter constraint \"{}\" not deferrable;\n",
                        self.schema, self.table_name, target.name
                    ));
                }
            }

            Some(script)
        } else {
            None
        }
    }

    /// Check if this constraint can be altered to match the target constraint
    /// without dropping and recreating
    pub fn can_be_altered_to(&self, target: &TableConstraint) -> bool {
        // Only FOREIGN KEY constraints can have their deferrable properties altered
        // All other changes require drop/recreate
        if self.constraint_type.eq_ignore_ascii_case("FOREIGN KEY")
            && target.constraint_type.eq_ignore_ascii_case("FOREIGN KEY")
        {
            // Check if only deferrable properties changed
            self.catalog == target.catalog
                && self.schema == target.schema
                && self.name == target.name
                && self.table_name == target.table_name
                && self.constraint_type == target.constraint_type
                && self.definition == target.definition
            // Only is_deferrable and initially_deferred can differ
        } else {
            false
        }
    }

    /// Get drop script for this constraint
    pub fn get_drop_script(&self) -> String {
        format!(
            "alter table {}.{} drop constraint \"{}\";\n",
            self.schema, self.table_name, self.name
        )
    }
}

impl PartialEq for TableConstraint {
    fn eq(&self, other: &Self) -> bool {
        self.schema == other.schema
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
    fn test_add_to_hasher_ignores_catalog() {
        let base_constraint = create_primary_key_constraint();
        let mut diff_catalog = base_constraint.clone();
        diff_catalog.catalog = "different_catalog".to_string();

        let mut hasher1 = Sha256::new();
        base_constraint.add_to_hasher(&mut hasher1);
        let hash1 = hasher1.finalize();

        let mut hasher2 = Sha256::new();
        diff_catalog.add_to_hasher(&mut hasher2);
        let hash2 = hasher2.finalize();

        assert_eq!(hash1, hash2);
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
    fn test_partial_eq_ignores_catalog() {
        let constraint1 = create_primary_key_constraint();
        let mut constraint2 = create_primary_key_constraint();
        constraint2.catalog = "different_catalog".to_string();

        assert_eq!(constraint1, constraint2);
        assert!(constraint1.eq(&constraint2));
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

    #[test]
    fn test_can_be_altered_to_foreign_key_deferrable_change() {
        let mut old_fk = create_foreign_key_constraint();
        old_fk.is_deferrable = false;
        old_fk.initially_deferred = false;

        let mut new_fk = old_fk.clone();
        new_fk.is_deferrable = true;
        new_fk.initially_deferred = true;

        assert!(old_fk.can_be_altered_to(&new_fk));
    }

    #[test]
    fn test_can_be_altered_to_foreign_key_definition_change() {
        let mut old_fk = create_foreign_key_constraint();
        old_fk.definition = Some("FOREIGN KEY (user_id) REFERENCES users(id)".to_string());

        let mut new_fk = old_fk.clone();
        new_fk.definition = Some("FOREIGN KEY (user_id) REFERENCES customers(id)".to_string());

        // Definition change requires drop/recreate
        assert!(!old_fk.can_be_altered_to(&new_fk));
    }

    #[test]
    fn test_can_be_altered_to_non_foreign_key() {
        let old_pk = create_primary_key_constraint();
        let mut new_pk = old_pk.clone();
        new_pk.is_deferrable = true; // This change is not supported for PK

        assert!(!old_pk.can_be_altered_to(&new_pk));
    }

    #[test]
    fn test_get_alter_script_foreign_key_to_deferrable() {
        let mut old_fk = create_foreign_key_constraint();
        old_fk.is_deferrable = false;
        old_fk.initially_deferred = false;

        let mut new_fk = old_fk.clone();
        new_fk.is_deferrable = true;
        new_fk.initially_deferred = true;

        let alter_script = old_fk.get_alter_script(&new_fk);
        assert!(alter_script.is_some());

        let script = alter_script.unwrap();
        assert!(script.contains("alter table app.orders alter constraint \"fk_orders_user_id\" deferrable initially deferred"));
    }

    #[test]
    fn test_get_alter_script_foreign_key_to_not_deferrable() {
        let old_fk = create_foreign_key_constraint(); // is_deferrable = true

        let mut new_fk = old_fk.clone();
        new_fk.is_deferrable = false;
        new_fk.initially_deferred = false;

        let alter_script = old_fk.get_alter_script(&new_fk);
        assert!(alter_script.is_some());

        let script = alter_script.unwrap();
        assert!(script.contains(
            "alter table app.orders alter constraint \"fk_orders_user_id\" not deferrable"
        ));
    }

    #[test]
    fn test_get_alter_script_no_change_needed() {
        let old_fk = create_foreign_key_constraint();
        let new_fk = old_fk.clone();

        let alter_script = old_fk.get_alter_script(&new_fk);
        assert!(alter_script.is_some());

        let script = alter_script.unwrap();
        assert!(script.is_empty()); // No changes needed
    }

    #[test]
    fn test_get_alter_script_non_foreign_key() {
        let old_pk = create_primary_key_constraint();
        let mut new_pk = old_pk.clone();
        new_pk.is_deferrable = true;

        let alter_script = old_pk.get_alter_script(&new_pk);
        assert!(alter_script.is_none()); // Cannot alter non-FK constraints
    }

    #[test]
    fn test_get_drop_script() {
        let constraint = create_foreign_key_constraint();
        let drop_script = constraint.get_drop_script();

        assert_eq!(
            drop_script,
            "alter table app.orders drop constraint \"fk_orders_user_id\";\n"
        );
    }
}
