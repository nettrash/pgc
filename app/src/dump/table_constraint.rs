use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

// This is an information about a PostgreSQL table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableConstraint {
    pub catalog: String,              // Catalog name
    pub schema: String,               // Schema name
    pub name: String,                 // Constraint name
    pub table_catalog: String,        // Table catalog
    pub table_schema: String,         // Table schema
    pub table_name: String,           // Table name
    pub constraint_type: String, // Type of the constraint (e.g., PRIMARY KEY, FOREIGN KEY, UNIQUE)
    pub is_deferrable: bool,     // Whether the constraint is deferrable
    pub initially_deferred: bool, // Whether the constraint is initially deferred
    pub enforced: bool,          // Whether the constraint is enforced
    pub nulls_distinct: Option<bool>, // Whether the constraint allows nulls to be distinct
}

impl TableConstraint {
    /// Hash
    pub fn add_to_hasher(&self, hasher: &mut Sha256) {
        hasher.update(self.catalog.as_bytes());
        hasher.update(self.schema.as_bytes());
        hasher.update(self.name.as_bytes());
        hasher.update(self.table_catalog.as_bytes());
        hasher.update(self.table_schema.as_bytes());
        hasher.update(self.table_name.as_bytes());
        hasher.update(self.constraint_type.as_bytes());
        hasher.update(self.is_deferrable.to_string().as_bytes());
        hasher.update(self.initially_deferred.to_string().as_bytes());
        hasher.update(self.enforced.to_string().as_bytes());
        if let Some(nulls_distinct) = self.nulls_distinct {
            hasher.update(nulls_distinct.to_string().as_bytes());
        }
    }

    /// Returns a string representation of the constraint
    pub fn get_script(&self) -> String {
        let mut script = String::new();
        script.push_str(&format!(
            "alter table {}.{} add constraint {} ",
            self.table_schema, self.table_name, self.name
        ));
        script.push_str(&format!("{} ", self.constraint_type.to_lowercase()));
        if self.is_deferrable {
            script.push_str("deferrable ");
        }
        if self.initially_deferred {
            script.push_str("initially deferred ");
        }
        if !self.enforced {
            script.push_str("not enforced ");
        }
        if let Some(nulls_distinct) = self.nulls_distinct {
            if nulls_distinct {
                script.push_str("nulls distinct ");
            } else {
                script.push_str("nulls not distinct ");
            }
        }
        script.push_str(";\n");
        script
    }
}

impl PartialEq for TableConstraint {
    fn eq(&self, other: &Self) -> bool {
        self.catalog == other.catalog
            && self.schema == other.schema
            && self.name == other.name
            && self.table_catalog == other.table_catalog
            && self.table_schema == other.table_schema
            && self.table_name == other.table_name
            && self.constraint_type == other.constraint_type
            && self.is_deferrable == other.is_deferrable
            && self.initially_deferred == other.initially_deferred
            && self.enforced == other.enforced
            && self.nulls_distinct == other.nulls_distinct
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
            table_catalog: "postgres".to_string(),
            table_schema: "public".to_string(),
            table_name: "users".to_string(),
            constraint_type: "PRIMARY KEY".to_string(),
            is_deferrable: false,
            initially_deferred: false,
            enforced: true,
            nulls_distinct: None,
        }
    }

    fn create_foreign_key_constraint() -> TableConstraint {
        TableConstraint {
            catalog: "postgres".to_string(),
            schema: "app".to_string(),
            name: "fk_orders_user_id".to_string(),
            table_catalog: "postgres".to_string(),
            table_schema: "app".to_string(),
            table_name: "orders".to_string(),
            constraint_type: "FOREIGN KEY".to_string(),
            is_deferrable: true,
            initially_deferred: true,
            enforced: true,
            nulls_distinct: None,
        }
    }

    fn create_unique_constraint() -> TableConstraint {
        TableConstraint {
            catalog: "analytics".to_string(),
            schema: "analytics".to_string(),
            name: "uk_products_sku".to_string(),
            table_catalog: "analytics".to_string(),
            table_schema: "analytics".to_string(),
            table_name: "products".to_string(),
            constraint_type: "UNIQUE".to_string(),
            is_deferrable: false,
            initially_deferred: false,
            enforced: true,
            nulls_distinct: Some(true),
        }
    }

    fn create_check_constraint() -> TableConstraint {
        TableConstraint {
            catalog: "test".to_string(),
            schema: "test".to_string(),
            name: "chk_age_positive".to_string(),
            table_catalog: "test".to_string(),
            table_schema: "test".to_string(),
            table_name: "persons".to_string(),
            constraint_type: "CHECK".to_string(),
            is_deferrable: false,
            initially_deferred: false,
            enforced: false,
            nulls_distinct: Some(false),
        }
    }

    #[test]
    fn test_table_constraint_creation_primary_key() {
        let constraint = create_primary_key_constraint();

        assert_eq!(constraint.catalog, "postgres");
        assert_eq!(constraint.schema, "public");
        assert_eq!(constraint.name, "pk_users_id");
        assert_eq!(constraint.table_catalog, "postgres");
        assert_eq!(constraint.table_schema, "public");
        assert_eq!(constraint.table_name, "users");
        assert_eq!(constraint.constraint_type, "PRIMARY KEY");
        assert!(!constraint.is_deferrable);
        assert!(!constraint.initially_deferred);
        assert!(constraint.enforced);
        assert_eq!(constraint.nulls_distinct, None);
    }

    #[test]
    fn test_table_constraint_creation_foreign_key() {
        let constraint = create_foreign_key_constraint();

        assert_eq!(constraint.catalog, "postgres");
        assert_eq!(constraint.schema, "app");
        assert_eq!(constraint.name, "fk_orders_user_id");
        assert_eq!(constraint.table_catalog, "postgres");
        assert_eq!(constraint.table_schema, "app");
        assert_eq!(constraint.table_name, "orders");
        assert_eq!(constraint.constraint_type, "FOREIGN KEY");
        assert!(constraint.is_deferrable);
        assert!(constraint.initially_deferred);
        assert!(constraint.enforced);
        assert_eq!(constraint.nulls_distinct, None);
    }

    #[test]
    fn test_table_constraint_creation_unique() {
        let constraint = create_unique_constraint();

        assert_eq!(constraint.catalog, "analytics");
        assert_eq!(constraint.schema, "analytics");
        assert_eq!(constraint.name, "uk_products_sku");
        assert_eq!(constraint.table_catalog, "analytics");
        assert_eq!(constraint.table_schema, "analytics");
        assert_eq!(constraint.table_name, "products");
        assert_eq!(constraint.constraint_type, "UNIQUE");
        assert!(!constraint.is_deferrable);
        assert!(!constraint.initially_deferred);
        assert!(constraint.enforced);
        assert_eq!(constraint.nulls_distinct, Some(true));
    }

    #[test]
    fn test_table_constraint_creation_check() {
        let constraint = create_check_constraint();

        assert_eq!(constraint.catalog, "test");
        assert_eq!(constraint.schema, "test");
        assert_eq!(constraint.name, "chk_age_positive");
        assert_eq!(constraint.table_catalog, "test");
        assert_eq!(constraint.table_schema, "test");
        assert_eq!(constraint.table_name, "persons");
        assert_eq!(constraint.constraint_type, "CHECK");
        assert!(!constraint.is_deferrable);
        assert!(!constraint.initially_deferred);
        assert!(!constraint.enforced);
        assert_eq!(constraint.nulls_distinct, Some(false));
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

        let mut constraint_diff_table_catalog = base_constraint.clone();
        constraint_diff_table_catalog.table_catalog = "different_table_catalog".to_string();

        let mut constraint_diff_table_schema = base_constraint.clone();
        constraint_diff_table_schema.table_schema = "different_table_schema".to_string();

        let mut constraint_diff_table_name = base_constraint.clone();
        constraint_diff_table_name.table_name = "different_table_name".to_string();

        let mut constraint_diff_type = base_constraint.clone();
        constraint_diff_type.constraint_type = "UNIQUE".to_string();

        let mut constraint_diff_deferrable = base_constraint.clone();
        constraint_diff_deferrable.is_deferrable = true;

        let mut constraint_diff_deferred = base_constraint.clone();
        constraint_diff_deferred.initially_deferred = true;

        let mut constraint_diff_enforced = base_constraint.clone();
        constraint_diff_enforced.enforced = false;

        let mut constraint_diff_nulls = base_constraint.clone();
        constraint_diff_nulls.nulls_distinct = Some(true);

        // Get base hash
        let mut hasher_base = Sha256::new();
        base_constraint.add_to_hasher(&mut hasher_base);
        let hash_base = format!("{:x}", hasher_base.finalize());

        // Test each variation produces different hash
        let constraints = vec![
            constraint_diff_catalog,
            constraint_diff_schema,
            constraint_diff_name,
            constraint_diff_table_catalog,
            constraint_diff_table_schema,
            constraint_diff_table_name,
            constraint_diff_type,
            constraint_diff_deferrable,
            constraint_diff_deferred,
            constraint_diff_enforced,
            constraint_diff_nulls,
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
    fn test_get_script_unique_nulls_distinct() {
        let constraint = create_unique_constraint();
        let script = constraint.get_script();

        let expected = "alter table analytics.products add constraint uk_products_sku unique nulls distinct ;\n";
        assert_eq!(script, expected);
    }

    #[test]
    fn test_get_script_check_not_enforced_nulls_not_distinct() {
        let constraint = create_check_constraint();
        let script = constraint.get_script();

        let expected = "alter table test.persons add constraint chk_age_positive check not enforced nulls not distinct ;\n";
        assert_eq!(script, expected);
    }

    #[test]
    fn test_get_script_with_all_options() {
        let constraint = TableConstraint {
            catalog: "test".to_string(),
            schema: "test".to_string(),
            name: "test_constraint".to_string(),
            table_catalog: "test".to_string(),
            table_schema: "test".to_string(),
            table_name: "test_table".to_string(),
            constraint_type: "UNIQUE".to_string(),
            is_deferrable: true,
            initially_deferred: true,
            enforced: false,
            nulls_distinct: Some(false),
        };

        let script = constraint.get_script();
        let expected = "alter table test.test_table add constraint test_constraint unique deferrable initially deferred not enforced nulls not distinct ;\n";
        assert_eq!(script, expected);
    }

    #[test]
    fn test_get_script_case_conversion() {
        let constraint = TableConstraint {
            catalog: "TEST".to_string(),
            schema: "PUBLIC".to_string(),
            name: "CONSTRAINT_NAME".to_string(),
            table_catalog: "TEST".to_string(),
            table_schema: "PUBLIC".to_string(),
            table_name: "USERS".to_string(),
            constraint_type: "PRIMARY KEY".to_string(),
            is_deferrable: false,
            initially_deferred: false,
            enforced: true,
            nulls_distinct: None,
        };

        let script = constraint.get_script();
        let expected = "alter table PUBLIC.USERS add constraint CONSTRAINT_NAME primary key ;\n";
        assert_eq!(script, expected);
    }

    #[test]
    fn test_get_script_with_empty_strings() {
        let constraint = TableConstraint {
            catalog: "".to_string(),
            schema: "".to_string(),
            name: "".to_string(),
            table_catalog: "".to_string(),
            table_schema: "".to_string(),
            table_name: "".to_string(),
            constraint_type: "".to_string(),
            is_deferrable: false,
            initially_deferred: false,
            enforced: true,
            nulls_distinct: None,
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
    fn test_partial_eq_different_table_catalog() {
        let constraint1 = create_primary_key_constraint();
        let mut constraint2 = create_primary_key_constraint();
        constraint2.table_catalog = "different_table_catalog".to_string();

        assert_ne!(constraint1, constraint2);
        assert!(!constraint1.eq(&constraint2));
    }

    #[test]
    fn test_partial_eq_different_table_schema() {
        let constraint1 = create_primary_key_constraint();
        let mut constraint2 = create_primary_key_constraint();
        constraint2.table_schema = "different_table_schema".to_string();

        assert_ne!(constraint1, constraint2);
        assert!(!constraint1.eq(&constraint2));
    }

    #[test]
    fn test_partial_eq_different_table_name() {
        let constraint1 = create_primary_key_constraint();
        let mut constraint2 = create_primary_key_constraint();
        constraint2.table_name = "different_table_name".to_string();

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
    fn test_partial_eq_different_enforced() {
        let constraint1 = create_primary_key_constraint();
        let mut constraint2 = create_primary_key_constraint();
        constraint2.enforced = false;

        assert_ne!(constraint1, constraint2);
        assert!(!constraint1.eq(&constraint2));
    }

    #[test]
    fn test_partial_eq_different_nulls_distinct() {
        let constraint1 = create_primary_key_constraint();
        let mut constraint2 = create_primary_key_constraint();
        constraint2.nulls_distinct = Some(true);

        assert_ne!(constraint1, constraint2);
        assert!(!constraint1.eq(&constraint2));
    }

    #[test]
    fn test_partial_eq_nulls_distinct_some_vs_none() {
        let mut constraint1 = create_primary_key_constraint();
        let mut constraint2 = create_primary_key_constraint();

        constraint1.nulls_distinct = Some(true);
        constraint2.nulls_distinct = None;

        assert_ne!(constraint1, constraint2);
        assert!(!constraint1.eq(&constraint2));
    }

    #[test]
    fn test_table_constraint_clone() {
        let original = create_primary_key_constraint();
        let cloned = original.clone();

        assert_eq!(original.catalog, cloned.catalog);
        assert_eq!(original.schema, cloned.schema);
        assert_eq!(original.name, cloned.name);
        assert_eq!(original.table_catalog, cloned.table_catalog);
        assert_eq!(original.table_schema, cloned.table_schema);
        assert_eq!(original.table_name, cloned.table_name);
        assert_eq!(original.constraint_type, cloned.constraint_type);
        assert_eq!(original.is_deferrable, cloned.is_deferrable);
        assert_eq!(original.initially_deferred, cloned.initially_deferred);
        assert_eq!(original.enforced, cloned.enforced);
        assert_eq!(original.nulls_distinct, cloned.nulls_distinct);
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
        assert!(debug_string.contains("table_catalog"));
        assert!(debug_string.contains("table_schema"));
        assert!(debug_string.contains("table_name"));
        assert!(debug_string.contains("users"));
        assert!(debug_string.contains("constraint_type"));
        assert!(debug_string.contains("PRIMARY KEY"));
        assert!(debug_string.contains("is_deferrable"));
        assert!(debug_string.contains("initially_deferred"));
        assert!(debug_string.contains("enforced"));
        assert!(debug_string.contains("nulls_distinct"));
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
        assert_eq!(constraint.table_catalog, deserialized.table_catalog);
        assert_eq!(constraint.table_schema, deserialized.table_schema);
        assert_eq!(constraint.table_name, deserialized.table_name);
        assert_eq!(constraint.constraint_type, deserialized.constraint_type);
        assert_eq!(constraint.is_deferrable, deserialized.is_deferrable);
        assert_eq!(
            constraint.initially_deferred,
            deserialized.initially_deferred
        );
        assert_eq!(constraint.enforced, deserialized.enforced);
        assert_eq!(constraint.nulls_distinct, deserialized.nulls_distinct);
        assert_eq!(constraint, deserialized);
    }

    #[test]
    fn test_constraint_with_special_characters() {
        let constraint = TableConstraint {
            catalog: "test-db".to_string(),
            schema: "app$schema".to_string(),
            name: "constraint@name".to_string(),
            table_catalog: "test-db".to_string(),
            table_schema: "app$schema".to_string(),
            table_name: "table#name".to_string(),
            constraint_type: "UNIQUE".to_string(),
            is_deferrable: false,
            initially_deferred: false,
            enforced: true,
            nulls_distinct: Some(true),
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
        assert!(script.contains("nulls distinct"));
        assert!(script.ends_with(";\n"));
    }

    #[test]
    fn test_different_constraint_types() {
        let constraints = vec![
            TableConstraint {
                catalog: "db".to_string(),
                schema: "public".to_string(),
                name: "pk_test".to_string(),
                table_catalog: "db".to_string(),
                table_schema: "public".to_string(),
                table_name: "test".to_string(),
                constraint_type: "PRIMARY KEY".to_string(),
                is_deferrable: false,
                initially_deferred: false,
                enforced: true,
                nulls_distinct: None,
            },
            TableConstraint {
                catalog: "db".to_string(),
                schema: "public".to_string(),
                name: "fk_test".to_string(),
                table_catalog: "db".to_string(),
                table_schema: "public".to_string(),
                table_name: "test".to_string(),
                constraint_type: "FOREIGN KEY".to_string(),
                is_deferrable: true,
                initially_deferred: false,
                enforced: true,
                nulls_distinct: None,
            },
            TableConstraint {
                catalog: "db".to_string(),
                schema: "public".to_string(),
                name: "uk_test".to_string(),
                table_catalog: "db".to_string(),
                table_schema: "public".to_string(),
                table_name: "test".to_string(),
                constraint_type: "UNIQUE".to_string(),
                is_deferrable: false,
                initially_deferred: false,
                enforced: true,
                nulls_distinct: Some(true),
            },
            TableConstraint {
                catalog: "db".to_string(),
                schema: "public".to_string(),
                name: "chk_test".to_string(),
                table_catalog: "db".to_string(),
                table_schema: "public".to_string(),
                table_name: "test".to_string(),
                constraint_type: "CHECK".to_string(),
                is_deferrable: false,
                initially_deferred: false,
                enforced: false,
                nulls_distinct: None,
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
            table_catalog: "tcat".to_string(),
            table_schema: "tsch".to_string(),
            table_name: "table".to_string(),
            constraint_type: "PK".to_string(),
            is_deferrable: false,
            initially_deferred: false,
            enforced: true,
            nulls_distinct: Some(true),
        };

        // Create the same hash as the implementation
        let mut hasher = Sha256::new();
        hasher.update("cat".as_bytes()); // catalog
        hasher.update("sch".as_bytes()); // schema
        hasher.update("name".as_bytes()); // name
        hasher.update("tcat".as_bytes()); // table_catalog
        hasher.update("tsch".as_bytes()); // table_schema
        hasher.update("table".as_bytes()); // table_name
        hasher.update("PK".as_bytes()); // constraint_type
        hasher.update("false".as_bytes()); // is_deferrable
        hasher.update("false".as_bytes()); // initially_deferred
        hasher.update("true".as_bytes()); // enforced
        hasher.update("true".as_bytes()); // nulls_distinct (Some(true))

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
            table_catalog: "tcat".to_string(),
            table_schema: "tsch".to_string(),
            table_name: "table".to_string(),
            constraint_type: "PK".to_string(),
            is_deferrable: false,
            initially_deferred: false,
            enforced: true,
            nulls_distinct: None,
        };

        // Create the same hash as the implementation (nulls_distinct=None means no update)
        let mut hasher = Sha256::new();
        hasher.update("cat".as_bytes()); // catalog
        hasher.update("sch".as_bytes()); // schema
        hasher.update("name".as_bytes()); // name
        hasher.update("tcat".as_bytes()); // table_catalog
        hasher.update("tsch".as_bytes()); // table_schema
        hasher.update("table".as_bytes()); // table_name
        hasher.update("PK".as_bytes()); // constraint_type
        hasher.update("false".as_bytes()); // is_deferrable
        hasher.update("false".as_bytes()); // initially_deferred
        hasher.update("true".as_bytes()); // enforced
        // No nulls_distinct update for None

        let expected_hash = format!("{:x}", hasher.finalize());

        let mut test_hasher = Sha256::new();
        constraint.add_to_hasher(&mut test_hasher);
        let actual_hash = format!("{:x}", test_hasher.finalize());

        assert_eq!(actual_hash, expected_hash);
    }
}
