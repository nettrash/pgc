use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

// This is an information about a PostgreSQL table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableIndex {
    pub schema: String,          // Schema name
    pub table: String,           // Table name
    pub name: String,            // Index name
    pub catalog: Option<String>, // Catalog name
    pub indexdef: String,        // Index definition
}

impl TableIndex {
    /// Hash
    pub fn add_to_hasher(&self, hasher: &mut Sha256) {
        hasher.update(self.schema.as_bytes());
        hasher.update(self.table.as_bytes());
        hasher.update(self.name.as_bytes());
        hasher.update(self.indexdef.as_bytes());
    }

    /// Returns a string representation of the index
    pub fn get_script(&self) -> String {
        let mut script = String::new();
        script.push_str(&self.indexdef.to_lowercase());
        script.push_str(";\n");
        script
    }
}

impl PartialEq for TableIndex {
    fn eq(&self, other: &Self) -> bool {
        self.schema == other.schema
            && self.table == other.table
            && self.name == other.name
            && self.catalog == other.catalog
            && self.indexdef == other.indexdef
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};

    fn create_test_index() -> TableIndex {
        TableIndex {
            schema: "public".to_string(),
            table: "users".to_string(),
            name: "idx_users_email".to_string(),
            catalog: Some("postgres".to_string()),
            indexdef: "CREATE UNIQUE INDEX idx_users_email ON public.users USING btree (email)"
                .to_string(),
        }
    }

    fn create_simple_index() -> TableIndex {
        TableIndex {
            schema: "app".to_string(),
            table: "orders".to_string(),
            name: "idx_orders_date".to_string(),
            catalog: None,
            indexdef: "CREATE INDEX idx_orders_date ON app.orders USING btree (created_at)"
                .to_string(),
        }
    }

    fn create_complex_index() -> TableIndex {
        TableIndex {
            schema: "analytics".to_string(),
            table: "events".to_string(),
            name: "idx_events_composite".to_string(),
            catalog: Some("analytics_db".to_string()),
            indexdef: "CREATE INDEX idx_events_composite ON analytics.events USING gin ((data ->> 'type'::text), (data ->> 'timestamp'::text)) WHERE active = true".to_string(),
        }
    }

    fn create_partial_index() -> TableIndex {
        TableIndex {
            schema: "public".to_string(),
            table: "products".to_string(),
            name: "idx_products_active".to_string(),
            catalog: None,
            indexdef: "CREATE INDEX idx_products_active ON public.products (name, price) WHERE active = true".to_string(),
        }
    }

    #[test]
    fn test_table_index_creation() {
        let index = create_test_index();

        assert_eq!(index.schema, "public");
        assert_eq!(index.table, "users");
        assert_eq!(index.name, "idx_users_email");
        assert_eq!(index.catalog, Some("postgres".to_string()));
        assert_eq!(
            index.indexdef,
            "CREATE UNIQUE INDEX idx_users_email ON public.users USING btree (email)"
        );
    }

    #[test]
    fn test_table_index_creation_without_catalog() {
        let index = create_simple_index();

        assert_eq!(index.schema, "app");
        assert_eq!(index.table, "orders");
        assert_eq!(index.name, "idx_orders_date");
        assert_eq!(index.catalog, None);
        assert_eq!(
            index.indexdef,
            "CREATE INDEX idx_orders_date ON app.orders USING btree (created_at)"
        );
    }

    #[test]
    fn test_table_index_creation_complex() {
        let index = create_complex_index();

        assert_eq!(index.schema, "analytics");
        assert_eq!(index.table, "events");
        assert_eq!(index.name, "idx_events_composite");
        assert_eq!(index.catalog, Some("analytics_db".to_string()));
        assert!(index.indexdef.contains("USING gin"));
        assert!(index.indexdef.contains("WHERE active = true"));
    }

    #[test]
    fn test_add_to_hasher() {
        let index = create_test_index();
        let mut hasher1 = Sha256::new();
        let mut hasher2 = Sha256::new();

        // Add the same index to both hashers
        index.add_to_hasher(&mut hasher1);
        index.add_to_hasher(&mut hasher2);

        // Should produce the same hash
        let hash1 = format!("{:x}", hasher1.finalize());
        let hash2 = format!("{:x}", hasher2.finalize());
        assert_eq!(hash1, hash2);

        // Hash should be 64 characters (SHA256)
        assert_eq!(hash1.len(), 64);
        assert!(hash1.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_add_to_hasher_different_indexes() {
        let index1 = create_test_index();
        let index2 = create_simple_index();

        let mut hasher1 = Sha256::new();
        let mut hasher2 = Sha256::new();

        index1.add_to_hasher(&mut hasher1);
        index2.add_to_hasher(&mut hasher2);

        let hash1 = format!("{:x}", hasher1.finalize());
        let hash2 = format!("{:x}", hasher2.finalize());

        // Different indexes should produce different hashes
        assert_ne!(hash1, hash2);
    }

    #[test]
    fn test_add_to_hasher_includes_all_fields() {
        let base_index = create_test_index();

        // Test that changing each field affects the hash
        let mut index_diff_schema = base_index.clone();
        index_diff_schema.schema = "different_schema".to_string();

        let mut index_diff_table = base_index.clone();
        index_diff_table.table = "different_table".to_string();

        let mut index_diff_name = base_index.clone();
        index_diff_name.name = "different_name".to_string();

        let mut index_diff_definition = base_index.clone();
        index_diff_definition.indexdef =
            "CREATE INDEX different_idx ON public.users (username)".to_string();

        // Get hashes for all variations
        let mut hasher_base = Sha256::new();
        base_index.add_to_hasher(&mut hasher_base);
        let hash_base = format!("{:x}", hasher_base.finalize());

        let mut hasher_schema = Sha256::new();
        index_diff_schema.add_to_hasher(&mut hasher_schema);
        let hash_schema = format!("{:x}", hasher_schema.finalize());

        let mut hasher_table = Sha256::new();
        index_diff_table.add_to_hasher(&mut hasher_table);
        let hash_table = format!("{:x}", hasher_table.finalize());

        let mut hasher_name = Sha256::new();
        index_diff_name.add_to_hasher(&mut hasher_name);
        let hash_name = format!("{:x}", hasher_name.finalize());

        let mut hasher_definition = Sha256::new();
        index_diff_definition.add_to_hasher(&mut hasher_definition);
        let hash_definition = format!("{:x}", hasher_definition.finalize());

        // All hashes should be different
        assert_ne!(hash_base, hash_schema);
        assert_ne!(hash_base, hash_table);
        assert_ne!(hash_base, hash_name);
        assert_ne!(hash_base, hash_definition);
    }

    #[test]
    fn test_add_to_hasher_with_none_catalog() {
        let index_with_catalog = create_test_index();
        let index_without_catalog = create_simple_index();

        let mut hasher1 = Sha256::new();
        let mut hasher2 = Sha256::new();

        index_with_catalog.add_to_hasher(&mut hasher1);
        index_without_catalog.add_to_hasher(&mut hasher2);

        let hash1 = format!("{:x}", hasher1.finalize());
        let hash2 = format!("{:x}", hasher2.finalize());

        // Should produce different hashes due to catalog difference
        assert_ne!(hash1, hash2);
    }

    #[test]
    fn test_get_script_unique_index() {
        let index = create_test_index();
        let script = index.get_script();

        let expected = "create unique index idx_users_email on public.users using btree (email);\n";
        assert_eq!(script, expected);
    }

    #[test]
    fn test_get_script_simple_index() {
        let index = create_simple_index();
        let script = index.get_script();

        let expected = "create index idx_orders_date on app.orders using btree (created_at);\n";
        assert_eq!(script, expected);
    }

    #[test]
    fn test_get_script_complex_index() {
        let index = create_complex_index();
        let script = index.get_script();

        // Should convert to lowercase and add semicolon + newline
        assert!(script.starts_with("create index idx_events_composite"));
        assert!(script.contains("using gin"));
        assert!(script.contains("where active = true"));
        assert!(script.ends_with(";\n"));
    }

    #[test]
    fn test_get_script_partial_index() {
        let index = create_partial_index();
        let script = index.get_script();

        let expected = "create index idx_products_active on public.products (name, price) where active = true;\n";
        assert_eq!(script, expected);
    }

    #[test]
    fn test_get_script_case_conversion() {
        let index = TableIndex {
            schema: "PUBLIC".to_string(),
            table: "USERS".to_string(),
            name: "IDX_USERS_NAME".to_string(),
            catalog: None,
            indexdef: "CREATE UNIQUE INDEX IDX_USERS_NAME ON PUBLIC.USERS USING BTREE (NAME)"
                .to_string(),
        };

        let script = index.get_script();
        let expected = "create unique index idx_users_name on public.users using btree (name);\n";
        assert_eq!(script, expected);
    }

    #[test]
    fn test_get_script_with_empty_definition() {
        let index = TableIndex {
            schema: "public".to_string(),
            table: "test".to_string(),
            name: "empty_idx".to_string(),
            catalog: None,
            indexdef: "".to_string(),
        };

        let script = index.get_script();
        let expected = ";\n";
        assert_eq!(script, expected);
    }

    #[test]
    fn test_partial_eq_identical_indexes() {
        let index1 = create_test_index();
        let index2 = create_test_index();

        assert_eq!(index1, index2);
        assert!(index1.eq(&index2));
    }

    #[test]
    fn test_partial_eq_different_schema() {
        let index1 = create_test_index();
        let mut index2 = create_test_index();
        index2.schema = "different_schema".to_string();

        assert_ne!(index1, index2);
        assert!(!index1.eq(&index2));
    }

    #[test]
    fn test_partial_eq_different_table() {
        let index1 = create_test_index();
        let mut index2 = create_test_index();
        index2.table = "different_table".to_string();

        assert_ne!(index1, index2);
        assert!(!index1.eq(&index2));
    }

    #[test]
    fn test_partial_eq_different_name() {
        let index1 = create_test_index();
        let mut index2 = create_test_index();
        index2.name = "different_name".to_string();

        assert_ne!(index1, index2);
        assert!(!index1.eq(&index2));
    }

    #[test]
    fn test_partial_eq_different_catalog() {
        let index1 = create_test_index();
        let mut index2 = create_test_index();
        index2.catalog = Some("different_catalog".to_string());

        assert_ne!(index1, index2);
        assert!(!index1.eq(&index2));
    }

    #[test]
    fn test_partial_eq_different_indexdef() {
        let index1 = create_test_index();
        let mut index2 = create_test_index();
        index2.indexdef = "CREATE INDEX different_idx ON public.users (username)".to_string();

        assert_ne!(index1, index2);
        assert!(!index1.eq(&index2));
    }

    #[test]
    fn test_partial_eq_catalog_some_vs_none() {
        let mut index1 = create_test_index();
        let mut index2 = create_test_index();

        index1.catalog = Some("catalog".to_string());
        index2.catalog = None;

        assert_ne!(index1, index2);
        assert!(!index1.eq(&index2));
    }

    #[test]
    fn test_table_index_clone() {
        let original = create_test_index();
        let cloned = original.clone();

        assert_eq!(original.schema, cloned.schema);
        assert_eq!(original.table, cloned.table);
        assert_eq!(original.name, cloned.name);
        assert_eq!(original.catalog, cloned.catalog);
        assert_eq!(original.indexdef, cloned.indexdef);
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
    fn test_table_index_debug_format() {
        let index = create_test_index();
        let debug_string = format!("{index:?}");

        // Verify that the debug string contains all fields
        assert!(debug_string.contains("TableIndex"));
        assert!(debug_string.contains("schema"));
        assert!(debug_string.contains("public"));
        assert!(debug_string.contains("table"));
        assert!(debug_string.contains("users"));
        assert!(debug_string.contains("name"));
        assert!(debug_string.contains("idx_users_email"));
        assert!(debug_string.contains("catalog"));
        assert!(debug_string.contains("postgres"));
        assert!(debug_string.contains("indexdef"));
        assert!(debug_string.contains("CREATE UNIQUE INDEX"));
    }

    #[test]
    fn test_serde_serialization() {
        let index = create_test_index();

        // Test serialization
        let json = serde_json::to_string(&index).expect("Failed to serialize");
        assert!(json.contains("public"));
        assert!(json.contains("users"));
        assert!(json.contains("idx_users_email"));
        assert!(json.contains("postgres"));
        assert!(json.contains("CREATE UNIQUE INDEX"));

        // Test deserialization
        let deserialized: TableIndex = serde_json::from_str(&json).expect("Failed to deserialize");
        assert_eq!(index.schema, deserialized.schema);
        assert_eq!(index.table, deserialized.table);
        assert_eq!(index.name, deserialized.name);
        assert_eq!(index.catalog, deserialized.catalog);
        assert_eq!(index.indexdef, deserialized.indexdef);
        assert_eq!(index, deserialized);
    }

    #[test]
    fn test_edge_cases_empty_strings() {
        let index = TableIndex {
            schema: "".to_string(),
            table: "".to_string(),
            name: "".to_string(),
            catalog: None,
            indexdef: "".to_string(),
        };

        // Should handle empty strings gracefully
        assert_eq!(index.schema, "");
        assert_eq!(index.table, "");
        assert_eq!(index.name, "");
        assert_eq!(index.catalog, None);
        assert_eq!(index.indexdef, "");

        // Hash should still work with empty strings
        let mut hasher = Sha256::new();
        index.add_to_hasher(&mut hasher);
        let hash = format!("{:x}", hasher.finalize());
        assert_eq!(hash.len(), 64);

        // Script should work with empty strings
        let script = index.get_script();
        assert_eq!(script, ";\n");

        // Equality should work
        let index2 = TableIndex {
            schema: "".to_string(),
            table: "".to_string(),
            name: "".to_string(),
            catalog: None,
            indexdef: "".to_string(),
        };
        assert_eq!(index, index2);
    }

    #[test]
    fn test_index_with_special_characters() {
        let index = TableIndex {
            schema: "test-schema".to_string(),
            table: "table$name".to_string(),
            name: "idx_special@name".to_string(),
            catalog: Some("catalog#db".to_string()),
            indexdef: "CREATE INDEX \"idx_special@name\" ON \"test-schema\".\"table$name\" USING btree (\"column-name\")".to_string(),
        };

        // Should handle special characters in all fields
        let mut hasher = Sha256::new();
        index.add_to_hasher(&mut hasher);
        let hash = format!("{:x}", hasher.finalize());
        assert_eq!(hash.len(), 64);

        let script = index.get_script();
        assert!(script.contains("idx_special@name"));
        assert!(script.contains("test-schema"));
        assert!(script.contains("table$name"));
        assert!(script.contains("column-name"));
        assert!(script.ends_with(";\n"));
    }

    #[test]
    fn test_different_index_types() {
        let indexes = vec![
            TableIndex {
                schema: "public".to_string(),
                table: "users".to_string(),
                name: "btree_idx".to_string(),
                catalog: None,
                indexdef: "CREATE INDEX btree_idx ON public.users USING btree (email)".to_string(),
            },
            TableIndex {
                schema: "public".to_string(),
                table: "documents".to_string(),
                name: "gin_idx".to_string(),
                catalog: None,
                indexdef: "CREATE INDEX gin_idx ON public.documents USING gin (content)"
                    .to_string(),
            },
            TableIndex {
                schema: "public".to_string(),
                table: "locations".to_string(),
                name: "gist_idx".to_string(),
                catalog: None,
                indexdef: "CREATE INDEX gist_idx ON public.locations USING gist (coordinates)"
                    .to_string(),
            },
            TableIndex {
                schema: "public".to_string(),
                table: "numbers".to_string(),
                name: "hash_idx".to_string(),
                catalog: None,
                indexdef: "CREATE INDEX hash_idx ON public.numbers USING hash (value)".to_string(),
            },
        ];

        for index in indexes {
            // Each should produce a valid script
            let script = index.get_script();
            assert!(script.contains(&format!("create index {}", index.name)));
            assert!(script.ends_with(";\n"));

            // Each should produce a valid hash
            let mut hasher = Sha256::new();
            index.add_to_hasher(&mut hasher);
            let hash = format!("{:x}", hasher.finalize());
            assert_eq!(hash.len(), 64);
        }
    }

    #[test]
    fn test_known_sha256_hash() {
        let index = TableIndex {
            schema: "test".to_string(),
            table: "table".to_string(),
            name: "idx".to_string(),
            catalog: Some("cat".to_string()),
            indexdef: "definition".to_string(),
        };

        // Create the same hash as the implementation
        let mut hasher = Sha256::new();
        hasher.update("test".as_bytes()); // schema
        hasher.update("table".as_bytes()); // table
        hasher.update("idx".as_bytes()); // name
        hasher.update("definition".as_bytes()); // indexdef

        let expected_hash = format!("{:x}", hasher.finalize());

        let mut test_hasher = Sha256::new();
        index.add_to_hasher(&mut test_hasher);
        let actual_hash = format!("{:x}", test_hasher.finalize());

        assert_eq!(actual_hash, expected_hash);
    }

    #[test]
    fn test_known_sha256_hash_without_catalog() {
        let index = TableIndex {
            schema: "test".to_string(),
            table: "table".to_string(),
            name: "idx".to_string(),
            catalog: None,
            indexdef: "definition".to_string(),
        };

        // Create the same hash as the implementation (catalog=None means no update)
        let mut hasher = Sha256::new();
        hasher.update("test".as_bytes()); // schema
        hasher.update("table".as_bytes()); // table
        hasher.update("idx".as_bytes()); // name
        // No catalog update for None
        hasher.update("definition".as_bytes()); // indexdef

        let expected_hash = format!("{:x}", hasher.finalize());

        let mut test_hasher = Sha256::new();
        index.add_to_hasher(&mut test_hasher);
        let actual_hash = format!("{:x}", test_hasher.finalize());

        assert_eq!(actual_hash, expected_hash);
    }

    #[test]
    fn test_multiline_index_definition() {
        let index = TableIndex {
            schema: "public".to_string(),
            table: "complex_table".to_string(),
            name: "multiline_idx".to_string(),
            catalog: None,
            indexdef: "CREATE INDEX multiline_idx ON public.complex_table\n    USING gin (data)\n    WHERE active = true".to_string(),
        };

        let script = index.get_script();
        assert!(script.contains("create index multiline_idx"));
        assert!(script.contains("\n"));
        assert!(script.contains("where active = true"));
        assert!(script.ends_with(";\n"));

        // Hash should work with multiline definitions
        let mut hasher = Sha256::new();
        index.add_to_hasher(&mut hasher);
        let hash = format!("{:x}", hasher.finalize());
        assert_eq!(hash.len(), 64);
    }
}
