use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::utils::string_extensions::StringExt;

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
    #[serde(default)]
    pub coninhcount: i32, // Number of direct inheritance ancestors (0 = local, >0 = inherited)
    #[serde(default = "TableConstraint::default_enforced")]
    pub is_enforced: bool, // Whether the constraint is enforced (PG18+ supports NOT ENFORCED)
    #[serde(default)]
    pub no_inherit: bool, // Whether the constraint is marked NO INHERIT
    #[serde(default)]
    pub nulls_not_distinct: bool, // PG15+: UNIQUE constraint treats NULLs as not distinct
    /// Optional comment on the constraint
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
}

impl TableConstraint {
    /// Default value for is_enforced (backward compat with old dumps)
    fn default_enforced() -> bool {
        true
    }

    /// Hash
    pub fn add_to_hasher(&self, hasher: &mut Sha256) {
        hasher.update(self.schema.as_bytes());
        hasher.update(self.name.as_bytes());
        hasher.update(self.table_name.as_bytes());
        hasher.update(self.constraint_type.as_bytes());
        hasher.update(self.is_deferrable.to_string().as_bytes());
        hasher.update(self.initially_deferred.to_string().as_bytes());
        hasher.update(self.is_enforced.to_string().as_bytes());
        hasher.update(self.no_inherit.to_string().as_bytes());
        hasher.update(self.nulls_not_distinct.to_string().as_bytes());
        if let Some(definition) = &self.definition {
            hasher.update(Self::normalize_definition(definition).as_bytes());
        }
        if let Some(ref comment) = self.comment {
            hasher.update((comment.len() as u32).to_be_bytes());
            hasher.update(comment.as_bytes());
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

        // If a definition is provided, lowercase only the SQL keywords/identifiers,
        // preserving the original case of string literal contents so that round-tripping
        // through PGC does not produce a spurious diff.
        // Otherwise, build from constraint_type and attribute flags.
        let clause = if let Some(def) = &self.definition {
            let mut base = Self::lowercase_outside_literals(def);
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
                "EXCLUDE" => parts.push("exclude".to_string()),
                "NOT NULL" => parts.push("not null".to_string()),
                _ => {}
            }
            parts.join(" ")
        };

        script.push_str(&format!("{} ", clause));
        if !self.is_enforced {
            // Remove trailing space before appending NOT ENFORCED
            let trimmed = script.trim_end().to_string();
            script.clear();
            script.push_str(&trimmed);
            script.push_str(" not enforced ");
        }
        script.append_block(";");
        if let Some(ref comment) = self.comment {
            script.append_block(&format!(
                "comment on constraint {} on {}.{} is '{}';",
                self.name,
                self.schema,
                self.table_name,
                comment.replace('\'', "''")
            ));
        }
        script
    }

    /// Normalizes a constraint definition for comparison purposes.
    ///
    /// `pg_get_constraintdef()` may produce two semantically identical but
    /// textually different representations for CHECK constraints that use
    /// `IN (...)` / `ANY (ARRAY[...])`:
    ///
    ///   Form A (array-level cast):   `ARRAY['v'::character varying, ...]::text[]`
    ///   Form B (element-level cast):  `ARRAY['v'::character varying::text, ...]`
    ///
    /// Which form is returned depends on how the constraint was originally
    /// created (e.g. via `IN(...)` in DDL versus applying a migration that
    /// reuses Form A verbatim).  Normalize by lowercasing outside literals
    /// and collapsing the redundant `::text` casts so both forms compare equal.
    ///
    /// Both the lowercasing and the cast replacements are applied only to
    /// text **outside** single-quoted string literals, so literal contents
    /// like `']::text[]'` are never altered.
    fn normalize_definition(s: &str) -> String {
        let mut out = String::with_capacity(s.len());
        let mut chars = s.chars();
        // Accumulates non-literal text so we can apply replacements on it
        // in one go before flushing.
        let mut buf = String::new();

        while let Some(c) = chars.next() {
            if c == '\'' {
                // Flush the non-literal buffer (lowercased + cast-normalized).
                Self::flush_outside_buf(&mut buf, &mut out);

                // Inside a single-quoted literal — copy verbatim.
                out.push('\'');
                loop {
                    match chars.next() {
                        Some('\'') => {
                            out.push('\'');
                            if chars.as_str().starts_with('\'') {
                                out.push('\'');
                                chars.next();
                            } else {
                                break;
                            }
                        }
                        Some(ch) => out.push(ch),
                        None => break,
                    }
                }
            } else {
                // Outside a literal — collect into buf for batch processing.
                for lc in c.to_lowercase() {
                    buf.push(lc);
                }
            }
        }

        // Flush any remaining non-literal text.
        Self::flush_outside_buf(&mut buf, &mut out);
        out
    }

    /// Applies cast-normalization replacements to `buf` (which contains only
    /// non-literal text), appends the result to `out`, and clears `buf`.
    fn flush_outside_buf(buf: &mut String, out: &mut String) {
        if buf.is_empty() {
            return;
        }
        let normalized = buf
            .replace("::character varying::text", "::character varying")
            .replace("]::text[]", "]");
        out.push_str(&normalized);
        buf.clear();
    }

    /// Lowercases a SQL expression while preserving the original case of text
    /// inside single-quoted string literals.  Handles the standard `''` escape
    /// for embedded quotes.  Iterates by `char` so multi-byte UTF-8 sequences
    /// are never split.
    fn lowercase_outside_literals(s: &str) -> String {
        let mut out = String::with_capacity(s.len());
        let mut chars = s.chars();
        while let Some(c) = chars.next() {
            if c == '\'' {
                // Inside a single-quoted literal — copy verbatim.
                out.push('\'');
                loop {
                    match chars.next() {
                        Some('\'') => {
                            out.push('\'');
                            // Doubled-quote escape — copy the second quote
                            // and stay inside the literal.
                            if chars.as_str().starts_with('\'') {
                                out.push('\'');
                                chars.next();
                            } else {
                                break; // closing quote
                            }
                        }
                        Some(ch) => out.push(ch),
                        None => break, // unterminated literal
                    }
                }
            } else {
                // Outside a literal — apply full Unicode lowercasing.
                for lc in c.to_lowercase() {
                    out.push(lc);
                }
            }
        }
        out
    }

    /// Get alter script to change this constraint to match the target constraint
    /// Returns None if the constraint needs to be dropped and recreated
    pub fn get_alter_script(&self, target: &TableConstraint) -> Option<String> {
        // Helper: emit comment change SQL if needed
        let comment_script = |s: &mut String| {
            if self.comment != target.comment {
                if let Some(ref cmt) = target.comment {
                    s.append_block(&format!(
                        "comment on constraint {} on {}.{} is '{}';",
                        target.name,
                        target.schema,
                        target.table_name,
                        cmt.replace('\'', "''")
                    ));
                } else {
                    s.append_block(&format!(
                        "comment on constraint {} on {}.{} is null;",
                        target.name, target.schema, target.table_name
                    ));
                }
            }
        };

        // FOREIGN KEY constraints can have their deferrable and enforced properties altered
        // CHECK constraints can have their enforced property altered (PG18+)
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
                        script.append_block(&format!(
                            "alter table {}.{} alter constraint {} deferrable initially deferred;",
                            self.schema, self.table_name, target.name
                        ));
                    } else {
                        script.append_block(&format!(
                            "alter table {}.{} alter constraint {} deferrable initially immediate;",
                            self.schema, self.table_name, target.name
                        ));
                    }
                } else {
                    script.append_block(&format!(
                        "alter table {}.{} alter constraint {} not deferrable;",
                        self.schema, self.table_name, target.name
                    ));
                }
            }

            // Handle enforced property changes (PG18+)
            if self.is_enforced != target.is_enforced {
                if target.is_enforced {
                    script.append_block(&format!(
                        "alter table {}.{} alter constraint {} enforced;",
                        self.schema, self.table_name, target.name
                    ));
                } else {
                    script.append_block(&format!(
                        "alter table {}.{} alter constraint {} not enforced;",
                        self.schema, self.table_name, target.name
                    ));
                }
            }

            // Handle no_inherit property changes (PG18+)
            if self.no_inherit != target.no_inherit {
                if target.no_inherit {
                    script.append_block(&format!(
                        "alter table {}.{} alter constraint {} no inherit;",
                        self.schema, self.table_name, target.name
                    ));
                } else {
                    script.append_block(&format!(
                        "alter table {}.{} alter constraint {} inherit;",
                        self.schema, self.table_name, target.name
                    ));
                }
            }

            comment_script(&mut script);
            Some(script)
        } else if (self.constraint_type.eq_ignore_ascii_case("CHECK")
            || self.constraint_type.eq_ignore_ascii_case("FOREIGN KEY"))
            && self.can_be_altered_to(target)
            && (self.is_enforced != target.is_enforced
                || self.no_inherit != target.no_inherit
                || self.comment != target.comment)
        {
            // CHECK/FK constraint enforced or no_inherit property change (PG18+)
            let mut script = String::new();
            if self.is_enforced != target.is_enforced {
                if target.is_enforced {
                    script.append_block(&format!(
                        "alter table {}.{} alter constraint {} enforced;",
                        self.schema, self.table_name, target.name
                    ));
                } else {
                    script.append_block(&format!(
                        "alter table {}.{} alter constraint {} not enforced;",
                        self.schema, self.table_name, target.name
                    ));
                }
            }
            if self.no_inherit != target.no_inherit {
                if target.no_inherit {
                    script.append_block(&format!(
                        "alter table {}.{} alter constraint {} no inherit;",
                        self.schema, self.table_name, target.name
                    ));
                } else {
                    script.append_block(&format!(
                        "alter table {}.{} alter constraint {} inherit;",
                        self.schema, self.table_name, target.name
                    ));
                }
            }
            comment_script(&mut script);
            Some(script)
        } else if self.comment != target.comment
            && self.schema == target.schema
            && self.name == target.name
            && self.table_name == target.table_name
            && self.constraint_type == target.constraint_type
            && self.is_deferrable == target.is_deferrable
            && self.initially_deferred == target.initially_deferred
            && self.is_enforced == target.is_enforced
            && self.no_inherit == target.no_inherit
            && self.nulls_not_distinct == target.nulls_not_distinct
            && self.definition.as_deref().map(Self::normalize_definition)
                == target.definition.as_deref().map(Self::normalize_definition)
        {
            // Only comment changed - any constraint type
            let mut script = String::new();
            comment_script(&mut script);
            Some(script)
        } else {
            None
        }
    }

    /// Check if this constraint can be altered to match the target constraint
    /// without dropping and recreating
    pub fn can_be_altered_to(&self, target: &TableConstraint) -> bool {
        // FOREIGN KEY constraints can have deferrable and enforced properties altered
        // CHECK constraints can have enforced property altered (PG18+)
        // All other changes require drop/recreate
        if self.constraint_type.eq_ignore_ascii_case("FOREIGN KEY")
            && target.constraint_type.eq_ignore_ascii_case("FOREIGN KEY")
        {
            // Check if only deferrable/enforced properties changed
            self.catalog == target.catalog
                && self.schema == target.schema
                && self.name == target.name
                && self.table_name == target.table_name
                && self.constraint_type == target.constraint_type
                && self.nulls_not_distinct == target.nulls_not_distinct
                && self.definition.as_deref().map(Self::normalize_definition)
                    == target.definition.as_deref().map(Self::normalize_definition)
            // is_deferrable, initially_deferred, is_enforced, and no_inherit can differ
        } else if self.constraint_type.eq_ignore_ascii_case("CHECK")
            && target.constraint_type.eq_ignore_ascii_case("CHECK")
        {
            // CHECK constraints: enforced and no_inherit can be altered (PG18+)
            self.catalog == target.catalog
                && self.schema == target.schema
                && self.name == target.name
                && self.table_name == target.table_name
                && self.constraint_type == target.constraint_type
                && self.is_deferrable == target.is_deferrable
                && self.initially_deferred == target.initially_deferred
                && self.nulls_not_distinct == target.nulls_not_distinct
                && self.definition.as_deref().map(Self::normalize_definition)
                    == target.definition.as_deref().map(Self::normalize_definition)
            // is_enforced and no_inherit can differ
        } else {
            false
        }
    }

    /// Get drop script for this constraint
    pub fn get_drop_script(&self) -> String {
        format!(
            "alter table {}.{} drop constraint {};",
            self.schema, self.table_name, self.name
        )
        .with_empty_lines()
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
            && self.is_enforced == other.is_enforced
            && self.no_inherit == other.no_inherit
            && self.nulls_not_distinct == other.nulls_not_distinct
            && self.comment == other.comment
            && self.definition.as_deref().map(Self::normalize_definition)
                == other.definition.as_deref().map(Self::normalize_definition)
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
            coninhcount: 0,
            is_enforced: true,
            no_inherit: false,
            nulls_not_distinct: false,
            comment: None,
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
            coninhcount: 0,
            is_enforced: true,
            no_inherit: false,
            nulls_not_distinct: false,
            comment: None,
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
            coninhcount: 0,
            is_enforced: true,
            no_inherit: false,
            nulls_not_distinct: false,
            comment: None,
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
            coninhcount: 0,
            is_enforced: true,
            no_inherit: false,
            nulls_not_distinct: false,
            comment: None,
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

        let expected = "alter table public.users add constraint pk_users_id primary key ;\n\n";
        assert_eq!(script, expected);
    }

    #[test]
    fn test_get_script_foreign_key_deferrable() {
        let constraint = create_foreign_key_constraint();
        let script = constraint.get_script();

        let expected = "alter table app.orders add constraint fk_orders_user_id foreign key deferrable initially deferred ;\n\n";
        assert_eq!(script, expected);
    }

    #[test]
    fn test_get_script_unique() {
        let constraint = create_unique_constraint();
        let script = constraint.get_script();
        // With reduced fields/behavior we no longer append null handling
        let expected = "alter table analytics.products add constraint uk_products_sku unique ;\n\n";
        assert_eq!(script, expected);
    }

    #[test]
    fn test_get_script_check() {
        let constraint = create_check_constraint();
        let script = constraint.get_script();
        // Simplified behavior: just the base type
        let expected = "alter table test.persons add constraint chk_age_positive check ;\n\n";
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
            coninhcount: 0,
            is_enforced: true,
            no_inherit: false,
            nulls_not_distinct: false,
            comment: None,
        };

        let script = constraint.get_script();
        let expected = "alter table test.test_table add constraint test_constraint unique (id) deferrable initially deferred ;\n\n";
        assert_eq!(script, expected);
    }

    #[test]
    fn test_get_script_case_conversion() {
        let constraint = TableConstraint {
            catalog: "TEST".to_string(),
            schema: "\"PUBLIC\"".to_string(),
            name: "\"CONSTRAINT_NAME\"".to_string(),
            table_name: "\"USERS\"".to_string(),
            constraint_type: "PRIMARY KEY".to_string(),
            is_deferrable: false,
            initially_deferred: false,
            definition: Some("PRIMARY KEY (id)".to_string()),
            coninhcount: 0,
            is_enforced: true,
            no_inherit: false,
            nulls_not_distinct: false,
            comment: None,
        };

        let script = constraint.get_script();
        let expected = "alter table \"PUBLIC\".\"USERS\" add constraint \"CONSTRAINT_NAME\" primary key (id) ;\n\n";
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
            coninhcount: 0,
            is_enforced: true,
            no_inherit: false,
            nulls_not_distinct: false,
            comment: None,
        };

        let script = constraint.get_script();
        // Note: constraint_type.to_lowercase() produces empty string, but format!("{} ", "") produces " "
        let expected = "alter table . add constraint   ;\n\n";
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
            catalog: "\"test-db\"".to_string(),
            schema: "\"app$schema\"".to_string(),
            name: "\"constraint@name\"".to_string(),
            table_name: "\"table#name\"".to_string(),
            constraint_type: "UNIQUE".to_string(),
            is_deferrable: false,
            initially_deferred: false,
            definition: Some("UNIQUE (column1, column2)".to_string()),
            coninhcount: 0,
            is_enforced: true,
            no_inherit: false,
            nulls_not_distinct: false,
            comment: None,
        };

        // Should handle special characters in all fields
        let mut hasher = Sha256::new();
        constraint.add_to_hasher(&mut hasher);
        let hash = format!("{:x}", hasher.finalize());
        assert_eq!(hash.len(), 64);

        let script = constraint.get_script();
        assert!(script.contains("\"app$schema\".\"table#name\""));
        assert!(script.contains("\"constraint@name\""));
        assert!(script.contains("unique"));
        assert!(script.ends_with(";\n\n"));
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
                coninhcount: 0,
                is_enforced: true,
                no_inherit: false,
                nulls_not_distinct: false,
                comment: None,
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
                coninhcount: 0,
                is_enforced: true,
                no_inherit: false,
                nulls_not_distinct: false,
                comment: None,
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
                coninhcount: 0,
                is_enforced: true,
                no_inherit: false,
                nulls_not_distinct: false,
                comment: None,
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
                coninhcount: 0,
                is_enforced: true,
                no_inherit: false,
                nulls_not_distinct: false,
                comment: None,
            },
        ];

        for constraint in constraints {
            // Each should produce a valid script
            let script = constraint.get_script();
            assert!(script.contains(&format!("add constraint {}", constraint.name)));
            assert!(script.contains(&constraint.constraint_type.to_lowercase()));
            assert!(script.ends_with(";\n\n"));

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
            coninhcount: 0,
            is_enforced: true,
            no_inherit: false,
            nulls_not_distinct: false,
            comment: None,
        };

        // Create the same hash as the implementation
        let mut hasher = Sha256::new();
        hasher.update("sch".as_bytes()); // schema
        hasher.update("name".as_bytes()); // name
        hasher.update("table".as_bytes()); // table_name
        hasher.update("PK".as_bytes()); // constraint_type
        hasher.update("false".as_bytes()); // is_deferrable
        hasher.update("false".as_bytes()); // initially_deferred
        hasher.update("true".as_bytes()); // is_enforced
        hasher.update("false".as_bytes()); // no_inherit
        hasher.update("false".as_bytes()); // nulls_not_distinct

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
            coninhcount: 0,
            is_enforced: true,
            no_inherit: false,
            nulls_not_distinct: false,
            comment: None,
        };

        // Create the same hash as the implementation (nulls_distinct=None means no update)
        let mut hasher = Sha256::new();
        hasher.update("sch".as_bytes()); // schema
        hasher.update("name".as_bytes()); // name
        hasher.update("table".as_bytes()); // table_name
        hasher.update("PK".as_bytes()); // constraint_type
        hasher.update("false".as_bytes()); // is_deferrable
        hasher.update("false".as_bytes()); // initially_deferred
        hasher.update("true".as_bytes()); // is_enforced
        hasher.update("false".as_bytes()); // no_inherit
        hasher.update("false".as_bytes()); // nulls_not_distinct
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
        assert!(script.contains("alter table app.orders alter constraint fk_orders_user_id deferrable initially deferred"));
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
        assert!(
            script.contains(
                "alter table app.orders alter constraint fk_orders_user_id not deferrable"
            )
        );
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
            "alter table app.orders drop constraint fk_orders_user_id;\n\n"
        );
    }

    // --- lowercase_outside_literals / string-literal preservation ---

    #[test]
    fn test_check_constraint_preserves_string_literal_case() {
        // This is the core regression: CHECK (status = 'Active') must NOT become
        // check (status = 'active'), or the next comparison produces a false diff.
        let constraint = TableConstraint {
            catalog: "db".to_string(),
            schema: "public".to_string(),
            name: "chk_status".to_string(),
            table_name: "orders".to_string(),
            constraint_type: "CHECK".to_string(),
            is_deferrable: false,
            initially_deferred: false,
            definition: Some("CHECK (status = 'Active')".to_string()),
            coninhcount: 0,
            is_enforced: true,
            no_inherit: false,
            nulls_not_distinct: false,
            comment: None,
        };
        let script = constraint.get_script();
        assert!(
            script.contains("check (status = 'Active')"),
            "string literal case must be preserved, got: {script}"
        );
    }

    #[test]
    fn test_check_constraint_preserves_multiple_literals() {
        let constraint = TableConstraint {
            catalog: "db".to_string(),
            schema: "public".to_string(),
            name: "chk_multi".to_string(),
            table_name: "t".to_string(),
            constraint_type: "CHECK".to_string(),
            is_deferrable: false,
            initially_deferred: false,
            definition: Some(
                "CHECK (status IN ('Active', 'Inactive', 'PendingReview'))".to_string(),
            ),
            coninhcount: 0,
            is_enforced: true,
            no_inherit: false,
            nulls_not_distinct: false,
            comment: None,
        };
        let script = constraint.get_script();
        assert!(script.contains("'Active'"), "got: {script}");
        assert!(script.contains("'Inactive'"), "got: {script}");
        assert!(script.contains("'PendingReview'"), "got: {script}");
        // keywords outside literals must still be lower-cased
        assert!(script.contains("check (status in ("), "got: {script}");
    }

    #[test]
    fn test_check_constraint_preserves_escaped_quote_in_literal() {
        // SQL-standard '' escape inside the literal
        let constraint = TableConstraint {
            catalog: "db".to_string(),
            schema: "public".to_string(),
            name: "chk_esc".to_string(),
            table_name: "t".to_string(),
            constraint_type: "CHECK".to_string(),
            is_deferrable: false,
            initially_deferred: false,
            definition: Some("CHECK (col <> 'It''s OK')".to_string()),
            coninhcount: 0,
            is_enforced: true,
            no_inherit: false,
            nulls_not_distinct: false,
            comment: None,
        };
        let script = constraint.get_script();
        assert!(
            script.contains("'It''s OK'"),
            "escaped quote must survive, got: {script}"
        );
    }

    #[test]
    fn test_check_constraint_lowercases_keywords_outside_literals() {
        let constraint = TableConstraint {
            catalog: "db".to_string(),
            schema: "public".to_string(),
            name: "chk_kw".to_string(),
            table_name: "t".to_string(),
            constraint_type: "CHECK".to_string(),
            is_deferrable: false,
            initially_deferred: false,
            definition: Some("CHECK (AGE > 0 AND NAME IS NOT NULL)".to_string()),
            coninhcount: 0,
            is_enforced: true,
            no_inherit: false,
            nulls_not_distinct: false,
            comment: None,
        };
        let script = constraint.get_script();
        assert!(
            script.contains("check (age > 0 and name is not null)"),
            "keywords must be lowered, got: {script}"
        );
    }

    #[test]
    fn test_definition_no_literals_fully_lowercased() {
        // When there are no string literals the entire expression is lowered,
        // matching the old behaviour.
        let constraint = TableConstraint {
            catalog: "db".to_string(),
            schema: "public".to_string(),
            name: "chk_num".to_string(),
            table_name: "t".to_string(),
            constraint_type: "CHECK".to_string(),
            is_deferrable: false,
            initially_deferred: false,
            definition: Some("CHECK (AMOUNT >= 0)".to_string()),
            coninhcount: 0,
            is_enforced: true,
            no_inherit: false,
            nulls_not_distinct: false,
            comment: None,
        };
        let script = constraint.get_script();
        assert!(script.contains("check (amount >= 0)"), "got: {script}");
    }

    #[test]
    fn test_lowercase_outside_literals_empty_string() {
        assert_eq!(TableConstraint::lowercase_outside_literals(""), "");
    }

    #[test]
    fn test_lowercase_outside_literals_no_quotes() {
        assert_eq!(
            TableConstraint::lowercase_outside_literals("CHECK (X > 0)"),
            "check (x > 0)"
        );
    }

    #[test]
    fn test_lowercase_outside_literals_preserves_literal() {
        assert_eq!(
            TableConstraint::lowercase_outside_literals("CHECK (status = 'Active')"),
            "check (status = 'Active')"
        );
    }

    #[test]
    fn test_lowercase_outside_literals_preserves_escaped_quotes() {
        assert_eq!(
            TableConstraint::lowercase_outside_literals("CHECK (x = 'It''s A Test')"),
            "check (x = 'It''s A Test')"
        );
    }

    #[test]
    fn test_lowercase_outside_literals_adjacent_literals() {
        assert_eq!(
            TableConstraint::lowercase_outside_literals("'Foo' || 'Bar'"),
            "'Foo' || 'Bar'"
        );
    }

    // --- normalize_definition: array cast distribution equivalence ---

    #[test]
    fn test_normalize_definition_array_level_cast() {
        // Form A from pg_get_constraintdef (created via IN (...))
        let def_a = "CHECK (priority::text = ANY (ARRAY['P1-Critical'::character varying, 'P2-High'::character varying]::text[]))";
        let norm = TableConstraint::normalize_definition(def_a);
        assert_eq!(
            norm,
            "check (priority::text = any (array['P1-Critical'::character varying, 'P2-High'::character varying]))"
        );
    }

    #[test]
    fn test_normalize_definition_element_level_cast() {
        // Form B from pg_get_constraintdef (created via migration applying Form A)
        let def_b = "CHECK (priority::text = ANY (ARRAY['P1-Critical'::character varying::text, 'P2-High'::character varying::text]))";
        let norm = TableConstraint::normalize_definition(def_b);
        assert_eq!(
            norm,
            "check (priority::text = any (array['P1-Critical'::character varying, 'P2-High'::character varying]))"
        );
    }

    #[test]
    fn test_normalize_definition_both_forms_equal() {
        // The two forms that pg_get_constraintdef actually produces
        let form_a = "CHECK (priority::text = ANY (ARRAY['P1-Critical'::character varying, 'P2-High'::character varying, 'P3-Medium'::character varying, 'P4-Low'::character varying, 'P5-Informational'::character varying]::text[]))";
        let form_b = "CHECK (priority::text = ANY (ARRAY['P1-Critical'::character varying::text, 'P2-High'::character varying::text, 'P3-Medium'::character varying::text, 'P4-Low'::character varying::text, 'P5-Informational'::character varying::text]))";

        assert_eq!(
            TableConstraint::normalize_definition(form_a),
            TableConstraint::normalize_definition(form_b),
        );
    }

    #[test]
    fn test_normalize_definition_no_change_for_simple_check() {
        let def = "CHECK (age > 0)";
        let norm = TableConstraint::normalize_definition(def);
        assert_eq!(norm, "check (age > 0)");
    }

    #[test]
    fn test_normalize_definition_preserves_standalone_text_cast() {
        // priority::text should NOT be stripped — it's a standalone cast, not a chain
        let def = "CHECK (priority::text = 'High')";
        let norm = TableConstraint::normalize_definition(def);
        assert_eq!(norm, "check (priority::text = 'High')");
    }

    #[test]
    fn test_normalize_definition_preserves_string_literal_case() {
        let def = "CHECK (status = 'Active')";
        let norm = TableConstraint::normalize_definition(def);
        assert_eq!(norm, "check (status = 'Active')");
    }

    #[test]
    fn test_partial_eq_with_equivalent_array_cast_forms() {
        let constraint_a = TableConstraint {
            catalog: "db".to_string(),
            schema: "test_schema".to_string(),
            name: "chk_priority".to_string(),
            table_name: "t".to_string(),
            constraint_type: "CHECK".to_string(),
            is_deferrable: false,
            initially_deferred: false,
            definition: Some("CHECK (priority::text = ANY (ARRAY['P1'::character varying, 'P2'::character varying]::text[]))".to_string()),
        coninhcount: 0,
            is_enforced: true,
                no_inherit: false,
                nulls_not_distinct: false,
            comment: None,
            };
        let constraint_b = TableConstraint {
            catalog: "db".to_string(),
            schema: "test_schema".to_string(),
            name: "chk_priority".to_string(),
            table_name: "t".to_string(),
            constraint_type: "CHECK".to_string(),
            is_deferrable: false,
            initially_deferred: false,
            definition: Some("CHECK (priority::text = ANY (ARRAY['P1'::character varying::text, 'P2'::character varying::text]))".to_string()),
        coninhcount: 0,
            is_enforced: true,
                no_inherit: false,
                nulls_not_distinct: false,
            comment: None,
            };
        assert_eq!(constraint_a, constraint_b);
    }

    #[test]
    fn test_hasher_with_equivalent_array_cast_forms() {
        let constraint_a = TableConstraint {
            catalog: "db".to_string(),
            schema: "test_schema".to_string(),
            name: "chk_priority".to_string(),
            table_name: "t".to_string(),
            constraint_type: "CHECK".to_string(),
            is_deferrable: false,
            initially_deferred: false,
            definition: Some("CHECK (priority::text = ANY (ARRAY['P1'::character varying, 'P2'::character varying]::text[]))".to_string()),
        coninhcount: 0,
            is_enforced: true,
                no_inherit: false,
                nulls_not_distinct: false,
            comment: None,
            };
        let constraint_b = TableConstraint {
            catalog: "db".to_string(),
            schema: "test_schema".to_string(),
            name: "chk_priority".to_string(),
            table_name: "t".to_string(),
            constraint_type: "CHECK".to_string(),
            is_deferrable: false,
            initially_deferred: false,
            definition: Some("CHECK (priority::text = ANY (ARRAY['P1'::character varying::text, 'P2'::character varying::text]))".to_string()),
        coninhcount: 0,
            is_enforced: true,
                no_inherit: false,
                nulls_not_distinct: false,
            comment: None,
            };

        let mut hasher_a = Sha256::new();
        let mut hasher_b = Sha256::new();
        constraint_a.add_to_hasher(&mut hasher_a);
        constraint_b.add_to_hasher(&mut hasher_b);
        assert_eq!(
            format!("{:x}", hasher_a.finalize()),
            format!("{:x}", hasher_b.finalize()),
        );
    }

    // --- Unicode correctness ---

    #[test]
    fn test_lowercase_outside_literals_unicode_outside() {
        // Non-ASCII identifier outside a literal must be lowercased properly.
        assert_eq!(
            TableConstraint::lowercase_outside_literals("CHECK (MÜLLER > 0)"),
            "check (müller > 0)"
        );
    }

    #[test]
    fn test_lowercase_outside_literals_unicode_inside_literal() {
        // Non-ASCII characters inside a literal must be preserved verbatim.
        assert_eq!(
            TableConstraint::lowercase_outside_literals("CHECK (name = 'Ñoño')"),
            "check (name = 'Ñoño')"
        );
    }

    #[test]
    fn test_lowercase_outside_literals_multibyte_mixed() {
        // Mix of multi-byte chars inside and outside literals.
        assert_eq!(
            TableConstraint::lowercase_outside_literals("CHECK (ГОРОД = 'Москва')"),
            "check (город = 'Москва')"
        );
    }

    #[test]
    fn test_lowercase_outside_literals_emoji_in_literal() {
        // 4-byte UTF-8 sequences (emoji) inside a literal must survive.
        assert_eq!(
            TableConstraint::lowercase_outside_literals("CHECK (label = '🚀Launch')"),
            "check (label = '🚀Launch')"
        );
    }

    // --- normalize_definition must not alter replacement patterns inside literals ---

    #[test]
    fn test_normalize_definition_preserves_text_array_cast_inside_literal() {
        // A literal containing "]::text[]" must NOT be rewritten.
        let def = "CHECK (note = ']::text[]')";
        let norm = TableConstraint::normalize_definition(def);
        assert_eq!(norm, "check (note = ']::text[]')");
    }

    #[test]
    fn test_normalize_definition_preserves_varying_text_cast_inside_literal() {
        // A literal containing "::character varying::text" must NOT be rewritten.
        let def = "CHECK (note = '::character varying::text')";
        let norm = TableConstraint::normalize_definition(def);
        assert_eq!(norm, "check (note = '::character varying::text')");
    }

    #[test]
    fn test_normalize_definition_mixed_literal_and_outside_casts() {
        // Cast outside the literal is normalized; identical text inside is preserved.
        let def = "CHECK (x::character varying::text = ']::text[]' AND y::character varying::text = 'ok')";
        let norm = TableConstraint::normalize_definition(def);
        assert_eq!(
            norm,
            "check (x::character varying = ']::text[]' and y::character varying = 'ok')"
        );
    }

    // --- coninhcount i32 compatibility (PG14 INT4 / PG16+ INT2) ---

    #[test]
    fn test_coninhcount_accepts_i16_range_values() {
        // PG16+ returns INT2 values, which fit in i32
        let constraint = TableConstraint {
            catalog: "db".to_string(),
            schema: "public".to_string(),
            name: "pk_test".to_string(),
            table_name: "test".to_string(),
            constraint_type: "PRIMARY KEY".to_string(),
            is_deferrable: false,
            initially_deferred: false,
            definition: None,
            coninhcount: 0,
            is_enforced: true,
            no_inherit: false,
            nulls_not_distinct: false,
            comment: None,
        };
        assert_eq!(constraint.coninhcount, 0);

        let mut inherited = constraint.clone();
        inherited.coninhcount = i16::MAX as i32;
        assert_eq!(inherited.coninhcount, 32767);
    }

    #[test]
    fn test_coninhcount_accepts_i32_range_values() {
        // PG14 returns INT4 values, which require i32
        let mut constraint = create_primary_key_constraint();
        constraint.coninhcount = i32::MAX;
        assert_eq!(constraint.coninhcount, i32::MAX);

        constraint.coninhcount = 100_000; // exceeds i16::MAX
        assert_eq!(constraint.coninhcount, 100_000);
    }

    #[test]
    fn test_coninhcount_serde_roundtrip_zero() {
        let constraint = create_primary_key_constraint();
        let json = serde_json::to_string(&constraint).unwrap();
        let deserialized: TableConstraint = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.coninhcount, 0);
    }

    #[test]
    fn test_coninhcount_serde_roundtrip_large_value() {
        let mut constraint = create_primary_key_constraint();
        constraint.coninhcount = 100_000;
        let json = serde_json::to_string(&constraint).unwrap();
        let deserialized: TableConstraint = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.coninhcount, 100_000);
    }

    #[test]
    fn test_coninhcount_serde_default_when_missing() {
        // Older dumps may not include the field; #[serde(default)] should yield 0
        let json = r#"{
            "catalog": "db",
            "schema": "public",
            "name": "pk",
            "table_name": "t",
            "constraint_type": "PRIMARY KEY",
            "is_deferrable": false,
            "initially_deferred": false,
            "definition": null
        }"#;
        let deserialized: TableConstraint = serde_json::from_str(json).unwrap();
        assert_eq!(deserialized.coninhcount, 0);
    }

    #[test]
    fn test_coninhcount_not_included_in_equality() {
        // coninhcount is intentionally excluded from PartialEq
        let mut a = create_primary_key_constraint();
        let mut b = create_primary_key_constraint();
        a.coninhcount = 0;
        b.coninhcount = 5;
        assert_eq!(a, b);
    }

    #[test]
    fn test_coninhcount_not_included_in_hash() {
        // coninhcount is intentionally excluded from the SHA256 hash
        let mut a = create_primary_key_constraint();
        let mut b = create_primary_key_constraint();
        a.coninhcount = 0;
        b.coninhcount = 42;

        let mut ha = Sha256::new();
        let mut hb = Sha256::new();
        a.add_to_hasher(&mut ha);
        b.add_to_hasher(&mut hb);
        assert_eq!(
            format!("{:x}", ha.finalize()),
            format!("{:x}", hb.finalize()),
        );
    }

    // ---- PG18: is_enforced tests ----

    #[test]
    fn test_get_script_not_enforced_check() {
        let constraint = TableConstraint {
            catalog: "test".to_string(),
            schema: "public".to_string(),
            name: "chk_status".to_string(),
            table_name: "orders".to_string(),
            constraint_type: "CHECK".to_string(),
            is_deferrable: false,
            initially_deferred: false,
            definition: Some("CHECK (status <> '')".to_string()),
            coninhcount: 0,
            is_enforced: false,
            no_inherit: false,
            nulls_not_distinct: false,
            comment: None,
        };
        let script = constraint.get_script();
        assert!(
            script.contains("not enforced"),
            "expected 'not enforced' in script: {script}"
        );
    }

    #[test]
    fn test_get_script_not_enforced_foreign_key() {
        let constraint = TableConstraint {
            catalog: "test".to_string(),
            schema: "public".to_string(),
            name: "fk_order_user".to_string(),
            table_name: "orders".to_string(),
            constraint_type: "FOREIGN KEY".to_string(),
            is_deferrable: false,
            initially_deferred: false,
            definition: Some("FOREIGN KEY (user_id) REFERENCES public.users(id)".to_string()),
            coninhcount: 0,
            is_enforced: false,
            no_inherit: false,
            nulls_not_distinct: false,
            comment: None,
        };
        let script = constraint.get_script();
        assert!(
            script.contains("not enforced"),
            "expected 'not enforced' in FK script: {script}"
        );
    }

    #[test]
    fn test_get_script_enforced_omits_keyword() {
        let constraint = TableConstraint {
            catalog: "test".to_string(),
            schema: "public".to_string(),
            name: "chk_positive".to_string(),
            table_name: "items".to_string(),
            constraint_type: "CHECK".to_string(),
            is_deferrable: false,
            initially_deferred: false,
            definition: Some("CHECK (qty > 0)".to_string()),
            coninhcount: 0,
            is_enforced: true,
            no_inherit: false,
            nulls_not_distinct: false,
            comment: None,
        };
        let script = constraint.get_script();
        assert!(
            !script.contains("not enforced") && !script.contains("enforced"),
            "enforced=true should not emit any enforced keyword: {script}"
        );
    }

    #[test]
    fn test_partial_eq_different_is_enforced() {
        let mut a = create_check_constraint();
        a.is_enforced = true;
        let mut b = a.clone();
        b.is_enforced = false;
        assert_ne!(
            a, b,
            "constraints with different is_enforced should not be equal"
        );
    }

    #[test]
    fn test_add_to_hasher_is_enforced_affects_hash() {
        let mut a = create_check_constraint();
        a.is_enforced = true;
        let mut b = a.clone();
        b.is_enforced = false;

        let mut ha = Sha256::new();
        let mut hb = Sha256::new();
        a.add_to_hasher(&mut ha);
        b.add_to_hasher(&mut hb);
        assert_ne!(
            format!("{:x}", ha.finalize()),
            format!("{:x}", hb.finalize()),
            "is_enforced should affect the hash"
        );
    }

    #[test]
    fn test_can_be_altered_to_check_enforced_change() {
        let a = TableConstraint {
            catalog: "test".to_string(),
            schema: "public".to_string(),
            name: "chk_val".to_string(),
            table_name: "t".to_string(),
            constraint_type: "CHECK".to_string(),
            is_deferrable: false,
            initially_deferred: false,
            definition: Some("CHECK (val > 0)".to_string()),
            coninhcount: 0,
            is_enforced: true,
            no_inherit: false,
            nulls_not_distinct: false,
            comment: None,
        };
        let mut b = a.clone();
        b.is_enforced = false;
        assert!(
            a.can_be_altered_to(&b),
            "CHECK constraint should be alterable when only is_enforced differs"
        );
    }

    #[test]
    fn test_can_be_altered_to_fk_enforced_change() {
        let a = TableConstraint {
            catalog: "test".to_string(),
            schema: "public".to_string(),
            name: "fk_ref".to_string(),
            table_name: "t".to_string(),
            constraint_type: "FOREIGN KEY".to_string(),
            is_deferrable: false,
            initially_deferred: false,
            definition: Some("FOREIGN KEY (x) REFERENCES public.y(id)".to_string()),
            coninhcount: 0,
            is_enforced: true,
            no_inherit: false,
            nulls_not_distinct: false,
            comment: None,
        };
        let mut b = a.clone();
        b.is_enforced = false;
        assert!(
            a.can_be_altered_to(&b),
            "FK constraint should be alterable when only is_enforced differs"
        );
    }

    #[test]
    fn test_get_alter_script_fk_enforced_to_not_enforced() {
        let old = TableConstraint {
            catalog: "test".to_string(),
            schema: "public".to_string(),
            name: "fk_ref".to_string(),
            table_name: "t".to_string(),
            constraint_type: "FOREIGN KEY".to_string(),
            is_deferrable: false,
            initially_deferred: false,
            definition: Some("FOREIGN KEY (x) REFERENCES public.y(id)".to_string()),
            coninhcount: 0,
            is_enforced: true,
            no_inherit: false,
            nulls_not_distinct: false,
            comment: None,
        };
        let mut new_c = old.clone();
        new_c.is_enforced = false;

        let script = old
            .get_alter_script(&new_c)
            .expect("should produce alter script");
        assert!(
            script.contains("alter table public.t alter constraint fk_ref not enforced"),
            "expected NOT ENFORCED alter: {script}"
        );
    }

    #[test]
    fn test_get_alter_script_fk_not_enforced_to_enforced() {
        let old = TableConstraint {
            catalog: "test".to_string(),
            schema: "public".to_string(),
            name: "fk_ref".to_string(),
            table_name: "t".to_string(),
            constraint_type: "FOREIGN KEY".to_string(),
            is_deferrable: false,
            initially_deferred: false,
            definition: Some("FOREIGN KEY (x) REFERENCES public.y(id)".to_string()),
            coninhcount: 0,
            is_enforced: false,
            no_inherit: false,
            nulls_not_distinct: false,
            comment: None,
        };
        let mut new_c = old.clone();
        new_c.is_enforced = true;

        let script = old
            .get_alter_script(&new_c)
            .expect("should produce alter script");
        assert!(
            script.contains("alter table public.t alter constraint fk_ref enforced"),
            "expected ENFORCED alter: {script}"
        );
        assert!(
            !script.contains("not enforced"),
            "should not contain 'not enforced': {script}"
        );
    }

    #[test]
    fn test_get_alter_script_check_enforced_change() {
        let old = TableConstraint {
            catalog: "test".to_string(),
            schema: "public".to_string(),
            name: "chk_val".to_string(),
            table_name: "t".to_string(),
            constraint_type: "CHECK".to_string(),
            is_deferrable: false,
            initially_deferred: false,
            definition: Some("CHECK (val > 0)".to_string()),
            coninhcount: 0,
            is_enforced: true,
            no_inherit: false,
            nulls_not_distinct: false,
            comment: None,
        };
        let mut new_c = old.clone();
        new_c.is_enforced = false;

        let script = old
            .get_alter_script(&new_c)
            .expect("should produce alter script");
        assert!(
            script.contains("alter table public.t alter constraint chk_val not enforced"),
            "expected NOT ENFORCED for CHECK alter: {script}"
        );
    }

    #[test]
    fn test_get_alter_script_check_not_enforced_to_enforced() {
        let old = TableConstraint {
            catalog: "test".to_string(),
            schema: "public".to_string(),
            name: "chk_val".to_string(),
            table_name: "t".to_string(),
            constraint_type: "CHECK".to_string(),
            is_deferrable: false,
            initially_deferred: false,
            definition: Some("CHECK (val > 0)".to_string()),
            coninhcount: 0,
            is_enforced: false,
            no_inherit: false,
            nulls_not_distinct: false,
            comment: None,
        };
        let mut new_c = old.clone();
        new_c.is_enforced = true;

        let script = old
            .get_alter_script(&new_c)
            .expect("should produce alter script");
        assert!(
            script.contains("alter table public.t alter constraint chk_val enforced"),
            "expected ENFORCED for CHECK alter: {script}"
        );
        assert!(
            !script.contains("not enforced"),
            "should not contain 'not enforced': {script}"
        );
    }

    #[test]
    fn test_get_alter_script_fk_deferrable_and_enforced_change() {
        let old = TableConstraint {
            catalog: "test".to_string(),
            schema: "public".to_string(),
            name: "fk_ref".to_string(),
            table_name: "t".to_string(),
            constraint_type: "FOREIGN KEY".to_string(),
            is_deferrable: false,
            initially_deferred: false,
            definition: Some("FOREIGN KEY (x) REFERENCES public.y(id)".to_string()),
            coninhcount: 0,
            is_enforced: true,
            no_inherit: false,
            nulls_not_distinct: false,
            comment: None,
        };
        let mut new_c = old.clone();
        new_c.is_deferrable = true;
        new_c.initially_deferred = true;
        new_c.is_enforced = false;

        let script = old
            .get_alter_script(&new_c)
            .expect("should produce alter script");
        assert!(
            script.contains("deferrable initially deferred"),
            "expected deferrable change: {script}"
        );
        assert!(
            script.contains("not enforced"),
            "expected enforced change: {script}"
        );
    }

    #[test]
    fn test_serde_default_enforced() {
        let json = r#"{"catalog":"db","schema":"s","name":"c","table_name":"t","constraint_type":"CHECK","is_deferrable":false,"initially_deferred":false,"definition":null}"#;
        let c: TableConstraint = serde_json::from_str(json).unwrap();
        assert!(c.is_enforced, "missing is_enforced should default to true");
    }
}
