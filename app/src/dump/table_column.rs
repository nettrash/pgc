use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

// This is an information about a PostgreSQL table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableColumn {
    pub catalog: String,                       // Catalog name
    pub schema: String,                        // Schema name
    pub table: String,                         // Table name
    pub name: String,                          // Column name
    pub ordinal_position: i32,                 // Ordinal position of the column
    pub column_default: Option<String>,        // Default value of the column
    pub is_nullable: bool,                     // Whether the column is nullable
    pub data_type: String,                     // Data type of the column
    pub character_maximum_length: Option<i32>, // Maximum length for character types
    pub character_octet_length: Option<i32>,   // Octet length for character types
    pub numeric_precision: Option<i32>,        // Numeric precision
    pub numeric_precision_radix: Option<i32>,  // Numeric precision radix
    pub numeric_scale: Option<i32>,            // Numeric scale
    pub datetime_precision: Option<i32>,       // Datetime precision
    pub interval_type: Option<String>,         // Interval type
    pub interval_precision: Option<i32>,       // Interval precision
    pub character_set_catalog: Option<String>, // Character set catalog
    pub character_set_schema: Option<String>,  // Character set schema
    pub character_set_name: Option<String>,    // Character set name
    pub collation_catalog: Option<String>,     // Collation catalog
    pub collation_schema: Option<String>,      // Collation schema
    pub collation_name: Option<String>,        // Collation name
    pub domain_catalog: Option<String>,        // Domain catalog
    pub domain_schema: Option<String>,         // Domain schema
    pub domain_name: Option<String>,           // Domain name
    pub udt_catalog: Option<String>,           // UDT catalog
    pub udt_schema: Option<String>,            // UDT schema
    pub udt_name: Option<String>,              // UDT name
    pub scope_catalog: Option<String>,         // Scope catalog
    pub scope_schema: Option<String>,          // Scope schema
    pub scope_name: Option<String>,            // Scope name
    pub maximum_cardinality: Option<i32>,      // Maximum cardinality
    pub dtd_identifier: Option<String>,        // DTD identifier
    pub is_self_referencing: bool,             // Whether the column is self-referencing
    pub is_identity: bool,                     // Whether the column is an identity column
    pub identity_generation: Option<String>,   // Identity generation method
    pub identity_start: Option<String>,        // Identity start value
    pub identity_increment: Option<String>,    // Identity increment value
    pub identity_maximum: Option<String>,      // Identity maximum value
    pub identity_minimum: Option<String>,      // Identity minimum value
    pub identity_cycle: bool,                  // Whether the identity column cycles
    pub is_generated: String,                  // Whether the column is generated
    pub generation_expression: Option<String>, // Generation expression for the column
    pub is_updatable: bool,                    // Whether the column is updatable
    pub related_views: Option<Vec<String>>,    // Related views (optional)
}

impl TableColumn {
    /// Render the type clause for alter statements (data type, length, collation, interval)
    fn render_type_clause(&self) -> String {
        let mut clause = String::new();
        clause.push_str(&self.data_type);

        let data_type_lower = self.data_type.to_lowercase();

        if let Some(length) = self.character_maximum_length {
            if data_type_lower.contains("char") {
                clause.push_str(&format!("({length})"));
            }
        } else if data_type_lower.contains("numeric") || data_type_lower.contains("decimal") {
            if let (Some(precision), Some(scale)) = (self.numeric_precision, self.numeric_scale) {
                clause.push_str(&format!("({precision}, {scale})"));
            } else if let Some(precision) = self.numeric_precision {
                clause.push_str(&format!("({precision})"));
            }
        }

        if data_type_lower.contains("interval")
            && let Some(interval_type) = &self.interval_type
            && !interval_type.is_empty()
        {
            clause.push(' ');
            clause.push_str(interval_type);
        }

        if let Some(collation) = &self.collation_name
            && !collation.is_empty()
        {
            clause.push_str(&format!(" collate \"{collation}\""));
        }

        clause
    }

    fn type_clause_differs(&self, other: &TableColumn) -> bool {
        self.render_type_clause() != other.render_type_clause()
    }

    fn normalized_identity_generation(value: Option<&String>) -> String {
        value
            .and_then(|v| {
                let trimmed = v.trim();
                if trimmed.is_empty() {
                    None
                } else {
                    Some(trimmed.to_uppercase())
                }
            })
            .unwrap_or_else(|| "BY DEFAULT".to_string())
    }

    fn build_identity_add_statement(&self, existing: &TableColumn) -> String {
        let generation = Self::normalized_identity_generation(self.identity_generation.as_ref());
        let mut statement = format!(
            "alter table \"{}\".\"{}\" alter column \"{}\" add generated {} as identity",
            self.schema, self.table, self.name, generation
        );

        let mut options = Vec::new();
        if let Some(start) = &self.identity_start
            && Some(start) != existing.identity_start.as_ref()
        {
            options.push(format!("start with {start}"));
        }
        if let Some(increment) = &self.identity_increment
            && Some(increment) != existing.identity_increment.as_ref()
        {
            options.push(format!("increment by {increment}"));
        }
        if let Some(min_val) = &self.identity_minimum {
            if Some(min_val) != existing.identity_minimum.as_ref() {
                options.push(format!("minvalue {min_val}"));
            }
        } else if existing.identity_minimum.is_some() {
            options.push("no minvalue".to_string());
        }
        if let Some(max_val) = &self.identity_maximum {
            if Some(max_val) != existing.identity_maximum.as_ref() {
                options.push(format!("maxvalue {max_val}"));
            }
        } else if existing.identity_maximum.is_some() {
            options.push("no maxvalue".to_string());
        }
        if self.identity_cycle != existing.identity_cycle {
            options.push(if self.identity_cycle {
                "cycle".to_string()
            } else {
                "no cycle".to_string()
            });
        }

        if !options.is_empty() {
            let opts = options
                .iter()
                .map(|opt| opt.to_uppercase())
                .collect::<Vec<_>>()
                .join(" ");
            statement.push_str(&format!(" ({opts})"));
        }

        statement.push_str(";\n");
        statement
    }

    fn build_identity_update_statements(
        &self,
        existing: &TableColumn,
        statements: &mut Vec<String>,
    ) {
        let new_generation =
            Self::normalized_identity_generation(self.identity_generation.as_ref());
        let old_generation =
            Self::normalized_identity_generation(existing.identity_generation.as_ref());
        if new_generation != old_generation {
            statements.push(format!(
                "alter table \"{}\".\"{}\" alter column \"{}\" set generated {};\n",
                self.schema, self.table, self.name, new_generation
            ));
        }

        let mut options = Vec::new();
        if self.identity_start != existing.identity_start
            && let Some(start) = &self.identity_start
        {
            options.push(format!("start with {start}"));
        }
        if self.identity_increment != existing.identity_increment
            && let Some(increment) = &self.identity_increment
        {
            options.push(format!("increment by {increment}"));
        }
        if self.identity_minimum != existing.identity_minimum {
            match &self.identity_minimum {
                Some(min_val) => options.push(format!("minvalue {min_val}")),
                None => options.push("no minvalue".to_string()),
            }
        }
        if self.identity_maximum != existing.identity_maximum {
            match &self.identity_maximum {
                Some(max_val) => options.push(format!("maxvalue {max_val}")),
                None => options.push("no maxvalue".to_string()),
            }
        }
        if self.identity_cycle != existing.identity_cycle {
            options.push(if self.identity_cycle {
                "cycle".to_string()
            } else {
                "no cycle".to_string()
            });
        }

        for option in options {
            statements.push(format!(
                "alter table \"{}\".\"{}\" alter column \"{}\" set {};\n",
                self.schema,
                self.table,
                self.name,
                option.to_uppercase()
            ));
        }
    }

    /// Hash
    pub fn add_to_hasher(&self, hasher: &mut Sha256) {
        hasher.update(self.name.as_bytes());
        hasher.update(self.data_type.as_bytes());
        hasher.update(self.is_nullable.to_string().as_bytes());

        if let Some(default) = &self.column_default {
            hasher.update(default.as_bytes());
        }
        if let Some(len) = self.character_maximum_length {
            hasher.update(len.to_string().as_bytes());
        }
        if let Some(precision) = self.numeric_precision {
            hasher.update(precision.to_string().as_bytes());
        }
        if let Some(scale) = self.numeric_scale {
            hasher.update(scale.to_string().as_bytes());
        }
        hasher.update(self.is_identity.to_string().as_bytes());
        if let Some(generation) = &self.identity_generation {
            hasher.update(generation.as_bytes());
        }
        if let Some(expr) = &self.generation_expression {
            hasher.update(expr.as_bytes());
        }
        // skip catalog/charset/related_views and other descriptive-only fields
    }

    /// Returns a string representation of the column
    pub fn get_script(&self) -> String {
        let mut script = String::new();
        // Name
        script.push_str(&format!("\"{}\" ", self.name));

        // Data type with length/precision/scale if applicable
        script.push_str(&self.data_type);
        // Character length
        if let Some(length) = self.character_maximum_length {
            // Only append for character types
            if self.data_type.to_lowercase().contains("char") {
                script.push_str(&format!("({length})"));
            }
        } else if let (Some(precision), Some(scale)) = (self.numeric_precision, self.numeric_scale)
        {
            // Numeric(precision, scale)
            if self.data_type.to_lowercase().contains("numeric")
                || self.data_type.to_lowercase().contains("decimal")
            {
                script.push_str(&format!("({precision}, {scale})"));
            }
        } else if let Some(precision) = self.numeric_precision {
            // Numeric(precision)
            if self.data_type.to_lowercase().contains("numeric")
                || self.data_type.to_lowercase().contains("decimal")
            {
                script.push_str(&format!("({precision})"));
            }
        }
        // Datetime precision
        //        if let Some(dt_precision) = self.datetime_precision {
        //            if self.data_type.to_lowercase().contains("timestamp") || self.data_type.to_lowercase().contains("time") {
        //                script.push_str(&format!("({})", dt_precision));
        //            }
        //        }
        // Interval type
        if let Some(interval_type) = &self.interval_type
            && self.data_type.to_lowercase().contains("interval")
        {
            script.push_str(&format!(" {interval_type}"));
        }

        // Collation
        if let Some(collation) = &self.collation_name
            && !collation.is_empty()
        {
            script.push_str(&format!(" collate \"{collation}\""));
        }

        // Identity
        if self.is_identity {
            script.push_str(" generated ");
            if let Some(ref generation) = self.identity_generation {
                script.push_str(&generation.to_uppercase());
            } else {
                script.push_str("by default");
            }
            script.push_str(" as identity");
            // Identity options
            let mut opts = Vec::new();
            if let Some(ref v) = self.identity_start {
                opts.push(format!("start with {v}"));
            }
            if let Some(ref v) = self.identity_increment {
                opts.push(format!("increment by {v}"));
            }
            if let Some(ref v) = self.identity_minimum {
                opts.push(format!("minvalue {v}"));
            }
            if let Some(ref v) = self.identity_maximum {
                opts.push(format!("maxvalue {v}"));
            }
            if self.identity_cycle {
                opts.push("cycle".to_string());
            }
            if !opts.is_empty() {
                script.push_str(&format!(" ({})", opts.join(" ")));
            }
        }

        // Generated always as (expression)
        if self.is_generated.to_lowercase() == "always"
            && let Some(expr) = &self.generation_expression
        {
            script.push_str(&format!(" generated always as ({expr}) stored "));
        }

        // Default
        if let Some(default) = &self.column_default {
            script.push_str(&format!(" default {default}"));
        }

        // Nullability
        if !self.is_nullable {
            script.push_str(" not null");
        }

        script.trim_end().to_string()
    }

    pub fn get_alter_script(&self, existing: &TableColumn) -> Option<String> {
        let mut statements = Vec::new();

        if self.type_clause_differs(existing) {
            statements.push(format!(
                "alter table \"{}\".\"{}\" alter column \"{}\" type {};\n",
                self.schema,
                self.table,
                self.name,
                self.render_type_clause()
            ));
        }

        if self.column_default != existing.column_default {
            match &self.column_default {
                Some(default) => statements.push(format!(
                    "alter table \"{}\".\"{}\" alter column \"{}\" set default {};\n",
                    self.schema, self.table, self.name, default
                )),
                None => statements.push(format!(
                    "alter table \"{}\".\"{}\" alter column \"{}\" drop default;\n",
                    self.schema, self.table, self.name
                )),
            }
        }

        if self.is_nullable != existing.is_nullable {
            if self.is_nullable {
                statements.push(format!(
                    "alter table \"{}\".\"{}\" alter column \"{}\" drop not null;\n",
                    self.schema, self.table, self.name
                ));
            } else {
                statements.push(format!(
                    "alter table \"{}\".\"{}\" alter column \"{}\" set not null;\n",
                    self.schema, self.table, self.name
                ));
            }
        }

        if self.is_identity != existing.is_identity {
            if self.is_identity {
                statements.push(self.build_identity_add_statement(existing));
            } else {
                statements.push(format!(
                    "alter table \"{}\".\"{}\" alter column \"{}\" drop identity if exists;\n",
                    self.schema, self.table, self.name
                ));
            }
        } else if self.is_identity {
            self.build_identity_update_statements(existing, &mut statements);
        }

        if statements.is_empty() {
            None
        } else {
            Some(statements.join(""))
        }
    }

    pub fn get_add_script(&self) -> String {
        let mut statement = format!(
            "alter table \"{}\".\"{}\" add column \"{}\" {}",
            self.schema,
            self.table,
            self.name,
            self.render_type_clause()
        );

        if self.is_identity {
            let generation =
                Self::normalized_identity_generation(self.identity_generation.as_ref());
            statement.push_str(" generated ");
            statement.push_str(&generation);
            statement.push_str(" as identity");

            let mut options = Vec::new();
            if let Some(start) = &self.identity_start {
                options.push(format!("start with {start}"));
            }
            if let Some(increment) = &self.identity_increment {
                options.push(format!("increment by {increment}"));
            }
            if let Some(min_val) = &self.identity_minimum {
                options.push(format!("minvalue {min_val}"));
            }
            if let Some(max_val) = &self.identity_maximum {
                options.push(format!("maxvalue {max_val}"));
            }
            if self.identity_cycle {
                options.push("cycle".to_string());
            }

            if !options.is_empty() {
                statement.push_str(" (");
                statement.push_str(
                    &options
                        .iter()
                        .map(|opt| opt.to_uppercase())
                        .collect::<Vec<_>>()
                        .join(" "),
                );
                statement.push(')');
            }
        }

        if self.is_generated.to_lowercase() == "always"
            && let Some(expr) = &self.generation_expression
        {
            statement.push_str(&format!(" generated always as ({expr}) stored"));
        }

        if let Some(default) = &self.column_default {
            statement.push_str(&format!(" default {default}"));
        }

        if !self.is_nullable {
            statement.push_str(" not null");
        }

        statement.push_str(";\n");
        statement
    }

    pub fn get_drop_script(&self) -> String {
        format!(
            "alter table \"{}\".\"{}\" drop column \"{}\";\n",
            self.schema, self.table, self.name
        )
    }
}

impl PartialEq for TableColumn {
    fn eq(&self, other: &Self) -> bool {
        self.schema == other.schema
            && self.table == other.table
            && self.name == other.name
            && self.ordinal_position == other.ordinal_position
            && self.column_default == other.column_default
            && self.is_nullable == other.is_nullable
            && self.data_type == other.data_type
            && self.character_maximum_length == other.character_maximum_length
            && self.character_octet_length == other.character_octet_length
            && self.numeric_precision == other.numeric_precision
            && self.numeric_precision_radix == other.numeric_precision_radix
            && self.numeric_scale == other.numeric_scale
            && self.datetime_precision == other.datetime_precision
            && self.interval_type == other.interval_type
            && self.interval_precision == other.interval_precision
            && self.character_set_catalog == other.character_set_catalog
            && self.character_set_schema == other.character_set_schema
            && self.character_set_name == other.character_set_name
            && self.collation_catalog == other.collation_catalog
            && self.collation_schema == other.collation_schema
            && self.collation_name == other.collation_name
            && self.domain_catalog == other.domain_catalog
            && self.domain_schema == other.domain_schema
            && self.domain_name == other.domain_name
            && self.udt_catalog == other.udt_catalog
            && self.udt_schema == other.udt_schema
            && self.udt_name == other.udt_name
            && self.scope_catalog == other.scope_catalog
            && self.scope_schema == other.scope_schema
            && self.scope_name == other.scope_name
            && self.maximum_cardinality == other.maximum_cardinality
            && self.dtd_identifier == other.dtd_identifier
            && self.is_self_referencing == other.is_self_referencing
            && self.is_identity == other.is_identity
            && self.identity_generation == other.identity_generation
            && self.identity_start == other.identity_start
            && self.identity_increment == other.identity_increment
            && self.identity_maximum == other.identity_maximum
            && self.identity_minimum == other.identity_minimum
            && self.identity_cycle == other.identity_cycle
            // is_generated is a string, so we compare it directly.
            // If it contains "ALWAYS" or "BY DEFAULT", we consider them equal.
            // This is a workaround for the fact that
            // PostgreSQL uses different strings for generated columns.
            && (self.is_generated.to_uppercase() == other.is_generated.to_uppercase()
                || self.is_generated.to_uppercase().contains("ALWAYS")
                || self.is_generated.to_uppercase().contains("BY DEFAULT"))
            && self.generation_expression == other.generation_expression
            && self.is_updatable == other.is_updatable
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};

    // Helper function to create a basic TableColumn for testing
    fn create_test_column() -> TableColumn {
        TableColumn {
            catalog: "test_catalog".to_string(),
            schema: "public".to_string(),
            table: "test_table".to_string(),
            name: "test_column".to_string(),
            ordinal_position: 1,
            column_default: None,
            is_nullable: true,
            data_type: "varchar".to_string(),
            character_maximum_length: Some(255),
            character_octet_length: Some(1020),
            numeric_precision: None,
            numeric_precision_radix: None,
            numeric_scale: None,
            datetime_precision: None,
            interval_type: None,
            interval_precision: None,
            character_set_catalog: None,
            character_set_schema: None,
            character_set_name: None,
            collation_catalog: None,
            collation_schema: None,
            collation_name: None,
            domain_catalog: None,
            domain_schema: None,
            domain_name: None,
            udt_catalog: None,
            udt_schema: None,
            udt_name: None,
            scope_catalog: None,
            scope_schema: None,
            scope_name: None,
            maximum_cardinality: None,
            dtd_identifier: None,
            is_self_referencing: false,
            is_identity: false,
            identity_generation: None,
            identity_start: None,
            identity_increment: None,
            identity_maximum: None,
            identity_minimum: None,
            identity_cycle: false,
            is_generated: "NEVER".to_string(),
            generation_expression: None,
            is_updatable: true,
            related_views: None,
        }
    }

    #[test]
    fn test_table_column_creation() {
        let column = create_test_column();
        assert_eq!(column.catalog, "test_catalog");
        assert_eq!(column.schema, "public");
        assert_eq!(column.table, "test_table");
        assert_eq!(column.name, "test_column");
        assert_eq!(column.ordinal_position, 1);
        assert!(column.is_nullable);
        assert_eq!(column.data_type, "varchar");
    }

    #[test]
    fn test_table_column_clone() {
        let column = create_test_column();
        let cloned = column.clone();
        assert_eq!(column, cloned);
    }

    #[test]
    fn test_table_column_debug_format() {
        let column = create_test_column();
        let debug_str = format!("{column:?}");
        assert!(debug_str.contains("TableColumn"));
        assert!(debug_str.contains("test_column"));
    }

    #[test]
    fn test_add_to_hasher() {
        let column = create_test_column();
        let mut hasher = Sha256::new();
        column.add_to_hasher(&mut hasher);
        let hash = hasher.finalize();
        assert_eq!(hash.len(), 32); // SHA256 produces 32-byte hash
    }

    #[test]
    fn test_add_to_hasher_consistency() {
        let column = create_test_column();

        let mut hasher1 = Sha256::new();
        column.add_to_hasher(&mut hasher1);
        let hash1 = hasher1.finalize();

        let mut hasher2 = Sha256::new();
        column.add_to_hasher(&mut hasher2);
        let hash2 = hasher2.finalize();

        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_add_to_hasher_different_for_different_content() {
        let column1 = create_test_column();
        let mut column2 = create_test_column();
        column2.name = "different_column".to_string();

        let mut hasher1 = Sha256::new();
        column1.add_to_hasher(&mut hasher1);
        let hash1 = hasher1.finalize();

        let mut hasher2 = Sha256::new();
        column2.add_to_hasher(&mut hasher2);
        let hash2 = hasher2.finalize();

        assert_ne!(hash1, hash2);
    }

    #[test]
    fn test_add_to_hasher_ignores_catalog() {
        let column1 = create_test_column();
        let mut column2 = create_test_column();
        column2.catalog = "different_catalog".to_string();

        let mut hasher1 = Sha256::new();
        column1.add_to_hasher(&mut hasher1);
        let hash1 = hasher1.finalize();

        let mut hasher2 = Sha256::new();
        column2.add_to_hasher(&mut hasher2);
        let hash2 = hasher2.finalize();

        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_add_to_hasher_includes_all_fields() {
        let mut column = create_test_column();
        column.column_default = Some("'default_value'".to_string());
        column.numeric_precision = Some(10);
        column.numeric_scale = Some(2);
        column.collation_name = Some("en_US.UTF-8".to_string());
        column.is_identity = true;
        column.identity_generation = Some("BY DEFAULT".to_string());
        column.is_generated = "ALWAYS".to_string();
        column.generation_expression = Some("(id * 2)".to_string());

        let mut hasher = Sha256::new();
        column.add_to_hasher(&mut hasher);
        let hash = hasher.finalize();
        assert_eq!(hash.len(), 32);
    }

    #[test]
    fn test_related_views_serde_roundtrip() {
        let mut column = create_test_column();
        column.related_views = Some(vec![
            "public.view_one".to_string(),
            "analytics.view_two".to_string(),
        ]);

        let json = serde_json::to_string(&column).expect("serialize");
        let de: TableColumn = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(de.related_views, column.related_views);
    }

    #[test]
    fn test_get_script_varchar_column() {
        let column = create_test_column();
        let script = column.get_script();
        assert_eq!(script, "\"test_column\" varchar(255)");
    }

    #[test]
    fn test_get_script_varchar_column_not_null() {
        let mut column = create_test_column();
        column.is_nullable = false;
        let script = column.get_script();
        assert_eq!(script, "\"test_column\" varchar(255) not null");
    }

    #[test]
    fn test_get_script_varchar_column_with_default() {
        let mut column = create_test_column();
        column.column_default = Some("'default_value'".to_string());
        let script = column.get_script();
        assert_eq!(
            script,
            "\"test_column\" varchar(255) default 'default_value'"
        );
    }

    #[test]
    fn test_get_script_numeric_column() {
        let mut column = create_test_column();
        column.data_type = "numeric".to_string();
        column.character_maximum_length = None;
        column.numeric_precision = Some(10);
        column.numeric_scale = Some(2);
        let script = column.get_script();
        assert_eq!(script, "\"test_column\" numeric(10, 2)");
    }

    #[test]
    fn test_get_script_numeric_column_precision_only() {
        let mut column = create_test_column();
        column.data_type = "numeric".to_string();
        column.character_maximum_length = None;
        column.numeric_precision = Some(10);
        column.numeric_scale = None;
        let script = column.get_script();
        assert_eq!(script, "\"test_column\" numeric(10)");
    }

    #[test]
    fn test_get_script_with_collation() {
        let mut column = create_test_column();
        column.collation_name = Some("en_US.UTF-8".to_string());
        let script = column.get_script();
        assert_eq!(
            script,
            "\"test_column\" varchar(255) collate \"en_US.UTF-8\""
        );
    }

    #[test]
    fn test_get_script_identity_column() {
        let mut column = create_test_column();
        column.data_type = "integer".to_string();
        column.character_maximum_length = None;
        column.is_identity = true;
        column.identity_generation = Some("BY DEFAULT".to_string());
        let script = column.get_script();
        assert_eq!(
            script,
            "\"test_column\" integer generated BY DEFAULT as identity"
        );
    }

    #[test]
    fn test_get_script_identity_column_generated_always() {
        let mut column = create_test_column();
        column.data_type = "integer".to_string();
        column.character_maximum_length = None;
        column.is_identity = true;
        column.identity_generation = Some("ALWAYS".to_string());
        let script = column.get_script();
        assert_eq!(
            script,
            "\"test_column\" integer generated ALWAYS as identity"
        );
    }

    #[test]
    fn test_get_script_identity_column_with_options() {
        let mut column = create_test_column();
        column.data_type = "integer".to_string();
        column.character_maximum_length = None;
        column.is_identity = true;
        column.identity_generation = Some("ALWAYS".to_string());
        column.identity_start = Some("1".to_string());
        column.identity_increment = Some("1".to_string());
        column.identity_minimum = Some("1".to_string());
        column.identity_maximum = Some("1000".to_string());
        column.identity_cycle = true;
        let script = column.get_script();
        assert_eq!(
            script,
            "\"test_column\" integer generated ALWAYS as identity (start with 1 increment by 1 minvalue 1 maxvalue 1000 cycle)"
        );
    }

    #[test]
    fn test_get_script_generated_column() {
        let mut column = create_test_column();
        column.data_type = "integer".to_string();
        column.character_maximum_length = None;
        column.is_generated = "ALWAYS".to_string();
        column.generation_expression = Some("(id * 2)".to_string());
        let script = column.get_script();
        assert_eq!(
            script,
            "\"test_column\" integer generated always as ((id * 2)) stored"
        );
    }

    #[test]
    fn test_get_script_interval_column() {
        let mut column = create_test_column();
        column.data_type = "interval".to_string();
        column.character_maximum_length = None;
        column.interval_type = Some("DAY TO SECOND".to_string());
        let script = column.get_script();
        assert_eq!(script, "\"test_column\" interval DAY TO SECOND");
    }

    #[test]
    fn test_get_script_decimal_column() {
        let mut column = create_test_column();
        column.data_type = "decimal".to_string();
        column.character_maximum_length = None;
        column.numeric_precision = Some(15);
        column.numeric_scale = Some(4);
        let script = column.get_script();
        assert_eq!(script, "\"test_column\" decimal(15, 4)");
    }

    #[test]
    fn test_get_script_complex_column() {
        let mut column = create_test_column();
        column.data_type = "varchar".to_string();
        column.character_maximum_length = Some(100);
        column.is_nullable = false;
        column.column_default = Some("'test'".to_string());
        column.collation_name = Some("C".to_string());
        let script = column.get_script();
        assert_eq!(
            script,
            "\"test_column\" varchar(100) collate \"C\" default 'test' not null"
        );
    }

    #[test]
    fn test_get_script_with_empty_collation() {
        let mut column = create_test_column();
        column.collation_name = Some("".to_string());
        let script = column.get_script();
        assert_eq!(script, "\"test_column\" varchar(255)");
    }

    #[test]
    fn test_partial_eq_identical_columns() {
        let column1 = create_test_column();
        let column2 = create_test_column();
        assert_eq!(column1, column2);
    }

    #[test]
    fn test_partial_eq_ignores_catalog() {
        let column1 = create_test_column();
        let mut column2 = create_test_column();
        column2.catalog = "different_catalog".to_string();
        assert_eq!(column1, column2);
    }

    #[test]
    fn test_partial_eq_different_schema() {
        let column1 = create_test_column();
        let mut column2 = create_test_column();
        column2.schema = "different_schema".to_string();
        assert_ne!(column1, column2);
    }

    #[test]
    fn test_partial_eq_different_table() {
        let column1 = create_test_column();
        let mut column2 = create_test_column();
        column2.table = "different_table".to_string();
        assert_ne!(column1, column2);
    }

    #[test]
    fn test_partial_eq_different_name() {
        let column1 = create_test_column();
        let mut column2 = create_test_column();
        column2.name = "different_column".to_string();
        assert_ne!(column1, column2);
    }

    #[test]
    fn test_partial_eq_different_ordinal_position() {
        let column1 = create_test_column();
        let mut column2 = create_test_column();
        column2.ordinal_position = 2;
        assert_ne!(column1, column2);
    }

    #[test]
    fn test_partial_eq_different_column_default() {
        let column1 = create_test_column();
        let mut column2 = create_test_column();
        column2.column_default = Some("'different'".to_string());
        assert_ne!(column1, column2);
    }

    #[test]
    fn test_partial_eq_different_is_nullable() {
        let column1 = create_test_column();
        let mut column2 = create_test_column();
        column2.is_nullable = false;
        assert_ne!(column1, column2);
    }

    #[test]
    fn test_partial_eq_different_data_type() {
        let column1 = create_test_column();
        let mut column2 = create_test_column();
        column2.data_type = "text".to_string();
        assert_ne!(column1, column2);
    }

    #[test]
    fn test_partial_eq_different_character_maximum_length() {
        let column1 = create_test_column();
        let mut column2 = create_test_column();
        column2.character_maximum_length = Some(500);
        assert_ne!(column1, column2);
    }

    #[test]
    fn test_partial_eq_different_numeric_precision() {
        let column1 = create_test_column();
        let mut column2 = create_test_column();
        column2.numeric_precision = Some(10);
        assert_ne!(column1, column2);
    }

    #[test]
    fn test_partial_eq_different_is_identity() {
        let column1 = create_test_column();
        let mut column2 = create_test_column();
        column2.is_identity = true;
        assert_ne!(column1, column2);
    }

    #[test]
    fn test_partial_eq_is_generated_special_logic() {
        let mut column1 = create_test_column();
        let mut column2 = create_test_column();

        // Test case where both contain "ALWAYS"
        column1.is_generated = "ALWAYS".to_string();
        column2.is_generated = "GENERATED ALWAYS".to_string();
        assert_eq!(column1, column2);

        // Test case where both contain "BY DEFAULT"
        column1.is_generated = "BY DEFAULT".to_string();
        column2.is_generated = "GENERATED BY DEFAULT".to_string();
        assert_eq!(column1, column2);
    }

    #[test]
    fn test_partial_eq_different_generation_expression() {
        let mut column1 = create_test_column();
        let mut column2 = create_test_column();
        column1.generation_expression = Some("(id * 2)".to_string());
        column2.generation_expression = Some("(id * 3)".to_string());
        assert_ne!(column1, column2);
    }

    #[test]
    fn test_partial_eq_different_is_updatable() {
        let column1 = create_test_column();
        let mut column2 = create_test_column();
        column2.is_updatable = false;
        assert_ne!(column1, column2);
    }

    #[test]
    fn test_serde_serialization() {
        let column = create_test_column();
        let serialized = serde_json::to_string(&column).expect("Failed to serialize");
        let deserialized: TableColumn =
            serde_json::from_str(&serialized).expect("Failed to deserialize");
        assert_eq!(column, deserialized);
    }

    #[test]
    fn test_edge_cases_empty_strings() {
        let mut column = create_test_column();
        column.catalog = "".to_string();
        column.schema = "".to_string();
        column.table = "".to_string();
        column.name = "".to_string();
        column.data_type = "".to_string();

        let script = column.get_script();
        assert_eq!(script, "\"\"");

        let mut hasher = Sha256::new();
        column.add_to_hasher(&mut hasher);
        let hash = hasher.finalize();
        assert_eq!(hash.len(), 32);
    }

    #[test]
    fn test_column_with_all_optional_fields() {
        let mut column = create_test_column();
        column.character_maximum_length = Some(1000);
        column.character_octet_length = Some(4000);
        column.numeric_precision = Some(15);
        column.numeric_precision_radix = Some(10);
        column.numeric_scale = Some(5);
        column.datetime_precision = Some(6);
        column.interval_type = Some("YEAR TO MONTH".to_string());
        column.interval_precision = Some(2);
        column.character_set_catalog = Some("catalog".to_string());
        column.character_set_schema = Some("schema".to_string());
        column.character_set_name = Some("UTF8".to_string());
        column.collation_catalog = Some("coll_catalog".to_string());
        column.collation_schema = Some("coll_schema".to_string());
        column.collation_name = Some("en_US".to_string());
        column.domain_catalog = Some("domain_cat".to_string());
        column.domain_schema = Some("domain_sch".to_string());
        column.domain_name = Some("domain_name".to_string());
        column.udt_catalog = Some("udt_cat".to_string());
        column.udt_schema = Some("udt_sch".to_string());
        column.udt_name = Some("udt_name".to_string());
        column.scope_catalog = Some("scope_cat".to_string());
        column.scope_schema = Some("scope_sch".to_string());
        column.scope_name = Some("scope_name".to_string());
        column.maximum_cardinality = Some(100);
        column.dtd_identifier = Some("dtd_id".to_string());
        column.is_self_referencing = true;

        let mut hasher = Sha256::new();
        column.add_to_hasher(&mut hasher);
        let hash = hasher.finalize();
        assert_eq!(hash.len(), 32);
    }

    #[test]
    fn test_column_with_special_characters() {
        let mut column = create_test_column();
        column.name = "test-column_with$special@chars".to_string();
        column.column_default = Some("'value with spaces and ''quotes'''".to_string());
        column.collation_name = Some("collation-with-dashes".to_string());

        let script = column.get_script();
        assert!(script.contains("test-column_with$special@chars"));
        assert!(script.contains("collation-with-dashes"));
    }

    #[test]
    fn test_known_sha256_hash() {
        let column = create_test_column();
        let mut hasher = Sha256::new();
        column.add_to_hasher(&mut hasher);
        let hash = hasher.finalize();
        let hash_hex = format!("{hash:x}");

        // This is a known hash for the test data - if the hashing logic changes, this will fail
        assert_eq!(
            hash_hex,
            "db78cc9acba9b94dc4dfd0bfebf0ba1a2bbf63fe7d1ef6d2b1a8291e222d6484"
        );
    }

    #[test]
    fn test_text_data_type_without_length() {
        let mut column = create_test_column();
        column.data_type = "text".to_string();
        column.character_maximum_length = None;
        let script = column.get_script();
        assert_eq!(script, "\"test_column\" text");
    }

    #[test]
    fn test_integer_data_type() {
        let mut column = create_test_column();
        column.data_type = "integer".to_string();
        column.character_maximum_length = None;
        let script = column.get_script();
        assert_eq!(script, "\"test_column\" integer");
    }

    // --- Script methods: alter/add/drop ---
    #[test]
    fn test_get_alter_script_type_change() {
        let existing = create_test_column();
        let mut updated = existing.clone();
        updated.data_type = "integer".to_string();
        updated.character_maximum_length = None;
        let script = updated
            .get_alter_script(&existing)
            .expect("expected alter statement for type change");
        assert_eq!(
            script,
            "alter table \"public\".\"test_table\" alter column \"test_column\" type integer;\n"
        );
    }

    #[test]
    fn test_get_alter_script_type_change_different_schema() {
        let existing = create_test_column();
        let mut updated = existing.clone();
        updated.schema = "app".to_string();
        updated.table = "users".to_string();
        updated.data_type = "integer".to_string();
        updated.character_maximum_length = None;
        let script = updated
            .get_alter_script(&existing)
            .expect("expected alter statement for type change");
        assert_eq!(
            script,
            "alter table \"app\".\"users\" alter column \"test_column\" type integer;\n"
        );
    }

    #[test]
    fn test_get_alter_script_default_change() {
        let mut existing = create_test_column();
        existing.column_default = None;
        let mut updated = existing.clone();
        updated.column_default = Some("'default_value'".to_string());

        let script = updated
            .get_alter_script(&existing)
            .expect("expected alter statement for default change");
        assert_eq!(
            script,
            "alter table \"public\".\"test_table\" alter column \"test_column\" set default 'default_value';\n"
        );
    }

    #[test]
    fn test_get_alter_script_nullability_change() {
        let mut existing = create_test_column();
        existing.is_nullable = true;
        let mut updated = existing.clone();
        updated.is_nullable = false;

        let script = updated
            .get_alter_script(&existing)
            .expect("expected alter statement for nullability change");
        assert_eq!(
            script,
            "alter table \"public\".\"test_table\" alter column \"test_column\" set not null;\n"
        );
    }

    #[test]
    fn test_get_alter_script_returns_none_when_no_change() {
        let column = create_test_column();
        assert!(column.get_alter_script(&column).is_none());
    }

    #[test]
    fn test_get_add_script_basic() {
        let column = create_test_column();
        // get_script() => "\"test_column\" varchar(255)", so remainder is "varchar(255)"
        let expected =
            "alter table \"public\".\"test_table\" add column \"test_column\" varchar(255);\n";
        assert_eq!(column.get_add_script(), expected);
    }

    #[test]
    fn test_get_add_script_with_constraints() {
        let mut column = create_test_column();
        column.is_nullable = false;
        column.column_default = Some("'default_value'".to_string());
        column.collation_name = Some("en_US.UTF-8".to_string());

        let expected = "alter table \"public\".\"test_table\" add column \"test_column\" varchar(255) collate \"en_US.UTF-8\" default 'default_value' not null;\n";
        assert_eq!(column.get_add_script(), expected);
    }

    #[test]
    fn test_get_add_script_identity() {
        let mut column = create_test_column();
        column.data_type = "integer".to_string();
        column.character_maximum_length = None;
        column.is_identity = true;
        column.identity_generation = Some("BY DEFAULT".to_string());
        // Remainder should include identity clause after type
        let expected = "alter table \"public\".\"test_table\" add column \"test_column\" integer generated BY DEFAULT as identity;\n";
        assert_eq!(column.get_add_script(), expected);
    }

    #[test]
    fn test_get_drop_script_basic() {
        let column = create_test_column();
        let expected = "alter table \"public\".\"test_table\" drop column \"test_column\";\n";
        assert_eq!(column.get_drop_script(), expected);
    }

    #[test]
    fn test_get_drop_script_with_special_name() {
        let mut column = create_test_column();
        column.name = "weird name$".to_string();
        let expected = "alter table \"public\".\"test_table\" drop column \"weird name$\";\n";
        assert_eq!(column.get_drop_script(), expected);
    }

    #[test]
    fn test_get_alter_script_identity_update() {
        let mut existing = create_test_column();
        existing.is_identity = true;
        existing.identity_generation = Some("BY DEFAULT".to_string());
        existing.identity_start = Some("1".to_string());
        existing.identity_increment = Some("1".to_string());

        let mut updated = existing.clone();
        updated.identity_start = Some("100".to_string());
        updated.identity_increment = Some("5".to_string());

        let script = updated
            .get_alter_script(&existing)
            .expect("expected alter statement for identity update");

        assert_eq!(
            script,
            "alter table \"public\".\"test_table\" alter column \"test_column\" set START WITH 100;\nalter table \"public\".\"test_table\" alter column \"test_column\" set INCREMENT BY 5;\n"
        );
    }
}
