use crate::dump::{
    table_column::TableColumn, table_constraint::TableConstraint, table_index::TableIndex,
    table_trigger::TableTrigger,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::{Error, PgPool, Row};

// This is an information about a PostgreSQL table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Table {
    pub schema: String,
    pub name: String,
    pub owner: String,                     // Owner of the table
    pub space: Option<String>,             // Tablespace of the table
    pub has_indexes: bool,                 // Whether the table has indexes
    pub has_triggers: bool,                // Whether the table has triggers
    pub has_rules: bool,                   // Whether the table has rules
    pub has_rowsecurity: bool,             // Whether the table has row security
    pub columns: Vec<TableColumn>,         // Column names
    pub constraints: Vec<TableConstraint>, // Constraint names
    pub indexes: Vec<TableIndex>,          // Index names
    pub triggers: Vec<TableTrigger>,       // Trigger names
    pub definition: Option<String>,        // Table definition (optional)
}

impl Table {
    /// Fill information about table.
    pub async fn fill(&mut self, pool: &PgPool) -> Result<(), Error> {
        self.fill_columns(pool).await?;
        self.fill_indexes(pool).await?;
        self.fill_constraints(pool).await?;
        self.fill_triggers(pool).await?;
        self.fill_definition(pool).await?;
        Ok(())
    }

    /// Fill information about columns.
    async fn fill_columns(&mut self, pool: &PgPool) -> Result<(), Error> {
        let query = format!(
            "SELECT * FROM information_schema.columns WHERE table_schema = '{}' AND table_name = '{}'",
            self.schema, self.name
        );
        let rows = sqlx::query(&query).fetch_all(pool).await?;

        if !rows.is_empty() {
            for row in rows {
                let table_column = TableColumn {
                    catalog: row.get("table_catalog"),
                    schema: row.get("table_schema"),
                    table: row.get("table_name"),
                    name: row.get("column_name"),
                    ordinal_position: row.get("ordinal_position"),
                    column_default: row.get("column_default"),
                    is_nullable: row.get::<&str, _>("is_nullable") == "YES", // Convert to boolean
                    data_type: row.get("data_type"),
                    character_maximum_length: row.get("character_maximum_length"),
                    character_octet_length: row.get("character_octet_length"),
                    numeric_precision: row.get("numeric_precision"),
                    numeric_precision_radix: row.get("numeric_precision_radix"),
                    numeric_scale: row.get("numeric_scale"),
                    datetime_precision: row.get("datetime_precision"),
                    interval_type: row.get("interval_type"),
                    interval_precision: row.get("interval_precision"),
                    character_set_catalog: row.get("character_set_catalog"),
                    character_set_schema: row.get("character_set_schema"),
                    character_set_name: row.get("character_set_name"),
                    collation_catalog: row.get("collation_catalog"),
                    collation_schema: row.get("collation_schema"),
                    collation_name: row.get("collation_name"),
                    domain_catalog: row.get("domain_catalog"),
                    domain_schema: row.get("domain_schema"),
                    domain_name: row.get("domain_name"),
                    udt_catalog: row.get("udt_catalog"),
                    udt_schema: row.get("udt_schema"),
                    udt_name: row.get("udt_name"),
                    scope_catalog: row.get("scope_catalog"),
                    scope_schema: row.get("scope_schema"),
                    scope_name: row.get("scope_name"),
                    maximum_cardinality: row.get("maximum_cardinality"),
                    dtd_identifier: row.get("dtd_identifier"),
                    is_self_referencing: row.get::<&str, _>("is_self_referencing") == "YES", // Convert to boolean
                    is_identity: row.get::<&str, _>("is_identity") == "YES", // Convert to boolean
                    identity_generation: row.get("identity_generation"),
                    identity_start: row.get("identity_start"),
                    identity_increment: row.get("identity_increment"),
                    identity_maximum: row.get("identity_maximum"),
                    identity_minimum: row.get("identity_minimum"),
                    identity_cycle: row.get::<&str, _>("identity_cycle") == "YES", // Convert to boolean
                    is_generated: row.get("is_generated"),
                    generation_expression: row.get("generation_expression"),
                    is_updatable: row.get::<&str, _>("is_updatable") == "YES", // Convert to boolean
                };

                self.columns.push(table_column.clone());
            }
        }

        Ok(())
    }

    /// Fill information about indexes.
    async fn fill_indexes(&mut self, pool: &PgPool) -> Result<(), Error> {
        let query = format!(
            "SELECT * FROM pg_indexes WHERE schemaname = '{}' AND tablename = '{}'",
            self.schema, self.name
        );
        let rows = sqlx::query(&query).fetch_all(pool).await?;

        if !rows.is_empty() {
            for row in rows {
                let table_index = TableIndex {
                    schema: row.get("schemaname"),
                    table: row.get("tablename"),
                    name: row.get("indexname"),
                    catalog: row.get("tablespace"),
                    indexdef: row.get("indexdef"),
                };

                self.indexes.push(table_index.clone());
            }
        }

        Ok(())
    }

    /// Fill information about constraints.
    async fn fill_constraints(&mut self, pool: &PgPool) -> Result<(), Error> {
        let query = format!(
            "SELECT * FROM information_schema.table_constraints WHERE table_schema = '{}' AND table_name = '{}'",
            self.schema, self.name
        );

        let rows = sqlx::query(&query).fetch_all(pool).await?;

        if !rows.is_empty() {
            for row in rows {
                let table_constraint = TableConstraint {
                    catalog: row.get("constraint_catalog"),
                    schema: row.get("constraint_schema"),
                    name: row.get("constraint_name"),
                    table_catalog: row.get("table_catalog"),
                    table_schema: row.get("table_schema"),
                    table_name: row.get("table_name"),
                    constraint_type: row.get("constraint_type"),
                    is_deferrable: row.get::<&str, _>("is_deferrable") == "YES", // Convert to boolean
                    initially_deferred: row.get::<&str, _>("initially_deferred") == "YES", // Convert to boolean
                    enforced: row.get::<&str, _>("enforced") == "YES", // Convert to boolean
                    nulls_distinct: row
                        .try_get::<Option<&str>, _>("nulls_distinct")?
                        .map(|v| v == "YES"), // Convert to boolean
                };

                self.constraints.push(table_constraint.clone());
            }
        }

        Ok(())
    }

    /// Fill information about triggers.
    async fn fill_triggers(&mut self, pool: &PgPool) -> Result<(), Error> {
        let query = format!(
            "SELECT *, pg_get_triggerdef(oid) as tgdef FROM pg_trigger WHERE tgrelid = '{}'::regclass and tgisinternal = false",
            format!("{}.{}", self.schema, self.name)
        );
        let rows = sqlx::query(&query).fetch_all(pool).await?;

        if !rows.is_empty() {
            for row in rows {
                let table_trigger = TableTrigger {
                    oid: row.get("oid"),
                    name: row.get("tgname"),
                    definition: row.get("tgdef"),
                };

                self.triggers.push(table_trigger.clone());
            }
        }

        Ok(())
    }

    /// Fill table definition.
    async fn fill_definition(&mut self, pool: &PgPool) -> Result<(), Error> {
        // Check if pg_get_tabledef exists
        let check_func = "select proname from pg_proc where proname = 'pg_get_tabledef';";
        let func_row = sqlx::query(check_func).fetch_optional(pool).await?;
        if func_row.is_some() {
            let query = format!(
                "select pg_get_tabledef(oid) AS definition from pg_class where relname = '{}' AND relnamespace = '{}'::regnamespace;",
                self.name, self.schema
            );
            let row = sqlx::query(&query).fetch_one(pool).await?;
            if let Some(definition) = row.get::<Option<String>, _>("definition") {
                self.definition = Some(definition);
            } else {
                self.definition = None;
            }
        } else {
            self.definition = None;
        }
        Ok(())
    }

    /// Hash
    pub fn hash(&self) -> String {
        let mut hasher = Sha256::new();
        hasher.update(self.schema.as_bytes());
        hasher.update(self.name.as_bytes());
        hasher.update(self.owner.as_bytes());
        if let Some(space) = &self.space {
            hasher.update(space.as_bytes());
        }
        hasher.update(self.has_indexes.to_string().as_bytes());
        hasher.update(self.has_triggers.to_string().as_bytes());
        hasher.update(self.has_rules.to_string().as_bytes());
        hasher.update(self.has_rowsecurity.to_string().as_bytes());

        for column in &self.columns {
            column.add_to_hasher(&mut hasher);
        }

        for constraint in &self.constraints {
            constraint.add_to_hasher(&mut hasher);
        }

        for index in &self.indexes {
            index.add_to_hasher(&mut hasher);
        }

        for trigger in &self.triggers {
            trigger.add_to_hasher(&mut hasher);
        }

        hasher.update(self.definition.as_deref().unwrap_or("").as_bytes());

        format!("{:x}", hasher.finalize())
    }

    /// Get script for the table
    pub fn get_script(&self) -> String {
        // 1. Build CREATE TABLE statement
        let mut script = format!("create table {}.{} (\n", self.schema, self.name);

        // 2. Add column definitions
        let mut column_definitions = Vec::new();
        for column in &self.columns {
            let mut col_def = String::new();

            // Column name
            col_def.push_str(&format!("    \"{}\" ", column.name));

            // Handle identity columns as serial/bigserial
            if column.is_identity {
                if column.data_type == "integer" || column.data_type == "int4" {
                    col_def.push_str("serial");
                } else if column.data_type == "bigint" || column.data_type == "int8" {
                    col_def.push_str("bigserial");
                } else {
                    // Use standard column definition with identity
                    let col_script = column.get_script();
                    // Extract just the type and constraints part (skip the quoted name)
                    if let Some(type_start) = col_script.find(' ') {
                        col_def.push_str(&col_script[type_start + 1..]);
                    }
                }
            } else {
                // Use standard column definition
                let col_script = column.get_script();
                // Extract just the type and constraints part (skip the quoted name)
                if let Some(type_start) = col_script.find(' ') {
                    col_def.push_str(&col_script[type_start + 1..]);
                }
            }

            column_definitions.push(col_def);
        }

        // 4. Add primary key constraint if exists
        let has_pk_constraint = self
            .constraints
            .iter()
            .any(|c| c.constraint_type.to_lowercase() == "primary key");

        if has_pk_constraint {
            // Find PK columns from indexes if available
            for index in &self.indexes {
                if index.indexdef.to_lowercase().contains("primary key") {
                    if let Some(start) = index.indexdef.to_lowercase().find("primary key (") {
                        let after = &index.indexdef[start + "primary key (".len()..];
                        if let Some(end) = after.find(')') {
                            let cols_part = &after[..end];
                            let pk_cols: Vec<&str> = cols_part
                                .split(',')
                                .map(|c| c.trim().trim_matches('"'))
                                .collect();
                            if !pk_cols.is_empty() {
                                let pk_def = format!(
                                    "    primary key ({})",
                                    pk_cols
                                        .iter()
                                        .map(|c| format!("\"{}\"", c))
                                        .collect::<Vec<_>>()
                                        .join(", ")
                                );
                                column_definitions.push(pk_def);
                            }
                        }
                    }
                    break;
                }
            }
        }

        // Join all column definitions
        script.push_str(&column_definitions.join(",\n"));
        script.push_str("\n);\n\n");

        // 5. Add other constraints (excluding primary key)
        for constraint in &self.constraints {
            if constraint.constraint_type.to_lowercase() != "primary key" {
                script.push_str(&constraint.get_script());
            }
        }

        // 6. Add indexes (excluding primary key indexes)
        for index in &self.indexes {
            if !index.indexdef.to_lowercase().contains("primary key") {
                script.push_str(&index.get_script());
            }
        }

        // 7. Add triggers
        for trigger in &self.triggers {
            script.push_str(&trigger.get_script());
        }

        script
    }

    /// Get drop script for the table
    pub fn get_drop_script(&self) -> String {
        format!("drop table if exists {}.{};\n", self.schema, self.name)
    }
}
