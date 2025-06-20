use crate::dump::{
    table_column::TableColumn, table_constraint::TableConstraint, table_index::TableIndex,
    table_trigger::TableTrigger,
};
use serde::{Deserialize, Serialize};
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
}

impl Table {
    pub async fn fill(&mut self, pool: &PgPool) -> Result<(), Error> {
        self.fill_columns(pool).await?;
        self.fill_indexes(pool).await?;
        self.fill_constraints(pool).await?;
        self.fill_triggers(pool).await?;
        Ok(())
    }

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
}
