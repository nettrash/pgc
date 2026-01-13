use crate::dump::{
    table_column::TableColumn, table_constraint::TableConstraint, table_index::TableIndex,
    table_trigger::TableTrigger,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::{Error, PgPool, Row};

fn escape_single_quotes(value: &str) -> String {
    value.replace('\'', "''")
}

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
    pub partition_key: Option<String>,     // Partition key (PARTITION BY ...)
    pub partition_of: Option<String>,      // Parent table (PARTITION OF ...)
    pub partition_bound: Option<String>,   // Partition bound (FOR VALUES ... or DEFAULT)
    #[serde(default)]
    pub comment: Option<String>, // Table comment
    pub hash: Option<String>,              // Hash of the table
}

impl Table {
    /// Creates a new Table with the given name
    #[allow(clippy::too_many_arguments)] // Table metadata naturally includes these fields (from pg_class and related catalogs).
    pub fn new(
        schema: String,
        name: String,
        owner: String,
        space: Option<String>,
        columns: Vec<TableColumn>,
        constraints: Vec<TableConstraint>,
        indexes: Vec<TableIndex>,
        triggers: Vec<TableTrigger>,
        definition: Option<String>,
    ) -> Self {
        let mut table = Self {
            schema,
            name,
            owner,
            space,
            has_indexes: !indexes.is_empty(),
            has_triggers: !triggers.is_empty(),
            has_rules: false,
            has_rowsecurity: false,
            columns,
            constraints,
            indexes,
            triggers,
            definition,
            partition_key: None,
            partition_of: None,
            partition_bound: None,
            comment: None,
            hash: None,
        };
        table.hash();
        table
    }
    /// Fill information about table.
    pub async fn fill(&mut self, pool: &PgPool) -> Result<(), Error> {
        self.fill_columns(pool).await?;
        self.fill_indexes(pool).await?;
        self.fill_constraints(pool).await?;
        self.fill_triggers(pool).await?;
        self.fill_partition_info(pool).await?;
        self.fill_definition(pool).await?;
        Ok(())
    }

    /// Fill information about columns.
    async fn fill_columns(&mut self, pool: &PgPool) -> Result<(), Error> {
        let query = format!(
                        "SELECT
                                c.table_catalog,
                                c.table_schema,
                                c.table_name,
                                c.column_name,
                                c.ordinal_position,
                                c.column_default,
                                c.is_nullable,
                                CASE
                                        WHEN c.data_type IN ('USER-DEFINED', 'ARRAY')
                                                THEN pg_catalog.format_type(a.atttypid, a.atttypmod)
                                        ELSE c.data_type
                                END AS formatted_data_type,
                                c.character_maximum_length,
                                c.character_octet_length,
                                c.numeric_precision,
                                c.numeric_precision_radix,
                                c.numeric_scale,
                                c.datetime_precision,
                                c.interval_type,
                                c.interval_precision,
                                c.character_set_catalog,
                                c.character_set_schema,
                                c.character_set_name,
                                c.collation_catalog,
                                c.collation_schema,
                                c.collation_name,
                                c.domain_catalog,
                                c.domain_schema,
                                c.domain_name,
                                c.udt_catalog,
                                c.udt_schema,
                                c.udt_name,
                                c.scope_catalog,
                                c.scope_schema,
                                c.scope_name,
                                c.maximum_cardinality,
                                c.dtd_identifier,
                                c.is_self_referencing,
                                c.is_identity,
                                c.identity_generation,
                                c.identity_start,
                                c.identity_increment,
                                c.identity_maximum,
                                c.identity_minimum,
                                c.identity_cycle,
                                c.is_generated,
                                c.generation_expression,
                                c.is_updatable,
                                pd.description as column_comment,
                                (
                                        SELECT string_agg(DISTINCT quote_ident(v.view_schema) || '.' || quote_ident(v.view_name), ', ')
                                        FROM information_schema.view_column_usage v
                                        WHERE v.table_schema = c.table_schema
                                            AND v.table_name  = c.table_name
                                            AND v.column_name = c.column_name
                                ) AS related_views
                         FROM information_schema.columns c
                         JOIN pg_catalog.pg_namespace ns
                             ON ns.nspname = c.table_schema
                         JOIN pg_catalog.pg_class cls
                             ON cls.relnamespace = ns.oid
                            AND cls.relname = c.table_name
                         JOIN pg_catalog.pg_attribute a
                             ON a.attrelid = cls.oid
                            AND a.attname = c.column_name
                            AND a.attnum > 0
                            AND a.attisdropped = false
                         LEFT JOIN pg_description pd
                             ON pd.objoid = cls.oid
                            AND pd.objsubid = a.attnum
                        WHERE c.table_schema = '{}' AND c.table_name = '{}'
                        ORDER BY c.table_schema, c.table_name, c.ordinal_position",
                        self.schema.replace('\'', "''"),
                        self.name.replace('\'', "''")
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
                    data_type: row.get("formatted_data_type"),
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
                    related_views: row.get::<Option<String>, _>("related_views").map(|s| {
                        let mut views: Vec<String> =
                            s.split(',').map(|v| v.trim().to_string()).collect();
                        views.sort_unstable();
                        views
                    }),
                    comment: row.get("column_comment"),
                };

                self.columns.push(table_column.clone());
            }

            self.columns
                .sort_by(|a, b| a.ordinal_position.cmp(&b.ordinal_position));
        }

        Ok(())
    }

    /// Fill information about indexes.
    async fn fill_indexes(&mut self, pool: &PgPool) -> Result<(), Error> {
        let query = format!(
            "SELECT i.schemaname, i.tablename, i.indexname, i.tablespace, i.indexdef FROM pg_indexes i JOIN pg_class ic ON ic.relname = i.indexname JOIN pg_namespace n ON n.oid = ic.relnamespace AND n.nspname = i.schemaname JOIN pg_index idx ON idx.indexrelid = ic.oid WHERE NOT idx.indisprimary AND NOT idx.indisunique AND i.schemaname = '{}' AND i.tablename = '{}' AND NOT idx.indisprimary AND NOT idx.indisunique ORDER BY i.schemaname, i.tablename, i.indexname",
            self.schema.replace('\'', "''"),
            self.name.replace('\'', "''")
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

            self.indexes
                .sort_by(|a, b| a.name.to_lowercase().cmp(&b.name.to_lowercase()));
        }

        Ok(())
    }

    /// Fill information about constraints.
    async fn fill_constraints(&mut self, pool: &PgPool) -> Result<(), Error> {
        let query = format!(
            "SELECT current_database() AS catalog, n.nspname AS schema, c.conname AS constraint_name, t.relname AS table_name, c.contype::text AS constraint_type, c.condeferrable::text AS is_deferrable, c.condeferred::text AS initially_deferred, pg_get_constraintdef(c.oid, true) AS definition FROM pg_constraint c JOIN pg_class t ON t.oid = c.conrelid JOIN pg_namespace n ON n.oid = t.relnamespace WHERE n.nspname = '{}' AND t.relname = '{}' AND c.contype IN ('p','u','f','c') ORDER BY n.nspname, t.relname, c.conname;",
            self.schema.replace('\'', "''"),
            self.name.replace('\'', "''")
        );

        let rows = sqlx::query(&query).fetch_all(pool).await?;

        if !rows.is_empty() {
            for row in rows {
                let table_constraint = TableConstraint {
                    catalog: row.get("catalog"),
                    schema: row.get("schema"),
                    name: row.get("constraint_name"),
                    table_name: row.get("table_name"),
                    constraint_type: row.get("constraint_type"),
                    is_deferrable: row.get::<&str, _>("is_deferrable") == "YES", // Convert to boolean
                    initially_deferred: row.get::<&str, _>("initially_deferred") == "YES", // Convert to boolean
                    definition: row.get("definition"),
                };

                self.constraints.push(table_constraint.clone());
            }
        }

        Ok(())
    }

    /// Fill information about triggers.
    async fn fill_triggers(&mut self, pool: &PgPool) -> Result<(), Error> {
        let query = format!(
            "SELECT *, pg_get_triggerdef(oid) as tgdef FROM pg_trigger WHERE tgrelid = '\"{}\".\"{}\"'::regclass and tgisinternal = false ORDER BY tgname",
            self.schema.replace('"', "\"\""),
            self.name.replace('"', "\"\"")
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

            self.triggers
                .sort_by(|a, b| a.name.to_lowercase().cmp(&b.name.to_lowercase()));
        }

        Ok(())
    }

    /// Fill information about partition.
    async fn fill_partition_info(&mut self, pool: &PgPool) -> Result<(), Error> {
        let query = format!(
            "SELECT
                c.relkind::text,
                pg_get_partkeydef(c.oid) AS partition_key,
                pg_get_expr(c.relpartbound, c.oid) AS partition_bound,
                p.relname AS parent_table,
                pn.nspname AS parent_schema
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            LEFT JOIN pg_inherits i ON i.inhrelid = c.oid
            LEFT JOIN pg_class p ON p.oid = i.inhparent
            LEFT JOIN pg_namespace pn ON pn.oid = p.relnamespace
            WHERE n.nspname = '{}' AND c.relname = '{}'",
            self.schema.replace('\'', "''"),
            self.name.replace('\'', "''")
        );

        let row = sqlx::query(&query).fetch_optional(pool).await?;

        if let Some(row) = row {
            let relkind: String = row.get("relkind");

            // If it is a partitioned table (parent)
            if relkind == "p" {
                self.partition_key = row.get("partition_key");
            }

            // If it is a partition (child)
            if let Some(parent_table) = row.get::<Option<String>, _>("parent_table")
                && let Some(parent_schema) = row.get::<Option<String>, _>("parent_schema")
            {
                self.partition_of = Some(format!("\"{}\".\"{}\"", parent_schema, parent_table));
                self.partition_bound = row.get("partition_bound");
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
                "select pg_get_tabledef(oid) AS definition from pg_class where relname = '{}' AND relnamespace = '\"{}\"'::regnamespace;",
                self.name.replace('\'', "''"),
                self.schema.replace('"', "\"\"")
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

    /// Hash the table
    pub fn hash(&mut self) {
        let mut hasher = Sha256::new();
        hasher.update(self.schema.as_bytes());
        hasher.update(self.name.as_bytes());
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

        if let Some(pk) = &self.partition_key {
            hasher.update(pk.as_bytes());
        }
        if let Some(po) = &self.partition_of {
            hasher.update(po.as_bytes());
        }
        if let Some(pb) = &self.partition_bound {
            hasher.update(pb.as_bytes());
        }
        if let Some(cmt) = &self.comment {
            hasher.update(cmt.as_bytes());
        }

        self.hash = Some(format!("{:x}", hasher.finalize()));
    }

    /// Get script for the table
    pub fn get_script(&self) -> String {
        let mut script = String::new();

        if let Some(parent) = &self.partition_of {
            script.push_str(&format!(
                "create table \"{}\".\"{}\" partition of {}",
                self.schema, self.name, parent
            ));
            if let Some(bound) = &self.partition_bound {
                script.push_str(&format!("\n    {}", bound));
            }

            if let Some(partition_key) = &self.partition_key {
                script.push_str(&format!("\npartition by {}", partition_key));
            }

            script.push_str(";\n\n");
        } else {
            // 1. Build CREATE TABLE statement
            script.push_str(&format!(
                "create table \"{}\".\"{}\" (\n",
                self.schema, self.name
            ));

            // 2. Add column definitions
            let mut column_definitions = Vec::new();
            for column in &self.columns {
                let mut col_def = String::new();

                // Column name
                col_def.push_str(&format!("    \"{}\" ", column.name));

                // Use standard column definition
                let col_script = column.get_script();
                // Extract just the type and constraints part (skip the quoted name)
                if let Some(type_start) = col_script.find(' ') {
                    col_def.push_str(&col_script[type_start + 1..]);
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
                                            .map(|c| format!("\"{c}\""))
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
            script.push_str("\n)");

            if let Some(partition_key) = &self.partition_key {
                script.push_str(&format!("\npartition by {}", partition_key));
            }

            script.push_str(";\n\n");
        }

        // 5. Add other constraints (excluding primary key and foreign key)
        for constraint in &self.constraints {
            let c_type = constraint.constraint_type.to_lowercase();
            if c_type != "primary key" && c_type != "foreign key" {
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

        // 8. Add table comment (if any) and column comments
        if let Some(comment) = &self.comment {
            script.push_str(&format!(
                "comment on table \"{}\".\"{}\" is '{}';\n",
                self.schema,
                self.name,
                escape_single_quotes(comment)
            ));
        }

        for column in &self.columns {
            if let Some(comment_script) = column.get_comment_script() {
                script.push_str(&comment_script);
            }
        }

        script
    }

    /// Get drop script for the table
    pub fn get_drop_script(&self) -> String {
        format!(
            "drop table if exists \"{}\".\"{}\";\n",
            self.schema, self.name
        )
    }

    /// Get script for creating foreign keys
    pub fn get_foreign_key_script(&self) -> String {
        let mut script = String::new();
        for constraint in &self.constraints {
            if constraint.constraint_type.to_lowercase() == "foreign key" {
                script.push_str(&constraint.get_script());
            }
        }
        script
    }

    /// Get script for altering foreign keys
    pub fn get_foreign_key_alter_script(&self, to_table: &Table) -> String {
        let mut script = String::new();
        for new_constraint in &to_table.constraints {
            if new_constraint.constraint_type.to_lowercase() != "foreign key" {
                continue;
            }

            if let Some(old_constraint) = self
                .constraints
                .iter()
                .find(|c| c.name == new_constraint.name)
            {
                if old_constraint != new_constraint {
                    if let Some(alter_script) = old_constraint.get_alter_script(new_constraint) {
                        script.push_str(&alter_script);
                    } else {
                        // Drop is handled in the table's get_alter_script, so just add the new FK here.
                        script.push_str(&new_constraint.get_script());
                    }
                }
            } else {
                // New FK
                script.push_str(&new_constraint.get_script());
            }
        }
        script
    }

    pub fn get_alter_script(&self, to_table: &Table, use_drop: bool) -> String {
        // If partition key changes (e.g. from LIST to RANGE, or different column), we must recreate the table.
        // Also if table changes from partitioned to non-partitioned or vice versa.
        if self.partition_key != to_table.partition_key {
            let drop_script = if use_drop {
                self.get_drop_script()
            } else {
                self.get_drop_script()
                    .lines()
                    .map(|l| format!("-- {}\n", l))
                    .collect()
            };
            return format!(
                "/* Partition key changed. Table must be recreated. Data loss will occur! */\n{}{}",
                drop_script,
                to_table.get_script()
            );
        }

        let mut constraint_pre_script = String::new();
        let mut column_alter_script = String::new();
        let mut column_drop_script = String::new();
        let mut constraint_post_script = String::new();
        let mut index_script = String::new();
        let mut trigger_script = String::new();
        let mut index_drop_script = String::new();
        let mut trigger_drop_script = String::new();
        let mut partition_script = String::new();

        // Handle partition changes
        if self.partition_of != to_table.partition_of
            || (self.partition_of.is_some() && self.partition_bound != to_table.partition_bound)
        {
            // If it was a partition, detach it
            if let Some(old_parent) = &self.partition_of {
                let detach_cmd = format!(
                    "alter table {} detach partition \"{}\".\"{}\";\n",
                    old_parent, self.schema, self.name
                );
                if use_drop {
                    partition_script.push_str(&detach_cmd);
                } else {
                    partition_script.push_str(&format!("-- {}", detach_cmd));
                }
            }

            // If it is now a partition, attach it
            if let Some(new_parent) = &to_table.partition_of
                && let Some(bound) = &to_table.partition_bound
            {
                partition_script.push_str(&format!(
                    "alter table {} attach partition \"{}\".\"{}\" {};\n",
                    new_parent, self.schema, self.name, bound
                ));
            }
        }

        // Collect column additions or alterations
        for new_col in &to_table.columns {
            if let Some(old_col) = self.columns.iter().find(|c| c.name == new_col.name) {
                if old_col != new_col
                    && let Some(alter_col_script) = new_col.get_alter_script(old_col)
                {
                    column_alter_script.push_str(&alter_col_script);
                }
            } else {
                column_alter_script.push_str(&new_col.get_add_script());
            }
        }

        // Collect column drops separately so they happen after constraint drops
        for old_col in &self.columns {
            if !to_table.columns.iter().any(|c| c.name == old_col.name) {
                let drop_cmd = old_col.get_drop_script();
                if use_drop {
                    column_drop_script.push_str(&drop_cmd);
                } else {
                    column_drop_script.push_str(
                        &drop_cmd
                            .lines()
                            .map(|l| format!("-- {}\n", l))
                            .collect::<String>(),
                    );
                }
            }
        }

        // Collect constraint changes; drop statements run before column drops
        for new_constraint in &to_table.constraints {
            let is_fk = new_constraint.constraint_type.to_lowercase() == "foreign key";
            if let Some(old_constraint) = self
                .constraints
                .iter()
                .find(|c| c.name == new_constraint.name)
            {
                if old_constraint != new_constraint {
                    if let Some(alter_script) = old_constraint.get_alter_script(new_constraint) {
                        if !is_fk {
                            constraint_post_script.push_str(&alter_script);
                        }
                    } else {
                        let drop_cmd = old_constraint.get_drop_script();
                        if use_drop {
                            constraint_pre_script.push_str(&drop_cmd);
                        } else {
                            constraint_pre_script.push_str(
                                &drop_cmd
                                    .lines()
                                    .map(|l| format!("-- {}\n", l))
                                    .collect::<String>(),
                            );
                        }

                        if !is_fk {
                            constraint_post_script.push_str(&new_constraint.get_script());
                        }
                    }
                }
            } else if !is_fk {
                constraint_post_script.push_str(&new_constraint.get_script());
            }
        }

        for old_constraint in &self.constraints {
            if !to_table
                .constraints
                .iter()
                .any(|c| c.name == old_constraint.name)
            {
                let drop_cmd = old_constraint.get_drop_script();
                if use_drop {
                    constraint_pre_script.push_str(&drop_cmd);
                } else {
                    constraint_pre_script.push_str(
                        &drop_cmd
                            .lines()
                            .map(|l| format!("-- {}\n", l))
                            .collect::<String>(),
                    );
                }
            }
        }

        // Table comment changes
        if self.comment != to_table.comment {
            let comment_stmt = if let Some(cmt) = &to_table.comment {
                format!(
                    "comment on table \"{}\".\"{}\" is '{}';\n",
                    to_table.schema,
                    to_table.name,
                    escape_single_quotes(cmt)
                )
            } else {
                format!(
                    "comment on table \"{}\".\"{}\" is null;\n",
                    to_table.schema, to_table.name
                )
            };
            constraint_post_script.push_str(&comment_stmt);
        }

        // Collect index updates
        for new_index in &to_table.indexes {
            if let Some(old_index) = self.indexes.iter().find(|i| i.name == new_index.name) {
                if old_index != new_index {
                    let drop_cmd = format!(
                        "drop index if exists \"{}\".\"{}\";\n",
                        new_index.schema, new_index.name
                    );
                    if use_drop {
                        index_drop_script.push_str(&drop_cmd);
                    } else {
                        index_drop_script.push_str(&format!("-- {}", drop_cmd));
                    }
                    index_script.push_str(&new_index.get_script());
                }
            } else {
                index_script.push_str(&new_index.get_script());
            }
        }

        // Collect trigger updates
        for new_trigger in &to_table.triggers {
            if let Some(old_trigger) = self.triggers.iter().find(|t| t.name == new_trigger.name) {
                if old_trigger != new_trigger {
                    let drop_cmd = format!(
                        "drop trigger if exists \"{}\" on \"{}\".\"{}\";\n",
                        old_trigger.name, self.schema, self.name
                    );
                    if use_drop {
                        trigger_drop_script.push_str(&drop_cmd);
                    } else {
                        trigger_drop_script.push_str(&format!("-- {}", drop_cmd));
                    }
                    trigger_script.push_str(&new_trigger.get_script());
                }
            } else {
                trigger_script.push_str(&new_trigger.get_script());
            }
        }

        for old_index in &self.indexes {
            if !to_table.indexes.iter().any(|i| i.name == old_index.name) {
                let drop_cmd = format!(
                    "drop index if exists \"{}\".\"{}\";\n",
                    old_index.schema, old_index.name
                );
                if use_drop {
                    index_drop_script.push_str(&drop_cmd);
                } else {
                    index_drop_script.push_str(&format!("-- {}", drop_cmd));
                }
            }
        }

        for old_trigger in &self.triggers {
            if !to_table.triggers.iter().any(|t| t.name == old_trigger.name) {
                let drop_cmd = format!(
                    "drop trigger if exists \"{}\" on \"{}\".\"{}\";\n",
                    old_trigger.name, self.schema, self.name
                );
                if use_drop {
                    trigger_drop_script.push_str(&drop_cmd);
                } else {
                    trigger_drop_script.push_str(&format!("-- {}", drop_cmd));
                }
            }
        }

        let mut script = String::new();
        script.push_str(&partition_script);
        script.push_str(&constraint_pre_script);
        script.push_str(&column_alter_script);
        script.push_str(&index_drop_script);
        script.push_str(&trigger_drop_script);
        script.push_str(&column_drop_script);
        script.push_str(&constraint_post_script);
        script.push_str(&index_script);
        script.push_str(&trigger_script);

        script
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::postgres::types::Oid;

    fn base_column(name: &str, ordinal_position: i32) -> TableColumn {
        TableColumn {
            catalog: "postgres".to_string(),
            schema: "public".to_string(),
            table: "users".to_string(),
            name: name.to_string(),
            ordinal_position,
            column_default: None,
            is_nullable: true,
            data_type: "text".to_string(),
            character_maximum_length: None,
            character_octet_length: None,
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
            comment: None,
        }
    }

    fn identity_column(name: &str, ordinal_position: i32, data_type: &str) -> TableColumn {
        let mut column = base_column(name, ordinal_position);
        column.data_type = data_type.to_string();
        column.is_identity = true;
        column.is_nullable = false;
        column.identity_generation = Some("BY DEFAULT".to_string());
        column
    }

    fn name_column() -> TableColumn {
        let mut column = base_column("name", 2);
        column.is_nullable = false;
        column
    }

    fn name_column_with_default() -> TableColumn {
        let mut column = name_column();
        column.column_default = Some("'unknown'::text".to_string());
        column
    }

    fn legacy_column() -> TableColumn {
        let mut column = base_column("legacy", 3);
        column.is_nullable = true;
        column
    }

    fn email_column() -> TableColumn {
        let mut column = base_column("email", 3);
        column.is_nullable = true;
        column
    }

    fn primary_key_constraint() -> TableConstraint {
        TableConstraint {
            catalog: "postgres".to_string(),
            schema: "public".to_string(),
            name: "users_pkey".to_string(),
            table_name: "users".to_string(),
            constraint_type: "PRIMARY KEY".to_string(),
            is_deferrable: false,
            initially_deferred: false,
            definition: None,
        }
    }

    fn check_constraint(name: &str, definition: &str) -> TableConstraint {
        TableConstraint {
            catalog: "postgres".to_string(),
            schema: "public".to_string(),
            name: name.to_string(),
            table_name: "users".to_string(),
            constraint_type: "CHECK".to_string(),
            is_deferrable: false,
            initially_deferred: false,
            definition: Some(definition.to_string()),
        }
    }

    fn foreign_key_constraint(is_deferrable: bool, initially_deferred: bool) -> TableConstraint {
        TableConstraint {
            catalog: "postgres".to_string(),
            schema: "public".to_string(),
            name: "users_account_fk".to_string(),
            table_name: "users".to_string(),
            constraint_type: "FOREIGN KEY".to_string(),
            is_deferrable,
            initially_deferred,
            definition: Some("FOREIGN KEY (account_id) REFERENCES public.accounts(id)".to_string()),
        }
    }

    fn unique_constraint(name: &str, definition: &str) -> TableConstraint {
        TableConstraint {
            catalog: "postgres".to_string(),
            schema: "public".to_string(),
            name: name.to_string(),
            table_name: "users".to_string(),
            constraint_type: "UNIQUE".to_string(),
            is_deferrable: false,
            initially_deferred: false,
            definition: Some(definition.to_string()),
        }
    }

    fn primary_key_index() -> TableIndex {
        TableIndex {
            schema: "public".to_string(),
            table: "users".to_string(),
            name: "users_pkey".to_string(),
            catalog: None,
            indexdef:
                "create unique index users_pkey on public.users using btree (\"id\") primary key (\"id\")"
                    .to_string(),
        }
    }

    fn name_index(definition: &str) -> TableIndex {
        TableIndex {
            schema: "public".to_string(),
            table: "users".to_string(),
            name: "idx_users_name".to_string(),
            catalog: None,
            indexdef: definition.to_string(),
        }
    }

    fn legacy_index() -> TableIndex {
        TableIndex {
            schema: "public".to_string(),
            table: "users".to_string(),
            name: "idx_users_old".to_string(),
            catalog: None,
            indexdef: "create index idx_users_old on public.users using btree (legacy)".to_string(),
        }
    }

    fn email_index() -> TableIndex {
        TableIndex {
            schema: "public".to_string(),
            table: "users".to_string(),
            name: "idx_users_email".to_string(),
            catalog: None,
            indexdef: "create index idx_users_email on public.users using btree (email)"
                .to_string(),
        }
    }

    fn trigger(name: &str, definition: &str, oid: u32) -> TableTrigger {
        TableTrigger {
            oid: Oid(oid),
            name: name.to_string(),
            definition: definition.to_string(),
        }
    }

    fn basic_table() -> Table {
        Table::new(
            "public".to_string(),
            "users".to_string(),
            "postgres".to_string(),
            Some("pg_default".to_string()),
            vec![identity_column("id", 1, "integer"), name_column()],
            vec![
                primary_key_constraint(),
                check_constraint("users_name_check", "CHECK (name <> '')"),
            ],
            vec![
                primary_key_index(),
                name_index("create index idx_users_name on public.users using btree (name)"),
            ],
            vec![trigger(
                "audit_user",
                "create trigger audit_user before insert on public.users for each row execute function log_user()",
                1,
            )],
            None,
        )
    }

    #[test]
    fn test_table_new_initializes_flags_and_hash() {
        let table = basic_table();

        assert!(table.has_indexes);
        assert!(table.has_triggers);
        assert!(!table.has_rules);
        assert!(!table.has_rowsecurity);
        assert!(table.hash.is_some());

        let mut recomputed = table.clone();
        recomputed.hash();
        assert_eq!(table.hash, recomputed.hash);

        let mut modified = table.clone();
        if let Some(column) = modified.columns.iter_mut().find(|col| col.name == "name") {
            column.column_default = Some("'anonymous'::text".to_string());
        }
        modified.hash();
        assert_ne!(table.hash, modified.hash);
    }

    #[test]
    fn test_get_script_generates_full_definition() {
        let table = basic_table();

        let script = table.get_script();

        let expected = concat!(
            "create table \"public\".\"users\" (\n",
            "    \"id\" integer generated BY DEFAULT as identity not null,\n",
            "    \"name\" text not null,\n",
            "    primary key (\"id\")\n",
            ");\n\n",
            "alter table \"public\".\"users\" add constraint \"users_name_check\" check (name <> '') ;\n",
            "create index idx_users_name on public.users using btree (name);\n",
            "create trigger audit_user before insert on public.users for each row execute function log_user();\n",
        );

        assert_eq!(script, expected);
    }

    #[test]
    fn test_get_script_identity_column_not_serial() {
        let table = Table::new(
            "public".to_string(),
            "users".to_string(),
            "postgres".to_string(),
            Some("pg_default".to_string()),
            vec![identity_column("id", 1, "integer")],
            vec![],
            vec![],
            vec![],
            None,
        );

        let script = table.get_script();
        assert!(script.contains("\"id\" integer generated BY DEFAULT as identity"));
        assert!(!script.contains("serial"));
    }

    #[test]
    fn test_get_drop_script_returns_statement() {
        let table = basic_table();
        assert_eq!(
            table.get_drop_script(),
            "drop table if exists \"public\".\"users\";\n"
        );
    }

    #[test]
    fn test_get_alter_script_handles_complex_differences() {
        let from_table = Table::new(
            "public".to_string(),
            "users".to_string(),
            "postgres".to_string(),
            Some("pg_default".to_string()),
            vec![
                identity_column("id", 1, "integer"),
                name_column(),
                legacy_column(),
            ],
            vec![
                primary_key_constraint(),
                check_constraint("users_name_check", "CHECK (name <> '')"),
                foreign_key_constraint(false, false),
                check_constraint("users_legacy_check", "CHECK (legacy IS NOT NULL)"),
            ],
            vec![
                primary_key_index(),
                name_index("create index idx_users_name on public.users using btree (name)"),
                legacy_index(),
            ],
            vec![
                trigger(
                    "audit_user",
                    "create trigger audit_user before insert on public.users for each row execute function log_user()",
                    1,
                ),
                trigger(
                    "cleanup_user",
                    "create trigger cleanup_user after delete on public.users for each row execute function cleanup()",
                    2,
                ),
            ],
            Some("create table public.users (...);".to_string()),
        );

        let to_table = Table::new(
            "public".to_string(),
            "users".to_string(),
            "postgres".to_string(),
            Some("pg_default".to_string()),
            vec![
                identity_column("id", 1, "integer"),
                name_column_with_default(),
                email_column(),
            ],
            vec![
                primary_key_constraint(),
                check_constraint("users_name_check", "CHECK (char_length(name) > 0)"),
                foreign_key_constraint(true, true),
                unique_constraint("users_email_unique", "UNIQUE (email)"),
            ],
            vec![
                primary_key_index(),
                name_index("create index idx_users_name on public.users using btree (lower(name))"),
                email_index(),
            ],
            vec![
                trigger(
                    "audit_user",
                    "create trigger audit_user after insert on public.users for each row execute function log_user_change()",
                    3,
                ),
                trigger(
                    "notify_user",
                    "create trigger notify_user after insert on public.users for each row execute function notify()",
                    4,
                ),
            ],
            Some("create table public.users (...);".to_string()),
        );

        let script = from_table.get_alter_script(&to_table, true);
        let fk_script = from_table.get_foreign_key_alter_script(&to_table);

        let expected_fragments = [
            "alter table \"public\".\"users\" drop constraint \"users_name_check\";\n",
            "alter table \"public\".\"users\" drop constraint \"users_legacy_check\";\n",
            "alter table \"public\".\"users\" alter column \"name\" set default 'unknown'::text;\n",
            "alter table \"public\".\"users\" add column \"email\" text;\n",
            "drop index if exists \"public\".\"idx_users_name\";\n",
            "drop index if exists \"public\".\"idx_users_old\";\n",
            "drop trigger if exists \"audit_user\" on \"public\".\"users\";\n",
            "drop trigger if exists \"cleanup_user\" on \"public\".\"users\";\n",
            "alter table \"public\".\"users\" drop column \"legacy\";\n",
            "alter table \"public\".\"users\" add constraint \"users_name_check\" check (char_length(name) > 0) ;\n",
            "alter table \"public\".\"users\" add constraint \"users_email_unique\" unique (email) ;\n",
            "create index idx_users_name on public.users using btree (lower(name));\n",
            "create index idx_users_email on public.users using btree (email);\n",
            "create trigger audit_user after insert on public.users for each row execute function log_user_change();\n",
            "create trigger notify_user after insert on public.users for each row execute function notify();\n",
        ];

        let mut last_position = 0usize;
        for fragment in expected_fragments {
            let position = script
                .find(fragment)
                .unwrap_or_else(|| panic!("fragment not found: {fragment}"));
            assert!(
                position >= last_position,
                "fragment `{fragment}` appears out of order"
            );
            last_position = position;
        }

        assert!(script.contains("'unknown'::text"));
        assert!(script.contains("lower(name)"));
        assert!(script.contains("notify_user"));

        assert!(fk_script.contains("alter table \"public\".\"users\" alter constraint \"users_account_fk\" deferrable initially deferred;\n"));
    }

    #[test]
    fn test_get_foreign_key_script() {
        let table = Table::new(
            "public".to_string(),
            "users".to_string(),
            "postgres".to_string(),
            Some("pg_default".to_string()),
            vec![identity_column("id", 1, "integer")],
            vec![
                primary_key_constraint(),
                check_constraint("users_name_check", "CHECK (name <> '')"),
                foreign_key_constraint(false, false),
            ],
            vec![],
            vec![],
            None,
        );

        let script = table.get_foreign_key_script();

        assert!(script.contains("alter table \"public\".\"users\" add constraint \"users_account_fk\" foreign key (account_id) references public.accounts(id)"));
        assert!(!script.contains("users_name_check"));
        assert!(!script.contains("users_pkey"));
    }

    fn custom_foreign_key_constraint(name: &str, definition: &str) -> TableConstraint {
        TableConstraint {
            catalog: "postgres".to_string(),
            schema: "public".to_string(),
            name: name.to_string(),
            table_name: "users".to_string(),
            constraint_type: "FOREIGN KEY".to_string(),
            is_deferrable: false,
            initially_deferred: false,
            definition: Some(definition.to_string()),
        }
    }

    #[test]
    fn test_get_foreign_key_alter_script_add_new_fk() {
        let from_table = Table::new(
            "public".to_string(),
            "users".to_string(),
            "postgres".to_string(),
            None,
            vec![],
            vec![],
            vec![],
            vec![],
            None,
        );

        let to_table = Table::new(
            "public".to_string(),
            "users".to_string(),
            "postgres".to_string(),
            None,
            vec![],
            vec![custom_foreign_key_constraint(
                "fk_new",
                "FOREIGN KEY (col) REFERENCES other(id)",
            )],
            vec![],
            vec![],
            None,
        );

        let script = from_table.get_foreign_key_alter_script(&to_table);
        assert!(script.contains(
            "alter table \"public\".\"users\" add constraint \"fk_new\" foreign key (col) references other(id)"
        ));
    }

    #[test]
    fn test_get_foreign_key_alter_script_drop_fk() {
        let from_table = Table::new(
            "public".to_string(),
            "users".to_string(),
            "postgres".to_string(),
            None,
            vec![],
            vec![custom_foreign_key_constraint(
                "fk_old",
                "FOREIGN KEY (col) REFERENCES other(id)",
            )],
            vec![],
            vec![],
            None,
        );

        let to_table = Table::new(
            "public".to_string(),
            "users".to_string(),
            "postgres".to_string(),
            None,
            vec![],
            vec![],
            vec![],
            vec![],
            None,
        );

        let script = from_table.get_foreign_key_alter_script(&to_table);
        assert_eq!(script, ""); // Should be empty as drop is handled in get_alter_script
    }

    #[test]
    fn test_get_foreign_key_alter_script_recreate_fk() {
        let from_table = Table::new(
            "public".to_string(),
            "users".to_string(),
            "postgres".to_string(),
            None,
            vec![],
            vec![custom_foreign_key_constraint(
                "fk_change",
                "FOREIGN KEY (col) REFERENCES table_a(id)",
            )],
            vec![],
            vec![],
            None,
        );

        let to_table = Table::new(
            "public".to_string(),
            "users".to_string(),
            "postgres".to_string(),
            None,
            vec![],
            vec![custom_foreign_key_constraint(
                "fk_change",
                "FOREIGN KEY (col) REFERENCES table_b(id)",
            )],
            vec![],
            vec![],
            None,
        );

        let script = from_table.get_foreign_key_alter_script(&to_table);
        // Should contain the add constraint part. Drop is elsewhere.
        assert!(script.contains("alter table \"public\".\"users\" add constraint \"fk_change\" foreign key (col) references table_b(id)"));
    }

    #[test]
    fn test_get_foreign_key_alter_script_no_change() {
        let fk = custom_foreign_key_constraint("fk_same", "FOREIGN KEY (col) REFERENCES other(id)");

        let from_table = Table::new(
            "public".to_string(),
            "users".to_string(),
            "postgres".to_string(),
            None,
            vec![],
            vec![fk.clone()],
            vec![],
            vec![],
            None,
        );

        let to_table = Table::new(
            "public".to_string(),
            "users".to_string(),
            "postgres".to_string(),
            None,
            vec![],
            vec![fk],
            vec![],
            vec![],
            None,
        );

        let script = from_table.get_foreign_key_alter_script(&to_table);
        assert_eq!(script, "");
    }

    #[test]
    fn test_foreign_key_full_lifecycle_workflow() {
        // 1. Drop FK (exists in from, not in to)
        let fk_drop_from = Table::new(
            "public".to_string(),
            "users".to_string(),
            "postgres".to_string(),
            None,
            vec![],
            vec![custom_foreign_key_constraint(
                "fk_drop",
                "FOREIGN KEY (col) REFERENCES other(id)",
            )],
            vec![],
            vec![],
            None,
        );
        let fk_drop_to = Table::new(
            "public".to_string(),
            "users".to_string(),
            "postgres".to_string(),
            None,
            vec![],
            vec![],
            vec![],
            vec![],
            None,
        );

        let drop_main_script = fk_drop_from.get_alter_script(&fk_drop_to, true);
        let drop_fk_script = fk_drop_from.get_foreign_key_alter_script(&fk_drop_to);

        assert!(
            drop_main_script
                .contains("alter table \"public\".\"users\" drop constraint \"fk_drop\";")
        );
        assert_eq!(drop_fk_script, "");

        // 2. Add FK (not in from, exists in to)
        let fk_add_from = fk_drop_to.clone();
        let fk_add_to = fk_drop_from.clone(); // reusing table with FK

        let add_main_script = fk_add_from.get_alter_script(&fk_add_to, true);
        let add_fk_script = fk_add_from.get_foreign_key_alter_script(&fk_add_to);

        assert!(!add_main_script.contains("fk_drop")); // Main script shouldn't touch new FKs
        assert!(add_fk_script.contains(
            "alter table \"public\".\"users\" add constraint \"fk_drop\" foreign key (col) references other(id)"
        ));

        // 3. Recreate FK (definition change)
        let fk_change_from = Table::new(
            "public".to_string(),
            "users".to_string(),
            "postgres".to_string(),
            None,
            vec![],
            vec![custom_foreign_key_constraint(
                "fk_change",
                "FOREIGN KEY (col) REFERENCES old_table(id)",
            )],
            vec![],
            vec![],
            None,
        );
        let fk_change_to = Table::new(
            "public".to_string(),
            "users".to_string(),
            "postgres".to_string(),
            None,
            vec![],
            vec![custom_foreign_key_constraint(
                "fk_change",
                "FOREIGN KEY (col) REFERENCES new_table(id)",
            )],
            vec![],
            vec![],
            None,
        );

        let change_main_script = fk_change_from.get_alter_script(&fk_change_to, true);
        let change_fk_script = fk_change_from.get_foreign_key_alter_script(&fk_change_to);

        assert!(
            change_main_script
                .contains("alter table \"public\".\"users\" drop constraint \"fk_change\";")
        );
        assert!(change_fk_script.contains("alter table \"public\".\"users\" add constraint \"fk_change\" foreign key (col) references new_table(id)"));
    }

    fn create_dummy_column(name: &str, data_type: &str) -> TableColumn {
        TableColumn {
            name: name.to_string(),
            data_type: data_type.to_string(),
            is_nullable: true,
            ordinal_position: 1,
            catalog: "".to_string(),
            schema: "".to_string(),
            table: "".to_string(),
            column_default: None,
            character_maximum_length: None,
            character_octet_length: None,
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
            is_generated: "".to_string(),
            generation_expression: None,
            is_updatable: true,
            related_views: None,
            comment: None,
        }
    }

    #[test]
    fn test_partitioned_table_script() {
        let mut table = Table::new(
            "data".to_string(),
            "test".to_string(),
            "owner".to_string(),
            None,
            vec![
                create_dummy_column("id", "bigint"),
                create_dummy_column("flow_id", "varchar"),
            ],
            vec![],
            vec![],
            vec![],
            None,
        );
        table.partition_key = Some("LIST (flow_id)".to_string());

        let script = table.get_script();
        assert!(script.contains("create table \"data\".\"test\""));
        assert!(script.contains("partition by LIST (flow_id)"));
    }

    #[test]
    fn test_partition_child_script() {
        let mut table = Table::new(
            "data".to_string(),
            "test_default".to_string(),
            "owner".to_string(),
            None,
            vec![],
            vec![],
            vec![],
            vec![],
            None,
        );
        table.partition_of = Some("\"data\".\"test\"".to_string());
        table.partition_bound = Some("DEFAULT".to_string());

        let script = table.get_script();
        assert!(
            script
                .contains("create table \"data\".\"test_default\" partition of \"data\".\"test\"")
        );
        assert!(script.contains("DEFAULT"));
    }

    #[test]
    fn test_get_alter_script_partition_bound_change() {
        let mut from_table = Table::new(
            "data".to_string(),
            "test_default".to_string(),
            "owner".to_string(),
            None,
            vec![],
            vec![],
            vec![],
            vec![],
            None,
        );
        from_table.partition_of = Some("\"data\".\"test\"".to_string());
        from_table.partition_bound = Some("FOR VALUES IN (1)".to_string());

        let mut to_table = Table::new(
            "data".to_string(),
            "test_default".to_string(),
            "owner".to_string(),
            None,
            vec![],
            vec![],
            vec![],
            vec![],
            None,
        );
        to_table.partition_of = Some("\"data\".\"test\"".to_string());
        to_table.partition_bound = Some("FOR VALUES IN (2)".to_string());

        let script = from_table.get_alter_script(&to_table, true);

        assert!(script.contains("detach partition"));
        assert!(script.contains("attach partition"));
    }

    #[test]
    fn test_get_alter_script_partition_key_change() {
        let mut from_table = Table::new(
            "data".to_string(),
            "test".to_string(),
            "owner".to_string(),
            None,
            vec![],
            vec![],
            vec![],
            vec![],
            None,
        );
        from_table.partition_key = Some("LIST (id)".to_string());

        let mut to_table = Table::new(
            "data".to_string(),
            "test".to_string(),
            "owner".to_string(),
            None,
            vec![],
            vec![],
            vec![],
            vec![],
            None,
        );
        to_table.partition_key = Some("LIST (flow_id)".to_string());

        let script = from_table.get_alter_script(&to_table, true);

        assert!(script.contains("Partition key changed"));
        assert!(script.contains("drop table"));
        assert!(script.contains("create table"));
    }

    #[test]
    fn test_get_alter_script_partition_key_change_no_drop() {
        let mut from_table = Table::new(
            "data".to_string(),
            "test".to_string(),
            "owner".to_string(),
            None,
            vec![],
            vec![],
            vec![],
            vec![],
            None,
        );
        from_table.partition_key = Some("LIST (id)".to_string());

        let mut to_table = Table::new(
            "data".to_string(),
            "test".to_string(),
            "owner".to_string(),
            None,
            vec![],
            vec![],
            vec![],
            vec![],
            None,
        );
        to_table.partition_key = Some("LIST (flow_id)".to_string());

        let script = from_table.get_alter_script(&to_table, false);

        assert!(script.contains("Partition key changed"));
        assert!(script.contains("-- drop table"));
        assert!(script.contains("create table"));
    }

    #[test]
    fn test_get_alter_script_detach_partition() {
        let mut from_table = Table::new(
            "data".to_string(),
            "test_default".to_string(),
            "owner".to_string(),
            None,
            vec![],
            vec![],
            vec![],
            vec![],
            None,
        );
        from_table.partition_of = Some("\"data\".\"test\"".to_string());
        from_table.partition_bound = Some("DEFAULT".to_string());

        let to_table = Table::new(
            "data".to_string(),
            "test_default".to_string(),
            "owner".to_string(),
            None,
            vec![],
            vec![],
            vec![],
            vec![],
            None,
        );
        // to_table has no partition info, so it's a standalone table

        let script = from_table.get_alter_script(&to_table, true);

        assert!(
            script.contains(
                "alter table \"data\".\"test\" detach partition \"data\".\"test_default\";"
            )
        );
        assert!(!script.contains("attach partition"));
    }

    #[test]
    fn test_get_alter_script_attach_partition() {
        let from_table = Table::new(
            "data".to_string(),
            "test_default".to_string(),
            "owner".to_string(),
            None,
            vec![],
            vec![],
            vec![],
            vec![],
            None,
        );
        // from_table is standalone

        let mut to_table = Table::new(
            "data".to_string(),
            "test_default".to_string(),
            "owner".to_string(),
            None,
            vec![],
            vec![],
            vec![],
            vec![],
            None,
        );
        to_table.partition_of = Some("\"data\".\"test\"".to_string());
        to_table.partition_bound = Some("DEFAULT".to_string());

        let script = from_table.get_alter_script(&to_table, true);

        assert!(!script.contains("detach partition"));
        assert!(script.contains(
            "alter table \"data\".\"test\" attach partition \"data\".\"test_default\" DEFAULT;"
        ));
    }
}
