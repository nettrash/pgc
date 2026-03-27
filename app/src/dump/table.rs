use crate::dump::{
    table_column::TableColumn, table_constraint::TableConstraint, table_index::TableIndex,
    table_policy::TablePolicy, table_trigger::TableTrigger,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::{Error, PgPool, Row};

fn escape_single_quotes(value: &str) -> String {
    value.replace('\'', "''")
}

fn quote_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
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
    #[serde(default)]
    pub policies: Vec<TablePolicy>, // Row-level security policies
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
            policies: Vec::new(),
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
    ///
    /// All sub-queries (columns, indexes, constraints, triggers, policies,
    /// partition info, definition) are fired **concurrently** via
    /// `tokio::try_join!` so that the total wall-clock time is dominated by
    /// the single slowest query rather than the sum of all seven.
    ///
    /// `has_tabledef_fn` should be pre-checked once per dump run so we don't
    /// repeat the `pg_proc` lookup for every table.
    pub async fn fill(&mut self, pool: &PgPool, has_tabledef_fn: bool) -> Result<(), Error> {
        let schema = self.schema.clone();
        let name = self.name.clone();

        let (columns, indexes, constraints, triggers, policies_data, partition, definition) = tokio::try_join!(
            Self::fetch_columns(pool, &schema, &name),
            Self::fetch_indexes(pool, &schema, &name),
            Self::fetch_constraints(pool, &schema, &name),
            Self::fetch_triggers(pool, &schema, &name),
            Self::fetch_policies(pool, &schema, &name),
            Self::fetch_partition_info(pool, &schema, &name),
            Self::fetch_definition(pool, &schema, &name, has_tabledef_fn),
        )?;

        self.columns = columns;
        self.indexes = indexes;
        self.constraints = constraints;
        self.triggers = triggers;

        let (policies, rowsecurity) = policies_data;
        self.policies = policies;
        self.has_rowsecurity = rowsecurity;

        let (partition_key, partition_of, partition_bound) = partition;
        self.partition_key = partition_key;
        self.partition_of = partition_of;
        self.partition_bound = partition_bound;

        self.definition = definition;

        Ok(())
    }

    /// Fetch columns for the given table.
    async fn fetch_columns(
        pool: &PgPool,
        schema: &str,
        name: &str,
    ) -> Result<Vec<TableColumn>, Error> {
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
                                        SELECT string_agg(DISTINCT rel, ', ')
                                        FROM (
                                            SELECT quote_ident(v.view_schema) || '.' || quote_ident(v.view_name) AS rel
                                            FROM information_schema.view_column_usage v
                                            WHERE v.table_schema = c.table_schema
                                                AND v.table_name  = c.table_name
                                                AND v.column_name = c.column_name
                                            UNION ALL
                                            SELECT quote_ident(mn.nspname) || '.' || quote_ident(mc.relname) AS rel
                                            FROM pg_attribute  pa
                                            JOIN pg_class      tc  ON tc.oid           = pa.attrelid
                                            JOIN pg_namespace  tn  ON tn.oid           = tc.relnamespace
                                            JOIN pg_depend     dep ON dep.refobjid     = pa.attrelid
                                                                  AND dep.refobjsubid  = pa.attnum
                                                                  AND dep.deptype      = 'n'
                                            JOIN pg_class      mc  ON mc.oid = dep.objid AND mc.relkind = 'm'
                                            JOIN pg_namespace  mn  ON mn.oid = mc.relnamespace
                                            WHERE tn.nspname = c.table_schema
                                              AND tc.relname = c.table_name
                                              AND pa.attname = c.column_name
                                              AND pa.attnum  > 0
                                              AND pa.attisdropped = false
                                        ) sub
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
                        escape_single_quotes(schema),
                        escape_single_quotes(name)
                );
        let rows = sqlx::query(&query).fetch_all(pool).await?;

        let mut columns = Vec::new();
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
                    serial_type: None,
                };

                columns.push(table_column);
            }

            columns.sort_by(|a, b| a.ordinal_position.cmp(&b.ordinal_position));
        }

        Ok(columns)
    }

    /// Fetch indexes for the given table.
    async fn fetch_indexes(
        pool: &PgPool,
        schema: &str,
        name: &str,
    ) -> Result<Vec<TableIndex>, Error> {
        let query = format!(
                        "SELECT i.schemaname,
                                        i.tablename,
                                        i.indexname,
                                        i.tablespace,
                                        i.indexdef,
                                        EXISTS (SELECT 1 FROM pg_inherits inh WHERE inh.inhrelid = ic.oid) AS is_partition_index
                         FROM pg_indexes i
                         JOIN pg_class ic ON ic.relname = i.indexname
                         JOIN pg_namespace n ON n.oid = ic.relnamespace AND n.nspname = i.schemaname
                         JOIN pg_index idx ON idx.indexrelid = ic.oid
                         LEFT JOIN pg_constraint con ON con.conindid = ic.oid AND con.contype IN ('p', 'u')
                         WHERE idx.indisprimary = false
                             AND (idx.indisunique = false OR con.oid IS NULL)
                             AND i.schemaname = '{}' AND i.tablename = '{}'
                         ORDER BY i.schemaname, i.tablename, i.indexname",
                        escape_single_quotes(schema),
                        escape_single_quotes(name)
                );
        let rows = sqlx::query(&query).fetch_all(pool).await?;

        let mut indexes = Vec::new();
        if !rows.is_empty() {
            for row in rows {
                let table_index = TableIndex {
                    schema: row.get("schemaname"),
                    table: row.get("tablename"),
                    name: row.get("indexname"),
                    catalog: row.get("tablespace"),
                    indexdef: row.get("indexdef"),
                    is_partition_index: row.get("is_partition_index"),
                };

                indexes.push(table_index);
            }

            indexes.sort_by(|a, b| a.name.to_lowercase().cmp(&b.name.to_lowercase()));
        }

        Ok(indexes)
    }

    /// Fetch constraints for the given table.
    async fn fetch_constraints(
        pool: &PgPool,
        schema: &str,
        name: &str,
    ) -> Result<Vec<TableConstraint>, Error> {
        let query = format!(
            "SELECT current_database() AS catalog, n.nspname AS schema, c.conname AS constraint_name, t.relname AS table_name, CASE c.contype WHEN 'p' THEN 'PRIMARY KEY' WHEN 'f' THEN 'FOREIGN KEY' WHEN 'u' THEN 'UNIQUE' WHEN 'c' THEN 'CHECK' ELSE c.contype::text END AS constraint_type, c.condeferrable AS is_deferrable, c.condeferred AS initially_deferred, pg_get_constraintdef(c.oid, true) AS definition FROM pg_constraint c JOIN pg_class t ON t.oid = c.conrelid JOIN pg_namespace n ON n.oid = t.relnamespace WHERE n.nspname = '{}' AND t.relname = '{}' AND c.contype IN ('p','u','f','c') AND c.conislocal ORDER BY n.nspname, t.relname, c.conname;",
            escape_single_quotes(schema),
            escape_single_quotes(name)
        );

        let rows = sqlx::query(&query).fetch_all(pool).await?;

        let mut constraints = Vec::new();
        for row in rows {
            constraints.push(TableConstraint {
                catalog: row.get("catalog"),
                schema: row.get("schema"),
                name: row.get("constraint_name"),
                table_name: row.get("table_name"),
                constraint_type: row.get("constraint_type"),
                is_deferrable: row.get("is_deferrable"),
                initially_deferred: row.get("initially_deferred"),
                definition: row.get("definition"),
            });
        }

        Ok(constraints)
    }

    /// Fetch triggers for the given table.
    async fn fetch_triggers(
        pool: &PgPool,
        schema: &str,
        name: &str,
    ) -> Result<Vec<TableTrigger>, Error> {
        let query = format!(
            "SELECT *, pg_get_triggerdef(oid) as tgdef FROM pg_trigger WHERE tgrelid = '\"{}\".\"{}\"'::regclass and tgisinternal = false ORDER BY tgname",
            schema.replace('"', "\"\""),
            name.replace('"', "\"\"")
        );
        let rows = sqlx::query(&query).fetch_all(pool).await?;

        let mut triggers = Vec::new();
        for row in rows {
            triggers.push(TableTrigger {
                oid: row.get("oid"),
                name: row.get("tgname"),
                definition: row.get("tgdef"),
            });
        }
        triggers.sort_by(|a, b| a.name.to_lowercase().cmp(&b.name.to_lowercase()));

        Ok(triggers)
    }

    /// Fetch row-level security policies and the row security flag.
    async fn fetch_policies(
        pool: &PgPool,
        schema: &str,
        name: &str,
    ) -> Result<(Vec<TablePolicy>, bool), Error> {
        let query = format!(
            "SELECT p.polname,
                    n.nspname AS schemaname,
                    c.relname AS tablename,
                    p.polcmd::text AS polcmd,
                    p.polpermissive,
                    array(SELECT rolname::text FROM pg_roles r WHERE r.oid = ANY(p.polroles) ORDER BY rolname) AS roles,
                    pg_get_expr(p.polqual, p.polrelid) AS using_clause,
                    pg_get_expr(p.polwithcheck, p.polrelid) AS check_clause,
                    c.relrowsecurity
             FROM pg_policy p
             JOIN pg_class c ON c.oid = p.polrelid
             JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname = '{}' AND c.relname = '{}'
             ORDER BY p.polname",
            escape_single_quotes(schema),
            escape_single_quotes(name),
        );

        let rows = sqlx::query(&query).fetch_all(pool).await?;

        if !rows.is_empty() {
            let mut policies = Vec::new();
            for row in &rows {
                policies.push(TablePolicy::from_row(row)?);
            }
            let rowsecurity: bool = rows[0].get("relrowsecurity");
            Ok((policies, rowsecurity))
        } else {
            // No policies; still record whether row security is enabled on the table.
            let flag_query = format!(
                "SELECT relrowsecurity FROM pg_class c
                 JOIN pg_namespace n ON n.oid = c.relnamespace
                 WHERE n.nspname = '{}' AND c.relname = '{}';",
                escape_single_quotes(schema),
                escape_single_quotes(name),
            );

            let rowsecurity =
                if let Some(row) = sqlx::query(&flag_query).fetch_optional(pool).await? {
                    row.get("relrowsecurity")
                } else {
                    false
                };
            Ok((Vec::new(), rowsecurity))
        }
    }

    /// Fetch partition information for the given table.
    ///
    /// Returns (partition_key, partition_of, partition_bound).
    async fn fetch_partition_info(
        pool: &PgPool,
        schema: &str,
        name: &str,
    ) -> Result<(Option<String>, Option<String>, Option<String>), Error> {
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
            escape_single_quotes(schema),
            escape_single_quotes(name)
        );

        let row = sqlx::query(&query).fetch_optional(pool).await?;

        if let Some(row) = row {
            let relkind: String = row.get("relkind");

            let partition_key = if relkind == "p" {
                row.get("partition_key")
            } else {
                None
            };

            let (partition_of, partition_bound) = if let Some(parent_table) =
                row.get::<Option<String>, _>("parent_table")
                && let Some(parent_schema) = row.get::<Option<String>, _>("parent_schema")
            {
                (
                    Some(format!("\"{}\".\"{}\"", parent_schema, parent_table)),
                    row.get("partition_bound"),
                )
            } else {
                (None, None)
            };

            Ok((partition_key, partition_of, partition_bound))
        } else {
            Ok((None, None, None))
        }
    }

    /// Fetch the table definition using `pg_get_tabledef` if available.
    ///
    /// The `has_tabledef_fn` flag should be pre-checked once per dump run to
    /// avoid repeating the `pg_proc` lookup for every table.
    async fn fetch_definition(
        pool: &PgPool,
        schema: &str,
        name: &str,
        has_tabledef_fn: bool,
    ) -> Result<Option<String>, Error> {
        if has_tabledef_fn {
            let query = format!(
                "select pg_get_tabledef(oid) AS definition from pg_class where relname = '{}' AND relnamespace = '\"{}\"'::regnamespace;",
                escape_single_quotes(name),
                schema.replace('"', "\"\"")
            );
            let row = sqlx::query(&query).fetch_one(pool).await?;
            Ok(row.get::<Option<String>, _>("definition"))
        } else {
            Ok(None)
        }
    }

    /// Hash the table
    pub fn hash(&mut self) {
        let mut hasher = Sha256::new();
        hasher.update(self.schema.as_bytes());
        hasher.update(self.name.as_bytes());
        hasher.update(self.owner.as_bytes());
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

        for policy in &self.policies {
            policy.add_to_hasher(&mut hasher);
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
        if let Some(sp) = &self.space {
            hasher.update(sp.as_bytes());
        }
        if let Some(cmt) = &self.comment {
            hasher.update(cmt.as_bytes());
        }

        self.hash = Some(format!("{:x}", hasher.finalize()));
    }

    fn build_script(&self, include_triggers: bool) -> String {
        let mut script = String::new();

        if let Some(parent) = &self.partition_of {
            script.push_str(&format!(
                "create table \"{}\".\"{}\" partition of {}",
                self.schema, self.name, parent
            ));
            if let Some(bound) = &self.partition_bound {
                script.push_str(&format!("\n    {}", bound));
            }

            // A sub-partition is itself partitioned — emit PARTITION BY before tablespace/semicolon
            if let Some(partition_key) = &self.partition_key {
                script.push_str(&format!("\npartition by {}", partition_key));
            }

            if let Some(space) = &self.space {
                script.push_str(&format!("\ntablespace {}", quote_ident(space)));
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
            let pk_constraint = self
                .constraints
                .iter()
                .find(|c| c.constraint_type.eq_ignore_ascii_case("primary key"));
            let has_pk_constraint = pk_constraint.is_some();
            let pk_constraint_name = pk_constraint
                .map(|constraint| quote_ident(&constraint.name))
                .unwrap_or_default();

            if has_pk_constraint {
                let mut pk_added = false;
                // Prefer PK columns from indexes if available (preserves order expressions)
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
                                    let pk_def = if pk_constraint_name.is_empty() {
                                        format!(
                                            "    primary key ({})",
                                            pk_cols
                                                .iter()
                                                .map(|c| format!("\"{c}\""))
                                                .collect::<Vec<_>>()
                                                .join(", ")
                                        )
                                    } else {
                                        format!(
                                            "    constraint {} primary key ({})",
                                            pk_constraint_name,
                                            pk_cols
                                                .iter()
                                                .map(|c| format!("\"{c}\""))
                                                .collect::<Vec<_>>()
                                                .join(", ")
                                        )
                                    };
                                    column_definitions.push(pk_def);
                                    pk_added = true;
                                }
                            }
                        }
                        break;
                    }
                }

                // Fallback: parse PK constraint definition if no index info was found
                if !pk_added
                    && let Some(pk_constraint) = self
                        .constraints
                        .iter()
                        .find(|c| c.constraint_type.eq_ignore_ascii_case("primary key"))
                        .and_then(|c| c.definition.as_deref())
                    && let Some(start) = pk_constraint.find('(')
                    && let Some(end) = pk_constraint[start + 1..].find(')')
                {
                    let cols_part = &pk_constraint[start + 1..start + 1 + end];
                    let pk_cols: Vec<&str> = cols_part
                        .split(',')
                        .map(|c| c.trim().trim_matches('"'))
                        .collect();
                    if !pk_cols.is_empty() {
                        let pk_def = if pk_constraint_name.is_empty() {
                            format!(
                                "    primary key ({})",
                                pk_cols
                                    .iter()
                                    .map(|c| format!("\"{c}\""))
                                    .collect::<Vec<_>>()
                                    .join(", ")
                            )
                        } else {
                            format!(
                                "    constraint {} primary key ({})",
                                pk_constraint_name,
                                pk_cols
                                    .iter()
                                    .map(|c| format!("\"{c}\""))
                                    .collect::<Vec<_>>()
                                    .join(", ")
                            )
                        };
                        column_definitions.push(pk_def);
                    }
                }
            }

            // Join all column definitions
            script.push_str(&column_definitions.join(",\n"));
            script.push_str("\n)");

            if let Some(partition_key) = &self.partition_key {
                script.push_str(&format!("\npartition by {}", partition_key));
            }

            if let Some(space) = &self.space {
                script.push_str(&format!("\ntablespace {}", quote_ident(space)));
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

        // 6. Add indexes (excluding primary key indexes and partition-inherited indexes)
        for index in &self.indexes {
            if !index.indexdef.to_lowercase().contains("primary key") && !index.is_partition_index {
                script.push_str(&index.get_script());
            }
        }

        // 7. Add triggers
        if include_triggers {
            for trigger in &self.triggers {
                script.push_str(&trigger.get_script());
            }
        }

        // 8. Enable row-level security before creating policies
        if self.has_rowsecurity {
            script.push_str(&format!(
                "alter table \"{}\".\"{}\" enable row level security;\n",
                self.schema, self.name
            ));
        }

        // 9. Add row-level security policies
        for policy in &self.policies {
            script.push_str(&policy.get_script());
        }

        // 10. Add table comment (if any) and column comments
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

        script.push_str(&self.get_owner_script());

        script
    }

    /// Get script for the table
    pub fn get_script(&self) -> String {
        self.build_script(true)
    }

    /// Get script for the table without triggers (for deferred trigger creation)
    pub fn get_script_without_triggers(&self) -> String {
        self.build_script(false)
    }

    /// Get trigger creation scripts only
    pub fn get_trigger_script(&self) -> String {
        let mut script = String::new();
        for trigger in &self.triggers {
            script.push_str(&trigger.get_script());
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

    pub fn get_owner_script(&self) -> String {
        if self.owner.is_empty() {
            return String::new();
        }

        format!(
            "alter table \"{}\".\"{}\" owner to {};\n",
            self.schema,
            self.name,
            quote_ident(&self.owner)
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

    fn build_alter_script(
        &self,
        to_table: &Table,
        use_drop: bool,
        include_triggers: bool,
    ) -> String {
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
        let mut index_drop_script = String::new();
        let mut partition_script = String::new();
        let mut policy_script = String::new();
        let mut policy_drop_script = String::new();
        let mut row_security_script = String::new();

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
                if old_col != new_col {
                    let type_changed = old_col.data_type != new_col.data_type
                        || old_col.udt_name != new_col.udt_name
                        || old_col.numeric_precision != new_col.numeric_precision
                        || old_col.numeric_scale != new_col.numeric_scale
                        || old_col.character_maximum_length != new_col.character_maximum_length;

                    let is_partition_child = self.partition_of.is_some();
                    let in_partition_key = self.partition_key.as_ref().is_some_and(|pk| {
                        let pk_norm: String = pk
                            .chars()
                            .filter(|c| !c.is_whitespace() && !matches!(c, '"' | '\'' | '`'))
                            .collect::<String>()
                            .to_lowercase();
                        pk_norm.contains(&new_col.name.to_lowercase())
                    });

                    if type_changed && (is_partition_child || in_partition_key) {
                        let drop_script = if use_drop {
                            self.get_drop_script()
                        } else {
                            self.get_drop_script()
                                .lines()
                                .map(|l| format!("-- {}\n", l))
                                .collect()
                        };
                        return format!(
                            "/* Column {} participates in partitioning; type change requires table recreation. Data loss will occur! */\n{}{}",
                            new_col.name,
                            drop_script,
                            to_table.get_script()
                        );
                    }

                    if let Some(alter_col_script) = new_col.get_alter_script(old_col, use_drop) {
                        column_alter_script.push_str(&alter_col_script);
                    }
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

        // Collect index updates (skip partition-inherited indexes, managed by parent)
        for new_index in &to_table.indexes {
            if new_index.is_partition_index {
                continue;
            }
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

        // Collect policy updates
        for new_policy in &to_table.policies {
            if let Some(old_policy) = self.policies.iter().find(|p| p.name == new_policy.name) {
                if old_policy != new_policy {
                    let drop_cmd = format!(
                        "drop policy if exists \"{}\" on \"{}\".\"{}\";\n",
                        old_policy.name.replace('"', "\"\""),
                        self.schema,
                        self.name
                    );
                    if use_drop {
                        policy_drop_script.push_str(&drop_cmd);
                    } else {
                        policy_drop_script.push_str(&format!("-- {}", drop_cmd));
                    }
                    policy_script.push_str(&new_policy.get_script());
                }
            } else {
                policy_script.push_str(&new_policy.get_script());
            }
        }

        for old_index in &self.indexes {
            if old_index.is_partition_index {
                continue;
            }
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

        for old_policy in &self.policies {
            if !to_table.policies.iter().any(|p| p.name == old_policy.name) {
                let drop_cmd = format!(
                    "drop policy if exists \"{}\" on \"{}\".\"{}\";\n",
                    old_policy.name, self.schema, self.name
                );
                if use_drop {
                    policy_drop_script.push_str(&drop_cmd);
                } else {
                    policy_drop_script.push_str(&format!("-- {}", drop_cmd));
                }
            }
        }

        if self.has_rowsecurity != to_table.has_rowsecurity {
            let stmt = if to_table.has_rowsecurity {
                format!(
                    "alter table \"{}\".\"{}\" enable row level security;\n",
                    self.schema, self.name
                )
            } else {
                format!(
                    "alter table \"{}\".\"{}\" disable row level security;\n",
                    self.schema, self.name
                )
            };
            row_security_script.push_str(&stmt);
        }

        let (trigger_drop_script, trigger_script) = if include_triggers {
            self.get_trigger_alter_parts(to_table, use_drop)
        } else {
            (String::new(), String::new())
        };

        let mut script = String::new();
        script.push_str(&partition_script);
        script.push_str(&constraint_pre_script);
        script.push_str(&column_alter_script);
        script.push_str(&index_drop_script);
        if include_triggers {
            script.push_str(&trigger_drop_script);
        }
        script.push_str(&policy_drop_script);
        script.push_str(&column_drop_script);
        script.push_str(&constraint_post_script);
        script.push_str(&index_script);
        if include_triggers {
            script.push_str(&trigger_script);
        }

        // When enabling row security, PostgreSQL requires row security to be
        // enabled before policies are created. Adjust the order accordingly.
        if !self.has_rowsecurity && to_table.has_rowsecurity {
            script.push_str(&row_security_script);
            script.push_str(&policy_script);
        } else {
            script.push_str(&policy_script);
            script.push_str(&row_security_script);
        }

        if self.owner != to_table.owner {
            script.push_str(&to_table.get_owner_script());
        }

        if self.space != to_table.space
            && let Some(new_space) = &to_table.space
        {
            script.push_str(&format!(
                "alter table \"{}\".\"{}\" set tablespace {};\n",
                to_table.schema,
                to_table.name,
                quote_ident(new_space)
            ));
        }

        script
    }

    /// Get script for altering the table (including triggers)
    pub fn get_alter_script(&self, to_table: &Table, use_drop: bool) -> String {
        self.build_alter_script(to_table, use_drop, true)
    }

    /// Get script for altering the table without triggers (for deferred trigger creation)
    pub fn get_alter_script_without_triggers(&self, to_table: &Table, use_drop: bool) -> String {
        self.build_alter_script(to_table, use_drop, false)
    }

    fn get_trigger_alter_parts(&self, to_table: &Table, use_drop: bool) -> (String, String) {
        let mut trigger_script = String::new();
        let mut trigger_drop_script = String::new();

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

        (trigger_drop_script, trigger_script)
    }

    /// Trigger-only alter script
    pub fn get_trigger_alter_script(&self, to_table: &Table, use_drop: bool) -> String {
        let (drop_part, create_part) = self.get_trigger_alter_parts(to_table, use_drop);
        format!("{}{}", drop_part, create_part)
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
            serial_type: None,
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
            is_partition_index: false,
        }
    }

    fn name_index(definition: &str) -> TableIndex {
        TableIndex {
            schema: "public".to_string(),
            table: "users".to_string(),
            name: "idx_users_name".to_string(),
            catalog: None,
            indexdef: definition.to_string(),
            is_partition_index: false,
        }
    }

    fn legacy_index() -> TableIndex {
        TableIndex {
            schema: "public".to_string(),
            table: "users".to_string(),
            name: "idx_users_old".to_string(),
            catalog: None,
            indexdef: "create index idx_users_old on public.users using btree (legacy)".to_string(),
            is_partition_index: false,
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
            is_partition_index: false,
        }
    }

    fn unique_email_index() -> TableIndex {
        TableIndex {
            schema: "public".to_string(),
            table: "users".to_string(),
            name: "idx_users_email".to_string(),
            catalog: None,
            indexdef: "create unique index idx_users_email on public.users using btree (email)"
                .to_string(),
            is_partition_index: false,
        }
    }

    fn trigger(name: &str, definition: &str, oid: u32) -> TableTrigger {
        TableTrigger {
            oid: Oid(oid),
            name: name.to_string(),
            definition: definition.to_string(),
        }
    }

    fn policy(
        name: &str,
        command: &str,
        using_clause: Option<&str>,
        check_clause: Option<&str>,
    ) -> TablePolicy {
        TablePolicy {
            schema: "public".to_string(),
            table: "users".to_string(),
            name: name.to_string(),
            command: command.to_string(),
            permissive: true,
            roles: vec!["public".to_string()],
            using_clause: using_clause.map(|c| c.to_string()),
            check_clause: check_clause.map(|c| c.to_string()),
        }
    }

    #[test]
    fn test_escape_single_quotes() {
        let input = "O'Reilly";
        let escaped = super::escape_single_quotes(input);
        assert_eq!(escaped, "O''Reilly");
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
    fn test_table_hash_changes_with_policy() {
        let mut table = basic_table();
        table.hash();
        let original_hash = table.hash.clone();

        table.policies = vec![policy(
            "users_rls",
            "select",
            Some("tenant_id = current_setting('app.current_tenant')::int"),
            None,
        )];
        table.has_rowsecurity = true;
        table.hash();

        assert_ne!(original_hash, table.hash);
    }

    #[test]
    fn test_get_script_generates_full_definition() {
        let table = basic_table();

        let script = table.get_script();

        let expected = concat!(
            "create table \"public\".\"users\" (\n",
            "    \"id\" integer generated BY DEFAULT as identity not null,\n",
            "    \"name\" text not null,\n",
            "    constraint \"users_pkey\" primary key (\"id\")\n",
            ")\n",
            "tablespace \"pg_default\";\n\n",
            "alter table \"public\".\"users\" add constraint \"users_name_check\" check (name <> '') ;\n",
            "create index idx_users_name on public.users using btree (name);\n",
            "create trigger audit_user before insert on public.users for each row execute function log_user();\n",
            "alter table \"public\".\"users\" owner to \"postgres\";\n",
        );

        assert_eq!(script, expected);
    }

    #[test]
    fn test_get_script_includes_policies_and_row_security() {
        let mut table = basic_table();
        table.policies = vec![policy(
            "users_tenant_select",
            "select",
            Some("tenant_id = current_setting('app.current_tenant')::int"),
            None,
        )];
        table.has_rowsecurity = true;

        let script = table.get_script();

        assert!(script.contains("create policy \"users_tenant_select\""));
        assert!(script.contains("for select"));
        assert!(script.contains("enable row level security"));
    }

    #[test]
    fn test_get_script_includes_unique_indexes() {
        let table = Table::new(
            "public".to_string(),
            "users".to_string(),
            "postgres".to_string(),
            Some("pg_default".to_string()),
            vec![identity_column("id", 1, "integer")],
            vec![],
            vec![unique_email_index()],
            vec![],
            None,
        );

        let script = table.get_script();

        assert!(
            script.contains(
                "create unique index idx_users_email on public.users using btree (email);"
            )
        );
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
    fn test_get_alter_script_handles_policy_changes() {
        let mut from_table = basic_table();
        let mut to_table = basic_table();

        to_table.policies = vec![policy(
            "users_tenant_insert",
            "insert",
            None,
            Some("tenant_id = current_setting('app.current_tenant')::int"),
        )];
        to_table.has_rowsecurity = true;

        let add_script = from_table.get_alter_script(&to_table, true);
        assert!(add_script.contains("create policy \"users_tenant_insert\""));
        assert!(add_script.contains("enable row level security"));

        from_table = to_table.clone();
        let to_table_no_policy = basic_table();
        let drop_script = from_table.get_alter_script(&to_table_no_policy, true);
        assert!(drop_script.contains("drop policy if exists \"users_tenant_insert\""));
        assert!(drop_script.contains("disable row level security"));
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
            serial_type: None,
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
    fn test_sub_partition_script() {
        // A sub-partition is both a child of a partitioned table AND itself partitioned.
        let mut table = Table::new(
            "data".to_string(),
            "test_2023".to_string(),
            "owner".to_string(),
            None,
            vec![create_dummy_column("id", "bigint")],
            vec![],
            vec![],
            vec![],
            None,
        );
        table.partition_of = Some("\"data\".\"test\"".to_string());
        table.partition_bound = Some("FOR VALUES FROM (2023) TO (2024)".to_string());
        table.partition_key = Some("LIST (id)".to_string());

        let script = table.get_script();
        assert!(
            script.contains("create table \"data\".\"test_2023\" partition of \"data\".\"test\""),
            "should reference parent table"
        );
        assert!(
            script.contains("FOR VALUES FROM (2023) TO (2024)"),
            "should contain partition bound"
        );
        assert!(
            script.contains("partition by LIST (id)"),
            "should contain sub-partition key"
        );
    }

    #[test]
    fn test_partition_child_with_tablespace() {
        let mut table = Table::new(
            "data".to_string(),
            "test_default".to_string(),
            "owner".to_string(),
            Some("fast_ssd".to_string()),
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
            script.contains("tablespace \"fast_ssd\""),
            "partition create should include tablespace"
        );
    }

    #[test]
    fn test_regular_table_with_tablespace() {
        let table = Table::new(
            "data".to_string(),
            "test".to_string(),
            "owner".to_string(),
            Some("fast_ssd".to_string()),
            vec![create_dummy_column("id", "bigint")],
            vec![],
            vec![],
            vec![],
            None,
        );

        let script = table.get_script();
        assert!(
            script.contains("tablespace \"fast_ssd\""),
            "regular table create should include tablespace"
        );
    }

    #[test]
    fn test_get_alter_script_tablespace_change() {
        let from_table = Table::new(
            "data".to_string(),
            "test".to_string(),
            "owner".to_string(),
            Some("old_space".to_string()),
            vec![],
            vec![],
            vec![],
            vec![],
            None,
        );

        let to_table = Table::new(
            "data".to_string(),
            "test".to_string(),
            "owner".to_string(),
            Some("new_space".to_string()),
            vec![],
            vec![],
            vec![],
            vec![],
            None,
        );

        let script = from_table.get_alter_script(&to_table, true);
        assert!(
            script.contains("set tablespace \"new_space\""),
            "alter script should set new tablespace"
        );
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
