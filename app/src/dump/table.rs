use crate::{
    dump::{
        table_column::TableColumn, table_constraint::TableConstraint, table_index::TableIndex,
        table_policy::TablePolicy, table_trigger::TableTrigger,
    },
    utils::string_extensions::StringExt,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::{Error, PgPool, Row};
use std::collections::HashMap;

fn escape_single_quotes(value: &str) -> String {
    value.replace('\'', "''")
}

fn quote_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

/// Extract column identifiers from a `pg_get_partkeydef` output string.
///
/// The input format is `method (ident1, ident2, ...)` where `method` is one of
/// `range`, `list`, or `hash`.  Identifiers may be double-quoted (PostgreSQL
/// style) and unquoted identifiers may contain `$` in addition to alphanumeric
/// characters and underscores.
///
/// Returns lowercased identifier names (with quotes stripped for quoted idents).
fn extract_partition_key_identifiers(pk: &str) -> Vec<String> {
    // Find the content inside the outermost parentheses, skipping the method keyword.
    let start = match pk.find('(') {
        Some(i) => i + 1,
        None => return Vec::new(),
    };
    let end = match pk.rfind(')') {
        Some(i) => i,
        None => pk.len(),
    };
    if start >= end {
        return Vec::new();
    }
    let inner = &pk[start..end];

    let mut identifiers = Vec::new();
    for part in inner.split(',') {
        let trimmed = part.trim();
        if trimmed.is_empty() {
            continue;
        }
        if trimmed.starts_with('"') {
            // Quoted identifier: find matching close quote (doubled quotes "" are escapes).
            let bytes = trimmed.as_bytes();
            let mut i = 1; // skip opening quote
            let mut ident = String::new();
            while i < bytes.len() {
                if bytes[i] == b'"' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                        ident.push('"');
                        i += 2;
                    } else {
                        break; // closing quote
                    }
                } else {
                    ident.push(bytes[i] as char);
                    i += 1;
                }
            }
            identifiers.push(ident.to_lowercase());
        } else {
            // Unquoted identifier: take leading identifier chars (alphanumeric, _, $).
            let ident: String = trimmed
                .chars()
                .take_while(|c| c.is_alphanumeric() || *c == '_' || *c == '$')
                .collect();
            if !ident.is_empty() {
                identifiers.push(ident.to_lowercase());
            }
        }
    }
    identifiers
}

/// Pre-computed catalog capability flags.
///
/// These booleans indicate whether certain columns / features exist in the
/// connected PostgreSQL server's system catalogs.  They are probed **once**
/// per dump run (via [`PgCatalogCaps::detect`]) and then threaded into every
/// per-table fetch function, avoiding a repeated `EXISTS` query per table.
#[derive(Debug, Clone, Copy)]
pub struct PgCatalogCaps {
    /// pg_attribute.attcompression exists (PG 14+)
    pub has_attcompression: bool,
    /// pg_constraint.conenforced exists (PG 18+)
    pub has_conenforced: bool,
    /// pg_constraint.connullsnotdistinct exists (PG 15+)
    pub has_connullsnotdistinct: bool,
}

impl PgCatalogCaps {
    /// Probe the catalog once and return the capability flags.
    pub async fn detect(pool: &PgPool, pg_version: i32) -> Self {
        let has_attcompression = pg_version >= 140000
            && sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = 'pg_attribute'::regclass AND attname = 'attcompression' AND NOT attisdropped)",
            )
            .fetch_one(pool)
            .await
            .unwrap_or(false);

        let has_conenforced = pg_version >= 180000
            && sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = 'pg_constraint'::regclass AND attname = 'conenforced' AND NOT attisdropped)",
            )
            .fetch_one(pool)
            .await
            .unwrap_or(false);

        let has_connullsnotdistinct = pg_version >= 150000
            && sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = 'pg_constraint'::regclass AND attname = 'connullsnotdistinct' AND NOT attisdropped)",
            )
            .fetch_one(pool)
            .await
            .unwrap_or(false);

        Self {
            has_attcompression,
            has_conenforced,
            has_connullsnotdistinct,
        }
    }
}

// This is an information about a PostgreSQL table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Table {
    pub schema: String,
    pub name: String,
    pub raw_schema: String,
    pub raw_name: String,
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
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub acl: Vec<String>, // ACL (grant) entries for this table
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub access_method: Option<String>, // Table access method (e.g., "heap", custom AM)
    #[serde(default)]
    pub is_unlogged: bool, // Whether the table is UNLOGGED (relpersistence = 'u')
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_parameters: Option<Vec<String>>, // Table-level storage parameters (reloptions)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replica_identity: Option<String>, // REPLICA IDENTITY setting (d=default, n=nothing, f=full, i=index)
    #[serde(default)]
    pub force_rowsecurity: bool, // Whether FORCE ROW LEVEL SECURITY is enabled
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub inherits_from: Vec<String>, // Classical inheritance parents (non-partition)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub typed_table_type: Option<String>, // OF type name for typed tables
}

impl Table {
    /// Creates a new Table with the given name
    #[allow(clippy::too_many_arguments)] // Table metadata naturally includes these fields (from pg_class and related catalogs).
    pub fn new(
        schema: String,
        name: String,
        raw_schema: String,
        raw_name: String,
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
            raw_schema,
            raw_name,
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
            acl: Vec::new(),
            access_method: None,
            is_unlogged: false,
            storage_parameters: None,
            replica_identity: None,
            force_rowsecurity: false,
            inherits_from: Vec::new(),
            typed_table_type: None,
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
    ///
    /// `caps` carries pre-computed catalog capability flags so that per-table
    /// fetches never issue redundant `EXISTS` probes.
    pub async fn fill(
        &mut self,
        pool: &PgPool,
        has_tabledef_fn: bool,
        pg_version: i32,
        caps: PgCatalogCaps,
    ) -> Result<(), Error> {
        let raw_schema = self.raw_schema.clone();
        let raw_name = self.raw_name.clone();

        let (columns, indexes, constraints, triggers, policies_data, partition, definition) = tokio::try_join!(
            Self::fetch_columns(pool, &raw_schema, &raw_name, caps),
            Self::fetch_indexes(pool, &raw_schema, &raw_name),
            Self::fetch_constraints(pool, &raw_schema, &raw_name, pg_version, caps),
            Self::fetch_triggers(pool, &raw_schema, &raw_name),
            Self::fetch_policies(pool, &raw_schema, &raw_name),
            Self::fetch_partition_info(pool, &raw_schema, &raw_name),
            Self::fetch_definition(pool, &raw_schema, &raw_name, has_tabledef_fn),
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
        caps: PgCatalogCaps,
    ) -> Result<Vec<TableColumn>, Error> {
        let compression_col = if caps.has_attcompression {
            ",\n                                a.attcompression::text as col_compression"
        } else {
            ""
        };
        let query = format!(
                        "SELECT
                                c.table_catalog,
                                quote_ident(c.table_schema) as table_schema,
                                quote_ident(c.table_name) as table_name,
                                quote_ident(c.column_name) as column_name,
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
                                a.attgenerated::text as attgenerated,
                                a.attstorage::text as col_storage,
                                a.attstattarget as col_stattarget,
                                c.is_updatable,
                                pd.description as column_comment{compression_col},
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
                    generation_type: {
                        let ag: String = row.get("attgenerated");
                        if ag.is_empty() { None } else { Some(ag) }
                    },
                    is_updatable: row.get::<&str, _>("is_updatable") == "YES", // Convert to boolean
                    related_views: row.get::<Option<String>, _>("related_views").map(|s| {
                        let mut views: Vec<String> =
                            s.split(',').map(|v| v.trim().to_string()).collect();
                        views.sort_unstable();
                        views
                    }),
                    comment: row.get("column_comment"),
                    storage: {
                        let s: String = row.get("col_storage");
                        match s.as_str() {
                            "p" => Some("PLAIN".to_string()),
                            "e" => Some("EXTERNAL".to_string()),
                            "m" => Some("MAIN".to_string()),
                            "x" => Some("EXTENDED".to_string()),
                            _ => None,
                        }
                    },
                    compression: if caps.has_attcompression {
                        let c: String = row.get("col_compression");
                        match c.as_str() {
                            "p" => Some("pglz".to_string()),
                            "l" => Some("lz4".to_string()),
                            _ => None,
                        }
                    } else {
                        None
                    },
                    statistics_target: {
                        let st: Option<i16> = row.get("col_stattarget");
                        st.filter(|&v| v >= 0).map(|v| v as i32)
                    },
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
                        "SELECT
                                quote_ident(i.schemaname) as schemaname,
                                quote_ident(i.tablename) as tablename,
                                quote_ident(i.indexname) as indexname,
                                i.tablespace,
                                i.indexdef,
                                EXISTS (SELECT 1 FROM pg_inherits inh WHERE inh.inhrelid = ic.oid) AS is_partition_index
                         FROM pg_indexes i
                         JOIN pg_class ic ON ic.relname = i.indexname
                         JOIN pg_namespace n ON n.oid = ic.relnamespace AND n.nspname = i.schemaname
                         JOIN pg_index idx ON idx.indexrelid = ic.oid
                         LEFT JOIN pg_constraint puc ON puc.conindid = ic.oid AND puc.contype IN ('p', 'u')
                         WHERE idx.indisprimary = false
                             AND (idx.indisunique = false OR puc.oid IS NULL)
                             AND NOT EXISTS (SELECT 1 FROM pg_constraint xc WHERE xc.conindid = ic.oid AND xc.contype = 'x')
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
        pg_version: i32,
        caps: PgCatalogCaps,
    ) -> Result<Vec<TableConstraint>, Error> {
        let conenforced_col = if caps.has_conenforced {
            ",\n                c.conenforced AS is_enforced"
        } else {
            ""
        };
        let connullsnotdistinct_col = if caps.has_connullsnotdistinct {
            ",\n                coalesce(c.connullsnotdistinct, false) AS nulls_not_distinct"
        } else {
            ""
        };
        // PG18 stores named NOT NULL constraints in pg_constraint as contype = 'n'.
        let contype_filter = if pg_version >= 180000 {
            "('p','u','f','c','x','n')"
        } else {
            "('p','u','f','c','x')"
        };
        let query = format!(
            "SELECT
                current_database() AS catalog,
                quote_ident(n.nspname) AS schema,
                quote_ident(c.conname) AS constraint_name,
                quote_ident(t.relname) AS table_name,
                CASE c.contype
                    WHEN 'p' THEN 'PRIMARY KEY'
                    WHEN 'f' THEN 'FOREIGN KEY'
                    WHEN 'u' THEN 'UNIQUE'
                    WHEN 'c' THEN 'CHECK'
                    WHEN 'x' THEN 'EXCLUDE'
                    WHEN 'n' THEN 'NOT NULL'
                    ELSE c.contype::text
                END AS constraint_type,
                c.condeferrable AS is_deferrable,
                c.condeferred AS initially_deferred,
                pg_get_constraintdef(c.oid, true) AS definition,
                c.coninhcount::int4 AS coninhcount,\n                c.connoinherit AS no_inherit{conenforced_col}{connullsnotdistinct_col}
            FROM
                pg_constraint c
                JOIN pg_class t ON t.oid = c.conrelid
                JOIN pg_namespace n ON n.oid = t.relnamespace
                WHERE
                    n.nspname = '{}' AND
                    t.relname = '{}' AND
                    c.contype IN {contype_filter} AND
                    c.conislocal
                ORDER BY
                    n.nspname,
                    t.relname,
                    c.conname;",
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
                coninhcount: row.get("coninhcount"),
                is_enforced: row.try_get("is_enforced").unwrap_or(true),
                no_inherit: row.get("no_inherit"),
                nulls_not_distinct: row.try_get("nulls_not_distinct").unwrap_or(false),
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
            "select
                oid,
                quote_ident(tgname) as tgname,
                pg_get_triggerdef(oid) as tgdef
            from
                pg_trigger
            where
                tgrelid = format('%I.%I', '{}', '{}')::regclass and
                tgisinternal = false
            order by
                tgname",
            escape_single_quotes(schema),
            escape_single_quotes(name)
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
            "SELECT
                    quote_ident(p.polname) as polname,
                    quote_ident(n.nspname) AS schemaname,
                    quote_ident(c.relname) AS tablename,
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
                quote_ident(p.relname) AS parent_table,
                quote_ident(pn.nspname) AS parent_schema
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
                    Some(format!("{}.{}", parent_schema, parent_table)),
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
                "select
                    pg_get_tabledef(oid) AS definition
                from
                    pg_class
                where
                    relname = '{}' and
                    relnamespace = format('%I', '{}')::regnamespace;",
                escape_single_quotes(name),
                escape_single_quotes(schema)
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
        if let Some(am) = &self.access_method {
            hasher.update(am.as_bytes());
        }
        hasher.update(self.is_unlogged.to_string().as_bytes());
        if let Some(params) = &self.storage_parameters {
            for p in params {
                hasher.update(p.as_bytes());
            }
        }
        if let Some(ri) = &self.replica_identity {
            hasher.update(ri.as_bytes());
        }
        hasher.update(self.force_rowsecurity.to_string().as_bytes());
        for parent in &self.inherits_from {
            hasher.update(parent.as_bytes());
        }
        if let Some(tt) = &self.typed_table_type {
            hasher.update(tt.as_bytes());
        }

        self.hash = Some(format!("{:x}", hasher.finalize()));
    }

    fn build_script(&self, include_triggers: bool) -> String {
        let mut script = String::new();

        let unlogged_prefix = if self.is_unlogged { "unlogged " } else { "" };

        if let Some(parent) = &self.partition_of {
            script.push_str(&format!(
                "create {}table {}.{} partition of {}",
                unlogged_prefix, self.schema, self.name, parent
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

            script.append_block(";");
        } else {
            // 1. Build CREATE TABLE statement
            script.push_str(&format!(
                "create {}table {}.{}",
                unlogged_prefix, self.schema, self.name
            ));

            // OF type for typed tables
            if let Some(ref type_name) = self.typed_table_type {
                script.push_str(&format!(" of {} (\n", type_name));
            } else {
                script.push_str(" (\n");
            }

            // 2. Add column definitions
            // Build a map from column name (lowered) to named NOT NULL constraint.
            // A NOT NULL constraint is "named" if its name differs from PG's
            // auto-generated default "{table}_{col}_not_null".
            let named_nn_constraints: HashMap<String, &TableConstraint> = self
                .constraints
                .iter()
                .filter(|c| c.constraint_type.eq_ignore_ascii_case("not null"))
                .filter_map(|c| {
                    let def = c.definition.as_deref()?;
                    let def_lower = def.to_lowercase();
                    let col_name = def_lower.strip_prefix("not null ")?.trim().to_string();
                    let raw_cname = c.name.trim_matches('"');
                    let raw_col = col_name.trim_matches('"');
                    let default_name = format!("{}_{}_not_null", self.raw_name, raw_col);
                    if !raw_cname.eq_ignore_ascii_case(&default_name) {
                        Some((col_name, c))
                    } else {
                        None
                    }
                })
                .collect();

            let mut column_definitions = Vec::new();
            for column in &self.columns {
                let mut col_def = String::new();

                // Column name
                col_def.push_str(&format!("    {} ", column.name));

                // Use standard column definition
                let col_script = column.get_script();
                // Extract just the type and constraints part (skip the quoted name)
                if let Some(type_start) = col_script.find(' ') {
                    let mut col_part = col_script[type_start + 1..].to_string();

                    // If there is a named NOT NULL constraint for this column,
                    // replace the plain "not null" with "constraint <name> not null"
                    // (plus any modifier flags like NO INHERIT / NOT ENFORCED).
                    if let Some(nn) = named_nn_constraints.get(&column.name.to_lowercase())
                        && col_part.ends_with("not null")
                    {
                        col_part.truncate(col_part.len() - "not null".len());
                        let mut nn_clause = format!("constraint {} not null", nn.name);
                        if nn.no_inherit {
                            nn_clause.push_str(" no inherit");
                        }
                        if !nn.is_enforced {
                            nn_clause.push_str(" not enforced");
                        }
                        col_part.push_str(&nn_clause);
                    }

                    col_def.push_str(&col_part);
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
                .map(|constraint| constraint.name.clone())
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
                    // For temporal PKs with WITHOUT OVERLAPS, preserve the raw definition
                    if cols_part.to_lowercase().contains("without overlaps") {
                        let pk_def = if pk_constraint_name.is_empty() {
                            format!("    primary key ({cols_part})")
                        } else {
                            format!(
                                "    constraint {} primary key ({cols_part})",
                                pk_constraint_name
                            )
                        };
                        column_definitions.push(pk_def);
                    } else {
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
            }

            // Join all column definitions
            script.push_str(&column_definitions.join(",\n"));
            script.push_str("\n)");

            // Classical inheritance
            if !self.inherits_from.is_empty() {
                script.push_str(&format!("\ninherits ({})", self.inherits_from.join(", ")));
            }

            if let Some(partition_key) = &self.partition_key {
                script.push_str(&format!("\npartition by {}", partition_key));
            }

            if let Some(am) = &self.access_method {
                script.push_str(&format!("\nusing {}", quote_ident(am)));
            }

            // Storage parameters
            if let Some(params) = &self.storage_parameters
                && !params.is_empty()
            {
                script.push_str(&format!("\nwith ({})", params.join(", ")));
            }

            if let Some(space) = &self.space {
                script.push_str(&format!("\ntablespace {}", quote_ident(space)));
            }

            script.append_block(";");
        }

        // 5. Add other constraints (excluding primary key, foreign key, and NOT NULL).
        // NOT NULL constraints are already expressed in column definitions
        // (named ones with CONSTRAINT <name>, unnamed ones as plain NOT NULL).
        for constraint in &self.constraints {
            let c_type = constraint.constraint_type.to_lowercase();
            if c_type != "primary key" && c_type != "foreign key" && c_type != "not null" {
                script.push_str(&constraint.get_script());
            }
        }

        // 5a. Emit SET STORAGE / SET COMPRESSION for columns with non-default values
        for column in &self.columns {
            if let Some(storage) = &column.storage {
                // EXTENDED is the default for most varlena types; always emit to be explicit
                script.append_block(&format!(
                    "alter table {}.{} alter column {} set storage {};",
                    self.schema, self.name, column.name, storage
                ));
            }
            if let Some(compression) = &column.compression {
                script.append_block(&format!(
                    "alter table {}.{} alter column {} set compression {};",
                    self.schema, self.name, column.name, compression
                ));
            }
            if let Some(stats) = column.statistics_target {
                script.append_block(&format!(
                    "alter table {}.{} alter column {} set statistics {};",
                    self.schema, self.name, column.name, stats
                ));
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
            script.append_block(&format!(
                "alter table {}.{} enable row level security;",
                self.schema, self.name
            ));
        }

        // 9. Add row-level security policies
        for policy in &self.policies {
            script.push_str(&policy.get_script());
        }

        // 9a. Force row-level security
        if self.force_rowsecurity {
            script.append_block(&format!(
                "alter table {}.{} force row level security;",
                self.schema, self.name
            ));
        }

        // 9b. Replica identity
        if let Some(ri) = &self.replica_identity {
            match ri.as_str() {
                "n" => script.append_block(&format!(
                    "alter table {}.{} replica identity nothing;",
                    self.schema, self.name
                )),
                "f" => script.append_block(&format!(
                    "alter table {}.{} replica identity full;",
                    self.schema, self.name
                )),
                // "d" is default, "i" requires USING INDEX which we don't track yet
                _ => {}
            }
        }

        // 10. Add table comment (if any) and column comments
        if let Some(comment) = &self.comment {
            script.append_block(&format!(
                "comment on table {}.{} is '{}';",
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
        format!("drop table if exists {}.{};", self.schema, self.name).with_empty_lines()
    }

    pub fn get_owner_script(&self) -> String {
        if self.owner.is_empty() {
            return String::new();
        }

        format!(
            "alter table {}.{} owner to {};",
            self.schema, self.name, self.owner
        )
        .with_empty_lines()
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
                    "alter table {} detach partition {}.{};",
                    old_parent, self.schema, self.name
                )
                .with_empty_lines();
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
                partition_script.append_block(&format!(
                    "alter table {} attach partition {}.{} {};",
                    new_parent, self.schema, self.name, bound
                ));
            }
        }

        // Partition children inherit column structure (add/drop/alter) and
        // non-FK constraints from their parent table.  Generating the same DDL
        // for both parent and partition causes errors (ADD/DROP COLUMN) or
        // redundant operations (SET NOT NULL, SET DEFAULT, constraint changes).
        // Only suppress when the table is a partition child in both FROM and TO.
        // If it is transitioning from standalone to partition (attach), structural
        // changes may be required to make it compatible with the parent first.
        let is_target_partition = self.partition_of.is_some() && to_table.partition_of.is_some();

        // Collect column additions or alterations
        for new_col in &to_table.columns {
            if let Some(old_col) = self.columns.iter().find(|c| c.name == new_col.name) {
                if old_col != new_col {
                    // Skip structural column changes for partition children;
                    // only emit column comment changes (not inherited).
                    if is_target_partition {
                        if new_col.comment != old_col.comment {
                            if let Some(comment_script) = new_col.get_comment_script() {
                                column_alter_script.push_str(&comment_script);
                            } else {
                                column_alter_script.push_str(&format!(
                                    "comment on column {}.{}.{} is null;\n",
                                    new_col.schema, new_col.table, new_col.name
                                ));
                            }
                        }
                        continue;
                    }

                    let type_changed = old_col.data_type != new_col.data_type
                        || old_col.udt_name != new_col.udt_name
                        || old_col.numeric_precision != new_col.numeric_precision
                        || old_col.numeric_scale != new_col.numeric_scale
                        || old_col.character_maximum_length != new_col.character_maximum_length;

                    let is_partition_child = self.partition_of.is_some();
                    let in_partition_key = self.partition_key.as_ref().is_some_and(|pk| {
                        let col_lower = new_col.name.to_lowercase();
                        extract_partition_key_identifiers(pk).contains(&col_lower)
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
            } else if !is_target_partition {
                column_alter_script.push_str(&new_col.get_add_script());
            }
        }

        // Collect column drops separately so they happen after constraint drops
        if !is_target_partition {
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
        }

        // Collect constraint changes; drop statements run before column drops
        for new_constraint in &to_table.constraints {
            let is_fk = new_constraint.constraint_type.to_lowercase() == "foreign key";
            // Skip inherited non-FK constraints for partition children;
            // these are managed by the parent table.  Truly-local partition
            // constraints (coninhcount == 0) are still diffed.
            if is_target_partition && !is_fk && new_constraint.coninhcount > 0 {
                continue;
            }
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
            // Skip inherited non-FK constraint drops for partition children;
            // these are managed by the parent table.
            if is_target_partition
                && !old_constraint
                    .constraint_type
                    .eq_ignore_ascii_case("foreign key")
                && old_constraint.coninhcount > 0
            {
                continue;
            }
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
                    "comment on table {}.{} is '{}';",
                    to_table.schema,
                    to_table.name,
                    escape_single_quotes(cmt)
                )
            } else {
                format!(
                    "comment on table {}.{} is null;",
                    to_table.schema, to_table.name
                )
            };
            constraint_post_script.append_block(&comment_stmt);
        }

        // Collect index updates (skip partition-inherited indexes, managed by parent)
        for new_index in &to_table.indexes {
            if new_index.is_partition_index {
                continue;
            }
            if let Some(old_index) = self.indexes.iter().find(|i| i.name == new_index.name) {
                if old_index != new_index {
                    let drop_cmd = format!(
                        "drop index if exists {}.{};",
                        new_index.schema, new_index.name
                    )
                    .with_empty_lines();
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
                        "drop policy if exists {} on {}.{};",
                        old_policy.name, self.schema, self.name
                    )
                    .with_empty_lines();
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
                    "drop index if exists {}.{};",
                    old_index.schema, old_index.name
                )
                .with_empty_lines();
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
                    "drop policy if exists {} on {}.{};",
                    old_policy.name, self.schema, self.name
                )
                .with_empty_lines();
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
                    "alter table {}.{} enable row level security;",
                    self.schema, self.name
                )
            } else {
                format!(
                    "alter table {}.{} disable row level security;",
                    self.schema, self.name
                )
            };
            row_security_script.append_block(&stmt);
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

        if self.access_method != to_table.access_method {
            if let Some(am) = &to_table.access_method {
                script.append_block(&format!(
                    "alter table {}.{} set access method {};",
                    to_table.schema,
                    to_table.name,
                    quote_ident(am)
                ));
            } else {
                script.append_block(&format!(
                    "alter table {}.{} set access method heap;",
                    to_table.schema, to_table.name
                ));
            }
        }

        if self.space != to_table.space
            && let Some(new_space) = &to_table.space
        {
            script.push_str(&format!(
                "alter table {}.{} set tablespace {};\n",
                to_table.schema,
                to_table.name,
                quote_ident(new_space)
            ));
        }

        // UNLOGGED / LOGGED change
        if self.is_unlogged != to_table.is_unlogged {
            let stmt = if to_table.is_unlogged {
                format!(
                    "alter table {}.{} set unlogged;",
                    to_table.schema, to_table.name
                )
            } else {
                format!(
                    "alter table {}.{} set logged;",
                    to_table.schema, to_table.name
                )
            };
            script.append_block(&stmt);
        }

        // Storage parameters change
        if self.storage_parameters != to_table.storage_parameters {
            // Reset old parameters
            if let Some(old_params) = &self.storage_parameters
                && !old_params.is_empty()
            {
                let param_names: Vec<&str> = old_params
                    .iter()
                    .filter_map(|p| p.split('=').next())
                    .collect();
                if !param_names.is_empty() {
                    script.append_block(&format!(
                        "alter table {}.{} reset ({});",
                        to_table.schema,
                        to_table.name,
                        param_names.join(", ")
                    ));
                }
            }
            // Set new parameters
            if let Some(new_params) = &to_table.storage_parameters
                && !new_params.is_empty()
            {
                script.append_block(&format!(
                    "alter table {}.{} set ({});",
                    to_table.schema,
                    to_table.name,
                    new_params.join(", ")
                ));
            }
        }

        // Replica identity change
        if self.replica_identity != to_table.replica_identity {
            let stmt = match to_table.replica_identity.as_deref() {
                Some("n") => format!(
                    "alter table {}.{} replica identity nothing;",
                    to_table.schema, to_table.name
                ),
                Some("f") => format!(
                    "alter table {}.{} replica identity full;",
                    to_table.schema, to_table.name
                ),
                _ => format!(
                    "alter table {}.{} replica identity default;",
                    to_table.schema, to_table.name
                ),
            };
            script.append_block(&stmt);
        }

        // Force row-level security change
        if self.force_rowsecurity != to_table.force_rowsecurity {
            let stmt = if to_table.force_rowsecurity {
                format!(
                    "alter table {}.{} force row level security;",
                    to_table.schema, to_table.name
                )
            } else {
                format!(
                    "alter table {}.{} no force row level security;",
                    to_table.schema, to_table.name
                )
            };
            script.append_block(&stmt);
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
                        "drop trigger if exists {} on {}.{};",
                        old_trigger.name, self.schema, self.name
                    )
                    .with_empty_lines();
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
                    "drop trigger if exists {} on {}.{};",
                    old_trigger.name, self.schema, self.name
                )
                .with_empty_lines();
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
            generation_type: None,
            is_updatable: true,
            related_views: None,
            comment: None,
            storage: None,
            compression: None,
            statistics_target: None,
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
            coninhcount: 0,
            is_enforced: true,
            no_inherit: false,
            nulls_not_distinct: false,
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
            coninhcount: 0,
            is_enforced: true,
            no_inherit: false,
            nulls_not_distinct: false,
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
            coninhcount: 0,
            is_enforced: true,
            no_inherit: false,
            nulls_not_distinct: false,
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
            coninhcount: 0,
            is_enforced: true,
            no_inherit: false,
            nulls_not_distinct: false,
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
            "create table public.users (\n",
            "    id integer generated BY DEFAULT as identity not null,\n",
            "    name text not null,\n",
            "    constraint users_pkey primary key (\"id\")\n",
            ")\n",
            "tablespace \"pg_default\";\n\n",
            "alter table public.users add constraint users_name_check check (name <> '') ;\n\n",
            "create index idx_users_name on public.users using btree (name);\n\n",
            "create trigger audit_user before insert on public.users for each row execute function log_user();\n\n",
            "alter table public.users owner to postgres;\n\n",
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

        assert!(script.contains("create policy users_tenant_select"));
        assert!(script.contains("for select"));
        assert!(script.contains("enable row level security"));
    }

    #[test]
    fn test_get_script_includes_unique_indexes() {
        let table = Table::new(
            "public".to_string(),
            "users".to_string(),
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
        assert!(script.contains("id integer generated BY DEFAULT as identity"));
        assert!(!script.contains("serial"));
    }

    #[test]
    fn test_get_drop_script_returns_statement() {
        let table = basic_table();
        assert_eq!(
            table.get_drop_script(),
            "drop table if exists public.users;\n\n"
        );
    }

    #[test]
    fn test_get_alter_script_handles_complex_differences() {
        let from_table = Table::new(
            "public".to_string(),
            "users".to_string(),
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
            "alter table public.users drop constraint users_name_check;\n",
            "alter table public.users drop constraint users_legacy_check;\n",
            "alter table public.users alter column name set default 'unknown'::text;\n",
            "alter table public.users add column email text;\n",
            "drop index if exists public.idx_users_name;\n",
            "drop index if exists public.idx_users_old;\n",
            "drop trigger if exists audit_user on public.users;\n",
            "drop trigger if exists cleanup_user on public.users;\n",
            "alter table public.users drop column legacy;\n",
            "alter table public.users add constraint users_name_check check (char_length(name) > 0) ;\n",
            "alter table public.users add constraint users_email_unique unique (email) ;\n",
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

        assert!(fk_script.contains("alter table public.users alter constraint users_account_fk deferrable initially deferred;\n"));
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
        assert!(add_script.contains("create policy users_tenant_insert"));
        assert!(add_script.contains("enable row level security"));

        from_table = to_table.clone();
        let to_table_no_policy = basic_table();
        let drop_script = from_table.get_alter_script(&to_table_no_policy, true);
        assert!(drop_script.contains("drop policy if exists users_tenant_insert"));
        assert!(drop_script.contains("disable row level security"));
    }

    #[test]
    fn test_get_foreign_key_script() {
        let table = Table::new(
            "public".to_string(),
            "users".to_string(),
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

        assert!(script.contains("alter table public.users add constraint users_account_fk foreign key (account_id) references public.accounts(id)"));
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
            coninhcount: 0,
            is_enforced: true,
            no_inherit: false,
            nulls_not_distinct: false,
        }
    }

    #[test]
    fn test_get_foreign_key_alter_script_add_new_fk() {
        let from_table = Table::new(
            "public".to_string(),
            "users".to_string(),
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
            "alter table public.users add constraint fk_new foreign key (col) references other(id)"
        ));
    }

    #[test]
    fn test_get_foreign_key_alter_script_drop_fk() {
        let from_table = Table::new(
            "public".to_string(),
            "users".to_string(),
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
        assert!(script.contains("alter table public.users add constraint fk_change foreign key (col) references table_b(id)"));
    }

    #[test]
    fn test_get_foreign_key_alter_script_no_change() {
        let fk = custom_foreign_key_constraint("fk_same", "FOREIGN KEY (col) REFERENCES other(id)");

        let from_table = Table::new(
            "public".to_string(),
            "users".to_string(),
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

        assert!(drop_main_script.contains("alter table public.users drop constraint fk_drop;"));
        assert_eq!(drop_fk_script, "");

        // 2. Add FK (not in from, exists in to)
        let fk_add_from = fk_drop_to.clone();
        let fk_add_to = fk_drop_from.clone(); // reusing table with FK

        let add_main_script = fk_add_from.get_alter_script(&fk_add_to, true);
        let add_fk_script = fk_add_from.get_foreign_key_alter_script(&fk_add_to);

        assert!(!add_main_script.contains("fk_drop")); // Main script shouldn't touch new FKs
        assert!(add_fk_script.contains(
            "alter table public.users add constraint fk_drop foreign key (col) references other(id)"
        ));

        // 3. Recreate FK (definition change)
        let fk_change_from = Table::new(
            "public".to_string(),
            "users".to_string(),
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

        assert!(change_main_script.contains("alter table public.users drop constraint fk_change;"));
        assert!(change_fk_script.contains("alter table public.users add constraint fk_change foreign key (col) references new_table(id)"));
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
            generation_type: None,
            is_updatable: true,
            related_views: None,
            comment: None,
            storage: None,
            compression: None,
            statistics_target: None,
            serial_type: None,
        }
    }

    #[test]
    fn test_partitioned_table_script() {
        let mut table = Table::new(
            "data".to_string(),
            "test".to_string(),
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
        assert!(script.contains("create table data.test"));
        assert!(script.contains("partition by LIST (flow_id)"));
    }

    #[test]
    fn test_partition_child_script() {
        let mut table = Table::new(
            "data".to_string(),
            "test_default".to_string(),
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
        table.partition_of = Some("data.test".to_string());
        table.partition_bound = Some("DEFAULT".to_string());

        let script = table.get_script();
        assert!(script.contains("create table data.test_default partition of data.test"));
        assert!(script.contains("DEFAULT"));
    }

    #[test]
    fn test_sub_partition_script() {
        // A sub-partition is both a child of a partitioned table AND itself partitioned.
        let mut table = Table::new(
            "data".to_string(),
            "test_2023".to_string(),
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
        table.partition_of = Some("data.test".to_string());
        table.partition_bound = Some("FOR VALUES FROM (2023) TO (2024)".to_string());
        table.partition_key = Some("LIST (id)".to_string());

        let script = table.get_script();
        assert!(
            script.contains("create table data.test_2023 partition of data.test"),
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
        table.partition_of = Some("data.test".to_string());
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
        from_table.partition_of = Some("data.test".to_string());
        from_table.partition_bound = Some("DEFAULT".to_string());

        let to_table = Table::new(
            "data".to_string(),
            "test_default".to_string(),
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

        assert!(script.contains("alter table data.test detach partition data.test_default;"));
        assert!(!script.contains("attach partition"));
    }

    #[test]
    fn test_get_alter_script_attach_partition() {
        let from_table = Table::new(
            "data".to_string(),
            "test_default".to_string(),
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
        to_table.partition_of = Some("data.test".to_string());
        to_table.partition_bound = Some("DEFAULT".to_string());

        let script = from_table.get_alter_script(&to_table, true);

        assert!(!script.contains("detach partition"));
        assert!(
            script.contains("alter table data.test attach partition data.test_default DEFAULT;")
        );
    }

    // --- Helper for building a partition child table ---
    fn partition_child_table(
        columns: Vec<TableColumn>,
        constraints: Vec<TableConstraint>,
        indexes: Vec<TableIndex>,
    ) -> Table {
        let mut table = Table::new(
            "public".to_string(),
            "users_p1".to_string(),
            "public".to_string(),
            "users_p1".to_string(),
            "postgres".to_string(),
            None,
            columns,
            constraints,
            indexes,
            vec![],
            None,
        );
        table.partition_of = Some("public.users".to_string());
        table.partition_bound = Some("FOR VALUES FROM (1) TO (100)".to_string());
        table.hash();
        table
    }

    fn partition_child_column(name: &str, ordinal_position: i32) -> TableColumn {
        let mut col = base_column(name, ordinal_position);
        col.table = "users_p1".to_string();
        col
    }

    fn partition_child_column_not_null(name: &str, ordinal_position: i32) -> TableColumn {
        let mut col = partition_child_column(name, ordinal_position);
        col.is_nullable = false;
        col
    }

    fn partition_child_identity_column(
        name: &str,
        ordinal_position: i32,
        data_type: &str,
    ) -> TableColumn {
        let mut col = partition_child_column(name, ordinal_position);
        col.data_type = data_type.to_string();
        col.is_identity = true;
        col.is_nullable = false;
        col.identity_generation = Some("BY DEFAULT".to_string());
        col
    }

    fn partition_child_constraint(
        name: &str,
        constraint_type: &str,
        definition: Option<&str>,
    ) -> TableConstraint {
        TableConstraint {
            catalog: "postgres".to_string(),
            schema: "public".to_string(),
            name: name.to_string(),
            table_name: "users_p1".to_string(),
            constraint_type: constraint_type.to_string(),
            is_deferrable: false,
            initially_deferred: false,
            definition: definition.map(|d| d.to_string()),
            coninhcount: 1,
            is_enforced: true,
            no_inherit: false,
            nulls_not_distinct: false,
        }
    }

    // --- Partition child: column ADD is skipped ---
    #[test]
    fn test_partition_child_skips_add_column() {
        let from = partition_child_table(
            vec![partition_child_identity_column("id", 1, "integer")],
            vec![],
            vec![],
        );
        let mut to = partition_child_table(
            vec![
                partition_child_identity_column("id", 1, "integer"),
                partition_child_column("email", 2),
            ],
            vec![],
            vec![],
        );
        to.hash();

        let script = from.get_alter_script(&to, true);
        assert!(
            !script.contains("add column"),
            "ADD COLUMN must not appear for partition child, got: {script}"
        );
    }

    // --- Partition child: column DROP is skipped ---
    #[test]
    fn test_partition_child_skips_drop_column() {
        let from = partition_child_table(
            vec![
                partition_child_identity_column("id", 1, "integer"),
                partition_child_column("legacy", 2),
            ],
            vec![],
            vec![],
        );
        let to = partition_child_table(
            vec![partition_child_identity_column("id", 1, "integer")],
            vec![],
            vec![],
        );

        let script = from.get_alter_script(&to, true);
        assert!(
            !script.contains("drop column"),
            "DROP COLUMN must not appear for partition child, got: {script}"
        );
    }

    // --- Partition child: SET NOT NULL / DROP NOT NULL is skipped ---
    #[test]
    fn test_partition_child_skips_set_not_null() {
        let from = partition_child_table(
            vec![
                partition_child_identity_column("id", 1, "integer"),
                partition_child_column("name", 2), // nullable
            ],
            vec![],
            vec![],
        );
        let to = partition_child_table(
            vec![
                partition_child_identity_column("id", 1, "integer"),
                partition_child_column_not_null("name", 2), // not null
            ],
            vec![],
            vec![],
        );

        let script = from.get_alter_script(&to, true);
        assert!(
            !script.contains("set not null"),
            "SET NOT NULL must not appear for partition child, got: {script}"
        );
    }

    #[test]
    fn test_partition_child_skips_drop_not_null() {
        let from = partition_child_table(
            vec![
                partition_child_identity_column("id", 1, "integer"),
                partition_child_column_not_null("name", 2), // not null
            ],
            vec![],
            vec![],
        );
        let to = partition_child_table(
            vec![
                partition_child_identity_column("id", 1, "integer"),
                partition_child_column("name", 2), // nullable
            ],
            vec![],
            vec![],
        );

        let script = from.get_alter_script(&to, true);
        assert!(
            !script.contains("drop not null"),
            "DROP NOT NULL must not appear for partition child, got: {script}"
        );
    }

    // --- Partition child: SET DEFAULT / DROP DEFAULT is skipped ---
    #[test]
    fn test_partition_child_skips_set_default() {
        let from = partition_child_table(
            vec![
                partition_child_identity_column("id", 1, "integer"),
                partition_child_column("name", 2),
            ],
            vec![],
            vec![],
        );

        let mut name_with_default = partition_child_column("name", 2);
        name_with_default.column_default = Some("'unknown'::text".to_string());
        let to = partition_child_table(
            vec![
                partition_child_identity_column("id", 1, "integer"),
                name_with_default,
            ],
            vec![],
            vec![],
        );

        let script = from.get_alter_script(&to, true);
        assert!(
            !script.contains("set default"),
            "SET DEFAULT must not appear for partition child, got: {script}"
        );
    }

    #[test]
    fn test_partition_child_skips_drop_default() {
        let mut name_with_default = partition_child_column("name", 2);
        name_with_default.column_default = Some("'unknown'::text".to_string());
        let from = partition_child_table(
            vec![
                partition_child_identity_column("id", 1, "integer"),
                name_with_default,
            ],
            vec![],
            vec![],
        );
        let to = partition_child_table(
            vec![
                partition_child_identity_column("id", 1, "integer"),
                partition_child_column("name", 2),
            ],
            vec![],
            vec![],
        );

        let script = from.get_alter_script(&to, true);
        assert!(
            !script.contains("drop default"),
            "DROP DEFAULT must not appear for partition child, got: {script}"
        );
    }

    // --- Partition child: non-FK constraint add/drop/alter is skipped ---
    #[test]
    fn test_partition_child_skips_add_check_constraint() {
        let from = partition_child_table(
            vec![partition_child_identity_column("id", 1, "integer")],
            vec![],
            vec![],
        );
        let to = partition_child_table(
            vec![partition_child_identity_column("id", 1, "integer")],
            vec![partition_child_constraint(
                "users_p1_name_check",
                "CHECK",
                Some("CHECK (name <> '')"),
            )],
            vec![],
        );

        let script = from.get_alter_script(&to, true);
        assert!(
            !script.contains("add constraint"),
            "ADD CONSTRAINT (CHECK) must not appear for partition child, got: {script}"
        );
    }

    #[test]
    fn test_partition_child_skips_drop_check_constraint() {
        let from = partition_child_table(
            vec![partition_child_identity_column("id", 1, "integer")],
            vec![partition_child_constraint(
                "users_p1_name_check",
                "CHECK",
                Some("CHECK (name <> '')"),
            )],
            vec![],
        );
        let to = partition_child_table(
            vec![partition_child_identity_column("id", 1, "integer")],
            vec![],
            vec![],
        );

        let script = from.get_alter_script(&to, true);
        assert!(
            !script.contains("drop constraint"),
            "DROP CONSTRAINT (CHECK) must not appear for partition child, got: {script}"
        );
    }

    #[test]
    fn test_partition_child_skips_primary_key_constraint() {
        let from = partition_child_table(
            vec![partition_child_identity_column("id", 1, "integer")],
            vec![],
            vec![],
        );
        let to = partition_child_table(
            vec![partition_child_identity_column("id", 1, "integer")],
            vec![partition_child_constraint(
                "users_p1_pkey",
                "PRIMARY KEY",
                None,
            )],
            vec![],
        );

        let script = from.get_alter_script(&to, true);
        assert!(
            !script.contains("add constraint"),
            "ADD CONSTRAINT (PK) must not appear for partition child, got: {script}"
        );
    }

    // --- Partition child: FK constraints are still emitted ---
    #[test]
    fn test_partition_child_still_emits_fk_changes() {
        let from = partition_child_table(
            vec![partition_child_identity_column("id", 1, "integer")],
            vec![],
            vec![],
        );
        let fk = TableConstraint {
            catalog: "postgres".to_string(),
            schema: "public".to_string(),
            name: "users_p1_account_fk".to_string(),
            table_name: "users_p1".to_string(),
            constraint_type: "FOREIGN KEY".to_string(),
            is_deferrable: false,
            initially_deferred: false,
            definition: Some("FOREIGN KEY (account_id) REFERENCES public.accounts(id)".to_string()),
            coninhcount: 0,
            is_enforced: true,
            no_inherit: false,
            nulls_not_distinct: false,
        };
        let to = partition_child_table(
            vec![partition_child_identity_column("id", 1, "integer")],
            vec![fk],
            vec![],
        );

        // FKs on partitions are handled by get_foreign_key_alter_script, not
        // build_alter_script, so they should NOT appear in the alter script
        // itself.  The key point is that the FK is not suppressed when the
        // caller asks for it via the foreign-key path.
        let fk_script = from.get_foreign_key_alter_script(&to);
        assert!(
            fk_script.contains("users_p1_account_fk"),
            "FK constraint must still be emitted for partition child"
        );
    }

    // --- Partition child: column comment changes ARE emitted ---
    #[test]
    fn test_partition_child_emits_column_comment_change() {
        let mut col_from = partition_child_column("name", 2);
        col_from.comment = Some("old comment".to_string());
        let from = partition_child_table(
            vec![
                partition_child_identity_column("id", 1, "integer"),
                col_from,
            ],
            vec![],
            vec![],
        );

        let mut col_to = partition_child_column("name", 2);
        col_to.comment = Some("new comment".to_string());
        let to = partition_child_table(
            vec![partition_child_identity_column("id", 1, "integer"), col_to],
            vec![],
            vec![],
        );

        let script = from.get_alter_script(&to, true);
        assert!(
            script.contains("comment on column"),
            "Column comment change must be emitted for partition child, got: {script}"
        );
        assert!(
            script.contains("new comment"),
            "New comment text must appear, got: {script}"
        );
    }

    // --- Partition child: index changes ARE still emitted ---
    #[test]
    fn test_partition_child_still_emits_non_inherited_index_changes() {
        let idx = TableIndex {
            schema: "public".to_string(),
            table: "users_p1".to_string(),
            name: "idx_users_p1_name".to_string(),
            catalog: None,
            indexdef: "create index idx_users_p1_name on public.users_p1 using btree (name)"
                .to_string(),
            is_partition_index: false,
        };
        let from = partition_child_table(
            vec![partition_child_identity_column("id", 1, "integer")],
            vec![],
            vec![],
        );
        let to = partition_child_table(
            vec![partition_child_identity_column("id", 1, "integer")],
            vec![],
            vec![idx],
        );

        let script = from.get_alter_script(&to, true);
        assert!(
            script.contains("create index idx_users_p1_name"),
            "Non-inherited index on partition child must still be emitted, got: {script}"
        );
    }

    // --- Non-partitioned table: all changes are still emitted ---
    #[test]
    fn test_non_partition_table_emits_all_column_changes() {
        let from = basic_table();
        let mut to = basic_table();
        // Add a new column
        to.columns.push(email_column());
        to.hash();

        let script = from.get_alter_script(&to, true);
        assert!(
            script.contains("add column email"),
            "ADD COLUMN must appear for non-partitioned table, got: {script}"
        );
    }

    // --- Partition child: combined add + alter + drop all skipped ---
    #[test]
    fn test_partition_child_skips_combined_column_changes() {
        // From has: id, legacy (nullable)
        // To has: id, name (not null), email (new)
        // This means: legacy dropped, name added, no alter on id
        let from = partition_child_table(
            vec![
                partition_child_identity_column("id", 1, "integer"),
                partition_child_column("legacy", 2),
            ],
            vec![partition_child_constraint(
                "users_p1_check",
                "CHECK",
                Some("CHECK (legacy IS NOT NULL)"),
            )],
            vec![],
        );
        let to = partition_child_table(
            vec![
                partition_child_identity_column("id", 1, "integer"),
                partition_child_column_not_null("name", 2),
                partition_child_column("email", 3),
            ],
            vec![partition_child_constraint(
                "users_p1_name_check",
                "CHECK",
                Some("CHECK (name <> '')"),
            )],
            vec![],
        );

        let script = from.get_alter_script(&to, true);
        assert!(
            !script.contains("add column"),
            "ADD COLUMN must not appear, got: {script}"
        );
        assert!(
            !script.contains("drop column"),
            "DROP COLUMN must not appear, got: {script}"
        );
        assert!(
            !script.contains("add constraint"),
            "ADD CONSTRAINT must not appear, got: {script}"
        );
        assert!(
            !script.contains("drop constraint"),
            "DROP CONSTRAINT must not appear, got: {script}"
        );
    }

    // Helper: local partition constraint (coninhcount = 0)
    fn partition_child_local_constraint(
        name: &str,
        constraint_type: &str,
        definition: Option<&str>,
    ) -> TableConstraint {
        TableConstraint {
            catalog: "postgres".to_string(),
            schema: "public".to_string(),
            name: name.to_string(),
            table_name: "users_p1".to_string(),
            constraint_type: constraint_type.to_string(),
            is_deferrable: false,
            initially_deferred: false,
            definition: definition.map(|d| d.to_string()),
            coninhcount: 0,
            is_enforced: true,
            no_inherit: false,
            nulls_not_distinct: false,
        }
    }

    // --- Partition child: local (non-inherited) constraint add IS emitted ---
    #[test]
    fn test_partition_child_emits_add_local_check_constraint() {
        let from = partition_child_table(
            vec![partition_child_identity_column("id", 1, "integer")],
            vec![],
            vec![],
        );
        let to = partition_child_table(
            vec![partition_child_identity_column("id", 1, "integer")],
            vec![partition_child_local_constraint(
                "users_p1_local_chk",
                "CHECK",
                Some("CHECK (id > 0)"),
            )],
            vec![],
        );

        let script = from.get_alter_script(&to, true);
        assert!(
            script.contains("add constraint users_p1_local_chk"),
            "Local CHECK constraint must be emitted for partition child, got: {script}"
        );
    }

    // --- Partition child: local constraint drop IS emitted ---
    #[test]
    fn test_partition_child_emits_drop_local_check_constraint() {
        let from = partition_child_table(
            vec![partition_child_identity_column("id", 1, "integer")],
            vec![partition_child_local_constraint(
                "users_p1_local_chk",
                "CHECK",
                Some("CHECK (id > 0)"),
            )],
            vec![],
        );
        let to = partition_child_table(
            vec![partition_child_identity_column("id", 1, "integer")],
            vec![],
            vec![],
        );

        let script = from.get_alter_script(&to, true);
        assert!(
            script.contains("drop constraint users_p1_local_chk"),
            "Local CHECK constraint drop must be emitted for partition child, got: {script}"
        );
    }

    // --- Partition child: local constraint modification IS emitted ---
    #[test]
    fn test_partition_child_emits_alter_local_check_constraint() {
        let from = partition_child_table(
            vec![partition_child_identity_column("id", 1, "integer")],
            vec![partition_child_local_constraint(
                "users_p1_local_chk",
                "CHECK",
                Some("CHECK (id > 0)"),
            )],
            vec![],
        );
        let to = partition_child_table(
            vec![partition_child_identity_column("id", 1, "integer")],
            vec![partition_child_local_constraint(
                "users_p1_local_chk",
                "CHECK",
                Some("CHECK (id > 100)"),
            )],
            vec![],
        );

        let script = from.get_alter_script(&to, true);
        assert!(
            script.contains("drop constraint users_p1_local_chk"),
            "Local CHECK modification must emit drop, got: {script}"
        );
        assert!(
            script.contains("add constraint users_p1_local_chk"),
            "Local CHECK modification must emit add, got: {script}"
        );
    }

    // --- Partition child: inherited constraint skipped, local constraint emitted in same table ---
    #[test]
    fn test_partition_child_mixed_inherited_and_local_constraints() {
        let from = partition_child_table(
            vec![partition_child_identity_column("id", 1, "integer")],
            vec![partition_child_constraint(
                "users_p1_name_check",
                "CHECK",
                Some("CHECK (name <> '')"),
            )],
            vec![],
        );
        let to = partition_child_table(
            vec![partition_child_identity_column("id", 1, "integer")],
            vec![
                partition_child_constraint(
                    "users_p1_name_check",
                    "CHECK",
                    Some("CHECK (char_length(name) > 0)"),
                ),
                partition_child_local_constraint(
                    "users_p1_local_chk",
                    "CHECK",
                    Some("CHECK (id > 0)"),
                ),
            ],
            vec![],
        );

        let script = from.get_alter_script(&to, true);
        // Inherited constraint change should be suppressed
        assert!(
            !script.contains("users_p1_name_check"),
            "Inherited constraint must be suppressed, got: {script}"
        );
        // Local constraint addition should be emitted
        assert!(
            script.contains("add constraint users_p1_local_chk"),
            "Local constraint must be emitted, got: {script}"
        );
    }

    // --- Partition key substring false-positive regression ---
    #[test]
    fn test_partitioned_parent_non_key_col_type_change_uses_alter() {
        // Parent table partitioned by expense_date.
        // Changing `amount` numeric(10,2) → numeric(15,4) must NOT trigger
        // DROP+CREATE because `amount` is not in the partition key.
        // This exercises the extract_partition_key_identifiers path
        // (is_target_partition == false, in_partition_key must be false).
        let schema = "pt_test";
        let table_name = "s6_issue2_expenses";

        let mut col_amount_old = create_dummy_column("amount", "numeric");
        col_amount_old.schema = schema.to_string();
        col_amount_old.table = table_name.to_string();
        col_amount_old.numeric_precision = Some(10);
        col_amount_old.numeric_scale = Some(2);
        col_amount_old.ordinal_position = 3;

        let mut col_amount_new = create_dummy_column("amount", "numeric");
        col_amount_new.schema = schema.to_string();
        col_amount_new.table = table_name.to_string();
        col_amount_new.numeric_precision = Some(15);
        col_amount_new.numeric_scale = Some(4);
        col_amount_new.ordinal_position = 3;

        let mut id_col = create_dummy_column("id", "bigint");
        id_col.schema = schema.to_string();
        id_col.table = table_name.to_string();
        id_col.ordinal_position = 1;

        let mut date_col_old = create_dummy_column("expense_date", "date");
        date_col_old.schema = schema.to_string();
        date_col_old.table = table_name.to_string();
        date_col_old.ordinal_position = 2;
        let date_col_new = date_col_old.clone();

        let mut from = Table::new(
            schema.to_string(),
            table_name.to_string(),
            schema.to_string(),
            table_name.to_string(),
            "postgres".to_string(),
            None,
            vec![id_col.clone(), date_col_old, col_amount_old],
            vec![],
            vec![],
            vec![],
            None,
        );
        from.partition_key = Some("range (expense_date)".to_string());
        from.hash();

        let mut to = Table::new(
            schema.to_string(),
            table_name.to_string(),
            schema.to_string(),
            table_name.to_string(),
            "postgres".to_string(),
            None,
            vec![id_col, date_col_new, col_amount_new],
            vec![],
            vec![],
            vec![],
            None,
        );
        to.partition_key = Some("range (expense_date)".to_string());
        to.hash();

        let script = from.get_alter_script(&to, true);
        assert!(
            !script.contains("drop table"),
            "Non-partition-key column type change must not trigger DROP TABLE, got: {script}"
        );
        assert!(
            !script.contains("Data loss"),
            "No data loss warning expected for non-partition-key column, got: {script}"
        );
        assert!(
            script.contains("alter"),
            "Should produce an ALTER statement for non-key column, got: {script}"
        );
    }

    #[test]
    fn test_non_partition_key_column_type_change_uses_alter() {
        // Partition key references "category_id" but we change column "id".
        // The old substring check matched "id" inside "category_id" and
        // incorrectly triggered DROP + CREATE.
        let mut from_table = Table::new(
            "data".to_string(),
            "test".to_string(),
            "data".to_string(),
            "test".to_string(),
            "owner".to_string(),
            None,
            vec![
                create_dummy_column("id", "integer"),
                create_dummy_column("category_id", "integer"),
            ],
            vec![],
            vec![],
            vec![],
            None,
        );
        from_table.partition_key = Some("LIST (category_id)".to_string());

        let mut to_table = Table::new(
            "data".to_string(),
            "test".to_string(),
            "data".to_string(),
            "test".to_string(),
            "owner".to_string(),
            None,
            vec![
                create_dummy_column("id", "bigint"),
                create_dummy_column("category_id", "integer"),
            ],
            vec![],
            vec![],
            vec![],
            None,
        );
        to_table.partition_key = Some("LIST (category_id)".to_string());

        let script = from_table.get_alter_script(&to_table, true);
        assert!(
            !script.contains("drop table"),
            "Non-partition-key column type change must not trigger DROP TABLE, got: {script}"
        );
        assert!(
            !script.contains("Data loss"),
            "Non-partition-key column type change must not warn about data loss, got: {script}"
        );
        assert!(
            script.contains("alter"),
            "Should produce an ALTER statement, got: {script}"
        );
    }

    #[test]
    fn test_partition_key_column_type_change_still_triggers_recreate() {
        // When the actual partition key column type changes, DROP + CREATE is correct.
        let mut from_table = Table::new(
            "data".to_string(),
            "test".to_string(),
            "data".to_string(),
            "test".to_string(),
            "owner".to_string(),
            None,
            vec![
                create_dummy_column("id", "integer"),
                create_dummy_column("category_id", "integer"),
            ],
            vec![],
            vec![],
            vec![],
            None,
        );
        from_table.partition_key = Some("LIST (category_id)".to_string());

        let mut to_table = Table::new(
            "data".to_string(),
            "test".to_string(),
            "data".to_string(),
            "test".to_string(),
            "owner".to_string(),
            None,
            vec![
                create_dummy_column("id", "integer"),
                create_dummy_column("category_id", "bigint"),
            ],
            vec![],
            vec![],
            vec![],
            None,
        );
        to_table.partition_key = Some("LIST (category_id)".to_string());

        let script = from_table.get_alter_script(&to_table, true);
        assert!(
            script.contains("drop table"),
            "Partition key column type change must trigger DROP TABLE, got: {script}"
        );
        assert!(
            script.contains("Data loss"),
            "Partition key column type change must warn about data loss, got: {script}"
        );
    }

    #[test]
    fn test_extract_partition_key_single_unquoted() {
        let ids = extract_partition_key_identifiers("range (created_at)");
        assert_eq!(ids, vec!["created_at"]);
    }

    #[test]
    fn test_extract_partition_key_multiple_unquoted() {
        let ids = extract_partition_key_identifiers("range (region, created_at)");
        assert_eq!(ids, vec!["region", "created_at"]);
    }

    #[test]
    fn test_extract_partition_key_list_method() {
        let ids = extract_partition_key_identifiers("LIST (flow_id)");
        assert_eq!(ids, vec!["flow_id"]);
    }

    #[test]
    fn test_extract_partition_key_hash_method() {
        let ids = extract_partition_key_identifiers("hash (id)");
        assert_eq!(ids, vec!["id"]);
    }

    #[test]
    fn test_extract_partition_key_does_not_include_method() {
        // Column named "range" should NOT match the method keyword
        let ids = extract_partition_key_identifiers("range (created_at)");
        assert!(!ids.contains(&"range".to_string()));
    }

    #[test]
    fn test_extract_partition_key_dollar_identifier() {
        let ids = extract_partition_key_identifiers("range (my$col)");
        assert_eq!(ids, vec!["my$col"]);
    }

    #[test]
    fn test_extract_partition_key_quoted_identifier() {
        let ids = extract_partition_key_identifiers("list (\"My Column\")");
        assert_eq!(ids, vec!["my column"]);
    }

    #[test]
    fn test_extract_partition_key_quoted_with_escaped_quote() {
        let ids = extract_partition_key_identifiers("list (\"a\"\"b\")");
        assert_eq!(ids, vec!["a\"b"]);
    }

    #[test]
    fn test_extract_partition_key_mixed_quoted_and_unquoted() {
        let ids = extract_partition_key_identifiers("range (\"Region\", created_at)");
        assert_eq!(ids, vec!["region", "created_at"]);
    }

    #[test]
    fn test_extract_partition_key_empty_parens() {
        let ids = extract_partition_key_identifiers("range ()");
        assert!(ids.is_empty());
    }

    #[test]
    fn test_extract_partition_key_no_parens() {
        let ids = extract_partition_key_identifiers("range");
        assert!(ids.is_empty());
    }

    #[test]
    fn test_extract_partition_key_column_named_list() {
        // A column actually named "list" inside the key should be extracted
        let ids = extract_partition_key_identifiers("range (list)");
        assert_eq!(ids, vec!["list"]);
    }

    // ---- PG18: WITHOUT OVERLAPS temporal constraint tests ----

    #[test]
    fn test_get_script_temporal_pk_without_overlaps() {
        let table = Table::new(
            "public".to_string(),
            "reservations".to_string(),
            "public".to_string(),
            "reservations".to_string(),
            "postgres".to_string(),
            None,
            vec![
                {
                    let mut c = base_column("id", 1);
                    c.data_type = "integer".to_string();
                    c.is_nullable = false;
                    c
                },
                {
                    let mut c = base_column("valid_range", 2);
                    c.table = "reservations".to_string();
                    c.data_type = "tsrange".to_string();
                    c.is_nullable = false;
                    c
                },
            ],
            vec![TableConstraint {
                catalog: "postgres".to_string(),
                schema: "public".to_string(),
                name: "reservations_pkey".to_string(),
                table_name: "reservations".to_string(),
                constraint_type: "PRIMARY KEY".to_string(),
                is_deferrable: false,
                initially_deferred: false,
                definition: Some("PRIMARY KEY (id, valid_range WITHOUT OVERLAPS)".to_string()),
                coninhcount: 0,
                is_enforced: true,
                no_inherit: false,
                nulls_not_distinct: false,
            }],
            vec![],
            vec![],
            None,
        );

        let script = table.get_script();
        assert!(
            script.contains("primary key (id, valid_range WITHOUT OVERLAPS)"),
            "expected WITHOUT OVERLAPS in PK definition: {script}"
        );
    }

    #[test]
    fn test_get_script_temporal_pk_named_constraint_without_overlaps() {
        let table = Table::new(
            "public".to_string(),
            "bookings".to_string(),
            "public".to_string(),
            "bookings".to_string(),
            "postgres".to_string(),
            None,
            vec![
                {
                    let mut c = base_column("room_id", 1);
                    c.data_type = "integer".to_string();
                    c.is_nullable = false;
                    c
                },
                {
                    let mut c = base_column("period", 2);
                    c.table = "bookings".to_string();
                    c.data_type = "tsrange".to_string();
                    c.is_nullable = false;
                    c
                },
            ],
            vec![TableConstraint {
                catalog: "postgres".to_string(),
                schema: "public".to_string(),
                name: "bookings_pk".to_string(),
                table_name: "bookings".to_string(),
                constraint_type: "PRIMARY KEY".to_string(),
                is_deferrable: false,
                initially_deferred: false,
                definition: Some("PRIMARY KEY (room_id, period WITHOUT OVERLAPS)".to_string()),
                coninhcount: 0,
                is_enforced: true,
                no_inherit: false,
                nulls_not_distinct: false,
            }],
            vec![],
            vec![],
            None,
        );

        let script = table.get_script();
        assert!(
            script
                .contains("constraint bookings_pk primary key (room_id, period WITHOUT OVERLAPS)"),
            "expected named constraint with WITHOUT OVERLAPS: {script}"
        );
    }

    #[test]
    fn test_get_script_not_enforced_constraint_in_table() {
        let table = Table::new(
            "public".to_string(),
            "orders".to_string(),
            "public".to_string(),
            "orders".to_string(),
            "postgres".to_string(),
            None,
            vec![
                {
                    let mut c = base_column("id", 1);
                    c.data_type = "integer".to_string();
                    c.is_nullable = false;
                    c
                },
                {
                    let mut c = base_column("status", 2);
                    c.data_type = "text".to_string();
                    c
                },
            ],
            vec![TableConstraint {
                catalog: "postgres".to_string(),
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
            }],
            vec![],
            vec![],
            None,
        );

        let script = table.get_script();
        assert!(
            script.contains("not enforced"),
            "expected NOT ENFORCED check constraint in table script: {script}"
        );
    }

    #[test]
    fn test_get_script_virtual_generated_column_in_table() {
        let table = Table::new(
            "public".to_string(),
            "products".to_string(),
            "public".to_string(),
            "products".to_string(),
            "postgres".to_string(),
            None,
            vec![
                {
                    let mut c = base_column("price", 1);
                    c.data_type = "numeric".to_string();
                    c.is_nullable = false;
                    c
                },
                {
                    let mut c = base_column("qty", 2);
                    c.data_type = "integer".to_string();
                    c
                },
                {
                    let mut c = base_column("total", 3);
                    c.table = "products".to_string();
                    c.data_type = "numeric".to_string();
                    c.is_generated = "ALWAYS".to_string();
                    c.generation_expression = Some("(price * qty)".to_string());
                    c.generation_type = Some("v".to_string());
                    c
                },
            ],
            vec![],
            vec![],
            vec![],
            None,
        );

        let script = table.get_script();
        assert!(
            script.contains("generated always as (price * qty) virtual"),
            "expected virtual generated column in table script: {script}"
        );
    }

    // --- Named NOT NULL constraint tests (PG18 contype='n') ---

    fn not_null_constraint(name: &str, column: &str) -> TableConstraint {
        TableConstraint {
            catalog: "postgres".to_string(),
            schema: "public".to_string(),
            name: name.to_string(),
            table_name: "users".to_string(),
            constraint_type: "NOT NULL".to_string(),
            is_deferrable: false,
            initially_deferred: false,
            definition: Some(format!("NOT NULL {column}")),
            coninhcount: 0,
            is_enforced: true,
            no_inherit: false,
            nulls_not_distinct: false,
        }
    }

    #[test]
    fn test_named_not_null_constraint_emitted_in_column_def() {
        let table = Table::new(
            "public".to_string(),
            "users".to_string(),
            "public".to_string(),
            "users".to_string(),
            "postgres".to_string(),
            None,
            vec![identity_column("id", 1, "integer"), name_column()],
            vec![
                primary_key_constraint(),
                not_null_constraint("name_must_exist", "name"),
            ],
            vec![primary_key_index()],
            vec![],
            None,
        );

        let script = table.get_script();
        assert!(
            script.contains("constraint name_must_exist not null"),
            "expected named NOT NULL constraint in column definition: {script}"
        );
        // Must NOT appear as a separate ALTER TABLE statement
        assert!(
            !script.contains("alter table public.users add constraint name_must_exist"),
            "named NOT NULL should not be emitted as ALTER TABLE: {script}"
        );
    }

    #[test]
    fn test_auto_generated_not_null_name_skips_constraint_keyword() {
        // Auto-generated name follows the pattern {table}_{col}_not_null
        let table = Table::new(
            "public".to_string(),
            "users".to_string(),
            "public".to_string(),
            "users".to_string(),
            "postgres".to_string(),
            None,
            vec![identity_column("id", 1, "integer"), name_column()],
            vec![
                primary_key_constraint(),
                not_null_constraint("users_name_not_null", "name"),
            ],
            vec![primary_key_index()],
            vec![],
            None,
        );

        let script = table.get_script();
        // Auto-generated name: plain "not null" without CONSTRAINT keyword
        assert!(
            script.contains("name text not null"),
            "expected plain NOT NULL for auto-generated name: {script}"
        );
        assert!(
            !script.contains("constraint users_name_not_null"),
            "auto-generated NOT NULL name should not use CONSTRAINT keyword: {script}"
        );
    }

    #[test]
    fn test_named_not_null_with_not_enforced() {
        let mut nn = not_null_constraint("name_nn", "name");
        nn.is_enforced = false;

        let table = Table::new(
            "public".to_string(),
            "users".to_string(),
            "public".to_string(),
            "users".to_string(),
            "postgres".to_string(),
            None,
            vec![identity_column("id", 1, "integer"), name_column()],
            vec![primary_key_constraint(), nn],
            vec![primary_key_index()],
            vec![],
            None,
        );

        let script = table.get_script();
        assert!(
            script.contains("constraint name_nn not null not enforced"),
            "expected NOT ENFORCED on named NOT NULL constraint: {script}"
        );
    }

    #[test]
    fn test_named_not_null_with_no_inherit() {
        let mut nn = not_null_constraint("name_nn", "name");
        nn.no_inherit = true;

        let table = Table::new(
            "public".to_string(),
            "users".to_string(),
            "public".to_string(),
            "users".to_string(),
            "postgres".to_string(),
            None,
            vec![identity_column("id", 1, "integer"), name_column()],
            vec![primary_key_constraint(), nn],
            vec![primary_key_index()],
            vec![],
            None,
        );

        let script = table.get_script();
        assert!(
            script.contains("constraint name_nn not null no inherit"),
            "expected NO INHERIT on named NOT NULL constraint: {script}"
        );
    }
}
