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
    /// Fill metadata for every table in `tables` using **schema-wide** queries.
    ///
    /// Previously each table fired seven `WHERE schema='X' AND name='Y'` queries,
    /// yielding `7 × N` round-trips for `N` tables. This variant fires seven
    /// queries total (one per sub-resource), filtered by the same accessible-
    /// schema list that was used to collect the shell tables, and distributes
    /// rows into the matching `Table` in memory. On high-latency connections
    /// this reduces dump time dramatically.
    ///
    /// `has_tabledef_fn` and `caps` are pre-probed once per dump run so this
    /// function avoids redundant catalog checks.
    pub async fn fill_all(
        tables: &mut [Table],
        pool: &PgPool,
        has_tabledef_fn: bool,
        pg_version: i32,
        caps: PgCatalogCaps,
        schema_filter: &str,
    ) -> Result<(), Error> {
        if tables.is_empty() {
            return Ok(());
        }

        // (raw_schema, raw_name) -> index into `tables`
        let table_idx: HashMap<(String, String), usize> = tables
            .iter()
            .enumerate()
            .map(|(i, t)| ((t.raw_schema.clone(), t.raw_name.clone()), i))
            .collect();

        let (
            columns_by_key,
            indexes_by_key,
            constraints_by_key,
            triggers_by_key,
            (policies_by_key, rowsecurity_by_key),
            partitions_by_key,
            definitions_by_key,
        ) = tokio::try_join!(
            Self::fetch_columns_bulk(pool, schema_filter, caps),
            Self::fetch_indexes_bulk(pool, schema_filter),
            Self::fetch_constraints_bulk(pool, schema_filter, pg_version, caps),
            Self::fetch_triggers_bulk(pool, schema_filter),
            Self::fetch_policies_bulk(pool, schema_filter),
            Self::fetch_partition_info_bulk(pool, schema_filter),
            Self::fetch_definitions_bulk(pool, schema_filter, has_tabledef_fn),
        )?;

        for (key, mut cols) in columns_by_key {
            if let Some(&i) = table_idx.get(&key) {
                cols.sort_by_key(|a| a.ordinal_position);
                tables[i].columns = cols;
            }
        }
        for (key, mut idxs) in indexes_by_key {
            if let Some(&i) = table_idx.get(&key) {
                idxs.sort_by_key(|a| a.name.to_lowercase());
                tables[i].indexes = idxs;
            }
        }
        for (key, cons) in constraints_by_key {
            if let Some(&i) = table_idx.get(&key) {
                tables[i].constraints = cons;
            }
        }
        for (key, mut trigs) in triggers_by_key {
            if let Some(&i) = table_idx.get(&key) {
                trigs.sort_by_key(|a| a.name.to_lowercase());
                tables[i].triggers = trigs;
            }
        }
        for (key, pols) in policies_by_key {
            if let Some(&i) = table_idx.get(&key) {
                tables[i].policies = pols;
            }
        }
        for (key, rs) in rowsecurity_by_key {
            if let Some(&i) = table_idx.get(&key) {
                tables[i].has_rowsecurity = rs;
            }
        }
        for (key, (partition_key, partition_of, partition_bound)) in partitions_by_key {
            if let Some(&i) = table_idx.get(&key) {
                tables[i].partition_key = partition_key;
                tables[i].partition_of = partition_of;
                tables[i].partition_bound = partition_bound;
            }
        }
        for (key, def) in definitions_by_key {
            if let Some(&i) = table_idx.get(&key) {
                tables[i].definition = def;
            }
        }

        Ok(())
    }

    fn build_columns_query(compression_col: &str, schema_filter: &str) -> String {
        format!(
                        "SELECT
                                c.table_catalog,
                                quote_ident(c.table_schema) as table_schema,
                                quote_ident(c.table_name) as table_name,
                                c.table_schema as raw_table_schema,
                                c.table_name as raw_table_name,
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
                                a.attstattarget::int4 as col_stattarget,
                                c.is_updatable,
                                pd.description as column_comment{compression_col},
                                coalesce(
                                        (select array_agg(acl_item::text) from unnest(a.attacl) as acl_item),
                                        '{{}}'::text[]
                                ) as col_acl,
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
                            AND pd.classoid = 'pg_class'::regclass
                            AND pd.objsubid = a.attnum
                        WHERE c.table_schema IN {schema_filter}
                        ORDER BY c.table_schema, c.table_name, c.ordinal_position"
        )
    }

    /// Fetch columns for every table in the accessible schemas in one query.
    async fn fetch_columns_bulk(
        pool: &PgPool,
        schema_filter: &str,
        caps: PgCatalogCaps,
    ) -> Result<HashMap<(String, String), Vec<TableColumn>>, Error> {
        let compression_col = if caps.has_attcompression {
            ",\n                                a.attcompression::text as col_compression"
        } else {
            ""
        };
        let query = Self::build_columns_query(compression_col, schema_filter);
        let rows = sqlx::query(&query).fetch_all(pool).await?;

        let mut columns_by_key: HashMap<(String, String), Vec<TableColumn>> = HashMap::new();
        if !rows.is_empty() {
            for row in rows {
                let raw_schema: String = row.get("raw_table_schema");
                let raw_name: String = row.get("raw_table_name");
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
                        let st: Option<i32> = row.get("col_stattarget");
                        st.filter(|&v| v >= 0)
                    },
                    acl: row.get::<Vec<String>, _>("col_acl"),
                    serial_type: None,
                };

                columns_by_key
                    .entry((raw_schema, raw_name))
                    .or_default()
                    .push(table_column);
            }
        }

        Ok(columns_by_key)
    }

    /// Fetch indexes for every table in the accessible schemas in one query.
    async fn fetch_indexes_bulk(
        pool: &PgPool,
        schema_filter: &str,
    ) -> Result<HashMap<(String, String), Vec<TableIndex>>, Error> {
        let query = Self::build_indexes_bulk_query(schema_filter);
        let rows = sqlx::query(&query).fetch_all(pool).await?;

        let mut indexes_by_key: HashMap<(String, String), Vec<TableIndex>> = HashMap::new();
        for row in rows {
            let raw_schema: String = row.get("raw_schemaname");
            let raw_name: String = row.get("raw_tablename");
            let table_index = TableIndex {
                schema: row.get("schemaname"),
                table: row.get("tablename"),
                name: row.get("indexname"),
                catalog: row.get("tablespace"),
                indexdef: row.get("indexdef"),
                is_partition_index: row.get("is_partition_index"),
                comment: row.get("index_comment"),
            };
            indexes_by_key
                .entry((raw_schema, raw_name))
                .or_default()
                .push(table_index);
        }

        Ok(indexes_by_key)
    }

    fn build_indexes_bulk_query(schema_filter: &str) -> String {
        format!(
            "select
                quote_ident(i.schemaname) as schemaname,
                quote_ident(i.tablename) as tablename,
                i.schemaname as raw_schemaname,
                i.tablename as raw_tablename,
                quote_ident(i.indexname) as indexname,
                i.tablespace,
                i.indexdef,
                EXISTS (SELECT 1 FROM pg_inherits inh WHERE inh.inhrelid = ic.oid) AS is_partition_index,
                d.description as index_comment
            from
                pg_indexes i
                join pg_class ic on ic.relname = i.indexname
                join pg_namespace n on n.oid = ic.relnamespace and n.nspname = i.schemaname
                join pg_index idx on idx.indexrelid = ic.oid
                left join pg_constraint puc on puc.conindid = ic.oid and puc.contype in ('p', 'u')
                left join pg_description d
                    on d.objoid = ic.oid
                    and d.classoid = 'pg_class'::regclass
                    and d.objsubid = 0
            where
                idx.indisprimary = false
                and (idx.indisunique = false or puc.oid is null)
                and not exists (
                    select 1 from pg_constraint xc
                    where xc.conindid = ic.oid
                    and xc.contype = 'x'
                )
                and i.schemaname in {schema_filter}
            order by
                i.schemaname,
                i.tablename,
                i.indexname"
        )
    }

    /// Fetch constraints for every table in the accessible schemas in one query.
    async fn fetch_constraints_bulk(
        pool: &PgPool,
        schema_filter: &str,
        pg_version: i32,
        caps: PgCatalogCaps,
    ) -> Result<HashMap<(String, String), Vec<TableConstraint>>, Error> {
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
                n.nspname AS raw_schema,
                t.relname AS raw_table_name,
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
                c.coninhcount::int4 AS coninhcount,\n                c.connoinherit AS no_inherit,\n                d.description AS constraint_comment{conenforced_col}{connullsnotdistinct_col}
            FROM
                pg_constraint c
                JOIN pg_class t ON t.oid = c.conrelid
                JOIN pg_namespace n ON n.oid = t.relnamespace
                LEFT JOIN pg_description d ON d.objoid = c.oid AND d.classoid = 'pg_constraint'::regclass AND d.objsubid = 0
                WHERE
                    n.nspname IN {schema_filter} AND
                    c.contype IN {contype_filter} AND
                    c.conislocal
                ORDER BY
                    n.nspname,
                    t.relname,
                    c.conname;"
        );

        let rows = sqlx::query(&query).fetch_all(pool).await?;

        let mut constraints_by_key: HashMap<(String, String), Vec<TableConstraint>> =
            HashMap::new();
        for row in rows {
            let raw_schema: String = row.get("raw_schema");
            let raw_name: String = row.get("raw_table_name");
            let constraint = TableConstraint {
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
                comment: row.get("constraint_comment"),
            };
            constraints_by_key
                .entry((raw_schema, raw_name))
                .or_default()
                .push(constraint);
        }

        Ok(constraints_by_key)
    }

    /// Fetch triggers for every table in the accessible schemas in one query.
    async fn fetch_triggers_bulk(
        pool: &PgPool,
        schema_filter: &str,
    ) -> Result<HashMap<(String, String), Vec<TableTrigger>>, Error> {
        let query = format!(
            "select
                t.oid,
                n.nspname as raw_schema,
                c.relname as raw_table_name,
                quote_ident(t.tgname) as tgname,
                pg_get_triggerdef(t.oid) as tgdef,
                t.tgenabled::text as tgenabled,
                d.description as tgcomment
            from
                pg_trigger t
            join pg_class c on c.oid = t.tgrelid
            join pg_namespace n on n.oid = c.relnamespace
            left join pg_description d on d.objoid = t.oid and d.classoid = 'pg_trigger'::regclass
            where
                n.nspname IN {schema_filter} and
                t.tgisinternal = false
            order by
                n.nspname, c.relname, t.tgname"
        );
        let rows = sqlx::query(&query).fetch_all(pool).await?;

        let mut triggers_by_key: HashMap<(String, String), Vec<TableTrigger>> = HashMap::new();
        for row in rows {
            let raw_schema: String = row.get("raw_schema");
            let raw_name: String = row.get("raw_table_name");
            let trig = TableTrigger {
                oid: row.get("oid"),
                name: row.get("tgname"),
                definition: row.get("tgdef"),
                enabled: row.get("tgenabled"),
                comment: row.try_get("tgcomment").ok().flatten(),
            };
            triggers_by_key
                .entry((raw_schema, raw_name))
                .or_default()
                .push(trig);
        }

        Ok(triggers_by_key)
    }

    /// Fetch row-level security policies and per-table rowsecurity flag in
    /// two schema-wide queries. Returns separate maps keyed by
    /// `(raw_schema, raw_table_name)`.
    #[allow(clippy::type_complexity)]
    async fn fetch_policies_bulk(
        pool: &PgPool,
        schema_filter: &str,
    ) -> Result<
        (
            HashMap<(String, String), Vec<TablePolicy>>,
            HashMap<(String, String), bool>,
        ),
        Error,
    > {
        let policies_query = format!(
            "SELECT
                    quote_ident(p.polname) as polname,
                    quote_ident(n.nspname) AS schemaname,
                    quote_ident(c.relname) AS tablename,
                    n.nspname AS raw_schema,
                    c.relname AS raw_table_name,
                    p.polcmd::text AS polcmd,
                    p.polpermissive,
                    array(SELECT rolname::text FROM pg_roles r WHERE r.oid = ANY(p.polroles) ORDER BY rolname) AS roles,
                    pg_get_expr(p.polqual, p.polrelid) AS using_clause,
                    pg_get_expr(p.polwithcheck, p.polrelid) AS check_clause
             FROM pg_policy p
             JOIN pg_class c ON c.oid = p.polrelid
             JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname IN {schema_filter}
             ORDER BY n.nspname, c.relname, p.polname"
        );

        // Separate query so we can populate rowsecurity for tables WITHOUT
        // policies too; matches the old per-table semantics.
        let rowsecurity_query = format!(
            "SELECT n.nspname AS raw_schema, c.relname AS raw_table_name, c.relrowsecurity
             FROM pg_class c
             JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname IN {schema_filter} AND c.relkind IN ('r','p')"
        );

        let (policy_rows, rowsecurity_rows) = tokio::try_join!(
            sqlx::query(&policies_query).fetch_all(pool),
            sqlx::query(&rowsecurity_query).fetch_all(pool),
        )?;

        let mut policies_by_key: HashMap<(String, String), Vec<TablePolicy>> = HashMap::new();
        for row in &policy_rows {
            let raw_schema: String = row.get("raw_schema");
            let raw_name: String = row.get("raw_table_name");
            policies_by_key
                .entry((raw_schema, raw_name))
                .or_default()
                .push(TablePolicy::from_row(row)?);
        }

        let mut rowsecurity_by_key: HashMap<(String, String), bool> = HashMap::new();
        for row in &rowsecurity_rows {
            let raw_schema: String = row.get("raw_schema");
            let raw_name: String = row.get("raw_table_name");
            let rs: bool = row.get("relrowsecurity");
            rowsecurity_by_key.insert((raw_schema, raw_name), rs);
        }

        Ok((policies_by_key, rowsecurity_by_key))
    }

    /// Fetch partition information for the given table.
    ///
    /// Returns a map from `(raw_schema, raw_table_name)` to
    /// `(partition_key, partition_of, partition_bound)`.
    #[allow(clippy::type_complexity)]
    async fn fetch_partition_info_bulk(
        pool: &PgPool,
        schema_filter: &str,
    ) -> Result<HashMap<(String, String), (Option<String>, Option<String>, Option<String>)>, Error>
    {
        // `pg_inherits` records BOTH declarative partition children
        // (parent.relkind = 'p') AND classical-inheritance children
        // (parent.relkind = 'r', child created with `INHERITS (...)`).
        // The `partition_of` field is only meaningful for partition
        // children — classical-inheritance parents are tracked
        // separately in `inherits_from`. Without the `p.relkind = 'p'`
        // guard, classical-inheritance children land with both fields
        // set, which (a) makes `build_script` emit invalid
        // `CREATE TABLE … PARTITION OF parent` for a non-partitioned
        // parent, and (b) trips `column_type_change_forces_recreate`
        // into a wholesale drop+recreate for column type changes
        // PostgreSQL would happily ALTER in place. That second effect
        // is what leaked a non-idempotent diff after migration:
        // recreated tables inherit current default privileges, and a
        // second `pgc compare` then emitted REVOKE statements for the
        // newly-attached grants.
        let query = format!(
            "SELECT
                n.nspname AS raw_schema,
                c.relname AS raw_table_name,
                c.relkind::text,
                pg_get_partkeydef(c.oid) AS partition_key,
                pg_get_expr(c.relpartbound, c.oid) AS partition_bound,
                quote_ident(p.relname) AS parent_table,
                quote_ident(pn.nspname) AS parent_schema
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            LEFT JOIN pg_inherits i ON i.inhrelid = c.oid
            LEFT JOIN pg_class p ON p.oid = i.inhparent AND p.relkind = 'p'
            LEFT JOIN pg_namespace pn ON pn.oid = p.relnamespace
            WHERE n.nspname IN {schema_filter} AND c.relkind IN ('r','p')"
        );

        let rows = sqlx::query(&query).fetch_all(pool).await?;

        let mut out: HashMap<(String, String), (Option<String>, Option<String>, Option<String>)> =
            HashMap::new();
        for row in rows {
            let raw_schema: String = row.get("raw_schema");
            let raw_name: String = row.get("raw_table_name");
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

            out.insert(
                (raw_schema, raw_name),
                (partition_key, partition_of, partition_bound),
            );
        }
        Ok(out)
    }

    /// Fetch table definitions via `pg_get_tabledef` for all tables at once.
    /// Returns an empty map when the extension function is absent.
    async fn fetch_definitions_bulk(
        pool: &PgPool,
        schema_filter: &str,
        has_tabledef_fn: bool,
    ) -> Result<HashMap<(String, String), Option<String>>, Error> {
        if !has_tabledef_fn {
            return Ok(HashMap::new());
        }
        let query = format!(
            "select
                n.nspname as raw_schema,
                c.relname as raw_table_name,
                pg_get_tabledef(c.oid) AS definition
            from
                pg_class c
                join pg_namespace n on n.oid = c.relnamespace
            where
                n.nspname IN {schema_filter}
                and c.relkind in ('r','p')"
        );
        let rows = sqlx::query(&query).fetch_all(pool).await?;
        let mut out: HashMap<(String, String), Option<String>> = HashMap::new();
        for row in rows {
            let raw_schema: String = row.get("raw_schema");
            let raw_name: String = row.get("raw_table_name");
            let def: Option<String> = row.get("definition");
            out.insert((raw_schema, raw_name), def);
        }
        Ok(out)
    }

    /// Hash the table
    pub fn hash(&mut self) {
        let mut hasher = Sha256::new();
        hasher.update(self.schema.as_bytes());
        hasher.update(self.name.as_bytes());
        hasher.update(self.owner.as_bytes());
        hasher.update(self.has_indexes.to_string().as_bytes());
        hasher.update(self.has_triggers.to_string().as_bytes());
        // has_rules is intentionally excluded from the hash: rules are compared
        // separately via compare_rules() and should not influence view dependency
        // detection or trigger unnecessary table processing.
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
            // auto-generated default "{table}_{col}_not_null" (or that name with
            // a numeric suffix that PG appends to resolve cross-table collisions).
            let named_nn_constraints: HashMap<String, &TableConstraint> = self
                .constraints
                .iter()
                .filter(|c| c.constraint_type.eq_ignore_ascii_case("not null"))
                .filter_map(|c| {
                    let def = c.definition.as_deref()?;
                    let def_lower = def.to_lowercase();
                    let col_name = def_lower.strip_prefix("not null ")?.trim().to_string();
                    if c.auto_not_null_column(&self.name).is_some() {
                        None
                    } else {
                        Some((col_name, c))
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
                script.push_str(&trigger.get_script(&self.schema, &self.name));
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
            script.push_str(&trigger.get_script(&self.schema, &self.name));
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

                    if self.column_type_change_forces_recreate(old_col, new_col) {
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

        // Match constraints by name, except for NOT NULL constraints with
        // auto-generated names: those are matched by column because PG's
        // auto-generated name may differ between databases (collision suffixes).
        let find_old = |new_c: &TableConstraint| -> Option<&TableConstraint> {
            if let Some(found) = self.constraints.iter().find(|c| c.name == new_c.name) {
                return Some(found);
            }
            let new_col = new_c.auto_not_null_column(&to_table.name)?;
            self.constraints
                .iter()
                .find(|c| c.auto_not_null_column(&self.name).as_deref() == Some(new_col.as_str()))
        };
        let find_new = |old_c: &TableConstraint| -> Option<&TableConstraint> {
            if let Some(found) = to_table.constraints.iter().find(|c| c.name == old_c.name) {
                return Some(found);
            }
            let old_col = old_c.auto_not_null_column(&self.name)?;
            to_table.constraints.iter().find(|c| {
                c.auto_not_null_column(&to_table.name).as_deref() == Some(old_col.as_str())
            })
        };

        // Collect constraint changes; drop statements run before column drops
        for new_constraint in &to_table.constraints {
            let is_fk = new_constraint.constraint_type.to_lowercase() == "foreign key";
            // Skip inherited non-FK constraints for partition children;
            // these are managed by the parent table.  Truly-local partition
            // constraints (coninhcount == 0) are still diffed.
            if is_target_partition && !is_fk && new_constraint.coninhcount > 0 {
                continue;
            }
            if let Some(old_constraint) = find_old(new_constraint) {
                // Auto-named NOT NULL on the same column is semantically a no-op
                // regardless of the numeric suffix PG chose for the name.
                //
                // The field set checked here intentionally mirrors
                // `TableConstraint::PartialEq` *minus* the schema/name/table_name
                // identifiers (which we have already established describe the
                // same constraint via `find_old`) and the `definition` string
                // (whose only NOT NULL payload is the column name, also already
                // matched).
                //
                // `coninhcount` is deliberately NOT included: it is not part of
                // `TableConstraint::PartialEq` or `add_to_hasher` either, and
                // inherited NOT NULL constraints on partition children are
                // filtered out earlier by the
                // `is_target_partition && coninhcount > 0` guard, so a change
                // in `coninhcount` cannot reach this branch in the
                // partitioned-table case. For plain `CREATE TABLE ... INHERITS`
                // hierarchies a `coninhcount` flip would still be silently
                // accepted, but that matches the existing behavior for every
                // other constraint type and is intentionally out of scope here.
                let both_auto_nn = old_constraint.auto_not_null_column(&self.name).is_some()
                    && new_constraint
                        .auto_not_null_column(&to_table.name)
                        .is_some();
                let nn_equivalent = both_auto_nn
                    && old_constraint.is_enforced == new_constraint.is_enforced
                    && old_constraint.no_inherit == new_constraint.no_inherit
                    && old_constraint.comment == new_constraint.comment;

                if old_constraint != new_constraint && !nn_equivalent {
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
            if find_new(old_constraint).is_none() {
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
                    // Check if only the comment changed (no need to drop+recreate)
                    let def_changed = old_index.indexdef != new_index.indexdef;
                    if def_changed {
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
                    } else {
                        // Only comment changed
                        if let Some(ref cmt) = new_index.comment {
                            index_script.append_block(&format!(
                                "comment on index {}.{} is '{}';",
                                new_index.schema,
                                new_index.name,
                                cmt.replace('\'', "''")
                            ));
                        } else if old_index.comment.is_some() {
                            index_script.append_block(&format!(
                                "comment on index {}.{} is null;",
                                new_index.schema, new_index.name
                            ));
                        }
                    }
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

        // UNLOGGED / LOGGED change is emitted by the comparer in a
        // separate FK-ordered phase (see issue #180). Doing it inline
        // here would interleave SET UNLOGGED/LOGGED statements with the
        // alphabetical table order PostgreSQL rejects:
        //   * SET UNLOGGED fails if any LOGGED table references this
        //     one — referencers (leaves) must be converted first.
        //   * SET LOGGED   fails if this table references any UNLOGGED
        //     one — referenced tables (roots) must be converted first.
        // Both directions therefore need a topological pass that this
        // per-table function can't perform. The comparer collects
        // `Self::persistence_change_for(&to_table)` results from every
        // pair and emits them in the right order at end of compare.

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

    /// True when comparing `self` (FROM) against `to_table` (TO) would
    /// force `build_alter_script` down a wholesale `DROP TABLE` +
    /// `CREATE TABLE` path rather than an in-place ALTER. The comparer
    /// uses this to drive the `recreated_tables` flag for grants
    /// (issue #180) and to skip Path B column-dependent restoration
    /// (issue #188) — when the table is recreated wholesale, all
    /// dependents are already re-emitted as part of `CREATE TABLE`,
    /// so the column-dependent graph walk would just duplicate work.
    ///
    /// Conditions mirror the early-return branches in
    /// `build_alter_script`. `column_type_change_forces_recreate`
    /// keeps the partition-key column check in a single helper so the
    /// predicate and the SQL emission cannot drift apart.
    pub fn will_be_dropped_and_recreated(&self, to_table: &Table) -> bool {
        if self.partition_key != to_table.partition_key {
            return true;
        }
        for new_col in &to_table.columns {
            if let Some(old_col) = self.columns.iter().find(|c| c.name == new_col.name)
                && self.column_type_change_forces_recreate(old_col, new_col)
            {
                return true;
            }
        }
        false
    }

    /// True when a column's type changed *and* the column either
    /// participates in this table's partition key or the table is a
    /// partition child — both cases require dropping and recreating
    /// the table (PostgreSQL forbids in-place type changes on
    /// partition-key columns and on inherited columns of partition
    /// children).
    ///
    /// Shared between `build_alter_script`'s emission path and
    /// `will_be_dropped_and_recreated`'s predicate so the recreate
    /// decision lives in exactly one place.
    fn column_type_change_forces_recreate(
        &self,
        old_col: &TableColumn,
        new_col: &TableColumn,
    ) -> bool {
        let type_changed = old_col.data_type != new_col.data_type
            || old_col.udt_name != new_col.udt_name
            || old_col.numeric_precision != new_col.numeric_precision
            || old_col.numeric_scale != new_col.numeric_scale
            || old_col.character_maximum_length != new_col.character_maximum_length;
        if !type_changed {
            return false;
        }
        let is_partition_child = self.partition_of.is_some();
        let in_partition_key = self.partition_key.as_ref().is_some_and(|pk| {
            let col_lower = new_col.name.to_lowercase();
            extract_partition_key_identifiers(pk).contains(&col_lower)
        });
        is_partition_child || in_partition_key
    }

    /// Returns `Some(target_is_unlogged)` when this table's logging
    /// status differs from `to_table`. The comparer collects these per
    /// (FROM, TO) pair and emits the resulting `ALTER TABLE ... SET
    /// LOGGED|UNLOGGED` statements in FK-topological order (see issue
    /// #180): inline emission in `build_alter_script` would honour the
    /// alphabetical table iteration order, which PostgreSQL rejects
    /// whenever a referencing-table / referenced-table pair flip
    /// persistence in the same migration.
    pub fn persistence_change_for(&self, to_table: &Table) -> Option<bool> {
        if self.is_unlogged != to_table.is_unlogged {
            Some(to_table.is_unlogged)
        } else {
            None
        }
    }

    fn get_trigger_alter_parts(&self, to_table: &Table, use_drop: bool) -> (String, String) {
        let mut trigger_script = String::new();
        let mut trigger_drop_script = String::new();

        for new_trigger in &to_table.triggers {
            if let Some(old_trigger) = self.triggers.iter().find(|t| t.name == new_trigger.name) {
                if old_trigger != new_trigger {
                    if old_trigger.definition != new_trigger.definition {
                        // Definition changed: emit drop in drop section, new trigger in create section
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
                        trigger_script
                            .push_str(&new_trigger.get_script(&to_table.schema, &to_table.name));
                        // Handle enabled/comment changes too
                        let alter = old_trigger.get_alter_script(
                            new_trigger,
                            &self.schema,
                            &self.name,
                            use_drop,
                        );
                        // get_alter_script already emitted drop + definition; skip those lines,
                        // only take the enable/comment parts (lines after the new definition)
                        // by using a fresh get_alter_script call that ignores definition changes:
                        // Actually, easier: only emit enabled/comment changes here
                        let _ = alter; // already handled above
                        if old_trigger.enabled != new_trigger.enabled {
                            let stmt = match new_trigger.enabled.as_str() {
                                "D" => format!(
                                    "alter table {}.{} disable trigger {};",
                                    to_table.schema, to_table.name, new_trigger.name
                                ),
                                "R" => format!(
                                    "alter table {}.{} enable replica trigger {};",
                                    to_table.schema, to_table.name, new_trigger.name
                                ),
                                "A" => format!(
                                    "alter table {}.{} enable always trigger {};",
                                    to_table.schema, to_table.name, new_trigger.name
                                ),
                                _ => format!(
                                    "alter table {}.{} enable trigger {};",
                                    to_table.schema, to_table.name, new_trigger.name
                                ),
                            };
                            trigger_script.append_block(&stmt);
                        }
                        if old_trigger.comment != new_trigger.comment {
                            if let Some(ref comment) = new_trigger.comment {
                                trigger_script.append_block(&format!(
                                    "comment on trigger {} on {}.{} is '{}';",
                                    new_trigger.name,
                                    to_table.schema,
                                    to_table.name,
                                    comment.replace('\'', "''")
                                ));
                            } else {
                                trigger_script.append_block(&format!(
                                    "comment on trigger {} on {}.{} is null;",
                                    new_trigger.name, to_table.schema, to_table.name
                                ));
                            }
                        }
                    } else {
                        // Only enabled/comment changed — no drop needed
                        let alter = old_trigger.get_alter_script(
                            new_trigger,
                            &self.schema,
                            &self.name,
                            use_drop,
                        );
                        trigger_script.push_str(&alter);
                    }
                }
            } else {
                trigger_script.push_str(&new_trigger.get_script(&to_table.schema, &to_table.name));
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
#[path = "table_tests.rs"]
mod tests;
