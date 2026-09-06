//! Table columns (`pg_attribute` / `information_schema.columns`).
//!
//! Most changes are an in-place `ALTER COLUMN`, but some — a type change with no
//! valid cast, a stored ↔ virtual generated-column flip — require `DROP COLUMN`
//! plus `ADD COLUMN`. That distinction matters beyond this module:
//! [`TableColumn::would_drop_and_re_add`] is what tells the comparer that
//! PostgreSQL will CASCADE away the column's indexes, constraints and policies,
//! which then have to be re-emitted.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::utils::string_extensions::StringExt;

/// Whether two optional generated-column expressions are equivalent, ignoring the
/// non-idempotent ways PostgreSQL deparses an `IN`-list expression (issue #226).
fn generation_expressions_equivalent(a: &Option<String>, b: &Option<String>) -> bool {
    match (a, b) {
        (Some(x), Some(y)) => {
            x == y
                || crate::utils::sql_normalize::canonicalize_definition(x)
                    == crate::utils::sql_normalize::canonicalize_definition(y)
        }
        (None, None) => true,
        _ => false,
    }
}

/// This is an information about a PostgreSQL table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableColumn {
    /// Catalog name
    pub catalog: String,
    /// Schema name
    pub schema: String,
    /// Table name
    pub table: String,
    /// Column name
    pub name: String,
    /// Ordinal position of the column
    pub ordinal_position: i32,
    /// Default value of the column
    pub column_default: Option<String>,
    /// Whether the column is nullable
    pub is_nullable: bool,
    /// Data type of the column
    pub data_type: String,
    /// Maximum length for character types
    pub character_maximum_length: Option<i32>,
    /// Octet length for character types
    pub character_octet_length: Option<i32>,
    /// Numeric precision
    pub numeric_precision: Option<i32>,
    /// Numeric precision radix
    pub numeric_precision_radix: Option<i32>,
    /// Numeric scale
    pub numeric_scale: Option<i32>,
    /// Datetime precision
    pub datetime_precision: Option<i32>,
    /// Interval type
    pub interval_type: Option<String>,
    /// Interval precision
    pub interval_precision: Option<i32>,
    /// Character set catalog
    pub character_set_catalog: Option<String>,
    /// Character set schema
    pub character_set_schema: Option<String>,
    /// Character set name
    pub character_set_name: Option<String>,
    /// Collation catalog
    pub collation_catalog: Option<String>,
    /// Collation schema
    pub collation_schema: Option<String>,
    /// Collation name
    pub collation_name: Option<String>,
    /// Domain catalog
    pub domain_catalog: Option<String>,
    /// Domain schema
    pub domain_schema: Option<String>,
    /// Domain name
    pub domain_name: Option<String>,
    /// UDT catalog
    pub udt_catalog: Option<String>,
    /// UDT schema
    pub udt_schema: Option<String>,
    /// UDT name
    pub udt_name: Option<String>,
    /// Scope catalog
    pub scope_catalog: Option<String>,
    /// Scope schema
    pub scope_schema: Option<String>,
    /// Scope name
    pub scope_name: Option<String>,
    /// Maximum cardinality
    pub maximum_cardinality: Option<i32>,
    /// DTD identifier
    pub dtd_identifier: Option<String>,
    /// Whether the column is self-referencing
    pub is_self_referencing: bool,
    /// Whether the column is an identity column
    pub is_identity: bool,
    /// Identity generation method
    pub identity_generation: Option<String>,
    /// Identity start value
    pub identity_start: Option<String>,
    /// Identity increment value
    pub identity_increment: Option<String>,
    /// Identity maximum value
    pub identity_maximum: Option<String>,
    /// Identity minimum value
    pub identity_minimum: Option<String>,
    /// Whether the identity column cycles
    pub identity_cycle: bool,
    /// Whether the column is generated
    pub is_generated: String,
    /// Generation expression for the column
    pub generation_expression: Option<String>,
    /// 's' for stored, 'v' for virtual (PG18+); None treated as stored
    #[serde(default)]
    pub generation_type: Option<String>,
    /// Whether the column is updatable
    pub is_updatable: bool,
    /// Related views (optional)
    pub related_views: Option<Vec<String>>,
    /// Column comment
    #[serde(default)]
    pub comment: Option<String>,
    /// TOAST storage strategy (PLAIN, EXTERNAL, MAIN, EXTENDED)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage: Option<String>,
    /// Column compression method (pglz, lz4; PG14+)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compression: Option<String>,
    /// Per-column statistics target (attstattarget; -1 = use default)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub statistics_target: Option<i32>,
    /// Column-level ACL entries (attacl)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub acl: Vec<String>,
    /// Transient: set at comparison time to "serial", "bigserial", or "smallserial"
    #[serde(skip)]
    pub serial_type: Option<String>,
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

    fn normalized_generated(value: &str) -> String {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            "NEVER".to_string()
        } else {
            let upper = trimmed.to_uppercase();
            if upper.contains("ALWAYS") {
                "ALWAYS".to_string()
            } else {
                "NEVER".to_string()
            }
        }
    }

    /// Returns the effective generation type: `"s"` (stored) or `"v"` (virtual).
    /// Treats `None` as `"s"` so that older dumps missing the field compare
    /// equal to newer dumps that explicitly record `Some("s")`.
    fn effective_generation_type(&self) -> &str {
        match self.generation_type.as_deref() {
            Some("v") => "v",
            _ => "s",
        }
    }

    /// True when comparing `self` (the new TO-side column) against
    /// `existing` (the FROM-side column) would route through the
    /// `needs_full_recreate` branch in [`TableColumn::get_alter_script`] — i.e., the
    /// migration is `DROP COLUMN` + `ADD COLUMN` rather than an
    /// in-place ALTER. This is the Path B trigger from issue #188:
    /// PostgreSQL CASCADE-drops every index / FK / CHECK / EXCLUDE
    /// constraint / RLS policy attached to the column, and the
    /// comparer must re-emit them afterwards.
    ///
    /// `get_alter_script` calls this method to gate the actual
    /// drop+add emission, so the predicate body and the SQL emission
    /// share a single source of truth — they cannot drift apart.
    pub fn would_drop_and_re_add(&self, existing: &TableColumn) -> bool {
        let new_generated = Self::normalized_generated(&self.is_generated);
        let old_generated = Self::normalized_generated(&existing.is_generated);

        let generation_type_changed = new_generated == "ALWAYS"
            && old_generated == "ALWAYS"
            && self.effective_generation_type() != existing.effective_generation_type();
        let generated_changed = new_generated != old_generated
            || (new_generated == "ALWAYS"
                && self.generation_expression != existing.generation_expression)
            || generation_type_changed;

        if !generated_changed {
            return false;
        }

        let new_is_generated = new_generated == "ALWAYS";
        let old_is_generated = old_generated != "NEVER";

        (!old_is_generated && new_is_generated)
            || (old_is_generated
                && (existing.effective_generation_type() == "v"
                    || self.effective_generation_type() == "v"))
    }

    fn normalized_generation_expression(expr: &str) -> String {
        let mut trimmed = expr.trim();
        // Strip redundant outer parentheses to avoid emitted ((expr)) which some servers reject
        while trimmed.starts_with('(') && trimmed.ends_with(')') && trimmed.len() > 2 {
            let candidate = trimmed[1..trimmed.len() - 1].trim();
            trimmed = candidate;
        }
        trimmed.to_string()
    }

    fn build_identity_add_statement(&self, existing: &TableColumn) -> String {
        let generation = Self::normalized_identity_generation(self.identity_generation.as_ref());
        let mut statement = format!(
            "alter table {}.{} alter column {} add generated {} as identity",
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
            statements.push(
                format!(
                    "alter table {}.{} alter column {} set generated {};",
                    self.schema, self.table, self.name, new_generation
                )
                .with_empty_lines(),
            );
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
            statements.push(
                format!(
                    "alter table {}.{} alter column {} set {};",
                    self.schema,
                    self.table,
                    self.name,
                    option.to_uppercase()
                )
                .with_empty_lines(),
            );
        }
    }

    /// Hash
    /// Whether this column's **type** differs from `other`'s, in any way that
    /// makes the comparer emit `ALTER TABLE ... ALTER COLUMN ... TYPE`.
    ///
    /// That statement is what PostgreSQL refuses while a view or rule depends on
    /// the column ("cannot alter type of a column used by a view or rule"), and
    /// it refuses it for the *statement*, not for whether the type meaningfully
    /// changed — a collation-only retype is rejected just as firmly. So this
    /// predicate has to answer for exactly the set of changes that produce the
    /// statement, and its first term is the emission's own test.
    ///
    /// Defining it any other way is a trap this codebase already fell into
    /// (PR #247 review). Spelling out a list of type attributes here left
    /// `interval_type` and `collation_name` off it, because they reach the type
    /// clause through `render_type_clause` rather than through a field
    /// comparison. A column whose collation changed *and* whose default changed
    /// then produced a migration that altered the type with the materialized
    /// view still in place, and PostgreSQL rejected it — verified live on
    /// PostgreSQL 16, and the reason there is a test for it.
    ///
    /// The remaining terms are what the partition-recreate gate compared before
    /// this became shared, and they stay: `udt_name` distinguishes two
    /// `USER-DEFINED` columns of different types, which the rendered clause
    /// cannot, and the precision and length comparisons are unconditional where
    /// the clause only renders them for the types they apply to. The union is at
    /// least as eager as either caller was on its own — and for both callers,
    /// eager is the safe direction: an unnecessary drop or recreate costs time,
    /// a missing one produces SQL PostgreSQL will not run.
    pub fn type_differs(&self, other: &TableColumn) -> bool {
        self.type_clause_differs(other)
            || self.udt_name != other.udt_name
            || self.numeric_precision != other.numeric_precision
            || self.numeric_scale != other.numeric_scale
            || self.character_maximum_length != other.character_maximum_length
    }

    pub fn add_to_hasher(&self, hasher: &mut Sha256) {
        hasher.update(self.name.as_bytes());
        hasher.update(self.data_type.as_bytes());
        hasher.update(self.is_nullable.to_string().as_bytes());

        hasher.update(Self::normalized_generated(&self.is_generated).as_bytes());

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
            // Canonicalize so a non-idempotent IN-list deparse in the generation
            // expression does not make the column look changed every run (issue #226).
            hasher.update(crate::utils::sql_normalize::canonicalize_definition(expr).as_bytes());
        }
        hasher.update(self.effective_generation_type().as_bytes());
        if let Some(comment) = &self.comment {
            hasher.update(comment.as_bytes());
        }
        if let Some(storage) = &self.storage {
            hasher.update(storage.as_bytes());
        }
        if let Some(compression) = &self.compression {
            hasher.update(compression.as_bytes());
        }
        if let Some(stats) = self.statistics_target {
            hasher.update(stats.to_string().as_bytes());
        }
        // skip catalog/charset/related_views and other descriptive-only fields
    }

    pub fn get_comment_script(&self) -> Option<String> {
        self.comment.as_ref().map(|comment| {
            format!(
                "comment on column {}.{}.{} is '{}';",
                self.schema,
                self.table,
                self.name,
                comment.replace('\'', "''")
            )
            .with_empty_lines()
        })
    }

    /// Returns a string representation of the column
    pub fn get_script(&self) -> String {
        let mut script = String::new();
        // Name
        script.push_str(&format!("{} ", self.name));

        // If this column is a serial/bigserial/smallserial type, output the serial type directly
        // and skip the default clause (the sequence is created automatically by PostgreSQL).
        if let Some(ref serial_type) = self.serial_type {
            script.push_str(serial_type);
            if !self.is_nullable {
                script.push_str(" not null");
            }
            return script.trim_end().to_string();
        }

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
            let norm_expr = Self::normalized_generation_expression(expr);
            let wrapped = format!("({norm_expr})");
            let gen_kind = match self.generation_type.as_deref() {
                Some("v") => "virtual",
                _ => "stored",
            };
            script.push_str(&format!(" generated always as {wrapped} {gen_kind}"));
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

    pub fn get_alter_script(&self, existing: &TableColumn, use_drop: bool) -> Option<String> {
        let mut statements = Vec::new();

        let new_generated = Self::normalized_generated(&self.is_generated);
        let old_generated = Self::normalized_generated(&existing.is_generated);

        // Identity is dropped FIRST, ahead of the type, default and
        // nullability changes below, and added LAST, after all three. Both
        // ends of that follow from what an identity column *is*: PostgreSQL
        // holds it NOT NULL, forbids it a default, and restricts it to an
        // integer type, and it enforces all three at the moment of the ALTER
        // rather than at the end of the transaction. So while the column is
        // still an identity column none of the three can be relaxed, and
        // before it can become one all three have to be true already.
        //
        // Verified live on PostgreSQL 16 — each of these is the error the
        // wrong order produces:
        //
        //   drop not null   before drop identity  -> column "id" ... is an
        //                                            identity column
        //   set default     before drop identity  -> column "id" ... is an
        //                                            identity column
        //   alter type text before drop identity  -> identity column type must
        //                                            be smallint, integer, or
        //                                            bigint
        //   add identity    before set not null   -> column "id" ... must be
        //                                            declared NOT NULL before
        //                                            identity can be added
        //   add identity    before drop default   -> column "id" ... already
        //                                            has a default value
        //
        // Only the first of those was reported (issue #243); the other two
        // drop-side orderings were broken the same way and fail the same way.
        let identity_dropped = existing.is_identity && !self.is_identity;
        if identity_dropped {
            let drop_cmd = format!(
                "alter table {}.{} alter column {} drop identity if exists;",
                self.schema, self.table, self.name
            )
            .with_empty_lines();
            if use_drop {
                statements.push(drop_cmd);
            } else {
                statements.push(
                    drop_cmd
                        .lines()
                        .map(|l| format!("-- {}\n", l))
                        .collect::<String>(),
                );
            }
        }

        if self.type_clause_differs(existing) {
            statements.push(
                format!(
                    "alter table {}.{} alter column {} type {};",
                    self.schema,
                    self.table,
                    self.name,
                    self.render_type_clause()
                )
                .with_empty_lines(),
            );
        }

        if self.column_default != existing.column_default {
            match &self.column_default {
                Some(default) => statements.push(
                    format!(
                        "alter table {}.{} alter column {} set default {};",
                        self.schema, self.table, self.name, default
                    )
                    .with_empty_lines(),
                ),
                None => {
                    let drop_cmd = format!(
                        "alter table {}.{} alter column {} drop default;",
                        self.schema, self.table, self.name
                    )
                    .with_empty_lines();
                    if use_drop {
                        statements.push(drop_cmd);
                    } else {
                        statements.push(
                            drop_cmd
                                .lines()
                                .map(|l| format!("-- {}\n", l))
                                .collect::<String>(),
                        );
                    }
                }
            }
        }

        if self.is_nullable != existing.is_nullable {
            if self.is_nullable {
                if use_drop {
                    statements.push(
                        format!(
                            "alter table {}.{} alter column {} drop not null;",
                            self.schema, self.table, self.name
                        )
                        .with_empty_lines(),
                    );
                } else {
                    let commented = format!(
                        "-- use_drop=false: would drop NOT NULL\n-- alter table {}.{} alter column {} drop not null;\n",
                        self.schema, self.table, self.name
                    );
                    statements.push(commented);
                }
            } else {
                statements.push(
                    format!(
                        "alter table {}.{} alter column {} set not null;",
                        self.schema, self.table, self.name
                    )
                    .with_empty_lines(),
                );
            }
        }

        // The add side. Its drop counterpart was emitted at the top of this
        // function; see the ordering note there.
        if self.is_identity && !existing.is_identity {
            statements.push(self.build_identity_add_statement(existing));
        } else if self.is_identity && existing.is_identity {
            // Still an identity column on both sides, only its parameters
            // moved — no ordering constraint, the column never stops being
            // NOT NULL or gains a default.
            self.build_identity_update_statements(existing, &mut statements);
        }

        // Issue #181: `generation_type` (`s`/`v`) flips must also count
        // as a change. PG18 introduced VIRTUAL generated columns and
        // they CANNOT be ALTERed in place — neither
        // `ALTER COLUMN DROP EXPRESSION` (which only works for
        // STORED) nor `ALTER COLUMN ADD GENERATED ALWAYS AS (...)
        // VIRTUAL` (invalid syntax) is valid. Without this check a
        // STORED ↔ VIRTUAL flip with the same expression would be
        // silently skipped.
        let generation_type_changed = new_generated == "ALWAYS"
            && old_generated == "ALWAYS"
            && self.effective_generation_type() != existing.effective_generation_type();
        let generated_changed = new_generated != old_generated
            || (new_generated == "ALWAYS"
                && self.generation_expression != existing.generation_expression)
            || generation_type_changed;

        if generated_changed {
            let new_is_generated = new_generated == "ALWAYS";
            let old_is_generated = old_generated != "NEVER";

            // The drop+add gate lives in `would_drop_and_re_add` so the
            // comparer's Path B detection (issue #188) cannot drift
            // from this emission site. The predicate inlines the same
            // `generated_changed` short-circuit above, so calling it
            // here is redundant but safe — and it keeps the invariant
            // enforced by code, not by a comment. Rationale: PG18+
            // VIRTUAL generated columns reject in-place ALTER
            // (`DROP EXPRESSION` only works for STORED; the
            // `ADD GENERATED ... VIRTUAL` form is invalid syntax), so
            // any participant that is VIRTUAL on either side — plus
            // the non-generated→GENERATED transition — has no choice
            // but the full DROP COLUMN + ADD COLUMN round-trip.
            // (Issue #181)
            let needs_full_recreate = self.would_drop_and_re_add(existing);

            if needs_full_recreate {
                if use_drop {
                    statements.push(self.get_drop_script());
                    statements.push(self.get_add_script());
                } else {
                    let header = if !old_is_generated && new_is_generated {
                        "-- use_drop=false: converting column to generated requires drop/add; statements commented out\n"
                    } else {
                        "-- use_drop=false: virtual generated column cannot be ALTERed in place (PG18+); drop/add required and statements commented out\n"
                    };
                    let mut commented = String::from(header);
                    for line in self.get_drop_script().lines() {
                        commented.push_str(&format!("-- {line}\n"));
                    }
                    for line in self.get_add_script().lines() {
                        commented.push_str(&format!("-- {line}\n"));
                    }
                    statements.push(commented);
                }
            } else {
                if old_is_generated {
                    let drop_cmd = format!(
                        "alter table {}.{} alter column {} drop expression;",
                        self.schema, self.table, self.name
                    )
                    .with_empty_lines();
                    if use_drop {
                        statements.push(drop_cmd);
                    } else {
                        statements.push(
                            drop_cmd
                                .lines()
                                .map(|l| format!("-- {}\n", l))
                                .collect::<String>(),
                        );
                    }
                }

                if new_is_generated && let Some(expr) = &self.generation_expression {
                    let norm_expr = Self::normalized_generation_expression(expr);
                    let wrapped = format!("({norm_expr})");
                    // We only reach this branch for STORED-side
                    // additions — `needs_full_recreate` already
                    // routed any VIRTUAL participant through the
                    // drop+add path above.
                    let add_cmd = format!(
                        "alter table {}.{} alter column {} add generated always as {wrapped} stored;",
                        self.schema, self.table, self.name
                    ).with_empty_lines();
                    if use_drop || !old_is_generated {
                        statements.push(add_cmd);
                    } else {
                        // Drop was commented out, so add would fail; comment it out too
                        statements.push("-- use_drop=false: drop expression + add generated requires drop; statements commented out (manual intervention needed)\n".to_string());
                        statements.push(
                            add_cmd
                                .lines()
                                .map(|l| format!("-- {}\n", l))
                                .collect::<String>(),
                        );
                    }
                }
            }
        }

        if self.storage != existing.storage
            && let Some(storage) = &self.storage
        {
            statements.push(
                format!(
                    "alter table {}.{} alter column {} set storage {};",
                    self.schema, self.table, self.name, storage
                )
                .with_empty_lines(),
            );
        }

        if self.compression != existing.compression {
            if let Some(compression) = &self.compression {
                statements.push(
                    format!(
                        "alter table {}.{} alter column {} set compression {};",
                        self.schema, self.table, self.name, compression
                    )
                    .with_empty_lines(),
                );
            } else {
                statements.push(
                    format!(
                        "alter table {}.{} alter column {} set compression default;",
                        self.schema, self.table, self.name
                    )
                    .with_empty_lines(),
                );
            }
        }

        if self.statistics_target != existing.statistics_target {
            if let Some(stats) = self.statistics_target
                && stats >= 0
            {
                statements.push(
                    format!(
                        "alter table {}.{} alter column {} set statistics {};",
                        self.schema, self.table, self.name, stats
                    )
                    .with_empty_lines(),
                );
            } else {
                statements.push(
                    format!(
                        "alter table {}.{} alter column {} set statistics -1;",
                        self.schema, self.table, self.name
                    )
                    .with_empty_lines(),
                );
            }
        }

        if statements.is_empty() {
            None
        } else {
            let mut joined = statements.join("");
            if self.comment != existing.comment {
                let comment_stmt = if let Some(cmt) = &self.comment {
                    format!(
                        "comment on column {}.{}.{} is '{}';",
                        self.schema,
                        self.table,
                        self.name,
                        cmt.replace('\'', "''")
                    )
                } else {
                    format!(
                        "comment on column {}.{}.{} is null;",
                        self.schema, self.table, self.name
                    )
                };
                joined.append_block(&comment_stmt);
            }
            Some(joined)
        }
    }

    pub fn get_add_script(&self) -> String {
        let mut statement = format!(
            "alter table {}.{} add column {} {}",
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
            let norm_expr = Self::normalized_generation_expression(expr);
            let wrapped = format!("({norm_expr})");
            let gen_kind = match self.generation_type.as_deref() {
                Some("v") => "virtual",
                _ => "stored",
            };
            statement.push_str(&format!(" generated always as {wrapped} {gen_kind}"));
        }

        if let Some(default) = &self.column_default {
            statement.push_str(&format!(" default {default}"));
        }

        if !self.is_nullable {
            statement.push_str(" not null");
        }

        statement.append_block(";");

        if let Some(comment) = &self.comment {
            statement.append_block(&format!(
                "comment on column {}.{}.{} is '{}';",
                self.schema,
                self.table,
                self.name,
                comment.replace('\'', "''")
            ));
        }
        statement
    }

    pub fn get_drop_script(&self) -> String {
        format!(
            "alter table {}.{} drop column {};",
            self.schema, self.table, self.name
        )
        .with_empty_lines()
    }
}

impl PartialEq for TableColumn {
    fn eq(&self, other: &Self) -> bool {
        let self_generated = Self::normalized_generated(&self.is_generated);
        let other_generated = Self::normalized_generated(&other.is_generated);

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
            && self_generated == other_generated
            && (self_generated != "ALWAYS"
                || (generation_expressions_equivalent(
                    &self.generation_expression,
                    &other.generation_expression,
                ) && self.effective_generation_type() == other.effective_generation_type()))
            && self.is_updatable == other.is_updatable
            && self.comment == other.comment
            && self.storage == other.storage
            && self.compression == other.compression
            && self.statistics_target == other.statistics_target
    }
}

#[cfg(test)]
#[path = "tests/table_column.rs"]
mod tests;
