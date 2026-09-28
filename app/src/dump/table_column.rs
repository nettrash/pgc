//! Table columns (`pg_attribute` / `information_schema.columns`).
//!
//! Most changes are an in-place `ALTER COLUMN`, but some — a type change with no
//! valid cast, a stored ↔ virtual generated-column flip — require `DROP COLUMN`
//! plus `ADD COLUMN`. That distinction matters beyond this module:
//! [`TableColumn::would_drop_and_re_add`] is what tells the comparer that
//! PostgreSQL will CASCADE away the column's indexes, constraints and policies,
//! which then have to be re-emitted. On PostgreSQL 14 a generated column made
//! plain can need the same treatment with its values kept — a new column
//! filled from the old one, which is dropped (`TableColumn::plain_needs_rewrite`).
//!
//! A generated column's expression can change in place with
//! `ALTER COLUMN ... SET EXPRESSION AS (...)` where the migrated server accepts
//! it — STORED columns on PostgreSQL 17+, VIRTUAL ones on PostgreSQL 19+ (and
//! on 18 in a table without CHECK constraints) unless the table is an explicit
//! publication member. The comparer works that out per table and passes it in
//! as a [`GeneratedAlterCaps`]; without it the change is a `DROP COLUMN` +
//! `ADD COLUMN`.
//!
//! Either form can depend on columns the same migration adds — a new
//! expression may read a new column — so the table alter path takes them from
//! `TableColumn::alter_parts` separately and emits them after the table's
//! `ADD COLUMN`s.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::utils::string_extensions::{
    StringExt, comment_out, line_comment_safe, quote_identifier,
};

/// One column's alter statements, split by where the table alter path has to
/// emit them (see `TableColumn::alter_parts`). Concatenated in field order
/// they are exactly what [`TableColumn::get_alter_script_with_caps`] returns.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ColumnAlterParts {
    /// Statements that run in column order, among the table's
    /// `ADD COLUMN`s: everything, unless the generated part is deferred.
    pub(crate) inline: String,
    /// The `DROP COLUMN` of a live drop + add; empty otherwise.
    pub(crate) drop: String,
    /// The deferred generated part — `SET EXPRESSION`, or the `ADD COLUMN`
    /// of a live drop + add. It may read a column the migration adds, so it
    /// runs after every `ADD COLUMN` of the table. Empty when nothing is
    /// deferred.
    pub(crate) deferred: String,
    /// Storage, compression and statistics changes that follow a deferred
    /// part (they must reach the re-added column, not the dropped one).
    pub(crate) post: String,
    /// The column comment change that follows a deferred part.
    pub(crate) comment: String,
}

impl ColumnAlterParts {
    /// Whether the generated part is deferred past the table's `ADD COLUMN`s.
    pub(crate) fn is_deferred(&self) -> bool {
        !self.deferred.is_empty()
    }

    fn concat(self) -> Option<String> {
        let joined = [
            self.inline,
            self.drop,
            self.deferred,
            self.post,
            self.comment,
        ]
        .concat();
        (!joined.is_empty()).then_some(joined)
    }
}

/// Where the migrated server (the FROM database) accepts
/// `ALTER TABLE ... ALTER COLUMN c SET EXPRESSION AS (expr)` for the table
/// being altered. The [`Default`] — both flags off — is the conservative
/// answer for a server of unknown version: every expression change becomes a
/// `DROP COLUMN` + `ADD COLUMN`, which every supported server accepts.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GeneratedAlterCaps {
    /// SET EXPRESSION on a STORED generated column (PostgreSQL 17+).
    pub stored_set_expression: bool,
    /// SET EXPRESSION on a VIRTUAL generated column: PostgreSQL 19+, or 18
    /// when the table has no CHECK constraints; never while the table (or a
    /// child it recurses to) is an explicit `FOR TABLE` publication member.
    pub virtual_set_expression: bool,
}

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

/// Whether the `(` that starts `expr` is closed by the `)` that ends it, so
/// one pair of parentheses encloses the whole expression. Parentheses inside
/// string literals and quoted identifiers (with their doubled-quote escapes)
/// do not count. Only ASCII bytes are matched, so walking bytes is safe for
/// any UTF-8 input.
fn is_enclosed_in_parens(expr: &str) -> bool {
    let bytes = expr.as_bytes();
    if bytes.len() < 2 || bytes[0] != b'(' || bytes[bytes.len() - 1] != b')' {
        return false;
    }
    let last = bytes.len() - 1;
    let mut depth = 0usize;
    let mut quote: Option<u8> = None;
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        match quote {
            Some(q) if b == q => {
                if bytes.get(i + 1) == Some(&q) {
                    i += 1; // A doubled quote stays inside the literal.
                } else {
                    quote = None;
                }
            }
            Some(_) => {}
            None => match b {
                b'\'' | b'"' => quote = Some(b),
                b'(' => depth += 1,
                b')' => {
                    depth = depth.saturating_sub(1);
                    if depth == 0 && i != last {
                        return false; // The opening pair closed early.
                    }
                }
                _ => {}
            },
        }
        i += 1;
    }
    depth == 0 && quote.is_none()
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
    /// PostgreSQL 14 only: this generated column's expression uses an object
    /// — a function, an operator, a type or collation other than the
    /// column's own — that PostgreSQL 14 records as a dependency of the
    /// column itself (15 and later record it on the column's default,
    /// `pg_attrdef`). PostgreSQL 14 then refuses `ALTER COLUMN ... TYPE` on
    /// it ("found unexpected dependency for column"), so the comparer drops
    /// and re-adds such a column to change its type (see
    /// `TableColumn::retype_needs_drop_and_add`).
    ///
    /// A fact about the dumped server, not about the schema: left out of
    /// `PartialEq` and the hash, and only read on the FROM side. Always
    /// `false` in a dump of PostgreSQL 15+ and in one written by pgc 1.0.x,
    /// and then left out of the dump.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub generation_blocks_retype: bool,
    /// Transient, set by the comparer on a FROM column whose
    /// [`TableColumn::generation_blocks_retype`] is set: the migration drops
    /// a routine this column's expression calls with `DROP ... CASCADE`,
    /// which on PostgreSQL 14 takes the column along even once `DROP
    /// EXPRESSION` has made it plain (see `TableColumn::plain_needs_rewrite`).
    /// Never written to a dump, and left out of `PartialEq` and the hash.
    #[serde(skip)]
    pub generation_routine_dropped: bool,
    /// Transient: set at comparison time to "serial", "bigserial", or "smallserial"
    #[serde(skip)]
    pub serial_type: Option<String>,
}

impl TableColumn {
    /// Render the type clause for alter statements (data type, length, collation, interval)
    pub(crate) fn render_type_clause(&self) -> String {
        let mut clause = self.render_type_clause_without_collation();
        clause.push_str(&self.collate_clause());
        clause
    }

    /// [`TableColumn::render_type_clause`] without the `collate` clause.
    fn render_type_clause_without_collation(&self) -> String {
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

        clause
    }

    /// The column's ` collate <name>` clause, with its leading space, or an
    /// empty string when the column has no collation of its own.
    ///
    /// The dump stores the collation's schema and name raw (`nspname`,
    /// `collname`), so both are quoted here. A `pg_catalog` collation (`"C"`,
    /// `"POSIX"`, `"en_US"`, `"ucs_basic"`) keeps the spelling every pgc
    /// version has emitted — the bare name, always in double quotes, which
    /// resolves because `pg_catalog` is always searched first. Any other
    /// collation is qualified with its schema, since it may not be on the
    /// migrated database's `search_path` (or a same-named collation earlier
    /// on it would silently take its place). A collation schema the dump does
    /// not record — a hand-built fixture — is treated as `pg_catalog`.
    ///
    /// A name holding a `"` has it doubled, so it cannot end the identifier
    /// early and run the rest of itself as SQL.
    fn collate_clause(&self) -> String {
        let Some(name) = self.collation_name.as_deref().filter(|n| !n.is_empty()) else {
            return String::new();
        };
        match self
            .collation_schema
            .as_deref()
            .filter(|s| !s.is_empty() && *s != "pg_catalog")
        {
            None => format!(" collate \"{}\"", name.replace('"', "\"\"")),
            Some(schema) => format!(
                " collate {}.{}",
                quote_identifier(schema),
                quote_identifier(name)
            ),
        }
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
    ///
    /// Assumes a migrated server that cannot `SET EXPRESSION`; see
    /// [`TableColumn::would_drop_and_re_add_with_caps`].
    pub fn would_drop_and_re_add(&self, existing: &TableColumn) -> bool {
        self.would_drop_and_re_add_with_caps(existing, GeneratedAlterCaps::default())
    }

    /// Like [`TableColumn::would_drop_and_re_add`], for a migrated server with
    /// the `SET EXPRESSION` capabilities in `caps`.
    ///
    /// Every generated-column change is a drop + add except two: a STORED
    /// column losing its expression (`DROP EXPRESSION`), and an expression
    /// change the server can apply with `SET EXPRESSION`. In particular a
    /// STORED expression change without `caps.stored_set_expression` is a
    /// drop + add — PostgreSQL has no `ALTER COLUMN ... ADD GENERATED ALWAYS
    /// AS (...) STORED`.
    pub fn would_drop_and_re_add_with_caps(
        &self,
        existing: &TableColumn,
        caps: GeneratedAlterCaps,
    ) -> bool {
        if self.retype_needs_drop_and_add(existing) || self.plain_needs_rewrite(existing) {
            return true;
        }
        if !self.generated_changed(existing) || self.can_set_expression(existing, caps) {
            return false;
        }
        let new_is_generated = Self::normalized_generated(&self.is_generated) == "ALWAYS";
        let old_is_virtual = Self::normalized_generated(&existing.is_generated) == "ALWAYS"
            && existing.effective_generation_type() == "v";
        // Anything that ends up generated (plain -> generated, a STORED <->
        // VIRTUAL flip, an expression change SET EXPRESSION cannot make),
        // plus VIRTUAL -> plain (DROP EXPRESSION is STORED-only).
        new_is_generated || old_is_virtual
    }

    /// Whether changing `existing` (the FROM column) into `self` changes the
    /// type of a column that stays generated while the migrated server,
    /// PostgreSQL 14, refuses `ALTER COLUMN ... TYPE` on it
    /// ([`TableColumn::generation_blocks_retype`]). Such a column is dropped
    /// and re-added instead, like a generated-column change `ALTER COLUMN`
    /// cannot make: [`TableColumn::would_drop_and_re_add_with_caps`] answers
    /// true for it, so the comparer restores what the drop takes along.
    pub(crate) fn retype_needs_drop_and_add(&self, existing: &TableColumn) -> bool {
        existing.generation_blocks_retype
            && self.is_generated_column()
            && existing.is_generated_column()
            && self.type_clause_differs(existing)
    }

    /// Whether turning the generated column `existing` (FROM) into the plain
    /// column `self` has to rewrite it, keeping its values, rather than run
    /// `ALTER COLUMN ... DROP EXPRESSION`. On PostgreSQL 14
    /// ([`TableColumn::generation_blocks_retype`]) the column keeps
    /// depending on what its expression used after `DROP EXPRESSION`, so an
    /// `ALTER COLUMN ... TYPE` still fails ("found unexpected dependency for
    /// column"), and a `DROP ... CASCADE` of a routine it called takes the
    /// column, data and all. The rewrite is needed when the type changes
    /// too, or when the migration drops such a routine
    /// ([`TableColumn::generation_routine_dropped`]); otherwise `DROP
    /// EXPRESSION` stays. Never for an identity column.
    ///
    /// The rewrite adds the TO column under a temporary name, fills it from
    /// the old one with `ALTER COLUMN ... TYPE ... USING`, which rewrites the
    /// table without firing its triggers, drops the old column and renames
    /// the new one (see `TableColumn::plain_rewrite_parts`), so
    /// [`TableColumn::would_drop_and_re_add_with_caps`] answers true for it
    /// and the comparer restores what the drop takes along.
    pub(crate) fn plain_needs_rewrite(&self, existing: &TableColumn) -> bool {
        existing.generation_blocks_retype
            && existing.is_generated_column()
            && !self.is_generated_column()
            && !self.is_identity
            && (self.type_clause_differs(existing) || existing.generation_routine_dropped)
    }

    /// The name `TableColumn::plain_rewrite_parts` gives the new column until
    /// it replaces this one: `pgc_rewrite_` and this column's name, quoted,
    /// cut to PostgreSQL's 63-byte identifier limit.
    fn rewrite_temp_name(&self) -> String {
        let raw = match self
            .name
            .strip_prefix('"')
            .and_then(|n| n.strip_suffix('"'))
        {
            Some(quoted) => quoted.replace("\"\"", "\""),
            None => self.name.clone(),
        };
        let mut temp = format!("pgc_rewrite_{raw}");
        if temp.len() > 63 {
            let mut end = 63;
            while !temp.is_char_boundary(end) {
                end -= 1;
            }
            temp.truncate(end);
        }
        format!("\"{}\"", temp.replace('"', "\"\""))
    }

    /// The statements of the rewrite `TableColumn::plain_needs_rewrite`
    /// calls for, as `(drop part, deferred part)` of a [`ColumnAlterParts`]:
    /// the new column is added under a temporary name, filled from the old
    /// one and the old one dropped where a drop + add drops; it takes the
    /// old name, with the TO default, `NOT NULL`, storage, compression and
    /// statistics target, where a drop + add adds.
    fn plain_rewrite_parts(&self) -> (String, String) {
        let temp = self.rewrite_temp_name();
        let table = format!("{}.{}", self.schema, self.table);
        let type_clause = self.render_type_clause();
        let mut drop = String::new();
        drop.push_str(
            &format!("alter table {table} add column {temp} {type_clause};").with_empty_lines(),
        );
        drop.push_str(
            &format!(
                "alter table {table} alter column {temp} type {type_clause} using ({});",
                self.name
            )
            .with_empty_lines(),
        );
        drop.push_str(&self.get_drop_script());
        let mut deferred = format!("alter table {table} rename column {temp} to {};", self.name)
            .with_empty_lines();
        if let Some(default) = &self.column_default {
            deferred.push_str(
                &format!(
                    "alter table {table} alter column {} set default {default};",
                    self.name
                )
                .with_empty_lines(),
            );
        }
        if !self.is_nullable {
            deferred.push_str(
                &format!(
                    "alter table {table} alter column {} set not null;",
                    self.name
                )
                .with_empty_lines(),
            );
        }
        if let Some(storage) = &self.storage {
            deferred.push_str(
                &format!(
                    "alter table {table} alter column {} set storage {storage};",
                    self.name
                )
                .with_empty_lines(),
            );
        }
        if let Some(compression) = &self.compression {
            deferred.push_str(
                &format!(
                    "alter table {table} alter column {} set compression {compression};",
                    self.name
                )
                .with_empty_lines(),
            );
        }
        if let Some(target) = self.statistics_target.filter(|target| *target >= 0) {
            deferred.push_str(
                &format!(
                    "alter table {table} alter column {} set statistics {target};",
                    self.name
                )
                .with_empty_lines(),
            );
        }
        (drop, deferred)
    }

    /// Whether this is a generated column, STORED or VIRTUAL.
    pub(crate) fn is_generated_column(&self) -> bool {
        Self::normalized_generated(&self.is_generated) == "ALWAYS"
    }

    /// Whether this generated column's expression reads the column named
    /// `column` (in the `quote_ident` form the dump stores names in).
    ///
    /// `pg_get_expr` prints a generated column's expression with its column
    /// references unqualified and in that same form, so they are found as
    /// identifier tokens outside string literals — skipping one that names
    /// a function (followed by `(`), a type (after `::`) or part of a
    /// qualified name (next to a `.`). A token that is none of these but
    /// happens to spell the name is still counted: the caller then does
    /// more than it had to, never less. `false` for a column that is not
    /// generated.
    pub(crate) fn generation_expression_reads(&self, column: &str) -> bool {
        if !self.is_generated_column() {
            return false;
        }
        let Some(expr) = self.generation_expression.as_deref() else {
            return false;
        };
        let bytes = expr.as_bytes();
        let mut i = 0;
        while i < bytes.len() {
            let b = bytes[i];
            let start = i;
            if b == b'\'' {
                // A literal; a doubled quote stays inside it.
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == b'\'' {
                        if bytes.get(i + 1) == Some(&b'\'') {
                            i += 1;
                        } else {
                            break;
                        }
                    }
                    i += 1;
                }
                i += 1;
                continue;
            }
            if b == b'"' {
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == b'"' {
                        if bytes.get(i + 1) == Some(&b'"') {
                            i += 1;
                        } else {
                            break;
                        }
                    }
                    i += 1;
                }
                i = (i + 1).min(bytes.len());
            } else if b.is_ascii_alphabetic() || b == b'_' || !b.is_ascii() {
                while i < bytes.len()
                    && (bytes[i].is_ascii_alphanumeric()
                        || bytes[i] == b'_'
                        || bytes[i] == b'$'
                        || !bytes[i].is_ascii())
                {
                    i += 1;
                }
            } else {
                i += 1;
                continue;
            }
            let token = &expr[start..i];
            let before = expr[..start].trim_end();
            let after = expr[i..].trim_start();
            let qualified = before.ends_with('.') || after.starts_with('.');
            let is_type = before.ends_with("::");
            let is_call = after.starts_with('(');
            if token == column && !qualified && !is_type && !is_call {
                return true;
            }
        }
        false
    }

    /// Whether the generated part of the column — generated or not, its
    /// expression, STORED vs VIRTUAL — differs. Expressions are compared with
    /// [`generation_expressions_equivalent`], the same test `PartialEq` and
    /// the hash use, so a non-idempotent deparse alone is not a change.
    fn generated_changed(&self, existing: &TableColumn) -> bool {
        let new_generated = Self::normalized_generated(&self.is_generated);
        let old_generated = Self::normalized_generated(&existing.is_generated);
        // Issue #181: a `generation_type` (`s`/`v`) flip with the same
        // expression counts as a change too.
        new_generated != old_generated
            || (new_generated == "ALWAYS"
                && (!generation_expressions_equivalent(
                    &self.generation_expression,
                    &existing.generation_expression,
                ) || self.effective_generation_type() != existing.effective_generation_type()))
    }

    /// True when the change is an expression change of a generated column
    /// that stays generated of the same kind and type, and the migrated server
    /// can apply it with `ALTER COLUMN ... SET EXPRESSION AS (...)`.
    ///
    /// A type change rules it out. `ALTER COLUMN ... TYPE` would have to run
    /// first, and on a generated column it casts the *old* expression to the
    /// new type (`USING` is not allowed there), which fails whenever that cast
    /// is not automatic — `SET EXPRESSION` then fails too, because the column
    /// still has the old type. A drop + add changes both at once.
    fn can_set_expression(&self, existing: &TableColumn, caps: GeneratedAlterCaps) -> bool {
        let both_generated = Self::normalized_generated(&self.is_generated) == "ALWAYS"
            && Self::normalized_generated(&existing.is_generated) == "ALWAYS";
        let allowed = match self.effective_generation_type() {
            "v" => caps.virtual_set_expression,
            _ => caps.stored_set_expression,
        };
        both_generated
            && allowed
            && self.effective_generation_type() == existing.effective_generation_type()
            && !self.type_differs(existing)
            && self.generation_expression.is_some()
            && !generation_expressions_equivalent(
                &self.generation_expression,
                &existing.generation_expression,
            )
    }

    /// Whether altering `existing` into `self` changes the column in place in
    /// a way that makes PostgreSQL rebuild the extended statistics on it:
    /// `ALTER COLUMN ... TYPE` (a collation change included) or `SET
    /// EXPRESSION`, whether this table's own alter runs it or a parent's
    /// recurses to it. The rebuild re-creates each statistics object from its
    /// definition, which keeps the owner and the comment but resets an explicit
    /// statistics target to the default (verified live on PostgreSQL 14, 17,
    /// 18 and 19), so the comparer has to set the target again.
    ///
    /// A live drop + add is not a rebuild: the statistics go with the column
    /// and are restored whole. With `use_drop` off that drop + add is only
    /// commented out, and a type change ahead of it still runs — unless the
    /// column is generated already (see [`TableColumn::alters_type_in_place`]).
    pub(crate) fn rebuilds_statistics_in_place(
        &self,
        existing: &TableColumn,
        caps: GeneratedAlterCaps,
        use_drop: bool,
    ) -> bool {
        self.rewrites_in_place(existing, caps, use_drop)
    }

    /// Whether altering `existing` into `self` runs `ALTER COLUMN ... TYPE`
    /// or `SET EXPRESSION`, the in-place changes that recompute the column
    /// for every row. PostgreSQL rebuilds what depends on the column along
    /// the way: the extended statistics on it (see
    /// [`TableColumn::rebuilds_statistics_in_place`]), and the CHECK
    /// constraints that read it, each checked against every row while it is
    /// still enforced. Run on a partitioned table, either statement recurses
    /// to the partitions (see [`TableColumn::alters_type_in_place`] for an
    /// inheritance parent).
    ///
    /// A live drop + add is neither: it replaces the column, and what
    /// depended on it goes with the old one. With `use_drop` off that drop +
    /// add is only commented out, and a type change ahead of it still runs —
    /// unless the column is generated already (see
    /// [`TableColumn::alters_type_in_place`]).
    pub(crate) fn rewrites_in_place(
        &self,
        existing: &TableColumn,
        caps: GeneratedAlterCaps,
        use_drop: bool,
    ) -> bool {
        self.alters_type_in_place(existing, caps, use_drop)
            || (self.generated_changed(existing) && self.can_set_expression(existing, caps))
    }

    /// The `ALTER COLUMN ... TYPE` half of [`TableColumn::rewrites_in_place`]
    /// (a collation change included): the only one of the two that also
    /// recurses from an inheritance parent to its children, since the
    /// comparer writes a non-partitioned table's `SET EXPRESSION` with
    /// `ONLY`.
    ///
    /// Not when the column is dropped and re-added: live, the add carries the
    /// new type. Commented out (`use_drop` off), a plain column's type change
    /// still runs ahead of it, but a generated column's does not: PostgreSQL
    /// would recompute it from the old expression cast to the new type,
    /// which fails when no such cast exists and is superseded by the drop +
    /// add when it does.
    pub(crate) fn alters_type_in_place(
        &self,
        existing: &TableColumn,
        caps: GeneratedAlterCaps,
        use_drop: bool,
    ) -> bool {
        self.type_clause_differs(existing)
            && !(self.would_drop_and_re_add_with_caps(existing, caps)
                && (use_drop || Self::normalized_generated(&existing.is_generated) != "NEVER"))
    }

    /// Whether an `ADD COLUMN` of `other` on an inheritance parent gives a
    /// child exactly this column: the same type, nullability, default and
    /// generation. PostgreSQL copies those to the children an `ADD COLUMN`
    /// recurses to; it does not copy a comment, and a child cannot receive an
    /// identity column that way, so an identity column never qualifies.
    pub(crate) fn is_recreated_by_parent_add(&self, other: &TableColumn) -> bool {
        !self.is_identity
            && !other.is_identity
            && self.name == other.name
            && !self.type_differs(other)
            && self.is_nullable == other.is_nullable
            && self.column_default == other.column_default
            && Self::normalized_generated(&self.is_generated)
                == Self::normalized_generated(&other.is_generated)
            && self.effective_generation_type() == other.effective_generation_type()
            && generation_expressions_equivalent(
                &self.generation_expression,
                &other.generation_expression,
            )
    }

    /// The generation expression with its redundant enclosing parentheses
    /// removed, ready to be wrapped in exactly one pair (`AS (expr)`).
    ///
    /// Only a pair that encloses the *whole* expression is redundant.
    /// PostgreSQL deparses `a * 2 + b * 3` as `((a * 2) + (b * 3))`; peeling
    /// the outer pair leaves `(a * 2) + (b * 3)`, which still starts with `(`
    /// and ends with `)` without being one group — stripping those too used
    /// to emit `AS (a * 2) + (b * 3)`, a syntax error.
    fn normalized_generation_expression(expr: &str) -> String {
        let mut trimmed = expr.trim();
        while trimmed.len() > 2 && is_enclosed_in_parens(trimmed) {
            trimmed = trimmed[1..trimmed.len() - 1].trim();
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
    /// This column with the type of `other` — its data type, length,
    /// precision, scale, interval, collation, domain and UDT — in place of
    /// its own, so an alter from `other` to it leaves the type alone. The
    /// table alter path uses it for a column an inheritance ancestor's
    /// recursing `ALTER COLUMN ... TYPE` retypes (see
    /// `InheritedRecreates::retyped_by_ancestor`).
    pub(crate) fn with_type_of(&self, other: &TableColumn) -> TableColumn {
        TableColumn {
            data_type: other.data_type.clone(),
            character_maximum_length: other.character_maximum_length,
            character_octet_length: other.character_octet_length,
            numeric_precision: other.numeric_precision,
            numeric_precision_radix: other.numeric_precision_radix,
            numeric_scale: other.numeric_scale,
            datetime_precision: other.datetime_precision,
            interval_type: other.interval_type.clone(),
            interval_precision: other.interval_precision,
            character_set_catalog: other.character_set_catalog.clone(),
            character_set_schema: other.character_set_schema.clone(),
            character_set_name: other.character_set_name.clone(),
            collation_catalog: other.collation_catalog.clone(),
            collation_schema: other.collation_schema.clone(),
            collation_name: other.collation_name.clone(),
            domain_catalog: other.domain_catalog.clone(),
            domain_schema: other.domain_schema.clone(),
            domain_name: other.domain_name.clone(),
            udt_catalog: other.udt_catalog.clone(),
            udt_schema: other.udt_schema.clone(),
            udt_name: other.udt_name.clone(),
            maximum_cardinality: other.maximum_cardinality,
            ..self.clone()
        }
    }

    /// Whether the collation is all that [`TableColumn::type_differs`]
    /// finds different: the same type, length, precision and interval
    /// qualifier, another `collate` clause.
    pub(crate) fn only_collation_differs(&self, other: &TableColumn) -> bool {
        self.collate_clause() != other.collate_clause()
            && self.render_type_clause_without_collation()
                == other.render_type_clause_without_collation()
            && self.udt_name == other.udt_name
            && self.numeric_precision == other.numeric_precision
            && self.numeric_scale == other.numeric_scale
            && self.character_maximum_length == other.character_maximum_length
    }

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
        // The column's own collation, when it has one: without it a change
        // of collation alone left the table's hash as it was, and nothing was
        // migrated. A column without one hashes as it always has.
        if let Some(collation) = self.collation_name.as_deref().filter(|c| !c.is_empty()) {
            hasher.update(b"collate");
            let schema = self.collation_schema.as_deref().unwrap_or_default();
            hasher.update((schema.len() as u32).to_be_bytes());
            hasher.update(schema.as_bytes());
            hasher.update((collation.len() as u32).to_be_bytes());
            hasher.update(collation.as_bytes());
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
        script.push_str(&self.collate_clause());

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

    /// The statements that turn `existing` (the FROM-side column) into
    /// `self` (the TO-side column), or `None` when nothing differs.
    ///
    /// Assumes a migrated server that cannot `SET EXPRESSION`; see
    /// [`TableColumn::get_alter_script_with_caps`].
    pub fn get_alter_script(&self, existing: &TableColumn, use_drop: bool) -> Option<String> {
        self.get_alter_script_with_caps(existing, use_drop, GeneratedAlterCaps::default())
    }

    /// Like [`TableColumn::get_alter_script`], for a migrated server with the
    /// `SET EXPRESSION` capabilities in `caps`.
    ///
    /// `SET EXPRESSION` is emitted without `ONLY` here, so it recurses to
    /// inheritance children and partitions. The table alter path uses the
    /// `ONLY` form for a table that is not partitioned, so that each
    /// inheritance child keeps an expression of its own.
    pub fn get_alter_script_with_caps(
        &self,
        existing: &TableColumn,
        use_drop: bool,
        caps: GeneratedAlterCaps,
    ) -> Option<String> {
        self.alter_parts(existing, use_drop, caps, false).concat()
    }

    /// The statements of [`TableColumn::get_alter_script_with_caps`], split by
    /// where the table alter path emits them (see [`ColumnAlterParts`]).
    ///
    /// With `set_expression_only`, `SET EXPRESSION` is emitted as
    /// `ALTER TABLE ONLY ...`: the inheritance children keep their own
    /// expressions and are altered by their own diff. PostgreSQL 17+ accepts
    /// that form for inheritance parents and partitioned tables alike (verified
    /// live on 17, 18 and 19), so the caller decides; a partitioned table has
    /// to recurse, because partitions are never diffed on their own.
    ///
    /// A live drop + add re-creates the column from its TO definition, so the
    /// type, default, nullability and identity changes that would otherwise
    /// precede it are left out. They are redundant, and on a generated column
    /// a type change can fail outright ("generation expression ... cannot be
    /// cast automatically").
    pub(crate) fn alter_parts(
        &self,
        existing: &TableColumn,
        use_drop: bool,
        caps: GeneratedAlterCaps,
        set_expression_only: bool,
    ) -> ColumnAlterParts {
        self.alter_parts_recreating(existing, use_drop, caps, set_expression_only, false)
    }

    /// [`TableColumn::alter_parts`], with `recreate` forcing the drop + add
    /// of a column that is generated in TO even when nothing about it would
    /// otherwise need one — not even a change. The table alter path forces
    /// it for a generated column that stands in the way of retyping a column
    /// it reads (see `Table::columns_recreated_for_retype`). Ignored without
    /// `use_drop`, and for a column that is not generated in TO.
    pub(crate) fn alter_parts_recreating(
        &self,
        existing: &TableColumn,
        use_drop: bool,
        caps: GeneratedAlterCaps,
        set_expression_only: bool,
        recreate: bool,
    ) -> ColumnAlterParts {
        let recreate = recreate && use_drop && self.is_generated_column();
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
                statements.push(comment_out(&drop_cmd));
            }
        }

        let type_statement = self
            .type_clause_differs(existing)
            .then_some(statements.len());
        if type_statement.is_some() {
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
                        statements.push(comment_out(&drop_cmd));
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
                        line_comment_safe(&self.schema),
                        line_comment_safe(&self.table),
                        line_comment_safe(&self.name)
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

        let mut parts = ColumnAlterParts::default();
        // Whether the column is rewritten (`plain_needs_rewrite`): its new
        // copy starts with no storage, compression, statistics target or
        // comment of its own.
        let mut rewrite = false;

        // Issue #181: `generation_type` (`s`/`v`) flips count as a change
        // too; see `generated_changed`. So does a type change PostgreSQL 14
        // cannot make in place (see `retype_needs_drop_and_add`).
        if recreate || self.generated_changed(existing) || self.retype_needs_drop_and_add(existing)
        {
            let new_is_generated = new_generated == "ALWAYS";
            let old_is_generated = old_generated != "NEVER";

            if !recreate && self.can_set_expression(existing, caps) {
                // Same kind and type, new expression, and the migrated server
                // accepts the in-place form. It keeps the column's attnum,
                // grants, comment and every dependent (PostgreSQL rebuilds
                // indexes, constraints and statistics itself), and a generated
                // value is derived data, so it is emitted even with
                // use_drop=false. Deferred: the new expression may read a
                // column this migration adds.
                if let Some(expr) = &self.generation_expression {
                    parts.deferred = format!(
                        "alter table {}{}.{} alter column {} set expression as ({});",
                        if set_expression_only { "only " } else { "" },
                        self.schema,
                        self.table,
                        self.name,
                        Self::normalized_generation_expression(expr)
                    )
                    .with_empty_lines();
                }
            } else if recreate || self.would_drop_and_re_add_with_caps(existing, caps) {
                // The drop+add gate lives in `would_drop_and_re_add_with_caps`
                // so the comparer's Path B detection (issue #188) cannot
                // drift from this emission site. Everything but
                // STORED -> plain and a SET EXPRESSION change lands here:
                // VIRTUAL columns reject `DROP EXPRESSION`, and PostgreSQL has
                // no `ALTER COLUMN ... ADD GENERATED ALWAYS AS (...)` for
                // either kind (issue #181).
                if use_drop {
                    // The re-added column carries the whole TO definition.
                    statements.clear();
                    if self.plain_needs_rewrite(existing) {
                        (parts.drop, parts.deferred) = self.plain_rewrite_parts();
                        rewrite = true;
                    } else {
                        parts.drop = self.get_drop_script();
                        parts.deferred = self.get_add_script();
                    }
                } else {
                    // The in-place statements stay live, all but a type change
                    // of a column that is generated now: PostgreSQL recomputes
                    // it from the OLD expression cast to the new type, which
                    // fails when no such cast exists (`integer` -> `date`:
                    // "cannot be cast automatically") and is superseded by the
                    // drop + add below when it does not.
                    if old_is_generated && let Some(index) = type_statement {
                        statements.remove(index);
                    }
                    let header = if self.plain_needs_rewrite(existing) {
                        "-- use_drop=false: PostgreSQL 14 keeps a generated column depending on the functions and operators its expression used after DROP EXPRESSION, so making it plain with a new type needs a rewrite that drops the column; statements commented out\n"
                    } else if !old_is_generated && new_is_generated {
                        "-- use_drop=false: converting column to generated requires drop/add; statements commented out\n"
                    } else if self.retype_needs_drop_and_add(existing)
                        && !self.generated_changed(existing)
                    {
                        "-- use_drop=false: PostgreSQL 14 cannot change the type of a generated column whose expression uses a function or operator; drop/add required and statements commented out\n"
                    } else if existing.effective_generation_type() == "v"
                        || self.effective_generation_type() == "v"
                    {
                        "-- use_drop=false: virtual generated column cannot be ALTERed in place (PG18+); drop/add required and statements commented out\n"
                    } else if self.type_differs(existing) {
                        "-- use_drop=false: changing a stored generated column's type together with its expression needs drop/add (SET EXPRESSION keeps the type); statements commented out\n"
                    } else {
                        "-- use_drop=false: changing a stored generated column's expression needs drop/add on this server (SET EXPRESSION requires PostgreSQL 17+); statements commented out\n"
                    };
                    let mut commented = String::from(header);
                    if self.plain_needs_rewrite(existing) {
                        let (drop, rename) = self.plain_rewrite_parts();
                        commented.push_str(&comment_out(&drop));
                        commented.push_str(&comment_out(&rename));
                    } else {
                        commented.push_str(&comment_out(&self.get_drop_script()));
                        commented.push_str(&comment_out(&self.get_add_script()));
                    }
                    statements.push(commented);
                }
            } else if old_is_generated {
                // STORED -> plain: the column keeps its values.
                let drop_cmd = format!(
                    "alter table {}.{} alter column {} drop expression;",
                    self.schema, self.table, self.name
                )
                .with_empty_lines();
                if use_drop {
                    statements.push(drop_cmd);
                } else {
                    statements.push(comment_out(&drop_cmd));
                }
            }
        }

        let mut post = Vec::new();
        if self.storage != existing.storage
            && let Some(storage) = &self.storage
        {
            post.push(
                format!(
                    "alter table {}.{} alter column {} set storage {};",
                    self.schema, self.table, self.name, storage
                )
                .with_empty_lines(),
            );
        }

        if self.compression != existing.compression {
            if let Some(compression) = &self.compression {
                post.push(
                    format!(
                        "alter table {}.{} alter column {} set compression {};",
                        self.schema, self.table, self.name, compression
                    )
                    .with_empty_lines(),
                );
            } else {
                post.push(
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
                post.push(
                    format!(
                        "alter table {}.{} alter column {} set statistics {};",
                        self.schema, self.table, self.name, stats
                    )
                    .with_empty_lines(),
                );
            } else {
                post.push(
                    format!(
                        "alter table {}.{} alter column {} set statistics -1;",
                        self.schema, self.table, self.name
                    )
                    .with_empty_lines(),
                );
            }
        }

        // The rewrite's own statements carry the rest.
        if rewrite {
            post.clear();
        }

        // A comment change rides along with the other statements, as it
        // always has; a rewritten column gets its comment back.
        let comment = if (rewrite && self.comment.is_some())
            || (self.comment != existing.comment
                && (parts.is_deferred() || !statements.is_empty() || !post.is_empty()))
        {
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
            let mut block = String::new();
            block.append_block(&comment_stmt);
            block
        } else {
            String::new()
        };

        if parts.is_deferred() {
            parts.inline = statements.concat();
            parts.post = post.concat();
            parts.comment = comment;
        } else {
            parts.inline = [statements.concat(), post.concat(), comment].concat();
        }
        parts
    }

    pub fn get_add_script(&self) -> String {
        self.get_add_script_with_not_null("not null")
    }

    /// [`TableColumn::get_add_script`] with `not_null` — such as
    /// `constraint c_nn not null` — in place of its plain `not null` when
    /// the column is not nullable.
    pub(crate) fn get_add_script_with_not_null(&self, not_null: &str) -> String {
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
            statement.push(' ');
            statement.push_str(not_null);
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
