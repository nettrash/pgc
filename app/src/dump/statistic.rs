//! Extended statistics (`pg_statistic_ext`) — `CREATE STATISTICS`.
//!
//! Three fields describe the same object from different angles, and only one
//! of them is fit to rebuild it from:
//!
//! * `definition` is `pg_get_statisticsobjdef(oid)` verbatim. It is hashed, so
//!   any change the server can see changes the hash.
//! * `kinds` and `columns` are what pgc has always derived from it — the
//!   `stxkind` letters mapped to names, and the `ON` list split on `,`. Both
//!   are hashed too, which is why they keep that exact shape: normalising
//!   either would change the stored hash of every existing object, and an old
//!   dump compared with a new one would then drop and recreate statistics that
//!   never changed. The split is lossy (a `,` inside a literal, the `FROM` in
//!   `EXTRACT(year FROM d)`), so it is only ever a fallback for emission.
//! * `columns_def` is `pg_get_statisticsobjdef_columns(oid)`: the `ON` list
//!   exactly as the server deparses it. It is what `get_script` emits, and it is
//!   deliberately not hashed — `definition` already covers it.
//!
//! The hash therefore sees the order in which the server prints the plain
//! columns, which is their attnum order, not the order `CREATE STATISTICS`
//! named them in. Two tables whose columns sit in different physical orders
//! (a column dropped and re-added on one side) print the same object
//! differently, so a differing hash is only a reason to look:
//! `Statistic::requires_recreate` compares the plain columns as a set.
//!
//! A dump written before pgc 1.1.0 has no `columns_def`. The same text is then
//! cut out of `definition` with a scan that respects quotes and parentheses,
//! and only if that fails is `columns` re-joined.
//!
//! `kinds` also carries `"expressions"` (`stxkind` `'e'`) for statistics on
//! expressions, and — on PostgreSQL 19 — on virtual generated columns, which
//! the server stores as expressions. That is not a kind `CREATE STATISTICS`
//! accepts: PostgreSQL adds it on its own whenever the `ON` list holds an
//! expression, so `get_script` leaves it out of the kind list.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::utils::string_extensions::{StringExt, comment_out, line_comment_safe};

/// Information about a PostgreSQL extended statistics object (CREATE STATISTICS).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Statistic {
    pub schema: String,
    pub name: String,
    pub owner: String,
    /// The target table schema.
    pub table_schema: String,
    /// The target table name.
    pub table_name: String,
    /// The statistics kinds (e.g., "ndistinct", "dependencies", "mcv"), one per
    /// `stxkind` letter. `'e'` maps to `"expressions"`, which is kept here for
    /// hash stability but never emitted (see the module docs).
    pub kinds: Vec<String>,
    /// The `ON` list of `definition` split on `,`. Hashed, so it keeps that
    /// lossy shape; emission prefers `columns_def`.
    pub columns: Vec<String>,
    /// The full definition as returned by pg_get_statisticsobjdef.
    pub definition: String,
    /// The `ON` list exactly as `pg_get_statisticsobjdef_columns(oid)`
    /// (PostgreSQL 14+) deparses it, e.g. `a, (v), EXTRACT(year FROM d)`.
    /// Authoritative for emission. Not hashed — `definition` already covers it.
    /// `None` in dumps written before pgc 1.1.0 and for servers older than 14;
    /// emission then cuts the `ON` list out of `definition`, and joins
    /// `columns` only when that fails (see the module docs).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub columns_def: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    /// Statistics target (number of most-common-values entries)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stxstattarget: Option<i32>,
    pub hash: Option<String>,
}

impl Statistic {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        schema: String,
        name: String,
        owner: String,
        table_schema: String,
        table_name: String,
        kinds: Vec<String>,
        columns: Vec<String>,
        definition: String,
    ) -> Self {
        let mut stat = Self {
            schema,
            name,
            owner,
            table_schema,
            table_name,
            kinds,
            columns,
            definition,
            columns_def: None,
            comment: None,
            stxstattarget: None,
            hash: None,
        };
        stat.hash();
        stat
    }

    pub fn hash(&mut self) {
        let mut hasher = Sha256::new();
        hasher.update(self.schema.as_bytes());
        hasher.update(self.name.as_bytes());
        hasher.update(self.owner.as_bytes());
        hasher.update(self.table_schema.as_bytes());
        hasher.update(self.table_name.as_bytes());

        hasher.update((self.kinds.len() as u32).to_be_bytes());
        for kind in &self.kinds {
            hasher.update(kind.as_bytes());
        }

        hasher.update((self.columns.len() as u32).to_be_bytes());
        for col in &self.columns {
            hasher.update(col.as_bytes());
        }

        if let Some(comment) = &self.comment {
            hasher.update((comment.len() as u32).to_be_bytes());
            hasher.update(comment.as_bytes());
        }

        hasher.update(self.definition.as_bytes());

        if let Some(target) = self.stxstattarget {
            hasher.update(target.to_be_bytes());
        }

        self.hash = Some(format!("{:x}", hasher.finalize()));
    }

    /// The ` (kind, ...)` clause of `CREATE STATISTICS`, with its leading
    /// space, or an empty string when no kind may be named.
    ///
    /// `"expressions"` is dropped: PostgreSQL records it whenever the `ON` list
    /// holds an expression, but rejects it as a kind name. What is left is
    /// empty only for univariate statistics — a single expression, or on
    /// PostgreSQL 19 a single virtual generated column — whose `stxkind` is
    /// `{e}` alone, and those accept no kind list at all. Multivariate
    /// statistics always carry at least one of ndistinct, dependencies or mcv.
    fn kinds_clause(&self) -> String {
        let kinds: Vec<&str> = self
            .kinds
            .iter()
            .map(String::as_str)
            .filter(|kind| *kind != "expressions" && *kind != "e")
            .collect();
        if kinds.is_empty() {
            String::new()
        } else {
            format!(" ({})", kinds.join(", "))
        }
    }

    /// The `ON` list to emit and compare: `columns_def` when the dump has it;
    /// for dumps written before pgc 1.1.0, the same text cut out of
    /// `definition`; and only if that fails, the split `columns` re-joined.
    fn on_list(&self) -> String {
        if let Some(columns_def) = &self.columns_def {
            return columns_def.clone();
        }
        Self::on_list_from_definition(&self.definition).unwrap_or_else(|| self.columns.join(", "))
    }

    /// Cut the `ON` list out of a `pg_get_statisticsobjdef` definition,
    /// `CREATE STATISTICS <name> [(<kinds>)] ON <list> FROM <table>`.
    ///
    /// The list runs from the first ` ON ` to the first ` FROM ` after it
    /// that sits outside quotes and parentheses. That keeps the `FROM` of
    /// `EXTRACT(year FROM d)` and a `,` inside a literal where they belong,
    /// which the `,` split in `columns` does not. Both keywords are matched in
    /// upper case, the only way the server prints them; a lower-case ` on ` in
    /// the name can only occur inside a quoted identifier, which is skipped.
    /// `None` when the text does not have that shape.
    fn on_list_from_definition(definition: &str) -> Option<String> {
        let bytes = definition.as_bytes();
        let mut depth = 0usize;
        let mut on_end: Option<usize> = None;
        let mut i = 0;
        while i < bytes.len() {
            match bytes[i] {
                quote @ (b'\'' | b'"') => {
                    // Skip to the closing quote; a doubled quote is an escape.
                    i += 1;
                    while i < bytes.len() {
                        if bytes[i] == quote {
                            if bytes.get(i + 1) == Some(&quote) {
                                i += 1;
                            } else {
                                break;
                            }
                        }
                        i += 1;
                    }
                }
                b'(' => depth += 1,
                b')' => depth = depth.saturating_sub(1),
                _ if depth == 0 => match on_end {
                    None if bytes[i..].starts_with(b" ON ") => {
                        i += b" ON ".len();
                        on_end = Some(i);
                        continue;
                    }
                    Some(start) if bytes[i..].starts_with(b" FROM ") => {
                        let list = definition[start..i].trim();
                        return (!list.is_empty()).then(|| list.to_string());
                    }
                    _ => {}
                },
                _ => {}
            }
            i += 1;
        }
        None
    }

    /// Returns a CREATE STATISTICS script.
    pub fn get_script(&self) -> String {
        let mut result = format!(
            "create statistics {}.{}{} on {} from {}.{};",
            self.schema,
            self.name,
            self.kinds_clause(),
            self.on_list(),
            self.table_schema,
            self.table_name
        )
        .with_empty_lines();

        if !self.owner.is_empty() {
            result.push_str(
                &format!(
                    "alter statistics {}.{} owner to {};",
                    self.schema, self.name, self.owner
                )
                .with_empty_lines(),
            );
        }

        if let Some(target_script) = self.get_target_script() {
            result.push_str(&target_script);
        }

        if let Some(comment) = &self.comment {
            result.push_str(
                &format!(
                    "comment on statistics {}.{} is '{}';",
                    self.schema,
                    self.name,
                    comment.replace('\'', "''")
                )
                .with_empty_lines(),
            );
        }

        result
    }

    /// Returns a DROP STATISTICS script.
    pub fn get_drop_script(&self) -> String {
        format!("drop statistics if exists {}.{};", self.schema, self.name).with_empty_lines()
    }

    /// `ALTER STATISTICS ... SET STATISTICS n` for an explicit statistics
    /// target, `None` for the default (`NULL` from PostgreSQL 17 on, `-1`
    /// before).
    pub(crate) fn get_target_script(&self) -> Option<String> {
        self.stxstattarget
            .filter(|target| *target >= 0)
            .map(|target| {
                format!(
                    "alter statistics {}.{} set statistics {};",
                    self.schema, self.name, target
                )
                .with_empty_lines()
            })
    }

    /// Whether turning `self` into `target` needs a drop + recreate: extended
    /// statistics cannot change their kinds, `ON` list or table in place.
    ///
    /// The `ON` lists are compared by [`Statistic::on_list_key`], which
    /// ignores the order of the plain columns: PostgreSQL does not keep it.
    /// The whole list is compared, not the split `columns`, which cannot
    /// see a change inside an expression that its `,` split mangles the
    /// same way on both sides (`EXTRACT(year FROM d1)` and
    /// `EXTRACT(year FROM d2)` both split to `EXTRACT(year`). The hash
    /// still covers the text as printed, so a reordered list is looked at
    /// here and found unchanged.
    pub(crate) fn requires_recreate(&self, target: &Statistic) -> bool {
        self.kinds != target.kinds
            || self.on_list_key() != target.on_list_key()
            || self.table_schema != target.table_schema
            || self.table_name != target.table_name
    }

    /// The `ON` list as PostgreSQL keeps it: the plain columns as a sorted
    /// set, then the expressions in the order they were written.
    ///
    /// `CREATE STATISTICS` stores the plain columns as `stxkeys` sorted by
    /// attnum, and the server prints them in that order, ahead of the
    /// expressions, whatever order the statement named them in. So the same
    /// object prints `ON b, c` on a table whose `b` was dropped and re-added
    /// after `c`, and `ON c, b` elsewhere: the order says nothing about the
    /// object. The expressions (`stxexprs`, which on PostgreSQL 19 also hold
    /// virtual generated columns, printed in parentheses) keep theirs.
    ///
    /// The list is split at the commas outside quotes and parentheses; an
    /// item that is one identifier, bare or quoted, is a plain column, and
    /// anything else — a parenthesised expression, or a function call the
    /// server prints without parentheses (`lower(code)`) — an expression.
    fn on_list_key(&self) -> (Vec<String>, Vec<String>) {
        let on_list = self.on_list();
        let mut columns = Vec::new();
        let mut expressions = Vec::new();
        for item in Self::split_top_level(&on_list) {
            if Self::is_identifier(item) {
                columns.push(item.to_string());
            } else {
                expressions.push(item.to_string());
            }
        }
        columns.sort_unstable();
        (columns, expressions)
    }

    /// `list` split at the commas outside quotes and parentheses, each item
    /// trimmed; empty items are left out.
    fn split_top_level(list: &str) -> Vec<&str> {
        let bytes = list.as_bytes();
        let mut items = Vec::new();
        let mut depth = 0usize;
        let mut start = 0;
        let mut i = 0;
        while i < bytes.len() {
            match bytes[i] {
                quote @ (b'\'' | b'"') => {
                    // Skip to the closing quote; a doubled quote is an escape.
                    i += 1;
                    while i < bytes.len() {
                        if bytes[i] == quote {
                            if bytes.get(i + 1) == Some(&quote) {
                                i += 1;
                            } else {
                                break;
                            }
                        }
                        i += 1;
                    }
                }
                b'(' => depth += 1,
                b')' => depth = depth.saturating_sub(1),
                b',' if depth == 0 => {
                    items.push(list[start..i].trim());
                    start = i + 1;
                }
                _ => {}
            }
            i += 1;
        }
        items.push(list[start.min(list.len())..].trim());
        items.retain(|item| !item.is_empty());
        items
    }

    /// Whether `item` is a single identifier: a quoted one (`"Code"`, with
    /// doubled quotes inside), or a bare one as the server prints it.
    fn is_identifier(item: &str) -> bool {
        if let Some(inner) = item
            .strip_prefix('"')
            .and_then(|rest| rest.strip_suffix('"'))
        {
            return !inner.is_empty() && !inner.replace("\"\"", "").contains('"');
        }
        let mut chars = item.chars();
        chars.next().is_some_and(|c| c.is_alphabetic() || c == '_')
            && chars.all(|c| c.is_alphanumeric() || c == '_' || c == '$')
    }

    /// Returns an ALTER script to transform self into target.
    /// Extended statistics objects can't be altered in place for definition changes;
    /// they must be dropped and recreated (see `requires_recreate`).
    pub fn get_alter_script(&self, target: &Statistic, use_drop: bool) -> String {
        let mut statements = Vec::new();

        // Owner change
        if self.owner != target.owner && !target.owner.is_empty() {
            statements.push(
                format!(
                    "alter statistics {}.{} owner to {};",
                    target.schema, target.name, target.owner
                )
                .with_empty_lines(),
            );
        }

        // If the structural definition changed, drop and recreate.
        if self.requires_recreate(target) {
            let drop = self.get_drop_script();
            let create = target.get_script();
            if use_drop {
                return format!("{}{}", drop, create);
            } else {
                let commented_drop = comment_out(&drop);
                let commented_create = comment_out(&create);
                return format!(
                    "-- use_drop=false: statistics {}.{} requires drop+recreate; statements commented out\n{}{}",
                    line_comment_safe(&self.schema),
                    line_comment_safe(&self.name),
                    commented_drop,
                    commented_create
                );
            }
        }

        // Comment change
        if self.comment != target.comment {
            if let Some(comment) = &target.comment {
                statements.push(
                    format!(
                        "comment on statistics {}.{} is '{}';",
                        target.schema,
                        target.name,
                        comment.replace('\'', "''")
                    )
                    .with_empty_lines(),
                );
            } else {
                statements.push(
                    format!(
                        "comment on statistics {}.{} is null;",
                        target.schema, target.name
                    )
                    .with_empty_lines(),
                );
            }
        }

        // Statistics target change
        if self.stxstattarget != target.stxstattarget {
            if let Some(t) = target.stxstattarget {
                if t >= 0 {
                    statements.push(
                        format!(
                            "alter statistics {}.{} set statistics {};",
                            target.schema, target.name, t
                        )
                        .with_empty_lines(),
                    );
                }
            } else {
                // Reset to default (-1)
                statements.push(
                    format!(
                        "alter statistics {}.{} set statistics -1;",
                        target.schema, target.name
                    )
                    .with_empty_lines(),
                );
            }
        }

        statements.join("")
    }
}

#[cfg(test)]
#[path = "tests/statistic.rs"]
mod tests;
