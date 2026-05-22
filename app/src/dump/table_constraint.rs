use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::utils::string_extensions::StringExt;

// This is an information about a PostgreSQL table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableConstraint {
    pub catalog: String,            // Catalog name
    pub schema: String,             // Schema name
    pub name: String,               // Constraint name
    pub table_name: String,         // Table name
    pub constraint_type: String, // Type of the constraint (e.g., PRIMARY KEY, FOREIGN KEY, UNIQUE)
    pub is_deferrable: bool,     // Whether the constraint is deferrable
    pub initially_deferred: bool, // Whether the constraint is initially deferred
    pub definition: Option<String>, // Definition of the constraint (e.g., check expression)
    #[serde(default)]
    pub coninhcount: i32, // Number of direct inheritance ancestors (0 = local, >0 = inherited)
    #[serde(default = "TableConstraint::default_enforced")]
    pub is_enforced: bool, // Whether the constraint is enforced (PG18+ supports NOT ENFORCED)
    #[serde(default)]
    pub no_inherit: bool, // Whether the constraint is marked NO INHERIT
    #[serde(default)]
    pub nulls_not_distinct: bool, // PG15+: UNIQUE constraint treats NULLs as not distinct
    /// Optional comment on the constraint
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
}

impl TableConstraint {
    /// Default value for is_enforced (backward compat with old dumps)
    fn default_enforced() -> bool {
        true
    }

    /// Hash
    pub fn add_to_hasher(&self, hasher: &mut Sha256) {
        hasher.update(self.schema.as_bytes());
        hasher.update(self.name.as_bytes());
        hasher.update(self.table_name.as_bytes());
        hasher.update(self.constraint_type.as_bytes());
        hasher.update(self.is_deferrable.to_string().as_bytes());
        hasher.update(self.initially_deferred.to_string().as_bytes());
        hasher.update(self.is_enforced.to_string().as_bytes());
        hasher.update(self.no_inherit.to_string().as_bytes());
        hasher.update(self.nulls_not_distinct.to_string().as_bytes());
        if let Some(definition) = &self.definition {
            hasher.update(Self::normalize_definition(definition).as_bytes());
        }
        if let Some(ref comment) = self.comment {
            hasher.update((comment.len() as u32).to_be_bytes());
            hasher.update(comment.as_bytes());
        }
    }

    /// Returns a string representation of the constraint
    /// ALTER TABLE ... ADD CONSTRAINT ...
    pub fn get_script(&self) -> String {
        let mut script = String::new();
        script.push_str(&format!(
            "alter table {}.{} add constraint {} ",
            self.schema, self.table_name, self.name
        ));

        // If a definition is provided, lowercase only the SQL keywords/identifiers,
        // preserving the original case of string literal contents so that round-tripping
        // through PGC does not produce a spurious diff.
        // Otherwise, build from constraint_type and attribute flags.
        let clause = if let Some(def) = &self.definition {
            let mut base = Self::lowercase_outside_literals(def);
            // Append deferrable flags for foreign key or unique if flags set
            if self.constraint_type.eq_ignore_ascii_case("FOREIGN KEY")
                || self.constraint_type.eq_ignore_ascii_case("UNIQUE")
            {
                if self.is_deferrable && !base.contains("deferrable") {
                    base.push_str(" deferrable");
                }
                if self.initially_deferred && !base.contains("initially deferred") {
                    base.push_str(" initially deferred");
                }
            }
            base
        } else {
            let mut parts: Vec<String> = Vec::new();
            match self.constraint_type.to_uppercase().as_str() {
                "PRIMARY KEY" => parts.push("primary key".to_string()),
                "FOREIGN KEY" => {
                    parts.push("foreign key".to_string());
                    if self.is_deferrable {
                        parts.push("deferrable".to_string());
                    }
                    if self.initially_deferred {
                        parts.push("initially deferred".to_string());
                    }
                }
                "UNIQUE" => parts.push("unique".to_string()),
                "CHECK" => parts.push("check".to_string()),
                "EXCLUDE" => parts.push("exclude".to_string()),
                "NOT NULL" => parts.push("not null".to_string()),
                _ => {}
            }
            parts.join(" ")
        };

        script.push_str(&format!("{} ", clause));
        if !self.is_enforced {
            // Issue #182: PG18+ `pg_get_constraintdef()` already emits
            // `NOT ENFORCED` for non-enforced constraints, so the
            // lowercased deparser output flowing through `clause`
            // already contains `not enforced`. Appending again
            // produces `... not enforced not enforced ;`. Older PG
            // versions and PGC's own `parts.join`-built form don't
            // include the keyword, and still need it appended.
            //
            // PR #187 review: the substring scan must skip string
            // literals — a CHECK predicate like
            // `CHECK (msg <> 'not enforced')` would otherwise match
            // and silently suppress the keyword.
            // `lowercase_outside_literals` preserves literal contents
            // verbatim, so the literal text survives in `clause`.
            if !Self::lowercased_contains_outside_literals(&clause, "not enforced") {
                let trimmed = script.trim_end().to_string();
                script.clear();
                script.push_str(&trimmed);
                script.push_str(" not enforced ");
            }
        }
        script.append_block(";");
        if let Some(ref comment) = self.comment {
            script.append_block(&format!(
                "comment on constraint {} on {}.{} is '{}';",
                self.name,
                self.schema,
                self.table_name,
                comment.replace('\'', "''")
            ));
        }
        script
    }

    /// Normalizes a constraint definition for comparison purposes.
    ///
    /// `pg_get_constraintdef()` may produce two semantically identical but
    /// textually different representations for CHECK constraints that use
    /// `IN (...)` / `ANY (ARRAY[...])`:
    ///
    ///   Form A (array-level cast):   `ARRAY['v'::character varying, ...]::text[]`
    ///   Form B (element-level cast):  `ARRAY['v'::character varying::text, ...]`
    ///
    /// Which form is returned depends on how the constraint was originally
    /// created (e.g. via `IN(...)` in DDL versus applying a migration that
    /// reuses Form A verbatim).  Normalize by lowercasing outside literals
    /// and collapsing the redundant `::text` casts so both forms compare equal.
    ///
    /// Both the lowercasing and the cast replacements are applied only to
    /// text **outside** single-quoted string literals, so literal contents
    /// like `']::text[]'` are never altered.
    fn normalize_definition(s: &str) -> String {
        let mut out = String::with_capacity(s.len());
        let mut chars = s.chars();
        // Accumulates non-literal text so we can apply replacements on it
        // in one go before flushing.
        let mut buf = String::new();

        while let Some(c) = chars.next() {
            if c == '\'' {
                // Flush the non-literal buffer (lowercased + cast-normalized).
                Self::flush_outside_buf(&mut buf, &mut out);

                // Inside a single-quoted literal — copy verbatim.
                out.push('\'');
                loop {
                    match chars.next() {
                        Some('\'') => {
                            out.push('\'');
                            if chars.as_str().starts_with('\'') {
                                out.push('\'');
                                chars.next();
                            } else {
                                break;
                            }
                        }
                        Some(ch) => out.push(ch),
                        None => break,
                    }
                }
            } else {
                // Outside a literal — collect into buf for batch processing.
                for lc in c.to_lowercase() {
                    buf.push(lc);
                }
            }
        }

        // Flush any remaining non-literal text.
        Self::flush_outside_buf(&mut buf, &mut out);
        out
    }

    /// Applies cast-normalization replacements to `buf` (which contains only
    /// non-literal text), appends the result to `out`, and clears `buf`.
    fn flush_outside_buf(buf: &mut String, out: &mut String) {
        if buf.is_empty() {
            return;
        }
        let normalized = buf
            .replace("::character varying::text", "::character varying")
            .replace("]::text[]", "]");
        out.push_str(&normalized);
        buf.clear();
    }

    /// Lowercases a SQL expression while preserving the original case of text
    /// inside single-quoted string literals.  Handles the standard `''` escape
    /// for embedded quotes.  Iterates by `char` so multi-byte UTF-8 sequences
    /// are never split.
    /// True when `needle` (must already be lowercase) appears in `s`
    /// OUTSIDE every single-quoted string literal. Mirrors the
    /// quote-tracking state machine used by [`Self::lowercase_outside_
    /// literals`] so a `CHECK (msg <> 'not enforced')` predicate
    /// doesn't get confused with the keyword position. PR #187 review
    /// (issue #182): a plain `clause.contains("not enforced")` would
    /// also match string literals and silently suppress the
    /// constraint flag.
    fn lowercased_contains_outside_literals(s: &str, needle: &str) -> bool {
        debug_assert!(needle.chars().all(|c| !c.is_ascii_uppercase()));
        if needle.is_empty() {
            return true;
        }
        let mut buf = String::with_capacity(needle.len());
        let mut chars = s.chars();
        while let Some(c) = chars.next() {
            if c == '\'' {
                // Skip past the literal entirely; nothing inside it
                // counts toward keyword detection.
                loop {
                    match chars.next() {
                        Some('\'') => {
                            if chars.as_str().starts_with('\'') {
                                chars.next(); // doubled-quote escape
                            } else {
                                break;
                            }
                        }
                        Some(_) => {}
                        None => return false, // unterminated literal
                    }
                }
                // The literal is treated as a token boundary — flush
                // any partial match accumulated before it.
                buf.clear();
                continue;
            }
            for lc in c.to_lowercase() {
                buf.push(lc);
                while !needle.starts_with(buf.as_str()) {
                    if buf.is_empty() {
                        break;
                    }
                    // Drop one char from the front and retry. Because
                    // the haystack is searched as a stream, this
                    // simple back-off is enough for ASCII keyword
                    // needles like `not enforced`.
                    buf.remove(0);
                }
                if buf == needle {
                    return true;
                }
            }
        }
        false
    }

    fn lowercase_outside_literals(s: &str) -> String {
        let mut out = String::with_capacity(s.len());
        let mut chars = s.chars();
        while let Some(c) = chars.next() {
            if c == '\'' {
                // Inside a single-quoted literal — copy verbatim.
                out.push('\'');
                loop {
                    match chars.next() {
                        Some('\'') => {
                            out.push('\'');
                            // Doubled-quote escape — copy the second quote
                            // and stay inside the literal.
                            if chars.as_str().starts_with('\'') {
                                out.push('\'');
                                chars.next();
                            } else {
                                break; // closing quote
                            }
                        }
                        Some(ch) => out.push(ch),
                        None => break, // unterminated literal
                    }
                }
            } else {
                // Outside a literal — apply full Unicode lowercasing.
                for lc in c.to_lowercase() {
                    out.push(lc);
                }
            }
        }
        out
    }

    /// Get alter script to change this constraint to match the target constraint
    /// Returns None if the constraint needs to be dropped and recreated
    pub fn get_alter_script(&self, target: &TableConstraint) -> Option<String> {
        // Helper: emit comment change SQL if needed
        let comment_script = |s: &mut String| {
            if self.comment != target.comment {
                if let Some(ref cmt) = target.comment {
                    s.append_block(&format!(
                        "comment on constraint {} on {}.{} is '{}';",
                        target.name,
                        target.schema,
                        target.table_name,
                        cmt.replace('\'', "''")
                    ));
                } else {
                    s.append_block(&format!(
                        "comment on constraint {} on {}.{} is null;",
                        target.name, target.schema, target.table_name
                    ));
                }
            }
        };

        // FOREIGN KEY constraints can have their deferrable and enforced properties altered
        // CHECK constraints can have their enforced property altered (PG18+)
        // All other changes require drop/recreate
        if self.constraint_type.eq_ignore_ascii_case("FOREIGN KEY")
            && target.constraint_type.eq_ignore_ascii_case("FOREIGN KEY")
            && self.can_be_altered_to(target)
        {
            let mut script = String::new();

            // Handle FOREIGN KEY deferrable property changes
            if self.is_deferrable != target.is_deferrable
                || self.initially_deferred != target.initially_deferred
            {
                if target.is_deferrable {
                    if target.initially_deferred {
                        script.append_block(&format!(
                            "alter table {}.{} alter constraint {} deferrable initially deferred;",
                            self.schema, self.table_name, target.name
                        ));
                    } else {
                        script.append_block(&format!(
                            "alter table {}.{} alter constraint {} deferrable initially immediate;",
                            self.schema, self.table_name, target.name
                        ));
                    }
                } else {
                    script.append_block(&format!(
                        "alter table {}.{} alter constraint {} not deferrable;",
                        self.schema, self.table_name, target.name
                    ));
                }
            }

            // Handle enforced property changes (PG18+)
            if self.is_enforced != target.is_enforced {
                if target.is_enforced {
                    script.append_block(&format!(
                        "alter table {}.{} alter constraint {} enforced;",
                        self.schema, self.table_name, target.name
                    ));
                } else {
                    script.append_block(&format!(
                        "alter table {}.{} alter constraint {} not enforced;",
                        self.schema, self.table_name, target.name
                    ));
                }
            }

            // Handle no_inherit property changes (PG18+)
            if self.no_inherit != target.no_inherit {
                if target.no_inherit {
                    script.append_block(&format!(
                        "alter table {}.{} alter constraint {} no inherit;",
                        self.schema, self.table_name, target.name
                    ));
                } else {
                    script.append_block(&format!(
                        "alter table {}.{} alter constraint {} inherit;",
                        self.schema, self.table_name, target.name
                    ));
                }
            }

            comment_script(&mut script);
            Some(script)
        } else if (self.constraint_type.eq_ignore_ascii_case("CHECK")
            || self.constraint_type.eq_ignore_ascii_case("FOREIGN KEY"))
            && self.can_be_altered_to(target)
            && (self.is_enforced != target.is_enforced
                || self.no_inherit != target.no_inherit
                || self.comment != target.comment)
        {
            // CHECK/FK constraint enforced or no_inherit property change (PG18+)
            let mut script = String::new();
            if self.is_enforced != target.is_enforced {
                if target.is_enforced {
                    script.append_block(&format!(
                        "alter table {}.{} alter constraint {} enforced;",
                        self.schema, self.table_name, target.name
                    ));
                } else {
                    script.append_block(&format!(
                        "alter table {}.{} alter constraint {} not enforced;",
                        self.schema, self.table_name, target.name
                    ));
                }
            }
            if self.no_inherit != target.no_inherit {
                if target.no_inherit {
                    script.append_block(&format!(
                        "alter table {}.{} alter constraint {} no inherit;",
                        self.schema, self.table_name, target.name
                    ));
                } else {
                    script.append_block(&format!(
                        "alter table {}.{} alter constraint {} inherit;",
                        self.schema, self.table_name, target.name
                    ));
                }
            }
            comment_script(&mut script);
            Some(script)
        } else if self.comment != target.comment
            && self.schema == target.schema
            && self.name == target.name
            && self.table_name == target.table_name
            && self.constraint_type == target.constraint_type
            && self.is_deferrable == target.is_deferrable
            && self.initially_deferred == target.initially_deferred
            && self.is_enforced == target.is_enforced
            && self.no_inherit == target.no_inherit
            && self.nulls_not_distinct == target.nulls_not_distinct
            && self.definition.as_deref().map(Self::normalize_definition)
                == target.definition.as_deref().map(Self::normalize_definition)
        {
            // Only comment changed - any constraint type
            let mut script = String::new();
            comment_script(&mut script);
            Some(script)
        } else {
            None
        }
    }

    /// Check if this constraint can be altered to match the target constraint
    /// without dropping and recreating
    pub fn can_be_altered_to(&self, target: &TableConstraint) -> bool {
        // FOREIGN KEY constraints can have deferrable and enforced properties altered
        // CHECK constraints can have enforced property altered (PG18+)
        // All other changes require drop/recreate
        if self.constraint_type.eq_ignore_ascii_case("FOREIGN KEY")
            && target.constraint_type.eq_ignore_ascii_case("FOREIGN KEY")
        {
            // Check if only deferrable/enforced properties changed
            self.catalog == target.catalog
                && self.schema == target.schema
                && self.name == target.name
                && self.table_name == target.table_name
                && self.constraint_type == target.constraint_type
                && self.nulls_not_distinct == target.nulls_not_distinct
                && self.definition.as_deref().map(Self::normalize_definition)
                    == target.definition.as_deref().map(Self::normalize_definition)
            // is_deferrable, initially_deferred, is_enforced, and no_inherit can differ
        } else if self.constraint_type.eq_ignore_ascii_case("CHECK")
            && target.constraint_type.eq_ignore_ascii_case("CHECK")
        {
            // CHECK constraints: enforced and no_inherit can be altered (PG18+)
            self.catalog == target.catalog
                && self.schema == target.schema
                && self.name == target.name
                && self.table_name == target.table_name
                && self.constraint_type == target.constraint_type
                && self.is_deferrable == target.is_deferrable
                && self.initially_deferred == target.initially_deferred
                && self.nulls_not_distinct == target.nulls_not_distinct
                && self.definition.as_deref().map(Self::normalize_definition)
                    == target.definition.as_deref().map(Self::normalize_definition)
            // is_enforced and no_inherit can differ
        } else {
            false
        }
    }

    /// Get drop script for this constraint
    pub fn get_drop_script(&self) -> String {
        format!(
            "alter table {}.{} drop constraint {};",
            self.schema, self.table_name, self.name
        )
        .with_empty_lines()
    }

    fn strip_not_null_prefix(definition: &str) -> Option<&str> {
        let definition = definition.trim();
        let prefix = "not null ";
        if definition
            .get(..prefix.len())
            .is_some_and(|part| part.eq_ignore_ascii_case(prefix))
        {
            Some(definition[prefix.len()..].trim())
        } else {
            None
        }
    }
    fn parse_pg_identifier(identifier: &str) -> Option<String> {
        let identifier = identifier.trim();
        if identifier.is_empty() {
            return None;
        }
        if let Some(rest) = identifier.strip_prefix('"') {
            let mut parsed = String::new();
            let mut chars = rest.chars().peekable();
            while let Some(ch) = chars.next() {
                if ch == '"' {
                    if chars.peek() == Some(&'"') {
                        parsed.push('"');
                        chars.next();
                    } else {
                        let remaining: String = chars.collect();
                        if remaining.trim().is_empty() {
                            return Some(parsed);
                        } else {
                            return None;
                        }
                    }
                } else {
                    parsed.push(ch);
                }
            }
            None
        } else {
            Some(identifier.to_lowercase())
        }
    }
    fn normalize_identifier_for_name(identifier: &str) -> String {
        Self::parse_pg_identifier(identifier).unwrap_or_else(|| identifier.trim().to_lowercase())
    }
    /// If this is a NOT NULL constraint whose name was auto-generated by
    /// PostgreSQL (matching `{table}_{col}_not_null`, optionally followed by a
    /// numeric collision suffix, and respecting PG's 63-byte identifier
    /// truncation), returns the normalized column name. Quoted identifiers
    /// preserve their original case; unquoted identifiers are lowercased to
    /// match PostgreSQL identifier folding. Otherwise returns `None`.
    ///
    /// Auto-named NOT NULL constraints are semantically anonymous: their names
    /// are non-deterministic side effects of PG's name resolution and may
    /// differ between databases that have an otherwise identical schema.
    ///
    /// Caveat (very rare): a user-chosen constraint name that *happens* to
    /// follow the exact `{table}_{col}_not_null[_digits]` pattern will be
    /// treated as auto-generated. In practice users either name them
    /// distinctly (e.g. `..._not_null_v2`, which has a non-digit suffix and is
    /// rejected) or do not name them at all, so this should not collide with
    /// real-world naming. If you specifically need to preserve a custom name
    /// that matches the pattern, rename it to something else.
    pub fn auto_not_null_column(&self, table_raw: &str) -> Option<String> {
        if !self.constraint_type.eq_ignore_ascii_case("not null") {
            return None;
        }
        let definition = self.definition.as_deref()?;
        let col = Self::parse_pg_identifier(Self::strip_not_null_prefix(definition)?)?;
        let table = Self::normalize_identifier_for_name(table_raw);
        let base = format!("{}_{}_not_null", table, col);
        let cname = Self::normalize_identifier_for_name(&self.name);

        // Split cname into (head, trailing_digits). PG appends a numeric
        // collision suffix to auto-generated names when needed.
        let digit_split = cname
            .char_indices()
            .rev()
            .take_while(|(_, c)| c.is_ascii_digit())
            .last()
            .map(|(i, _)| i)
            .unwrap_or(cname.len());
        let head = &cname[..digit_split];
        let suffix = &cname[digit_split..];

        // 1. Exact match against the un-truncated base.
        if cname == base {
            return Some(col);
        }

        // 2. base + numeric collision suffix, no truncation involved.
        if !suffix.is_empty() && head == base {
            return Some(col);
        }

        // 3. PostgreSQL truncates identifiers to NAMEDATALEN-1 = 63 bytes.
        // When `{table}_{col}_not_null` (optionally with a `_N` collision
        // suffix) exceeds 63 bytes, PG truncates the head before appending the
        // suffix, so the recorded constraint name is a byte-prefix of `base`,
        // possibly followed by a numeric collision suffix. Accept either form
        // when the base length would have required truncation. Note: PG
        // truncates by bytes, so compare byte prefixes (identifiers here are
        // typically ASCII, but `starts_with` on bytes is correct either way).
        const PG_NAMEDATALEN_MAX: usize = 63;
        if base.len() > PG_NAMEDATALEN_MAX && !head.is_empty() {
            // PG clips identifiers by *bytes* to NAMEDATALEN-1, but on a UTF-8
            // character boundary (pg_mbcliplen).
            let suffix_len = suffix.len();
            if suffix_len < PG_NAMEDATALEN_MAX && cname.len() <= PG_NAMEDATALEN_MAX {
                let max_head_bytes = PG_NAMEDATALEN_MAX - suffix_len;
                let mut cut = max_head_bytes.min(base.len());
                while cut > 0 && !base.is_char_boundary(cut) {
                    cut -= 1;
                }
                let clipped_base = &base[..cut];
                if head == clipped_base {
                    return Some(col);
                }
            }
        }

        None
    }
}

impl PartialEq for TableConstraint {
    fn eq(&self, other: &Self) -> bool {
        self.schema == other.schema
            && self.name == other.name
            && self.table_name == other.table_name
            && self.constraint_type == other.constraint_type
            && self.is_deferrable == other.is_deferrable
            && self.initially_deferred == other.initially_deferred
            && self.is_enforced == other.is_enforced
            && self.no_inherit == other.no_inherit
            && self.nulls_not_distinct == other.nulls_not_distinct
            && self.comment == other.comment
            && self.definition.as_deref().map(Self::normalize_definition)
                == other.definition.as_deref().map(Self::normalize_definition)
    }
}

#[cfg(test)]
#[path = "table_constraint_tests.rs"]
mod tests;
