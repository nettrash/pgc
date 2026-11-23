//! Rewrite rules (`pg_rewrite`) — `CREATE RULE`.
//!
//! The rule that backs a view (`_RETURN`) is excluded; views are handled by
//! [`view`](super::view), and nothing here ever emits a statement for it.
//!
//! [`Rule::definition`] is `pg_get_ruledef` verbatim, which always reads
//! `CREATE RULE name AS ON event TO relation ... ;` — including the trailing
//! semicolon. The script builders strip that semicolon before adding their own,
//! and a changed rule is applied with `CREATE OR REPLACE RULE` built from the
//! same text: a plain `CREATE RULE` for a rule that already exists fails with
//! `rule "..." for relation "..." already exists`. `CREATE OR REPLACE RULE`
//! keeps the rule's OID and so its comment, and can change the event, the
//! qualification, `INSTEAD`/`ALSO` and every action (verified on PostgreSQL 14
//! and 19).
//!
//! [`Rule::depends_on`] records what the rule's actions and qualification read,
//! so the comparer can drop a rule before the migration drops or retypes
//! something it depends on, and recreate it afterwards.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::utils::string_extensions::StringExt;

/// What a rule's actions and qualification depend on: its `pg_depend` rows with
/// `deptype = 'n'`.
///
/// These are exactly the objects PostgreSQL refuses to drop — or, for a column,
/// to retype — while the rule exists (`DROP TABLE`, `DROP VIEW`,
/// `DROP FOREIGN TABLE` and `DROP SEQUENCE` without `CASCADE`,
/// `ALTER COLUMN ... TYPE`), plus the routines, and views, whose
/// `DROP ... CASCADE` silently takes the rule with it.
///
/// None of it is part of [`Rule::hash`]: it follows from the definition, which
/// is hashed already.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuleDependencies {
    /// Relations of any kind, as `schema.name` in raw catalog spelling (no
    /// `quote_ident`), the way [`View::table_relation`](super::view::View::table_relation)
    /// spells them. A relation read only through some of its columns is listed
    /// too, although `pg_depend` records only the columns.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub relations: Vec<String>,
    /// Columns, as `schema.table.column` in raw catalog spelling.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub columns: Vec<String>,
    /// Routines, as `schema.name(identity arguments)` with the schema and name
    /// `quote_ident`-ed and the arguments as `pg_get_function_identity_arguments`
    /// prints them — the same spelling as a
    /// [`Routine`](super::routine::Routine)'s `schema`, `name` and `arguments`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub routines: Vec<String>,
}

/// Represents a PostgreSQL rule (from pg_rewrite).
///
/// `schema`, `table_name` and `rule_name` are `quote_ident`-ed by the dump query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Rule {
    pub schema: String,
    pub table_name: String,
    pub rule_name: String,
    /// `pg_get_ruledef(oid, true)`: a complete `CREATE RULE ... ;` statement.
    pub definition: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    pub hash: Option<String>,
    /// The rule's dependencies. `None` for a dump written before pgc recorded
    /// them (1.0.x), which the comparer reads as "unknown", never as "none".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub depends_on: Option<RuleDependencies>,
}

impl Rule {
    pub fn new(
        schema: String,
        table_name: String,
        rule_name: String,
        definition: String,
        comment: Option<String>,
    ) -> Self {
        let mut rule = Rule {
            schema,
            table_name,
            rule_name,
            definition,
            comment,
            hash: None,
            depends_on: None,
        };
        rule.hash();
        rule
    }

    pub fn hash(&mut self) {
        let mut hasher = Sha256::new();
        hasher.update(self.schema.as_bytes());
        hasher.update(self.table_name.as_bytes());
        hasher.update(self.rule_name.as_bytes());
        hasher.update(self.definition.as_bytes());
        if let Some(comment) = &self.comment {
            hasher.update((comment.len() as u32).to_be_bytes());
            hasher.update(comment.as_bytes());
        }
        self.hash = Some(format!("{:x}", hasher.finalize()));
    }

    /// The `CREATE RULE` statement, with the trailing semicolon(s)
    /// `pg_get_ruledef` ends it with removed.
    fn statement(&self) -> &str {
        self.definition.trim().trim_end_matches(';').trim_end()
    }

    /// [`Rule::statement`] turned into `CREATE OR REPLACE RULE`, which creates
    /// the rule when it is missing and replaces it in place when it exists. A
    /// definition that does not start with `CREATE RULE` is left as it is.
    fn replace_statement(&self) -> String {
        const CREATE_RULE: &str = "create rule ";
        let statement = self.statement();
        match statement.get(..CREATE_RULE.len()) {
            Some(prefix) if prefix.eq_ignore_ascii_case(CREATE_RULE) => {
                format!("CREATE OR REPLACE RULE {}", &statement[CREATE_RULE.len()..])
            }
            _ => statement.to_string(),
        }
    }

    /// `schema.table` for `DROP RULE` / `COMMENT ON RULE`.
    fn relation(&self) -> String {
        format!("{}.{}", self.schema, quoted(&self.table_name))
    }

    fn comment_script(&self, comment: Option<&str>) -> String {
        format!(
            "comment on rule {} on {} is {};",
            quoted(&self.rule_name),
            self.relation(),
            comment.map_or_else(
                || "null".to_string(),
                |c| format!("'{}'", c.replace('\'', "''"))
            )
        )
    }

    /// `CREATE RULE` for a rule the migrated database does not have, followed
    /// by its comment.
    pub fn get_script(&self) -> String {
        let mut script = format!("{};", self.statement()).with_empty_lines();
        if let Some(comment) = &self.comment {
            script.append_block(&self.comment_script(Some(comment)));
        }
        script
    }

    /// `CREATE OR REPLACE RULE` plus the comment: the rule as it must be after
    /// the migration, whether or not the migrated database still has it. Used
    /// where the rule may have been dropped along the way (its relation was
    /// recreated, or it was dropped ahead of a change to something it reads)
    /// and wherever a replayable statement is wanted.
    pub fn get_replace_script(&self) -> String {
        let mut script = format!("{};", self.replace_statement()).with_empty_lines();
        if let Some(comment) = &self.comment {
            script.append_block(&self.comment_script(Some(comment)));
        }
        script
    }

    /// [`Rule::get_replace_script`] for a rule (`self`, TO) that the
    /// migration may have dropped on the way, given its FROM version.
    ///
    /// Whether the rule really went is not always known: a view flagged as
    /// taken by a routine's `CASCADE` can be a textual false positive, and
    /// then the rule is still there, FROM comment included. So a comment TO no
    /// longer has is cleared with `comment ... is null`, harmless on a freshly
    /// created rule. With `use_drop` off it is left in place, as in
    /// [`Rule::get_alter_script`].
    pub fn get_recreate_script(&self, from: &Rule, use_drop: bool) -> String {
        let mut script = self.get_replace_script();
        if use_drop && self.comment.is_none() && from.comment.is_some() {
            script.append_block(&self.comment_script(None));
        }
        script
    }

    pub fn get_drop_script(&self) -> String {
        format!(
            "drop rule if exists {} on {} cascade;",
            quoted(&self.rule_name),
            self.relation()
        )
        .with_empty_lines()
    }

    /// The migration from `self` (FROM) to `target` (TO): `CREATE OR REPLACE
    /// RULE` when the definition changed, then the comment change. With
    /// `use_drop` off a removed comment is left in place.
    pub fn get_alter_script(&self, target: &Rule, use_drop: bool) -> String {
        let mut script = String::new();

        if self.definition != target.definition {
            script.append_block(&format!("{};", target.replace_statement()));
        }

        if self.comment != target.comment {
            if let Some(cmt) = &target.comment {
                script.append_block(&target.comment_script(Some(cmt)));
            } else if use_drop {
                script.append_block(&target.comment_script(None));
            }
        }

        script
    }
}

/// A rule or table name as it goes into `DROP RULE` / `COMMENT ON RULE`. The
/// dump stores both `quote_ident`-ed, so a name that needed quoting already
/// carries its quotes and is used as it is — wrapping it again turned
/// `"My Rule"` into `"""My Rule"""`, a different name, and the drop silently
/// did nothing. A name that did not need quoting is quoted here, which is how
/// these statements have always spelled it.
fn quoted(ident: &str) -> String {
    if ident.starts_with('"') {
        ident.to_string()
    } else {
        format!("\"{}\"", ident.replace('"', "\"\""))
    }
}

#[cfg(test)]
#[path = "tests/rule.rs"]
mod tests;
