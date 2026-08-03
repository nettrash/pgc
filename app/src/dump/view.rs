//! Views and materialized views (`pg_class`), carried as their deparsed
//! definition.
//!
//! Ordering is the whole problem: a view may read other views, so creation is
//! topologically sorted and dropping runs in reverse. A change is applied with
//! `CREATE OR REPLACE` where PostgreSQL allows it — which requires the existing
//! column list to be a prefix of the new one — and otherwise degrades to a drop
//! and recreate that also takes every dependent view with it.
//!
//! Materialized views are separate objects with their own indexes, and a regular ↔
//! materialized transition is always a drop and recreate.

use serde::{Deserialize, Serialize};

use crate::dump::table::IndexAlterPlan;
use crate::dump::table_index::TableIndex;
use crate::utils::string_extensions::StringExt;

/// One output column of a regular view, as PostgreSQL records it in
/// `pg_attribute`. Captured at dump time solely to decide whether a changed view
/// can be updated with `CREATE OR REPLACE VIEW` or must be dropped and recreated
/// (issue #227).
///
/// Deliberately excluded from `View::hash`: dumps written before this field
/// existed carry no column data, so hashing it would make every regular view
/// compare as changed against an older dump, and `format_type` renderings could
/// in principle drift across server versions and churn the hash. Exclusion is
/// safe for change *detection* because the deparsed definition — which is hashed
/// — always reflects the current column names and expressions (verified live:
/// `ALTER VIEW ... RENAME COLUMN` re-renders the select list with an `AS` alias
/// for the new name). The column list therefore only decides *how* an
/// already-detected change is emitted, never *whether* a change exists.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ViewColumn {
    /// Column name (`pg_attribute.attname`)
    pub name: String,
    /// Formatted type including any typmod, e.g. `character varying(10)`
    /// (`format_type(atttypid, atttypmod)`)
    pub data_type: String,
    /// Collation of the column when it is collatable; `None` for non-collatable
    /// types. `pg_catalog` collations are recorded by bare name (`default`, `C`);
    /// any other collation is schema-qualified (`myschema.mycoll`), because
    /// collations are schema-scoped and two different collations may share a bare
    /// name — PostgreSQL rejects an OR REPLACE across them ("cannot change
    /// collation of view column ... from \"mycoll\" to \"mycoll\""), so the
    /// captured value must distinguish them too
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub collation: Option<String>,
}

/// This is an information about a PostgreSQL view.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct View {
    /// Schema where the view is defined
    pub schema: String,
    /// Name of the view
    pub name: String,
    /// Definition of the view
    pub definition: String,
    /// Table relation (list of tables that used by this view)
    pub table_relation: Vec<String>,
    /// Owner of the view
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub owner: String,
    /// Optional comment on the view
    #[serde(default)]
    pub comment: Option<String>,
    /// Whether this is a materialized view
    #[serde(default)]
    pub is_materialized: bool,
    /// Whether a materialized view holds data. `false` means it was created (or last
    /// refreshed) `WITH NO DATA` and is not scannable until refreshed. Meaningless for
    /// regular views. Dumps written before this field existed default to populated,
    /// which is how every materialized view they could describe was created.
    #[serde(default = "View::default_is_populated")]
    pub is_populated: bool,
    /// Hash of the view
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hash: Option<String>,
    /// ACL (grant) entries for this view
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub acl: Vec<String>,
    /// Whether this view uses SECURITY INVOKER (PG15+)
    #[serde(default)]
    pub security_invoker: bool,
    /// WITH CHECK OPTION: "local" or "cascaded" (None = no check option)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub check_option: Option<String>,
    /// Column comments: (column_name, comment_text)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub column_comments: Vec<(String, String)>,
    /// Storage parameters for materialized views (e.g. fillfactor=70)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_parameters: Option<Vec<String>>,
    /// Tablespace for materialized views
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tablespace: Option<String>,
    /// Output columns in ordinal order (regular views only; empty for materialized
    /// views and for dumps written by older pgc versions). Used only by
    /// [`View::or_replace_compatible`]; not part of the hash.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub columns: Vec<ViewColumn>,
    /// Indexes defined on a materialized view, ordered by name. Always empty for
    /// regular views — PostgreSQL only allows indexing a materialized one — and
    /// for dumps written before this field existed (issue #235).
    ///
    /// Deliberately excluded from `View::hash`: a materialized view is dropped and
    /// fully rebuilt whenever its hash changes, so hashing the index list would
    /// turn "an index was added" into a full refresh of the view's contents, and
    /// would additionally make every materialized view compare as changed against
    /// an older dump that carries no index data. The comparer diffs this list on
    /// its own and emits plain `CREATE INDEX` / `DROP INDEX` for a view whose
    /// definition did not change.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub indexes: Vec<TableIndex>,
}

impl View {
    /// Creates a new View with the given name, definition, and schema
    pub fn new(
        name: String,
        definition: String,
        schema: String,
        table_relation: Vec<String>,
    ) -> Self {
        let mut view = Self {
            schema,
            name,
            definition,
            table_relation,
            owner: String::new(),
            comment: None,
            is_materialized: false,
            is_populated: Self::default_is_populated(),
            hash: None,
            acl: Vec::new(),
            security_invoker: false,
            check_option: None,
            column_comments: Vec::new(),
            storage_parameters: None,
            tablespace: None,
            columns: Vec::new(),
            indexes: Vec::new(),
        };
        view.hash();
        view
    }

    fn default_is_populated() -> bool {
        true
    }

    /// Whether `CREATE OR REPLACE VIEW` can turn this view into `target`.
    ///
    /// PostgreSQL accepts `OR REPLACE` only when every existing column keeps its
    /// name, type (including typmod) and collation at the same position, and any
    /// new columns are appended strictly at the end — inserting, reordering,
    /// renaming, retyping or dropping a column is rejected (`cannot change name of
    /// view column ...`, verified live on PostgreSQL 16). In prefix terms: the old
    /// column list must be an exact prefix of the new one.
    ///
    /// When column data is missing on either side — a dump written by an older pgc
    /// — incompatibility cannot be proven and this returns `true`, preserving the
    /// historical `CREATE OR REPLACE` behavior for old dumps.
    pub fn or_replace_compatible(&self, target: &View) -> bool {
        if self.columns.is_empty() || target.columns.is_empty() {
            return true;
        }
        if target.columns.len() < self.columns.len() {
            return false;
        }
        self.columns
            .iter()
            .zip(&target.columns)
            .all(|(a, b)| a == b)
    }

    /// Returns the SQL keyword for this view type ("view" or "materialized view")
    pub fn view_keyword(&self) -> &'static str {
        if self.is_materialized {
            "materialized view"
        } else {
            "view"
        }
    }

    /// Hash
    pub fn hash(&mut self) {
        let col_comments_str = self
            .column_comments
            .iter()
            .map(|(c, t)| format!("{c}={t}"))
            .collect::<Vec<_>>()
            .join(",");
        let storage_str = self
            .storage_parameters
            .as_ref()
            .map(|v| v.join(","))
            .unwrap_or_default();
        self.hash = Some(format!(
            "{:x}",
            md5::compute(format!(
                "{}.{}.{}.{}.{}.{}.{}.{}.{}.{}.{}",
                self.schema,
                self.name,
                // Canonicalize so PostgreSQL's non-idempotent IN-list deparsing does
                // not make a materialized view look changed on every run (issue #226).
                crate::utils::sql_normalize::canonicalize_definition(&self.definition),
                self.owner,
                self.comment.clone().unwrap_or_default(),
                self.is_materialized,
                self.security_invoker,
                self.check_option.clone().unwrap_or_default(),
                col_comments_str,
                storage_str,
                self.tablespace.clone().unwrap_or_default(),
            ))
        ));
    }

    /// Returns a string to create the view, including the indexes of a
    /// materialized view. A materialized view is always dropped and recreated
    /// rather than replaced in place, and `DROP MATERIALIZED VIEW` takes its
    /// indexes with it, so the CREATE has to put them back (issue #235).
    pub fn get_script(&self) -> String {
        let mut script = self.get_script_without_indexes();
        for index in &self.indexes {
            script.push_str(&index.get_script());
        }
        script
    }

    /// The CREATE script without the materialized view's indexes. Used by the
    /// production output path, which emits them separately so they can be built
    /// concurrently, mirroring `Table::get_script_without_triggers_no_indexes`.
    pub fn get_script_without_indexes(&self) -> String {
        let keyword = self.view_keyword();
        let with_clause = if self.security_invoker {
            " with (security_invoker = true)"
        } else {
            ""
        };

        // PostgreSQL renders a view definition with its terminating semicolon, but
        // every trailing clause below belongs *inside* the statement. Drop the
        // semicolon here and put it back once the clauses are attached, otherwise
        // they land after the statement has already ended and fail to parse.
        let definition = self.definition.trim_end();
        let body = definition.strip_suffix(';').unwrap_or(definition);

        let mut create_stmt = format!(
            "create {} {}.{}{} as\n{}",
            keyword, self.schema, self.name, with_clause, body
        );

        // WITH CHECK OPTION (regular views only)
        if !self.is_materialized
            && let Some(ref co) = self.check_option
        {
            match co.to_lowercase().as_str() {
                "local" => create_stmt.push_str("\nwith local check option"),
                _ => create_stmt.push_str("\nwith cascaded check option"),
            }
        }

        // An unpopulated materialized view has to be created empty: without the
        // clause the definition runs and fills it, which is what WITH NO DATA exists
        // to avoid.
        if self.is_materialized && !self.is_populated {
            create_stmt.push_str("\nwith no data");
        }

        create_stmt.push(';');

        let mut script = create_stmt.with_empty_lines();

        // Storage parameters and tablespace for materialized views
        if self.is_materialized {
            if let Some(ref params) = self.storage_parameters
                && !params.is_empty()
            {
                script.append_block(&format!(
                    "alter materialized view {}.{} set ({});",
                    self.schema,
                    self.name,
                    params.join(", ")
                ));
            }
            if let Some(ref space) = self.tablespace {
                script.append_block(&format!(
                    "alter materialized view {}.{} set tablespace {};",
                    self.schema, self.name, space
                ));
            }
        }

        // View comment
        if let Some(comment) = &self.comment {
            script.append_block(&format!(
                "comment on {} {}.{} is '{}';",
                keyword,
                self.schema,
                self.name,
                comment.replace('\'', "''")
            ));
        }

        // Column comments
        for (col, text) in &self.column_comments {
            script.append_block(&format!(
                "comment on column {}.{}.{} is '{}';",
                self.schema,
                self.name,
                col,
                text.replace('\'', "''")
            ));
        }

        script.push_str(&self.get_owner_script());
        script
    }

    /// Returns a string to drop the view.
    pub fn get_drop_script(&self) -> String {
        format!(
            "drop {} if exists {}.{};",
            self.view_keyword(),
            self.schema,
            self.name
        )
        .with_empty_lines()
    }

    /// Structured index diff between `self` (FROM) and `to_view` (TO) for a
    /// materialized view whose definition did not change, so it is not being
    /// dropped and recreated and its indexes have to be reconciled in place.
    /// Mirrors `Table::index_alter_plan`; a materialized view can never carry a
    /// partition-inherited index, so there is nothing to skip.
    pub fn index_alter_plan<'a>(&'a self, to_view: &'a View) -> IndexAlterPlan<'a> {
        let mut plan = IndexAlterPlan::default();

        for new_index in &to_view.indexes {
            if let Some(old_index) = self.indexes.iter().find(|i| i.name == new_index.name) {
                if old_index != new_index {
                    if crate::dump::table_index::indexdefs_equivalent(
                        &old_index.indexdef,
                        &new_index.indexdef,
                    ) {
                        plan.comment_changes.push(new_index);
                    } else {
                        plan.drop.push(old_index);
                        plan.create.push(new_index);
                    }
                }
            } else {
                plan.create.push(new_index);
            }
        }

        for old_index in &self.indexes {
            if !to_view.indexes.iter().any(|i| i.name == old_index.name) {
                plan.drop.push(old_index);
            }
        }

        plan
    }

    pub fn get_owner_script(&self) -> String {
        if self.owner.is_empty() {
            return String::new();
        }

        format!(
            "alter {} {}.{} owner to {};",
            self.view_keyword(),
            self.schema,
            self.name,
            self.owner
        )
        .with_empty_lines()
    }

    /// Returns a script that alters the current view to match the target definition.
    pub fn get_alter_script(&self, target: &View, use_drop: bool) -> String {
        if self.schema != target.schema || self.name != target.name {
            return format!(
                "-- Cannot alter view {}.{} because target is {}.{}\n",
                self.schema, self.name, target.schema, target.name
            );
        }

        // Compare canonicalized forms so a non-idempotent IN-list deparse (issue #226)
        // is not seen as a definition change, while still emitting the raw definition.
        // `canonicalize_definition` trims, so this matches what `hash()` feeds the hash.
        let has_definition_change =
            crate::utils::sql_normalize::canonicalize_definition(&self.definition)
                != crate::utils::sql_normalize::canonicalize_definition(&target.definition);
        let has_kind_change = self.is_materialized != target.is_materialized;
        let has_security_invoker_change = self.security_invoker != target.security_invoker;
        let has_check_option_change = self.check_option != target.check_option;
        let has_comment_change = self.comment != target.comment;
        let has_column_comment_change = self.column_comments != target.column_comments;
        let has_storage_change = self.storage_parameters != target.storage_parameters;
        let has_tablespace_change = self.tablespace != target.tablespace;

        if !has_definition_change
            && !has_kind_change
            && !has_security_invoker_change
            && !has_check_option_change
            && !has_comment_change
            && !has_column_comment_change
            && !has_storage_change
            && !has_tablespace_change
        {
            return format!(
                "-- View {}.{} requires no changes.\n",
                self.schema, self.name
            );
        }

        // When the view kind changes (regular <-> materialized) or the target is
        // a materialized view, we must drop and recreate because neither kind
        // supports an in-place ALTER to the other, and materialized views do not
        // support CREATE OR REPLACE. The same applies when the column list changed
        // incompatibly (issue #227): CREATE OR REPLACE VIEW only allows appending
        // columns at the end, so inserting/reordering/renaming/retyping requires
        // drop+recreate as well.
        if target.is_materialized
            || has_kind_change
            || (has_definition_change && !self.or_replace_compatible(target))
        {
            // DROP must match the *current* object type so the existing object
            // is actually removed.
            let drop_script = self.get_drop_script();
            if use_drop {
                return format!("{}{}", drop_script, target.get_script());
            } else {
                let commented_drop = drop_script
                    .lines()
                    .map(|l| format!("-- {}\n", l))
                    .collect::<String>();
                let commented_create = target
                    .get_script()
                    .lines()
                    .map(|l| format!("-- {}\n", l))
                    .collect::<String>();
                return format!(
                    "-- use_drop=false: view {}.{} requires drop+recreate; statements commented out (manual intervention needed)\n{}{}",
                    target.schema, target.name, commented_drop, commented_create
                );
            }
        }

        let mut script = String::new();

        // Definition or check_option change requires CREATE OR REPLACE
        if has_definition_change || has_check_option_change {
            let with_clause = if target.security_invoker {
                " with (security_invoker = true)"
            } else {
                ""
            };
            // As in get_script: the check option belongs inside the statement, so the
            // definition's terminating semicolon comes off and goes back on at the end.
            let desired = target.definition.trim_end();
            let body = desired.strip_suffix(';').unwrap_or(desired);
            let mut create_stmt = format!(
                "CREATE OR REPLACE VIEW {}.{}{} AS\n{}",
                target.schema, target.name, with_clause, body
            );
            if let Some(ref co) = target.check_option {
                match co.to_lowercase().as_str() {
                    "local" => create_stmt.push_str("\nwith local check option"),
                    _ => create_stmt.push_str("\nwith cascaded check option"),
                }
            }
            create_stmt.push(';');
            script = create_stmt.with_empty_lines();
        }

        // Handle security_invoker changes (only when definition didn't change,
        // because CREATE OR REPLACE already includes the with clause)
        if has_security_invoker_change && !has_definition_change && !has_check_option_change {
            if target.security_invoker {
                script.append_block(&format!(
                    "alter view {}.{} set (security_invoker = true);",
                    target.schema, target.name
                ));
            } else {
                script.append_block(&format!(
                    "alter view {}.{} reset (security_invoker);",
                    target.schema, target.name
                ));
            }
        }

        // Handle view comment change
        if has_comment_change {
            let keyword = target.view_keyword();
            if let Some(ref comment) = target.comment {
                script.append_block(&format!(
                    "comment on {} {}.{} is '{}';",
                    keyword,
                    target.schema,
                    target.name,
                    comment.replace('\'', "''")
                ));
            } else {
                script.append_block(&format!(
                    "comment on {} {}.{} is null;",
                    keyword, target.schema, target.name
                ));
            }
        }

        // Handle column comment changes
        if has_column_comment_change {
            // Build maps for old and new column comments
            let old_map: std::collections::HashMap<&str, &str> = self
                .column_comments
                .iter()
                .map(|(c, t)| (c.as_str(), t.as_str()))
                .collect();
            let new_map: std::collections::HashMap<&str, &str> = target
                .column_comments
                .iter()
                .map(|(c, t)| (c.as_str(), t.as_str()))
                .collect();

            // Add/update column comments
            for (col, text) in &target.column_comments {
                if old_map.get(col.as_str()) != Some(&text.as_str()) {
                    script.append_block(&format!(
                        "comment on column {}.{}.{} is '{}';",
                        target.schema,
                        target.name,
                        col,
                        text.replace('\'', "''")
                    ));
                }
            }
            // Remove old column comments
            for (col, _) in &self.column_comments {
                if !new_map.contains_key(col.as_str()) {
                    script.append_block(&format!(
                        "comment on column {}.{}.{} is null;",
                        target.schema, target.name, col
                    ));
                }
            }
        }

        script
    }
}

#[cfg(test)]
#[path = "tests/view.rs"]
mod tests;
