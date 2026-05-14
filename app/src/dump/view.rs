use serde::{Deserialize, Serialize};

use crate::utils::string_extensions::StringExt;

// This is an information about a PostgreSQL view.
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
            hash: None,
            acl: Vec::new(),
            security_invoker: false,
            check_option: None,
            column_comments: Vec::new(),
            storage_parameters: None,
            tablespace: None,
        };
        view.hash();
        view
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
                self.definition,
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

    /// Returns a string to create the view.
    pub fn get_script(&self) -> String {
        let keyword = self.view_keyword();
        let with_clause = if self.security_invoker {
            " with (security_invoker = true)"
        } else {
            ""
        };

        let mut create_stmt = format!(
            "create {} {}.{}{} as\n{}",
            keyword,
            self.schema,
            self.name,
            with_clause,
            self.definition.trim_end()
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

        let current_definition = self.definition.trim();
        let desired_definition = target.definition.trim();

        let has_definition_change = current_definition != desired_definition;
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
        // support CREATE OR REPLACE.
        if target.is_materialized || has_kind_change {
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
            let mut create_stmt = format!(
                "CREATE OR REPLACE VIEW {}.{}{} AS\n{}",
                target.schema,
                target.name,
                with_clause,
                target.definition.trim_end()
            );
            if let Some(ref co) = target.check_option {
                match co.to_lowercase().as_str() {
                    "local" => create_stmt.push_str("\nwith local check option"),
                    _ => create_stmt.push_str("\nwith cascaded check option"),
                }
            }
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
#[path = "view_tests.rs"]
mod tests;
