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
        self.hash = Some(format!(
            "{:x}",
            md5::compute(format!(
                "{}.{}.{}.{}.{}.{}",
                self.schema,
                self.name,
                self.definition,
                self.owner,
                self.comment.clone().unwrap_or_default(),
                self.is_materialized
            ))
        ));
    }

    /// Returns a string to create the view.
    pub fn get_script(&self) -> String {
        let keyword = self.view_keyword();
        let script = format!(
            "create {} {}.{} as\n{}",
            keyword,
            self.schema,
            self.name,
            self.definition.trim_end()
        )
        .with_empty_lines();

        let mut script = if let Some(comment) = &self.comment {
            format!(
                "{}comment on {} {}.{} is '{}';",
                script,
                keyword,
                self.schema,
                self.name,
                comment.replace('\'', "''")
            )
            .with_empty_lines()
        } else {
            script
        };

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

        if current_definition == desired_definition
            && self.is_materialized == target.is_materialized
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
        let kind_changed = self.is_materialized != target.is_materialized;
        if target.is_materialized || kind_changed {
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

        let script = target.get_script();
        if script.to_uppercase().contains("CREATE OR REPLACE VIEW") {
            return script;
        }

        format!(
            "CREATE OR REPLACE VIEW {}.{} AS\n{}",
            target.schema,
            target.name,
            target.definition.trim_end()
        )
        .with_empty_lines()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_view(definition: &str) -> View {
        View::new(
            "active_users".to_string(),
            definition.to_string(),
            "analytics".to_string(),
            vec!["public.users".to_string(), "public.sessions".to_string()],
        )
    }

    fn create_materialized_view(definition: &str) -> View {
        let mut view = View::new(
            "active_users".to_string(),
            definition.to_string(),
            "analytics".to_string(),
            vec!["public.users".to_string()],
        );
        view.is_materialized = true;
        view.hash();
        view
    }

    #[test]
    fn test_view_new_initializes_hash() {
        let definition = "select id from public.users where active";
        let view = create_view(definition);

        let expected_hash = format!(
            "{:x}",
            md5::compute(format!("analytics.active_users.{definition}...false"))
        );

        assert_eq!(view.hash.as_deref(), Some(expected_hash.as_str()));
        assert_eq!(view.schema, "analytics");
        assert_eq!(view.name, "active_users");
        assert_eq!(view.definition, definition);
    }

    #[test]
    fn test_hash_updates_on_mutation() {
        let mut view = create_view("select 1");
        let original_hash = view.hash.clone();

        view.definition = "select 2".to_string();
        view.hash();

        assert_ne!(view.hash, original_hash);
    }

    #[test]
    fn test_get_script_returns_create_statement() {
        let view = create_view("select id from public.users");
        assert_eq!(
            view.get_script(),
            "create view analytics.active_users as\nselect id from public.users\n\n"
        );
    }

    #[test]
    fn test_get_script_returns_create_materialized_statement() {
        let view = create_materialized_view("select id from public.users");
        assert_eq!(
            view.get_script(),
            "create materialized view analytics.active_users as\nselect id from public.users\n\n"
        );
    }

    #[test]
    fn test_get_script_includes_owner_when_present() {
        let mut view = create_view("select id from public.users");
        view.owner = "pgc_owner".to_string();
        view.hash();

        assert_eq!(
            view.get_script(),
            "create view analytics.active_users as\nselect id from public.users\n\nalter view analytics.active_users owner to pgc_owner;\n\n"
        );
    }

    #[test]
    fn test_get_script_includes_owner_for_materialized_view() {
        let mut view = create_materialized_view("select id from public.users");
        view.owner = "pgc_owner".to_string();
        view.hash();

        assert_eq!(
            view.get_script(),
            "create materialized view analytics.active_users as\nselect id from public.users\n\nalter materialized view analytics.active_users owner to pgc_owner;\n\n"
        );
    }

    #[test]
    fn test_get_drop_script_returns_drop_statement() {
        let view = create_view("select id from public.users");
        assert_eq!(
            view.get_drop_script(),
            "drop view if exists analytics.active_users;\n\n"
        );
    }

    #[test]
    fn test_get_drop_script_returns_drop_materialized_statement() {
        let view = create_materialized_view("select id from public.users");
        assert_eq!(
            view.get_drop_script(),
            "drop materialized view if exists analytics.active_users;\n\n"
        );
    }

    #[test]
    fn test_get_alter_script_returns_noop_when_definitions_match() {
        let view = create_view("select 1");
        let mut target = view.clone();
        target.definition = "select 1".to_string();

        assert_eq!(
            view.get_alter_script(&target, true),
            "-- View analytics.active_users requires no changes.\n"
        );
    }

    #[test]
    fn test_get_alter_script_returns_error_for_different_identifiers() {
        let view = create_view("select 1");
        let target = View::new(
            "other".to_string(),
            "select 2".to_string(),
            "analytics".to_string(),
            vec![],
        );

        assert_eq!(
            view.get_alter_script(&target, true),
            "-- Cannot alter view analytics.active_users because target is analytics.other\n"
        );
    }

    #[test]
    fn test_get_alter_script_respects_create_or_replace_definition() {
        let current = create_view("select 1");
        let replacement = create_view("create or replace view analytics.active_users as select 2");

        assert_eq!(
            current.get_alter_script(&replacement, true),
            "create view analytics.active_users as\ncreate or replace view analytics.active_users as select 2\n\n"
        );
    }

    #[test]
    fn test_get_alter_script_generates_replace_statement() {
        let current = create_view("select 1");
        let target = create_view("select id, active from public.users where active");

        assert_eq!(
            current.get_alter_script(&target, true),
            "CREATE OR REPLACE VIEW analytics.active_users AS\nselect id, active from public.users where active\n\n"
        );
    }

    #[test]
    fn test_get_alter_script_materialized_drops_and_recreates() {
        let current = create_materialized_view("select 1");
        let target = create_materialized_view("select id from public.users");

        assert_eq!(
            current.get_alter_script(&target, true),
            "drop materialized view if exists analytics.active_users;\n\ncreate materialized view analytics.active_users as\nselect id from public.users\n\n"
        );
    }

    #[test]
    fn test_get_alter_script_materialized_use_drop_false() {
        let current = create_materialized_view("select 1");
        let target = create_materialized_view("select id from public.users");

        let script = current.get_alter_script(&target, false);

        // Should contain a warning about manual intervention
        assert!(
            script.contains("use_drop=false") && script.contains("manual intervention needed"),
            "should contain a warning comment, script:\n{}",
            script
        );

        // Both drop and create should be commented out
        for line in script.lines() {
            if line.contains("drop materialized view") || line.contains("create materialized view")
            {
                assert!(line.starts_with("--"), "should be commented: {}", line);
            }
        }
    }

    #[test]
    fn test_get_alter_script_materialized_use_drop_true_contains_active_drop() {
        let current = create_materialized_view("select 1");
        let target = create_materialized_view("select id from public.users");

        let script = current.get_alter_script(&target, true);

        // The drop line should NOT be commented
        for line in script.lines() {
            if line.contains("drop materialized view") {
                assert!(!line.starts_with("--"), "drop should be active: {}", line);
            }
        }
    }

    #[test]
    fn test_get_alter_script_regular_view_unaffected_by_use_drop() {
        let current = create_view("select 1");
        let target = create_view("select id, active from public.users where active");

        let with_drop = current.get_alter_script(&target, true);
        let without_drop = current.get_alter_script(&target, false);

        // Regular views use CREATE OR REPLACE, no drop involved
        assert_eq!(with_drop, without_drop);
        assert!(!with_drop.contains("drop"));
    }

    #[test]
    fn test_get_alter_script_regular_to_materialized_drops_view() {
        let current = create_view("select 1");
        let target = create_materialized_view("select 1");

        let script = current.get_alter_script(&target, true);

        // DROP must target the current kind (regular view), not the target kind
        assert!(
            script.contains("drop view if exists"),
            "should drop the regular view, script:\n{}",
            script
        );
        assert!(
            !script.contains("drop materialized view"),
            "should NOT emit DROP MATERIALIZED VIEW for a regular view"
        );
        // Then create the materialized view
        assert!(script.contains("create materialized view"));
    }

    #[test]
    fn test_get_alter_script_materialized_to_regular_drops_materialized() {
        let current = create_materialized_view("select 1");
        let target = create_view("select 1");

        let script = current.get_alter_script(&target, true);

        // DROP must target the current kind (materialized view)
        assert!(
            script.contains("drop materialized view if exists"),
            "should drop the materialized view, script:\n{}",
            script
        );
        // Then create the regular view
        assert!(script.contains("create view"));
    }

    #[test]
    fn test_get_alter_script_regular_to_materialized_use_drop_false() {
        let current = create_view("select 1");
        let target = create_materialized_view("select 1");

        let script = current.get_alter_script(&target, false);

        assert!(
            script.contains("use_drop=false") && script.contains("manual intervention needed"),
            "should warn about manual intervention, script:\n{}",
            script
        );

        // Both drop and create should be commented out
        for line in script.lines() {
            if line.contains("drop view") || line.contains("create materialized view") {
                assert!(line.starts_with("--"), "should be commented: {}", line);
            }
        }
    }

    #[test]
    fn test_get_alter_script_materialized_to_regular_use_drop_false() {
        let current = create_materialized_view("select 1");
        let target = create_view("select 1");

        let script = current.get_alter_script(&target, false);

        assert!(
            script.contains("use_drop=false") && script.contains("manual intervention needed"),
            "should warn about manual intervention, script:\n{}",
            script
        );

        for line in script.lines() {
            if line.contains("drop materialized view") || line.contains("create view") {
                assert!(line.starts_with("--"), "should be commented: {}", line);
            }
        }
    }
}
