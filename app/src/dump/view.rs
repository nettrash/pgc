use serde::{Deserialize, Serialize};

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
    /// Hash of the view
    pub hash: Option<String>,
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
            hash: None,
        };
        view.hash();
        view
    }

    /// Hash
    pub fn hash(&mut self) {
        self.hash = Some(format!(
            "{:x}",
            md5::compute(format!("{}.{}.{}", self.schema, self.name, self.definition))
        ));
    }

    /// Returns a string to create the view.
    pub fn get_script(&self) -> String {
        let script = format!(
            "create view {}.{} as\n{}\n",
            self.schema, self.name, self.definition
        );

        script
    }

    /// Returns a string to drop the view.
    pub fn get_drop_script(&self) -> String {
        format!("drop view if exists {}.{};\n", self.schema, self.name)
    }

    /// Returns a script that alters the current view to match the target definition.
    pub fn get_alter_script(&self, target: &View) -> String {
        if self.schema != target.schema || self.name != target.name {
            return format!(
                "-- Cannot alter view {}.{} because target is {}.{}\n",
                self.schema, self.name, target.schema, target.name
            );
        }

        let current_definition = self.definition.trim();
        let desired_definition = target.definition.trim();

        if current_definition == desired_definition {
            return format!(
                "-- View {}.{} requires no changes.\n",
                self.schema, self.name
            );
        }

        let script = target.get_script();
        if script.to_uppercase().contains("CREATE OR REPLACE VIEW") {
            return script;
        }

        format!(
            "CREATE OR REPLACE VIEW {}.{} AS\n{}\n",
            target.schema,
            target.name,
            target.definition.trim_end()
        )
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

    #[test]
    fn test_view_new_initializes_hash() {
        let definition = "select id from public.users where active";
        let view = create_view(definition);

        let expected_hash = format!(
            "{:x}",
            md5::compute(format!("analytics.active_users.{definition}"))
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
            "create view analytics.active_users as\nselect id from public.users\n"
        );
    }

    #[test]
    fn test_get_drop_script_returns_drop_statement() {
        let view = create_view("select id from public.users");
        assert_eq!(
            view.get_drop_script(),
            "drop view if exists analytics.active_users;\n"
        );
    }

    #[test]
    fn test_get_alter_script_returns_noop_when_definitions_match() {
        let view = create_view("select 1");
        let mut target = view.clone();
        target.definition = "select 1".to_string();

        assert_eq!(
            view.get_alter_script(&target),
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
            view.get_alter_script(&target),
            "-- Cannot alter view analytics.active_users because target is analytics.other\n"
        );
    }

    #[test]
    fn test_get_alter_script_respects_create_or_replace_definition() {
        let current = create_view("select 1");
        let replacement = create_view("create or replace view analytics.active_users as select 2");

        assert_eq!(
            current.get_alter_script(&replacement),
            "create view analytics.active_users as\ncreate or replace view analytics.active_users as select 2\n"
        );
    }

    #[test]
    fn test_get_alter_script_generates_replace_statement() {
        let current = create_view("select 1");
        let target = create_view("select id, active from public.users where active");

        assert_eq!(
            current.get_alter_script(&target),
            "CREATE OR REPLACE VIEW analytics.active_users AS\nselect id, active from public.users where active\n"
        );
    }
}
