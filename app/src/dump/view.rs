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
}

impl View {
    /// Creates a new View with the given name, definition, and schema
    pub fn new(
        name: String,
        definition: String,
        schema: String,
        table_relation: Vec<String>,
    ) -> Self {
        Self {
            schema,
            name,
            definition,
            table_relation,
        }
    }

    /// Hash
    pub fn hash(&self) -> String {
        format!(
            "{:x}",
            md5::compute(format!("{}.{}.{}", self.schema, self.name, self.definition))
        )
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
    use super::View;

    fn make_view(definition: &str) -> View {
        View {
            schema: "public".to_string(),
            name: "example_view".to_string(),
            definition: definition.to_string(),
            table_relation: vec!["public.example".to_string()],
        }
    }

    #[test]
    fn alter_script_returns_comment_when_definitions_match() {
        let current = make_view("select 1 as id");
        let target = make_view("select 1 as id");

        let script = current.get_alter_script(&target);

        assert!(script.contains("requires no changes"));
    }

    #[test]
    fn alter_script_replaces_definition_when_changed() {
        let current = make_view("select 1 as id");
        let target = make_view("select 2 as id");

        let script = current.get_alter_script(&target);

        assert!(script.starts_with("CREATE OR REPLACE VIEW public.example_view AS"));
        assert!(script.contains("select 2 as id"));
    }

    #[test]
    fn alter_script_handles_mismatched_names() {
        let current = make_view("select 1 as id");
        let mut target = make_view("select 1 as id");
        target.name = "other_view".to_string();

        let script = current.get_alter_script(&target);

        assert!(script.contains("Cannot alter view"));
    }
}
