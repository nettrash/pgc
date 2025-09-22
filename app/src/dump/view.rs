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
            md5::compute(format!("{}.{} {}", self.schema, self.name, self.definition))
        )
    }

    /// Returns a string to create the view.
    pub fn get_script(&self) -> String {
        let script = format!(
            "create view {}.{} as {};\n",
            self.schema, self.name, self.definition
        );

        script
    }

    /// Returns a string to drop the view.
    pub fn get_drop_script(&self) -> String {
        format!("drop view if exists {}.{};\n", self.schema, self.name)
    }
}
