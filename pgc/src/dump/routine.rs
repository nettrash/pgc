use serde::{Deserialize, Serialize};
use sqlx::postgres::types::Oid;

// This is an information about a PostgreSQL routine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Routine {
    /// The schema name of the routine.
    pub schema: String,
    /// The object identifier of the routine.
    pub oid: Oid,
    /// The name of the routine.
    pub name: String,
    /// The language of the routine (e.g., 'plpgsql', 'sql').
    pub lang: String,
    /// The kind of the routine (e.g., 'function', 'procedure').
    pub kind: String,
    /// The return type of the routine (e.g., 'void', 'integer').
    pub return_type: String,
    /// The arguments of the routine, formatted as a string.
    pub arguments: String,
    /// The default values for the arguments, formatted as a string.
    pub arguments_defaults: Option<String>,
    /// The description of the routine.
    pub source_code: String,
}

impl Routine {
    /// Creates a new `Routine` instance.
    pub fn new(
        schema: String,
        oid: Oid,
        name: String,
        lang: String,
        kind: String,
        return_type: String,
        arguments: String,
        arguments_defaults: Option<String>,
        source_code: String,
    ) -> Self {
        Self {
            schema,
            oid,
            name,
            lang,
            kind,
            return_type,
            arguments,
            arguments_defaults,
            source_code,
        }
    }

    /// Hash
    pub fn hash(&self) -> String {
        format!("{:x}", md5::compute(format!(
            "{}.{}.{}.{}.{}.{}.{}.{}",
            self.schema,
            self.name,
            self.lang,
            self.kind,
            self.return_type,
            self.arguments,
            self.arguments_defaults.as_deref().unwrap_or(""),
            self.source_code
        )))
    }

    /// Returns a string to create the routine.
    pub fn get_script(&self) -> String {
        let mut script = format!(
            "create or replace {} {}.{}({}) returns {} as $${}$$ language {};\n",
            self.kind.to_lowercase(),
            self.schema,
            self.name,
            self.arguments,
            self.return_type,
            self.source_code,
            self.lang
        );

        if let Some(defaults) = &self.arguments_defaults {
            script.push_str(&format!("-- Defaults: {}\n", defaults));
        }

        script
    }

    /// Returns a string to drop the routine.
    pub fn get_drop_script(&self) -> String {
        format!(
            "drop {} if exists {}.{} ({});\n",
            self.kind.to_lowercase(),
            self.schema,
            self.name,
            self.arguments
        )
    }
}
