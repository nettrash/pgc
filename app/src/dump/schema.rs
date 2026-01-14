use md5;
use serde::{Deserialize, Serialize};

// This is an information about a PostgreSQL schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    /// Name of the schema
    pub name: String,
    /// Optional comment on the schema
    #[serde(default)]
    pub comment: Option<String>,
    /// Hash of the schema
    pub hash: Option<String>,
}

fn escape_single_quotes(value: &str) -> String {
    value.replace('\'', "''")
}

impl Schema {
    /// Creates a new Schema with the given name
    pub fn new(name: String, comment: Option<String>) -> Self {
        let mut hasher = md5::Context::new();
        hasher.consume(name.as_bytes());
        if let Some(ref c) = comment {
            hasher.consume(c.as_bytes());
        }

        let hash = Some(format!("{:x}", hasher.compute()));
        Self {
            name,
            comment,
            hash,
        }
    }

    /// Returns a string to create the schema.
    pub fn get_script(&self) -> String {
        let mut script = format!("create schema if not exists \"{}\";\n", self.name);

        if let Some(comment) = &self.comment {
            script.push_str(&format!(
                "comment on schema \"{}\" is '{}';\n",
                self.name,
                escape_single_quotes(comment)
            ));
        }

        script
    }

    /// Returns a string to drop the schema.
    pub fn get_drop_script(&self) -> String {
        format!("drop schema if exists \"{}\";\n", self.name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_new_sets_fields() {
        let name = "public".to_string();
        let schema = Schema::new(name.clone(), None);

        assert_eq!(schema.name, name);
        assert!(schema.hash.is_some());
        assert_eq!(
            schema.hash.unwrap(),
            format!("{:x}", md5::compute(name.as_bytes()))
        );
    }

    #[test]
    fn test_get_script_returns_create_statement() {
        let schema = Schema::new("analytics".to_string(), Some("reporting".to_string()));

        assert_eq!(
            schema.get_script(),
            "create schema if not exists \"analytics\";\ncomment on schema \"analytics\" is 'reporting';\n"
        );
    }

    #[test]
    fn test_get_drop_script_returns_drop_statement() {
        let schema = Schema::new("archive".to_string(), None);

        assert_eq!(
            schema.get_drop_script(),
            "drop schema if exists \"archive\";\n"
        );
    }
}
