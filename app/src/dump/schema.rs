use serde::{Deserialize, Serialize};

// This is an information about a PostgreSQL schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    /// Name of the schema
    pub name: String,
    /// Hash of the schema
    pub hash: Option<String>,
}

impl Schema {
    /// Creates a new Schema with the given name
    pub fn new(name: String) -> Self {
        Self {
            name: name.clone(),
            hash: Some(format!("{:x}", md5::compute(&name))),
        }
    }

    /// Returns a string to create the schema.
    pub fn get_script(&self) -> String {
        format!("create schema if not exists {};\n", self.name)
    }

    /// Returns a string to drop the schema.
    pub fn get_drop_script(&self) -> String {
        format!("drop schema if exists {};\n", self.name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_new_sets_fields() {
        let name = "public".to_string();
        let schema = Schema::new(name.clone());

        assert_eq!(schema.name, name);
        assert!(schema.hash.is_some());
        assert_eq!(schema.hash.unwrap(), format!("{:x}", md5::compute(name)));
    }

    #[test]
    fn test_get_script_returns_create_statement() {
        let schema = Schema::new("analytics".to_string());

        assert_eq!(
            schema.get_script(),
            "create schema if not exists analytics;\n"
        );
    }

    #[test]
    fn test_get_drop_script_returns_drop_statement() {
        let schema = Schema::new("archive".to_string());

        assert_eq!(schema.get_drop_script(), "drop schema if exists archive;\n");
    }
}
