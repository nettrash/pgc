use serde::{Deserialize, Serialize};

// This is an information about a PostgreSQL extension.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    /// Name of the extension
    pub name: String,
}

impl Schema {
    /// Creates a new Schema with the given name
    pub fn new(name: String) -> Self {
        Self { name }
    }

    /// Hash
    pub fn hash(&self) -> String {
        format!("{:x}", md5::compute(&self.name))
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
    fn test_schema_new() {
        let s = Schema::new("public".to_string());
        assert_eq!(s.name, "public");
    }

    #[test]
    fn test_schema_hash_consistency() {
        let s1 = Schema::new("app".to_string());
        let s2 = Schema::new("app".to_string());
        assert_eq!(s1.hash(), s2.hash());
    }

    #[test]
    fn test_schema_hash_differs() {
        let s1 = Schema::new("a".to_string());
        let s2 = Schema::new("b".to_string());
        assert_ne!(s1.hash(), s2.hash());
    }

    #[test]
    fn test_get_script() {
        let s = Schema::new("custom".to_string());
        assert_eq!(s.get_script(), "create schema if not exists custom;\n");
    }

    #[test]
    fn test_get_drop_script() {
        let s = Schema::new("public".to_string());
        assert_eq!(s.get_drop_script(), "drop schema if exists public;\n");
    }
}
