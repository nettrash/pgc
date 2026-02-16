use md5;
use serde::{Deserialize, Serialize};

// This is an information about a PostgreSQL schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    /// Name of the schema
    pub name: String,
    /// Owner of the schema
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub owner: String,
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
        let mut schema = Self {
            name,
            owner: String::new(),
            comment,
            hash: None,
        };
        schema.hash();
        schema
    }

    /// Computes schema hash.
    pub fn hash(&mut self) {
        let mut hasher = md5::Context::new();
        hasher.consume(self.name.as_bytes());
        hasher.consume(self.owner.as_bytes());
        if let Some(ref c) = self.comment {
            hasher.consume(c.as_bytes());
        }

        self.hash = Some(format!("{:x}", hasher.compute()));
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

        script.push_str(&self.get_owner_script());

        script
    }

    pub fn get_owner_script(&self) -> String {
        if self.owner.is_empty() {
            return String::new();
        }

        format!(
            "alter schema \"{}\" owner to \"{}\";\n",
            self.name,
            self.owner.replace('"', "\"\"")
        )
    }

    pub fn get_alter_script(&self, target: &Schema) -> String {
        let mut script = String::new();

        if self.comment != target.comment {
            if let Some(comment) = &target.comment {
                script.push_str(&format!(
                    "comment on schema \"{}\" is '{}';\n",
                    target.name,
                    escape_single_quotes(comment)
                ));
            } else {
                script.push_str(&format!("comment on schema \"{}\" is null;\n", target.name));
            }
        }

        if self.owner != target.owner {
            script.push_str(&target.get_owner_script());
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
        assert_eq!(schema.owner, "");
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
    fn test_get_script_includes_owner_when_present() {
        let mut schema = Schema::new("analytics".to_string(), None);
        schema.owner = "pgc_owner".to_string();
        schema.hash();

        assert_eq!(
            schema.get_script(),
            "create schema if not exists \"analytics\";\nalter schema \"analytics\" owner to \"pgc_owner\";\n"
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
