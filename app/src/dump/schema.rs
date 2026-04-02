use md5;
use serde::{Deserialize, Serialize};

// This is an information about a PostgreSQL schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    /// Schema name as returned by PostgreSQL's quote_ident() (optionally quoted)
    pub name: String,
    /// Raw schema name (without quotes)
    pub raw_name: String,
    /// Owner of the schema
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub owner: String,
    /// Optional comment on the schema
    #[serde(default)]
    pub comment: Option<String>,
    /// Hash of the schema
    pub hash: Option<String>,
    /// ACL (grant) entries for this schema
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub acl: Vec<String>,
}

fn escape_single_quotes(value: &str) -> String {
    value.replace('\'', "''")
}

impl Schema {
    /// Creates a new Schema with the given name
    pub fn new(name: String, raw_name: String, comment: Option<String>) -> Self {
        let mut schema = Self {
            name,
            raw_name,
            owner: String::new(),
            comment,
            hash: None,
            acl: Vec::new(),
        };
        schema.hash();
        schema
    }

    /// Computes schema hash.
    pub fn hash(&mut self) {
        let mut hasher = md5::Context::new();
        hasher.consume(self.name.as_bytes());
        hasher.consume(self.raw_name.as_bytes());
        hasher.consume(self.owner.as_bytes());
        if let Some(ref c) = self.comment {
            hasher.consume(c.as_bytes());
        }

        self.hash = Some(format!("{:x}", hasher.compute()));
    }

    /// Returns a string to create the schema.
    pub fn get_script(&self) -> String {
        let mut script = format!("create schema if not exists {};\n", self.name);

        if let Some(comment) = &self.comment {
            script.push_str(&format!(
                "comment on schema {} is '{}';\n",
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

        format!("alter schema {} owner to {};\n", self.name, self.owner)
    }

    pub fn get_alter_script(&self, target: &Schema) -> String {
        let mut script = String::new();

        if self.comment != target.comment {
            if let Some(comment) = &target.comment {
                script.push_str(&format!(
                    "comment on schema {} is '{}';\n",
                    target.name,
                    escape_single_quotes(comment)
                ));
            } else {
                script.push_str(&format!("comment on schema {} is null;\n", target.name));
            }
        }

        if self.owner != target.owner {
            script.push_str(&target.get_owner_script());
        }

        script
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
        let schema = Schema::new(name.clone(), name.clone(), None);

        assert_eq!(schema.name, name);
        assert_eq!(schema.raw_name, name);
        assert_eq!(schema.owner, "");
        assert!(schema.hash.is_some());

        let mut hasher = md5::Context::new();
        hasher.consume(name.as_bytes());
        hasher.consume(name.as_bytes());

        assert_eq!(schema.hash.unwrap(), format!("{:x}", hasher.compute()));
    }

    #[test]
    fn test_get_script_returns_create_statement() {
        let name: String = String::from("analytics");
        let raw_name: String = name.clone();

        let schema = Schema::new(name, raw_name, Some("reporting".to_string()));

        assert_eq!(
            schema.get_script(),
            "create schema if not exists analytics;\ncomment on schema analytics is 'reporting';\n"
        );
    }

    #[test]
    fn test_get_script_includes_owner_when_present() {
        let name: String = String::from("analytics");
        let raw_name: String = name.clone();

        let mut schema = Schema::new(name, raw_name, None);
        schema.owner = "pgc_owner".to_string();
        schema.hash();

        assert_eq!(
            schema.get_script(),
            "create schema if not exists analytics;\nalter schema analytics owner to pgc_owner;\n"
        );
    }

    #[test]
    fn test_get_drop_script_returns_drop_statement() {
        let name: String = String::from("archive");
        let raw_name: String = name.clone();

        let schema = Schema::new(name, raw_name, None);

        assert_eq!(schema.get_drop_script(), "drop schema if exists archive;\n");
    }

    #[test]
    fn test_get_script_quoted_name_no_comment() {
        let schema = Schema::new("\"my-schema\"".to_string(), "my-schema".to_string(), None);

        assert_eq!(
            schema.get_script(),
            "create schema if not exists \"my-schema\";\n"
        );
    }

    #[test]
    fn test_get_script_quoted_name_with_comment() {
        let schema = Schema::new(
            "\"my-schema\"".to_string(),
            "my-schema".to_string(),
            Some("my comment".to_string()),
        );

        assert_eq!(
            schema.get_script(),
            "create schema if not exists \"my-schema\";\ncomment on schema \"my-schema\" is 'my comment';\n"
        );
    }

    #[test]
    fn test_get_owner_script_quoted_name() {
        let mut schema = Schema::new("\"my-schema\"".to_string(), "my-schema".to_string(), None);
        schema.owner = "pgc_owner".to_string();

        assert_eq!(
            schema.get_owner_script(),
            "alter schema \"my-schema\" owner to pgc_owner;\n"
        );
    }

    #[test]
    fn test_get_alter_script_quoted_name_comment_changed() {
        let source = Schema::new(
            "\"my-schema\"".to_string(),
            "my-schema".to_string(),
            Some("old comment".to_string()),
        );
        let target = Schema::new(
            "\"my-schema\"".to_string(),
            "my-schema".to_string(),
            Some("new comment".to_string()),
        );

        assert_eq!(
            source.get_alter_script(&target),
            "comment on schema \"my-schema\" is 'new comment';\n"
        );
    }

    #[test]
    fn test_get_alter_script_quoted_name_comment_removed() {
        let source = Schema::new(
            "\"my-schema\"".to_string(),
            "my-schema".to_string(),
            Some("old comment".to_string()),
        );
        let target = Schema::new("\"my-schema\"".to_string(), "my-schema".to_string(), None);

        assert_eq!(
            source.get_alter_script(&target),
            "comment on schema \"my-schema\" is null;\n"
        );
    }

    #[test]
    fn test_get_alter_script_quoted_name_owner_changed() {
        let source = Schema::new("\"my-schema\"".to_string(), "my-schema".to_string(), None);
        let mut target = Schema::new("\"my-schema\"".to_string(), "my-schema".to_string(), None);
        target.owner = "new_owner".to_string();

        assert_eq!(
            source.get_alter_script(&target),
            "alter schema \"my-schema\" owner to new_owner;\n"
        );
    }

    #[test]
    fn test_get_drop_script_quoted_name() {
        let schema = Schema::new("\"my-schema\"".to_string(), "my-schema".to_string(), None);

        assert_eq!(
            schema.get_drop_script(),
            "drop schema if exists \"my-schema\";\n"
        );
    }
}
