use md5;
use serde::{Deserialize, Serialize};

use crate::utils::string_extensions::StringExt;

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
        let mut script: String =
            format!("create schema if not exists {};", self.name).with_empty_lines();

        if let Some(comment) = &self.comment {
            script.append_block(&format!(
                "comment on schema {} is '{}';",
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

        format!("alter schema {} owner to {};", self.name, self.owner).with_empty_lines()
    }

    pub fn get_alter_script(&self, target: &Schema) -> String {
        let mut script = String::new();

        if self.comment != target.comment {
            if let Some(comment) = &target.comment {
                script.append_block(&format!(
                    "comment on schema {} is '{}';",
                    target.name,
                    escape_single_quotes(comment)
                ));
            } else {
                script.append_block(&format!("comment on schema {} is null;", target.name));
            }
        }

        if self.owner != target.owner {
            script.push_str(&target.get_owner_script());
        }

        script
    }

    /// Returns a string to drop the schema.
    pub fn get_drop_script(&self) -> String {
        format!("drop schema if exists {};", self.name).with_empty_lines()
    }
}

#[cfg(test)]
#[path = "schema_tests.rs"]
mod tests;
