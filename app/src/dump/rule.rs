use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::utils::string_extensions::StringExt;

/// Represents a PostgreSQL rule (from pg_rewrite).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Rule {
    pub schema: String,
    pub table_name: String,
    pub rule_name: String,
    pub definition: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    pub hash: Option<String>,
}

impl Rule {
    pub fn new(
        schema: String,
        table_name: String,
        rule_name: String,
        definition: String,
        comment: Option<String>,
    ) -> Self {
        let mut rule = Rule {
            schema,
            table_name,
            rule_name,
            definition,
            comment,
            hash: None,
        };
        rule.hash();
        rule
    }

    pub fn hash(&mut self) {
        let mut hasher = Sha256::new();
        hasher.update(self.schema.as_bytes());
        hasher.update(self.table_name.as_bytes());
        hasher.update(self.rule_name.as_bytes());
        hasher.update(self.definition.as_bytes());
        if let Some(comment) = &self.comment {
            hasher.update((comment.len() as u32).to_be_bytes());
            hasher.update(comment.as_bytes());
        }
        self.hash = Some(format!("{:x}", hasher.finalize()));
    }

    pub fn get_script(&self) -> String {
        let mut script = format!("{};", self.definition).with_empty_lines();
        if let Some(comment) = &self.comment {
            script.append_block(&format!(
                "comment on rule \"{}\" on {}.\"{}\" is '{}';",
                self.rule_name.replace('"', "\"\""),
                self.schema,
                self.table_name.replace('"', "\"\""),
                comment.replace('\'', "''")
            ));
        }
        script
    }

    pub fn get_drop_script(&self) -> String {
        format!(
            "drop rule if exists \"{}\" on {}.\"{}\" cascade;",
            self.rule_name.replace('"', "\"\""),
            self.schema,
            self.table_name.replace('"', "\"\"")
        )
        .with_empty_lines()
    }

    pub fn get_alter_script(&self, target: &Rule, use_drop: bool) -> String {
        let mut script = String::new();

        if self.definition != target.definition {
            // Rules can be replaced with CREATE OR REPLACE RULE
            script.append_block(&format!("{};", target.definition));
        }

        if self.comment != target.comment {
            if let Some(cmt) = &target.comment {
                script.append_block(&format!(
                    "comment on rule \"{}\" on {}.\"{}\" is '{}';",
                    target.rule_name.replace('"', "\"\""),
                    target.schema,
                    target.table_name.replace('"', "\"\""),
                    cmt.replace('\'', "''")
                ));
            } else if use_drop {
                script.append_block(&format!(
                    "comment on rule \"{}\" on {}.\"{}\" is null;",
                    target.rule_name.replace('"', "\"\""),
                    target.schema,
                    target.table_name.replace('"', "\"\"")
                ));
            }
        }

        script
    }
}

#[cfg(test)]
#[path = "rule_tests.rs"]
mod tests;
