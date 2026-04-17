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
mod tests {
    use super::*;

    #[test]
    fn test_rule_hash() {
        let mut r = Rule::new(
            "public".into(),
            "orders".into(),
            "protect_delete".into(),
            "CREATE OR REPLACE RULE protect_delete AS ON DELETE TO public.orders DO INSTEAD NOTHING".into(),
            None,
        );
        assert!(r.hash.is_some());
        let h1 = r.hash.clone();
        r.definition =
            "CREATE OR REPLACE RULE protect_delete AS ON DELETE TO public.orders DO ALSO NOTHING"
                .into();
        r.hash();
        assert_ne!(h1, r.hash);
    }

    #[test]
    fn test_get_script() {
        let r = Rule::new(
            "public".into(),
            "orders".into(),
            "protect_delete".into(),
            "CREATE OR REPLACE RULE protect_delete AS ON DELETE TO public.orders DO INSTEAD NOTHING".into(),
            Some("Prevent deletes".into()),
        );
        let script = r.get_script();
        assert!(script.contains("CREATE OR REPLACE RULE"));
        assert!(script.contains("comment on rule"));
        assert!(script.contains("Prevent deletes"));
    }

    #[test]
    fn test_get_drop_script() {
        let r = Rule::new(
            "public".into(),
            "orders".into(),
            "protect_delete".into(),
            "CREATE OR REPLACE RULE protect_delete AS ON DELETE TO public.orders DO INSTEAD NOTHING".into(),
            None,
        );
        let script = r.get_drop_script();
        assert!(script.contains("drop rule if exists"));
        assert!(script.contains("cascade"));
    }

    #[test]
    fn test_get_alter_script_definition_change() {
        let from = Rule::new(
            "public".into(),
            "orders".into(),
            "protect_delete".into(),
            "CREATE OR REPLACE RULE protect_delete AS ON DELETE TO public.orders DO INSTEAD NOTHING".into(),
            None,
        );
        let to = Rule::new(
            "public".into(),
            "orders".into(),
            "protect_delete".into(),
            "CREATE OR REPLACE RULE protect_delete AS ON DELETE TO public.orders DO ALSO NOTHING"
                .into(),
            None,
        );
        let script = from.get_alter_script(&to, true);
        assert!(script.contains("DO ALSO NOTHING"));
    }
}
