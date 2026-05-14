use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::postgres::types::Oid;

use crate::utils::string_extensions::StringExt;

// This is an information about a PostgreSQL table trigger.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableTrigger {
    pub oid: Oid,           // Object identifier of the trigger
    pub name: String,       // Name of the trigger
    pub definition: String, // Definition of the trigger
    /// Trigger enabled state from pg_trigger.tgenabled:
    /// 'O' = fires in "origin" and "local" modes (the default),
    /// 'D' = disabled,
    /// 'R' = fires in "replica" mode only,
    /// 'A' = fires always.
    #[serde(default = "TableTrigger::default_enabled")]
    pub enabled: String,
    /// Optional comment on the trigger
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
}

impl TableTrigger {
    fn default_enabled() -> String {
        "O".to_string()
    }

    /// Hash
    pub fn add_to_hasher(&self, hasher: &mut Sha256) {
        hasher.update(self.name.as_bytes());
        hasher.update(self.definition.as_bytes());
        hasher.update(self.enabled.as_bytes());
        if let Some(ref comment) = self.comment {
            hasher.update((comment.len() as u64).to_le_bytes());
            hasher.update(comment.as_bytes());
        }
    }

    /// Returns a string representation of the trigger
    pub fn get_script(&self, schema: &str, table: &str) -> String {
        let mut script = String::new();
        script.push_str(&self.definition);
        script.append_block(";");
        // Emit enable/disable state if not the default 'O'
        match self.enabled.as_str() {
            "D" => {
                script.append_block(&format!(
                    "alter table {schema}.{table} disable trigger {};",
                    self.name
                ));
            }
            "R" => {
                script.append_block(&format!(
                    "alter table {schema}.{table} enable replica trigger {};",
                    self.name
                ));
            }
            "A" => {
                script.append_block(&format!(
                    "alter table {schema}.{table} enable always trigger {};",
                    self.name
                ));
            }
            _ => {} // 'O' is the default, no extra statement needed
        }
        // Emit comment if present
        if let Some(ref comment) = self.comment {
            script.append_block(&format!(
                "comment on trigger {} on {schema}.{table} is '{}';",
                self.name,
                comment.replace('\'', "''")
            ));
        }
        script
    }

    /// Returns ALTER statements for changes between two trigger versions
    pub fn get_alter_script(
        &self,
        target: &TableTrigger,
        schema: &str,
        table: &str,
        use_drop: bool,
    ) -> String {
        let mut script = String::new();

        // If the definition changed, drop and recreate
        if self.definition != target.definition {
            let drop_cmd = format!("drop trigger if exists {} on {schema}.{table};", self.name)
                .with_empty_lines();
            if use_drop {
                script.push_str(&drop_cmd);
            } else {
                script.push_str(&format!("-- {}", drop_cmd));
            }
            script.push_str(&target.get_script(schema, table));
            return script;
        }

        // Handle enabled state change
        if self.enabled != target.enabled {
            let stmt = match target.enabled.as_str() {
                "D" => format!(
                    "alter table {schema}.{table} disable trigger {};",
                    target.name
                ),
                "R" => format!(
                    "alter table {schema}.{table} enable replica trigger {};",
                    target.name
                ),
                "A" => format!(
                    "alter table {schema}.{table} enable always trigger {};",
                    target.name
                ),
                _ => format!(
                    "alter table {schema}.{table} enable trigger {};",
                    target.name
                ),
            };
            script.append_block(&stmt);
        }

        // Handle comment change
        if self.comment != target.comment {
            if let Some(ref comment) = target.comment {
                script.append_block(&format!(
                    "comment on trigger {} on {schema}.{table} is '{}';",
                    target.name,
                    comment.replace('\'', "''")
                ));
            } else {
                script.append_block(&format!(
                    "comment on trigger {} on {schema}.{table} is null;",
                    target.name
                ));
            }
        }

        script
    }
}

impl PartialEq for TableTrigger {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.definition == other.definition
            && self.enabled == other.enabled
            && self.comment == other.comment
    }
}

#[cfg(test)]
#[path = "table_trigger_tests.rs"]
mod tests;
