use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::utils::string_extensions::StringExt;

fn escape_single_quotes(value: &str) -> String {
    value.replace('\'', "''")
}

/// Represents a PostgreSQL event trigger (from pg_event_trigger).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventTrigger {
    pub name: String,
    pub event: String, // e.g. "ddl_command_start", "ddl_command_end", "sql_drop", "table_rewrite"
    pub function_name: String, // Fully-qualified function name
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>, // Filter tags (e.g. "CREATE TABLE", "DROP TABLE")
    pub enabled: String, // O=origin/local, D=disabled, R=replica, A=always
    pub owner: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    pub hash: Option<String>,
}

impl EventTrigger {
    pub fn new(
        name: String,
        event: String,
        function_name: String,
        tags: Vec<String>,
        enabled: String,
        owner: String,
        comment: Option<String>,
    ) -> Self {
        let mut et = EventTrigger {
            name,
            event,
            function_name,
            tags,
            enabled,
            owner,
            comment,
            hash: None,
        };
        et.hash();
        et
    }

    pub fn hash(&mut self) {
        let mut hasher = Sha256::new();
        hasher.update(self.name.as_bytes());
        hasher.update(self.event.as_bytes());
        hasher.update(self.function_name.as_bytes());
        hasher.update((self.tags.len() as u32).to_be_bytes());
        for tag in &self.tags {
            hasher.update((tag.len() as u32).to_be_bytes());
            hasher.update(tag.as_bytes());
        }
        hasher.update(self.enabled.as_bytes());
        hasher.update(self.owner.as_bytes());
        if let Some(comment) = &self.comment {
            hasher.update((comment.len() as u32).to_be_bytes());
            hasher.update(comment.as_bytes());
        }
        self.hash = Some(format!("{:x}", hasher.finalize()));
    }

    pub fn get_script(&self) -> String {
        let mut script = format!(
            "create event trigger \"{}\" on {}",
            self.name.replace('"', "\"\""),
            self.event
        );

        if !self.tags.is_empty() {
            let tags_str = self
                .tags
                .iter()
                .map(|t| format!("'{}'", escape_single_quotes(t)))
                .collect::<Vec<_>>()
                .join(", ");
            script.push_str(&format!("\n    when tag in ({})", tags_str));
        }

        script.push_str(&format!("\n    execute function {}();", self.function_name));
        let mut result = script.with_empty_lines();

        if self.enabled != "O" {
            let state = match self.enabled.as_str() {
                "D" => "disable",
                "R" => "enable replica",
                "A" => "enable always",
                _ => "enable",
            };
            result.append_block(&format!(
                "alter event trigger \"{}\" {};",
                self.name.replace('"', "\"\""),
                state
            ));
        }

        if !self.owner.is_empty() {
            result.append_block(&format!(
                "alter event trigger \"{}\" owner to {};",
                self.name.replace('"', "\"\""),
                self.owner
            ));
        }

        if let Some(comment) = &self.comment {
            result.append_block(&format!(
                "comment on event trigger \"{}\" is '{}';",
                self.name.replace('"', "\"\""),
                escape_single_quotes(comment)
            ));
        }

        result
    }

    pub fn get_drop_script(&self) -> String {
        format!(
            "drop event trigger if exists \"{}\";",
            self.name.replace('"', "\"\"")
        )
        .with_empty_lines()
    }

    pub fn get_alter_script(&self, target: &EventTrigger, use_drop: bool) -> String {
        let mut script = String::new();

        // If event/function/tags changed, we must drop and recreate
        if self.event != target.event
            || self.function_name != target.function_name
            || self.tags != target.tags
        {
            if use_drop {
                script.push_str(&self.get_drop_script());
                script.push_str(&target.get_script());
            } else {
                script.push_str(&format!(
                    "-- Event trigger \"{}\" requires drop+recreate (definition changed); use_drop=false\n",
                    self.name
                ));
            }
            return script;
        }

        if self.enabled != target.enabled {
            let state = match target.enabled.as_str() {
                "D" => "disable",
                "R" => "enable replica",
                "A" => "enable always",
                _ => "enable",
            };
            script.append_block(&format!(
                "alter event trigger \"{}\" {};",
                target.name.replace('"', "\"\""),
                state
            ));
        }

        if self.owner != target.owner && !target.owner.is_empty() {
            script.append_block(&format!(
                "alter event trigger \"{}\" owner to {};",
                target.name.replace('"', "\"\""),
                target.owner
            ));
        }

        if self.comment != target.comment {
            if let Some(cmt) = &target.comment {
                script.append_block(&format!(
                    "comment on event trigger \"{}\" is '{}';",
                    target.name.replace('"', "\"\""),
                    escape_single_quotes(cmt)
                ));
            } else if use_drop {
                script.append_block(&format!(
                    "comment on event trigger \"{}\" is null;",
                    target.name.replace('"', "\"\"")
                ));
            }
        }

        script
    }
}

#[cfg(test)]
#[path = "event_trigger_tests.rs"]
mod tests;
