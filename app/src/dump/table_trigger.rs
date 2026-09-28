//! Triggers (`pg_trigger`) — `CREATE TRIGGER`.
//!
//! A trigger needs both its table and its trigger function, so creation is
//! deferred to the comparer's `trigger_post_script` buffer, which is emitted last.
//! Constraint triggers backing a foreign key are excluded. A trigger an unchanged
//! definition keeps, but a routine's `DROP … CASCADE` takes along, is put back
//! right after the routines ([`TableTrigger::get_recreate_script`]).
//!
//! A partitioned table's row trigger is cloned onto every partition. The
//! clones belong to it: PostgreSQL creates and drops them with it, so they
//! are not a partition's own triggers, and only one whose enabled state or
//! comment is its own is dumped, apart — every one, on a partition whose
//! parent the dump does not have (see
//! [`Table::trigger_clones`](crate::dump::table::Table::trigger_clones)).

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::postgres::types::Oid;

use crate::utils::string_extensions::{StringExt, comment_out_keep_blank_lines};

/// This is an information about a PostgreSQL table trigger.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableTrigger {
    /// Object identifier of the trigger
    pub oid: Oid,
    /// Name of the trigger
    pub name: String,
    /// Definition of the trigger
    pub definition: String,
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

    /// `drop trigger if exists` followed by [`TableTrigger::get_script`]: how
    /// the comparer puts back a trigger that a routine's `DROP … CASCADE`
    /// took with it (the routine is its function, or its `WHEN` clause calls
    /// it). The drop is guarded because such a trigger is found by reading the
    /// routine's name in its definition, which cannot tell overloads apart.
    pub fn get_recreate_script(&self, schema: &str, table: &str) -> String {
        let mut script =
            format!("drop trigger if exists {} on {schema}.{table};", self.name).with_empty_lines();
        script.push_str(&self.get_script(schema, table));
        script
    }

    /// `alter table … enable|disable … trigger …;` setting this trigger's
    /// enabled state. Without `ONLY`, so on a partitioned table it sets the
    /// state of every partition's clone as well.
    pub(crate) fn enabled_state_statement(&self, schema: &str, table: &str) -> String {
        match self.enabled.as_str() {
            "D" => format!(
                "alter table {schema}.{table} disable trigger {};",
                self.name
            ),
            "R" => format!(
                "alter table {schema}.{table} enable replica trigger {};",
                self.name
            ),
            "A" => format!(
                "alter table {schema}.{table} enable always trigger {};",
                self.name
            ),
            _ => format!("alter table {schema}.{table} enable trigger {};", self.name),
        }
    }

    /// What this trigger, a partition's clone of its parent's trigger of the
    /// same name, needs once the comparer has created the parent's trigger
    /// again: PostgreSQL clones it onto every partition in the parent's
    /// enabled state `parent_enabled` (setting the parent's state recurses
    /// to the partitions) and with no comment. So the clone's own enabled
    /// state when it differs, and its comment when it has one; empty when it
    /// needs neither.
    pub fn get_clone_state_script(
        &self,
        schema: &str,
        table: &str,
        parent_enabled: &str,
    ) -> String {
        let mut script = String::new();
        if self.enabled != parent_enabled {
            script.append_block(&self.enabled_state_statement(schema, table));
        }
        if let Some(ref comment) = self.comment {
            script.append_block(&format!(
                "comment on trigger {} on {schema}.{table} is '{}';",
                self.name,
                comment.replace('\'', "''")
            ));
        }
        script
    }

    /// Whether the definition fires `FOR EACH ROW` rather than `FOR EACH
    /// STATEMENT` — a row-level trigger, the kind PostgreSQL clones onto the
    /// partitions of a partitioned table. `pg_get_triggerdef` spells the
    /// level once, ahead of any `WHEN` clause and the function's arguments;
    /// the first such clause outside a quoted identifier or a string literal
    /// decides, so neither a trigger or table name nor an argument spelling
    /// the words counts.
    pub fn is_row_level(&self) -> bool {
        const ROW: &[u8] = b" FOR EACH ROW";
        const STATEMENT: &[u8] = b" FOR EACH STATEMENT";
        let src = self.definition.as_bytes();
        let ends_word = |at: usize| src.get(at).is_none_or(|b| !b.is_ascii_alphanumeric());
        let mut i = 0;
        while i < src.len() {
            match src[i] {
                quote @ (b'\'' | b'"') => {
                    // Skip to the closing quote; a doubled quote is an escape
                    // and keeps the literal or identifier open.
                    i += 1;
                    while i < src.len() {
                        if src[i] == quote {
                            if src.get(i + 1) == Some(&quote) {
                                i += 2;
                                continue;
                            }
                            break;
                        }
                        i += 1;
                    }
                    i += 1;
                }
                _ => {
                    let rest = &src[i..];
                    if rest.len() >= ROW.len()
                        && rest[..ROW.len()].eq_ignore_ascii_case(ROW)
                        && ends_word(i + ROW.len())
                    {
                        return true;
                    }
                    if rest.len() >= STATEMENT.len()
                        && rest[..STATEMENT.len()].eq_ignore_ascii_case(STATEMENT)
                        && ends_word(i + STATEMENT.len())
                    {
                        return false;
                    }
                    i += 1;
                }
            }
        }
        false
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
                script.push_str(&comment_out_keep_blank_lines(&drop_cmd));
            }
            script.push_str(&target.get_script(schema, table));
            return script;
        }

        // Handle enabled state change
        if self.enabled != target.enabled {
            script.append_block(&target.enabled_state_statement(schema, table));
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
#[path = "tests/table_trigger.rs"]
mod tests;
