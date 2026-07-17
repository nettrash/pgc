use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::utils::string_extensions::StringExt;

// This is an information about a PostgreSQL table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableIndex {
    pub schema: String,          // Schema name
    pub table: String,           // Table name
    pub name: String,            // Index name
    pub catalog: Option<String>, // Catalog name
    pub indexdef: String,        // Index definition
    #[serde(default)]
    pub is_partition_index: bool, // Whether this index is inherited from a partitioned parent
    /// Optional comment on the index
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
}

/// Drop the `ONLY` from an index definition's `ON ONLY <table>` target.
///
/// `pg_get_indexdef` renders a partitioned parent's index with `ON ONLY`, which
/// creates only the metadata index on the parent and leaves it `indisvalid = false`
/// until every partition's index is built and attached. The default (non-production)
/// output has no attach step, so it must emit a plain `CREATE INDEX ... ON <table>`:
/// PostgreSQL then builds and attaches the partition indexes itself and the parent
/// index is valid immediately. A definition without `ON ONLY` (every non-partitioned
/// index) is returned unchanged. The production path builds its own `ON ONLY` form
/// with the concurrent per-partition attach sequence (see `comparer::production`) and
/// does not go through here.
fn strip_on_only(indexdef: &str) -> String {
    let Some(pos) = indexdef.find(" ON ") else {
        return indexdef.to_string();
    };
    let after = &indexdef[pos + " ON ".len()..];
    match after.strip_prefix("ONLY ") {
        Some(rest) => format!("{} ON {}", &indexdef[..pos], rest),
        None => indexdef.to_string(),
    }
}

impl TableIndex {
    /// Hash
    pub fn add_to_hasher(&self, hasher: &mut Sha256) {
        hasher.update(self.schema.as_bytes());
        hasher.update(self.table.as_bytes());
        hasher.update(self.name.as_bytes());
        hasher.update(self.indexdef.as_bytes());
        if let Some(comment) = &self.comment {
            hasher.update((comment.len() as u32).to_be_bytes());
            hasher.update(comment.as_bytes());
        }
    }

    /// Returns a string representation of the index
    pub fn get_script(&self) -> String {
        let mut script = String::new();
        script.push_str(&strip_on_only(&self.indexdef));
        script.append_block(";");
        if let Some(comment) = &self.comment {
            script.append_block(&format!(
                "comment on index {}.{} is '{}';",
                self.schema,
                self.name,
                comment.replace('\'', "''")
            ));
        }
        script
    }
}

impl PartialEq for TableIndex {
    fn eq(&self, other: &Self) -> bool {
        self.schema == other.schema
            && self.table == other.table
            && self.name == other.name
            && self.catalog == other.catalog
            && self.indexdef == other.indexdef
            && self.comment == other.comment
    }
}

#[cfg(test)]
#[path = "table_index_tests.rs"]
mod tests;
