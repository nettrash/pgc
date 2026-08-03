//! Indexes (`pg_index`), carried as their `pg_get_indexdef` text.
//!
//! Indexes backing a constraint are owned by that constraint and must not be
//! emitted independently. Under `--output-for-production` index builds and drops
//! become `CONCURRENTLY`, which cannot run inside a transaction and so moves to
//! the post-commit section.

use std::borrow::Cow;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::utils::string_extensions::StringExt;

/// This is an information about a PostgreSQL table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableIndex {
    /// Schema name
    pub schema: String,
    /// Table name
    pub table: String,
    /// Index name
    pub name: String,
    /// Catalog name
    pub catalog: Option<String>,
    /// Index definition
    pub indexdef: String,
    /// Whether this index is inherited from a partitioned parent
    #[serde(default)]
    pub is_partition_index: bool,
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
/// index — the common case) is borrowed unchanged, so only the rare partitioned-parent
/// rewrite allocates. The production path builds its own `ON ONLY` form with the
/// concurrent per-partition attach sequence (see `comparer::production`) and does not
/// go through here.
fn strip_on_only(indexdef: &str) -> Cow<'_, str> {
    let Some(pos) = indexdef.find(" ON ") else {
        return Cow::Borrowed(indexdef);
    };
    let after = &indexdef[pos + " ON ".len()..];
    match after.strip_prefix("ONLY ") {
        Some(rest) => Cow::Owned(format!("{} ON {}", &indexdef[..pos], rest)),
        None => Cow::Borrowed(indexdef),
    }
}

/// Whether two `CREATE INDEX` definitions describe the same index, ignoring the
/// non-idempotent ways PostgreSQL deparses an `IN`-list partial-index predicate
/// (issue #226). Falls back to a canonicalized comparison of the full statement.
pub(crate) fn indexdefs_equivalent(a: &str, b: &str) -> bool {
    a == b
        || crate::utils::sql_normalize::canonicalize_definition(a)
            == crate::utils::sql_normalize::canonicalize_definition(b)
}

impl TableIndex {
    /// Hash
    pub fn add_to_hasher(&self, hasher: &mut Sha256) {
        hasher.update(self.schema.as_bytes());
        hasher.update(self.table.as_bytes());
        hasher.update(self.name.as_bytes());
        // Canonicalize the definition (a partial-index predicate may carry a
        // non-idempotent IN-list expression) so the index does not look changed on
        // every run (issue #226).
        hasher.update(
            crate::utils::sql_normalize::canonicalize_definition(&self.indexdef).as_bytes(),
        );
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
            && indexdefs_equivalent(&self.indexdef, &other.indexdef)
            && self.comment == other.comment
    }
}

#[cfg(test)]
#[path = "tests/table_index.rs"]
mod tests;
