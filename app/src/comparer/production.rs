//! Production-mode SQL rewriting for the `output_for_production` flag.
//!
//! When the flag is enabled the comparer routes index and foreign-key DDL
//! through these helpers so the resulting migration is convenient to apply to a
//! live database:
//!
//!   * indexes are built with `CREATE INDEX CONCURRENTLY` (no long write lock);
//!   * indexes on partitioned parents use the only-safe pattern —
//!     `CREATE INDEX ON ONLY parent`, then `CREATE INDEX CONCURRENTLY` on each
//!     partition, then `ALTER INDEX ... ATTACH PARTITION` — because
//!     `CONCURRENTLY` is rejected on a partitioned table directly;
//!   * foreign keys are added `NOT VALID` and validated in a separate step
//!     (`VALIDATE CONSTRAINT`) so the long validation scan does not hold a
//!     table lock for the whole migration;
//!   * indexes are dropped with `DROP INDEX CONCURRENTLY`.
//!
//! `CONCURRENTLY`, `VALIDATE CONSTRAINT` and `ATTACH PARTITION` cannot run
//! inside a transaction block, so every statement that must run after the main
//! transaction commits is collected in the `post_commit` half of a
//! [`ProdSplit`]. The comparer appends that buffer after `commit;`.

use std::collections::{HashMap, HashSet};

use crate::dump::table_constraint::TableConstraint;
use crate::dump::table_index::TableIndex;
use crate::utils::string_extensions::StringExt;

/// A pair of script fragments produced by a production rewrite: statements that
/// stay inside the migration transaction, and statements that must run after it
/// commits (each in its own autocommit statement).
#[derive(Debug, Default, PartialEq, Eq)]
pub struct ProdSplit {
    pub in_txn: String,
    pub post_commit: String,
}

/// Direct child of a partitioned parent table. Identifiers are stored in the
/// same `quote_ident`-applied form as [`TableIndex::schema`] / [`TableIndex::table`].
#[derive(Debug, Clone)]
pub struct ChildRef {
    pub schema: String,
    pub table: String,
}

/// Partition topology the index rewriters need. All qualified names use the
/// `quote_ident(schema).quote_ident(name)` form already produced by the dump.
pub struct PartitionContext<'a> {
    /// Qualified names of partitioned parent tables (relkind `p`).
    pub partitioned_parents: &'a HashSet<String>,
    /// Parent qualified name -> its direct partitions.
    pub children: &'a HashMap<String, Vec<ChildRef>>,
    /// Qualified index names (`schema.name`) whose underlying table is a
    /// partitioned parent. `DROP INDEX CONCURRENTLY` is illegal for these.
    pub partitioned_indexes: &'a HashSet<String>,
}

/// Insert `CONCURRENTLY` after the `CREATE [UNIQUE] INDEX` keyword.
/// `pg_get_indexdef` always emits uppercase keywords, so a literal prefix match
/// suffices; an unrecognised statement is returned unchanged.
fn insert_concurrently(indexdef: &str) -> String {
    if let Some(rest) = indexdef.strip_prefix("CREATE UNIQUE INDEX ") {
        return format!("CREATE UNIQUE INDEX CONCURRENTLY {rest}");
    }
    if let Some(rest) = indexdef.strip_prefix("CREATE INDEX ") {
        return format!("CREATE INDEX CONCURRENTLY {rest}");
    }
    indexdef.to_string()
}

/// Insert `ONLY` after the `ON` of an index definition so the index is created
/// on the partitioned parent alone (invalid until every partition's index is
/// attached). Returns `None` if the statement has no ` ON ` token.
///
/// `pg_get_indexdef` already emits `ON ONLY` for an index on a partitioned
/// parent, so the rewrite is idempotent: a definition that is already
/// `ON ONLY` is returned unchanged rather than producing `ON ONLY ONLY`.
fn make_on_only(indexdef: &str) -> Option<String> {
    let pos = indexdef.find(" ON ")?;
    let rest = &indexdef[pos + " ON ".len()..];
    if rest.starts_with("ONLY ") {
        return Some(indexdef.to_string());
    }
    let mut out = String::with_capacity(indexdef.len() + 5);
    out.push_str(&indexdef[..pos]);
    out.push_str(" ON ONLY ");
    out.push_str(rest);
    Some(out)
}

/// Split an index definition into its `CREATE [UNIQUE] INDEX` keyword and the
/// `USING ...` tail (access method, columns, predicate). Returns `None` when
/// the statement does not have the expected shape.
fn create_kw_and_tail(indexdef: &str) -> Option<(&'static str, &str)> {
    let create_kw = if indexdef.starts_with("CREATE UNIQUE INDEX ") {
        "CREATE UNIQUE INDEX"
    } else if indexdef.starts_with("CREATE INDEX ") {
        "CREATE INDEX"
    } else {
        return None;
    };
    let using = indexdef.find(" USING ")?;
    Some((create_kw, indexdef[using + 1..].trim_end()))
}

fn unquote_ident(s: &str) -> String {
    let t = s.trim();
    if t.len() >= 2 && t.starts_with('"') && t.ends_with('"') {
        t[1..t.len() - 1].replace("\"\"", "\"")
    } else {
        t.to_string()
    }
}

fn quote_ident(s: &str) -> String {
    let needs_quote = s.is_empty()
        || s.chars()
            .next()
            .map(|c| !(c.is_ascii_lowercase() || c == '_'))
            == Some(true)
        || s.chars()
            .any(|c| !(c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_'));
    if needs_quote {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

fn comment_on_index(index: &TableIndex, comment: &str) -> String {
    format!(
        "comment on index {}.{} is '{}';",
        index.schema,
        index.name,
        comment.replace('\'', "''")
    )
}

/// Derive a unique per-partition index name from the parent index and the
/// partition table name, e.g. parent index `idx_orders_total` on partition
/// `orders_2024` -> `orders_2024_idx_orders_total`. PostgreSQL truncates names
/// to 63 bytes; on the rare collision the migration would error rather than do
/// the wrong thing.
fn child_index_name(parent_index: &TableIndex, child: &ChildRef) -> String {
    quote_ident(&format!(
        "{}_{}",
        unquote_ident(&child.table),
        unquote_ident(&parent_index.name)
    ))
}

/// Production rewrite of a single index *creation*.
///
/// `parent_is_new` is true when the index's table is being created from scratch
/// in this same migration (the new-table path). For a brand-new *partitioned*
/// parent every partition is also new and empty: each is created later via
/// `CREATE TABLE ... PARTITION OF`, at which point PostgreSQL automatically
/// creates and attaches the matching partition index. Running the manual
/// per-partition `CREATE INDEX CONCURRENTLY ... ; ALTER INDEX ... ATTACH
/// PARTITION` dance on top of that collides with the auto-attached index
/// (`another index is already attached for partition ...`), so a new
/// partitioned parent gets a plain in-transaction `CREATE INDEX` — correct and
/// cheap, since the table holds no rows yet.
pub fn index_create_split(
    index: &TableIndex,
    ctx: &PartitionContext,
    parent_is_new: bool,
) -> ProdSplit {
    let mut split = ProdSplit::default();
    let parent_qualified = format!("{}.{}", index.schema, index.table);
    let is_partitioned_parent = ctx.partitioned_parents.contains(&parent_qualified);

    // New partitioned parent: let PostgreSQL manage the partition indexes when
    // the (also new, empty) partitions are created. A plain in-txn build avoids
    // the double-attach conflict.
    if is_partitioned_parent && parent_is_new {
        split.in_txn.push_str(&index.indexdef);
        split.in_txn.append_block(";");
        if let Some(comment) = &index.comment {
            split.in_txn.append_block(&comment_on_index(index, comment));
        }
        return split;
    }

    // Plain (non-partitioned) table: a straight concurrent build, post-commit.
    if !is_partitioned_parent {
        split
            .post_commit
            .push_str(&insert_concurrently(&index.indexdef));
        split.post_commit.append_block(";");
        if let Some(comment) = &index.comment {
            split
                .post_commit
                .append_block(&comment_on_index(index, comment));
        }
        return split;
    }

    let children = ctx
        .children
        .get(&parent_qualified)
        .map(Vec::as_slice)
        .unwrap_or(&[]);

    // Concurrent per-partition handling needs the parsed keyword/tail and a
    // single level of partitioning. Sub-partitioned children (a partition that
    // is itself a partitioned table) and unparseable definitions fall back to a
    // plain, in-transaction CREATE INDEX on the parent (which recurses to every
    // partition under a brief lock — CONCURRENTLY is illegal on a partitioned
    // table, so this is the correct, if less convenient, fallback).
    let has_sub_partition = children.iter().any(|c| {
        ctx.partitioned_parents
            .contains(&format!("{}.{}", c.schema, c.table))
    });
    let parsed = create_kw_and_tail(&index.indexdef);

    if children.is_empty() || has_sub_partition || parsed.is_none() {
        let reason = if parsed.is_none() {
            "unrecognised index definition"
        } else if children.is_empty() {
            "no partitions found"
        } else {
            "multi-level partitioning"
        };
        split.in_txn.push_str(&format!(
            "/* partitioned index {}.{}: {reason} — created non-concurrently */\n",
            index.schema, index.name
        ));
        split.in_txn.push_str(&index.indexdef);
        split.in_txn.append_block(";");
        if let Some(comment) = &index.comment {
            split.in_txn.append_block(&comment_on_index(index, comment));
        }
        return split;
    }

    let (create_kw, tail) = parsed.unwrap();

    // 1. Create the index on the parent only (metadata-only, fast) in-txn.
    match make_on_only(&index.indexdef) {
        Some(only) => split.in_txn.append_block(&format!("{only};")),
        None => split.in_txn.append_block(&format!(
            "{create_kw} {} ON ONLY {}.{} {tail};",
            index.name, index.schema, index.table
        )),
    }
    if let Some(comment) = &index.comment {
        split.in_txn.append_block(&comment_on_index(index, comment));
    }

    // 2. Build each partition's index concurrently, then attach it. Once every
    //    partition is attached the parent index becomes valid automatically.
    for child in children {
        let child_idx = child_index_name(index, child);
        split.post_commit.append_block(&format!(
            "{create_kw} CONCURRENTLY {child_idx} ON {}.{} {tail};",
            child.schema, child.table
        ));
        split.post_commit.append_block(&format!(
            "alter index {}.{} attach partition {}.{};",
            index.schema, index.name, child.schema, child_idx
        ));
    }
    split
}

/// Production rewrite of a single index *drop*. Returns the statement plus
/// whether it must run post-commit (`true`) or stay in-transaction (`false`).
/// `DROP INDEX CONCURRENTLY` is illegal for an index on a partitioned table, so
/// those stay in-transaction and non-concurrent.
pub fn index_drop_statement(index: &TableIndex, ctx: &PartitionContext) -> (String, bool) {
    let qualified_index = format!("{}.{}", index.schema, index.name);
    if ctx.partitioned_indexes.contains(&qualified_index) {
        (
            format!("drop index if exists {}.{};", index.schema, index.name),
            false,
        )
    } else {
        (
            format!(
                "drop index concurrently if exists {}.{};",
                index.schema, index.name
            ),
            true,
        )
    }
}

/// Production rewrite of a foreign-key constraint creation: add it `NOT VALID`
/// inside the transaction, then `VALIDATE CONSTRAINT` afterwards. Only enforced
/// FOREIGN KEY constraints are split; anything else is emitted unchanged
/// in-transaction (a `NOT ENFORCED` constraint is never validated, and other
/// constraint kinds do not support `NOT VALID` in this path).
pub fn foreign_key_split(constraint: &TableConstraint) -> ProdSplit {
    let mut split = ProdSplit::default();
    let full = constraint.get_script();

    if !constraint
        .constraint_type
        .eq_ignore_ascii_case("foreign key")
        || !constraint.is_enforced
    {
        split.in_txn.push_str(&full);
        return split;
    }

    // The ADD CONSTRAINT statement ends at the first ';' (a FOREIGN KEY clause
    // never contains one); any trailing `comment on constraint` block follows.
    match full.find(';') {
        Some(semi) => {
            let add_stmt = full[..semi].trim_end();
            let remainder = full[semi + 1..].trim_start_matches('\n');

            let mut in_txn = add_stmt.to_string();
            in_txn.push_str(" not valid;");
            in_txn = in_txn.with_empty_lines();
            if !remainder.trim().is_empty() {
                in_txn.push_str(remainder);
            }
            split.in_txn = in_txn;
            split.post_commit = format!(
                "alter table {}.{} validate constraint {};",
                constraint.schema, constraint.table_name, constraint.name
            )
            .with_empty_lines();
        }
        None => split.in_txn.push_str(&full),
    }
    split
}

#[cfg(test)]
#[path = "production_tests.rs"]
mod tests;
