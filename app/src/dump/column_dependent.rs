//! Column → dependent-object edges read from `pg_depend`.
//!
//! When a column is dropped, PostgreSQL silently CASCADEs the drop to the indexes,
//! constraints and policies attached to it. The text-based scanner cannot see
//! these: the dependent's definition references the *column*, not the routine
//! whose change triggered the CASCADE chain. Recording the edges at dump time lets
//! the comparer re-emit them afterwards (issue #188).
//!
//! Empty in dumps written before that issue; the comparer then degrades to the
//! previously documented "run `pgc compare` twice" workaround.

use serde::{Deserialize, Serialize};

/// Kind of database object that depends on a column. When the column is
/// dropped, PostgreSQL CASCADEs the drop to objects of these kinds.
/// Issue #188.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ColumnDependentKind {
    Index,
    Constraint,
    Policy,
}

/// One column → dependent-object edge harvested from `pg_catalog.pg_depend`.
///
/// Captures the fact that dropping `(schema, table, column)` will
/// CASCADE-drop `(dep_schema, dep_table, dep_name)` (an index, constraint,
/// or policy). The comparer uses this graph to re-emit secondary
/// dependents that the text-based Phase 7 scanner cannot detect — the
/// dependent's own definition references the column, not the routine
/// whose drop triggered the CASCADE chain. Same machinery is used by the
/// virtual-generated-column drop+add path in
/// `TableColumn::get_alter_script` (issue #181 / PR #186).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDependent {
    /// Anchor column's table schema (quote_ident-wrapped).
    pub schema: String,
    /// Anchor column's table name (quote_ident-wrapped).
    pub table: String,
    /// Anchor column name (quote_ident-wrapped).
    pub column: String,
    pub kind: ColumnDependentKind,
    /// Dependent object's schema (always equals the table's schema for
    /// index/constraint/policy, but stored explicitly for symmetry).
    pub dep_schema: String,
    /// Table that owns the dependent object.
    pub dep_table: String,
    /// Dependent object's name.
    pub dep_name: String,
}

#[cfg(test)]
#[path = "tests/column_dependent.rs"]
mod tests;
