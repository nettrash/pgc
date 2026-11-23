//! Column → dependent-object edges read from `pg_depend`.
//!
//! When a column is dropped, PostgreSQL silently CASCADEs the drop to the indexes,
//! constraints and extended statistics attached to it, and refuses it while a
//! policy, a trigger or another table's foreign key still depends on it. The text-based
//! scanner cannot see these: the dependent's definition references the
//! *column*, not the routine whose change triggered the CASCADE chain.
//! Recording the edges at dump time lets the comparer re-emit them afterwards
//! (issue #188).
//!
//! Empty in dumps written before that issue; the comparer then degrades to the
//! previously documented "run `pgc compare` twice" workaround.

use serde::{Deserialize, Serialize};

/// Kind of database object that depends on a column. When the column is
/// dropped, PostgreSQL CASCADEs the drop to objects of these kinds.
/// Issue #188.
///
/// Serialized by variant name. A reader only accepts the variants it knows,
/// so a dump that carries `Statistic` (written by pgc 1.1.0 or later) cannot
/// be read by pgc 1.0.x; older dumps still load here. That is not a corner
/// case: every extended statistics object has a column-level `pg_depend`
/// edge for each column it reads (plain keys, columns inside expressions and
/// virtual columns alike), so any 1.1.0 dump of a schema with extended
/// statistics carries one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ColumnDependentKind {
    Index,
    Constraint,
    Policy,
    /// An extended statistics object (`CREATE STATISTICS`) whose `ON` list
    /// names the column, directly or inside an expression. Its
    /// [`ColumnDependent::dep_schema`] is the statistics object's own schema,
    /// which need not be the table's.
    Statistic,
    /// A trigger that names the column in `UPDATE OF` or reads it in its
    /// `WHEN` clause: a table's own trigger, never a partition's clone of
    /// its parent's (see
    /// [`Table::trigger_clones`](crate::dump::table::Table::trigger_clones)).
    /// Unlike the other kinds it does not go with the column: PostgreSQL
    /// refuses to drop the column while the trigger is there ("other objects
    /// depend on it"), so a column drop + add has to drop the trigger first
    /// and create it again afterwards.
    Trigger,
}

/// One column → dependent-object edge harvested from `pg_catalog.pg_depend`.
///
/// Captures the fact that dropping `(schema, table, column)` will
/// CASCADE-drop `(dep_schema, dep_table, dep_name)` (an index, constraint,
/// policy, or extended statistics object). The comparer uses this graph to
/// re-emit secondary dependents that the text-based Phase 7 scanner cannot
/// detect — the dependent's own definition references the column, not the
/// routine whose drop triggered the CASCADE chain. Same machinery is used by the
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
    /// Dependent object's schema. Always equals the owning table's schema
    /// for index/constraint/policy; a statistics object lives in its own
    /// namespace, which may differ from the table's.
    pub dep_schema: String,
    /// Table that owns the dependent object.
    pub dep_table: String,
    /// Dependent object's name.
    pub dep_name: String,
}

#[cfg(test)]
#[path = "tests/column_dependent.rs"]
mod tests;
