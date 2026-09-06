//! PostgreSQL introspection: one module per catalog object kind, plus
//! [`core`] which owns the [`Dump`](core::Dump) snapshot and orchestrates the
//! parallel fill.
//!
//! Nearly every object module follows the same shape, so learning one teaches
//! the rest:
//!
//! | Item | Role |
//! | --- | --- |
//! | `struct <Object>` | Mirrors the catalog row; fields are named after the catalog columns |
//! | `hash(&mut self)` | Folds the identity-bearing fields into a digest — change detection compares hashes, never field-by-field |
//! | `get_script(&self)` | `CREATE …` for this object |
//! | `get_drop_script(&self)` | `DROP …` for this object |
//! | `get_alter_script(&self, target)` | The in-place migration to `target`, or a drop/recreate pair when PostgreSQL has no in-place form |
//!
//! The comparer never builds SQL itself for these kinds; it decides *which* of
//! the four to call and in *what order*. Adding an object kind therefore means
//! adding a module here with those methods, a field on [`Dump`](core::Dump)
//! carrying `#[serde(default, skip_serializing_if = "Vec::is_empty")]` so older
//! dump files stay readable, and a comparison pass in
//! [`Comparer`](crate::comparer::core::Comparer).

pub mod acl;
pub mod cast;
pub mod collation;
pub mod column_dependent;
pub mod core;
pub mod default_privilege;
pub mod event_trigger;
pub mod extension;
pub mod fdw;
pub mod foreign_table;
pub mod operator;
pub mod pg_enum;
pub mod pg_type;
pub mod publication;
pub mod routine;
pub mod rule;
pub mod schema;
pub mod sequence;
pub mod statistic;
pub mod table;
pub mod table_column;
pub mod table_constraint;
pub mod table_index;
pub mod table_policy;
pub mod table_trigger;
pub mod text_search;
pub mod view;
