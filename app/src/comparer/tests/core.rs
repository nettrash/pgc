//! Unit tests for [`Comparer`](super::Comparer).
//!
//! Split by concern. Every submodule reaches the private internals of
//! `comparer::core` through `use crate::comparer::core::*;`, and fixture
//! builders shared by more than one submodule live in [`helpers`].
//!
//! The `#[path]` attributes are required: this module is itself loaded via
//! `#[path = "tests/core.rs"]`, so rustc resolves child modules relative to
//! `src/comparer/tests/` rather than to `src/comparer/tests/core/`.

#[path = "core/buffer_ordering.rs"]
mod buffer_ordering;
#[path = "core/cascade_dependents.rs"]
mod cascade_dependents;
#[path = "core/column_dependents.rs"]
mod column_dependents;
#[path = "core/grants.rs"]
mod grants;
#[path = "core/helpers.rs"]
mod helpers;
#[path = "core/matview_indexes.rs"]
mod matview_indexes;
#[path = "core/persistence.rs"]
mod persistence;
#[path = "core/production.rs"]
mod production;
#[path = "core/routines.rs"]
mod routines;
#[path = "core/schemas_extensions.rs"]
mod schemas_extensions;
#[path = "core/script_output.rs"]
mod script_output;
#[path = "core/sequences.rs"]
mod sequences;
#[path = "core/tables.rs"]
mod tables;
#[path = "core/types.rs"]
mod types;
#[path = "core/view_column_dependencies.rs"]
mod view_column_dependencies;
#[path = "core/views.rs"]
mod views;
