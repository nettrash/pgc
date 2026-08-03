//! Unit tests for [`Comparer`](super::Comparer).
//!
//! Split by concern; every submodule reaches the private internals of
//! `comparer::core` through `use crate::comparer::core::*;`, and shared
//! fixture builders live in [`helpers`].

mod buffer_ordering;
mod cascade_dependents;
mod column_dependents;
mod grants;
mod helpers;
mod matview_indexes;
mod persistence;
mod production;
mod routines;
mod schemas_extensions;
mod script_output;
mod sequences;
mod tables;
mod types;
mod views;
