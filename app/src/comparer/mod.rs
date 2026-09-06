//! Diff generation: turns two [`Dump`](crate::dump::core::Dump)s into the
//! migration SQL that makes the `FROM` schema equal to the `TO` schema.
//!
//! - [`core`] — the [`Comparer`](core::Comparer) itself, which walks every object
//!   kind in dependency order and accumulates the ordered script buffers.
//! - [`production`] — rewrites a finished script so it can be applied to a live
//!   database with minimal locking (`--output-for-production`).
//! - `scanner` (private) — the shared SQL-aware scanner both use to skip string literals,
//!   quoted identifiers, comments and dollar-quoted bodies.

pub mod core;
pub mod production;
mod scanner;
