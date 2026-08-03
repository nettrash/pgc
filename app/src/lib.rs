//! `pgc` — PostgreSQL database schema comparer.
//!
//! The crate is split into four modules, mirroring the three user-facing
//! commands of the `pgc` binary:
//!
//! - [`dump`] — per-object PostgreSQL introspection; [`dump::core::Dump`] owns
//!   the schema snapshot and can serialize it to a zip-compressed JSON file.
//! - [`comparer`] — [`comparer::core::Comparer`] reads two [`dump::core::Dump`]s
//!   and emits the migration SQL that turns `FROM` into `TO`.
//! - [`config`] — the `pgc.conf` key-value parser and its value types.
//! - [`utils`] — small shared helpers (SQL normalisation, string extensions).
//!
//! The library target exists so that the binary in `src/main.rs` and the
//! integration tests in `app/tests/` can both build on the same public API.
//! Unit tests that need access to private internals live beside their module,
//! under each module's `tests/` directory.

pub mod comparer;
pub mod config;
pub mod dump;
pub mod utils;
