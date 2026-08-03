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
//!
//! # Example
//!
//! The `compare` command, end to end: read two dumps from disk and write the
//! migration that turns the first into the second.
//!
//! ```no_run
//! use pgc::comparer::core::Comparer;
//! use pgc::config::grants_mode::GrantsMode;
//! use pgc::dump::core::Dump;
//!
//! # fn main() -> Result<(), std::io::Error> {
//! # tokio::runtime::Runtime::new()?.block_on(async {
//! let from = Dump::read_from_file("dump.from").await?;
//! let to = Dump::read_from_file("dump.to").await?;
//!
//! let mut comparer = Comparer::new(
//!     from,
//!     to,
//!     true,                 // use_drop: emit DROP statements
//!     true,                 // use_single_transaction: wrap in begin/commit
//!     true,                 // use_comments
//!     GrantsMode::Ignore,
//! );
//! comparer.compare().await?;
//! comparer.save_script("migration.sql").await?;
//! # Ok::<(), std::io::Error>(())
//! # })?;
//! # Ok(())
//! # }
//! ```
//!
//! Note the direction: applying the generated script to `FROM` makes its schema
//! equal to `TO`. `TO` is the reference schema and is never modified.
//!
//! Because the crate is documented for contributors rather than API consumers,
//! and most of the interesting code is private, build the docs with
//! `cargo doc --no-deps --lib --document-private-items`.

pub mod comparer;
pub mod config;
pub mod dump;
pub mod utils;
