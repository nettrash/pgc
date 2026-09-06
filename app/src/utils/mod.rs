//! Small helpers shared across the dump and comparer modules.
//!
//! - [`sql_normalize`] — canonicalises a deparsed SQL definition so that two
//!   renderings of the same expression compare equal.
//! - [`string_extensions`] — quoting, identifier and script-assembly helpers.

pub mod sql_normalize;
pub mod string_extensions;
