//! Enum labels (`pg_enum`).
//!
//! Enums are ordered, and PostgreSQL can only *add* labels
//! (`ALTER TYPE … ADD VALUE`), never remove or reorder them. That asymmetry is why
//! [`Comparer`](crate::comparer::core::Comparer) keeps separate `enum_pre_script`
//! and `enum_post_script` buffers: additions can run early, while anything
//! requiring a type recreation has to wait until its dependents are gone.

use serde::{Deserialize, Serialize};
use sqlx::postgres::types::Oid;

/// This is an information about a PostgreSQL type.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PgEnum {
    /// Oid of Enum type
    pub oid: Oid,
    /// Oid of the Enum type
    pub enumtypid: Oid,
    /// Sort order of the enum value
    pub enumsortorder: f32,
    /// Label of the enum value
    pub enumlabel: String,
}

#[cfg(test)]
#[path = "tests/pg_enum.rs"]
mod tests;
