use serde::{Deserialize, Serialize};
use sqlx::postgres::types::Oid;

// This is an information about a PostgreSQL type.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PgEnum {
    pub oid: Oid,           // Oid of Enum type
    pub enumtypid: Oid,     // Oid of the Enum type
    pub enumsortorder: f32, // Sort order of the enum value
    pub enumlabel: String,  // Label of the enum value
}
