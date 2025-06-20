use serde::{Deserialize, Serialize};

// This is an information about a PostgreSQL table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableConstraint {
    pub catalog: String,              // Catalog name
    pub schema: String,               // Schema name
    pub name: String,                 // Constraint name
    pub table_catalog: String,        // Table catalog
    pub table_schema: String,         // Table schema
    pub table_name: String,           // Table name
    pub constraint_type: String, // Type of the constraint (e.g., PRIMARY KEY, FOREIGN KEY, UNIQUE)
    pub is_deferrable: bool,     // Whether the constraint is deferrable
    pub initially_deferred: bool, // Whether the constraint is initially deferred
    pub enforced: bool,          // Whether the constraint is enforced
    pub nulls_distinct: Option<bool>, // Whether the constraint allows nulls to be distinct
}
