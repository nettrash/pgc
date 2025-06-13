use serde::{Deserialize, Serialize};

// This is an information about a PostgreSQL table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableColumn {
    pub catalog: String,                       // Catalog name
    pub schema: String,                        // Schema name
    pub table: String,                         // Table name
    pub name: String,                          // Column name
    pub ordinal_position: i32,                 // Ordinal position of the column
    pub column_default: Option<String>,        // Default value of the column
    pub is_nullable: bool,                     // Whether the column is nullable
    pub data_type: String,                     // Data type of the column
    pub character_maximum_length: Option<i32>, // Maximum length for character types
    pub character_octet_length: Option<i32>,   // Octet length for character types
    pub numeric_precision: Option<i32>,        // Numeric precision
    pub numeric_precision_radix: Option<i32>,  // Numeric precision radix
    pub numeric_scale: Option<i32>,            // Numeric scale
    pub datetime_precision: Option<i32>,       // Datetime precision
    pub interval_type: Option<String>,         // Interval type
    pub interval_precision: Option<i32>,       // Interval precision
    pub character_set_catalog: Option<String>, // Character set catalog
    pub character_set_schema: Option<String>,  // Character set schema
    pub character_set_name: Option<String>,    // Character set name
    pub collation_catalog: Option<String>,     // Collation catalog
    pub collation_schema: Option<String>,      // Collation schema
    pub collation_name: Option<String>,        // Collation name
    pub domain_catalog: Option<String>,        // Domain catalog
    pub domain_schema: Option<String>,         // Domain schema
    pub domain_name: Option<String>,           // Domain name
    pub udt_catalog: Option<String>,           // UDT catalog
    pub udt_schema: Option<String>,            // UDT schema
    pub udt_name: Option<String>,              // UDT name
    pub scope_catalog: Option<String>,         // Scope catalog
    pub scope_schema: Option<String>,          // Scope schema
    pub scope_name: Option<String>,            // Scope name
    pub maximum_cardinality: Option<i32>,      // Maximum cardinality
    pub dtd_identifier: Option<String>,        // DTD identifier
    pub is_self_referencing: bool,             // Whether the column is self-referencing
    pub is_identity: bool,                     // Whether the column is an identity column
    pub identity_generation: Option<String>,   // Identity generation method
    pub identity_start: Option<String>,        // Identity start value
    pub identity_increment: Option<String>,    // Identity increment value
    pub identity_maximum: Option<String>,      // Identity maximum value
    pub identity_minimum: Option<String>,      // Identity minimum value
    pub identity_cycle: bool,                  // Whether the identity column cycles
    pub is_generated: String,                  // Whether the column is generated
    pub generation_expression: Option<String>, // Generation expression for the column
    pub is_updatable: bool,                    // Whether the column is updatable
}
