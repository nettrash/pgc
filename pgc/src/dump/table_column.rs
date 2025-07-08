use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

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

impl TableColumn {
    /// Hash
    pub fn add_to_hasher(&self, hasher: &mut Sha256) {
        hasher.update(self.catalog.as_bytes());
        hasher.update(self.schema.as_bytes());
        hasher.update(self.table.as_bytes());
        hasher.update(self.name.as_bytes());
        hasher.update(self.ordinal_position.to_string().as_bytes());
        hasher.update(self.column_default.as_deref().unwrap_or("").as_bytes());
        hasher.update(self.is_nullable.to_string().as_bytes());
        hasher.update(self.data_type.as_bytes());
        hasher.update(self.character_maximum_length.unwrap_or(-1).to_string().as_bytes());
        hasher.update(self.character_octet_length.unwrap_or(-1).to_string().as_bytes());
        hasher.update(self.numeric_precision.unwrap_or(-1).to_string().as_bytes());
        hasher.update(self.numeric_precision_radix.unwrap_or(-1).to_string().as_bytes());
        hasher.update(self.numeric_scale.unwrap_or(-1).to_string().as_bytes());
        hasher.update(self.datetime_precision.unwrap_or(-1).to_string().as_bytes());
        hasher.update(self.interval_type.as_deref().unwrap_or("").as_bytes());
        hasher.update(self.interval_precision.unwrap_or(-1).to_string().as_bytes());
        hasher.update(self.character_set_catalog.as_deref().unwrap_or("").as_bytes());
        hasher.update(self.character_set_schema.as_deref().unwrap_or("").as_bytes());
        hasher.update(self.character_set_name.as_deref().unwrap_or("").as_bytes());
        hasher.update(self.collation_catalog.as_deref().unwrap_or("").as_bytes());
        hasher.update(self.collation_schema.as_deref().unwrap_or("").as_bytes());
        hasher.update(self.collation_name.as_deref().unwrap_or("").as_bytes());
        hasher.update(self.domain_catalog.as_deref().unwrap_or("").as_bytes());
        hasher.update(self.domain_schema.as_deref().unwrap_or("").as_bytes());
        hasher.update(self.domain_name.as_deref().unwrap_or("").as_bytes());
        hasher.update(self.udt_catalog.as_deref().unwrap_or("").as_bytes());
        hasher.update(self.udt_schema.as_deref().unwrap_or("").as_bytes());
        hasher.update(self.udt_name.as_deref().unwrap_or("").as_bytes());
        hasher.update(self.scope_catalog.as_deref().unwrap_or("").as_bytes());
        hasher.update(self.scope_schema.as_deref().unwrap_or("").as_bytes());
        hasher.update(self.scope_name.as_deref().unwrap_or("").as_bytes());
        hasher.update(self.maximum_cardinality.unwrap_or(-1).to_string().as_bytes());
        hasher.update(self.dtd_identifier.as_deref().unwrap_or("").as_bytes());
        hasher.update(self.is_self_referencing.to_string().as_bytes());
        hasher.update(self.is_identity.to_string().as_bytes());
        hasher.update(self.identity_generation.as_deref().unwrap_or("").as_bytes());
        hasher.update(self.identity_start.as_deref().unwrap_or("").as_bytes());
        hasher.update(self.identity_increment.as_deref().unwrap_or("").as_bytes());
        hasher.update(self.identity_maximum.as_deref().unwrap_or("").as_bytes());
        hasher.update(self.identity_minimum.as_deref().unwrap_or("").as_bytes());
        hasher.update(self.identity_cycle.to_string().as_bytes());
        hasher.update(self.is_generated.as_bytes());
        hasher.update(self.generation_expression.as_deref().unwrap_or("").as_bytes());
        hasher.update(self.is_updatable.to_string().as_bytes());
    }
}