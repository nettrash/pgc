use serde::{Deserialize, Serialize};
use sqlx::postgres::types::Oid;

// This is an information about a PostgreSQL type.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PgType {
    pub schema: String, // Schema where the type is defined
    pub typname: String, // Name of the type
    pub typnamespace: Oid, // Schema where the type is defined
    pub typowner: Oid, // Owner of the type
    pub typlen: i16, // Length of the type in bytes
    pub typbyval: bool, // Whether the type is passed by value
    pub typtype: i8, // Type of the type (e.g., base, composite, domain)
    pub typcategory: i8, // Category of the type (e.g., numeric, string)
    pub typispreferred: bool, // Whether the type is preferred for implicit casts
    pub typisdefined: bool, // Whether the type is defined
    pub typdelim: i8, // Delimiter for array types
    pub typrelid: Option<Oid>, // Type of the type if it is a domain
    pub typsubscript: Option<String>, // Subscript type if it is an array
    pub typelem: Option<Oid>, // Element type if it is an array
    pub typarray: Option<Oid>, // Array type if it is an array
    pub typinput: String, // Input function for the type
    pub typoutput: String, // Output function for the type
    pub typreceive: Option<String>, // Receive function for the type
    pub typsend: Option<String>, // Send function for the type
    pub typmodin: Option<String>, // Type modifier input function
    pub typmodout: Option<String>, // Type modifier output function
    pub typanalyze: Option<String>, // Analyze function for the type
    pub typalign: i8, // Alignment of the type (e.g., char, int, double)
    pub typstorage: i8, // Storage type of the type (e.g., plain, extended)
    pub typnotnull: bool, // Whether the type is not null
    pub typbasetype: Option<Oid>, // Base type if it is a domain
    pub typtypmod: Option<i32>, // Type modifier for the type
    pub typndims: i32, // Number of dimensions if it is an array
    pub typcollation: Option<Oid>, // Collation for the type
    pub typdefault: Option<String>, // Default value for the type
}