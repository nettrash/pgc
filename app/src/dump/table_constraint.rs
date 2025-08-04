use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

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

impl TableConstraint {
    /// Hash
    pub fn add_to_hasher(&self, hasher: &mut Sha256) {
        hasher.update(self.catalog.as_bytes());
        hasher.update(self.schema.as_bytes());
        hasher.update(self.name.as_bytes());
        hasher.update(self.table_catalog.as_bytes());
        hasher.update(self.table_schema.as_bytes());
        hasher.update(self.table_name.as_bytes());
        hasher.update(self.constraint_type.as_bytes());
        hasher.update(self.is_deferrable.to_string().as_bytes());
        hasher.update(self.initially_deferred.to_string().as_bytes());
        hasher.update(self.enforced.to_string().as_bytes());
        if let Some(nulls_distinct) = self.nulls_distinct {
            hasher.update(nulls_distinct.to_string().as_bytes());
        }
    }

    /// Returns a string representation of the constraint
    pub fn get_script(&self) -> String {
        let mut script = String::new();
        script.push_str(&format!(
            "alter table {}.{} add constraint {} ",
            self.table_schema, self.table_name, self.name
        ));
        script.push_str(&format!("{} ", self.constraint_type.to_lowercase()));
        if self.is_deferrable {
            script.push_str("deferrable ");
        }
        if self.initially_deferred {
            script.push_str("initially deferred ");
        }
        if !self.enforced {
            script.push_str("not enforced ");
        }
        if let Some(nulls_distinct) = self.nulls_distinct {
            if nulls_distinct {
                script.push_str("nulls distinct ");
            } else {
                script.push_str("nulls not distinct ");
            }
        }
        script.push_str(";\n");
        script
    }
}

impl PartialEq for TableConstraint {
    fn eq(&self, other: &Self) -> bool {
        self.catalog == other.catalog
            && self.schema == other.schema
            && self.name == other.name
            && self.table_catalog == other.table_catalog
            && self.table_schema == other.table_schema
            && self.table_name == other.table_name
            && self.constraint_type == other.constraint_type
            && self.is_deferrable == other.is_deferrable
            && self.initially_deferred == other.initially_deferred
            && self.enforced == other.enforced
            && self.nulls_distinct == other.nulls_distinct
    }
}
