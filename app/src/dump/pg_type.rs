use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::postgres::types::Oid;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DomainConstraint {
    pub name: String,
    pub definition: String,
}

fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

fn escape_single_quotes(value: &str) -> String {
    value.replace('\'', "''")
}

// This is an information about a PostgreSQL type.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PgType {
    pub oid: Oid,                     // Unique identifier of the type
    pub schema: String,               // Schema where the type is defined
    pub typname: String,              // Name of the type
    pub typnamespace: Oid,            // Schema where the type is defined
    pub typowner: Oid,                // Owner of the type
    pub typlen: i16,                  // Length of the type in bytes
    pub typbyval: bool,               // Whether the type is passed by value
    pub typtype: i8,                  // Type of the type (e.g., base, composite, domain)
    pub typcategory: i8,              // Category of the type (e.g., numeric, string)
    pub typispreferred: bool,         // Whether the type is preferred for implicit casts
    pub typisdefined: bool,           // Whether the type is defined
    pub typdelim: i8,                 // Delimiter for array types
    pub typrelid: Option<Oid>,        // Type of the type if it is a domain
    pub typsubscript: Option<String>, // Subscript type if it is an array
    pub typelem: Option<Oid>,         // Element type if it is an array
    pub typarray: Option<Oid>,        // Array type if it is an array
    pub typinput: String,             // Input function for the type
    pub typoutput: String,            // Output function for the type
    pub typreceive: Option<String>,   // Receive function for the type
    pub typsend: Option<String>,      // Send function for the type
    pub typmodin: Option<String>,     // Type modifier input function
    pub typmodout: Option<String>,    // Type modifier output function
    pub typanalyze: Option<String>,   // Analyze function for the type
    pub typalign: i8,                 // Alignment of the type (e.g., char, int, double)
    pub typstorage: i8,               // Storage type of the type (e.g., plain, extended)
    pub typnotnull: bool,             // Whether the type is not null
    pub typbasetype: Option<Oid>,     // Base type if it is a domain
    pub typtypmod: Option<i32>,       // Type modifier for the type
    pub typndims: i32,                // Number of dimensions if it is an array
    pub typcollation: Option<Oid>,    // Collation for the type
    pub typdefault: Option<String>,   // Default value for the type
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub formatted_basetype: Option<String>, // Human-readable base type (for domains)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub enum_labels: Vec<String>, // Enum labels ordered by sort order
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub domain_constraints: Vec<DomainConstraint>, // Domain constraints (check, etc.)
}

impl PgType {
    /// Computes a SHA256 hash of the type definition.
    ///
    /// This hash can be used for change detection, caching, or verifying the integrity
    /// of the type's metadata. It includes all relevant fields of the `PgType` struct.
    pub fn hash(&self) -> String {
        let mut hasher = Sha256::new();

        hasher.update(self.schema.as_bytes());
        hasher.update(self.typname.as_bytes());
        hasher.update(self.typnamespace.0.to_be_bytes());
        hasher.update(self.typowner.0.to_be_bytes());
        hasher.update(self.typlen.to_be_bytes());
        hasher.update([self.typbyval as u8]);
        hasher.update(self.typtype.to_be_bytes());
        hasher.update(self.typcategory.to_be_bytes());
        hasher.update([self.typispreferred as u8]);
        hasher.update([self.typisdefined as u8]);
        hasher.update(self.typdelim.to_be_bytes());

        update_option(&mut hasher, &self.typrelid, |hasher, value| {
            hasher.update(value.0.to_be_bytes());
        });
        update_option(&mut hasher, &self.typsubscript, |hasher, value| {
            hasher.update(value.as_bytes());
        });
        update_option(&mut hasher, &self.typelem, |hasher, value| {
            hasher.update(value.0.to_be_bytes());
        });
        update_option(&mut hasher, &self.typarray, |hasher, value| {
            hasher.update(value.0.to_be_bytes());
        });

        hasher.update(self.typinput.as_bytes());
        hasher.update(self.typoutput.as_bytes());

        update_option(&mut hasher, &self.typreceive, |hasher, value| {
            hasher.update(value.as_bytes());
        });
        update_option(&mut hasher, &self.typsend, |hasher, value| {
            hasher.update(value.as_bytes());
        });
        update_option(&mut hasher, &self.typmodin, |hasher, value| {
            hasher.update(value.as_bytes());
        });
        update_option(&mut hasher, &self.typmodout, |hasher, value| {
            hasher.update(value.as_bytes());
        });
        update_option(&mut hasher, &self.typanalyze, |hasher, value| {
            hasher.update(value.as_bytes());
        });

        hasher.update(self.typalign.to_be_bytes());
        hasher.update(self.typstorage.to_be_bytes());
        hasher.update([self.typnotnull as u8]);

        update_option(&mut hasher, &self.typbasetype, |hasher, value| {
            hasher.update(value.0.to_be_bytes());
        });
        update_option(&mut hasher, &self.typtypmod, |hasher, value| {
            hasher.update(value.to_be_bytes());
        });

        hasher.update(self.typndims.to_be_bytes());

        update_option(&mut hasher, &self.typcollation, |hasher, value| {
            hasher.update(value.0.to_be_bytes());
        });
        update_option(&mut hasher, &self.typdefault, |hasher, value| {
            hasher.update(value.as_bytes());
        });

        hasher.update((self.enum_labels.len() as u32).to_be_bytes());
        for label in &self.enum_labels {
            hasher.update((label.len() as u32).to_be_bytes());
            hasher.update(label.as_bytes());
        }

        hasher.update((self.domain_constraints.len() as u32).to_be_bytes());
        for constraint in &self.domain_constraints {
            hasher.update((constraint.name.len() as u32).to_be_bytes());
            hasher.update(constraint.name.as_bytes());
            hasher.update((constraint.definition.len() as u32).to_be_bytes());
            hasher.update(constraint.definition.as_bytes());
        }

        format!("{:x}", hasher.finalize())
    }

    /// Returns a string to create the user-defined type.
    pub fn get_script(&self) -> String {
        match self.typtype as u8 as char {
            'e' => {
                if self.enum_labels.is_empty() {
                    return format!(
                        "-- Enum {}.{} has no labels available in dump\n",
                        self.schema, self.typname
                    );
                }

                let variants = self
                    .enum_labels
                    .iter()
                    .map(|label| format!("'{}'", escape_single_quotes(label)))
                    .collect::<Vec<_>>()
                    .join(", ");

                format!(
                    "create type {}.{} as enum ({});\n",
                    self.schema, self.typname, variants
                )
            }
            'd' => {
                let base_type = self.formatted_basetype.as_deref().unwrap_or("text");

                let mut clauses = Vec::new();

                if let Some(default) = &self.typdefault
                    && !default.trim().is_empty()
                {
                    clauses.push(format!("default {}", default));
                }

                if self.typnotnull {
                    clauses.push("not null".to_string());
                }

                let mut script = format!(
                    "create domain {}.{} as {}",
                    self.schema, self.typname, base_type
                );

                if !clauses.is_empty() {
                    script.push(' ');
                    script.push_str(&clauses.join(" "));
                }

                script.push_str(";\n");

                for constraint in &self.domain_constraints {
                    script.push_str(&format!(
                        "alter domain {}.{} add constraint {} {};\n",
                        self.schema,
                        self.typname,
                        quote_ident(&constraint.name),
                        constraint.definition
                    ));
                }
                script
            }
            'r' => format!(
                "-- Range type {}.{} is not supported yet\n",
                self.schema, self.typname
            ),
            'm' => format!(
                "-- Multirange type {}.{} is not supported yet\n",
                self.schema, self.typname
            ),
            other => format!(
                "-- Type {}.{} (typtype = {}) is not supported yet\n",
                self.schema, self.typname, other
            ),
        }
    }

    /// Returns a string to alter the existing user-defined type to match the target definition.
    pub fn get_alter_script(&self, target: &PgType) -> String {
        if self.schema != target.schema || self.typname != target.typname {
            return format!(
                "-- Cannot alter type {}.{} because target is {}.{}\n",
                self.schema, self.typname, target.schema, target.typname
            );
        }

        if self.typtype != target.typtype {
            return format!(
                "-- Cannot change type kind of {}.{} ({} -> {})\n",
                self.schema, self.typname, self.typtype as u8 as char, target.typtype as u8 as char
            );
        }

        match (self.typtype as u8 as char, target.typtype as u8 as char) {
            ('e', 'e') => {
                let mut script = String::new();
                let mut known_labels = self.enum_labels.clone();

                for (idx, label) in target.enum_labels.iter().enumerate() {
                    if !known_labels.contains(label) {
                        let escaped_label = escape_single_quotes(label);
                        let mut statement = format!(
                            "alter type {}.{} add value if not exists '{}'",
                            self.schema, self.typname, escaped_label
                        );

                        if let Some(next_existing) = target.enum_labels[idx + 1..]
                            .iter()
                            .find(|value| self.enum_labels.contains(value))
                        {
                            statement.push_str(&format!(
                                " before '{}'",
                                escape_single_quotes(next_existing)
                            ));
                        } else if let Some(prev_existing) = target.enum_labels[..idx]
                            .iter()
                            .rev()
                            .find(|value| known_labels.contains(value))
                        {
                            statement.push_str(&format!(
                                " after '{}'",
                                escape_single_quotes(prev_existing)
                            ));
                        }

                        statement.push_str(";\n");
                        script.push_str(&statement);
                        known_labels.push(label.clone());
                    }
                }

                for label in &self.enum_labels {
                    if !target.enum_labels.contains(label) {
                        script.push_str(&format!(
                            "-- Enum {}.{} cannot automatically remove value '{}'.\n",
                            self.schema,
                            self.typname,
                            escape_single_quotes(label)
                        ));
                    }
                }

                if script.is_empty() {
                    format!(
                        "-- Enum {}.{} requires no changes.\n",
                        self.schema, self.typname
                    )
                } else {
                    script
                }
            }
            ('d', 'd') => {
                let mut statements = Vec::new();

                if self.formatted_basetype != target.formatted_basetype {
                    statements.push(format!(
                        "-- Changing base type of domain {}.{} ({} -> {}) is not supported automatically.\n",
                        self.schema,
                        self.typname,
                        self
                            .formatted_basetype
                            .as_deref()
                            .unwrap_or("unknown"),
                        target
                            .formatted_basetype
                            .as_deref()
                            .unwrap_or("unknown")
                    ));
                }

                if self.typdefault != target.typdefault {
                    if let Some(default) = &target.typdefault {
                        statements.push(format!(
                            "alter domain {}.{} set default {};",
                            self.schema, self.typname, default
                        ));
                    } else {
                        statements.push(format!(
                            "alter domain {}.{} drop default;",
                            self.schema, self.typname
                        ));
                    }
                }

                if self.typnotnull != target.typnotnull {
                    if target.typnotnull {
                        statements.push(format!(
                            "alter domain {}.{} set not null;",
                            self.schema, self.typname
                        ));
                    } else {
                        statements.push(format!(
                            "alter domain {}.{} drop not null;",
                            self.schema, self.typname
                        ));
                    }
                }

                let current_constraints: BTreeMap<_, _> = self
                    .domain_constraints
                    .iter()
                    .map(|constraint| (constraint.name.as_str(), constraint))
                    .collect();
                let target_constraints: BTreeMap<_, _> = target
                    .domain_constraints
                    .iter()
                    .map(|constraint| (constraint.name.as_str(), constraint))
                    .collect();
                let mut replaced_or_added = BTreeSet::new();

                for (name, current_constraint) in &current_constraints {
                    match target_constraints.get(name) {
                        Some(target_constraint) => {
                            if current_constraint.definition != target_constraint.definition {
                                statements.push(format!(
                                    "alter domain {}.{} drop constraint {};",
                                    self.schema,
                                    self.typname,
                                    quote_ident(name)
                                ));
                                statements.push(format!(
                                    "alter domain {}.{} add constraint {} {};",
                                    self.schema,
                                    self.typname,
                                    quote_ident(name),
                                    target_constraint.definition
                                ));
                                replaced_or_added.insert((*name).to_string());
                            }
                        }
                        None => {
                            statements.push(format!(
                                "alter domain {}.{} drop constraint {};",
                                self.schema,
                                self.typname,
                                quote_ident(name)
                            ));
                        }
                    }
                }

                for (name, target_constraint) in &target_constraints {
                    if replaced_or_added.contains(*name) {
                        continue;
                    }

                    if !current_constraints.contains_key(name) {
                        statements.push(format!(
                            "alter domain {}.{} add constraint {} {};",
                            self.schema,
                            self.typname,
                            quote_ident(name),
                            target_constraint.definition
                        ));
                    }
                }

                if statements.is_empty() {
                    format!(
                        "-- Domain {}.{} requires no supported changes.\n",
                        self.schema, self.typname
                    )
                } else {
                    statements.join("\n") + "\n"
                }
            }
            ('r', 'r') => format!(
                "-- Altering range type {}.{} is not supported yet.\n",
                self.schema, self.typname
            ),
            ('m', 'm') => format!(
                "-- Altering multirange type {}.{} is not supported yet.\n",
                self.schema, self.typname
            ),
            _ => format!(
                "-- Altering type {}.{} (typtype = {}) is not supported yet.\n",
                self.schema, self.typname, target.typtype as u8 as char
            ),
        }
    }

    /// Returns a string to drop the user-defined type.
    pub fn get_drop_script(&self) -> String {
        format!("drop type if exists {}.{};\n", self.schema, self.typname)
    }
}

fn update_option<T, F>(hasher: &mut Sha256, option: &Option<T>, mut f: F)
where
    F: FnMut(&mut Sha256, &T),
{
    match option {
        Some(value) => {
            hasher.update([1]);
            f(hasher, value);
        }
        None => hasher.update([0]),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_pg_type() -> PgType {
        PgType {
            oid: Oid(1),
            schema: "public".to_string(),
            typname: "custom_type".to_string(),
            typnamespace: Oid(1234),
            typowner: Oid(42),
            typlen: 8,
            typbyval: false,
            typtype: b'c' as i8,
            typcategory: b'U' as i8,
            typispreferred: false,
            typisdefined: true,
            typdelim: b',' as i8,
            typrelid: None,
            typsubscript: None,
            typelem: Some(Oid(99)),
            typarray: Some(Oid(100)),
            typinput: "pg_catalog.custom_in".to_string(),
            typoutput: "pg_catalog.custom_out".to_string(),
            typreceive: Some("pg_catalog.custom_recv".to_string()),
            typsend: Some("pg_catalog.custom_send".to_string()),
            typmodin: None,
            typmodout: None,
            typanalyze: None,
            typalign: b'd' as i8,
            typstorage: b'p' as i8,
            typnotnull: false,
            typbasetype: None,
            typtypmod: Some(-1),
            typndims: 0,
            typcollation: None,
            typdefault: Some("default".to_string()),
            formatted_basetype: None,
            enum_labels: Vec::new(),
            domain_constraints: Vec::new(),
        }
    }

    #[test]
    fn hash_is_stable_and_hex() {
        let pg_type = sample_pg_type();
        let hash1 = pg_type.hash();
        let hash2 = pg_type.hash();

        assert_eq!(hash1, hash2);
        assert_eq!(hash1.len(), 64);
        assert!(hash1.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn hash_changes_when_optional_value_differs() {
        let base = sample_pg_type();
        let mut other = base.clone();
        other.typdefault = Some("something else".to_string());

        assert_ne!(base.hash(), other.hash());
    }

    #[test]
    fn hash_changes_when_oid_option_added() {
        let base = sample_pg_type();
        let mut other = base.clone();
        other.typrelid = Some(Oid(555));

        assert_ne!(base.hash(), other.hash());
    }

    #[test]
    fn enum_type_get_script() {
        let mut pg_type = sample_pg_type();
        pg_type.schema = "public".to_string();
        pg_type.typname = "status".to_string();
        pg_type.typtype = b'e' as i8;
        pg_type.enum_labels = vec!["active".to_string(), "inactive".to_string()];

        let script = pg_type.get_script();
        assert_eq!(
            script,
            "create type public.status as enum ('active', 'inactive');\n"
        );
    }

    #[test]
    fn domain_type_get_script() {
        let mut pg_type = sample_pg_type();
        pg_type.schema = "public".to_string();
        pg_type.typname = "positive_integer".to_string();
        pg_type.typtype = b'd' as i8;
        pg_type.formatted_basetype = Some("integer".to_string());
        pg_type.typdefault = Some("0".to_string());
        pg_type.typnotnull = true;

        let script = pg_type.get_script();
        assert_eq!(
            script,
            "create domain public.positive_integer as integer default 0 not null;\n"
        );
    }

    #[test]
    fn domain_type_get_script_includes_constraints() {
        let mut pg_type = sample_pg_type();
        pg_type.schema = "public".to_string();
        pg_type.typname = "positive_integer".to_string();
        pg_type.typtype = b'd' as i8;
        pg_type.formatted_basetype = Some("integer".to_string());
        pg_type.typdefault = Some("0".to_string());
        pg_type.typnotnull = true;
        pg_type.domain_constraints = vec![DomainConstraint {
            name: "positive_integer_check".to_string(),
            definition: "CHECK ((VALUE >= 0))".to_string(),
        }];

        let script = pg_type.get_script();
        assert_eq!(
            script,
            "create domain public.positive_integer as integer default 0 not null;\n".to_string()
                + "alter domain public.positive_integer add constraint \"positive_integer_check\" CHECK ((VALUE >= 0));\n"
        );
    }

    #[test]
    fn enum_type_get_alter_script_adds_new_label() {
        let mut current = sample_pg_type();
        current.schema = "public".to_string();
        current.typname = "status".to_string();
        current.typtype = b'e' as i8;
        current.enum_labels = vec!["active".to_string(), "inactive".to_string()];

        let mut target = current.clone();
        target.enum_labels.push("pending".to_string());

        let script = current.get_alter_script(&target);
        assert!(script.contains("add value if not exists 'pending'"));
    }

    #[test]
    fn domain_type_get_alter_script_updates_constraints() {
        let mut current = sample_pg_type();
        current.schema = "public".to_string();
        current.typname = "positive_integer".to_string();
        current.typtype = b'd' as i8;
        current.formatted_basetype = Some("integer".to_string());
        current.typdefault = None;
        current.typnotnull = false;
        current.domain_constraints = vec![DomainConstraint {
            name: "is_positive".to_string(),
            definition: "CHECK ((VALUE > 0))".to_string(),
        }];

        let mut target = current.clone();
        target.domain_constraints = vec![
            DomainConstraint {
                name: "is_positive".to_string(),
                definition: "CHECK ((VALUE >= 0))".to_string(),
            },
            DomainConstraint {
                name: "less_than_max".to_string(),
                definition: "CHECK ((VALUE < 100))".to_string(),
            },
        ];

        let script = current.get_alter_script(&target);
        assert_eq!(
            script,
            "alter domain public.positive_integer drop constraint \"is_positive\";\n".to_string()
                + "alter domain public.positive_integer add constraint \"is_positive\" CHECK ((VALUE >= 0));\n"
                + "alter domain public.positive_integer add constraint \"less_than_max\" CHECK ((VALUE < 100));\n"
        );
    }

    #[test]
    fn domain_type_get_alter_script_updates_default_and_nullability() {
        let mut current = sample_pg_type();
        current.schema = "public".to_string();
        current.typname = "positive_integer".to_string();
        current.typtype = b'd' as i8;
        current.formatted_basetype = Some("integer".to_string());
        current.typdefault = None;
        current.typnotnull = false;

        let mut target = current.clone();
        target.typdefault = Some("0".to_string());
        target.typnotnull = true;

        let script = current.get_alter_script(&target);
        assert!(script.contains("set default 0"));
        assert!(script.contains("set not null"));
    }
}
