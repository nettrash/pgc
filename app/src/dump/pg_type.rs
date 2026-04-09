use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::postgres::types::Oid;
use std::collections::{BTreeMap, BTreeSet};

use crate::utils::string_extensions::StringExt;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DomainConstraint {
    pub name: String,
    pub definition: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CompositeAttribute {
    pub name: String,
    pub data_type: String,
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
    pub oid: Oid,          // Unique identifier of the type
    pub schema: String,    // Schema where the type is defined
    pub typname: String,   // Name of the type
    pub typnamespace: Oid, // Schema where the type is defined
    pub typowner: Oid,     // Owner of the type
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub owner: String, // Owner role name of the type
    pub typlen: i16,       // Length of the type in bytes
    pub typbyval: bool,    // Whether the type is passed by value
    pub typtype: i8,       // Type of the type (e.g., base, composite, domain)
    pub typcategory: i8,   // Category of the type (e.g., numeric, string)
    pub typispreferred: bool, // Whether the type is preferred for implicit casts
    pub typisdefined: bool, // Whether the type is defined
    pub typdelim: i8,      // Delimiter for array types
    pub typrelid: Option<Oid>, // Type of the type if it is a domain
    pub typsubscript: Option<String>, // Subscript type if it is an array
    pub typelem: Option<Oid>, // Element type if it is an array
    pub typarray: Option<Oid>, // Array type if it is an array
    pub typinput: String,  // Input function for the type
    pub typoutput: String, // Output function for the type
    pub typreceive: Option<String>, // Receive function for the type
    pub typsend: Option<String>, // Send function for the type
    pub typmodin: Option<String>, // Type modifier input function
    pub typmodout: Option<String>, // Type modifier output function
    pub typanalyze: Option<String>, // Analyze function for the type
    pub typalign: i8,      // Alignment of the type (e.g., char, int, double)
    pub typstorage: i8,    // Storage type of the type (e.g., plain, extended)
    pub typnotnull: bool,  // Whether the type is not null
    pub typbasetype: Option<Oid>, // Base type if it is a domain
    pub typtypmod: Option<i32>, // Type modifier for the type
    pub typndims: i32,     // Number of dimensions if it is an array
    pub typcollation: Option<Oid>, // Collation for the type
    pub typdefault: Option<String>, // Default value for the type
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub formatted_basetype: Option<String>, // Human-readable base type (for domains)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub enum_labels: Vec<String>, // Enum labels ordered by sort order
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub domain_constraints: Vec<DomainConstraint>, // Domain constraints (check, etc.)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub composite_attributes: Vec<CompositeAttribute>, // Composite type attributes ordered by attnum
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub range_subtype: Option<String>, // Subtype for range types
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub range_collation: Option<String>, // Collation for range types
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub range_opclass: Option<String>, // Operator class for range types
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub range_canonical: Option<String>, // Canonical function for range types
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub range_subdiff: Option<String>, // Subtype diff function for range types
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub multirange_name: Option<String>, // Multirange type name (for range types)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>, // Optional comment on the type
    pub hash: Option<String>, // SHA256 hash of the type definition
}

impl PgType {
    /// Creates a new `PgType` instance with the provided parameters.
    #[allow(clippy::too_many_arguments)] // Mapping every pg_type column keeps this constructor ergonomic for callers.
    pub fn new(
        oid: Oid,
        schema: String,
        typname: String,
        typnamespace: Oid,
        typowner: Oid,
        owner: String,
        typlen: i16,
        typbyval: bool,
        typtype: i8,
        typcategory: i8,
        typispreferred: bool,
        typisdefined: bool,
        typdelim: i8,
        typrelid: Option<Oid>,
        typsubscript: Option<String>,
        typelem: Option<Oid>,
        typarray: Option<Oid>,
        typinput: String,
        typoutput: String,
        typreceive: Option<String>,
        typsend: Option<String>,
        typmodin: Option<String>,
        typmodout: Option<String>,
        typanalyze: Option<String>,
        typalign: i8,
        typstorage: i8,
        typnotnull: bool,
        typbasetype: Option<Oid>,
        typtypmod: Option<i32>,
        typndims: i32,
        typcollation: Option<Oid>,
        typdefault: Option<String>,
        formatted_basetype: Option<String>,
        enum_labels: Vec<String>,
        domain_constraints: Vec<DomainConstraint>,
        comment: Option<String>,
    ) -> Self {
        let mut pg_type = PgType {
            oid,
            schema,
            typname,
            typnamespace,
            typowner,
            owner,
            typlen,
            typbyval,
            typtype,
            typcategory,
            typispreferred,
            typisdefined,
            typdelim,
            typrelid,
            typsubscript,
            typelem,
            typarray,
            typinput,
            typoutput,
            typreceive,
            typsend,
            typmodin,
            typmodout,
            typanalyze,
            typalign,
            typstorage,
            typnotnull,
            typbasetype,
            typtypmod,
            typndims,
            typcollation,
            typdefault,
            formatted_basetype,
            enum_labels,
            domain_constraints,
            composite_attributes: Vec::new(),
            range_subtype: None,
            range_collation: None,
            range_opclass: None,
            range_canonical: None,
            range_subdiff: None,
            multirange_name: None,
            comment,
            hash: None,
        };
        pg_type.hash();
        pg_type
    }

    /// Computes a SHA256 hash of the type definition.
    ///
    /// This hash can be used for change detection, caching, or verifying the integrity
    /// of the type's metadata. It includes all relevant fields of the `PgType` struct.
    pub fn hash(&mut self) {
        let mut hasher = Sha256::new();

        hasher.update(self.schema.as_bytes());
        hasher.update(self.typname.as_bytes());
        hasher.update(self.owner.as_bytes());
        hasher.update(self.typlen.to_be_bytes());
        hasher.update([self.typbyval as u8]);
        hasher.update(self.typtype.to_be_bytes());
        hasher.update(self.typcategory.to_be_bytes());
        hasher.update([self.typispreferred as u8]);
        hasher.update([self.typisdefined as u8]);
        hasher.update(self.typdelim.to_be_bytes());

        update_option(&mut hasher, &self.typsubscript, |hasher, value| {
            hasher.update(value.as_bytes());
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

        update_option(&mut hasher, &self.formatted_basetype, |hasher, value| {
            hasher.update(value.as_bytes());
        });
        update_option(&mut hasher, &self.typtypmod, |hasher, value| {
            hasher.update(value.to_be_bytes());
        });

        hasher.update(self.typndims.to_be_bytes());

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

        hasher.update((self.composite_attributes.len() as u32).to_be_bytes());
        for attribute in &self.composite_attributes {
            hasher.update((attribute.name.len() as u32).to_be_bytes());
            hasher.update(attribute.name.as_bytes());
            hasher.update((attribute.data_type.len() as u32).to_be_bytes());
            hasher.update(attribute.data_type.as_bytes());
        }

        // Range type fields
        update_option(&mut hasher, &self.range_subtype, |hasher, value| {
            hasher.update(value.as_bytes());
        });
        update_option(&mut hasher, &self.range_collation, |hasher, value| {
            hasher.update(value.as_bytes());
        });
        update_option(&mut hasher, &self.range_opclass, |hasher, value| {
            hasher.update(value.as_bytes());
        });
        update_option(&mut hasher, &self.range_canonical, |hasher, value| {
            hasher.update(value.as_bytes());
        });
        update_option(&mut hasher, &self.range_subdiff, |hasher, value| {
            hasher.update(value.as_bytes());
        });
        update_option(&mut hasher, &self.multirange_name, |hasher, value| {
            hasher.update(value.as_bytes());
        });

        if let Some(comment) = &self.comment {
            hasher.update((comment.len() as u32).to_be_bytes());
            hasher.update(comment.as_bytes());
        }

        self.hash = Some(format!("{:x}", hasher.finalize()));
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

                let mut script = format!(
                    "create type {}.{} as enum ({});",
                    self.schema, self.typname, variants
                )
                .with_empty_lines();

                if let Some(comment) = &self.comment {
                    script.append_block(&format!(
                        "comment on type {}.{} is '{}';",
                        self.schema,
                        self.typname,
                        escape_single_quotes(comment)
                    ));
                }

                script.push_str(&self.get_owner_script());

                script
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

                script.append_block(";");

                for constraint in &self.domain_constraints {
                    script.append_block(&format!(
                        "alter domain {}.{} add constraint {} {};",
                        self.schema,
                        self.typname,
                        quote_ident(&constraint.name),
                        constraint.definition
                    ));
                }
                if let Some(comment) = &self.comment {
                    script.append_block(&format!(
                        "comment on domain {}.{} is '{}';",
                        self.schema,
                        self.typname,
                        escape_single_quotes(comment)
                    ));
                }

                script.push_str(&self.get_owner_script());

                script
            }
            'c' => {
                if self.composite_attributes.is_empty() {
                    return format!(
                        "-- Composite type {}.{} has no attributes available in dump\n",
                        self.schema, self.typname
                    );
                }

                let attributes = self
                    .composite_attributes
                    .iter()
                    .map(|attribute| {
                        format!(
                            "    \"{}\" {}",
                            attribute.name.replace('"', "\"\""),
                            attribute.data_type
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(",\n");

                let mut script = format!(
                    "create type {}.{} as (\n{}\n);",
                    self.schema, self.typname, attributes
                )
                .with_empty_lines();

                if let Some(comment) = &self.comment {
                    script.append_block(&format!(
                        "comment on type {}.{} is '{}';",
                        self.schema,
                        self.typname,
                        escape_single_quotes(comment)
                    ));
                }

                script.push_str(&self.get_owner_script());

                script
            }
            'r' => {
                let mut script = format!(
                    "create type {}.{} as range (\n    subtype = {}",
                    self.schema,
                    self.typname,
                    self.range_subtype.as_deref().unwrap_or("unknown")
                );
                if let Some(collation) = &self.range_collation {
                    script.push_str(&format!(",\n    collation = {}", collation));
                }
                if let Some(opclass) = &self.range_opclass {
                    script.push_str(&format!(",\n    subtype_opclass = {}", opclass));
                }
                if let Some(canonical) = &self.range_canonical {
                    script.push_str(&format!(",\n    canonical = {}", canonical));
                }
                if let Some(subdiff) = &self.range_subdiff {
                    script.push_str(&format!(",\n    subtype_diff = {}", subdiff));
                }
                if let Some(multirange) = &self.multirange_name {
                    script.push_str(&format!(",\n    multirange_type_name = {}", multirange));
                }
                script.push_str("\n)");
                script.append_block(";");

                if let Some(comment) = &self.comment {
                    script.append_block(&format!(
                        "comment on type {}.{} is '{}';",
                        self.schema,
                        self.typname,
                        escape_single_quotes(comment)
                    ));
                }

                script.push_str(&self.get_owner_script());

                script
            }
            'm' => {
                // Multirange types are automatically created with their associated range type.
                // We just emit a comment, the range type CREATE handles them.
                let mut script = format!(
                    "-- Multirange type {}.{} is created automatically with its range type\n",
                    self.schema, self.typname
                );
                if let Some(comment) = &self.comment {
                    script.append_block(&format!(
                        "comment on type {}.{} is '{}';",
                        self.schema,
                        self.typname,
                        escape_single_quotes(comment)
                    ));
                }
                script
            }
            other => format!(
                "-- Type {}.{} (typtype = {}) is not supported yet\n",
                self.schema, self.typname, other
            ),
        }
    }

    /// Returns a statement to drop the user-defined type if it exists.
    pub fn get_drop_script(&self) -> String {
        format!("drop type if exists {}.{};", self.schema, self.typname).with_empty_lines()
    }

    /// Returns a string to alter the existing user-defined type to match the target definition.
    pub fn get_alter_script(&self, target: &PgType, use_drop: bool) -> String {
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

        let mut script = match (self.typtype as u8 as char, target.typtype as u8 as char) {
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

                        statement.append_block(";");
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
                        let drop_cmd = format!(
                            "alter domain {}.{} drop default;",
                            self.schema, self.typname
                        );
                        if use_drop {
                            statements.push(drop_cmd);
                        } else {
                            statements.push(format!("-- {}", drop_cmd));
                        }
                    }
                }

                if self.typnotnull != target.typnotnull {
                    if target.typnotnull {
                        statements.push(format!(
                            "alter domain {}.{} set not null;",
                            self.schema, self.typname
                        ));
                    } else {
                        let drop_cmd = format!(
                            "alter domain {}.{} drop not null;",
                            self.schema, self.typname
                        );
                        if use_drop {
                            statements.push(drop_cmd);
                        } else {
                            statements.push(format!("-- {}", drop_cmd));
                        }
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
                                let drop_cmd = format!(
                                    "alter domain {}.{} drop constraint {};",
                                    self.schema,
                                    self.typname,
                                    quote_ident(name)
                                );
                                let add_cmd = format!(
                                    "alter domain {}.{} add constraint {} {};",
                                    self.schema,
                                    self.typname,
                                    quote_ident(name),
                                    target_constraint.definition
                                );
                                if use_drop {
                                    statements.push(drop_cmd);
                                    statements.push(add_cmd);
                                } else {
                                    statements.push(format!(
                                        "-- use_drop=false: constraint {} requires drop+add; statements commented out (manual intervention needed)",
                                        quote_ident(name)
                                    ));
                                    statements.push(format!("-- {}", drop_cmd));
                                    statements.push(format!("-- {}", add_cmd));
                                }
                                replaced_or_added.insert((*name).to_string());
                            }
                        }
                        None => {
                            let drop_cmd = format!(
                                "alter domain {}.{} drop constraint {};",
                                self.schema,
                                self.typname,
                                quote_ident(name)
                            );
                            if use_drop {
                                statements.push(drop_cmd);
                            } else {
                                statements.push(format!("-- {}", drop_cmd));
                            }
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
                    statements.join("\n\n").with_empty_lines()
                }
            }
            ('r', 'r') => {
                // Range types cannot be altered in place; must be dropped and recreated
                let drop_script = self.get_drop_script();
                let create_script = target.get_script();
                if use_drop {
                    format!("{}{}", drop_script, create_script)
                } else {
                    let commented = format!(
                        "-- use_drop=false: range type {}.{} requires drop+recreate; statements commented out\n{}{}",
                        self.schema,
                        self.typname,
                        drop_script
                            .lines()
                            .map(|l| format!("-- {}\n", l))
                            .collect::<String>(),
                        create_script
                            .lines()
                            .map(|l| format!("-- {}\n", l))
                            .collect::<String>()
                    );
                    commented
                }
            }
            ('m', 'm') => {
                // Multirange types are auto-managed with range types
                format!(
                    "-- Multirange type {}.{} is managed automatically with its range type\n",
                    self.schema, self.typname
                )
            }
            (kind, other) => format!(
                "-- Alter script not implemented for type {}.{} ({} -> {})\n",
                self.schema, self.typname, kind, other
            ),
        };

        if self.comment != target.comment {
            let comment_stmt = if let Some(cmt) = &target.comment {
                format!(
                    "comment on type {}.{} is '{}';",
                    target.schema,
                    target.typname,
                    escape_single_quotes(cmt)
                )
            } else {
                format!(
                    "comment on type {}.{} is null;",
                    target.schema, target.typname
                )
            };
            script.append_block(&comment_stmt);
        }

        if self.owner != target.owner {
            script.push_str(&target.get_owner_script());
        }

        script
    }

    pub fn get_owner_script(&self) -> String {
        if self.owner.is_empty() {
            return String::new();
        }

        let object_keyword = if (self.typtype as u8 as char) == 'd' {
            "domain"
        } else {
            "type"
        };

        format!(
            "alter {} {}.{} owner to {};",
            object_keyword, self.schema, self.typname, self.owner
        )
        .with_empty_lines()
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

    fn base_pg_type(typtype: char) -> PgType {
        PgType {
            oid: Oid(1),
            schema: "public".to_string(),
            typname: "my_type".to_string(),
            typnamespace: Oid(2200),
            typowner: Oid(10),
            owner: String::new(),
            typlen: -1,
            typbyval: false,
            typtype: typtype as i8,
            typcategory: 'U' as i8,
            typispreferred: false,
            typisdefined: true,
            typdelim: ',' as i8,
            typrelid: None,
            typsubscript: None,
            typelem: None,
            typarray: None,
            typinput: "record_in".to_string(),
            typoutput: "record_out".to_string(),
            typreceive: None,
            typsend: None,
            typmodin: None,
            typmodout: None,
            typanalyze: None,
            typalign: 'd' as i8,
            typstorage: 'p' as i8,
            typnotnull: false,
            typbasetype: None,
            typtypmod: None,
            typndims: 0,
            typcollation: None,
            typdefault: None,
            formatted_basetype: None,
            enum_labels: Vec::new(),
            domain_constraints: Vec::new(),
            composite_attributes: Vec::new(),
            range_subtype: None,
            range_collation: None,
            range_opclass: None,
            range_canonical: None,
            range_subdiff: None,
            multirange_name: None,
            comment: None,
            hash: None,
        }
    }

    #[test]
    fn hash_populates_hash_field() {
        let mut pg_type = base_pg_type('e');
        pg_type.enum_labels = vec!["alpha".to_string(), "beta".to_string()];

        pg_type.hash();
        let first = pg_type.hash.clone();

        let value = first.as_ref().expect("hash should be present");
        assert_eq!(value.len(), 64);
        assert!(value.chars().all(|c| c.is_ascii_hexdigit()));

        pg_type.hash();
        assert_eq!(pg_type.hash, first);
    }

    #[test]
    fn hash_differs_when_fields_change() {
        let mut left = base_pg_type('e');
        left.enum_labels = vec!["alpha".to_string()];
        left.hash();
        let left_hash = left.hash.clone().unwrap();

        let mut right = base_pg_type('e');
        right.enum_labels = vec!["alpha".to_string()];
        right.typname = "different".to_string();
        right.hash();
        let right_hash = right.hash.clone().unwrap();

        assert_ne!(left_hash, right_hash);
    }

    #[test]
    fn enum_get_script_generates_create_statement() {
        let mut pg_type = base_pg_type('e');
        pg_type.typname = "status".to_string();
        pg_type.enum_labels = vec!["simple".to_string(), "O'Reilly".to_string()];

        let script = pg_type.get_script();

        assert_eq!(
            script,
            "create type public.status as enum ('simple', 'O''Reilly');\n\n"
        );
    }

    #[test]
    fn enum_get_script_handles_missing_labels() {
        let pg_type = base_pg_type('e');

        let script = pg_type.get_script();

        assert_eq!(
            script,
            "-- Enum public.my_type has no labels available in dump\n"
        );
    }

    #[test]
    fn domain_get_script_includes_constraints() {
        let mut pg_type = base_pg_type('d');
        pg_type.typname = "amount".to_string();
        pg_type.formatted_basetype = Some("integer".to_string());
        pg_type.typdefault = Some("42".to_string());
        pg_type.typnotnull = true;
        pg_type.domain_constraints = vec![DomainConstraint {
            name: "ValueCheck".to_string(),
            definition: "check (value > 0)".to_string(),
        }];

        let script = pg_type.get_script();

        let expected = "create domain public.amount as integer default 42 not null;\n\n\
alter domain public.amount add constraint \"ValueCheck\" check (value > 0);\n\n";
        assert_eq!(script, expected);
    }

    #[test]
    fn get_alter_script_enum_adds_missing_labels() {
        let mut current = base_pg_type('e');
        current.typname = "status".to_string();
        current.enum_labels = vec!["pending".to_string(), "completed".to_string()];

        let mut target = base_pg_type('e');
        target.typname = "status".to_string();
        target.enum_labels = vec![
            "pending".to_string(),
            "in_progress".to_string(),
            "completed".to_string(),
        ];

        let script = current.get_alter_script(&target, true);

        assert_eq!(
            script,
            "alter type public.status add value if not exists 'in_progress' before 'completed';\n\n"
        );
    }

    #[test]
    fn get_alter_script_enum_requires_no_changes() {
        let mut current = base_pg_type('e');
        current.enum_labels = vec!["pending".to_string(), "completed".to_string()];
        let target = current.clone();

        let script = current.get_alter_script(&target, true);

        assert_eq!(script, "-- Enum public.my_type requires no changes.\n");
    }

    #[test]
    fn get_alter_script_domain_handles_changes() {
        let mut current = base_pg_type('d');
        current.typname = "amount".to_string();
        current.formatted_basetype = Some("integer".to_string());
        current.typdefault = Some("42".to_string());
        current.typnotnull = true;
        current.domain_constraints = vec![DomainConstraint {
            name: "ValueCheck".to_string(),
            definition: "check (value > 0)".to_string(),
        }];

        let mut target = current.clone();
        target.typdefault = Some("84".to_string());
        target.typnotnull = false;
        target.domain_constraints = vec![
            DomainConstraint {
                name: "ValueCheck".to_string(),
                definition: "check (value >= 0)".to_string(),
            },
            DomainConstraint {
                name: "FreshConstraint".to_string(),
                definition: "check (value <> 0)".to_string(),
            },
        ];

        let script = current.get_alter_script(&target, true);

        let expected = "alter domain public.amount set default 84;\n\n\
alter domain public.amount drop not null;\n\n\
alter domain public.amount drop constraint \"ValueCheck\";\n\n\
alter domain public.amount add constraint \"ValueCheck\" check (value >= 0);\n\n\
alter domain public.amount add constraint \"FreshConstraint\" check (value <> 0);\n\n";

        assert_eq!(script, expected);
    }

    #[test]
    fn composite_get_script_generates_create_statement() {
        let mut pg_type = base_pg_type('c');
        pg_type.typname = "address_type".to_string();
        pg_type.composite_attributes = vec![
            CompositeAttribute {
                name: "street".to_string(),
                data_type: "varchar(255)".to_string(),
            },
            CompositeAttribute {
                name: "city".to_string(),
                data_type: "varchar(100)".to_string(),
            },
        ];

        let script = pg_type.get_script();

        let expected = "create type public.address_type as (\n    \"street\" varchar(255),\n    \"city\" varchar(100)\n);\n\n";
        assert_eq!(script, expected);
    }

    #[test]
    fn get_drop_script_returns_drop_statement() {
        let pg_type = base_pg_type('e');

        assert_eq!(
            pg_type.get_drop_script(),
            "drop type if exists public.my_type;\n\n"
        );
    }

    #[test]
    fn get_alter_script_includes_owner_change() {
        let mut current = base_pg_type('e');
        current.typname = "status".to_string();
        current.enum_labels = vec!["pending".to_string()];
        current.owner = "old_owner".to_string();

        let mut target = current.clone();
        target.owner = "new_owner".to_string();

        let script = current.get_alter_script(&target, true);

        assert!(script.contains("alter type public.status owner to new_owner;"));
    }

    #[test]
    fn get_alter_script_domain_drop_default_use_drop_false() {
        let mut current = base_pg_type('d');
        current.typname = "amount".to_string();
        current.formatted_basetype = Some("integer".to_string());
        current.typdefault = Some("42".to_string());

        let mut target = current.clone();
        target.typdefault = None;

        let script = current.get_alter_script(&target, false);

        assert!(script.contains("drop default"));
        // The drop default line should be commented out
        for line in script.lines() {
            if line.contains("drop default") {
                assert!(
                    line.starts_with("--"),
                    "drop default should be commented: {}",
                    line
                );
            }
        }
    }

    #[test]
    fn get_alter_script_domain_drop_default_use_drop_true() {
        let mut current = base_pg_type('d');
        current.typname = "amount".to_string();
        current.formatted_basetype = Some("integer".to_string());
        current.typdefault = Some("42".to_string());

        let mut target = current.clone();
        target.typdefault = None;

        let script = current.get_alter_script(&target, true);

        assert!(script.contains("alter domain public.amount drop default;"));
        // Should NOT be commented out
        for line in script.lines() {
            if line.contains("drop default") {
                assert!(
                    !line.starts_with("--"),
                    "drop default should be active: {}",
                    line
                );
            }
        }
    }

    #[test]
    fn get_alter_script_domain_drop_not_null_use_drop_false() {
        let mut current = base_pg_type('d');
        current.typname = "amount".to_string();
        current.formatted_basetype = Some("integer".to_string());
        current.typnotnull = true;

        let mut target = current.clone();
        target.typnotnull = false;

        let script = current.get_alter_script(&target, false);

        assert!(script.contains("drop not null"));
        for line in script.lines() {
            if line.contains("drop not null") {
                assert!(
                    line.starts_with("--"),
                    "drop not null should be commented: {}",
                    line
                );
            }
        }
    }

    #[test]
    fn get_alter_script_domain_drop_not_null_use_drop_true() {
        let mut current = base_pg_type('d');
        current.typname = "amount".to_string();
        current.formatted_basetype = Some("integer".to_string());
        current.typnotnull = true;

        let mut target = current.clone();
        target.typnotnull = false;

        let script = current.get_alter_script(&target, true);

        assert!(script.contains("alter domain public.amount drop not null;"));
        for line in script.lines() {
            if line.contains("drop not null") {
                assert!(
                    !line.starts_with("--"),
                    "drop not null should be active: {}",
                    line
                );
            }
        }
    }

    #[test]
    fn get_alter_script_domain_drop_constraint_use_drop_false() {
        let mut current = base_pg_type('d');
        current.typname = "amount".to_string();
        current.formatted_basetype = Some("integer".to_string());
        current.domain_constraints = vec![DomainConstraint {
            name: "ValueCheck".to_string(),
            definition: "check (value > 0)".to_string(),
        }];

        let mut target = current.clone();
        target.domain_constraints = vec![DomainConstraint {
            name: "ValueCheck".to_string(),
            definition: "check (value >= 0)".to_string(),
        }];

        let script = current.get_alter_script(&target, false);

        // Should contain a warning about manual intervention
        assert!(
            script.contains("use_drop=false") && script.contains("manual intervention needed"),
            "should contain a warning comment, script:\n{}",
            script
        );

        // Both drop and add constraint should be commented out
        for line in script.lines() {
            if line.contains("drop constraint") || line.contains("add constraint") {
                assert!(line.starts_with("--"), "should be commented: {}", line);
            }
        }
    }

    #[test]
    fn get_alter_script_domain_remove_constraint_use_drop_false() {
        let mut current = base_pg_type('d');
        current.typname = "amount".to_string();
        current.formatted_basetype = Some("integer".to_string());
        current.domain_constraints = vec![DomainConstraint {
            name: "OldCheck".to_string(),
            definition: "check (value > 0)".to_string(),
        }];

        let mut target = current.clone();
        target.domain_constraints = vec![];

        let script = current.get_alter_script(&target, false);

        assert!(script.contains("drop constraint"));
        for line in script.lines() {
            if line.contains("drop constraint") {
                assert!(
                    line.starts_with("--"),
                    "drop constraint should be commented: {}",
                    line
                );
            }
        }
    }

    #[test]
    fn get_alter_script_domain_all_drops_use_drop_false() {
        let mut current = base_pg_type('d');
        current.typname = "amount".to_string();
        current.formatted_basetype = Some("integer".to_string());
        current.typdefault = Some("42".to_string());
        current.typnotnull = true;
        current.domain_constraints = vec![DomainConstraint {
            name: "ValueCheck".to_string(),
            definition: "check (value > 0)".to_string(),
        }];

        let mut target = current.clone();
        target.typdefault = Some("84".to_string());
        target.typnotnull = false;
        target.domain_constraints = vec![
            DomainConstraint {
                name: "ValueCheck".to_string(),
                definition: "check (value >= 0)".to_string(),
            },
            DomainConstraint {
                name: "FreshConstraint".to_string(),
                definition: "check (value <> 0)".to_string(),
            },
        ];

        let script = current.get_alter_script(&target, false);

        // set default should still be active (not a drop)
        assert!(script.contains("set default 84"));
        // drop not null should be commented
        for line in script.lines() {
            if line.contains("drop not null") {
                assert!(
                    line.starts_with("--"),
                    "drop not null should be commented: {}",
                    line
                );
            }
        }
        // drop constraint should be commented
        for line in script.lines() {
            if line.contains("drop constraint") {
                assert!(
                    line.starts_with("--"),
                    "drop constraint should be commented: {}",
                    line
                );
            }
        }
        // add constraint for changed constraint should also be commented (depends on drop)
        // but add constraint for new constraint should be active
        assert!(
            script.contains("-- use_drop=false: constraint \"ValueCheck\""),
            "should warn about ValueCheck requiring manual intervention"
        );
        // FreshConstraint is brand new, its add should be active
        for line in script.lines() {
            if line.contains("add constraint") && line.contains("FreshConstraint") {
                assert!(
                    !line.starts_with("--"),
                    "new constraint add should be active: {}",
                    line
                );
            }
        }
    }
}
