//! User-defined types (`pg_type`) — domains, composites, ranges and multiranges,
//! along with their domain constraints and composite attributes.
//!
//! Types are dropped *after* routines and recreated *before* them, since a routine
//! signature can name a type. Multirange types are created and dropped implicitly
//! with their range type and must never be emitted on their own.
//!
//! Altering an existing type is not one step, so [`PgType::get_alter_script_parts`]
//! returns a [`TypeAlterScript`] the comparer runs at three points: enum values a
//! single-transaction migration adds before its `begin;`, the in-place changes, and
//! the domain CHECKs added or validated only after the tables, routines, operators
//! and casts they check against.
//!
//! A composite type is altered attribute by attribute (`ALTER TYPE … ADD / DROP /
//! ALTER ATTRIBUTE`), which has to respect what the migrated database keeps in
//! it: typed tables need `CASCADE`, and a stored column of the type forbids a
//! retype. [`CompositeUsage`] carries what the dump recorded about both. What
//! cannot be migrated is reported both in the script and, through
//! [`TypeAlterScript::warnings`], on stderr, so that it does not pass silently
//! when comments are stripped (`USE_COMMENTS=false`).

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::postgres::types::Oid;
use std::collections::{BTreeMap, BTreeSet};

use crate::utils::string_extensions::{StringExt, comment_out, line_comment_safe};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DomainConstraint {
    pub name: String,
    pub definition: String,
}

/// One attribute of a composite type (`pg_attribute` of its `pg_class` row).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CompositeAttribute {
    pub name: String,
    /// `format_type(atttypid, atttypmod)`, without a collation.
    pub data_type: String,
    /// The attribute's own collation (`attcollation`), qualified and quoted
    /// (`pg_catalog."C"`), when it is not its data type's default: `text
    /// collate "C"`, but not a plain `text`, nor a domain carrying its own
    /// collation. `None` otherwise, and in a dump written by pgc 1.0.x, which
    /// did not record it. Hashed only when set, so every other type keeps the
    /// hash pgc 1.0.x gave it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub collation: Option<String>,
}

impl CompositeAttribute {
    /// The attribute's type as `CREATE TYPE`, `ADD ATTRIBUTE` and `ALTER
    /// ATTRIBUTE … SET DATA TYPE` take it: [`CompositeAttribute::data_type`],
    /// then ` collate <collation>` when it has one of its own.
    pub fn type_clause(&self) -> String {
        match &self.collation {
            Some(collation) => format!("{} collate {collation}", self.data_type),
            None => self.data_type.clone(),
        }
    }
}

/// The migration of one type to its target definition, split by *when* a
/// migration has to run each statement (see [`PgType::get_alter_script_parts`]).
///
/// Every part is either empty or a run of blocks that each end in a blank
/// line, as [`PgType::get_alter_script`] has always returned them.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TypeAlterScript {
    /// `alter type … add value if not exists` statements. A value added
    /// inside a transaction block cannot be used until that transaction
    /// commits, so a single-transaction migration runs these before its
    /// `begin;`; they are idempotent, so they replay safely.
    pub add_values: String,
    /// Everything that runs where the type is compared, before the tables,
    /// routines and casts: domain default / not-null changes, the dropping
    /// half of a changed domain CHECK, comment and owner, and notes.
    pub in_place: String,
    /// Domain `add constraint` and `validate constraint`. Both scan every
    /// stored value of the domain — including columns and tables the same
    /// migration drops later — and a CHECK may call a routine, operator or
    /// cast the migration creates later, so they run after all of those.
    pub deferred: String,
    /// Differences this alteration leaves in place, one line each, without a
    /// `Warning: ` prefix. The script carries each as a comment too, but
    /// `USE_COMMENTS=false` strips those, so the comparer also prints these
    /// on stderr.
    pub warnings: Vec<String>,
}

/// What altering a composite type in place has to respect in the migrated
/// database (see [`PgType::get_alter_script_parts_with`]).
///
/// Checked live on PostgreSQL 14 and 19: `ADD` and `DROP ATTRIBUTE` work
/// while table columns store values of the type (the stored rows read a new
/// attribute as NULL), but `ALTER ATTRIBUTE … TYPE` is refused ("cannot alter
/// type … because column … uses it"), through an array, a domain or another
/// composite type too. A typed table makes all three fail unless they say
/// `CASCADE`, which then alters the typed tables with the type.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CompositeUsage {
    /// The typed tables of the type (`CREATE TABLE … OF`), qualified.
    pub typed_tables: Vec<String>,
    /// The columns that store values of the type, as `schema.relation.column`;
    /// `None` when that is not known (a dump written by pgc 1.0.x), in which
    /// case a retype is emitted and PostgreSQL decides, and attributes are
    /// never dropped and re-added to reorder them.
    pub stored_in: Option<Vec<String>>,
}

impl CompositeUsage {
    /// What the dump recorded about `pg_type` ([`PgType::typed_tables`] and
    /// [`PgType::composite_stored_in`]): no typed tables and unknown stored
    /// columns for a dump written by pgc 1.0.x.
    pub fn recorded(pg_type: &PgType) -> Self {
        Self {
            typed_tables: pg_type.typed_tables.clone().unwrap_or_default(),
            stored_in: pg_type.composite_stored_in.clone(),
        }
    }

    /// Whether the migrated database is known to keep no value of the type:
    /// no typed table and no column stores one, so dropping and re-adding an
    /// attribute loses nothing.
    fn holds_no_values(&self) -> bool {
        self.typed_tables.is_empty() && self.stored_in.as_ref().is_some_and(Vec::is_empty)
    }

    /// Why the migrated database may hold values of the type, for a note.
    fn why_values_may_exist(&self) -> String {
        if !self.typed_tables.is_empty() {
            format!(
                "typed table(s) {} hold its rows",
                self.typed_tables.join(", ")
            )
        } else {
            match &self.stored_in {
                Some(columns) if !columns.is_empty() => {
                    format!("column(s) {} store values of it", columns.join(", "))
                }
                Some(_) => "nothing stores values of it".to_string(),
                None => "the FROM dump (written by pgc 1.0.x) does not record which columns \
                         store values of it"
                    .to_string(),
            }
        }
    }
}

fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

fn escape_single_quotes(value: &str) -> String {
    value.replace('\'', "''")
}

/// This is an information about a PostgreSQL type.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PgType {
    /// Unique identifier of the type
    pub oid: Oid,
    /// Schema where the type is defined
    pub schema: String,
    /// Name of the type
    pub typname: String,
    /// Schema where the type is defined
    pub typnamespace: Oid,
    /// Owner of the type
    pub typowner: Oid,
    /// Owner role name of the type
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub owner: String,
    /// Length of the type in bytes
    pub typlen: i16,
    /// Whether the type is passed by value
    pub typbyval: bool,
    /// Type of the type (e.g., base, composite, domain)
    pub typtype: i8,
    /// Category of the type (e.g., numeric, string)
    pub typcategory: i8,
    /// Whether the type is preferred for implicit casts
    pub typispreferred: bool,
    /// Whether the type is defined
    pub typisdefined: bool,
    /// Delimiter for array types
    pub typdelim: i8,
    /// Type of the type if it is a domain
    pub typrelid: Option<Oid>,
    /// Subscript type if it is an array
    pub typsubscript: Option<String>,
    /// Element type if it is an array
    pub typelem: Option<Oid>,
    /// Array type if it is an array
    pub typarray: Option<Oid>,
    /// Input function for the type
    pub typinput: String,
    /// Output function for the type
    pub typoutput: String,
    /// Receive function for the type
    pub typreceive: Option<String>,
    /// Send function for the type
    pub typsend: Option<String>,
    /// Type modifier input function
    pub typmodin: Option<String>,
    /// Type modifier output function
    pub typmodout: Option<String>,
    /// Analyze function for the type
    pub typanalyze: Option<String>,
    /// Alignment of the type (e.g., char, int, double)
    pub typalign: i8,
    /// Storage type of the type (e.g., plain, extended)
    pub typstorage: i8,
    /// Whether the type is not null
    pub typnotnull: bool,
    /// Base type if it is a domain
    pub typbasetype: Option<Oid>,
    /// Type modifier for the type
    pub typtypmod: Option<i32>,
    /// Number of dimensions if it is an array
    pub typndims: i32,
    /// Collation for the type
    pub typcollation: Option<Oid>,
    /// Default value for the type
    pub typdefault: Option<String>,
    /// Human-readable base type (for domains)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub formatted_basetype: Option<String>,
    /// Enum labels ordered by sort order
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub enum_labels: Vec<String>,
    /// Domain constraints (check, etc.)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub domain_constraints: Vec<DomainConstraint>,
    /// Composite type attributes ordered by attnum
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub composite_attributes: Vec<CompositeAttribute>,
    /// Subtype for range types
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub range_subtype: Option<String>,
    /// Collation for range types
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub range_collation: Option<String>,
    /// Operator class for range types
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub range_opclass: Option<String>,
    /// Canonical function for range types
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub range_canonical: Option<String>,
    /// Subtype diff function for range types
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub range_subdiff: Option<String>,
    /// Multirange type name (for range types), `quote_ident`ed, without its
    /// schema: see [`PgType::multirange_schema`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub multirange_name: Option<String>,
    /// Schema of the multirange type (for range types), `quote_ident`ed.
    /// Usually the range's own schema, but pgc before 1.1.0 created every
    /// multirange unqualified, so in the migration role's `search_path`
    /// schema. Not hashed, so every range type keeps the hash pgc 1.0.x gave
    /// it; the comparer moves a misplaced multirange on its own
    /// ([`PgType::get_multirange_set_schema_script`]). `None` for other types
    /// and in a dump written by pgc 1.0.x.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub multirange_schema: Option<String>,
    /// Resolved collation name for domain types
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub domain_collation_name: Option<String>,
    /// Optional comment on the type
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    /// ACL entries for GRANT/REVOKE
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub acl: Vec<String>,
    /// `typacl` is an empty array rather than NULL: every privilege was
    /// revoked, the owner's own included. Both dump as an empty
    /// [`PgType::acl`], but NULL stands for the default privileges, which give
    /// PUBLIC `USAGE`, so the grants comparison has to tell them apart
    /// (`acl::effective_acl`). Not hashed, so every type keeps the hash it
    /// had: the two hashed alike before. Absent (`false`) from dumps written
    /// by pgc 1.0.x, which could not tell them apart.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub acl_explicitly_empty: bool,
    /// For a composite type: its typed tables (`CREATE TABLE … OF`), as
    /// `schema.table`, in any schema — see [`CompositeUsage::typed_tables`].
    /// Not hashed. `None` for other types and in a dump written by pgc 1.0.x.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub typed_tables: Option<Vec<String>>,
    /// For a composite type: the table and materialized-view columns, in any
    /// schema, that store values of it — directly, or through an array, a
    /// domain or another composite type, as PostgreSQL's own check follows
    /// them — as `schema.relation.column`; see
    /// [`CompositeUsage::stored_in`]. Not hashed. `None` for other types and
    /// in a dump written by pgc 1.0.x.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub composite_stored_in: Option<Vec<String>>,
    /// SHA256 hash of the type definition
    pub hash: Option<String>,
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
            multirange_schema: None,
            domain_collation_name: None,
            comment,
            acl: Vec::new(),
            acl_explicitly_empty: false,
            typed_tables: None,
            composite_stored_in: None,
            hash: None,
        };
        pg_type.hash();
        pg_type
    }

    /// Computes a SHA256 hash of the type definition.
    ///
    /// This hash can be used for change detection, caching, or verifying the integrity
    /// of the type's metadata. It includes all relevant fields of the `PgType` struct.
    ///
    /// Call it only once [`enum_labels`](Self::enum_labels) and
    /// [`domain_constraints`](Self::domain_constraints) are filled in: the dump
    /// loads both after the type rows, and a hash taken before them misses
    /// every enum-label-only and domain-CHECK-only change. pgc 1.0.x did
    /// exactly that, so [`Dump::read_from_file`](crate::dump::core::Dump::read_from_file)
    /// rehashes every type it loads.
    ///
    /// Enum labels are hashed in their stored (`enumsortorder`) order, because
    /// label order is part of an enum's definition. Domain constraints are
    /// hashed in name order whatever order they are stored in: a domain's
    /// constraints are a set keyed by name, and every consumer
    /// (`get_alter_script`) matches them by name.
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

        // Name order, not storage order. A live dump already stores them
        // sorted by name, so this changes no hash pgc has ever written; it
        // only stops a dump holding the same set in another order from
        // reading as changed.
        let mut constraints: Vec<&DomainConstraint> = self.domain_constraints.iter().collect();
        constraints.sort_by(|a, b| {
            a.name
                .cmp(&b.name)
                .then_with(|| a.definition.cmp(&b.definition))
        });
        hasher.update((constraints.len() as u32).to_be_bytes());
        for constraint in constraints {
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
            // Only an attribute with a collation of its own: every other one
            // hashes as it always has.
            if let Some(collation) = &attribute.collation {
                hasher.update(b"collate");
                hasher.update((collation.len() as u32).to_be_bytes());
                hasher.update(collation.as_bytes());
            }
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
        update_option(&mut hasher, &self.domain_collation_name, |hasher, value| {
            hasher.update(value.as_bytes());
        });

        if let Some(comment) = &self.comment {
            hasher.update((comment.len() as u32).to_be_bytes());
            hasher.update(comment.as_bytes());
        }

        hasher.update((self.acl.len() as u32).to_be_bytes());
        for entry in &self.acl {
            hasher.update((entry.len() as u32).to_be_bytes());
            hasher.update(entry.as_bytes());
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
                        line_comment_safe(&self.schema),
                        line_comment_safe(&self.typname)
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

                if let Some(collation) = &self.domain_collation_name {
                    clauses.push(format!("collate {}", collation));
                }

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
                        line_comment_safe(&self.schema),
                        line_comment_safe(&self.typname)
                    );
                }

                let attributes = self
                    .composite_attributes
                    .iter()
                    .map(|attribute| {
                        format!(
                            "    \"{}\" {}",
                            attribute.name.replace('"', "\"\""),
                            attribute.type_clause()
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
                    // Qualified: PostgreSQL creates an unqualified
                    // multirange_type_name in the creation schema of
                    // `search_path`, not in the range's schema.
                    script.push_str(&format!(
                        ",\n    multirange_type_name = {}.{}",
                        self.multirange_schema.as_deref().unwrap_or(&self.schema),
                        multirange
                    ));
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
                    line_comment_safe(&self.schema),
                    line_comment_safe(&self.typname)
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
                line_comment_safe(&self.schema),
                line_comment_safe(&self.typname),
                other
            ),
        }
    }

    /// Why [`PgType::get_script`] leaves this type out of the migration with
    /// only a comment — an enum with no labels, a composite type with no
    /// attributes, a kind pgc does not create — as a stderr warning; `None`
    /// when it creates it (a multirange comes with its range type).
    pub fn create_warning(&self) -> Option<String> {
        let reason = match self.typtype as u8 as char {
            'e' if self.enum_labels.is_empty() => "the dump holds no labels for this enum",
            'c' if self.composite_attributes.is_empty() => {
                "the dump holds no attributes for this composite type"
            }
            'e' | 'd' | 'c' | 'r' | 'm' => return None,
            _ => "pgc does not create types of this kind",
        };
        Some(format!(
            "type {}.{} (typtype {}) is not created: {reason} (manual intervention needed).",
            self.schema, self.typname, self.typtype as u8 as char
        ))
    }

    /// `alter domain … drop constraint if exists` followed by `alter domain …
    /// add constraint` for the CHECK `constraint` of this domain: how the
    /// comparer puts back a CHECK that a routine's `DROP … CASCADE` took with
    /// it. The drop is guarded because the comparer finds such a CHECK by
    /// reading the routine's name in its text, which cannot tell overloads
    /// apart: when the CHECK called another overload and so survived, the pair
    /// re-creates it rather than failing on an existing name.
    pub fn get_domain_constraint_recreate_script(&self, constraint: &DomainConstraint) -> String {
        format!(
            "alter domain {}.{} drop constraint if exists {};\n\nalter domain {}.{} add constraint {} {};",
            self.schema,
            self.typname,
            quote_ident(&constraint.name),
            self.schema,
            self.typname,
            quote_ident(&constraint.name),
            constraint.definition
        )
        .with_empty_lines()
    }

    /// `alter domain … drop default;` for this domain.
    ///
    /// PostgreSQL records a domain's `DEFAULT` expression as a dependency of
    /// the domain *itself*, so `DROP FUNCTION … CASCADE` of a routine the
    /// default calls drops the whole domain and, with it, every table column
    /// of that type and the data in them. The comparer detaches such a
    /// default right before the drop and sets it again afterwards
    /// ([`PgType::get_set_default_script`]).
    pub fn get_drop_default_script(&self) -> String {
        format!(
            "alter domain {}.{} drop default;",
            self.schema, self.typname
        )
        .with_empty_lines()
    }

    /// `alter domain … set default …;` with this domain's default, or `None`
    /// when it has none.
    pub fn get_set_default_script(&self) -> Option<String> {
        self.typdefault
            .as_deref()
            .filter(|default| !default.trim().is_empty())
            .map(|default| {
                format!(
                    "alter domain {}.{} set default {};",
                    self.schema, self.typname, default
                )
                .with_empty_lines()
            })
    }

    /// Whether altering this domain into `target` leaves its CHECK `name` in
    /// place: both sides have it, with the same text or with this side still
    /// `NOT VALID` (validated, not re-created — see
    /// [`PgType::get_alter_script_parts`]). Any other difference drops the
    /// CHECK early and adds the target's text back late, after every routine
    /// is in place.
    pub fn keeps_domain_constraint(&self, target: &PgType, name: &str) -> bool {
        let current = self.domain_constraints.iter().find(|c| c.name == name);
        let wanted = target.domain_constraints.iter().find(|c| c.name == name);
        match (current, wanted) {
            (Some(current), Some(wanted)) => {
                current.definition == wanted.definition
                    || only_validation_differs(&current.definition, &wanted.definition)
            }
            _ => false,
        }
    }

    /// `alter type … set schema …` moving this range type's multirange into
    /// the schema `target`'s multirange is in, when the two are the same range
    /// with the same multirange name and only its schema differs; `None`
    /// otherwise, and when either dump did not record the schema (pgc 1.0.x).
    ///
    /// pgc before 1.1.0 created every multirange with an unqualified
    /// `multirange_type_name`, which PostgreSQL puts in the creation schema of
    /// `search_path` (usually `public`) rather than the range's, and nothing
    /// ever moved it back. `ALTER TYPE … SET SCHEMA` works on a multirange
    /// (checked on PostgreSQL 14 and 19), so a misplaced one is moved without
    /// recreating the range, which would drop every column of it.
    pub fn get_multirange_set_schema_script(&self, target: &PgType) -> Option<String> {
        if (self.typtype as u8 as char) != 'r'
            || (target.typtype as u8 as char) != 'r'
            || self.schema != target.schema
            || self.typname != target.typname
            || self.multirange_name != target.multirange_name
        {
            return None;
        }
        let name = self.multirange_name.as_deref()?;
        let current = self.multirange_schema.as_deref()?;
        let wanted = target.multirange_schema.as_deref()?;
        (current != wanted)
            .then(|| format!("alter type {current}.{name} set schema {wanted};").with_empty_lines())
    }

    /// Whether `name` — a type as `format_type` or `::regtype` renders it,
    /// possibly with array brackets — names this type: qualified, or bare for
    /// a type in `public`, which is on the dumping session's `search_path`.
    pub fn is_named_by(&self, name: &str) -> bool {
        let name = name.trim().trim_end_matches("[]");
        name == format!("{}.{}", self.schema, self.typname)
            || (self.schema == "public" && name == self.typname)
    }

    /// Returns a statement to drop the user-defined type if it exists.
    pub fn get_drop_script(&self) -> String {
        format!(
            "drop type if exists {}.{} cascade;",
            self.schema, self.typname
        )
        .with_empty_lines()
    }

    /// Returns a string to alter the existing user-defined type to match the target definition.
    ///
    /// All of [`get_alter_script_parts`](Self::get_alter_script_parts) in one
    /// string — `add_values`, then `in_place`, then `deferred` — for a caller
    /// that applies them at one point. The comparer does not: it runs each
    /// part where the migration needs it.
    pub fn get_alter_script(&self, target: &PgType, use_drop: bool) -> String {
        let parts = self.get_alter_script_parts(target, use_drop);
        format!("{}{}{}", parts.add_values, parts.in_place, parts.deferred)
    }

    /// Whether this type and `other` differ in more than their ACLs.
    ///
    /// [`PgType::hash`] folds in the ACL, but privileges are migrated by
    /// `GRANT` / `REVOKE`, not by altering (or, for a range type, dropping and
    /// recreating) the type. Their raw text also cannot always converge: once
    /// any `GRANT` or `REVOKE` has run on a type, its NULL ACL is replaced by
    /// an explicit one, which says the same as NULL but never hashes like it.
    /// Both sides are hashed again without the ACL, so the stored hashes stay
    /// as they are.
    pub fn differs_beyond_acl(&self, other: &PgType) -> bool {
        let without_acl = |t: &PgType| {
            let mut t = t.clone();
            t.acl.clear();
            t.hash();
            t.hash
        };
        without_acl(self) != without_acl(other)
    }

    /// Whether this type and `other` differ in anything but their owner,
    /// comment and ACL: what [`PgType::hash`] covers, with those three taken
    /// from `other`.
    fn differs_beyond_owner_and_comment(&self, other: &PgType) -> bool {
        let rehashed = |t: &PgType| {
            let mut t = t.clone();
            t.owner = other.owner.clone();
            t.comment = other.comment.clone();
            t.acl = other.acl.clone();
            t.acl_explicitly_empty = other.acl_explicitly_empty;
            t.hash();
            t.hash
        };
        rehashed(self) != rehashed(other)
    }

    /// Whether [`get_alter_script_parts`](Self::get_alter_script_parts) turns
    /// this type into a different `target` by dropping it and creating
    /// `target` (with `use_drop`), rather than altering it in place: true for
    /// a range type, which PostgreSQL cannot alter. The recreated type loses
    /// its ACL with the `DROP`.
    pub fn alter_recreates(&self, target: &PgType) -> bool {
        self.schema == target.schema
            && self.typname == target.typname
            && (self.typtype as u8 as char) == 'r'
            && (target.typtype as u8 as char) == 'r'
    }

    /// The statements that alter this type to match `target`, split by when
    /// a migration has to run them (see [`TypeAlterScript`]).
    ///
    /// Domain CHECK constraints are matched by name. PostgreSQL cannot change
    /// a constraint's expression in place, so a changed one is dropped (in
    /// place) and re-added (deferred); `NOT VALID` travels inside the
    /// `pg_get_constraintdef` text. One that differs only in being `NOT
    /// VALID` on this side is validated (deferred) instead. With `use_drop`
    /// off, every drop — and the re-add that depends on it — is emitted
    /// commented out, line by line, in place.
    ///
    /// Enum labels missing from this side are added with `add value if not
    /// exists` (`add_values`). A label only this side has, and a difference
    /// in the order of the labels both sides have, get a note instead:
    /// PostgreSQL can neither remove nor reorder an enum value.
    ///
    /// A composite type is altered with what the dump recorded about its
    /// typed tables and stored columns ([`CompositeUsage::recorded`]); the
    /// comparer passes what it knows through
    /// [`get_alter_script_parts_with`](Self::get_alter_script_parts_with).
    ///
    /// Every difference left in place is also listed in
    /// [`TypeAlterScript::warnings`].
    pub fn get_alter_script_parts(&self, target: &PgType, use_drop: bool) -> TypeAlterScript {
        self.get_alter_script_parts_with(target, use_drop, &CompositeUsage::recorded(self))
    }

    /// [`get_alter_script_parts`](Self::get_alter_script_parts), with
    /// `usage` saying what the migrated database keeps in this type when it
    /// is a composite type (ignored for every other kind).
    ///
    /// Composite attributes are matched by name, so a renamed attribute is
    /// dropped and added (its stored values are lost; nothing tells a rename
    /// apart). One only this side has is dropped (commented out without
    /// `use_drop`), one only `target` has is added, and one whose type differs
    /// — its collation included ([`CompositeAttribute::type_clause`]) — is
    /// retyped with `ALTER ATTRIBUTE … SET DATA TYPE` — unless `usage` says
    /// columns store values of the type, which PostgreSQL refuses, and a note
    /// is left instead. Every statement says `CASCADE` when the type has
    /// typed tables, so that they follow.
    ///
    /// `ADD ATTRIBUTE` appends, so an attribute `target` adds anywhere but at
    /// the end, or a different order of the attributes both sides have, is
    /// reached by dropping and re-adding every attribute from the first one
    /// out of place — only when that loses nothing (`use_drop`, no typed
    /// table and no stored column); otherwise the order is left as it is,
    /// with a note.
    pub fn get_alter_script_parts_with(
        &self,
        target: &PgType,
        use_drop: bool,
        usage: &CompositeUsage,
    ) -> TypeAlterScript {
        let qualified = format!("{}.{}", self.schema, self.typname);
        if self.schema != target.schema || self.typname != target.typname {
            return TypeAlterScript {
                in_place: format!(
                    "-- Cannot alter type {}.{} because target is {}.{}\n",
                    line_comment_safe(&self.schema),
                    line_comment_safe(&self.typname),
                    line_comment_safe(&target.schema),
                    line_comment_safe(&target.typname)
                ),
                warnings: vec![format!(
                    "type {qualified} cannot be altered into {}.{}; left unchanged.",
                    target.schema, target.typname
                )],
                ..TypeAlterScript::default()
            };
        }

        if self.typtype != target.typtype {
            let (from_kind, to_kind) = (self.typtype as u8 as char, target.typtype as u8 as char);
            return TypeAlterScript {
                in_place: format!(
                    "-- Cannot change type kind of {}.{} ({} -> {})\n",
                    line_comment_safe(&self.schema),
                    line_comment_safe(&self.typname),
                    from_kind,
                    to_kind
                ),
                warnings: vec![format!(
                    "type {qualified} changes its kind (typtype {from_kind} -> {to_kind}), which \
                     pgc does not migrate; drop and re-create it by hand (manual intervention needed)."
                )],
                ..TypeAlterScript::default()
            };
        }

        let mut add_values = String::new();
        let mut deferred = String::new();
        let mut warnings = Vec::new();
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
                        add_values.push_str(&statement);
                        known_labels.push(label.clone());
                    }
                }

                for label in &self.enum_labels {
                    if !target.enum_labels.contains(label) {
                        script.push_str(&format!(
                            "-- Enum {}.{} cannot automatically remove value '{}'.\n",
                            line_comment_safe(&self.schema),
                            line_comment_safe(&self.typname),
                            line_comment_safe(&escape_single_quotes(label))
                        ));
                        warnings.push(format!(
                            "enum {qualified} keeps value '{}', which the target does not have: \
                             PostgreSQL cannot remove an enum value (manual intervention needed).",
                            escape_single_quotes(label)
                        ));
                    }
                }

                // The labels both sides have, in each side's order. Order is
                // part of an enum's definition (it drives comparison and
                // sorting), and no ALTER TYPE can change it.
                let current_order: Vec<&String> = self
                    .enum_labels
                    .iter()
                    .filter(|label| target.enum_labels.contains(label))
                    .collect();
                let target_order: Vec<&String> = target
                    .enum_labels
                    .iter()
                    .filter(|label| self.enum_labels.contains(label))
                    .collect();
                if current_order != target_order {
                    let quoted = |labels: &[&String]| {
                        labels
                            .iter()
                            .map(|label| format!("'{}'", escape_single_quotes(label)))
                            .collect::<Vec<_>>()
                            .join(", ")
                    };
                    // Every line commented: a label may hold a newline.
                    let note = format!(
                        "Enum {}.{} has its values in a different order ({}) than the target ({}); PostgreSQL cannot reorder enum values (manual intervention needed).",
                        self.schema,
                        self.typname,
                        quoted(&current_order),
                        quoted(&target_order)
                    );
                    script.push_str(&comment_out_joined(&note));
                    script.push('\n');
                    warnings.push(note);
                }

                if script.is_empty() && add_values.is_empty() {
                    format!(
                        "-- Enum {}.{} requires no changes.\n",
                        line_comment_safe(&self.schema),
                        line_comment_safe(&self.typname)
                    )
                } else {
                    script
                }
            }
            ('d', 'd') => {
                let mut statements = Vec::new();

                if self.formatted_basetype != target.formatted_basetype {
                    let (current, wanted) = (
                        self.formatted_basetype.as_deref().unwrap_or("unknown"),
                        target.formatted_basetype.as_deref().unwrap_or("unknown"),
                    );
                    statements.push(format!(
                        "-- Changing base type of domain {}.{} ({} -> {}) is not supported automatically.\n",
                        line_comment_safe(&self.schema),
                        line_comment_safe(&self.typname),
                        line_comment_safe(current),
                        line_comment_safe(wanted)
                    ));
                    warnings.push(format!(
                        "domain {qualified} changes its base type ({current} -> {wanted}), which \
                         pgc does not migrate (manual intervention needed)."
                    ));
                }

                // PostgreSQL has no `ALTER DOMAIN … COLLATE`.
                if self.domain_collation_name != target.domain_collation_name {
                    let (current, wanted) = (
                        self.domain_collation_name.as_deref().unwrap_or("default"),
                        target.domain_collation_name.as_deref().unwrap_or("default"),
                    );
                    statements.push(format!(
                        "-- Changing the collation of domain {}.{} ({} -> {}) is not supported automatically.\n",
                        line_comment_safe(&self.schema),
                        line_comment_safe(&self.typname),
                        line_comment_safe(current),
                        line_comment_safe(wanted)
                    ));
                    warnings.push(format!(
                        "domain {qualified} changes its collation ({current} -> {wanted}), which \
                         pgc does not migrate: PostgreSQL has no ALTER DOMAIN ... COLLATE (manual \
                         intervention needed)."
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
                            statements.push(comment_out_joined(&drop_cmd));
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
                            statements.push(comment_out_joined(&drop_cmd));
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
                // `add constraint` / `validate constraint`: both check stored
                // data, so they go to `deferred` (see `TypeAlterScript`).
                let mut checks = Vec::new();

                for (name, current_constraint) in &current_constraints {
                    match target_constraints.get(name) {
                        Some(target_constraint)
                            if only_validation_differs(
                                &current_constraint.definition,
                                &target_constraint.definition,
                            ) =>
                        {
                            // Same CHECK, NOT VALID on the migrated side only:
                            // validate it without re-creating it. Nothing is
                            // dropped, so this runs whatever `use_drop` says.
                            checks.push(format!(
                                "alter domain {}.{} validate constraint {};",
                                self.schema,
                                self.typname,
                                quote_ident(name)
                            ));
                            replaced_or_added.insert((*name).to_string());
                        }
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
                                    checks.push(add_cmd);
                                } else {
                                    statements.push(comment_out_joined(&format!(
                                        "use_drop=false: constraint {} requires drop+add; statements commented out (manual intervention needed)",
                                        quote_ident(name)
                                    )));
                                    statements.push(comment_out_joined(&drop_cmd));
                                    statements.push(comment_out_joined(&add_cmd));
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
                                statements.push(comment_out_joined(&drop_cmd));
                            }
                        }
                    }
                }

                for (name, target_constraint) in &target_constraints {
                    if replaced_or_added.contains(*name) {
                        continue;
                    }

                    if !current_constraints.contains_key(name) {
                        checks.push(format!(
                            "alter domain {}.{} add constraint {} {};",
                            self.schema,
                            self.typname,
                            quote_ident(name),
                            target_constraint.definition
                        ));
                    }
                }

                if !checks.is_empty() {
                    deferred = checks.join("\n\n").with_empty_lines();
                }
                if !statements.is_empty() {
                    statements.join("\n\n").with_empty_lines()
                } else if checks.is_empty() {
                    // Nothing above covers the difference. An owner or
                    // comment change is emitted below; anything else is left
                    // in place, and says so.
                    if self.differs_beyond_owner_and_comment(target) {
                        warnings.push(format!(
                            "domain {qualified} differs from the target in a way pgc cannot alter \
                             (manual intervention needed)."
                        ));
                    }
                    format!(
                        "-- Domain {}.{} requires no supported changes.\n",
                        line_comment_safe(&self.schema),
                        line_comment_safe(&self.typname)
                    )
                } else {
                    String::new()
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
                        line_comment_safe(&self.schema),
                        line_comment_safe(&self.typname),
                        comment_out(&drop_script),
                        comment_out(&create_script)
                    );
                    commented
                }
            }
            ('m', 'm') => {
                // Multirange types are auto-managed with range types
                format!(
                    "-- Multirange type {}.{} is managed automatically with its range type\n",
                    line_comment_safe(&self.schema),
                    line_comment_safe(&self.typname)
                )
            }
            ('c', 'c') => self.composite_alter_script(target, use_drop, usage, &mut warnings),
            (kind, other) => {
                warnings.push(format!(
                    "type {qualified} (typtype {kind}) differs from the target, and pgc cannot \
                     alter a type of this kind (manual intervention needed)."
                ));
                format!(
                    "-- Alter script not implemented for type {}.{} ({} -> {})\n",
                    line_comment_safe(&self.schema),
                    line_comment_safe(&self.typname),
                    kind,
                    other
                )
            }
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

        TypeAlterScript {
            add_values,
            in_place: script,
            deferred,
            warnings,
        }
    }

    /// The order the plain `DROP` and `ADD ATTRIBUTE`s turning this composite
    /// type into `target` leave its attributes in — the attributes both sides
    /// have, in this side's order, then the added ones, appended — and the
    /// first position where `target` wants another attribute, if any.
    fn composite_appended_order<'a>(&'a self, target: &'a PgType) -> (Vec<&'a str>, Option<usize>) {
        let current = &self.composite_attributes;
        let wanted = &target.composite_attributes;
        let has = |attributes: &[CompositeAttribute], name: &str| {
            attributes.iter().any(|a| a.name == name)
        };
        let appended: Vec<&str> = current
            .iter()
            .filter(|a| has(wanted, &a.name))
            .chain(wanted.iter().filter(|a| !has(current, &a.name)))
            .map(|a| a.name.as_str())
            .collect();
        let out_of_place = wanted.iter().zip(&appended).position(|(w, a)| w.name != *a);
        (appended, out_of_place)
    }

    /// The attributes both this composite type and `target` have that
    /// [`get_alter_script_parts_with`](Self::get_alter_script_parts_with)
    /// drops and adds again to reach `target`'s order: those from the first
    /// one out of place on, and only when that loses nothing (`use_drop`, no
    /// typed table and no stored column, see `usage`).
    fn composite_readded_attributes<'a>(
        &'a self,
        target: &'a PgType,
        use_drop: bool,
        usage: &CompositeUsage,
    ) -> BTreeSet<&'a str> {
        let (_, out_of_place) = self.composite_appended_order(target);
        match out_of_place {
            Some(at) if use_drop && usage.holds_no_values() => target.composite_attributes[at..]
                .iter()
                .filter(|a| self.composite_attributes.iter().any(|c| c.name == a.name))
                .map(|a| a.name.as_str())
                .collect(),
            _ => BTreeSet::new(),
        }
    }

    /// The attributes, in this side's order, whose type — collation included
    /// ([`CompositeAttribute::type_clause`]) — the migration of this composite
    /// type into `target` changes with a live `ALTER ATTRIBUTE … SET DATA
    /// TYPE` (see [`get_alter_script_parts_with`](Self::get_alter_script_parts_with)):
    /// neither dropped and re-added to reorder the attributes, nor left in
    /// place, commented out, because `usage` says columns store values of
    /// the type. With typed tables (`usage`) that statement says `CASCADE`
    /// and retypes their column of the same name, as `ALTER COLUMN … TYPE`
    /// would, recursing to their partitions and inheritance children, so
    /// the comparer drops what is in the way of it first. Empty for any
    /// other kind of type.
    pub fn composite_retyped_attributes(
        &self,
        target: &PgType,
        use_drop: bool,
        usage: &CompositeUsage,
    ) -> Vec<String> {
        if (self.typtype as u8 as char) != 'c'
            || (target.typtype as u8 as char) != 'c'
            || self.schema != target.schema
            || self.typname != target.typname
            || usage.stored_in.as_ref().is_some_and(|c| !c.is_empty())
        {
            return Vec::new();
        }
        let readded = self.composite_readded_attributes(target, use_drop, usage);
        self.composite_attributes
            .iter()
            .filter(|attribute| {
                !readded.contains(attribute.name.as_str())
                    && target
                        .composite_attributes
                        .iter()
                        .find(|a| a.name == attribute.name)
                        .is_some_and(|to| to.type_clause() != attribute.type_clause())
            })
            .map(|attribute| attribute.name.clone())
            .collect()
    }

    /// The `('c', 'c')` part of
    /// [`get_alter_script_parts_with`](Self::get_alter_script_parts_with):
    /// the attribute changes that turn this composite type into `target`,
    /// each a block ending in a blank line, notes included; empty when the
    /// attributes already match. What is left in place is pushed to
    /// `warnings`.
    fn composite_alter_script(
        &self,
        target: &PgType,
        use_drop: bool,
        usage: &CompositeUsage,
        warnings: &mut Vec<String>,
    ) -> String {
        let qualified = format!("{}.{}", self.schema, self.typname);
        let cascade = if usage.typed_tables.is_empty() {
            ""
        } else {
            " cascade"
        };
        let current = &self.composite_attributes;
        let wanted = &target.composite_attributes;
        let find = |attributes: &'_ [CompositeAttribute], name: &str| {
            attributes.iter().position(|a| a.name == name)
        };

        let (appended, out_of_place) = self.composite_appended_order(target);

        let mut notes = Vec::new();
        // Kept attributes dropped and added again to reach the target order.
        let readded = self.composite_readded_attributes(target, use_drop, usage);
        if out_of_place.is_some() && !(use_drop && usage.holds_no_values()) {
            let quoted = |names: &mut dyn Iterator<Item = &str>| {
                names.map(quote_ident).collect::<Vec<_>>().join(", ")
            };
            let reason = if use_drop {
                usage.why_values_may_exist()
            } else {
                "use_drop=false".to_string()
            };
            let note = format!(
                "Composite type {qualified} will have its attributes in a different order ({}) than the target ({}); \
                 pgc reorders attributes only by dropping and re-adding them, which would lose stored values ({reason}) (manual intervention needed).",
                quoted(&mut appended.iter().copied()),
                quoted(&mut wanted.iter().map(|a| a.name.as_str()))
            );
            notes.push(comment_out_joined(&note));
            warnings.push(note);
        }

        let mut statements = Vec::new();
        let dropped: Vec<&str> = current
            .iter()
            .filter(|a| find(wanted, &a.name).is_none() || readded.contains(a.name.as_str()))
            .map(|a| a.name.as_str())
            .collect();
        // `CASCADE` is how the drop reaches the typed tables, and it is also
        // the drop behaviour: every object depending on the dropped columns
        // goes with them, silently (a NOTICE), where `ALTER TABLE … DROP
        // COLUMN` on an ordinary table refuses. PostgreSQL has no way to
        // recurse without it. The views pgc compares are dropped ahead of it
        // (`drop_views`); what else goes is said here and on stderr.
        if use_drop && !cascade.is_empty() && !dropped.is_empty() {
            let note = format!(
                "Composite type {qualified} drops attribute(s) {} with CASCADE, which PostgreSQL needs to drop them from its typed table(s) {}; \
                 CASCADE also drops every object that depends on those columns there, including views, rules, policies, foreign keys and routines outside the compared schemas.",
                dropped
                    .iter()
                    .map(|name| quote_ident(name))
                    .collect::<Vec<_>>()
                    .join(", "),
                usage.typed_tables.join(", ")
            );
            statements.push(comment_out_joined(&note));
            warnings.push(note);
        }
        for name in &dropped {
            let drop = format!(
                "alter type {qualified} drop attribute if exists {}{cascade};",
                quote_ident(name)
            );
            if use_drop {
                statements.push(drop);
            } else {
                statements.push(comment_out_joined(&drop));
            }
        }

        for attribute in current {
            let Some(to) = find(wanted, &attribute.name).map(|i| &wanted[i]) else {
                continue;
            };
            // A collation change is a retype too: `SET DATA TYPE … COLLATE`
            // (without one, the data type's default collation).
            if to.type_clause() == attribute.type_clause()
                || readded.contains(attribute.name.as_str())
            {
                continue;
            }
            let retype = format!(
                "alter type {qualified} alter attribute {} set data type {}{cascade};",
                quote_ident(&attribute.name),
                to.type_clause()
            );
            match &usage.stored_in {
                Some(columns) if !columns.is_empty() => {
                    let note = format!(
                        "Attribute {} of composite type {qualified} cannot change its type ({} -> {}) in place: \
                         column(s) {} store values of the type, and PostgreSQL refuses to retype an attribute then (manual intervention needed).",
                        quote_ident(&attribute.name),
                        attribute.type_clause(),
                        to.type_clause(),
                        columns.join(", ")
                    );
                    statements.push(comment_out_joined(&note));
                    statements.push(comment_out_joined(&retype));
                    warnings.push(note);
                }
                _ => statements.push(retype),
            }
        }

        for attribute in wanted {
            if find(current, &attribute.name).is_some()
                && !readded.contains(attribute.name.as_str())
            {
                continue;
            }
            statements.push(format!(
                "alter type {qualified} add attribute {} {}{cascade};",
                quote_ident(&attribute.name),
                attribute.type_clause()
            ));
        }

        notes.extend(statements);
        if notes.is_empty() {
            String::new()
        } else {
            notes.join("\n\n").with_empty_lines()
        }
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

/// Whether two `pg_get_constraintdef` texts of one domain constraint differ
/// only in that `current` is still `NOT VALID`, so `ALTER DOMAIN ... VALIDATE
/// CONSTRAINT` reaches `target` without dropping anything. PostgreSQL 14–19
/// all deparse an unvalidated CHECK as the validated text plus ` NOT VALID`.
fn only_validation_differs(current: &str, target: &str) -> bool {
    current.strip_suffix(" NOT VALID") == Some(target)
}

/// Comments out every line of `sql`, not just the first: a CHECK body can
/// hold a string literal with a newline, and a quoted name can hold one too,
/// and a lone leading `-- ` would leave the rest of such a statement live.
/// [`comment_out`] without the final newline, because the domain statements
/// are joined with blank lines by the caller. Lines end at a bare `\r` too.
fn comment_out_joined(sql: &str) -> String {
    let mut commented = comment_out(sql);
    if commented.ends_with('\n') {
        commented.pop();
    }
    commented
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
#[path = "tests/pg_type.rs"]
mod tests;
