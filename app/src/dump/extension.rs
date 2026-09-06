//! Installed extensions (`pg_extension`) — `CREATE EXTENSION`.
//!
//! Only the name, version and schema are compared; an extension's own objects are
//! owned by the extension and are never emitted individually.
//!
//! An extension is identified by its **name alone**: `pg_extension` carries a
//! unique index on `extname`, so a database holds at most one extension of a
//! given name and [`Extension::schema`] only records where it was installed.
//! `Comparer::compare_extensions` matches on name for that reason — pairing on
//! `(schema, name)` made a relocated extension match nothing, and the create
//! plus drop that produced deleted it outright (issue #241).

use serde::{Deserialize, Serialize};

use crate::utils::string_extensions::StringExt;

/// This is an information about a PostgreSQL extension.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Extension {
    /// Name of the extension
    pub name: String,
    /// Version of the extension
    pub version: String,
    /// Schema where the extension is installed
    pub schema: String,
    /// Owner of the extension
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub owner: String,
}

impl Extension {
    /// Creates a new Extension with the given name, version, and schema
    pub fn new(name: String, version: String, schema: String) -> Self {
        Self {
            name,
            version,
            schema,
            owner: String::new(),
        }
    }

    /// Hash
    pub fn hash(&self) -> String {
        format!(
            "{:x}",
            md5::compute(format!(
                "{}.{}.{}.{}",
                self.schema, self.name, self.version, self.owner
            ))
        )
    }

    /// Returns a string to create the extension.
    pub fn get_script(&self) -> String {
        format!(
            "create extension if not exists {} with schema {} version '{}';",
            self.name, self.schema, self.version
        )
        .with_empty_lines()
    }

    /// Returns a string to drop the extension.
    pub fn get_drop_script(&self) -> String {
        format!("drop extension if exists {};", self.name).with_empty_lines()
    }

    /// Returns a script to alter this extension to match the target.
    ///
    /// A schema difference is emitted as `ALTER EXTENSION ... SET SCHEMA`,
    /// which PostgreSQL accepts only for a **relocatable** extension. The
    /// dump does not capture `pg_extension.extrelocatable`, so a move of a
    /// non-relocatable extension (the PL languages, `xml2`, …) is emitted the
    /// same way and fails on replay with "does not support SET SCHEMA".
    /// There is no correct alternative to emit: such an extension can only
    /// change schema by being dropped and recreated, which takes its objects
    /// with it — so failing is the honest outcome.
    pub fn get_alter_script(&self, target: &Extension) -> String {
        let mut script = String::new();
        if self.version != target.version {
            script.append_block(&format!(
                "alter extension {} update to '{}';",
                target.name, target.version
            ));
        }
        if self.schema != target.schema {
            script.append_block(&format!(
                "alter extension {} set schema {};",
                target.name, target.schema
            ));
        }
        script
    }
}

#[cfg(test)]
#[path = "tests/extension.rs"]
mod tests;
