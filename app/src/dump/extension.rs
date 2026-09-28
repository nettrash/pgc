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
//!
//! [`AvailableExtension`] records, per dump, which versions of each extension
//! the dumped server can install (`pg_available_extension_versions`). The
//! comparer consults the FROM dump's list — the migration runs on that server —
//! so it never pins a `VERSION` the server cannot install. PostgreSQL 19 made
//! that reachable: it ships `btree_gist` 1.9 as the only installable version,
//! so a TO dump from an older major (or from a `pg_upgrade`d 19 cluster still
//! at 1.7 / 1.8) asks for a version PostgreSQL 19 rejects outright.

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

    /// Returns a string to create the extension at the server's default
    /// version — the form `pg_dump` emits. Used when this extension's
    /// [`version`](Self::version) cannot be installed on the migrated server.
    pub fn get_script_default_version(&self) -> String {
        format!(
            "create extension if not exists {} with schema {};",
            self.name, self.schema
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
        let mut script = self.get_update_script(target);
        script.push_str(&self.get_set_schema_script(target));
        script
    }

    /// The version half of [`get_alter_script`](Self::get_alter_script):
    /// `alter extension ... update to '...'`, or empty when the versions match.
    pub fn get_update_script(&self, target: &Extension) -> String {
        if self.version == target.version {
            return String::new();
        }
        format!(
            "alter extension {} update to '{}';",
            target.name, target.version
        )
        .with_empty_lines()
    }

    /// The schema half of [`get_alter_script`](Self::get_alter_script):
    /// `alter extension ... set schema ...`, or empty when the schemas match.
    pub fn get_set_schema_script(&self, target: &Extension) -> String {
        if self.schema == target.schema {
            return String::new();
        }
        format!(
            "alter extension {} set schema {};",
            target.name, target.schema
        )
        .with_empty_lines()
    }
}

/// The versions of one extension a server can install, from
/// `pg_available_extensions` / `pg_available_extension_versions`.
///
/// Not an object pgc migrates: it describes the server a dump was taken from.
/// The comparer reads it on the FROM dump to avoid emitting a pinned
/// `CREATE EXTENSION ... VERSION` or `ALTER EXTENSION ... UPDATE TO` that the
/// migrated server cannot execute.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AvailableExtension {
    /// `quote_ident`-ed name, the same spelling as [`Extension::name`].
    pub name: String,
    /// The version `CREATE EXTENSION` without `VERSION` installs; empty when
    /// the control file names none.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub default_version: String,
    /// Every version `CREATE EXTENSION ... VERSION` can install, directly or
    /// through a chain of update scripts, as `pg_available_extension_versions`
    /// reports them.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub versions: Vec<String>,
}

impl AvailableExtension {
    /// Whether `CREATE EXTENSION ... VERSION 'version'` can succeed.
    pub fn can_install(&self, version: &str) -> bool {
        self.versions.iter().any(|v| v == version)
    }

    /// Whether `ALTER EXTENSION ... UPDATE TO 'to'` from `installed` is known
    /// to have no update path.
    ///
    /// `pg_available_extension_versions` lists every version reachable from an
    /// installable one, so when `installed` is itself installable and `to` is
    /// not, no update path can lead there. When `installed` is not installable
    /// (a `pg_upgrade`d cluster still at a version the new major no longer
    /// ships a script for), an update script from it may still exist, so this
    /// answers `false` and the update is attempted.
    pub fn cannot_update(&self, installed: &str, to: &str) -> bool {
        self.can_install(installed) && !self.can_install(to)
    }

    /// The installable versions, comma-separated, for script notes.
    pub fn versions_list(&self) -> String {
        if self.versions.is_empty() {
            "none".to_string()
        } else {
            self.versions.join(", ")
        }
    }
}

#[cfg(test)]
#[path = "tests/extension.rs"]
mod tests;
