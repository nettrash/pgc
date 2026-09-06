//! Tests for `compare_schemas` and `compare_extensions`.

use crate::comparer::core::*;
use crate::config::dump_config::DumpConfig;
use crate::config::grants_mode::GrantsMode;
use crate::dump::extension::Extension;
use crate::dump::schema::Schema;

#[tokio::test]
async fn compare_schemas_emits_owner_change() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_schema = Schema::new("public".to_string(), "public".to_string(), None);
    from_schema.owner = "old_owner".to_string();
    from_schema.hash();

    let mut to_schema = Schema::new("public".to_string(), "public".to_string(), None);
    to_schema.owner = "new_owner".to_string();
    to_schema.hash();

    from_dump.schemas.push(from_schema);
    to_dump.schemas.push(to_schema);

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_schemas().await.unwrap();
    let script = comparer.get_script();

    assert!(script.contains("alter schema public owner to new_owner;"));
}

#[tokio::test]
async fn compare_extensions_notes_owner_change_as_unsupported() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_ext = Extension::new(
        "hstore".to_string(),
        "1.0".to_string(),
        "public".to_string(),
    );
    from_ext.owner = "old_owner".to_string();

    let mut to_ext = Extension::new(
        "hstore".to_string(),
        "1.0".to_string(),
        "public".to_string(),
    );
    to_ext.owner = "new_owner".to_string();

    from_dump.extensions.push(from_ext);
    to_dump.extensions.push(to_ext);

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_extensions().await.unwrap();
    let script = comparer.get_script();

    assert!(script.contains(
        "-- Extension owner change is not supported by PostgreSQL ALTER EXTENSION syntax (old_owner -> new_owner)."
    ));
}

// ── issue #241: an extension's identity is its name ────────────────────
// `pg_extension` has a unique index on `extname`; the schema is where the
// extension was installed, not part of what it is. Matching on
// `(schema, name)` made a move look like two unrelated events, and the
// pair they produced destroyed the extension: `create extension if not
// exists` is a no-op once the name is taken, and the `drop` that followed
// removed the only copy.

fn ext(name: &str, version: &str, schema: &str) -> Extension {
    Extension::new(name.to_string(), version.to_string(), schema.to_string())
}

#[tokio::test]
async fn compare_extensions_relocates_a_moved_extension() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());
    from_dump
        .extensions
        .push(ext("fuzzystrmatch", "1.2", "schema_a"));
    to_dump
        .extensions
        .push(ext("fuzzystrmatch", "1.2", "public"));

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_extensions().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("alter extension fuzzystrmatch set schema public;"),
        "the move must be emitted as a relocation:\n{script}"
    );
    // Either half of the old pair is enough to lose the extension, so both
    // are asserted away rather than just the drop.
    assert!(
        !script.contains("drop extension"),
        "a moved extension must not be dropped:\n{script}"
    );
    assert!(
        !script.contains("create extension"),
        "a moved extension must not be re-created:\n{script}"
    );
}

#[tokio::test]
async fn compare_extensions_relocates_in_either_direction() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());
    from_dump
        .extensions
        .push(ext("fuzzystrmatch", "1.2", "public"));
    to_dump
        .extensions
        .push(ext("fuzzystrmatch", "1.2", "schema_a"));

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_extensions().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("alter extension fuzzystrmatch set schema schema_a;"),
        "moving out of public is the same event:\n{script}"
    );
    assert!(!script.contains("drop extension"), "{script}");
}

#[tokio::test]
async fn compare_extensions_updates_and_relocates_together() {
    // `get_alter_script` emits the version update before the schema move;
    // both belong to one extension and both must survive the name-keyed
    // match.
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());
    from_dump.extensions.push(ext("hstore", "1.4", "schema_a"));
    to_dump.extensions.push(ext("hstore", "1.8", "public"));

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_extensions().await.unwrap();
    let script = comparer.get_script();

    let update = script
        .find("alter extension hstore update to '1.8';")
        .unwrap_or_else(|| panic!("version update missing:\n{script}"));
    let relocate = script
        .find("alter extension hstore set schema public;")
        .unwrap_or_else(|| panic!("relocation missing:\n{script}"));
    assert!(update < relocate, "update runs before the move:\n{script}");
    assert!(!script.contains("drop extension"), "{script}");
}

#[tokio::test]
async fn compare_extensions_still_creates_a_genuinely_new_one() {
    // The guard against over-matching: keying by name must not make a
    // never-installed extension look like something that already exists.
    let from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());
    to_dump
        .extensions
        .push(ext("fuzzystrmatch", "1.2", "public"));

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_extensions().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains(
            "create extension if not exists fuzzystrmatch with schema public version '1.2';"
        ),
        "{script}"
    );
    assert!(!script.contains("alter extension"), "{script}");
}

#[tokio::test]
async fn compare_extensions_still_drops_a_genuinely_removed_one() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());
    from_dump
        .extensions
        .push(ext("fuzzystrmatch", "1.2", "schema_a"));

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_extensions().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("drop extension if exists fuzzystrmatch;"),
        "{script}"
    );
    assert!(!script.contains("alter extension"), "{script}");
}

#[tokio::test]
async fn compare_extensions_leaves_an_unmoved_extension_alone() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());
    from_dump
        .extensions
        .push(ext("fuzzystrmatch", "1.2", "public"));
    to_dump
        .extensions
        .push(ext("fuzzystrmatch", "1.2", "public"));

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_extensions().await.unwrap();
    let script = comparer.get_script();

    assert!(!script.contains("alter extension"), "{script}");
    assert!(!script.contains("drop extension"), "{script}");
    assert!(!script.contains("create extension"), "{script}");
}
