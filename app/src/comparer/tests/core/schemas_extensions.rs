//! Tests for `compare_schemas` and `compare_extensions`.

use crate::comparer::core::*;
use crate::config::dump_config::DumpConfig;
use crate::config::grants_mode::GrantsMode;
use crate::dump::extension::{AvailableExtension, Extension};
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

// ── PG19: extension versions the migrated server cannot install ────────
// PostgreSQL 19 ships btree_gist 1.9 as the only installable version (the
// 1.2 base script is gone), so `version '1.8'` — from an older major, or from
// a pg_upgrade'd 19 cluster still at 1.8 — fails, and so does every GiST
// index and exclusion constraint that needs btree_gist. The FROM dump records
// what its server can install; the output only changes when the requested
// version is not in that list.

fn avail(name: &str, default_version: &str, versions: &[&str]) -> AvailableExtension {
    AvailableExtension {
        name: name.to_string(),
        default_version: default_version.to_string(),
        versions: versions.iter().map(|v| v.to_string()).collect(),
    }
}

async fn compare_ext(from_dump: Dump, to_dump: Dump, use_comments: bool) -> String {
    let mut comparer = Comparer::new(
        from_dump,
        to_dump,
        true,
        false,
        use_comments,
        GrantsMode::Ignore,
    );
    comparer.compare_extensions().await.unwrap();
    comparer.get_script()
}

#[tokio::test]
async fn create_of_an_uninstallable_version_uses_the_server_default() {
    let mut from_dump = Dump::new(DumpConfig::default());
    from_dump
        .available_extensions
        .push(avail("btree_gist", "1.9", &["1.9"]));
    let mut to_dump = Dump::new(DumpConfig::default());
    to_dump.extensions.push(ext("btree_gist", "1.8", "public"));

    let script = compare_ext(from_dump, to_dump, true).await;

    assert!(
        script.contains(
            "-- Extension btree_gist version '1.8' is not installable on the migrated server \
             (installable: 1.9); creating its default version '1.9' instead.\n\
             create extension if not exists btree_gist with schema public;\n"
        ),
        "{script}"
    );
    assert!(!script.contains("version '1.8';"), "{script}");
}

#[tokio::test]
async fn default_version_create_note_is_stripped_without_comments() {
    let mut from_dump = Dump::new(DumpConfig::default());
    from_dump
        .available_extensions
        .push(avail("btree_gist", "1.9", &["1.9"]));
    let mut to_dump = Dump::new(DumpConfig::default());
    to_dump.extensions.push(ext("btree_gist", "1.8", "public"));

    let script = compare_ext(from_dump, to_dump, false).await;

    assert_eq!(
        script,
        "create extension if not exists btree_gist with schema public;\n"
    );
}

#[tokio::test]
async fn impossible_downgrade_is_skipped_with_a_note() {
    // FROM: a PG19 server at btree_gist 1.9; TO: a PG18 database at 1.8.
    let mut from_dump = Dump::new(DumpConfig::default());
    from_dump
        .extensions
        .push(ext("btree_gist", "1.9", "public"));
    from_dump
        .available_extensions
        .push(avail("btree_gist", "1.9", &["1.9"]));
    let mut to_dump = Dump::new(DumpConfig::default());
    to_dump.extensions.push(ext("btree_gist", "1.8", "public"));

    let script = compare_ext(from_dump.clone(), to_dump.clone(), true).await;
    assert!(
        script.contains(
            "-- Extension btree_gist version '1.8' is not available on the migrated server \
             (installed: '1.9'; installable: 1.9); version left unchanged.\n"
        ),
        "{script}"
    );
    assert!(!script.contains("alter extension"), "{script}");

    // With comments off nothing executable is left, so a re-run is empty.
    assert_eq!(compare_ext(from_dump, to_dump, false).await, "");
}

#[tokio::test]
async fn skipped_update_still_relocates() {
    let mut from_dump = Dump::new(DumpConfig::default());
    from_dump
        .extensions
        .push(ext("btree_gist", "1.9", "public"));
    from_dump
        .available_extensions
        .push(avail("btree_gist", "1.9", &["1.9"]));
    let mut to_dump = Dump::new(DumpConfig::default());
    to_dump
        .extensions
        .push(ext("btree_gist", "1.8", "shared_schema"));

    let script = compare_ext(from_dump, to_dump, false).await;
    assert_eq!(
        script,
        "alter extension btree_gist set schema shared_schema;\n"
    );
}

#[tokio::test]
async fn upgrade_from_a_pg_upgraded_version_is_still_emitted() {
    // pg_upgrade to 19 keeps btree_gist at 1.8, which 19 cannot install
    // from scratch but can update from (btree_gist--1.8--1.9.sql).
    let mut from_dump = Dump::new(DumpConfig::default());
    from_dump
        .extensions
        .push(ext("btree_gist", "1.8", "public"));
    from_dump
        .available_extensions
        .push(avail("btree_gist", "1.9", &["1.9"]));
    let mut to_dump = Dump::new(DumpConfig::default());
    to_dump.extensions.push(ext("btree_gist", "1.9", "public"));

    let script = compare_ext(from_dump, to_dump, false).await;
    assert_eq!(script, "alter extension btree_gist update to '1.9';\n");
}

#[tokio::test]
async fn update_between_uninstallable_versions_is_still_attempted() {
    // Installed 1.7 is not installable on 19 either; an update script from
    // it (1.7--1.8) may exist, and the list cannot tell, so emit as before.
    let mut from_dump = Dump::new(DumpConfig::default());
    from_dump
        .extensions
        .push(ext("btree_gist", "1.7", "public"));
    from_dump
        .available_extensions
        .push(avail("btree_gist", "1.9", &["1.9"]));
    let mut to_dump = Dump::new(DumpConfig::default());
    to_dump.extensions.push(ext("btree_gist", "1.8", "public"));

    let script = compare_ext(from_dump, to_dump, false).await;
    assert_eq!(script, "alter extension btree_gist update to '1.8';\n");
}

#[tokio::test]
async fn installable_versions_are_emitted_exactly_as_before() {
    let mut from_dump = Dump::new(DumpConfig::default());
    from_dump
        .extensions
        .push(ext("postgres_fdw", "1.2", "public"));
    from_dump.available_extensions.extend([
        avail("postgres_fdw", "1.3", &["1.0", "1.1", "1.2", "1.3"]),
        avail("hstore", "1.8", &["1.4", "1.5", "1.6", "1.7", "1.8"]),
    ]);
    let mut to_dump = Dump::new(DumpConfig::default());
    to_dump
        .extensions
        .push(ext("postgres_fdw", "1.3", "public"));
    to_dump.extensions.push(ext("hstore", "1.7", "public"));

    let script = compare_ext(from_dump, to_dump, false).await;
    assert_eq!(
        script,
        "alter extension postgres_fdw update to '1.3';\n\n\
         create extension if not exists hstore with schema public version '1.7';\n"
    );
}

#[tokio::test]
async fn unknown_availability_is_byte_identical_to_before() {
    // A FROM dump written by pgc 1.0.x has no availability list.
    let mut from_dump = Dump::new(DumpConfig::default());
    from_dump
        .extensions
        .push(ext("btree_gist", "1.9", "public"));
    let mut to_dump = Dump::new(DumpConfig::default());
    to_dump.extensions.push(ext("btree_gist", "1.8", "public"));
    to_dump.extensions.push(ext("btree_gin", "1.3", "public"));

    // The TO dump's own list is never consulted.
    to_dump
        .available_extensions
        .push(avail("btree_gist", "1.8", &["1.8"]));

    let script = compare_ext(from_dump, to_dump, true).await;
    assert!(
        script.contains("alter extension btree_gist update to '1.8';"),
        "{script}"
    );
    assert!(
        script
            .contains("create extension if not exists btree_gin with schema public version '1.3';"),
        "{script}"
    );
    assert!(!script.contains("migrated server"), "{script}");
}

#[tokio::test]
async fn extension_missing_from_the_list_keeps_the_pinned_create() {
    // pg_stash_advice (new in 19) going to an 18 server: not available at
    // all, which no spelling fixes, so the statement stays as it was.
    let mut from_dump = Dump::new(DumpConfig::default());
    from_dump
        .available_extensions
        .push(avail("btree_gist", "1.8", &["1.8"]));
    let mut to_dump = Dump::new(DumpConfig::default());
    to_dump
        .extensions
        .push(ext("pg_stash_advice", "1.0", "public"));

    let script = compare_ext(from_dump, to_dump, false).await;
    assert_eq!(
        script,
        "create extension if not exists pg_stash_advice with schema public version '1.0';\n"
    );
}

#[tokio::test]
async fn extension_without_a_default_keeps_the_pinned_create() {
    let mut from_dump = Dump::new(DumpConfig::default());
    from_dump
        .available_extensions
        .push(avail("custom_ext", "", &["2.0"]));
    let mut to_dump = Dump::new(DumpConfig::default());
    to_dump.extensions.push(ext("custom_ext", "1.0", "public"));

    let script = compare_ext(from_dump, to_dump, false).await;
    assert_eq!(
        script,
        "create extension if not exists custom_ext with schema public version '1.0';\n"
    );
}
