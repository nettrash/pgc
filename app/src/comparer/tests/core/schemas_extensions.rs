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
