//! Integration tests for the on-disk dump format.
//!
//! A dump file is a zip archive holding a single `dump.io` entry with the
//! JSON-serialized [`Dump`]. Two contracts matter here and neither is covered
//! by the unit tests, which build `Dump` values in memory and never touch the
//! filesystem:
//!
//! 1. `write_to_file` → `read_from_file` must round-trip losslessly.
//! 2. Dumps written by *older* `pgc` versions must stay readable — every field
//!    added after the initial release carries `#[serde(default)]`, so a JSON
//!    document missing those keys has to deserialize rather than error.

mod common;

use common::{ScratchDir, empty_dump, populated_dump};
use pgc::dump::core::Dump;
use std::io::{Read, Write};

#[tokio::test]
async fn write_then_read_round_trips_every_object_kind() {
    let dir = ScratchDir::new("roundtrip");
    let path = dir.path_str("schema.dump");
    let original = populated_dump("shop");

    original.write_to_file(&path).expect("write dump");
    let reloaded = Dump::read_from_file(&path).await.expect("read dump");

    assert_eq!(original.get_info(), reloaded.get_info());
    assert_eq!(original.schemas.len(), reloaded.schemas.len());
    assert_eq!(original.tables.len(), reloaded.tables.len());
    assert_eq!(original.views.len(), reloaded.views.len());
    assert_eq!(original.routines.len(), reloaded.routines.len());
    assert_eq!(original.sequences.len(), reloaded.sequences.len());
    assert_eq!(original.extensions.len(), reloaded.extensions.len());

    // Hashes drive every comparison, so they must survive serialization —
    // a dropped hash would silently turn "unchanged" into "recreate".
    for (before, after) in original.tables.iter().zip(&reloaded.tables) {
        assert_eq!(before.name, after.name);
        assert_eq!(
            before.hash, after.hash,
            "table {} hash changed",
            before.name
        );
    }
    for (before, after) in original.views.iter().zip(&reloaded.views) {
        assert_eq!(before.hash, after.hash, "view {} hash changed", before.name);
    }
}

#[tokio::test]
async fn round_trip_of_an_empty_dump_produces_an_empty_dump() {
    let dir = ScratchDir::new("empty");
    let path = dir.path_str("empty.dump");

    empty_dump("blank")
        .write_to_file(&path)
        .expect("write dump");
    let reloaded = Dump::read_from_file(&path).await.expect("read dump");

    assert!(reloaded.schemas.is_empty());
    assert!(reloaded.tables.is_empty());
    assert!(reloaded.column_dependents.is_empty());
}

#[tokio::test]
async fn dump_file_is_a_zip_holding_a_single_dump_io_entry() {
    let dir = ScratchDir::new("layout");
    let path = dir.path_str("layout.dump");
    populated_dump("shop")
        .write_to_file(&path)
        .expect("write dump");

    let mut archive =
        zip::ZipArchive::new(std::fs::File::open(&path).expect("open dump")).expect("read zip");
    assert_eq!(archive.len(), 1, "dump archive holds exactly one entry");

    let mut entry = archive.by_index(0).expect("first entry");
    assert_eq!(entry.name(), "dump.io");

    let mut json = String::new();
    entry
        .read_to_string(&mut json)
        .expect("entry is utf-8 json");
    let parsed: serde_json::Value = serde_json::from_str(&json).expect("entry is valid json");
    assert!(
        parsed.get("tables").is_some(),
        "tables key is always present"
    );
    // `configuration` is `skip_serializing`: connection details, including the
    // password, must never reach the dump file.
    assert!(
        parsed.get("configuration").is_none(),
        "connection configuration must not be serialized into the dump"
    );
}

#[tokio::test]
async fn dump_written_before_the_optional_fields_existed_still_loads() {
    let dir = ScratchDir::new("legacy");
    let path = dir.path_str("legacy.dump");

    // Exactly the keys a pre-`foreign_tables` pgc would have written: the
    // eight non-defaulted vectors and nothing else.
    let legacy = r#"{
        "schemas": [],
        "extensions": [],
        "types": [],
        "enums": [],
        "sequences": [],
        "routines": [],
        "tables": [],
        "views": []
    }"#;
    write_dump_io(&path, legacy);

    let dump = Dump::read_from_file(&path)
        .await
        .expect("a dump missing every optional field must still deserialize");

    assert!(dump.foreign_tables.is_empty());
    assert!(dump.statistics.is_empty());
    assert!(dump.publications.is_empty());
    assert!(dump.column_dependents.is_empty());
    assert!(dump.user_mappings.is_empty());
}

#[tokio::test]
async fn dump_missing_a_required_field_reports_an_error() {
    let dir = ScratchDir::new("corrupt");
    let path = dir.path_str("corrupt.dump");
    write_dump_io(&path, r#"{"schemas": []}"#);

    let err = Dump::read_from_file(&path)
        .await
        .expect_err("a truncated dump must not silently succeed");
    assert!(
        err.to_string().contains("Failed to deserialize dump"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn reading_a_file_that_is_not_a_zip_reports_an_error() {
    let dir = ScratchDir::new("notzip");
    let path = dir.path_str("notzip.dump");
    std::fs::write(&path, b"this is not a zip archive").expect("write file");

    assert!(
        Dump::read_from_file(&path).await.is_err(),
        "a non-zip file must not parse as a dump"
    );
}

/// Write `json` into a zip at `path` under the `dump.io` entry name that
/// `Dump::read_from_file` looks for.
fn write_dump_io(path: &str, json: &str) {
    let mut zip = zip::ZipWriter::new(std::fs::File::create(path).expect("create dump"));
    zip.start_file(
        "dump.io",
        zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Deflated),
    )
    .expect("start entry");
    zip.write_all(json.as_bytes()).expect("write entry");
    zip.finish().expect("finish zip");
}
