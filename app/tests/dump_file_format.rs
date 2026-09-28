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
use pgc::dump::column_dependent::{ColumnDependent, ColumnDependentKind};
use pgc::dump::core::Dump;
use pgc::dump::statistic::Statistic;
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
    assert_eq!(dump.server_version_num, None);
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

// ==== pg19:publications tests ====

// pgc 1.1 models PG19 publication membership (ALL SEQUENCES, EXCEPT) and the
// older gaps it depends on (TABLES IN SCHEMA, column lists, row filters,
// publish_via_partition_root, publish_generated_columns). A 1.0.x dump has
// none of these keys, and a 1.1 dump of a publication that uses none of them
// must look exactly like the 1.0.x one.

const PUBLICATION_FIELDS_ADDED_IN_1_1: [&str; 6] = [
    "all_sequences",
    "table_specs",
    "schemas",
    "except_tables",
    "via_partition_root",
    "publish_generated_columns",
];

#[tokio::test]
async fn publication_written_by_pgc_1_0_loads_with_the_new_fields_at_default() {
    let dir = ScratchDir::new("legacy_pub");
    let path = dir.path_str("legacy.dump");
    let legacy = r#"{
        "schemas": [], "extensions": [], "types": [], "enums": [],
        "sequences": [], "routines": [], "tables": [], "views": [],
        "publications": [{
            "name": "p_tab", "owner": "postgres", "all_tables": true,
            "publish": "insert,update,delete,truncate",
            "hash": "40cbc4da36e4b32910f5dcbbf3e5ea7f88ead8495802dd51445d1aadad90ca0b"
        }]
    }"#;
    write_dump_io(&path, legacy);

    let dump = Dump::read_from_file(&path)
        .await
        .expect("a 1.0.x publication must still deserialize");
    let loaded = &dump.publications[0];
    assert!(!loaded.all_sequences);
    assert!(loaded.table_specs.is_empty());
    assert!(loaded.schemas.is_empty());
    assert!(loaded.except_tables.is_empty());
    assert!(!loaded.via_partition_root);
    assert_eq!(loaded.publish_generated_columns, None);

    // The stored hash is what a comparison reads; recomputing it from the
    // loaded fields must give the same digest, or every 1.0.x FROM dump would
    // report its publications as changed.
    let mut recomputed = loaded.clone();
    recomputed.hash();
    assert_eq!(recomputed.hash, loaded.hash);
}

#[tokio::test]
async fn publication_fields_added_in_1_1_round_trip_and_stay_out_of_the_file_at_default() {
    use pgc::dump::publication::{Publication, PublicationTableSpec};

    let dir = ScratchDir::new("pg19_pub");
    let path = dir.path_str("pubs.dump");

    let mut full = Publication {
        name: "p_full".into(),
        owner: "postgres".into(),
        publish: "insert".into(),
        tables: vec!["s.t1".into()],
        schemas: vec!["s2".into()],
        via_partition_root: true,
        publish_generated_columns: Some("stored".into()),
        ..Default::default()
    };
    full.table_specs.insert(
        "s.t1".into(),
        PublicationTableSpec {
            columns: vec!["a".into(), "id".into()],
            row_filter: Some("(a > 1)".into()),
        },
    );
    full.hash();
    let mut all = Publication {
        name: "p_all".into(),
        owner: "postgres".into(),
        all_tables: true,
        all_sequences: true,
        except_tables: vec!["s.t2".into()],
        publish: "insert".into(),
        ..Default::default()
    };
    all.hash();
    let mut plain = Publication {
        name: "p_plain".into(),
        owner: "postgres".into(),
        publish: "insert".into(),
        tables: vec!["s.t1".into()],
        ..Default::default()
    };
    plain.hash();

    let mut dump = empty_dump("pubs");
    dump.publications = vec![full.clone(), all.clone(), plain.clone()];
    dump.write_to_file(&path).expect("write dump");

    let reloaded = Dump::read_from_file(&path).await.expect("read dump");
    for (before, after) in [&full, &all, &plain]
        .into_iter()
        .zip(&reloaded.publications)
    {
        assert_eq!(format!("{before:?}"), format!("{after:?}"));
    }

    let mut json = String::new();
    zip::ZipArchive::new(std::fs::File::open(&path).expect("open dump"))
        .expect("zip")
        .by_name("dump.io")
        .expect("dump.io entry")
        .read_to_string(&mut json)
        .expect("read entry");
    let value: serde_json::Value = serde_json::from_str(&json).expect("json");
    let written = &value["publications"];
    for key in PUBLICATION_FIELDS_ADDED_IN_1_1 {
        assert!(
            written[2].get(key).is_none(),
            "{key} written for a publication that does not use it: {}",
            written[2]
        );
    }
    assert_eq!(written[0]["table_specs"]["s.t1"]["row_filter"], "(a > 1)");
    assert_eq!(written[1]["except_tables"][0], "s.t2");
}

// ==== pg19:subscriptions-fdw tests ====

#[tokio::test]
async fn subscription_and_fdw_written_by_pgc_1_0_x_still_load() {
    let dir = ScratchDir::new("legacy-subscription");
    let path = dir.path_str("legacy.dump");

    // The exact subscription and FDW shapes pgc 1.0.27 wrote: no `server`,
    // `retain_dead_tuples`, `max_retention_duration`, `wal_receiver_timeout`
    // or `connection_func` keys.
    let legacy = r#"{
        "schemas": [], "extensions": [], "types": [], "enums": [],
        "sequences": [], "routines": [], "tables": [], "views": [],
        "subscriptions": [{
            "name": "s", "owner": "postgres",
            "connection": "host=primary dbname=mydb",
            "publications": ["p"], "enabled": false,
            "hash": "3f70947e2c5caf9758701238ccfb76f84c9fc94190b6b1a8ba909eceab29b316"
        }],
        "foreign_data_wrappers": [{
            "name": "w", "owner": "postgres",
            "handler_func": "public.postgres_fdw_handler",
            "hash": "0000"
        }]
    }"#;
    write_dump_io(&path, legacy);

    let dump = Dump::read_from_file(&path)
        .await
        .expect("a pgc 1.0.x subscription / FDW must still deserialize");

    let sub = &dump.subscriptions[0];
    assert_eq!(sub.connection, "host=primary dbname=mydb");
    assert_eq!(sub.server, None);
    assert!(!sub.retain_dead_tuples);
    assert_eq!(sub.max_retention_duration, 0);
    assert_eq!(sub.wal_receiver_timeout, None);
    let fdw = &dump.foreign_data_wrappers[0];
    assert_eq!(fdw.connection_func, None);
    assert_eq!(fdw.validator_func, None);

    // Re-hashing a loaded 1.0.x subscription gives back the stored digest,
    // so an old dump compared against a new one reports no change.
    let mut rehashed = sub.clone();
    rehashed.hash();
    assert_eq!(rehashed.hash, sub.hash);
}

#[tokio::test]
async fn pg19_subscription_and_fdw_fields_round_trip() {
    let dir = ScratchDir::new("pg19-subscription");
    let path = dir.path_str("pg19.dump");

    let json = r#"{
        "schemas": [], "extensions": [], "types": [], "enums": [],
        "sequences": [], "routines": [], "tables": [], "views": [],
        "subscriptions": [{
            "name": "s", "owner": "postgres", "connection": "",
            "server": "srv", "publications": ["p"], "enabled": false,
            "retain_dead_tuples": true, "max_retention_duration": 60000,
            "wal_receiver_timeout": "30s", "hash": null
        }],
        "foreign_data_wrappers": [{
            "name": "w", "owner": "postgres",
            "connection_func": "public.postgres_fdw_connection", "hash": null
        }]
    }"#;
    write_dump_io(&path, json);
    let dump = Dump::read_from_file(&path).await.expect("read pg19 dump");
    let rewritten = dir.path_str("rewritten.dump");
    dump.write_to_file(&rewritten).expect("write dump");
    let reloaded = Dump::read_from_file(&rewritten).await.expect("reload dump");

    let sub = &reloaded.subscriptions[0];
    assert_eq!(sub.server.as_deref(), Some("srv"));
    assert!(sub.retain_dead_tuples);
    assert_eq!(sub.max_retention_duration, 60000);
    assert_eq!(sub.wal_receiver_timeout.as_deref(), Some("30s"));
    assert_eq!(
        reloaded.foreign_data_wrappers[0].connection_func.as_deref(),
        Some("public.postgres_fdw_connection")
    );
}

// ==== pg19:statistics tests ====

/// A pgc 1.0.x dump: an expression statistic without `columns_def`, and a
/// column-dependent graph holding only the kinds 1.0.x knew.
#[tokio::test]
async fn dump_with_a_pre_1_1_statistic_and_column_dependents_still_loads() {
    let dir = ScratchDir::new("legacy-statistics");
    let path = dir.path_str("legacy.dump");
    let legacy = r#"{
        "schemas": [], "extensions": [], "types": [], "enums": [],
        "sequences": [], "routines": [], "tables": [], "views": [],
        "statistics": [{
            "schema": "s", "name": "st", "owner": "postgres",
            "table_schema": "s", "table_name": "t",
            "kinds": ["ndistinct", "dependencies", "mcv", "expressions"],
            "columns": ["qty", "COALESCE(code", "'", "'::text)", "EXTRACT(year"],
            "definition": "CREATE STATISTICS s.st ON qty, COALESCE(code, ','::text), EXTRACT(year FROM created_on) FROM s.t",
            "hash": "stored-hash"
        }],
        "column_dependents": [{
            "schema": "s", "table": "t", "column": "qty", "kind": "Index",
            "dep_schema": "s", "dep_table": "t", "dep_name": "t_qty_idx"
        }]
    }"#;
    write_dump_io(&path, legacy);

    let dump = Dump::read_from_file(&path)
        .await
        .expect("a 1.0.x dump with statistics must still deserialize");
    let stat = &dump.statistics[0];
    assert_eq!(stat.columns_def, None);
    assert_eq!(
        stat.hash.as_deref(),
        Some("stored-hash"),
        "the stored hash is compared verbatim and must survive loading"
    );
    // Even without columns_def the CREATE comes out valid: no "expressions"
    // kind, and the ON list taken whole from the definition.
    assert!(
        stat.get_script().starts_with(
            "create statistics s.st (ndistinct, dependencies, mcv) on qty, COALESCE(code, ','::text), EXTRACT(year FROM created_on) from s.t;"
        ),
        "{}",
        stat.get_script()
    );
    assert_eq!(dump.column_dependents.len(), 1);
    assert_eq!(dump.column_dependents[0].kind, ColumnDependentKind::Index);
}

#[tokio::test]
async fn statistic_columns_def_and_statistic_dependents_round_trip() {
    let dir = ScratchDir::new("statistics-roundtrip");
    let path = dir.path_str("statistics.dump");
    let mut original = empty_dump("shop");
    let mut stat = Statistic::new(
        "s".to_string(),
        "st".to_string(),
        "postgres".to_string(),
        "s".to_string(),
        "t".to_string(),
        vec!["ndistinct".to_string(), "expressions".to_string()],
        vec!["a".to_string(), "(v)".to_string()],
        "CREATE STATISTICS s.st (ndistinct) ON a, (v) FROM s.t".to_string(),
    );
    stat.columns_def = Some("a, (v)".to_string());
    original.statistics.push(stat);
    original.column_dependents.push(ColumnDependent {
        schema: "s".to_string(),
        table: "t".to_string(),
        column: "v".to_string(),
        kind: ColumnDependentKind::Statistic,
        dep_schema: "stats".to_string(),
        dep_table: "t".to_string(),
        dep_name: "st".to_string(),
    });

    original.write_to_file(&path).expect("write dump");
    let reloaded = Dump::read_from_file(&path).await.expect("read dump");

    assert_eq!(
        reloaded.statistics[0].columns_def.as_deref(),
        Some("a, (v)")
    );
    assert_eq!(reloaded.statistics[0].hash, original.statistics[0].hash);
    let dep = &reloaded.column_dependents[0];
    assert_eq!(dep.kind, ColumnDependentKind::Statistic);
    assert_eq!(dep.dep_schema, "stats");
}

// ==== pg19:misc tests ====

/// A pgc 1.0.x dump has no `available_extensions` key; it must load with an
/// empty list, which makes the comparer emit extension versions unchanged.
#[tokio::test]
async fn dump_without_available_extensions_still_loads() {
    let dir = ScratchDir::new("legacy-ext");
    let path = dir.path_str("legacy.dump");
    let legacy = r#"{
        "schemas": [],
        "extensions": [{"name": "btree_gist", "version": "1.8", "schema": "public"}],
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
        .expect("legacy dump loads");
    assert_eq!(dump.extensions.len(), 1);
    assert!(dump.available_extensions.is_empty());
}

#[tokio::test]
async fn available_extensions_round_trip_and_are_omitted_when_empty() {
    use pgc::dump::extension::AvailableExtension;

    let dir = ScratchDir::new("avail-ext");

    // Empty: the key is not written at all, so the JSON of a dump without
    // the information is exactly what pgc 1.0.x wrote.
    let empty_path = dir.path_str("empty.dump");
    empty_dump("blank")
        .write_to_file(&empty_path)
        .expect("write dump");
    let mut json = String::new();
    zip::ZipArchive::new(std::fs::File::open(&empty_path).expect("open"))
        .expect("zip")
        .by_index(0)
        .expect("entry")
        .read_to_string(&mut json)
        .expect("utf-8");
    let parsed: serde_json::Value = serde_json::from_str(&json).expect("json");
    assert!(parsed.get("available_extensions").is_none(), "{json}");

    let path = dir.path_str("avail.dump");
    let mut dump = empty_dump("shop");
    dump.available_extensions = vec![
        AvailableExtension {
            name: "btree_gist".to_string(),
            default_version: "1.9".to_string(),
            versions: vec!["1.9".to_string()],
        },
        AvailableExtension {
            name: "\"uuid-ossp\"".to_string(),
            default_version: "1.1".to_string(),
            versions: vec!["1.1".to_string()],
        },
    ];
    dump.write_to_file(&path).expect("write dump");
    let reloaded = Dump::read_from_file(&path).await.expect("read dump");
    assert_eq!(reloaded.available_extensions, dump.available_extensions);
}

/// Read the JSON a dump file holds.
fn dump_json(path: &str) -> serde_json::Value {
    let mut json = String::new();
    zip::ZipArchive::new(std::fs::File::open(path).expect("open"))
        .expect("zip")
        .by_index(0)
        .expect("entry")
        .read_to_string(&mut json)
        .expect("utf-8");
    serde_json::from_str(&json).expect("json")
}

/// `standard_conforming_strings_off` is written only when true, so a dump of
/// a server at the default is the JSON pgc 1.0.x wrote, and a 1.0.x dump
/// (no key) loads as "on", which keeps its migrations unchanged.
#[tokio::test]
async fn standard_conforming_strings_off_round_trips_and_defaults_to_on() {
    let dir = ScratchDir::new("scs-off");

    let on_path = dir.path_str("on.dump");
    empty_dump("on")
        .write_to_file(&on_path)
        .expect("write dump");
    assert!(
        dump_json(&on_path)
            .get("standard_conforming_strings_off")
            .is_none()
    );
    let reloaded = Dump::read_from_file(&on_path).await.expect("read dump");
    assert!(!reloaded.standard_conforming_strings_off);

    let off_path = dir.path_str("off.dump");
    let mut dump = empty_dump("off");
    dump.standard_conforming_strings_off = true;
    dump.write_to_file(&off_path).expect("write dump");
    assert_eq!(
        dump_json(&off_path).get("standard_conforming_strings_off"),
        Some(&serde_json::Value::Bool(true))
    );
    let reloaded = Dump::read_from_file(&off_path).await.expect("read dump");
    assert!(reloaded.standard_conforming_strings_off);

    let legacy_path = dir.path_str("legacy.dump");
    write_dump_io(
        &legacy_path,
        r#"{"schemas": [], "extensions": [], "types": [], "enums": [],
            "sequences": [], "routines": [], "tables": [], "views": []}"#,
    );
    let legacy = Dump::read_from_file(&legacy_path)
        .await
        .expect("legacy dump loads");
    assert!(!legacy.standard_conforming_strings_off);
}

// ==== fix:rules tests ====

// pgc 1.1 records each rule's pg_depend dependencies (`Rule::depends_on`) so
// the comparer can drop a rule before the migration drops or retypes what it
// reads. A 1.0.x dump has no such key and must load as "unknown" (None), not
// as "depends on nothing", and keep its stored hash.

#[tokio::test]
async fn rule_written_by_pgc_1_0_loads_with_dependencies_unknown() {
    let dir = ScratchDir::new("legacy_rule");
    let path = dir.path_str("legacy.dump");
    let legacy = r#"{
        "schemas": [], "extensions": [], "types": [], "enums": [],
        "sequences": [], "routines": [], "tables": [], "views": [],
        "rules": [{
            "schema": "public", "table_name": "orders", "rule_name": "protect_delete",
            "definition": "CREATE RULE protect_delete AS\n    ON DELETE TO public.orders DO INSTEAD NOTHING;",
            "hash": "3054af4232aa7827b665d205599e80926c2543d2da746d53d4138d3c9df43829"
        }]
    }"#;
    write_dump_io(&path, legacy);

    let dump = Dump::read_from_file(&path)
        .await
        .expect("a 1.0.x rule must still deserialize");
    let loaded = &dump.rules[0];
    assert_eq!(loaded.depends_on, None);
    assert_eq!(loaded.comment, None);

    let mut recomputed = loaded.clone();
    recomputed.hash();
    assert_eq!(recomputed.hash, loaded.hash);
}

#[tokio::test]
async fn rule_dependencies_round_trip_and_stay_out_of_the_file_when_unknown() {
    use pgc::dump::rule::{Rule, RuleDependencies};

    let dir = ScratchDir::new("rule_deps");
    let path = dir.path_str("rules.dump");

    let make = |name: &str| {
        Rule::new(
            "public".into(),
            "orders".into(),
            name.into(),
            format!("CREATE RULE {name} AS\n    ON DELETE TO public.orders DO INSTEAD NOTHING;"),
            None,
        )
    };
    let mut full = make("r_full");
    full.depends_on = Some(RuleDependencies {
        relations: vec!["public.audit".into(), "public.orders".into()],
        columns: vec!["public.audit.id".into()],
        routines: vec!["public.tag(p integer)".into()],
    });
    let mut none_found = make("r_none_found");
    none_found.depends_on = Some(RuleDependencies::default());
    let unknown = make("r_unknown");

    let mut dump = empty_dump("rules");
    dump.rules = vec![full.clone(), none_found.clone(), unknown.clone()];
    dump.write_to_file(&path).expect("write dump");

    let json = dump_json(&path);
    let rules = json["rules"].as_array().expect("rules array");
    assert_eq!(
        rules[0]["depends_on"]["routines"][0],
        serde_json::Value::String("public.tag(p integer)".into())
    );
    // Recorded but empty: the key is there, its empty lists are not.
    assert_eq!(rules[1]["depends_on"], serde_json::json!({}));
    // Unknown: written exactly as pgc 1.0.x wrote a rule.
    assert!(rules[2].get("depends_on").is_none(), "{}", rules[2]);

    let reloaded = Dump::read_from_file(&path).await.expect("read dump");
    assert_eq!(reloaded.rules[0].depends_on, full.depends_on);
    assert_eq!(
        reloaded.rules[1].depends_on,
        Some(RuleDependencies::default())
    );
    assert_eq!(reloaded.rules[2].depends_on, None);
    for (before, after) in [&full, &none_found, &unknown].iter().zip(&reloaded.rules) {
        assert_eq!(
            before.hash, after.hash,
            "rule {} hash changed",
            before.rule_name
        );
    }
}

// ==== fix:sqlbody tests ====

/// A routine exactly as pgc 1.0.27 wrote it: no `is_sql_body` key, and the
/// hash it computed.
const LEGACY_STRING_BODY_ROUTINE: &str = r#"{
    "schema": "public", "oid": 42, "name": "add", "lang": "plpgsql",
    "kind": "FUNCTION", "return_type": "integer", "arguments": "a integer",
    "arguments_defaults": "DEFAULT 1", "comment": null,
    "source_code": "BEGIN RETURN a + 1; END", "volatility": "volatile",
    "is_strict": false, "is_leakproof": false, "parallel": "unsafe",
    "security_definer": false, "hash": "5d1522486bcb4f809c24696c11899a00"
}"#;

#[tokio::test]
async fn routine_written_by_pgc_1_0_x_loads_as_a_string_body_with_its_hash() {
    let dir = ScratchDir::new("legacy-routine");
    let path = dir.path_str("legacy.dump");
    write_dump_io(
        &path,
        &format!(
            r#"{{"schemas": [], "extensions": [], "types": [], "enums": [],
                "sequences": [], "routines": [{LEGACY_STRING_BODY_ROUTINE}],
                "tables": [], "views": []}}"#
        ),
    );

    let dump = Dump::read_from_file(&path)
        .await
        .expect("a pgc 1.0.x routine must still deserialize");
    let routine = &dump.routines[0];
    assert!(!routine.is_sql_body);

    // Recomputing the hash gives back the stored one, so an old dump of a
    // string-body routine compared against a new one reports no change.
    let mut rehashed = routine.clone();
    rehashed.hash();
    assert_eq!(rehashed.hash, routine.hash);
}

#[tokio::test]
async fn sql_body_flag_round_trips_and_is_omitted_for_string_bodies() {
    let dir = ScratchDir::new("sqlbody");
    let path = dir.path_str("sqlbody.dump");

    let string_body = common::routine("public", 1, "plain", "integer", "select x");
    let mut sql_body = common::routine("public", 2, "atomic", "integer", "");
    sql_body.source_code = "BEGIN ATOMIC\n SELECT (x + 1);\nEND".to_string();
    sql_body.is_sql_body = true;
    sql_body.hash();

    let mut dump = empty_dump("shop");
    dump.routines = vec![string_body.clone(), sql_body.clone()];
    dump.write_to_file(&path).expect("write dump");

    // A string-body routine is written exactly as pgc 1.0.x wrote it.
    let written = &dump_json(&path)["routines"];
    assert!(written[0].get("is_sql_body").is_none(), "{}", written[0]);
    assert_eq!(written[1]["is_sql_body"], serde_json::Value::Bool(true));

    let reloaded = Dump::read_from_file(&path).await.expect("read dump");
    for (before, after) in [&string_body, &sql_body]
        .into_iter()
        .zip(&reloaded.routines)
    {
        assert_eq!(before.is_sql_body, after.is_sql_body);
        assert_eq!(before.source_code, after.source_code);
        assert_eq!(before.hash, after.hash);
    }
}

#[tokio::test]
async fn reading_a_dump_normalises_crlf_only_in_string_bodies() {
    // A string body is CRLF-normalised (and rehashed) on load, as before. A
    // SQL-standard body is deparsed with LF only, so a CR LF in it sits inside
    // a literal, and rewriting it would change what the routine returns.
    let dir = ScratchDir::new("sqlbody-crlf");
    let path = dir.path_str("crlf.dump");

    let mut string_body = common::routine("public", 1, "plain", "text", "");
    string_body.source_code = "select 'a'\r\n".to_string();
    string_body.hash();
    let mut sql_body = common::routine("public", 2, "atomic", "text", "");
    sql_body.source_code = "BEGIN ATOMIC\n SELECT 'a\r\nb'::text;\nEND".to_string();
    sql_body.is_sql_body = true;
    sql_body.hash();

    let mut dump = empty_dump("shop");
    dump.routines = vec![string_body.clone(), sql_body.clone()];
    dump.write_to_file(&path).expect("write dump");
    let reloaded = Dump::read_from_file(&path).await.expect("read dump");

    assert_eq!(reloaded.routines[0].source_code, "select 'a'\n");
    assert_ne!(reloaded.routines[0].hash, string_body.hash);
    assert_eq!(reloaded.routines[1].source_code, sql_body.source_code);
    assert_eq!(reloaded.routines[1].hash, sql_body.hash);
}

#[tokio::test]
async fn sql_body_dependencies_round_trip_and_are_omitted_when_empty() {
    use pgc::dump::routine::SqlBodyDependencies;

    let dir = ScratchDir::new("sqlbody-deps");
    let path = dir.path_str("deps.dump");

    let string_body = common::routine("public", 1, "plain", "integer", "select x");
    let mut sql_body = common::routine("public", 2, "atomic", "bigint", "");
    sql_body.source_code = "RETURN (SELECT count(*) AS count FROM public.t)".to_string();
    sql_body.is_sql_body = true;
    sql_body.sql_body_dependencies = SqlBodyDependencies {
        relations: vec!["public.t".to_string()],
        columns: vec!["public.t.id".to_string()],
        routines: vec!["public.g(integer)".to_string()],
        operators: vec!["public.===(integer, integer)".to_string()],
    };
    sql_body.hash();

    let mut dump = empty_dump("shop");
    dump.routines = vec![string_body, sql_body.clone()];
    dump.write_to_file(&path).expect("write dump");

    // Nothing new is written for a routine without them, so a string-body
    // routine keeps the exact shape pgc 1.0.x wrote.
    let written = &dump_json(&path)["routines"];
    assert!(
        written[0].get("sql_body_dependencies").is_none(),
        "{}",
        written[0]
    );
    assert_eq!(
        written[1]["sql_body_dependencies"]["relations"][0],
        serde_json::Value::String("public.t".to_string())
    );

    let reloaded = Dump::read_from_file(&path).await.expect("read dump");
    assert!(reloaded.routines[0].sql_body_dependencies.is_empty());
    assert_eq!(
        reloaded.routines[1].sql_body_dependencies,
        sql_body.sql_body_dependencies
    );
    assert_eq!(reloaded.routines[1].hash, sql_body.hash);
}

#[tokio::test]
async fn sql_body_routine_without_dependencies_loads_with_none() {
    use pgc::dump::routine::SqlBodyDependencies;

    // A dump taken before the field existed (the first 1.1 builds, or any dump
    // edited by hand) still loads; each list defaults to empty, including a
    // partial object.
    let dir = ScratchDir::new("sqlbody-deps-legacy");
    let path = dir.path_str("legacy.dump");
    write_dump_io(
        &path,
        r#"{"schemas": [], "extensions": [], "types": [], "enums": [],
            "sequences": [], "tables": [], "views": [],
            "routines": [
                {"schema": "public", "oid": 7, "name": "atomic", "lang": "sql",
                 "kind": "function", "return_type": "integer", "arguments": "",
                 "arguments_defaults": null, "comment": null,
                 "source_code": "RETURN 1", "is_sql_body": true, "hash": "x"},
                {"schema": "public", "oid": 8, "name": "partial", "lang": "sql",
                 "kind": "function", "return_type": "integer", "arguments": "",
                 "arguments_defaults": null, "comment": null,
                 "source_code": "RETURN 1", "is_sql_body": true, "hash": "y",
                 "sql_body_dependencies": {"routines": ["public.g()"]}}
            ]}"#,
    );

    let dump = Dump::read_from_file(&path)
        .await
        .expect("a dump without sql_body_dependencies must still deserialize");
    assert!(dump.routines[0].is_sql_body);
    assert!(dump.routines[0].sql_body_dependencies.is_empty());
    assert_eq!(
        dump.routines[1].sql_body_dependencies,
        SqlBodyDependencies {
            routines: vec!["public.g()".to_string()],
            ..SqlBodyDependencies::default()
        }
    );
}

// ==== fix:domains tests ====

// pgc 1.0.x hashed each type before its domain constraints and enum labels
// were loaded, so the hash in its dumps ignores both. `read_from_file`
// rehashes every type, which makes such a dump compare correctly — against
// another 1.0.x dump and against one this version wrote.

/// A domain over `integer` in schema `app`, with its hash set the way this
/// version computes it.
fn domains_fixture_type(constraints: &[(&str, &str)]) -> pgc::dump::pg_type::PgType {
    use pgc::dump::pg_type::{DomainConstraint, PgType};
    use sqlx::postgres::types::Oid;
    // `PgType::new` hashes the finished value.
    PgType::new(
        Oid(16500),
        "app".to_string(),
        "amount".to_string(),
        Oid(2200),
        Oid(10),
        String::new(),
        4,
        true,
        b'd' as i8,
        b'N' as i8,
        false,
        true,
        b',' as i8,
        None,
        None,
        None,
        None,
        "domain_in".to_string(),
        "int4out".to_string(),
        Some("domain_recv".to_string()),
        Some("int4send".to_string()),
        None,
        None,
        None,
        b'i' as i8,
        b'p' as i8,
        false,
        Some(Oid(23)),
        Some(-1),
        0,
        None,
        None,
        Some("integer".to_string()),
        Vec::new(),
        constraints
            .iter()
            .map(|(name, definition)| DomainConstraint {
                name: name.to_string(),
                definition: definition.to_string(),
            })
            .collect(),
        None,
    )
}

/// The same type carrying the hash pgc 1.0.x would have stored for it:
/// computed with no domain constraints and no enum labels.
fn domains_with_legacy_hash(mut pg_type: pgc::dump::pg_type::PgType) -> pgc::dump::pg_type::PgType {
    let mut bare = pg_type.clone();
    bare.domain_constraints.clear();
    bare.enum_labels.clear();
    bare.hash();
    pg_type.hash = bare.hash;
    pg_type
}

async fn domains_write_and_read(
    dir: &ScratchDir,
    name: &str,
    pg_type: pgc::dump::pg_type::PgType,
) -> Dump {
    let path = dir.path_str(name);
    let mut dump = empty_dump(name);
    dump.types.push(pg_type);
    dump.write_to_file(&path).expect("write dump");
    Dump::read_from_file(&path).await.expect("read dump")
}

async fn domains_compare(from: Dump, to: Dump) -> String {
    let dir = ScratchDir::new("domains-compare");
    let out = dir.path_str("output.sql");
    let mut comparer = pgc::comparer::core::Comparer::new(
        from,
        to,
        true,
        false,
        true,
        pgc::config::grants_mode::GrantsMode::Ignore,
    );
    comparer.compare().await.expect("compare");
    comparer.save_script(&out).await.expect("save script");
    std::fs::read_to_string(&out).expect("read script")
}

#[tokio::test]
async fn reading_a_1_0_dump_rehashes_types_over_their_domain_constraints() {
    let dir = ScratchDir::new("domains-rehash");
    let current = domains_fixture_type(&[("amount_check", "CHECK ((VALUE > 0))")]);
    let legacy = domains_with_legacy_hash(current.clone());
    assert_ne!(legacy.hash, current.hash);

    let path = dir.path_str("legacy.dump");
    let mut dump = empty_dump("legacy");
    dump.types.push(legacy.clone());
    dump.write_to_file(&path).expect("write dump");
    // The file really carries the stale hash, as a 1.0.x dump does.
    assert_eq!(
        dump_json(&path)["types"][0]["hash"].as_str(),
        legacy.hash.as_deref()
    );

    let reloaded = Dump::read_from_file(&path).await.expect("read dump");
    assert_eq!(reloaded.types[0].hash, current.hash);
    assert_eq!(
        reloaded.types[0].domain_constraints,
        current.domain_constraints
    );
}

#[tokio::test]
async fn reading_a_1_0_dump_rehashes_enums_over_their_labels() {
    let dir = ScratchDir::new("enums-rehash");
    let mut current = domains_fixture_type(&[]);
    current.typname = "status".to_string();
    current.typtype = b'e' as i8;
    current.formatted_basetype = None;
    current.enum_labels = vec!["new".to_string(), "done".to_string()];
    current.hash();
    let legacy = domains_with_legacy_hash(current.clone());
    assert_ne!(legacy.hash, current.hash);

    let reloaded = domains_write_and_read(&dir, "legacy.dump", legacy).await;

    assert_eq!(reloaded.types[0].hash, current.hash);
}

#[tokio::test]
async fn two_1_0_dumps_differing_only_in_a_domain_check_compare_as_changed() {
    let dir = ScratchDir::new("domains-legacy-pair");
    let from = domains_with_legacy_hash(domains_fixture_type(&[(
        "amount_check",
        "CHECK ((VALUE > 0))",
    )]));
    let to = domains_with_legacy_hash(domains_fixture_type(&[(
        "amount_check",
        "CHECK ((VALUE > 5))",
    )]));
    // Identical stored hashes: pgc 1.0.x saw no difference at all.
    assert_eq!(from.hash, to.hash);

    let script = domains_compare(
        domains_write_and_read(&dir, "from.dump", from).await,
        domains_write_and_read(&dir, "to.dump", to).await,
    )
    .await;

    // The drop runs with the types; the re-add waits until after the
    // tables and routines, so the two are apart but in this order.
    let drop = script
        .find("alter domain app.amount drop constraint \"amount_check\";")
        .unwrap_or_else(|| panic!("no drop in:\n{script}"));
    let add = script
        .find("alter domain app.amount add constraint \"amount_check\" CHECK ((VALUE > 5));")
        .unwrap_or_else(|| panic!("no add in:\n{script}"));
    assert!(drop < add, "{script}");
    assert!(
        !script.contains("requires no supported changes"),
        "{script}"
    );
}

#[tokio::test]
async fn a_1_0_dump_against_a_current_dump_of_the_same_domain_is_clean() {
    let dir = ScratchDir::new("domains-mixed");
    let current = domains_fixture_type(&[("amount_check", "CHECK ((VALUE > 0))")]);
    let legacy = domains_with_legacy_hash(current.clone());

    let script = domains_compare(
        domains_write_and_read(&dir, "from.dump", legacy).await,
        domains_write_and_read(&dir, "to.dump", current).await,
    )
    .await;

    assert!(!script.contains("app.amount"), "{script}");
    assert!(
        !script.contains("requires no supported changes"),
        "{script}"
    );
}

// ==== fix:replident tests ====

/// `replica_identity_index` is written only for a USING INDEX table, so every
/// other table serializes exactly as pgc 1.0.x wrote it; a 1.0.x table (no
/// key) loads with `None` and keeps the hash its dump stored.
#[tokio::test]
async fn replica_identity_index_is_optional_in_the_dump_file() {
    let dir = ScratchDir::new("replident");
    let table_json = |path: &str| dump_json(path)["tables"][0].clone();

    let plain_path = dir.path_str("plain.dump");
    let mut dump = empty_dump("plain");
    let mut plain = common::table("public", "t", &["id"]);
    plain.replica_identity = Some("f".to_string());
    plain.hash();
    dump.tables.push(plain);
    dump.write_to_file(&plain_path).expect("write dump");
    assert!(
        table_json(&plain_path)
            .get("replica_identity_index")
            .is_none()
    );

    let using_path = dir.path_str("using.dump");
    let mut dump = empty_dump("using");
    let mut using = common::table("public", "t", &["id"]);
    using.replica_identity = Some("i".to_string());
    using.replica_identity_index = Some("t_id_uk".to_string());
    using.hash();
    let using_hash = using.hash.clone();
    dump.tables.push(using);
    dump.write_to_file(&using_path).expect("write dump");
    let mut json = dump_json(&using_path);
    assert_eq!(
        json["tables"][0].get("replica_identity_index"),
        Some(&serde_json::Value::String("t_id_uk".to_string()))
    );
    let reloaded = Dump::read_from_file(&using_path).await.expect("read dump");
    assert_eq!(
        reloaded.tables[0].replica_identity_index.as_deref(),
        Some("t_id_uk")
    );
    assert_eq!(reloaded.tables[0].hash, using_hash);

    // The same table as pgc 1.0.x wrote it: no index key.
    json["tables"][0]
        .as_object_mut()
        .expect("table object")
        .remove("replica_identity_index");
    let legacy_path = dir.path_str("legacy.dump");
    write_dump_io(&legacy_path, &json.to_string());
    let legacy = Dump::read_from_file(&legacy_path)
        .await
        .expect("a 1.0.x table without replica_identity_index loads");
    assert_eq!(legacy.tables[0].replica_identity.as_deref(), Some("i"));
    assert_eq!(legacy.tables[0].replica_identity_index, None);
    assert_eq!(legacy.tables[0].hash, using_hash, "stored hash is kept");
}

/// `replica_identity_columns` is written only when there are some, is not
/// part of the hash, and a dump without it (pgc 1.0.x, or a table not on
/// USING INDEX) loads with an empty list.
#[tokio::test]
async fn replica_identity_columns_are_optional_in_the_dump_file() {
    let dir = ScratchDir::new("replident_columns");
    let mut table = common::table("public", "t", &["id", "code"]);
    table.replica_identity = Some("i".to_string());
    table.replica_identity_index = Some("t_code_uk".to_string());
    table.hash();
    let hash_without = table.hash.clone();
    table.replica_identity_columns = vec!["code".to_string()];
    table.hash();
    assert_eq!(table.hash, hash_without, "the columns are not hashed");

    let path = dir.path_str("columns.dump");
    let mut dump = empty_dump("columns");
    dump.tables.push(table);
    dump.write_to_file(&path).expect("write dump");
    let mut json = dump_json(&path);
    assert_eq!(
        json["tables"][0].get("replica_identity_columns"),
        Some(&serde_json::json!(["code"]))
    );
    let reloaded = Dump::read_from_file(&path).await.expect("read dump");
    assert_eq!(reloaded.tables[0].replica_identity_columns, vec!["code"]);

    json["tables"][0]
        .as_object_mut()
        .expect("table object")
        .remove("replica_identity_columns");
    let legacy_path = dir.path_str("legacy.dump");
    write_dump_io(&legacy_path, &json.to_string());
    let legacy = Dump::read_from_file(&legacy_path)
        .await
        .expect("a table without replica_identity_columns loads");
    assert!(legacy.tables[0].replica_identity_columns.is_empty());
    assert_eq!(legacy.tables[0].hash, hash_without, "stored hash is kept");

    // An empty list is not written at all.
    let plain_path = dir.path_str("plain.dump");
    let mut dump = empty_dump("plain");
    let mut plain = common::table("public", "t", &["id"]);
    plain.hash();
    dump.tables.push(plain);
    dump.write_to_file(&plain_path).expect("write dump");
    assert!(
        dump_json(&plain_path)["tables"][0]
            .get("replica_identity_columns")
            .is_none()
    );
}

// ==== fix:comments tests ====

/// A materialized view named as `build_materialized_views_query` reads it.
fn comments_matview(schema: &str, name: &str, owner: &str) -> pgc::dump::view::View {
    let mut view = common::materialized_view(schema, name, " SELECT 1 AS a;", &[]);
    view.owner = owner.to_string();
    view.hash();
    view
}

async fn comments_write_and_read(
    dir: &ScratchDir,
    name: &str,
    server_version_num: Option<i32>,
    views: Vec<pgc::dump::view::View>,
) -> Dump {
    let path = dir.path_str(name);
    let mut dump = empty_dump(name);
    dump.server_version_num = server_version_num;
    dump.views = views;
    dump.write_to_file(&path).expect("write dump");
    Dump::read_from_file(&path).await.expect("read dump")
}

/// pgc 1.0.x read a materialized view's schema, name and owner raw, where a
/// current dump reads them through `quote_ident`. Such a dump loads with the
/// names quoted, so it names the view as the SQL needs it and matches a
/// current dump of the same view.
#[tokio::test]
async fn a_1_0_dump_names_its_materialized_views_as_a_current_dump_does() {
    let dir = ScratchDir::new("comments-legacy-matview");
    let legacy_plain = comments_matview("app", "plain_mv", "postgres");
    let from = comments_write_and_read(
        &dir,
        "legacy.dump",
        None,
        vec![
            comments_matview("App", "MyMv", "Owner"),
            legacy_plain.clone(),
        ],
    )
    .await;
    let to = comments_write_and_read(
        &dir,
        "current.dump",
        Some(190000),
        vec![
            comments_matview("\"App\"", "\"MyMv\"", "\"Owner\""),
            comments_matview("app", "plain_mv", "postgres"),
        ],
    )
    .await;

    assert_eq!(
        (
            from.views[0].schema.as_str(),
            from.views[0].name.as_str(),
            from.views[0].owner.as_str()
        ),
        ("\"App\"", "\"MyMv\"", "\"Owner\"")
    );
    assert_eq!(from.views[0].hash, to.views[0].hash);
    assert_eq!(from.views[1].name, "plain_mv");
    assert_eq!(from.views[1].hash, legacy_plain.hash, "stored hash is kept");

    let out = dir.path_str("output.sql");
    let mut comparer = pgc::comparer::core::Comparer::new(
        from,
        to,
        true,
        false,
        true,
        pgc::config::grants_mode::GrantsMode::Ignore,
    );
    comparer.compare().await.expect("compare");
    comparer.save_script(&out).await.expect("save script");
    let script = std::fs::read_to_string(&out).expect("read script");
    assert!(
        !script.contains("MyMv") && !script.contains("plain_mv"),
        "{script}"
    );
}

/// Only a dump without `server_version_num` is a 1.0.x dump: a current one
/// already stores the names quoted and is read as written.
#[tokio::test]
async fn a_current_dump_keeps_its_materialized_view_names() {
    let dir = ScratchDir::new("comments-current-matview");
    let stored = comments_matview("app", "Kept", "Owner");
    let reloaded =
        comments_write_and_read(&dir, "current.dump", Some(140000), vec![stored.clone()]).await;
    assert_eq!(reloaded.views[0].name, "Kept");
    assert_eq!(reloaded.views[0].owner, "Owner");
    assert_eq!(reloaded.views[0].hash, stored.hash);
}

// ==== pre-cascade tests ====

/// A view exactly as pgc 1.0.27 wrote it: no `routine_dependencies` key.
const LEGACY_CALLING_VIEW: &str = r#"{
    "schema": "public", "name": "v_items",
    "definition": " SELECT items.id,\n    public.norm(items.code) AS n\n   FROM public.items;",
    "table_relation": ["public.items"], "owner": "postgres", "comment": null,
    "is_materialized": false, "hash": null
}"#;

#[tokio::test]
async fn pre_cascade_view_without_routine_dependencies_loads_as_unknown() {
    let dir = ScratchDir::new("pre-cascade-legacy-view");
    let path = dir.path_str("legacy.dump");
    write_dump_io(
        &path,
        &format!(
            r#"{{"schemas": [], "extensions": [], "types": [], "enums": [],
                "sequences": [], "routines": [], "tables": [],
                "views": [{LEGACY_CALLING_VIEW}]}}"#
        ),
    );
    let dump = Dump::read_from_file(&path)
        .await
        .expect("a pgc 1.0.x view must still deserialize");
    let view = &dump.views[0];
    assert_eq!(view.routine_dependencies, None);

    // The field is not hashed: the same view with it recorded hashes alike.
    let mut legacy = view.clone();
    legacy.hash();
    let mut current = legacy.clone();
    current.routine_dependencies = Some(vec!["public.norm(t text)".to_string()]);
    current.hash();
    assert_eq!(legacy.hash, current.hash);
}

#[tokio::test]
async fn pre_cascade_routine_dependencies_round_trip() {
    let dir = ScratchDir::new("pre-cascade-roundtrip");
    let path = dir.path_str("current.dump");
    let mut calling = common::view("public", "v_items", "select public.norm(code)", &[]);
    calling.routine_dependencies = Some(vec!["public.norm(t text)".to_string()]);
    let mut plain = common::view("public", "v_one", "select 1", &[]);
    plain.routine_dependencies = Some(Vec::new());
    let legacy = common::view("public", "v_legacy", "select 2", &[]);

    let mut dump = empty_dump("shop");
    dump.views = vec![calling.clone(), plain.clone(), legacy.clone()];
    dump.write_to_file(&path).expect("write dump");

    // A view whose dependencies are unknown is written without the key.
    let written = &dump_json(&path)["views"];
    assert!(
        written[2].get("routine_dependencies").is_none(),
        "{}",
        written[2]
    );
    assert_eq!(written[1]["routine_dependencies"], serde_json::json!([]));

    let reloaded = Dump::read_from_file(&path).await.expect("read dump");
    for (before, after) in [&calling, &plain, &legacy].into_iter().zip(&reloaded.views) {
        assert_eq!(before.routine_dependencies, after.routine_dependencies);
        assert_eq!(before.hash, after.hash);
    }
}

/// FROM and TO for `public.norm(t text)` changing its return type, called
/// by `public.v_items`, which the materialized view `public.mv_items` reads;
/// FROM recording the routines `v_items` calls when `recorded`.
fn pre_cascade_dumps(recorded: bool) -> (Dump, Dump) {
    let mut dumps = (empty_dump("from"), empty_dump("to"));
    for (dump, return_type) in [(&mut dumps.0, "text"), (&mut dumps.1, "character varying")] {
        let mut norm = common::routine("public", 1, "norm", return_type, "select lower(t)");
        norm.arguments = "t text".to_string();
        norm.hash();
        dump.routines.push(norm);
        dump.tables
            .push(common::table("public", "items", &["id", "code"]));
        let mut calling = common::view(
            "public",
            "v_items",
            " SELECT items.id,\n    public.norm(items.code) AS n\n   FROM public.items;",
            &["public.items"],
        );
        calling.routine_dependencies = Some(vec!["public.norm(t text)".to_string()]);
        dump.views.push(calling);
        dump.views.push(common::materialized_view(
            "public",
            "mv_items",
            " SELECT v_items.id\n   FROM public.v_items;",
            &["public.v_items"],
        ));
    }
    if !recorded {
        // What a pgc 1.0.x FROM dump holds.
        dumps.0.server_version_num = None;
        for view in &mut dumps.0.views {
            view.routine_dependencies = None;
        }
    }
    dumps
}

async fn pre_cascade_script(from: Dump, to: Dump, dir: &ScratchDir) -> String {
    let out = dir.path_str("output.sql");
    let mut comparer = pgc::comparer::core::Comparer::new(
        from,
        to,
        true,
        false,
        true,
        pgc::config::grants_mode::GrantsMode::Ignore,
    );
    comparer.compare().await.expect("compare");
    comparer.save_script(&out).await.expect("save script");
    std::fs::read_to_string(&out).expect("read script")
}

#[tokio::test]
async fn pre_cascade_a_view_with_a_reader_goes_ahead_only_when_the_dump_says_what_it_calls() {
    let dir = ScratchDir::new("pre-cascade-pipeline");
    // Recorded: the view calling the routine goes ahead of everything with
    // the materialized view reading it, and both come back after it.
    let (from, to) = pre_cascade_dumps(true);
    let script = pre_cascade_script(from, to, &dir).await;
    let at = |needle: &str| {
        script
            .find(needle)
            .unwrap_or_else(|| panic!("expected {needle:?} in:\n{script}"))
    };
    assert!(
        at("drop materialized view if exists public.mv_items;")
            < at("drop view if exists public.v_items;")
    );
    assert!(
        at("drop view if exists public.v_items;")
            < at("drop function if exists public.norm (t text) cascade;")
    );
    assert!(
        at("CREATE OR REPLACE VIEW public.v_items as")
            < at("create materialized view public.mv_items as")
    );

    // A pgc 1.0.x FROM dump cannot say: the view is left to the routine's
    // CASCADE and re-created after it, as that release did.
    let (from, to) = pre_cascade_dumps(false);
    let script = pre_cascade_script(from, to, &dir).await;
    assert!(
        !script.contains("drop view if exists public.v_items;"),
        "{script}"
    );
    assert!(
        script.contains("CREATE OR REPLACE VIEW public.v_items as"),
        "{script}"
    );
}

// ==== fix:pre-grants tests ====

// pgc 1.1 tells a routine's or type's NULL ACL (the default privileges, which
// give PUBLIC EXECUTE / USAGE) from an empty one (everything revoked): both
// dump as an empty `acl`, so `acl_explicitly_empty` marks the second. It is
// written only when true, a dump without it (every pgc 1.0.x dump) loads as
// NULL ACLs, and no hash depends on it.

#[tokio::test]
async fn routine_and_type_written_by_pgc_1_0_x_load_with_null_acls() {
    let dir = ScratchDir::new("pregrants-legacy");
    let path = dir.path_str("legacy.dump");
    let legacy_type = serde_json::to_string(&domains_fixture_type(&[])).expect("type json");
    assert!(
        !legacy_type.contains("acl_explicitly_empty"),
        "{legacy_type}"
    );
    write_dump_io(
        &path,
        &format!(
            r#"{{"schemas": [], "extensions": [], "enums": [], "sequences": [],
                "types": [{legacy_type}],
                "routines": [{LEGACY_STRING_BODY_ROUTINE}],
                "tables": [], "views": []}}"#
        ),
    );

    let dump = Dump::read_from_file(&path)
        .await
        .expect("a dump without acl_explicitly_empty must still deserialize");
    assert!(!dump.routines[0].acl_explicitly_empty);
    assert!(!dump.types[0].acl_explicitly_empty);
    // The stored 1.0.27 routine hash still matches.
    let mut rehashed = dump.routines[0].clone();
    rehashed.hash();
    assert_eq!(rehashed.hash, dump.routines[0].hash);
}

#[tokio::test]
async fn acl_explicitly_empty_round_trips_without_touching_hashes() {
    let dir = ScratchDir::new("pregrants-flag");
    let path = dir.path_str("flag.dump");

    let null_acl = common::routine("public", 1, "null_acl", "integer", "select 1");
    let mut revoked = common::routine("public", 2, "revoked", "integer", "select 1");
    revoked.acl_explicitly_empty = true;
    let routine_hash = revoked.hash.clone();
    revoked.hash();
    assert_eq!(revoked.hash, routine_hash, "the flag is not hashed");

    let null_type = domains_fixture_type(&[]);
    let mut revoked_type = domains_fixture_type(&[]);
    revoked_type.typname = "revoked".to_string();
    revoked_type.acl_explicitly_empty = true;
    let mut unflagged = revoked_type.clone();
    unflagged.acl_explicitly_empty = false;
    unflagged.hash();
    revoked_type.hash();
    assert_eq!(revoked_type.hash, unflagged.hash, "the flag is not hashed");

    let mut dump = empty_dump("shop");
    dump.routines = vec![null_acl, revoked];
    dump.types = vec![null_type, revoked_type];
    dump.write_to_file(&path).expect("write dump");

    // Written only when true: a NULL ACL is written exactly as before.
    let written = dump_json(&path);
    assert!(written["routines"][0].get("acl_explicitly_empty").is_none());
    assert_eq!(
        written["routines"][1]["acl_explicitly_empty"],
        serde_json::Value::Bool(true)
    );
    assert!(written["types"][0].get("acl_explicitly_empty").is_none());
    assert_eq!(
        written["types"][1]["acl_explicitly_empty"],
        serde_json::Value::Bool(true)
    );

    let reloaded = Dump::read_from_file(&path).await.expect("read dump");
    assert!(!reloaded.routines[0].acl_explicitly_empty);
    assert!(reloaded.routines[1].acl_explicitly_empty);
    assert!(!reloaded.types[0].acl_explicitly_empty);
    assert!(reloaded.types[1].acl_explicitly_empty);
}

#[tokio::test]
async fn subscription_publication_names_stay_raw_in_the_dump() {
    // subpublications is dumped raw (quoting happens only in the DDL), so a
    // pgc 1.0.x subscription on a name that needs quotes keeps its hash.
    let dir = ScratchDir::new("pregrants-subpub");
    let path = dir.path_str("sub.dump");
    let mut sub = pgc::dump::publication::Subscription {
        name: "s".to_string(),
        owner: "postgres".to_string(),
        connection: "host=primary".to_string(),
        server: None,
        publications: vec!["MyPub".to_string(), "pub two".to_string()],
        enabled: false,
        slot_name: Default::default(),
        retain_dead_tuples: false,
        max_retention_duration: 0,
        wal_receiver_timeout: None,
        comment: None,
        hash: None,
    };
    sub.hash();
    let mut dump = empty_dump("shop");
    dump.subscriptions = vec![sub.clone()];
    dump.write_to_file(&path).expect("write dump");
    assert_eq!(
        dump_json(&path)["subscriptions"][0]["publications"],
        serde_json::json!(["MyPub", "pub two"])
    );
    let reloaded = Dump::read_from_file(&path).await.expect("read dump");
    assert_eq!(reloaded.subscriptions[0].hash, sub.hash);
    assert!(
        reloaded.subscriptions[0].get_script().contains(
            "PUBLICATION \"MyPub\", \"pub two\" WITH (connect = false, enabled = false);"
        )
    );
}

// ==== pre-columns tests ====

// pgc 1.1 records a partition's clones of its parent's row triggers apart
// from the partition's own triggers (`Table::trigger_clones`), and only when
// their state is their own. pgc 1.0.x listed every clone PostgreSQL 15+
// shows among the partition's own triggers; such a dump is read into the
// current shape, so comparing it with a current dump of the same schema
// emits nothing — and never a `DROP TRIGGER` on a clone, which PostgreSQL
// refuses.

/// A row trigger `name` on `public.<table>` in the given enabled state.
fn pre_columns_trigger(
    name: &str,
    table: &str,
    enabled: &str,
) -> pgc::dump::table_trigger::TableTrigger {
    pgc::dump::table_trigger::TableTrigger {
        oid: sqlx::postgres::types::Oid(1),
        name: name.to_string(),
        definition: format!(
            "CREATE TRIGGER {name} BEFORE INSERT ON public.{table} FOR EACH ROW EXECUTE FUNCTION public.trg()"
        ),
        enabled: enabled.to_string(),
        comment: None,
    }
}

/// `public.pt` with a row trigger, and partitions `pt_1` (clone in the
/// parent's state) and `pt_2` (clone disabled on its own). With `legacy`
/// the clones are listed among the partitions' own triggers, as pgc 1.0.x
/// wrote them; otherwise as pgc 1.1 records them.
fn pre_columns_tables(legacy: bool) -> Vec<pgc::dump::table::Table> {
    let mut parent = common::table("public", "pt", &["id"]);
    parent.partition_key = Some("RANGE (id)".to_string());
    parent.triggers = vec![pre_columns_trigger("trg", "pt", "O")];
    let mut tables = vec![parent];
    for (name, enabled) in [("pt_1", "O"), ("pt_2", "D")] {
        let mut partition = common::table("public", name, &["id"]);
        partition.partition_of = Some("public.pt".to_string());
        partition.partition_bound = Some("FOR VALUES FROM (0) TO (10)".to_string());
        let clone = pre_columns_trigger("trg", name, enabled);
        if legacy {
            partition.triggers = vec![clone];
        } else if enabled != "O" {
            partition.trigger_clones = vec![clone];
        }
        tables.push(partition);
    }
    for table in &mut tables {
        table.hash();
    }
    tables
}

#[tokio::test]
async fn pre_columns_partition_trigger_clones_of_a_1_0_dump_are_read_apart() {
    let dir = ScratchDir::new("pre-columns-legacy-clones");
    let legacy_path = dir.path_str("legacy.dump");
    let mut legacy = empty_dump("legacy");
    legacy.tables = pre_columns_tables(true);
    // No server_version_num: written by pgc 1.0.x.
    legacy
        .write_to_file(&legacy_path)
        .expect("write legacy dump");
    let json = dump_json(&legacy_path);
    assert!(json.get("server_version_num").is_none());
    assert!(json["tables"][2].get("trigger_clones").is_none());

    let from = Dump::read_from_file(&legacy_path)
        .await
        .expect("read legacy dump");
    let current = pre_columns_tables(false);
    for (read, expected) in from.tables.iter().zip(&current) {
        assert_eq!(read.name, expected.name);
        assert_eq!(
            read.triggers.len(),
            expected.triggers.len(),
            "{}",
            read.name
        );
        assert_eq!(
            read.trigger_clones.len(),
            expected.trigger_clones.len(),
            "{}",
            read.name
        );
        assert_eq!(read.hash, expected.hash, "{}", read.name);
    }

    let mut to = empty_dump("current");
    to.server_version_num = Some(180000);
    to.tables = current;
    let out = dir.path_str("output.sql");
    let mut comparer = pgc::comparer::core::Comparer::new(
        from,
        to,
        true,
        false,
        true,
        pgc::config::grants_mode::GrantsMode::Ignore,
    );
    comparer.compare().await.expect("compare");
    comparer.save_script(&out).await.expect("save script");
    let script = std::fs::read_to_string(&out).expect("read script");
    common::assert_no_ddl(
        &script,
        "a 1.0.x dump against a current dump of the same schema",
    );
    assert!(!script.contains("drop trigger"), "{script}");
}

#[tokio::test]
async fn pre_columns_trigger_clones_round_trip_and_stay_out_of_the_file_when_none() {
    let dir = ScratchDir::new("pre-columns-clones");
    let path = dir.path_str("current.dump");
    let mut dump = empty_dump("current");
    dump.server_version_num = Some(190000);
    dump.tables = pre_columns_tables(false);
    dump.write_to_file(&path).expect("write dump");

    let json = dump_json(&path);
    let tables = json["tables"].as_array().expect("tables array");
    assert!(tables[0].get("trigger_clones").is_none(), "{}", tables[0]);
    assert!(tables[1].get("trigger_clones").is_none(), "{}", tables[1]);
    assert_eq!(tables[2]["trigger_clones"][0]["enabled"], "D");

    let reloaded = Dump::read_from_file(&path).await.expect("read dump");
    for (before, after) in dump.tables.iter().zip(&reloaded.tables) {
        assert_eq!(before.hash, after.hash, "{}", before.name);
        assert_eq!(
            before.trigger_clones, after.trigger_clones,
            "{}",
            before.name
        );
        assert_eq!(before.triggers, after.triggers, "{}", before.name);
    }
}

// A partition whose parent the dump does not have (it lives in a schema
// outside the dump) cannot be told apart from its clones by the parent's
// triggers: a current dump records every clone of it, and a pgc 1.0.x dump
// (PostgreSQL 15+) listed them among its own triggers. Compared with a
// current dump of the same schema, such a 1.0.x dump must emit nothing — in
// particular no `DROP TRIGGER` on a clone, which PostgreSQL refuses.
#[tokio::test]
async fn pre_columns_1_0_dump_of_a_partition_of_an_undumped_parent_reads_clean() {
    let dir = ScratchDir::new("pre-columns-orphan-clones");
    let partition = |legacy: bool| {
        let mut table = common::table("public", "p1", &["id"]);
        table.partition_of = Some("outside.p".to_string());
        table.partition_bound = Some("FOR VALUES IN (1)".to_string());
        let clones = vec![
            pre_columns_trigger("trg", "p1", "O"),
            pre_columns_trigger("trg_off", "p1", "D"),
        ];
        if legacy {
            table.triggers = clones;
        } else {
            table.trigger_clones = clones;
        }
        table.hash();
        table
    };
    let legacy_path = dir.path_str("legacy.dump");
    let mut legacy = empty_dump("legacy");
    legacy.tables = vec![partition(true)];
    legacy
        .write_to_file(&legacy_path)
        .expect("write legacy dump");
    let from = Dump::read_from_file(&legacy_path)
        .await
        .expect("read legacy dump");
    // Nothing to judge them by on reading: still the partition's own.
    assert_eq!(from.tables[0].triggers.len(), 2);

    let mut to = empty_dump("current");
    to.server_version_num = Some(180000);
    to.tables = vec![partition(false)];
    // Written and read back, the current dump keeps every clone.
    let current_path = dir.path_str("current.dump");
    to.write_to_file(&current_path).expect("write current dump");
    let to = Dump::read_from_file(&current_path)
        .await
        .expect("read current dump");
    assert_eq!(to.tables[0].trigger_clones.len(), 2);

    let out = dir.path_str("output.sql");
    let mut comparer = pgc::comparer::core::Comparer::new(
        from,
        to,
        true,
        false,
        true,
        pgc::config::grants_mode::GrantsMode::Ignore,
    );
    comparer.compare().await.expect("compare");
    comparer.save_script(&out).await.expect("save script");
    let script = std::fs::read_to_string(&out).expect("read script");
    common::assert_no_ddl(
        &script,
        "a 1.0.x dump of a partition of an undumped parent against a current dump",
    );
    assert!(!script.contains("drop trigger"), "{script}");
}

// pgc 1.1 records the triggers that name a column (`UPDATE OF`, `WHEN`) in
// the column-dependent graph. The kind round-trips by name; a dump without
// such an edge — any older dump — still loads.
#[tokio::test]
async fn pre_columns_trigger_column_dependents_round_trip() {
    let dir = ScratchDir::new("pre-columns-trigger-edges");
    let path = dir.path_str("current.dump");
    let mut dump = empty_dump("current");
    dump.server_version_num = Some(190000);
    dump.column_dependents.push(ColumnDependent {
        schema: "public".to_string(),
        table: "t".to_string(),
        column: "g".to_string(),
        kind: ColumnDependentKind::Trigger,
        dep_schema: "public".to_string(),
        dep_table: "t".to_string(),
        dep_name: "t_upd".to_string(),
    });
    dump.write_to_file(&path).expect("write dump");
    assert_eq!(dump_json(&path)["column_dependents"][0]["kind"], "Trigger");
    let reloaded = Dump::read_from_file(&path).await.expect("read dump");
    assert_eq!(
        reloaded.column_dependents[0].kind,
        ColumnDependentKind::Trigger
    );
    assert_eq!(reloaded.column_dependents[0].dep_name, "t_upd");
}

// ==== pre-clear tests ====

/// `Dump::drop_dependencies` is filled only by `inspect` (the clear command),
/// so a dump file never carries it: the key is absent from what `dump` writes,
/// and a dump without it — every dump ever written — loads with none.
#[tokio::test]
async fn drop_dependencies_stay_out_of_dump_files_and_default_to_none() {
    let dir = ScratchDir::new("drop-deps");
    let path = dir.path_str("plain.dump");
    populated_dump("shop")
        .write_to_file(&path)
        .expect("write dump");
    let json = dump_json(&path);
    assert!(json.get("drop_dependencies").is_none(), "{json}");
    let reloaded = Dump::read_from_file(&path).await.expect("read dump");
    assert!(reloaded.drop_dependencies.is_empty());

    let legacy_path = dir.path_str("legacy.dump");
    write_dump_io(
        &legacy_path,
        r#"{"schemas": [], "extensions": [], "types": [], "enums": [],
            "sequences": [], "routines": [], "tables": [], "views": []}"#,
    );
    let legacy = Dump::read_from_file(&legacy_path)
        .await
        .expect("legacy dump loads");
    assert!(legacy.drop_dependencies.is_empty());
}

/// Should one ever be written, it round-trips, with a breaker only where
/// there is one.
#[tokio::test]
async fn drop_dependencies_round_trip_when_present() {
    use pgc::dump::drop_order::DropDependency;

    let dir = ScratchDir::new("drop-deps-rt");
    let path = dir.path_str("deps.dump");
    let mut dump = empty_dump("shop");
    dump.drop_dependencies = vec![
        DropDependency::new("type:app.sku", "proc:app.is_sku(t text)"),
        DropDependency::new("rel:app.t", "proc:app.f()")
            .with_breaker("alter table if exists app.t drop constraint if exists c"),
    ];
    dump.write_to_file(&path).expect("write dump");
    let json = dump_json(&path);
    assert_eq!(
        json["drop_dependencies"][0],
        serde_json::json!({"before": "type:app.sku", "after": "proc:app.is_sku(t text)"})
    );
    let reloaded = Dump::read_from_file(&path).await.expect("read dump");
    assert_eq!(reloaded.drop_dependencies, dump.drop_dependencies);
}

// ==== r3-gencol tests ====

/// `TableColumn::generation_blocks_retype` is written only when true (a
/// PostgreSQL 14 generated column whose expression uses a function or
/// operator), so every other dump is the JSON it was before; a dump without
/// the key — pgc 1.0.x, or a later server — loads as `false`. It is a fact
/// about the dumped server, so it leaves the table hash alone.
#[tokio::test]
async fn generation_blocks_retype_is_written_only_when_true_and_defaults_to_false() {
    let dir = ScratchDir::new("gen-blocks-retype");
    let path = dir.path_str("gen.dump");
    let mut dump = empty_dump("shop");
    let mut table = common::table("public", "t", &["id", "g"]);
    let plain_hash = table.hash.clone();
    table.columns[1].generation_blocks_retype = true;
    table.hash();
    assert_eq!(table.hash, plain_hash, "the flag must not change the hash");
    dump.tables.push(table);
    dump.write_to_file(&path).expect("write dump");

    let json = dump_json(&path);
    let columns = &json["tables"][0]["columns"];
    assert!(
        columns[0].get("generation_blocks_retype").is_none(),
        "{json}"
    );
    assert_eq!(
        columns[1]["generation_blocks_retype"],
        serde_json::json!(true)
    );
    let reloaded = Dump::read_from_file(&path).await.expect("read dump");
    assert!(!reloaded.tables[0].columns[0].generation_blocks_retype);
    assert!(reloaded.tables[0].columns[1].generation_blocks_retype);

    // Without the key, as every older dump has it.
    let mut legacy = json.clone();
    legacy["tables"][0]["columns"][1]
        .as_object_mut()
        .expect("column object")
        .remove("generation_blocks_retype");
    let legacy_path = dir.path_str("legacy.dump");
    write_dump_io(&legacy_path, &legacy.to_string());
    let loaded = Dump::read_from_file(&legacy_path)
        .await
        .expect("a column without the key loads");
    assert!(!loaded.tables[0].columns[1].generation_blocks_retype);
    assert_eq!(loaded.tables[0].hash, plain_hash);
}

/// `TableColumn::generation_routine_dropped` is the comparer's working note,
/// never part of a dump: not written even when set, and `false` on load.
#[tokio::test]
async fn generation_routine_dropped_is_never_written() {
    let dir = ScratchDir::new("gen-routine-dropped");
    let path = dir.path_str("gen.dump");
    let mut dump = empty_dump("shop");
    let mut table = common::table("public", "t", &["id", "g"]);
    table.columns[1].generation_blocks_retype = true;
    table.columns[1].generation_routine_dropped = true;
    dump.tables.push(table);
    dump.write_to_file(&path).expect("write dump");

    let json = dump_json(&path);
    assert!(
        json["tables"][0]["columns"][1]
            .get("generation_routine_dropped")
            .is_none(),
        "{json}"
    );
    let reloaded = Dump::read_from_file(&path).await.expect("read dump");
    assert!(reloaded.tables[0].columns[1].generation_blocks_retype);
    assert!(!reloaded.tables[0].columns[1].generation_routine_dropped);
}

// ==== r3-ddl tests ====

/// `app.t` with a `text` column collated as `pg_catalog."C"`, and the hash a
/// pgc 1.0.x dump stored for it: computed without the collation.
fn r3_ddl_collated_table() -> (pgc::dump::table::Table, Option<String>) {
    let mut table = common::table("app", "t", &["id", "label"]);
    let label = &mut table.columns[1];
    label.data_type = "text".to_string();
    label.numeric_precision = None;
    label.numeric_precision_radix = None;
    label.numeric_scale = None;
    table.hash();
    let hash_without_collation = table.hash.clone();
    let label = &mut table.columns[1];
    label.collation_catalog = Some("postgres".to_string());
    label.collation_schema = Some("pg_catalog".to_string());
    label.collation_name = Some("C".to_string());
    table.hash();
    (table, hash_without_collation)
}

/// A 1.0.x dump (no `server_version_num`) of a table with a collated column
/// stored a hash that leaves the collation out; it is rehashed on load, so
/// it matches a current dump of the same table and the compare is empty.
/// Materialized views named after column-name and type/function-name
/// keywords are quoted on load the way `quote_ident` stores them.
#[tokio::test]
async fn a_1_0_dump_matches_collated_tables_and_keyword_named_matviews() {
    let dir = ScratchDir::new("r3-ddl-legacy");
    let (current_table, legacy_hash) = r3_ddl_collated_table();
    assert_ne!(current_table.hash, legacy_hash);

    let mut legacy_table = current_table.clone();
    legacy_table.hash = legacy_hash;
    let mut legacy = empty_dump("legacy");
    legacy.tables = vec![legacy_table];
    legacy.views = vec![
        comments_matview("app", "left", "postgres"),
        comments_matview("app", "int", "postgres"),
        comments_matview("app", "position", "postgres"),
    ];
    let legacy_path = dir.path_str("legacy.dump");
    legacy
        .write_to_file(&legacy_path)
        .expect("write legacy dump");

    let mut current = empty_dump("current");
    current.server_version_num = Some(180000);
    current.tables = vec![current_table.clone()];
    current.views = vec![
        comments_matview("app", "\"left\"", "postgres"),
        comments_matview("app", "\"int\"", "postgres"),
        comments_matview("app", "\"position\"", "postgres"),
    ];
    let current_path = dir.path_str("current.dump");
    current
        .write_to_file(&current_path)
        .expect("write current dump");

    let from = Dump::read_from_file(&legacy_path)
        .await
        .expect("read legacy");
    let to = Dump::read_from_file(&current_path)
        .await
        .expect("read current");
    assert_eq!(from.tables[0].hash, current_table.hash);
    let names: Vec<&str> = from.views.iter().map(|v| v.name.as_str()).collect();
    assert_eq!(names, ["\"left\"", "\"int\"", "\"position\""]);
    for (a, b) in from.views.iter().zip(&to.views) {
        assert_eq!(a.hash, b.hash, "{}", a.name);
    }

    let out = dir.path_str("output.sql");
    let mut comparer = pgc::comparer::core::Comparer::new(
        from,
        to,
        true,
        false,
        true,
        pgc::config::grants_mode::GrantsMode::Full,
    );
    comparer.compare().await.expect("compare");
    comparer.save_script(&out).await.expect("save script");
    let script = std::fs::read_to_string(&out).expect("read script");
    common::assert_no_ddl(
        &script,
        "1.0.x FROM against a current TO of the same schema",
    );
}

/// A current dump is read as written: its table hashes already hold the
/// collation, and a stored hash is never recomputed.
#[tokio::test]
async fn a_current_dump_keeps_the_stored_hash_of_a_collated_table() {
    let dir = ScratchDir::new("r3-ddl-current");
    let (mut table, _) = r3_ddl_collated_table();
    table.hash = Some("stored".to_string());
    let mut dump = empty_dump("current");
    dump.server_version_num = Some(140000);
    dump.tables = vec![table];
    let path = dir.path_str("current.dump");
    dump.write_to_file(&path).expect("write dump");
    let reloaded = Dump::read_from_file(&path).await.expect("read dump");
    assert_eq!(reloaded.tables[0].hash.as_deref(), Some("stored"));
    // The collation's raw schema and name are what the file holds.
    let json = dump_json(&path);
    let label = &json["tables"][0]["columns"][1];
    assert_eq!(label["collation_schema"], "pg_catalog");
    assert_eq!(label["collation_name"], "C");
}

/// A 1.0.x dump does not record its server's version, so a materialized
/// view named after a keyword only PostgreSQL 16 or 17 quotes loads bare,
/// as PostgreSQL 14 and 15 store it; one that every version quotes (`left`)
/// loads quoted. Compared with a current dump, the view follows that dump's
/// server: nothing to do against PostgreSQL 15 (bare) nor 17 (quoted). The
/// load quoted the newer keywords whatever the server, so a 1.0.x dump of
/// PostgreSQL 14–16 had them dropped and recreated (review of r3-ddl).
#[tokio::test]
async fn a_1_0_dump_spells_newer_keyword_matview_names_as_the_other_dumps_server() {
    let dir = ScratchDir::new("r3-ddl-legacy-keywords");
    let legacy_names = ["json_object", "merge_action", "json", "left"];
    let from_views = || {
        legacy_names
            .iter()
            .map(|name| comments_matview("app", name, "postgres"))
            .collect::<Vec<_>>()
    };
    let from = comments_write_and_read(&dir, "legacy.dump", None, from_views()).await;
    let names: Vec<&str> = from.views.iter().map(|v| v.name.as_str()).collect();
    assert_eq!(names, ["json_object", "merge_action", "json", "\"left\""]);

    for (version, to_names) in [
        (150000, ["json_object", "merge_action", "json", "\"left\""]),
        (
            170000,
            [
                "\"json_object\"",
                "\"merge_action\"",
                "\"json\"",
                "\"left\"",
            ],
        ),
    ] {
        let from = comments_write_and_read(&dir, "legacy.dump", None, from_views()).await;
        let to = comments_write_and_read(
            &dir,
            &format!("current-{version}.dump"),
            Some(version),
            to_names
                .iter()
                .map(|name| comments_matview("app", name, "postgres"))
                .collect(),
        )
        .await;
        let out = dir.path_str(&format!("output-{version}.sql"));
        let mut comparer = pgc::comparer::core::Comparer::new(
            from,
            to,
            true,
            false,
            true,
            pgc::config::grants_mode::GrantsMode::Ignore,
        );
        comparer.compare().await.expect("compare");
        comparer.save_script(&out).await.expect("save script");
        let script = std::fs::read_to_string(&out).expect("read script");
        assert!(
            !script.contains("materialized view"),
            "PostgreSQL {version}:\n{script}"
        );
    }
}

// ==== r3:types tests ====

/// A type of kind `typtype` in schema `app`, as `PgType::new` builds it:
/// none of the fields pgc 1.1.0 added are set.
fn r3t_type(name: &str, typtype: u8) -> pgc::dump::pg_type::PgType {
    use pgc::dump::pg_type::PgType;
    use sqlx::postgres::types::Oid;
    PgType::new(
        Oid(17100),
        "app".to_string(),
        name.to_string(),
        Oid(2200),
        Oid(10),
        "postgres".to_string(),
        -1,
        false,
        typtype as i8,
        b'C' as i8,
        false,
        true,
        b',' as i8,
        None,
        None,
        None,
        None,
        "record_in".to_string(),
        "record_out".to_string(),
        None,
        None,
        None,
        None,
        None,
        b'd' as i8,
        b'x' as i8,
        false,
        None,
        None,
        0,
        None,
        None,
        None,
        Vec::new(),
        Vec::new(),
        None,
    )
}

/// `app.addr (street text, zip integer)` and `app.span` (a range over
/// integer with multirange `span_multirange`), hashed.
fn r3t_types() -> Vec<pgc::dump::pg_type::PgType> {
    use pgc::dump::pg_type::CompositeAttribute;
    let mut composite = r3t_type("addr", b'c');
    composite.composite_attributes = vec![
        CompositeAttribute {
            name: "street".to_string(),
            data_type: "text".to_string(),
            collation: None,
        },
        CompositeAttribute {
            name: "zip".to_string(),
            data_type: "integer".to_string(),
            collation: None,
        },
    ];
    composite.hash();
    let mut range = r3t_type("span", b'r');
    range.range_subtype = Some("integer".to_string());
    range.range_opclass = Some("pg_catalog.int4_ops".to_string());
    range.multirange_name = Some("span_multirange".to_string());
    range.hash();
    vec![composite, range]
}

#[tokio::test]
async fn r3t_types_written_by_pgc_1_0_x_load_without_usage_or_multirange_schema() {
    let dir = ScratchDir::new("r3t-legacy");
    let path = dir.path_str("legacy.dump");
    let types = r3t_types();
    // Without the new fields a type serializes exactly as pgc 1.0.x wrote it.
    let json = serde_json::to_string(&types).expect("serialize types");
    for key in ["multirange_schema", "typed_tables", "composite_stored_in"] {
        assert!(!json.contains(key), "{key} in {json}");
    }
    write_dump_io(
        &path,
        &format!(
            r#"{{"schemas": [], "extensions": [], "types": {json}, "enums": [],
                "sequences": [], "routines": [], "tables": [], "views": []}}"#
        ),
    );

    let dump = Dump::read_from_file(&path)
        .await
        .expect("a pgc 1.0.x type must still deserialize");
    for (loaded, written) in dump.types.iter().zip(&types) {
        assert_eq!(loaded.multirange_schema, None);
        assert_eq!(loaded.typed_tables, None);
        assert_eq!(loaded.composite_stored_in, None);
        // `read_from_file` rehashes every type: still the hash it had.
        assert_eq!(loaded.hash, written.hash, "{}", loaded.typname);
    }
}

#[tokio::test]
async fn r3t_usage_and_multirange_schema_round_trip_without_touching_the_hash() {
    let dir = ScratchDir::new("r3t-roundtrip");
    let path = dir.path_str("types.dump");
    let mut types = r3t_types();
    let hashes: Vec<_> = types.iter().map(|t| t.hash.clone()).collect();
    types[0].typed_tables = Some(Vec::new());
    types[0].composite_stored_in = Some(vec!["app.orders.ship_to".to_string()]);
    types[1].multirange_schema = Some("app".to_string());
    let mut dump = empty_dump("r3t");
    dump.types = types.clone();
    dump.write_to_file(&path).expect("write dump");

    let reloaded = Dump::read_from_file(&path).await.expect("read dump");
    // An empty list is recorded knowledge ("nothing"), not "unknown".
    assert_eq!(reloaded.types[0].typed_tables, Some(Vec::new()));
    assert_eq!(
        reloaded.types[0].composite_stored_in,
        Some(vec!["app.orders.ship_to".to_string()])
    );
    assert_eq!(reloaded.types[1].multirange_schema.as_deref(), Some("app"));
    let reloaded_hashes: Vec<_> = reloaded.types.iter().map(|t| t.hash.clone()).collect();
    assert_eq!(reloaded_hashes, hashes);
}

#[tokio::test]
async fn r3t_legacy_from_dump_still_migrates_composite_attributes() {
    // FROM written by pgc 1.0.x: stored columns unknown, so the retype is
    // emitted and PostgreSQL decides; the added attribute is appended.
    let from_types = r3t_types();
    let mut to_types = r3t_types();
    to_types[0].composite_attributes[1].data_type = "bigint".to_string();
    to_types[0]
        .composite_attributes
        .push(pgc::dump::pg_type::CompositeAttribute {
            name: "region".to_string(),
            data_type: "text".to_string(),
            collation: None,
        });
    to_types[0].typed_tables = Some(Vec::new());
    to_types[0].composite_stored_in = Some(Vec::new());
    to_types[0].hash();
    to_types[1].multirange_schema = Some("app".to_string());

    let mut from = empty_dump("r3t-from");
    from.types = from_types;
    let mut to = empty_dump("r3t-to");
    to.types = to_types;
    let mut comparer = pgc::comparer::core::Comparer::new(
        from,
        to,
        true,
        false,
        true,
        pgc::config::grants_mode::GrantsMode::Ignore,
    );
    comparer.compare().await.expect("compare");
    let dir = ScratchDir::new("r3t-compare");
    let out = dir.path_str("output.sql");
    comparer.save_script(&out).await.expect("save script");
    let script = std::fs::read_to_string(&out).expect("read script");

    assert!(
        script.contains("alter type app.addr alter attribute \"zip\" set data type bigint;"),
        "{script}"
    );
    assert!(
        script.contains("alter type app.addr add attribute \"region\" text;"),
        "{script}"
    );
    // The unchanged range needs nothing, and nothing is moved.
    assert!(!script.contains("app.span"), "{script}");
}

// ==== r3-constraints tests ====

/// `Table::enforced_inherited_checks` (pgc 1.1, PostgreSQL 18+) is written
/// only for a table with such a copy, so every other table's JSON — and its
/// hash — is what pgc 1.0.x wrote, and a dump without the key (every older
/// one) loads with none. One that has them round-trips, and they are part of
/// its hash.
#[tokio::test]
async fn r3_constraints_enforced_inherited_checks_are_written_only_when_present() {
    let dir = ScratchDir::new("r3c-enforced-inherited");
    let path = dir.path_str("tables.dump");
    let mut dump = empty_dump("shop");
    dump.server_version_num = Some(190000);
    let mut plain = common::table("public", "p_1", &["id"]);
    plain.partition_of = Some("public.p".to_string());
    plain.partition_bound = Some("FOR VALUES FROM (0) TO (10)".to_string());
    plain.hash();
    let plain_hash = plain.hash.clone();
    let mut own = plain.clone();
    own.name = "p_2".to_string();
    own.raw_name = "p_2".to_string();
    own.hash();
    let own_hash_without = own.hash.clone();
    own.enforced_inherited_checks = vec!["p_ck".to_string(), "\"P Ck\"".to_string()];
    own.hash();
    assert_ne!(own.hash, own_hash_without);
    dump.tables = vec![plain, own.clone()];
    dump.write_to_file(&path).expect("write dump");

    let json = dump_json(&path);
    assert!(
        json["tables"][0].get("enforced_inherited_checks").is_none(),
        "{json}"
    );
    assert_eq!(
        json["tables"][1]["enforced_inherited_checks"],
        serde_json::json!(["p_ck", "\"P Ck\""])
    );

    let reloaded = Dump::read_from_file(&path).await.expect("read dump");
    assert!(reloaded.tables[0].enforced_inherited_checks.is_empty());
    assert_eq!(reloaded.tables[0].hash, plain_hash);
    assert_eq!(
        reloaded.tables[1].enforced_inherited_checks,
        own.enforced_inherited_checks
    );
    assert_eq!(reloaded.tables[1].hash, own.hash);

    // A table as pgc 1.0.x wrote it: no key at all.
    let mut legacy = json.clone();
    legacy["tables"][1]
        .as_object_mut()
        .expect("table object")
        .remove("enforced_inherited_checks");
    legacy
        .as_object_mut()
        .expect("dump object")
        .remove("server_version_num");
    let legacy_path = dir.path_str("legacy.dump");
    write_dump_io(&legacy_path, &legacy.to_string());
    let legacy = Dump::read_from_file(&legacy_path)
        .await
        .expect("legacy dump loads");
    assert!(legacy.tables[1].enforced_inherited_checks.is_empty());
}

/// The privileges and partition data pgc 1.1 reads for foreign servers and
/// foreign tables — `ForeignServer::acl`, `ForeignTableColumn::acl`,
/// `ForeignTable::partition_of` / `partition_bound` — are written only when
/// set, are not part of any hash, and a dump without them (every one pgc
/// 1.0.x wrote) loads with none.
#[tokio::test]
async fn r3_constraints_fdw_privileges_and_foreign_partitions_are_written_only_when_present() {
    use pgc::dump::fdw::ForeignServer;
    use pgc::dump::foreign_table::{ForeignTable, ForeignTableColumn};

    let dir = ScratchDir::new("r3c-fdw-privileges");
    let path = dir.path_str("fdw.dump");
    let mut dump = empty_dump("shop");
    let server = |acl: Vec<String>| {
        let mut srv = ForeignServer {
            name: "srv".to_string(),
            owner: "postgres".to_string(),
            fdw_name: "w".to_string(),
            server_type: None,
            server_version: None,
            options: Vec::new(),
            comment: None,
            hash: None,
            acl,
        };
        srv.hash();
        srv
    };
    let column = |acl: Vec<String>| ForeignTableColumn {
        name: "id".to_string(),
        data_type: "integer".to_string(),
        is_nullable: true,
        column_default: None,
        options: Vec::new(),
        acl,
    };
    let plain_server = server(Vec::new());
    let granted_server = server(vec!["reader=U/postgres".to_string()]);
    assert_eq!(plain_server.hash, granted_server.hash);
    let plain_ft = ForeignTable::new(
        "public".to_string(),
        "ft".to_string(),
        "srv".to_string(),
        "postgres".to_string(),
        Vec::new(),
        vec![column(Vec::new())],
    );
    let mut part = ForeignTable::new(
        "public".to_string(),
        "ft".to_string(),
        "srv".to_string(),
        "postgres".to_string(),
        Vec::new(),
        vec![column(vec!["reader=w/postgres".to_string()])],
    );
    part.partition_of = Some("public.lp".to_string());
    part.partition_bound = Some("FOR VALUES FROM (0) TO (10)".to_string());
    part.hash();
    assert_eq!(part.hash, plain_ft.hash);
    dump.foreign_servers = vec![granted_server.clone(), plain_server];
    dump.foreign_tables = vec![part.clone(), plain_ft];
    dump.write_to_file(&path).expect("write dump");

    let json = dump_json(&path);
    assert_eq!(
        json["foreign_servers"][0]["acl"],
        serde_json::json!(["reader=U/postgres"])
    );
    assert!(json["foreign_servers"][1].get("acl").is_none(), "{json}");
    assert_eq!(json["foreign_tables"][0]["partition_of"], "public.lp");
    assert_eq!(
        json["foreign_tables"][0]["columns"][0]["acl"],
        serde_json::json!(["reader=w/postgres"])
    );
    for key in ["partition_of", "partition_bound"] {
        assert!(json["foreign_tables"][1].get(key).is_none(), "{json}");
    }
    assert!(
        json["foreign_tables"][1]["columns"][0].get("acl").is_none(),
        "{json}"
    );

    let reloaded = Dump::read_from_file(&path).await.expect("read dump");
    assert_eq!(reloaded.foreign_servers[0].acl, granted_server.acl);
    assert_eq!(
        reloaded.foreign_tables[0].partition_bound,
        part.partition_bound
    );
    assert_eq!(reloaded.foreign_tables[0].hash, part.hash);

    // As pgc 1.0.x wrote them: none of the keys.
    let mut legacy = json.clone();
    legacy["foreign_servers"][0]
        .as_object_mut()
        .expect("server object")
        .remove("acl");
    let ft = legacy["foreign_tables"][0]
        .as_object_mut()
        .expect("foreign table object");
    ft.remove("partition_of");
    ft.remove("partition_bound");
    legacy["foreign_tables"][0]["columns"][0]
        .as_object_mut()
        .expect("column object")
        .remove("acl");
    let legacy_path = dir.path_str("legacy.dump");
    write_dump_io(&legacy_path, &legacy.to_string());
    let legacy = Dump::read_from_file(&legacy_path)
        .await
        .expect("legacy dump loads");
    assert!(legacy.foreign_servers[0].acl.is_empty());
    assert!(legacy.foreign_tables[0].partition_of.is_none());
    assert!(legacy.foreign_tables[0].partition_bound.is_none());
    assert!(legacy.foreign_tables[0].columns[0].acl.is_empty());
}

// ==== r3-compat tests ====

/// A dump whose schema pattern matched nothing holds no object, but pgc 1.1+
/// still records its server: the version (what tells it from a pgc 1.0.x
/// dump), the installable extensions and the `standard_conforming_strings`
/// default. All three must reach the file and come back, and an empty dump
/// written by pgc 1.0.x must still read as one.
#[tokio::test]
async fn an_empty_dump_keeps_its_server_facts_through_the_file() {
    use pgc::dump::extension::AvailableExtension;

    let dir = ScratchDir::new("compat-empty-facts");
    let path = dir.path_str("empty.dump");
    let mut dump = empty_dump("fresh");
    dump.server_version_num = Some(180004);
    dump.standard_conforming_strings_off = true;
    dump.available_extensions = vec![AvailableExtension {
        name: "plpgsql".to_string(),
        default_version: "1.0".to_string(),
        versions: vec!["1.0".to_string()],
    }];
    dump.write_to_file(&path).expect("write dump");

    let json = dump_json(&path);
    assert_eq!(json["schemas"], serde_json::json!([]));
    assert_eq!(json["server_version_num"], 180004);
    assert_eq!(json["standard_conforming_strings_off"], true);
    assert_eq!(json["available_extensions"][0]["name"], "plpgsql");

    let reloaded = Dump::read_from_file(&path).await.expect("read dump");
    assert!(reloaded.schemas.is_empty() && reloaded.tables.is_empty());
    assert_eq!(reloaded.server_version_num, Some(180004));
    assert!(reloaded.standard_conforming_strings_off);
    assert_eq!(reloaded.available_extensions, dump.available_extensions);

    let legacy_path = dir.path_str("legacy.dump");
    write_dump_io(
        &legacy_path,
        r#"{"schemas": [], "extensions": [], "types": [], "enums": [],
            "sequences": [], "routines": [], "tables": [], "views": []}"#,
    );
    let legacy = Dump::read_from_file(&legacy_path)
        .await
        .expect("legacy dump loads");
    assert_eq!(legacy.server_version_num, None);
    assert!(legacy.available_extensions.is_empty());
}

// ==== r4-typed tests ====

#[tokio::test]
async fn r4t_composite_attribute_without_a_collation_loads_and_serializes_as_pgc_1_0_did() {
    let dir = ScratchDir::new("r4t-legacy");
    let path = dir.path_str("legacy.dump");
    let types = r3t_types();
    // An attribute without a collation of its own has no `collation` key.
    let json = serde_json::to_string(&types).expect("serialize types");
    let value: serde_json::Value = serde_json::from_str(&json).expect("parse types");
    for attribute in value[0]["composite_attributes"]
        .as_array()
        .expect("attributes")
    {
        let keys: Vec<&String> = attribute.as_object().expect("attribute").keys().collect();
        assert_eq!(keys, ["data_type", "name"], "{attribute}");
    }
    // A pgc 1.0.x attribute ({name, data_type}) still loads, with none.
    write_dump_io(
        &path,
        &format!(
            r#"{{"schemas": [], "extensions": [], "types": {json}, "enums": [],
                "sequences": [], "routines": [], "tables": [], "views": []}}"#
        ),
    );
    let dump = Dump::read_from_file(&path)
        .await
        .expect("a pgc 1.0.x composite type must still deserialize");
    let loaded = &dump.types[0];
    assert!(
        loaded
            .composite_attributes
            .iter()
            .all(|a| a.collation.is_none())
    );
    // `read_from_file` rehashes every type: still the hash it had.
    assert_eq!(loaded.hash, types[0].hash);
}

#[tokio::test]
async fn r4t_composite_attribute_collation_round_trips_and_is_hashed() {
    let dir = ScratchDir::new("r4t-roundtrip");
    let path = dir.path_str("types.dump");
    let mut types = r3t_types();
    let plain_hash = types[0].hash.clone();
    types[0].composite_attributes[0].collation = Some("pg_catalog.\"C\"".to_string());
    types[0].hash();
    assert_ne!(types[0].hash, plain_hash);
    let mut dump = empty_dump("r4t");
    dump.types = types.clone();
    dump.write_to_file(&path).expect("write dump");

    let json = dump_json(&path);
    assert_eq!(
        json["types"][0]["composite_attributes"][0]["collation"],
        "pg_catalog.\"C\""
    );
    assert!(
        json["types"][0]["composite_attributes"][1]
            .get("collation")
            .is_none()
    );

    let reloaded = Dump::read_from_file(&path).await.expect("read dump");
    assert_eq!(
        reloaded.types[0].composite_attributes[0]
            .collation
            .as_deref(),
        Some("pg_catalog.\"C\"")
    );
    assert_eq!(reloaded.types[0].composite_attributes[1].collation, None);
    assert_eq!(reloaded.types[0].hash, types[0].hash);
}

// ==== r4-sub tests ====

// pgc 1.1 records a subscription's replication slot (`subslotname`) only when
// it is not named after the subscription. A pgc 1.0.x dump, which did not
// read it, loads as a subscription on its own slot and keeps its hash.

#[tokio::test]
async fn r4s_subscription_without_a_slot_key_loads_on_its_own_slot() {
    use pgc::dump::publication::SubscriptionSlot;

    let dir = ScratchDir::new("r4s-legacy-slot");
    let path = dir.path_str("legacy.dump");
    // The pgc 1.0.27 subscription shape (see
    // subscription_and_fdw_written_by_pgc_1_0_x_still_load).
    write_dump_io(
        &path,
        r#"{
            "schemas": [], "extensions": [], "types": [], "enums": [],
            "sequences": [], "routines": [], "tables": [], "views": [],
            "subscriptions": [{
                "name": "s", "owner": "postgres",
                "connection": "host=primary dbname=mydb",
                "publications": ["p"], "enabled": false,
                "hash": "3f70947e2c5caf9758701238ccfb76f84c9fc94190b6b1a8ba909eceab29b316"
            }]
        }"#,
    );
    let dump = Dump::read_from_file(&path)
        .await
        .expect("legacy dump loads");
    let sub = &dump.subscriptions[0];
    assert_eq!(sub.slot_name, SubscriptionSlot::Own);
    let mut rehashed = sub.clone();
    rehashed.hash();
    assert_eq!(rehashed.hash, sub.hash, "the pgc 1.0.x hash holds");
    // Written back, the default stays out of the file.
    let rewritten = dir.path_str("rewritten.dump");
    dump.write_to_file(&rewritten).expect("write dump");
    assert!(
        dump_json(&rewritten)["subscriptions"][0]
            .get("slot_name")
            .is_none()
    );
}

#[tokio::test]
async fn r4s_subscription_slots_round_trip_through_the_dump_file() {
    use pgc::dump::publication::{Subscription, SubscriptionSlot};

    let dir = ScratchDir::new("r4s-slot");
    let path = dir.path_str("slots.dump");
    let make = |name: &str, slot_name: SubscriptionSlot| {
        let mut sub = Subscription {
            name: name.to_string(),
            owner: "postgres".to_string(),
            connection: "host=127.0.0.1 port=1 dbname=nowhere".to_string(),
            server: None,
            publications: vec!["p".to_string()],
            enabled: false,
            slot_name,
            retain_dead_tuples: false,
            max_retention_duration: 0,
            wal_receiver_timeout: None,
            comment: None,
            hash: None,
        };
        sub.hash();
        sub
    };
    let mut dump = empty_dump("shop");
    dump.subscriptions = vec![
        make("s_none", SubscriptionSlot::NoSlot),
        make("s_named", SubscriptionSlot::Named("pre_made".to_string())),
        make("s_own", SubscriptionSlot::Own),
    ];
    dump.write_to_file(&path).expect("write dump");

    let json = dump_json(&path);
    assert_eq!(json["subscriptions"][0]["slot_name"], "no_slot");
    assert_eq!(
        json["subscriptions"][1]["slot_name"],
        serde_json::json!({ "named": "pre_made" })
    );
    assert!(json["subscriptions"][2].get("slot_name").is_none());

    let reloaded = Dump::read_from_file(&path).await.expect("read dump");
    for (before, after) in dump.subscriptions.iter().zip(&reloaded.subscriptions) {
        assert_eq!(after.slot_name, before.slot_name);
        assert_eq!(after.hash, before.hash);
    }
    assert!(
        reloaded.subscriptions[0]
            .get_script()
            .contains(" WITH (connect = false, slot_name = NONE, enabled = false);")
    );
    assert!(
        reloaded.subscriptions[1]
            .get_script()
            .contains(" WITH (connect = false, slot_name = 'pre_made', enabled = false);")
    );
}
