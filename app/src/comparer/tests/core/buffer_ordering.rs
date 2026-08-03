//! Emission-order tests for the post-script buffers.
//!
//! [`Comparer::compare`] concatenates several ordered script buffers —
//! main → `sequence_post` → `type_post` → `enum_post` → `trigger_post`.
//! These tests pin that order so dependency-aware rearrangements cannot
//! regress silently.

use crate::comparer::core::*;
use super::helpers::*;
use crate::config::dump_config::DumpConfig;
use crate::config::grants_mode::GrantsMode;
use crate::dump::schema::Schema;
use crate::dump::sequence::Sequence;
use crate::dump::table::Table;
use crate::dump::table_trigger::TableTrigger;
use sqlx::postgres::types::Oid;

#[tokio::test]
async fn buffer_ordering_type_drop_before_enum_drop() {
    // Both buffers are populated when the FROM dump carries a domain type
    // AND an enum that are both absent in the TO dump. type_post must come
    // before enum_post in the final script (so enums outlive types that may
    // reference them).
    let mut from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());

    from_dump
        .types
        .push(make_domain_type("test_schema", "dropped_domain", 701));
    from_dump.types.push(make_enum_type(
        "test_schema",
        "dropped_enum",
        702,
        vec!["a", "b"],
    ));

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();

    let type_drop_pos = script
        .find("drop type if exists test_schema.dropped_domain cascade;")
        .unwrap_or_else(|| panic!("domain drop missing in:\n{script}"));
    let enum_drop_pos = script
        .find("drop type if exists test_schema.dropped_enum cascade;")
        .unwrap_or_else(|| panic!("enum drop missing in:\n{script}"));
    assert!(
        type_drop_pos < enum_drop_pos,
        "type_post_script must precede enum_post_script, got:\n{script}"
    );
}

#[tokio::test]
async fn buffer_ordering_enum_drop_before_trigger_create() {
    // FROM has an enum to drop (populates enum_post_script).
    // TO has a brand-new table with a trigger (populates trigger_post_script
    // for the CREATE TRIGGER). enum_post must come before trigger_post so
    // that triggers referencing newly-created routines/types run after all
    // type-dependency cleanup.
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    from_dump.types.push(make_enum_type(
        "test_schema",
        "legacy_status",
        703,
        vec!["ok", "err"],
    ));

    let mut new_table = Table::new(
        "public".to_string(),
        "events".to_string(),
        "public".to_string(),
        "events".to_string(),
        "postgres".to_string(),
        None,
        vec![int_column("public", "events", "id", 1)],
        vec![],
        vec![],
        vec![TableTrigger {
            oid: Oid(9999),
            name: "trg_events_audit".to_string(),
            definition:
                "create trigger trg_events_audit before insert on public.events for each row execute function audit()"
                    .to_string(),
            enabled: "O".to_string(),
            comment: None,
        }],
        None,
    );
    new_table.hash();
    to_dump.tables.push(new_table);

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();

    let enum_drop_pos = script
        .find("drop type if exists test_schema.legacy_status cascade;")
        .unwrap_or_else(|| panic!("enum drop missing in:\n{script}"));
    let trigger_create_pos = script
        .find("create trigger trg_events_audit")
        .unwrap_or_else(|| panic!("CREATE TRIGGER missing in:\n{script}"));

    assert!(
        enum_drop_pos < trigger_create_pos,
        "enum_post_script must precede trigger_post_script, got:\n{script}"
    );
}

#[tokio::test]
async fn buffer_ordering_sequence_drop_before_type_drop() {
    // FROM has an unowned sequence and a domain type, both absent in TO.
    // sequence_post_script is emitted before type_post_script so that
    // sequences with default-value dependencies on types are dropped first.
    let mut from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());

    let seq = crate::dump::sequence::Sequence::new(
        "test_schema".to_string(),
        "dropped_seq".to_string(),
        "postgres".to_string(),
        "bigint".to_string(),
        Some(1),
        Some(1),
        Some(9223372036854775807),
        Some(1),
        false,
        Some(1),
        Some(1),
        None,
        None,
        None,
    );
    from_dump.sequences.push(seq);
    from_dump
        .types
        .push(make_domain_type("test_schema", "dropped_domain", 704));

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();

    let seq_drop_pos = script
        .find("drop sequence if exists \"test_schema\".\"dropped_seq\"")
        .or_else(|| script.find("drop sequence if exists test_schema.dropped_seq"))
        .unwrap_or_else(|| panic!("sequence drop missing in:\n{script}"));
    let type_drop_pos = script
        .find("drop type if exists test_schema.dropped_domain cascade;")
        .unwrap_or_else(|| panic!("type drop missing in:\n{script}"));

    assert!(
        seq_drop_pos < type_drop_pos,
        "sequence_post_script must precede type_post_script, got:\n{script}"
    );
}

/// Regression for the dependency-scan needle bug. Dump fields are populated
/// via `quote_ident`, so a mixed-case identifier comes back literally
/// quoted (`"MyView"`). Previously the needle kept the quotes and the
/// quote-stripped haystack flavour could never match an unquoted reference,
/// silently dropping a real dependency.
#[test]
fn text_references_qualified_name_pre_matches_unquoted_reference() {
    let (lower, unquoted_lower) = Comparer::prelower_pair("SELECT * FROM public.regular_view;");
    // Needle as built from `quote_ident` for a mixed-case identifier.
    assert!(Comparer::text_references_qualified_name_pre(
        &lower,
        &unquoted_lower,
        "\"public\"",
        "\"regular_view\"",
    ));
}

#[test]
fn text_references_qualified_name_pre_still_matches_quoted_reference() {
    let (lower, unquoted_lower) = Comparer::prelower_pair("SELECT * FROM \"MySchema\".\"MyView\";");
    assert!(Comparer::text_references_qualified_name_pre(
        &lower,
        &unquoted_lower,
        "\"myschema\"",
        "\"myview\"",
    ));
}

/// Regression test for the `serial_columns` key change from a joined
/// `"schema.table.column"` `String` to a `(String, String, String)` tuple.
/// The old form was parsed back via `splitn(3, '.')`, which silently
/// misparsed any identifier containing a literal `.` (legal in PostgreSQL
/// when quoted). With the tuple key, dotted identifiers round-trip cleanly
/// and `mark_serial_columns` still finds the target column.
#[tokio::test]
async fn mark_serial_columns_handles_dotted_identifier_names() {
    let from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    // Schema, table, and column names all contain a literal dot — the
    // pre-fix `splitn(3, '.')` would slice these in the wrong place and
    // fail to locate the column.
    let schema = "weird.schema";
    let table = "weird.table";
    let column = "weird.id";

    let serial_seq = Sequence::new(
        schema.to_string(),
        format!("{table}_{column}_seq"),
        "postgres".to_string(),
        "integer".to_string(),
        Some(1),
        Some(1),
        Some(2147483647),
        Some(1),
        false,
        Some(1),
        Some(1),
        Some(schema.to_string()),
        Some(table.to_string()),
        Some(column.to_string()),
    );
    to_dump.sequences.push(serial_seq);

    let mut col = int_column(schema, table, column, 1);
    col.column_default = Some(format!(
        "nextval('{schema}.{table}_{column}_seq'::regclass)"
    ));
    col.is_nullable = false;

    let table_obj = Table::new(
        schema.to_string(),
        table.to_string(),
        schema.to_string(),
        table.to_string(),
        "postgres".to_string(),
        None,
        vec![col],
        Vec::new(),
        Vec::new(),
        Vec::new(),
        None,
    );
    to_dump.tables.push(table_obj);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Ignore);
    comparer.compare_sequences().await.unwrap();
    comparer.mark_serial_columns();

    let to_table = comparer
        .to
        .tables
        .iter()
        .find(|t| t.schema == schema && t.name == table)
        .expect("table must round-trip");
    let to_column = to_table
        .columns
        .iter()
        .find(|c| c.name == column)
        .expect("column must round-trip");
    assert_eq!(
        to_column.serial_type.as_deref(),
        Some("serial"),
        "dotted-name column must still be marked as serial"
    );
}
