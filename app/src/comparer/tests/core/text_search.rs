//! Text search dictionaries and configurations.

use super::helpers::int_column;
use crate::comparer::core::*;
use crate::config::dump_config::DumpConfig;
use crate::config::grants_mode::GrantsMode;
use crate::dump::table::Table;
use crate::dump::text_search::{TextSearchConfig, TextSearchDict};

fn dict(options: &[&str]) -> TextSearchDict {
    let mut d = TextSearchDict {
        schema: "test_schema".into(),
        name: "nl_stem".into(),
        owner: "postgres".into(),
        template: "pg_catalog.snowball".into(),
        options: options.iter().map(|o| o.to_string()).collect(),
        comment: None,
        hash: None,
    };
    d.hash();
    d
}

fn config_mapping_nl_stem() -> TextSearchConfig {
    let mut c = TextSearchConfig {
        schema: "test_schema".into(),
        name: "nl_cfg".into(),
        owner: "postgres".into(),
        parser: "pg_catalog.\"default\"".into(),
        mappings: vec![
            "asciiword:test_schema.nl_stem,pg_catalog.simple".into(),
            "word:test_schema.nl_stem,pg_catalog.simple".into(),
        ],
        comment: None,
        hash: None,
    };
    c.hash();
    c
}

fn dumps(from_dict: TextSearchDict, to_dict: TextSearchDict) -> (Dump, Dump) {
    let mut from = Dump::new(DumpConfig::default());
    let mut to = Dump::new(DumpConfig::default());
    from.ts_dicts.push(from_dict);
    to.ts_dicts.push(to_dict);
    from.ts_configs.push(config_mapping_nl_stem());
    to.ts_configs.push(config_mapping_nl_stem());
    (from, to)
}

// ---- PG19: snowball language change (dutch -> dutch_porter) ----

/// The PostgreSQL 19 upgrade step: keep the pre-19 Dutch stemming by moving
/// a dictionary to 'dutch_porter'. The configuration that maps it must be
/// left alone — a `DROP ... CASCADE` would take it (and every generated
/// tsvector column and index built on it) with the dictionary, and the
/// configurations are compared after the tables, so the next compare could
/// not even put them back in a working order.
#[tokio::test]
async fn dictionary_language_change_keeps_dependent_configuration() {
    let (from, to) = dumps(
        dict(&["language = 'dutch'", "stopwords = 'dutch'"]),
        dict(&["language = 'dutch_porter'", "stopwords = 'dutch'"]),
    );

    let mut comparer = Comparer::new(from, to, true, false, false, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains(
            "ALTER TEXT SEARCH DICTIONARY test_schema.nl_stem \
             (language = 'dutch_porter', stopwords = 'dutch');"
        ),
        "{script}"
    );
    assert!(!script.contains("DROP TEXT SEARCH"), "{script}");
    assert!(!script.contains("CASCADE"), "{script}");
    assert!(!script.contains("CREATE TEXT SEARCH"), "{script}");
    assert!(
        !script.contains("nl_cfg"),
        "the unchanged configuration is not touched:\n{script}"
    );
    assert!(
        !script.contains("re-process stored data"),
        "the rebuild note is a comment, stripped with use_comments off:\n{script}"
    );
}

#[tokio::test]
async fn dictionary_option_alter_keeps_its_note_with_comments_on() {
    let (from, to) = dumps(
        dict(&["language = 'dutch'"]),
        dict(&["language = 'dutch_porter'"]),
    );

    let mut comparer = Comparer::new(from, to, true, false, true, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("-- ALTER does not re-process stored data"),
        "{script}"
    );
}

#[tokio::test]
async fn unchanged_dictionary_emits_nothing() {
    let (from, to) = dumps(dict(&["language = 'dutch'"]), dict(&["language = 'dutch'"]));

    let mut comparer = Comparer::new(from, to, true, false, false, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    assert_eq!(comparer.get_script(), "");
}

#[tokio::test]
async fn dictionary_option_alter_is_left_alone_by_production_output() {
    // ALTER TEXT SEARCH DICTIONARY is transactional and has no IF EXISTS
    // form; replaying it converges on the same options.
    let (from, to) = dumps(
        dict(&["language = 'dutch'", "stopwords = 'dutch'"]),
        dict(&["language = 'dutch_porter'", "stopwords = 'dutch'"]),
    );

    let mut comparer = Comparer::new(from, to, true, true, false, GrantsMode::Ignore);
    comparer.set_output_for_production(true);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.lines().any(|l| l
            == "ALTER TEXT SEARCH DICTIONARY test_schema.nl_stem \
                (language = 'dutch_porter', stopwords = 'dutch');"),
        "{script}"
    );
    assert!(!script.contains("CASCADE"), "{script}");
}

// ---- In-place option changes come before the tables ----

/// A TO-only `test_schema.docs` whose STORED generated column goes through
/// `test_schema.nl_cfg`, and so through the `nl_stem` dictionary.
fn docs_table_with_generated_tsvector() -> Table {
    let mut id = int_column("test_schema", "docs", "id", 1);
    id.is_nullable = false;
    let mut body = int_column("test_schema", "docs", "body", 2);
    body.data_type = "text".to_string();
    body.numeric_precision = None;
    body.numeric_precision_radix = None;
    body.numeric_scale = None;
    let mut tsv = int_column("test_schema", "docs", "body_tsv", 3);
    tsv.data_type = "tsvector".to_string();
    tsv.numeric_precision = None;
    tsv.numeric_precision_radix = None;
    tsv.numeric_scale = None;
    tsv.is_generated = "ALWAYS".to_string();
    tsv.generation_expression =
        Some("to_tsvector('test_schema.nl_cfg'::regconfig, body)".to_string());
    tsv.generation_type = Some("s".to_string());
    let mut table = Table::new(
        "test_schema".to_string(),
        "docs".to_string(),
        "test_schema".to_string(),
        "docs".to_string(),
        "postgres".to_string(),
        None,
        vec![id, body, tsv],
        vec![],
        vec![],
        vec![],
        None,
    );
    table.hash();
    table
}

fn position(script: &str, needle: &str) -> usize {
    script
        .find(needle)
        .unwrap_or_else(|| panic!("`{needle}` missing from:\n{script}"))
}

/// The ALTER never re-processes stored data, so it has to run before the
/// tables phase creates anything computed through the dictionary: a
/// generated column added in the same migration would otherwise be filled
/// with the old stemmer's output, and no later compare could see it.
#[tokio::test]
async fn dictionary_option_alter_precedes_tables_created_in_the_same_migration() {
    let (from, mut to) = dumps(
        dict(&["language = 'dutch'"]),
        dict(&["language = 'dutch_porter'"]),
    );
    to.tables.push(docs_table_with_generated_tsvector());

    let mut comparer = Comparer::new(from, to, true, false, false, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();

    let alter = position(
        &script,
        "ALTER TEXT SEARCH DICTIONARY test_schema.nl_stem (language = 'dutch_porter');",
    );
    let table = position(&script, "test_schema.docs");
    assert!(
        alter < table,
        "the dictionary must change before the generated column is computed:\n{script}"
    );
    assert_eq!(
        script.matches("ALTER TEXT SEARCH DICTIONARY").count(),
        1,
        "emitted once, not again with the other dictionaries:\n{script}"
    );
}

/// With comments on, the early ALTER sits under its own header, and keeps
/// its rebuild note.
#[tokio::test]
async fn dictionary_option_alter_has_its_own_section_with_comments_on() {
    let (from, to) = dumps(
        dict(&["language = 'dutch'"]),
        dict(&["language = 'dutch_porter'"]),
    );

    let mut comparer = Comparer::new(from, to, true, false, true, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();

    let header = position(&script, "Text Search Dictionary Options (before tables)");
    let note = position(&script, "-- ALTER does not re-process stored data");
    let alter = position(&script, "ALTER TEXT SEARCH DICTIONARY test_schema.nl_stem");
    let tables = position(&script, "/* ---> Compare Text Search Dictionaries");
    assert!(header < note && note < alter && alter < tables, "{script}");
}

/// No such change, no header: every other migration is unchanged byte for
/// byte, comments included.
#[tokio::test]
async fn dictionary_options_section_is_absent_without_an_option_change() {
    let (from, to) = dumps(dict(&["language = 'dutch'"]), dict(&["language = 'dutch'"]));

    let mut comparer = Comparer::new(from, to, true, false, true, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    assert!(
        !comparer
            .get_script()
            .contains("Text Search Dictionary Options"),
        "{}",
        comparer.get_script()
    );
}

/// An owner-only change was never a recreate; it stays where pgc 1.0.x put
/// it, after the tables.
#[tokio::test]
async fn dictionary_owner_only_change_stays_after_the_tables() {
    let from_dict = dict(&["language = 'dutch'"]);
    let mut to_dict = dict(&["language = 'dutch'"]);
    to_dict.owner = "app_owner".into();
    to_dict.hash();
    let (from, mut to) = dumps(from_dict, to_dict);
    to.tables.push(docs_table_with_generated_tsvector());

    let mut comparer = Comparer::new(from, to, true, false, true, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();

    let table = position(&script, "test_schema.docs");
    let owner = position(
        &script,
        "ALTER TEXT SEARCH DICTIONARY test_schema.nl_stem OWNER TO app_owner;",
    );
    assert!(table < owner, "{script}");
    assert!(
        !script.contains("Text Search Dictionary Options"),
        "{script}"
    );
}

fn template_change_dumps() -> (Dump, Dump) {
    let from_dict = dict(&["language = 'dutch'"]);
    let mut to_dict = dict(&["language = 'dutch'"]);
    to_dict.template = "pg_catalog.simple".into();
    to_dict.options.clear();
    to_dict.hash();
    let (from, mut to) = dumps(from_dict, to_dict);
    to.tables.push(docs_table_with_generated_tsvector());
    (from, to)
}

/// A template change cannot be altered in place, and `DROP ... CASCADE`
/// took every configuration mapping the dictionary, with the generated
/// columns and indexes built on them. The new dictionary is created under a
/// temporary name before the tables, the configuration moves to it, the old
/// one goes without CASCADE and the new one takes its name.
#[tokio::test]
async fn dictionary_template_change_moves_its_configurations_before_the_tables() {
    let (from, to) = template_change_dumps();

    let mut comparer = Comparer::new(from, to, true, false, false, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();

    let temporary = "test_schema.pgc_new_";
    let create = position(
        &script,
        "CREATE TEXT SEARCH DICTIONARY test_schema.pgc_new_",
    );
    let replace = position(
        &script,
        "ALTER TEXT SEARCH CONFIGURATION test_schema.nl_cfg ALTER MAPPING REPLACE \
         test_schema.nl_stem WITH test_schema.pgc_new_",
    );
    let drop = position(&script, "DROP TEXT SEARCH DICTIONARY test_schema.nl_stem;");
    let rename = position(&script, "RENAME TO nl_stem;");
    let table = position(&script, "test_schema.docs");
    assert!(
        create < replace && replace < drop && drop < rename && rename < table,
        "{script}"
    );
    // All in one `DO` block, which a failing drop rolls back as a whole.
    let block = position(&script, "DO $pgc$\nBEGIN\n");
    let end = position(&script, "END\n$pgc$;");
    assert!(block < create && rename < end, "{script}");
    assert!(
        script[create..].contains("(TEMPLATE = pg_catalog.simple);"),
        "{script}"
    );
    assert!(script.contains(temporary), "{script}");
    assert!(!script.contains("CASCADE"), "{script}");
    assert!(
        !script.contains("CREATE TEXT SEARCH DICTIONARY test_schema.nl_stem"),
        "{script}"
    );

    // With comments on, the section has its own header and the statements a
    // note on the stored data they do not re-process.
    let (from, to) = template_change_dumps();
    let mut comparer = Comparer::new(from, to, true, false, true, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();
    let header = position(
        &script,
        "/* ---> Text Search Dictionaries created anew (before tables) ----- */",
    );
    let note = position(
        &script,
        "-- Re-created (its template or options cannot be altered",
    );
    let create = position(
        &script,
        "CREATE TEXT SEARCH DICTIONARY test_schema.pgc_new_",
    );
    assert!(header < note && note < create, "{script}");
}

/// Without `use_drop` the change stays the commented-out drop and the
/// create after the tables it always was.
#[tokio::test]
async fn dictionary_template_change_without_use_drop_stays_after_the_tables() {
    let (from, to) = template_change_dumps();

    let mut comparer = Comparer::new(from, to, false, false, true, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();

    let table = position(&script, "test_schema.docs");
    let drop = position(
        &script,
        "-- DROP TEXT SEARCH DICTIONARY IF EXISTS test_schema.nl_stem CASCADE;",
    );
    let create = position(&script, "CREATE TEXT SEARCH DICTIONARY test_schema.nl_stem");
    assert!(table < drop && drop < create, "{script}");
    assert!(!script.contains("pgc_new_"), "{script}");
}
