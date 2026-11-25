use super::*;

#[test]
fn test_ts_config_hash() {
    let mut c = TextSearchConfig {
        schema: "public".into(),
        name: "\"my_config\"".into(),
        owner: "postgres".into(),
        parser: "pg_catalog.default".into(),
        mappings: vec!["word:english_stem".into()],
        comment: None,
        hash: None,
    };
    c.hash();
    assert!(c.hash.is_some());
}

#[test]
fn test_ts_config_get_script() {
    let mut c = TextSearchConfig {
        schema: "public".into(),
        name: "\"my_config\"".into(),
        owner: "postgres".into(),
        parser: "pg_catalog.default".into(),
        mappings: vec!["word:english_stem".into()],
        comment: None,
        hash: None,
    };
    c.hash();
    let s = c.get_script();
    assert!(s.contains("CREATE TEXT SEARCH CONFIGURATION"));
    assert!(s.contains("ADD MAPPING FOR word WITH english_stem"));
}

#[test]
fn test_ts_dict_get_script() {
    let mut d = TextSearchDict {
        schema: "public".into(),
        name: "\"my_dict\"".into(),
        owner: "postgres".into(),
        template: "pg_catalog.simple".into(),
        options: vec!["STOPWORDS = english".into()],
        comment: None,
        hash: None,
    };
    d.hash();
    let s = d.get_script();
    assert!(s.contains("CREATE TEXT SEARCH DICTIONARY"));
    assert!(s.contains("STOPWORDS = english"));
}

// ---- PG19: snowball language change (dutch -> dutch_porter) ----
//
// PostgreSQL 19 rewrote the Dutch Snowball stemmer and kept the old one as
// language 'dutch_porter', so switching a dictionary's language is the
// natural upgrade step. Options are stored verbatim in `dictinitoption` as
// `quote_identifier(name) = value` entries; an options-only change must be
// ALTERed in place, because `DROP ... CASCADE` also drops every
// configuration mapping the dictionary (and the generated tsvector columns
// and indexes built on those configurations).

fn snowball_dict(options: &[&str]) -> TextSearchDict {
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

/// PostgreSQL's `AlterTSDictionary` option handling (tsearchcmds.c): for each
/// statement option, remove every existing option with the same name, then
/// append it when it carries a value. Used to check that the emitted list
/// really produces the target's stored options, order included.
fn apply_alter(from: &[String], items: &[&str]) -> Vec<String> {
    let mut stored: Vec<String> = from.to_vec();
    for item in items {
        let name = dict_option_name(item).unwrap_or(item.trim());
        stored.retain(|o| dict_option_name(o) != Some(name));
        if item.contains('=') {
            stored.push(item.trim().to_string());
        }
    }
    stored
}

/// The option list inside the single `ALTER TEXT SEARCH DICTIONARY ... (...)`
/// statement of `script`.
fn alter_items(script: &str) -> Vec<&str> {
    let stmt = script
        .lines()
        .find(|l| l.starts_with("ALTER TEXT SEARCH DICTIONARY") && l.contains(" ("))
        .unwrap_or_else(|| panic!("no option ALTER in:\n{script}"));
    let inner = &stmt[stmt.find(" (").unwrap() + 2..stmt.rfind(");").unwrap()];
    inner.split(", ").collect()
}

#[test]
fn ts_dict_language_change_is_altered_in_place() {
    let from = snowball_dict(&["language = 'dutch'", "stopwords = 'dutch'"]);
    let to = snowball_dict(&["language = 'dutch_porter'", "stopwords = 'dutch'"]);

    let script = from.get_alter_script(&to, true);

    assert!(
        script.contains(
            "ALTER TEXT SEARCH DICTIONARY test_schema.nl_stem \
             (language = 'dutch_porter', stopwords = 'dutch');"
        ),
        "{script}"
    );
    assert!(!script.contains("DROP"), "{script}");
    assert!(!script.contains("CASCADE"), "{script}");
    assert!(!script.contains("CREATE"), "{script}");
    assert_eq!(
        apply_alter(&from.options, &alter_items(&script)),
        to.options
    );
}

#[test]
fn ts_dict_option_removal_uses_the_bare_name() {
    // The existing fixture case: simple dictionary loses its stopwords.
    let mut from = snowball_dict(&["stopwords = 'english'"]);
    from.template = "pg_catalog.simple".into();
    from.hash();
    let mut to = snowball_dict(&[]);
    to.template = "pg_catalog.simple".into();
    to.hash();

    let script = from.get_alter_script(&to, true);

    assert!(
        script.contains("ALTER TEXT SEARCH DICTIONARY test_schema.nl_stem (stopwords);"),
        "{script}"
    );
    assert!(!script.contains("DROP"), "{script}");
    assert!(apply_alter(&from.options, &alter_items(&script)).is_empty());
}

#[test]
fn ts_dict_removal_and_change_in_one_statement() {
    let from = snowball_dict(&["language = 'dutch'", "stopwords = 'dutch'"]);
    let to = snowball_dict(&["language = 'dutch_porter'"]);

    let script = from.get_alter_script(&to, true);

    assert!(
        script.contains(
            "ALTER TEXT SEARCH DICTIONARY test_schema.nl_stem \
             (stopwords, language = 'dutch_porter');"
        ),
        "{script}"
    );
    assert_eq!(
        apply_alter(&from.options, &alter_items(&script)),
        to.options
    );
}

#[test]
fn ts_dict_reordered_options_are_listed_in_target_order() {
    // Same options, different stored order: the hash differs, and listing
    // every target option in target order reproduces the order exactly.
    let from = snowball_dict(&["language = 'dutch'", "stopwords = 'dutch'"]);
    let to = snowball_dict(&["stopwords = 'dutch'", "language = 'dutch'"]);
    assert_ne!(from.hash, to.hash);

    let script = from.get_alter_script(&to, true);

    assert_eq!(
        alter_items(&script),
        vec!["stopwords = 'dutch'", "language = 'dutch'"]
    );
    assert_eq!(
        apply_alter(&from.options, &alter_items(&script)),
        to.options
    );
}

#[test]
fn ts_dict_added_options_converge() {
    let from = snowball_dict(&["language = 'english'"]);
    let to = snowball_dict(&["language = 'english'", "stopwords = 'english'"]);

    let script = from.get_alter_script(&to, true);

    assert!(
        script.contains(
            "ALTER TEXT SEARCH DICTIONARY test_schema.nl_stem \
             (language = 'english', stopwords = 'english');"
        ),
        "{script}"
    );
    assert_eq!(
        apply_alter(&from.options, &alter_items(&script)),
        to.options
    );
}

#[test]
fn ts_dict_option_alter_is_emitted_even_without_use_drop() {
    // It is not a drop, so --use-drop false must not comment it out.
    let from = snowball_dict(&["language = 'dutch'"]);
    let to = snowball_dict(&["language = 'dutch_porter'"]);

    let script = from.get_alter_script(&to, false);

    assert!(
        script.lines().any(|l| l
            == "ALTER TEXT SEARCH DICTIONARY test_schema.nl_stem (language = 'dutch_porter');"),
        "{script}"
    );
    assert!(!script.contains("DROP"), "{script}");
}

#[test]
fn ts_dict_option_alter_carries_a_rebuild_note() {
    let from = snowball_dict(&["language = 'dutch'"]);
    let to = snowball_dict(&["language = 'dutch_porter'"]);

    let script = from.get_alter_script(&to, true);
    let note = script
        .find("-- ALTER does not re-process stored data")
        .unwrap_or_else(|| panic!("{script}"));
    let alter = script.find("ALTER TEXT SEARCH DICTIONARY").unwrap();
    assert!(note < alter, "the note precedes the statement:\n{script}");
}

#[test]
fn ts_dict_template_change_still_drops_and_recreates() {
    let from = snowball_dict(&["language = 'dutch'"]);
    let mut to = snowball_dict(&["language = 'dutch'"]);
    to.template = "test_schema.my_template".into();
    to.hash();

    let script = from.get_alter_script(&to, true);
    assert!(
        script.contains("DROP TEXT SEARCH DICTIONARY IF EXISTS test_schema.nl_stem CASCADE;"),
        "{script}"
    );
    assert!(
        script.contains(
            "CREATE TEXT SEARCH DICTIONARY test_schema.nl_stem \
             (TEMPLATE = test_schema.my_template, language = 'dutch');"
        ),
        "{script}"
    );
    assert!(!script.contains("ALTER TEXT SEARCH DICTIONARY test_schema.nl_stem ("));

    // Without use_drop the drop stays in the output, commented out.
    let script = from.get_alter_script(&to, false);
    assert!(
        script.contains("-- DROP TEXT SEARCH DICTIONARY IF EXISTS test_schema.nl_stem CASCADE;"),
        "{script}"
    );
}

#[test]
fn ts_dict_template_and_option_change_drops_and_recreates() {
    let from = snowball_dict(&["language = 'dutch'"]);
    let mut to = snowball_dict(&["language = 'dutch_porter'"]);
    to.template = "test_schema.my_template".into();
    to.hash();

    let script = from.get_alter_script(&to, true);
    assert!(script.contains("CASCADE;"), "{script}");
    assert!(
        script.contains("(TEMPLATE = test_schema.my_template, language = 'dutch_porter');"),
        "{script}"
    );
    assert!(!script.contains("ALTER TEXT SEARCH DICTIONARY test_schema.nl_stem ("));
}

#[test]
fn ts_dict_owner_and_comment_follow_the_option_alter() {
    let from = snowball_dict(&["language = 'dutch'"]);
    let mut to = snowball_dict(&["language = 'dutch_porter'"]);
    to.owner = "new_owner".into();
    to.comment = Some("Dutch (Porter)".into());
    to.hash();

    let script = from.get_alter_script(&to, true);

    let options = script
        .find("ALTER TEXT SEARCH DICTIONARY test_schema.nl_stem (language = 'dutch_porter');")
        .unwrap_or_else(|| panic!("{script}"));
    let owner = script
        .find("ALTER TEXT SEARCH DICTIONARY test_schema.nl_stem OWNER TO new_owner;")
        .unwrap_or_else(|| panic!("{script}"));
    let comment = script
        .find("COMMENT ON TEXT SEARCH DICTIONARY test_schema.nl_stem IS 'Dutch (Porter)';")
        .unwrap_or_else(|| panic!("{script}"));
    assert!(options < owner && owner < comment, "{script}");
}

#[test]
fn ts_dict_owner_only_change_emits_no_option_alter() {
    let from = snowball_dict(&["language = 'dutch'"]);
    let mut to = snowball_dict(&["language = 'dutch'"]);
    to.owner = "new_owner".into();
    to.hash();

    let script = from.get_alter_script(&to, true);
    assert_eq!(
        script,
        "ALTER TEXT SEARCH DICTIONARY test_schema.nl_stem OWNER TO new_owner;\n\n"
    );
}

#[test]
fn ts_dict_quoted_option_names_are_re_emitted_verbatim() {
    let from = snowball_dict(&["\"StopWords\" = 'dutch'", "language = 'dutch'"]);
    let to = snowball_dict(&["language = 'dutch_porter'"]);

    let script = from.get_alter_script(&to, true);
    assert!(
        script.contains("(\"StopWords\", language = 'dutch_porter');"),
        "{script}"
    );
}

#[test]
fn ts_dict_unparseable_option_falls_back_to_drop_and_recreate() {
    // A fragment without `name = value` (e.g. from a value containing a
    // comma, which the dump splits naively) cannot be addressed by name.
    let from = snowball_dict(&["language = 'dutch'", "b'"]);
    let to = snowball_dict(&["language = 'dutch_porter'"]);

    let script = from.get_alter_script(&to, true);
    assert!(
        script.contains("DROP TEXT SEARCH DICTIONARY IF EXISTS"),
        "{script}"
    );
    assert!(script.contains("CREATE TEXT SEARCH DICTIONARY"), "{script}");
}

#[test]
fn ts_dict_repeated_target_name_falls_back_to_drop_and_recreate() {
    // A repeated name would replace its first occurrence during ALTER.
    let from = snowball_dict(&["language = 'dutch'"]);
    let to = snowball_dict(&["language = 'dutch'", "language = 'dutch_porter'"]);

    let script = from.get_alter_script(&to, true);
    assert!(
        script.contains("DROP TEXT SEARCH DICTIONARY IF EXISTS"),
        "{script}"
    );
}

#[test]
fn dict_option_name_parses_serialize_deflist_output() {
    assert_eq!(dict_option_name("language = 'dutch'"), Some("language"));
    assert_eq!(dict_option_name("STOPWORDS = english"), Some("STOPWORDS"));
    assert_eq!(dict_option_name("maxlen=10"), Some("maxlen"));
    assert_eq!(dict_option_name("  accept = false"), Some("accept"));
    assert_eq!(
        dict_option_name("\"Stop Words\" = 'x'"),
        Some("\"Stop Words\"")
    );
    assert_eq!(dict_option_name("\"a\"\"b\" = 'x'"), Some("\"a\"\"b\""));
    assert_eq!(dict_option_name("\"a = b\" = 'x'"), Some("\"a = b\""));
    assert_eq!(dict_option_name("b'"), None);
    assert_eq!(dict_option_name("= 'x'"), None);
    assert_eq!(dict_option_name("\"unterminated = 'x'"), None);
    assert_eq!(dict_option_name(""), None);
}

#[test]
fn ts_dict_hash_is_unchanged_by_the_in_place_alter_work() {
    // Pinned: the ALTER work adds no field and does not touch hash(), so
    // dumps written by pgc 1.0.x keep comparing equal.
    let d = snowball_dict(&["language = 'dutch'", "stopwords = 'dutch'"]);
    assert_eq!(d.hash.as_deref(), Some(PINNED_DICT_HASH));
}

/// SHA-256 over the `TextSearchDict::hash` byte layout for `snowball_dict`
/// with `language = 'dutch', stopwords = 'dutch'`, computed independently of
/// pgc.
const PINNED_DICT_HASH: &str = "5df1adf86ae133170f3e3196a7a2f7bb479d8032c123c88b10471931d3b8d449";

// ==== r3-constraints tests ====

fn config_mapping(dicts: &str) -> TextSearchConfig {
    let mut c = TextSearchConfig {
        schema: "test_schema".into(),
        name: "cfg".into(),
        owner: "postgres".into(),
        parser: "pg_catalog.\"default\"".into(),
        mappings: vec![
            format!("asciiword:{dicts}"),
            "word:pg_catalog.simple".into(),
        ],
        comment: None,
        hash: None,
    };
    c.hash();
    c
}

#[test]
fn r3c_dictionary_needs_a_new_one_for_a_template_or_an_unalterable_option() {
    let from = snowball_dict(&["language = 'dutch'"]);
    let mut template = snowball_dict(&["language = 'dutch'"]);
    template.template = "pg_catalog.simple".into();
    assert!(from.is_recreated_as(&template));
    assert!(!from.is_recreated_as(&snowball_dict(&["language = 'dutch_porter'"])));
    assert!(!from.is_recreated_as(&from));
    // An option the in-place ALTER cannot reproduce (a repeated name).
    let repeated = snowball_dict(&["language = 'dutch'", "language = 'english'"]);
    assert!(from.is_recreated_as(&repeated));
}

#[test]
fn r3c_dictionary_recreate_moves_the_configurations_without_cascade() {
    let from = snowball_dict(&["language = 'dutch'"]);
    let mut to = snowball_dict(&[]);
    to.template = "pg_catalog.simple".into();
    to.owner = "app".into();
    to.comment = Some("it's new".into());
    to.hash();
    let cfg = config_mapping("test_schema.nl_stem,pg_catalog.simple");
    let script = from.get_recreate_script(&to, &[&cfg]);
    let temporary = from.temporary_name();
    // One `DO` block: a failing drop takes the temporary dictionary and
    // the re-pointed mappings with it, so the script can simply run again.
    assert_eq!(
        script,
        format!(
            "-- Re-created (its template or options cannot be altered in place) without CASCADE: \
             the text search configurations mapping it move to the new dictionary. Stored data is \
             not re-processed: rebuild the indexes and stored tsvector columns built through them \
             if its output changed.\n\
             DO $pgc$\n\
             BEGIN\n    \
             CREATE TEXT SEARCH DICTIONARY test_schema.{temporary} (TEMPLATE = pg_catalog.simple);\n    \
             ALTER TEXT SEARCH CONFIGURATION test_schema.cfg ALTER MAPPING REPLACE test_schema.nl_stem \
             WITH test_schema.{temporary};\n    \
             DROP TEXT SEARCH DICTIONARY test_schema.nl_stem;\n    \
             ALTER TEXT SEARCH DICTIONARY test_schema.{temporary} RENAME TO nl_stem;\n    \
             ALTER TEXT SEARCH DICTIONARY test_schema.nl_stem OWNER TO app;\n    \
             COMMENT ON TEXT SEARCH DICTIONARY test_schema.nl_stem IS 'it''s new';\n\
             END\n\
             $pgc$;\n\n"
        )
    );
    assert!(!script.contains("CASCADE;"), "{script}");
}

#[test]
fn r3c_dictionary_recreate_block_quotes_around_a_comment_holding_its_tag() {
    let from = snowball_dict(&["language = 'dutch'"]);
    let mut to = snowball_dict(&[]);
    to.template = "pg_catalog.simple".into();
    to.comment = Some("ends $pgc$; drop table x; --".into());
    to.hash();
    let script = from.get_recreate_script(&to, &[]);
    assert!(script.contains("DO $pgc1$\n"), "{script}");
    assert!(script.ends_with("END\n$pgc1$;\n\n"), "{script}");
    assert!(
        script.contains("IS 'ends $pgc$; drop table x; --';\n"),
        "{script}"
    );
}

#[test]
fn r3c_dictionary_temporary_name_is_a_stable_plain_identifier() {
    let a = snowball_dict(&[]);
    let mut quoted = snowball_dict(&[]);
    quoted.name = "\"Nl Stem\"".into();
    let name = a.temporary_name();
    assert_eq!(name, a.temporary_name());
    assert_ne!(name, quoted.temporary_name());
    assert!(name.starts_with("pgc_new_") && name.len() == "pgc_new_".len() + 16);
    assert!(
        name.chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
    );
}

#[test]
fn r3c_configuration_tells_the_dictionaries_it_maps() {
    let cfg = config_mapping("test_schema.nl_stem,pg_catalog.simple");
    assert!(cfg.maps_dictionary("test_schema", "nl_stem"));
    assert!(cfg.maps_dictionary("pg_catalog", "simple"));
    assert!(!cfg.maps_dictionary("test_schema", "nl"));
    assert!(!cfg.maps_dictionary("other", "nl_stem"));
    // A quoted name with a comma in it is one dictionary.
    let cfg = config_mapping("\"My, Schema\".\"Dict, A\",pg_catalog.simple");
    assert!(cfg.maps_dictionary("\"My, Schema\"", "\"Dict, A\""));
    assert!(!cfg.maps_dictionary("\"My", "Schema\".\"Dict"));
    assert_eq!(
        split_dictionary_list("a.b, \"c,d\".e ,f.g"),
        ["a.b", "\"c,d\".e", "f.g"]
    );
}
