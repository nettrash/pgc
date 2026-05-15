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
