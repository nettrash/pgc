use super::*;

fn make_collation() -> Collation {
    Collation {
        schema: "public".into(),
        name: "\"my_coll\"".into(),
        owner: "postgres".into(),
        provider: "i".into(),
        locale: None,
        lc_collate: None,
        lc_ctype: None,
        icu_locale: Some("en-US".into()),
        icu_rules: None,
        deterministic: true,
        comment: None,
        hash: None,
    }
}

#[test]
fn test_hash_populates() {
    let mut c = make_collation();
    c.hash();
    assert!(c.hash.is_some());
}

#[test]
fn test_hash_changes_with_icu_locale() {
    let mut c = make_collation();
    c.hash();
    let h1 = c.hash.clone();
    c.icu_locale = Some("fr-FR".into());
    c.hash();
    assert_ne!(h1, c.hash);
}

#[test]
fn test_get_script_contains_create_collation() {
    let mut c = make_collation();
    c.hash();
    let s = c.get_script();
    assert!(s.contains("CREATE COLLATION"));
    assert!(s.contains("en-US"));
}

#[test]
fn test_get_drop_script() {
    let mut c = make_collation();
    c.hash();
    assert!(c.get_drop_script().contains("DROP COLLATION"));
}
