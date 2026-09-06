use super::*;

fn make_cast() -> Cast {
    Cast {
        source_type: "text".into(),
        target_type: "integer".into(),
        cast_method: "f".into(),
        function_name: Some("pg_catalog.int4(text)".into()),
        cast_context: "e".into(),
        comment: None,
        hash: None,
    }
}

#[test]
fn test_hash_populates() {
    let mut c = make_cast();
    c.hash();
    assert!(c.hash.is_some());
}

#[test]
fn test_get_script_with_function() {
    let mut c = make_cast();
    c.hash();
    let s = c.get_script();
    assert!(s.contains("CREATE CAST"));
    assert!(s.contains("WITH FUNCTION"));
    assert!(s.contains("pg_catalog.int4(text)"));
}

#[test]
fn test_get_script_implicit() {
    let mut c = make_cast();
    c.cast_context = "i".into();
    c.hash();
    let s = c.get_script();
    assert!(s.contains("AS IMPLICIT"));
}

#[test]
fn test_get_drop_script() {
    let mut c = make_cast();
    c.hash();
    assert!(c.get_drop_script().contains("DROP CAST"));
}
