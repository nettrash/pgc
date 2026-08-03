use super::*;

fn make_op() -> Operator {
    Operator {
        schema: "public".into(),
        name: "===".into(),
        owner: "postgres".into(),
        left_type: Some("integer".into()),
        right_type: Some("integer".into()),
        result_type: "boolean".into(),
        procedure: "public.my_eq".into(),
        commutator: None,
        negator: None,
        restrict: None,
        join: None,
        is_hashes: false,
        is_merges: false,
        comment: None,
        hash: None,
    }
}

#[test]
fn test_hash_populates() {
    let mut op = make_op();
    op.hash();
    assert!(op.hash.is_some());
}

#[test]
fn test_get_script_contains_create_operator() {
    let mut op = make_op();
    op.hash();
    let s = op.get_script();
    assert!(s.contains("CREATE OPERATOR"));
    assert!(s.contains("PROCEDURE = public.my_eq"));
}

#[test]
fn test_get_drop_script() {
    let mut op = make_op();
    op.hash();
    assert!(op.get_drop_script().contains("DROP OPERATOR"));
}
