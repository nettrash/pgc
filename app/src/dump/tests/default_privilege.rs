use super::*;

#[test]
fn test_hash_populates() {
    let mut dp = DefaultPrivilege {
        role_name: "alice".into(),
        schema_name: "public".into(),
        object_type: "r".into(),
        acl: vec!["bob=rw/alice".into()],
        hash: None,
    };
    dp.hash();
    assert!(dp.hash.is_some());
}

#[test]
fn test_get_script_contains_alter_default_privileges() {
    let mut dp = DefaultPrivilege {
        role_name: "alice".into(),
        schema_name: "public".into(),
        object_type: "r".into(),
        acl: vec!["bob=r/alice".into()],
        hash: None,
    };
    dp.hash();
    let s = dp.get_script();
    assert!(s.contains("ALTER DEFAULT PRIVILEGES"));
    assert!(s.contains("FOR ROLE alice"));
    assert!(s.contains("IN SCHEMA public"));
    assert!(s.contains("GRANT SELECT ON TABLES TO bob"));
}

#[test]
fn test_object_type_name() {
    let dp = DefaultPrivilege {
        role_name: "".into(),
        schema_name: "".into(),
        object_type: "f".into(),
        acl: vec![],
        hash: None,
    };
    assert_eq!(dp.object_type_name(), "FUNCTIONS");
}
