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

// ==== fix:pre-grants tests ====

fn table_defaults(acl: &[&str]) -> DefaultPrivilege {
    DefaultPrivilege {
        role_name: "owner".into(),
        schema_name: "app".into(),
        object_type: "r".into(),
        acl: acl.iter().map(|s| s.to_string()).collect(),
        hash: None,
    }
}

#[test]
fn test_grantee_is_quoted_as_sql_needs_it() {
    // aclitem text leaves Reader and user bare; written as-is they named
    // reader or failed to parse.
    let dp = table_defaults(&[
        "Reader=r/owner",
        "user=a/owner",
        "reader=w/owner",
        "=r/owner",
    ]);
    assert_eq!(
        dp.get_script(),
        "ALTER DEFAULT PRIVILEGES FOR ROLE owner IN SCHEMA app GRANT SELECT ON TABLES TO \"Reader\";\n\n\
         ALTER DEFAULT PRIVILEGES FOR ROLE owner IN SCHEMA app GRANT INSERT ON TABLES TO \"user\";\n\n\
         ALTER DEFAULT PRIVILEGES FOR ROLE owner IN SCHEMA app GRANT UPDATE ON TABLES TO reader;\n\n\
         ALTER DEFAULT PRIVILEGES FOR ROLE owner IN SCHEMA app GRANT SELECT ON TABLES TO PUBLIC;\n\n"
    );
    assert_eq!(
        table_defaults(&["Reader=r/owner"]).get_revoke_script(),
        "ALTER DEFAULT PRIVILEGES FOR ROLE owner IN SCHEMA app REVOKE SELECT ON TABLES FROM \"Reader\";\n\n"
    );
}

#[test]
fn test_quoted_names_with_separators_are_parsed_whole() {
    // A `/` or `=` inside a quoted grantee or grantor no longer splits the
    // item in the wrong place.
    let dp = table_defaults(&["\"a=b\"=r/owner", "reader=w/\"we/ird\""]);
    assert_eq!(
        dp.get_script(),
        "ALTER DEFAULT PRIVILEGES FOR ROLE owner IN SCHEMA app GRANT SELECT ON TABLES TO \"a=b\";\n\n\
         ALTER DEFAULT PRIVILEGES FOR ROLE owner IN SCHEMA app GRANT UPDATE ON TABLES TO reader;\n\n"
    );
    // An item that grants nothing emits nothing, as before.
    assert_eq!(
        table_defaults(&["reader=/owner", "garbage"]).get_script(),
        ""
    );
}
