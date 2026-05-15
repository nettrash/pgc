use super::*;

#[test]
fn test_fdw_hash_populates() {
    let mut f = ForeignDataWrapper {
        name: "\"myfdw\"".into(),
        owner: "postgres".into(),
        handler_func: Some("my_handler".into()),
        validator_func: None,
        options: vec![],
        comment: None,
        hash: None,
    };
    f.hash();
    assert!(f.hash.is_some());
}

#[test]
fn test_fdw_get_script() {
    let mut f = ForeignDataWrapper {
        name: "\"myfdw\"".into(),
        owner: "postgres".into(),
        handler_func: Some("my_handler".into()),
        validator_func: None,
        options: vec![],
        comment: None,
        hash: None,
    };
    f.hash();
    let s = f.get_script();
    assert!(s.contains("CREATE FOREIGN DATA WRAPPER"));
    assert!(s.contains("HANDLER my_handler"));
}

#[test]
fn test_server_get_script() {
    let mut s = ForeignServer {
        name: "\"myserver\"".into(),
        owner: "postgres".into(),
        fdw_name: "myfdw".into(),
        server_type: None,
        server_version: Some("1.0".into()),
        options: vec!["host 'localhost'".into()],
        comment: None,
        hash: None,
    };
    s.hash();
    let script = s.get_script();
    assert!(script.contains("CREATE SERVER"));
    assert!(script.contains("FOREIGN DATA WRAPPER myfdw"));
}

#[test]
fn test_user_mapping_get_script() {
    let mut m = UserMapping {
        server_name: "myserver".into(),
        username: "alice".into(),
        options: vec!["user 'alice_remote'".into()],
        hash: None,
    };
    m.hash();
    let s = m.get_script();
    assert!(s.contains("CREATE USER MAPPING FOR alice SERVER myserver"));
}
