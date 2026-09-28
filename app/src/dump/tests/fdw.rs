use super::*;

#[test]
fn test_fdw_hash_populates() {
    let mut f = ForeignDataWrapper {
        name: "\"myfdw\"".into(),
        owner: "postgres".into(),
        handler_func: Some("my_handler".into()),
        validator_func: None,
        connection_func: None,
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
        connection_func: None,
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
        acl: Vec::new(),
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

// ---- PG19: FDW CONNECTION function ----

fn wrapper(connection: Option<&str>) -> ForeignDataWrapper {
    ForeignDataWrapper {
        name: "w".into(),
        owner: "postgres".into(),
        handler_func: Some("public.postgres_fdw_handler".into()),
        validator_func: Some("public.postgres_fdw_validator".into()),
        connection_func: connection.map(Into::into),
        options: vec!["debug=true".into()],
        comment: None,
        hash: None,
    }
}

fn hash_of(mut f: ForeignDataWrapper) -> String {
    f.hash();
    f.hash.unwrap()
}

fn statements(script: &str) -> Vec<&str> {
    script
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .collect()
}

#[test]
fn fdw_hash_without_connection_matches_pgc_1_0_x() {
    // Pinned digests of the pgc 1.0.27 algorithm (name, owner, handler,
    // validator, options, comment): hashes live in dump files and are
    // compared verbatim.
    let mut plain = ForeignDataWrapper {
        name: "\"myfdw\"".into(),
        owner: "postgres".into(),
        handler_func: Some("my_handler".into()),
        validator_func: None,
        connection_func: None,
        options: vec![],
        comment: None,
        hash: None,
    };
    plain.hash();
    assert_eq!(
        plain.hash.as_deref(),
        Some("7979b3dfb47d5938b5fed31dbcc59e4613e4e1c0ef7a0c8d17fc4e1bbec1847b")
    );
    assert_eq!(
        hash_of(wrapper(None)),
        "59182414f07b4aa79b00482e593e71788681535905a7346bd9095bb618de336f"
    );
}

#[test]
fn fdw_hash_changes_with_the_connection_function() {
    let none = hash_of(wrapper(None));
    let a = hash_of(wrapper(Some("public.postgres_fdw_connection")));
    let b = hash_of(wrapper(Some("probe.my_conn")));
    assert_ne!(none, a);
    assert_ne!(a, b);
}

#[test]
fn fdw_get_script_emits_connection_before_options() {
    let script = wrapper(Some("public.postgres_fdw_connection")).get_script();
    assert_eq!(
        statements(&script)[0],
        "CREATE FOREIGN DATA WRAPPER w HANDLER public.postgres_fdw_handler \
         VALIDATOR public.postgres_fdw_validator CONNECTION public.postgres_fdw_connection \
         OPTIONS (debug 'true');"
    );
    let bare = ForeignDataWrapper {
        handler_func: None,
        validator_func: None,
        options: vec![],
        ..wrapper(Some("public.postgres_fdw_connection"))
    };
    assert_eq!(
        statements(&bare.get_script())[0],
        "CREATE FOREIGN DATA WRAPPER w NO HANDLER NO VALIDATOR CONNECTION public.postgres_fdw_connection;"
    );
}

#[test]
fn fdw_get_script_without_connection_never_mentions_it() {
    // `NO CONNECTION` is the default and a syntax error before PG19.
    let script = wrapper(None).get_script();
    assert!(!script.contains("CONNECTION"), "got: {script}");
    assert_eq!(
        statements(&script)[0],
        "CREATE FOREIGN DATA WRAPPER w HANDLER public.postgres_fdw_handler \
         VALIDATOR public.postgres_fdw_validator OPTIONS (debug 'true');"
    );
}

#[test]
fn fdw_alter_connection_only_emits_just_the_connection_clause() {
    let none = wrapper(None);
    let with = wrapper(Some("public.postgres_fdw_connection"));
    let other = wrapper(Some("probe.my_conn"));
    assert_eq!(
        statements(&none.get_alter_script(&with, true)),
        vec!["ALTER FOREIGN DATA WRAPPER w CONNECTION public.postgres_fdw_connection;"]
    );
    assert_eq!(
        statements(&with.get_alter_script(&none, true)),
        vec!["ALTER FOREIGN DATA WRAPPER w NO CONNECTION;"]
    );
    assert_eq!(
        statements(&with.get_alter_script(&other, true)),
        vec!["ALTER FOREIGN DATA WRAPPER w CONNECTION probe.my_conn;"]
    );
    assert!(with.get_alter_script(&with.clone(), true).is_empty());
}

#[test]
fn fdw_alter_options_with_unchanged_connection_keeps_the_pre_pg19_output() {
    let from = wrapper(Some("public.postgres_fdw_connection"));
    let to = ForeignDataWrapper {
        options: vec!["debug=false".into()],
        ..wrapper(Some("public.postgres_fdw_connection"))
    };
    let script = from.get_alter_script(&to, true);
    assert!(!script.contains("CONNECTION"), "got: {script}");
    assert_eq!(
        statements(&script),
        vec![
            "ALTER FOREIGN DATA WRAPPER w HANDLER public.postgres_fdw_handler VALIDATOR public.postgres_fdw_validator;",
            "ALTER FOREIGN DATA WRAPPER w OPTIONS (DROP debug);",
            "ALTER FOREIGN DATA WRAPPER w OPTIONS (ADD debug 'false');",
        ]
    );
}

#[test]
fn fdw_alter_handler_and_connection_change_share_one_statement() {
    let from = ForeignDataWrapper {
        handler_func: None,
        ..wrapper(None)
    };
    let to = wrapper(Some("public.postgres_fdw_connection"));
    assert_eq!(
        statements(&from.get_alter_script(&to, true)),
        vec![
            "ALTER FOREIGN DATA WRAPPER w HANDLER public.postgres_fdw_handler \
             VALIDATOR public.postgres_fdw_validator CONNECTION public.postgres_fdw_connection;"
        ]
    );
}

#[test]
fn fdw_serde_skips_an_unset_connection_function() {
    let json = serde_json::to_value(wrapper(None)).unwrap();
    assert!(json.get("connection_func").is_none());
    let back: ForeignDataWrapper =
        serde_json::from_value(serde_json::to_value(wrapper(Some("public.f"))).unwrap()).unwrap();
    assert_eq!(back.connection_func.as_deref(), Some("public.f"));
}

// ==== r3-constraints tests ====

fn server_on(fdw: &str) -> ForeignServer {
    let mut s = ForeignServer {
        name: "\"Srv\"".into(),
        owner: "postgres".into(),
        fdw_name: fdw.into(),
        server_type: None,
        server_version: None,
        options: vec!["host=h".into()],
        comment: None,
        hash: None,
        acl: Vec::new(),
    };
    s.hash();
    s
}

#[test]
fn r3c_wrapper_change_is_told_and_dropped_without_cascade() {
    let a = server_on("w1");
    let b = server_on("w2");
    assert!(a.changes_wrapper(&b));
    assert!(!a.changes_wrapper(&a));
    let mut other_options = server_on("w1");
    other_options.options = vec!["host=other".into()];
    assert!(!a.changes_wrapper(&other_options));
    assert_eq!(
        a.get_restrict_drop_script(),
        "DROP SERVER IF EXISTS \"Srv\";\n\n"
    );
    // The alter itself still drops with CASCADE: the comparer never asks it
    // for a wrapper change with use_drop.
    assert!(
        a.get_alter_script(&b, true)
            .starts_with("DROP SERVER IF EXISTS \"Srv\" CASCADE;")
    );
}

#[test]
fn r3c_server_acl_is_not_hashed() {
    // `srvacl` is dumped only so a server the migration creates again gets
    // TO's grants; every server keeps the hash pgc 1.0.x gave it.
    let plain = server_on("w1");
    let mut granted = server_on("w1");
    granted.acl = vec!["postgres=U/postgres".into(), "reader=U/postgres".into()];
    granted.hash();
    assert_eq!(plain.hash, granted.hash);
    assert!(!granted.get_script().contains("GRANT"));
}
