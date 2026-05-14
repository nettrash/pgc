use super::*;

#[test]
fn test_publication_hash_populates() {
    let mut p = Publication {
        name: "\"my_pub\"".into(),
        owner: "postgres".into(),
        all_tables: false,
        publish: "insert,update,delete".into(),
        tables: vec!["public.orders".into()],
        comment: None,
        hash: None,
    };
    p.hash();
    assert!(p.hash.is_some());
}

#[test]
fn test_publication_get_script() {
    let mut p = Publication {
        name: "\"my_pub\"".into(),
        owner: "postgres".into(),
        all_tables: false,
        publish: "insert,update,delete".into(),
        tables: vec!["public.orders".into()],
        comment: None,
        hash: None,
    };
    p.hash();
    let s = p.get_script();
    assert!(s.contains("CREATE PUBLICATION"));
    assert!(s.contains("FOR TABLE public.orders"));
}

#[test]
fn test_subscription_get_script() {
    let mut s = Subscription {
        name: "\"my_sub\"".into(),
        owner: "postgres".into(),
        connection: "host=primary dbname=mydb".into(),
        publications: vec!["my_pub".into()],
        enabled: true,
        comment: None,
        hash: None,
    };
    s.hash();
    let script = s.get_script();
    assert!(script.contains("CREATE SUBSCRIPTION"));
    assert!(script.contains("my_pub"));
}

#[test]
fn alter_to_all_tables_use_drop_true_drops_and_recreates() {
    let from = Publication {
        name: "my_pub".into(),
        owner: "postgres".into(),
        all_tables: false,
        publish: "insert,update,delete".into(),
        tables: vec!["public.orders".into()],
        comment: None,
        hash: None,
    };
    let to = Publication {
        name: "my_pub".into(),
        owner: "postgres".into(),
        all_tables: true,
        publish: "insert,update,delete".into(),
        tables: vec![],
        comment: None,
        hash: None,
    };
    let script = from.get_alter_script(&to, true);
    assert!(
        script.contains("DROP PUBLICATION IF EXISTS my_pub;"),
        "use_drop=true must DROP, got: {script}"
    );
    assert!(
        script.contains("CREATE PUBLICATION my_pub FOR ALL TABLES"),
        "use_drop=true must CREATE with FOR ALL TABLES, got: {script}"
    );
}

#[test]
fn alter_to_all_tables_use_drop_false_comments_out() {
    let from = Publication {
        name: "my_pub".into(),
        owner: "postgres".into(),
        all_tables: false,
        publish: "insert,update,delete".into(),
        tables: vec!["public.orders".into()],
        comment: None,
        hash: None,
    };
    let to = Publication {
        name: "my_pub".into(),
        owner: "postgres".into(),
        all_tables: true,
        publish: "insert,update,delete".into(),
        tables: vec![],
        comment: None,
        hash: None,
    };
    let script = from.get_alter_script(&to, false);
    assert!(
        script.contains("-- use_drop=false"),
        "must include use_drop=false warning, got: {script}"
    );
    assert!(
        script.contains("-- DROP PUBLICATION IF EXISTS my_pub;"),
        "must have commented-out DROP, got: {script}"
    );
    assert!(
        script.contains("-- CREATE PUBLICATION my_pub FOR ALL TABLES"),
        "must have commented-out CREATE, got: {script}"
    );
    // Must NOT have uncommented DROP or CREATE
    let has_active_drop = script.lines().any(|l| {
        let t = l.trim_start();
        t.starts_with("DROP PUBLICATION") && !t.starts_with("--")
    });
    assert!(!has_active_drop, "must not have active DROP, got: {script}");
}
