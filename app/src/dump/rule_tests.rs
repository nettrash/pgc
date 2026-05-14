use super::*;

#[test]
fn test_rule_hash() {
    let mut r = Rule::new(
        "public".into(),
        "orders".into(),
        "protect_delete".into(),
        "CREATE OR REPLACE RULE protect_delete AS ON DELETE TO public.orders DO INSTEAD NOTHING"
            .into(),
        None,
    );
    assert!(r.hash.is_some());
    let h1 = r.hash.clone();
    r.definition =
        "CREATE OR REPLACE RULE protect_delete AS ON DELETE TO public.orders DO ALSO NOTHING"
            .into();
    r.hash();
    assert_ne!(h1, r.hash);
}

#[test]
fn test_get_script() {
    let r = Rule::new(
        "public".into(),
        "orders".into(),
        "protect_delete".into(),
        "CREATE OR REPLACE RULE protect_delete AS ON DELETE TO public.orders DO INSTEAD NOTHING"
            .into(),
        Some("Prevent deletes".into()),
    );
    let script = r.get_script();
    assert!(script.contains("CREATE OR REPLACE RULE"));
    assert!(script.contains("comment on rule"));
    assert!(script.contains("Prevent deletes"));
}

#[test]
fn test_get_drop_script() {
    let r = Rule::new(
        "public".into(),
        "orders".into(),
        "protect_delete".into(),
        "CREATE OR REPLACE RULE protect_delete AS ON DELETE TO public.orders DO INSTEAD NOTHING"
            .into(),
        None,
    );
    let script = r.get_drop_script();
    assert!(script.contains("drop rule if exists"));
    assert!(script.contains("cascade"));
}

#[test]
fn test_get_alter_script_definition_change() {
    let from = Rule::new(
        "public".into(),
        "orders".into(),
        "protect_delete".into(),
        "CREATE OR REPLACE RULE protect_delete AS ON DELETE TO public.orders DO INSTEAD NOTHING"
            .into(),
        None,
    );
    let to = Rule::new(
        "public".into(),
        "orders".into(),
        "protect_delete".into(),
        "CREATE OR REPLACE RULE protect_delete AS ON DELETE TO public.orders DO ALSO NOTHING"
            .into(),
        None,
    );
    let script = from.get_alter_script(&to, true);
    assert!(script.contains("DO ALSO NOTHING"));
}
