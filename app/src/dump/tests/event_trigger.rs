use super::*;

#[test]
fn test_hash() {
    let mut et = EventTrigger::new(
        "my_trigger".into(),
        "ddl_command_end".into(),
        "public.my_func".into(),
        vec!["CREATE TABLE".into()],
        "O".into(),
        "postgres".into(),
        None,
    );
    assert!(et.hash.is_some());
    let h1 = et.hash.clone();
    et.event = "ddl_command_start".into();
    et.hash();
    assert_ne!(h1, et.hash);
}

#[test]
fn test_get_script() {
    let et = EventTrigger::new(
        "audit_ddl".into(),
        "ddl_command_end".into(),
        "public.log_ddl".into(),
        vec!["CREATE TABLE".into(), "DROP TABLE".into()],
        "O".into(),
        "postgres".into(),
        Some("Audit DDL changes".into()),
    );
    let script = et.get_script();
    assert!(script.contains("create event trigger"));
    assert!(script.contains("when tag in"));
    assert!(script.contains("execute function public.log_ddl()"));
    assert!(script.contains("comment on event trigger"));
}

#[test]
fn test_get_script_disabled() {
    let et = EventTrigger::new(
        "audit_ddl".into(),
        "ddl_command_end".into(),
        "public.log_ddl".into(),
        vec![],
        "D".into(),
        "postgres".into(),
        None,
    );
    let script = et.get_script();
    assert!(script.contains("alter event trigger \"audit_ddl\" disable"));
}

#[test]
fn test_drop_script() {
    let et = EventTrigger::new(
        "audit_ddl".into(),
        "ddl_command_end".into(),
        "public.log_ddl".into(),
        vec![],
        "O".into(),
        "".into(),
        None,
    );
    assert!(
        et.get_drop_script()
            .contains("drop event trigger if exists")
    );
}

#[test]
fn test_alter_enabled_change() {
    let from = EventTrigger::new(
        "audit_ddl".into(),
        "ddl_command_end".into(),
        "public.log_ddl".into(),
        vec![],
        "O".into(),
        "postgres".into(),
        None,
    );
    let to = EventTrigger::new(
        "audit_ddl".into(),
        "ddl_command_end".into(),
        "public.log_ddl".into(),
        vec![],
        "D".into(),
        "postgres".into(),
        None,
    );
    let script = from.get_alter_script(&to, true);
    assert!(script.contains("disable"));
}
