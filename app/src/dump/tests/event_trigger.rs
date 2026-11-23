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

// ==== r3-ddl: event trigger names ====

fn named_trigger(name: &str, enabled: &str, comment: Option<&str>) -> EventTrigger {
    EventTrigger::new(
        name.into(),
        "ddl_command_start".into(),
        "app.evt_fn".into(),
        vec![],
        enabled.into(),
        "postgres".into(),
        comment.map(str::to_string),
    )
}

/// The dump stores the name `quote_ident`-ed, so a name that needs quoting
/// arrives with its quotes and goes into every statement as it is — not
/// quoted a second time, which named a different trigger.
#[test]
fn a_quoted_name_is_used_as_the_dump_stores_it() {
    let et = named_trigger("\"AuditTrg\"", "D", Some("audit"));
    assert_eq!(
        et.get_script(),
        "create event trigger \"AuditTrg\" on ddl_command_start\n    \
         execute function app.evt_fn();\n\n\
         alter event trigger \"AuditTrg\" disable;\n\n\
         alter event trigger \"AuditTrg\" owner to postgres;\n\n\
         comment on event trigger \"AuditTrg\" is 'audit';\n\n"
    );
    assert_eq!(
        et.get_drop_script(),
        "drop event trigger if exists \"AuditTrg\";\n\n"
    );

    let from = named_trigger("\"My \"\"Trg\"\"\"", "O", None);
    let to = named_trigger("\"My \"\"Trg\"\"\"", "A", Some("c"));
    assert_eq!(
        from.get_alter_script(&to, true),
        "alter event trigger \"My \"\"Trg\"\"\" enable always;\n\n\
         comment on event trigger \"My \"\"Trg\"\"\" is 'c';\n\n"
    );
    assert_eq!(
        to.get_alter_script(&from, true),
        "alter event trigger \"My \"\"Trg\"\"\" enable;\n\n\
         comment on event trigger \"My \"\"Trg\"\"\" is null;\n\n"
    );
}

/// A name `quote_ident` leaves bare is double-quoted, as it always was, so
/// the output for such a name is unchanged.
#[test]
fn a_bare_name_keeps_its_double_quotes() {
    let et = named_trigger("audit_ddl", "R", None);
    assert!(
        et.get_script()
            .starts_with("create event trigger \"audit_ddl\" on ddl_command_start"),
        "{}",
        et.get_script()
    );
    assert!(
        et.get_script()
            .contains("alter event trigger \"audit_ddl\" enable replica;")
    );
    assert_eq!(
        et.get_drop_script(),
        "drop event trigger if exists \"audit_ddl\";\n\n"
    );
}

/// With `use_drop` off, a definition change is noted in a comment naming the
/// trigger the way the statements do.
#[test]
fn the_use_drop_note_names_the_trigger_once_quoted() {
    let from = named_trigger("\"AuditTrg\"", "O", None);
    let mut to = named_trigger("\"AuditTrg\"", "O", None);
    to.event = "ddl_command_end".into();
    assert_eq!(
        from.get_alter_script(&to, false),
        "-- Event trigger \"AuditTrg\" requires drop+recreate (definition changed); use_drop=false\n"
    );
}
