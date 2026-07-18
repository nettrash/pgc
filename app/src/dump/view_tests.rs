use super::*;

fn create_view(definition: &str) -> View {
    View::new(
        "active_users".to_string(),
        definition.to_string(),
        "analytics".to_string(),
        vec!["public.users".to_string(), "public.sessions".to_string()],
    )
}

fn create_materialized_view(definition: &str) -> View {
    let mut view = View::new(
        "active_users".to_string(),
        definition.to_string(),
        "analytics".to_string(),
        vec!["public.users".to_string()],
    );
    view.is_materialized = true;
    view.hash();
    view
}

#[test]
fn test_view_new_initializes_hash() {
    let definition = "select id from public.users where active";
    let view = create_view(definition);

    let expected_hash = format!(
        "{:x}",
        md5::compute(format!(
            "analytics.active_users.{definition}...false.false...."
        ))
    );

    assert_eq!(view.hash.as_deref(), Some(expected_hash.as_str()));
    assert_eq!(view.schema, "analytics");
    assert_eq!(view.name, "active_users");
    assert_eq!(view.definition, definition);
}

#[test]
fn test_hash_updates_on_mutation() {
    let mut view = create_view("select 1");
    let original_hash = view.hash.clone();

    view.definition = "select 2".to_string();
    view.hash();

    assert_ne!(view.hash, original_hash);
}

#[test]
fn test_get_script_returns_create_statement() {
    let view = create_view("select id from public.users");
    assert_eq!(
        view.get_script(),
        "create view analytics.active_users as\nselect id from public.users;\n\n"
    );
}

#[test]
fn test_get_script_returns_create_materialized_statement() {
    let view = create_materialized_view("select id from public.users");
    assert_eq!(
        view.get_script(),
        "create materialized view analytics.active_users as\nselect id from public.users;\n\n"
    );
}

// A materialized view created WITH NO DATA is unpopulated; recreating it without the
// clause would run the query and fill it (issue #220).
#[test]
fn test_get_script_materialized_unpopulated_emits_with_no_data() {
    let mut view = create_materialized_view("select id from public.users");
    view.is_populated = false;
    assert_eq!(
        view.get_script(),
        "create materialized view analytics.active_users as\nselect id from public.users\nwith no data;\n\n"
    );
}

// PostgreSQL hands back definitions with a terminating semicolon; the clause has to
// land inside the statement rather than after it.
#[test]
fn test_get_script_with_no_data_precedes_definition_semicolon() {
    let mut view = create_materialized_view("select id from public.users;");
    view.is_populated = false;
    let script = view.get_script();
    assert_eq!(
        script,
        "create materialized view analytics.active_users as\nselect id from public.users\nwith no data;\n\n"
    );
    assert!(
        !script.contains("users;\nwith no data"),
        "with no data must not follow the statement-terminating semicolon: {script}"
    );
}

#[test]
fn test_get_script_populated_materialized_omits_with_no_data() {
    let view = create_materialized_view("select id from public.users;");
    assert!(!view.get_script().contains("with no data"));
}

// The same trap as WITH NO DATA: a check option appended after the terminating
// semicolon parses as a separate statement and is a syntax error.
#[test]
fn test_get_script_check_option_precedes_definition_semicolon() {
    let mut view = create_view("select id from public.users where active;");
    view.check_option = Some("cascaded".to_string());
    assert_eq!(
        view.get_script(),
        "create view analytics.active_users as\nselect id from public.users where active\nwith cascaded check option;\n\n"
    );
}

#[test]
fn test_get_alter_script_check_option_precedes_definition_semicolon() {
    let current = create_view("select id from public.users;");
    let mut target = create_view("select id, active from public.users;");
    target.check_option = Some("local".to_string());
    target.hash();
    let script = current.get_alter_script(&target, true);
    assert!(
        script.contains("select id, active from public.users\nwith local check option;"),
        "check option must sit inside the statement: {script}"
    );
}

#[test]
fn test_get_script_includes_owner_when_present() {
    let mut view = create_view("select id from public.users");
    view.owner = "pgc_owner".to_string();
    view.hash();

    assert_eq!(
        view.get_script(),
        "create view analytics.active_users as\nselect id from public.users;\n\nalter view analytics.active_users owner to pgc_owner;\n\n"
    );
}

#[test]
fn test_get_script_includes_owner_for_materialized_view() {
    let mut view = create_materialized_view("select id from public.users");
    view.owner = "pgc_owner".to_string();
    view.hash();

    assert_eq!(
        view.get_script(),
        "create materialized view analytics.active_users as\nselect id from public.users;\n\nalter materialized view analytics.active_users owner to pgc_owner;\n\n"
    );
}

#[test]
fn test_get_drop_script_returns_drop_statement() {
    let view = create_view("select id from public.users");
    assert_eq!(
        view.get_drop_script(),
        "drop view if exists analytics.active_users;\n\n"
    );
}

#[test]
fn test_get_drop_script_returns_drop_materialized_statement() {
    let view = create_materialized_view("select id from public.users");
    assert_eq!(
        view.get_drop_script(),
        "drop materialized view if exists analytics.active_users;\n\n"
    );
}

#[test]
fn test_get_alter_script_returns_noop_when_definitions_match() {
    let view = create_view("select 1");
    let mut target = view.clone();
    target.definition = "select 1".to_string();

    assert_eq!(
        view.get_alter_script(&target, true),
        "-- View analytics.active_users requires no changes.\n"
    );
}

#[test]
fn test_get_alter_script_returns_error_for_different_identifiers() {
    let view = create_view("select 1");
    let target = View::new(
        "other".to_string(),
        "select 2".to_string(),
        "analytics".to_string(),
        vec![],
    );

    assert_eq!(
        view.get_alter_script(&target, true),
        "-- Cannot alter view analytics.active_users because target is analytics.other\n"
    );
}

#[test]
fn test_get_alter_script_respects_create_or_replace_definition() {
    let current = create_view("select 1");
    let replacement = create_view("create or replace view analytics.active_users as select 2");

    assert_eq!(
        current.get_alter_script(&replacement, true),
        "CREATE OR REPLACE VIEW analytics.active_users AS\ncreate or replace view analytics.active_users as select 2;\n\n"
    );
}

#[test]
fn test_get_alter_script_generates_replace_statement() {
    let current = create_view("select 1");
    let target = create_view("select id, active from public.users where active");

    assert_eq!(
        current.get_alter_script(&target, true),
        "CREATE OR REPLACE VIEW analytics.active_users AS\nselect id, active from public.users where active;\n\n"
    );
}

#[test]
fn test_get_alter_script_materialized_drops_and_recreates() {
    let current = create_materialized_view("select 1");
    let target = create_materialized_view("select id from public.users");

    assert_eq!(
        current.get_alter_script(&target, true),
        "drop materialized view if exists analytics.active_users;\n\ncreate materialized view analytics.active_users as\nselect id from public.users;\n\n"
    );
}

#[test]
fn test_get_alter_script_materialized_use_drop_false() {
    let current = create_materialized_view("select 1");
    let target = create_materialized_view("select id from public.users");

    let script = current.get_alter_script(&target, false);

    // Should contain a warning about manual intervention
    assert!(
        script.contains("use_drop=false") && script.contains("manual intervention needed"),
        "should contain a warning comment, script:\n{}",
        script
    );

    // Both drop and create should be commented out
    for line in script.lines() {
        if line.contains("drop materialized view") || line.contains("create materialized view") {
            assert!(line.starts_with("--"), "should be commented: {}", line);
        }
    }
}

#[test]
fn test_get_alter_script_materialized_use_drop_true_contains_active_drop() {
    let current = create_materialized_view("select 1");
    let target = create_materialized_view("select id from public.users");

    let script = current.get_alter_script(&target, true);

    // The drop line should NOT be commented
    for line in script.lines() {
        if line.contains("drop materialized view") {
            assert!(!line.starts_with("--"), "drop should be active: {}", line);
        }
    }
}

#[test]
fn test_get_alter_script_regular_view_unaffected_by_use_drop() {
    let current = create_view("select 1");
    let target = create_view("select id, active from public.users where active");

    let with_drop = current.get_alter_script(&target, true);
    let without_drop = current.get_alter_script(&target, false);

    // Regular views use CREATE OR REPLACE, no drop involved
    assert_eq!(with_drop, without_drop);
    assert!(!with_drop.contains("drop"));
}

#[test]
fn test_get_alter_script_regular_to_materialized_drops_view() {
    let current = create_view("select 1");
    let target = create_materialized_view("select 1");

    let script = current.get_alter_script(&target, true);

    // DROP must target the current kind (regular view), not the target kind
    assert!(
        script.contains("drop view if exists"),
        "should drop the regular view, script:\n{}",
        script
    );
    assert!(
        !script.contains("drop materialized view"),
        "should NOT emit DROP MATERIALIZED VIEW for a regular view"
    );
    // Then create the materialized view
    assert!(script.contains("create materialized view"));
}

#[test]
fn test_get_alter_script_materialized_to_regular_drops_materialized() {
    let current = create_materialized_view("select 1");
    let target = create_view("select 1");

    let script = current.get_alter_script(&target, true);

    // DROP must target the current kind (materialized view)
    assert!(
        script.contains("drop materialized view if exists"),
        "should drop the materialized view, script:\n{}",
        script
    );
    // Then create the regular view
    assert!(script.contains("create view"));
}

#[test]
fn test_get_alter_script_regular_to_materialized_use_drop_false() {
    let current = create_view("select 1");
    let target = create_materialized_view("select 1");

    let script = current.get_alter_script(&target, false);

    assert!(
        script.contains("use_drop=false") && script.contains("manual intervention needed"),
        "should warn about manual intervention, script:\n{}",
        script
    );

    // Both drop and create should be commented out
    for line in script.lines() {
        if line.contains("drop view") || line.contains("create materialized view") {
            assert!(line.starts_with("--"), "should be commented: {}", line);
        }
    }
}

#[test]
fn test_get_alter_script_materialized_to_regular_use_drop_false() {
    let current = create_materialized_view("select 1");
    let target = create_view("select 1");

    let script = current.get_alter_script(&target, false);

    assert!(
        script.contains("use_drop=false") && script.contains("manual intervention needed"),
        "should warn about manual intervention, script:\n{}",
        script
    );

    for line in script.lines() {
        if line.contains("drop materialized view") || line.contains("create view") {
            assert!(line.starts_with("--"), "should be commented: {}", line);
        }
    }
}

// --- OR-REPLACE compatibility (issue #227) ---

fn vcol(name: &str, ty: &str) -> ViewColumn {
    ViewColumn {
        name: name.to_string(),
        data_type: ty.to_string(),
        collation: None,
    }
}

fn view_with_columns(definition: &str, cols: Vec<ViewColumn>) -> View {
    let mut v = create_view(definition);
    v.columns = cols;
    v.hash();
    v
}

#[test]
fn or_replace_compatible_same_columns() {
    let a = view_with_columns("select id from t", vec![vcol("id", "integer")]);
    let b = view_with_columns("select id from t where id > 0", vec![vcol("id", "integer")]);
    assert!(a.or_replace_compatible(&b));
}

#[test]
fn or_replace_compatible_append_at_end() {
    let a = view_with_columns("select id from t", vec![vcol("id", "integer")]);
    let b = view_with_columns(
        "select id, name from t",
        vec![vcol("id", "integer"), vcol("name", "text")],
    );
    assert!(a.or_replace_compatible(&b));
}

#[test]
fn or_replace_incompatible_insert_in_middle() {
    // The issue #227 case: a column inserted before an existing one.
    let a = view_with_columns(
        "select id, profile_id from t",
        vec![vcol("id", "integer"), vcol("profile_id", "integer")],
    );
    let b = view_with_columns(
        "select id, kind, profile_id from t",
        vec![
            vcol("id", "integer"),
            vcol("kind", "text"),
            vcol("profile_id", "integer"),
        ],
    );
    assert!(!a.or_replace_compatible(&b));
}

#[test]
fn or_replace_incompatible_rename_reorder_retype_drop() {
    let base = view_with_columns(
        "select a, b from t",
        vec![vcol("a", "integer"), vcol("b", "text")],
    );
    // rename
    assert!(!base.or_replace_compatible(&view_with_columns(
        "select a, b as c from t",
        vec![vcol("a", "integer"), vcol("c", "text")],
    )));
    // reorder
    assert!(!base.or_replace_compatible(&view_with_columns(
        "select b, a from t",
        vec![vcol("b", "text"), vcol("a", "integer")],
    )));
    // retype (typmod counts too)
    assert!(!base.or_replace_compatible(&view_with_columns(
        "select a, b::varchar(10) as b from t",
        vec![vcol("a", "integer"), vcol("b", "character varying(10)")],
    )));
    // drop trailing column
    assert!(!base.or_replace_compatible(&view_with_columns(
        "select a from t",
        vec![vcol("a", "integer")],
    )));
}

#[test]
fn or_replace_incompatible_collation_change() {
    let a = view_with_columns(
        "select b from t",
        vec![ViewColumn {
            name: "b".to_string(),
            data_type: "text".to_string(),
            collation: Some("default".to_string()),
        }],
    );
    let b = view_with_columns(
        "select b collate \"C\" as b from t",
        vec![ViewColumn {
            name: "b".to_string(),
            data_type: "text".to_string(),
            collation: Some("C".to_string()),
        }],
    );
    assert!(!a.or_replace_compatible(&b));
}

// A dump written before column capture has no column data; incompatibility cannot be
// proven, so the check must fall back to the historical OR REPLACE behavior.
#[test]
fn or_replace_compatible_when_column_data_missing() {
    let no_cols = create_view("select a from t");
    let with_cols = view_with_columns("select b from t", vec![vcol("b", "text")]);
    assert!(no_cols.or_replace_compatible(&with_cols));
    assert!(with_cols.or_replace_compatible(&no_cols));
    assert!(no_cols.or_replace_compatible(&no_cols));
}

// get_alter_script must route an incompatible column change to drop+recreate instead
// of emitting the CREATE OR REPLACE VIEW that PostgreSQL would reject.
#[test]
fn get_alter_script_incompatible_columns_drops_and_recreates() {
    let from = view_with_columns(
        "select id, profile_id from public.users",
        vec![vcol("id", "integer"), vcol("profile_id", "integer")],
    );
    let to = view_with_columns(
        "select id, kind, profile_id from public.users",
        vec![
            vcol("id", "integer"),
            vcol("kind", "text"),
            vcol("profile_id", "integer"),
        ],
    );
    let script = from.get_alter_script(&to, true);
    assert!(
        script.contains("drop view if exists analytics.active_users;"),
        "incompatible column change must drop first: {script}"
    );
    assert!(!script.contains("CREATE OR REPLACE"));
}

#[test]
fn get_alter_script_compatible_append_uses_or_replace() {
    let from = view_with_columns("select id from public.users", vec![vcol("id", "integer")]);
    let to = view_with_columns(
        "select id, name from public.users",
        vec![vcol("id", "integer"), vcol("name", "text")],
    );
    let script = from.get_alter_script(&to, true);
    assert!(
        script.contains("CREATE OR REPLACE VIEW"),
        "appending at the end must keep OR REPLACE: {script}"
    );
    assert!(!script.to_lowercase().contains("drop view"));
}
