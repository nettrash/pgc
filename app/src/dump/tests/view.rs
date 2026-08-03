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

// ── Issue #235: indexes on a materialized view ──────────────────────────────

fn mv_index(name: &str, indexdef: &str) -> TableIndex {
    TableIndex {
        schema: "analytics".to_string(),
        table: "active_users".to_string(),
        name: name.to_string(),
        catalog: None,
        indexdef: indexdef.to_string(),
        is_partition_index: false,
        comment: None,
    }
}

fn indexed_materialized_view(indexes: Vec<TableIndex>) -> View {
    let mut view = create_materialized_view("select id from public.users");
    view.indexes = indexes;
    view.hash();
    view
}

#[test]
fn matview_get_script_recreates_its_indexes() {
    let view = indexed_materialized_view(vec![
        mv_index(
            "ix_id",
            "CREATE UNIQUE INDEX ix_id ON analytics.active_users USING btree (id)",
        ),
        mv_index(
            "ix_name",
            "CREATE INDEX ix_name ON analytics.active_users USING btree (name)",
        ),
    ]);

    let script = view.get_script();
    assert!(script.contains("create materialized view analytics.active_users"));
    assert!(
        script.contains("CREATE UNIQUE INDEX ix_id ON analytics.active_users USING btree (id);"),
        "DROP MATERIALIZED VIEW takes the indexes with it, so the CREATE must put \
         them back: {script}"
    );
    assert!(script.contains("CREATE INDEX ix_name ON analytics.active_users USING btree (name);"));
}

#[test]
fn matview_index_comment_is_emitted_with_the_index() {
    let mut index = mv_index(
        "ix_id",
        "CREATE INDEX ix_id ON analytics.active_users USING btree (id)",
    );
    index.comment = Some("lookup by id".to_string());
    let view = indexed_materialized_view(vec![index]);

    assert!(
        view.get_script()
            .contains("comment on index analytics.ix_id is 'lookup by id';")
    );
}

#[test]
fn matview_get_script_without_indexes_omits_them() {
    let view = indexed_materialized_view(vec![mv_index(
        "ix_id",
        "CREATE UNIQUE INDEX ix_id ON analytics.active_users USING btree (id)",
    )]);

    let script = view.get_script_without_indexes();
    assert!(script.contains("create materialized view analytics.active_users"));
    assert!(
        !script.contains("CREATE UNIQUE INDEX"),
        "the production path emits the indexes itself: {script}"
    );
}

#[test]
fn regular_view_script_is_unchanged_by_the_index_field() {
    // A regular view can never be indexed, so its script must be byte-identical
    // to what it was before the field existed.
    let view = create_view("select id from public.users");
    assert_eq!(view.get_script(), view.get_script_without_indexes());
}

#[test]
fn matview_indexes_do_not_affect_the_hash() {
    // Hashing the index list would turn "an index was added" into a full drop +
    // rebuild of the view's contents, and would make every materialized view
    // look changed against a dump written before the field existed.
    let plain = indexed_materialized_view(Vec::new());
    let indexed = indexed_materialized_view(vec![mv_index(
        "ix_id",
        "CREATE UNIQUE INDEX ix_id ON analytics.active_users USING btree (id)",
    )]);

    assert_eq!(plain.hash, indexed.hash);
}

#[test]
fn view_without_index_field_deserializes_from_an_older_dump() {
    let json = r#"{
        "schema": "analytics",
        "name": "active_users",
        "definition": "select id from public.users",
        "table_relation": [],
        "is_materialized": true
    }"#;
    let view: View = serde_json::from_str(json).expect("older dumps must stay readable");
    assert!(view.indexes.is_empty());
}

#[test]
fn matview_index_alter_plan_classifies_every_change() {
    let from = indexed_materialized_view(vec![
        mv_index(
            "ix_dropped",
            "CREATE INDEX ix_dropped ON analytics.active_users USING btree (amount)",
        ),
        mv_index(
            "ix_redefined",
            "CREATE INDEX ix_redefined ON analytics.active_users USING btree (name)",
        ),
        mv_index(
            "ix_recommented",
            "CREATE INDEX ix_recommented ON analytics.active_users USING btree (id)",
        ),
    ]);

    let mut recommented = mv_index(
        "ix_recommented",
        "CREATE INDEX ix_recommented ON analytics.active_users USING btree (id)",
    );
    recommented.comment = Some("after".to_string());
    let to = indexed_materialized_view(vec![
        mv_index(
            "ix_redefined",
            "CREATE INDEX ix_redefined ON analytics.active_users USING btree (name DESC)",
        ),
        recommented,
        mv_index(
            "ix_added",
            "CREATE INDEX ix_added ON analytics.active_users USING btree (active)",
        ),
    ]);

    let plan = from.index_alter_plan(&to);

    let dropped: Vec<&str> = plan.drop.iter().map(|i| i.name.as_str()).collect();
    let created: Vec<&str> = plan.create.iter().map(|i| i.name.as_str()).collect();
    let recommented: Vec<&str> = plan
        .comment_changes
        .iter()
        .map(|i| i.name.as_str())
        .collect();

    assert_eq!(dropped, vec!["ix_redefined", "ix_dropped"]);
    assert_eq!(created, vec!["ix_redefined", "ix_added"]);
    assert_eq!(recommented, vec!["ix_recommented"]);
}

#[test]
fn matview_index_alter_plan_ignores_an_unchanged_index() {
    let index = mv_index(
        "ix_id",
        "CREATE INDEX ix_id ON analytics.active_users USING btree (id)",
    );
    let from = indexed_materialized_view(vec![index.clone()]);
    let to = indexed_materialized_view(vec![index]);

    let plan = from.index_alter_plan(&to);
    assert!(plan.drop.is_empty());
    assert!(plan.create.is_empty());
    assert!(plan.comment_changes.is_empty());
}
