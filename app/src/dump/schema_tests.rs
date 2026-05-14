use super::*;

#[test]
fn test_schema_new_sets_fields() {
    let name = "public".to_string();
    let schema = Schema::new(name.clone(), name.clone(), None);

    assert_eq!(schema.name, name);
    assert_eq!(schema.raw_name, name);
    assert_eq!(schema.owner, "");
    assert!(schema.hash.is_some());

    let mut hasher = md5::Context::new();
    hasher.consume(name.as_bytes());
    hasher.consume(name.as_bytes());

    assert_eq!(schema.hash.unwrap(), format!("{:x}", hasher.compute()));
}

#[test]
fn test_get_script_returns_create_statement() {
    let name: String = String::from("analytics");
    let raw_name: String = name.clone();

    let schema = Schema::new(name, raw_name, Some("reporting".to_string()));

    assert_eq!(
        schema.get_script(),
        "create schema if not exists analytics;\n\ncomment on schema analytics is 'reporting';\n\n"
    );
}

#[test]
fn test_get_script_includes_owner_when_present() {
    let name: String = String::from("analytics");
    let raw_name: String = name.clone();

    let mut schema = Schema::new(name, raw_name, None);
    schema.owner = "pgc_owner".to_string();
    schema.hash();

    assert_eq!(
        schema.get_script(),
        "create schema if not exists analytics;\n\nalter schema analytics owner to pgc_owner;\n\n"
    );
}

#[test]
fn test_get_drop_script_returns_drop_statement() {
    let name: String = String::from("archive");
    let raw_name: String = name.clone();

    let schema = Schema::new(name, raw_name, None);

    assert_eq!(
        schema.get_drop_script(),
        "drop schema if exists archive;\n\n"
    );
}

#[test]
fn test_get_script_quoted_name_no_comment() {
    let schema = Schema::new("\"my-schema\"".to_string(), "my-schema".to_string(), None);

    assert_eq!(
        schema.get_script(),
        "create schema if not exists \"my-schema\";\n\n"
    );
}

#[test]
fn test_get_script_quoted_name_with_comment() {
    let schema = Schema::new(
        "\"my-schema\"".to_string(),
        "my-schema".to_string(),
        Some("my comment".to_string()),
    );

    assert_eq!(
        schema.get_script(),
        "create schema if not exists \"my-schema\";\n\ncomment on schema \"my-schema\" is 'my comment';\n\n"
    );
}

#[test]
fn test_get_owner_script_quoted_name() {
    let mut schema = Schema::new("\"my-schema\"".to_string(), "my-schema".to_string(), None);
    schema.owner = "pgc_owner".to_string();

    assert_eq!(
        schema.get_owner_script(),
        "alter schema \"my-schema\" owner to pgc_owner;\n\n"
    );
}

#[test]
fn test_get_alter_script_quoted_name_comment_changed() {
    let source = Schema::new(
        "\"my-schema\"".to_string(),
        "my-schema".to_string(),
        Some("old comment".to_string()),
    );
    let target = Schema::new(
        "\"my-schema\"".to_string(),
        "my-schema".to_string(),
        Some("new comment".to_string()),
    );

    assert_eq!(
        source.get_alter_script(&target),
        "comment on schema \"my-schema\" is 'new comment';\n\n"
    );
}

#[test]
fn test_get_alter_script_quoted_name_comment_removed() {
    let source = Schema::new(
        "\"my-schema\"".to_string(),
        "my-schema".to_string(),
        Some("old comment".to_string()),
    );
    let target = Schema::new("\"my-schema\"".to_string(), "my-schema".to_string(), None);

    assert_eq!(
        source.get_alter_script(&target),
        "comment on schema \"my-schema\" is null;\n\n"
    );
}

#[test]
fn test_get_alter_script_quoted_name_owner_changed() {
    let source = Schema::new("\"my-schema\"".to_string(), "my-schema".to_string(), None);
    let mut target = Schema::new("\"my-schema\"".to_string(), "my-schema".to_string(), None);
    target.owner = "new_owner".to_string();

    assert_eq!(
        source.get_alter_script(&target),
        "alter schema \"my-schema\" owner to new_owner;\n\n"
    );
}

#[test]
fn test_get_drop_script_quoted_name() {
    let schema = Schema::new("\"my-schema\"".to_string(), "my-schema".to_string(), None);

    assert_eq!(
        schema.get_drop_script(),
        "drop schema if exists \"my-schema\";\n\n"
    );
}
