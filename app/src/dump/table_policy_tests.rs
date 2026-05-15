use super::*;
use sha2::Sha256;

fn sample_policy() -> TablePolicy {
    TablePolicy {
        schema: "public".to_string(),
        table: "users".to_string(),
        name: "p_users_select".to_string(),
        command: "select".to_string(),
        permissive: true,
        roles: vec!["analyst".to_string(), "auditor".to_string()],
        using_clause: Some("(tenant_id = current_setting('app.current_tenant')::int)".to_string()),
        check_clause: None,
    }
}

#[test]
fn test_get_script() {
    let script = sample_policy().get_script();
    assert!(script.contains("create policy p_users_select"));
    assert!(script.contains("for select"));
    assert!(script.contains("to \"analyst\", \"auditor\""));
    assert!(script.contains("using (tenant_id = current_setting"));
    assert!(script.ends_with(";\n\n"));
}

#[test]
fn test_get_script_restrictive_public_and_wrapped_clauses() {
    let policy = TablePolicy {
        schema: "public".to_string(),
        table: "docs".to_string(),
        name: "p_docs_update".to_string(),
        command: "update".to_string(),
        permissive: false,
        roles: Vec::new(),
        using_clause: Some("(owner_id = current_user_id())".to_string()),
        check_clause: Some("(owner_id = current_user_id())".to_string()),
    };

    let script = policy.get_script();

    assert!(script.contains("as restrictive"));
    assert!(script.contains("to public"));
    assert!(script.contains(" using (owner_id = current_user_id())"));
    assert!(script.contains(" with check (owner_id = current_user_id())"));
    assert!(script.ends_with(";\n\n"));
}

#[test]
fn test_get_script_escapes_role_quotes() {
    let policy = TablePolicy {
        schema: "app".to_string(),
        table: "projects".to_string(),
        name: "p_projects_select".to_string(),
        command: "select".to_string(),
        permissive: true,
        roles: vec!["ro\"le".to_string(), "viewer".to_string()],
        using_clause: None,
        check_clause: None,
    };

    let script = policy.get_script();

    assert!(script.contains("to \"ro\"\"le\", \"viewer\""));
}

#[test]
fn test_from_parts_maps_command_and_sorts_roles() {
    let policy = TablePolicy::from_parts(
        "s".to_string(),
        "t".to_string(),
        "p".to_string(),
        "w",
        false,
        vec!["b".to_string(), "a".to_string()],
        Some("x > 1".to_string()),
        Some("y < 5".to_string()),
    );

    assert_eq!(policy.command, "update");
    assert_eq!(policy.roles, vec!["a", "b"]);
    assert!(!policy.permissive);
    assert_eq!(policy.using_clause.as_deref(), Some("x > 1"));
    assert_eq!(policy.check_clause.as_deref(), Some("y < 5"));
}

#[test]
fn test_add_to_hasher_changes() {
    let base = sample_policy();
    let mut altered = base.clone();
    altered.command = "update".to_string();

    let mut hasher_base = Sha256::new();
    base.add_to_hasher(&mut hasher_base);
    let hash_base = format!("{:x}", hasher_base.finalize());

    let mut hasher_alt = Sha256::new();
    altered.add_to_hasher(&mut hasher_alt);
    let hash_alt = format!("{:x}", hasher_alt.finalize());

    assert_ne!(hash_base, hash_alt);
}
