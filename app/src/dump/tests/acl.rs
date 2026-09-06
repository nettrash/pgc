use super::*;

#[test]
fn test_parse_acl_entry() {
    let entry = AclEntry::parse("myuser=arw/owner").unwrap();
    assert_eq!(entry.grantee, "myuser");
    assert_eq!(entry.privileges, "arw");
    assert_eq!(entry.grantor, "owner");
}

#[test]
fn test_parse_public_grantee() {
    let entry = AclEntry::parse("=r/owner").unwrap();
    assert_eq!(entry.grantee, "");
    assert_eq!(entry.privileges, "r");
    assert_eq!(entry.grantor, "owner");
}

#[test]
fn test_parse_invalid() {
    assert!(AclEntry::parse("invalid").is_none());
    assert!(AclEntry::parse("=/owner").is_none());
}

#[test]
fn test_grant_script() {
    let script = AclEntry::get_grant_script("myuser=rw/owner", "TABLE", "public.my_table");
    assert_eq!(
        script,
        "GRANT SELECT, UPDATE ON TABLE public.my_table TO myuser;\n\n"
    );
}

#[test]
fn test_grant_script_with_grant_option() {
    let script = AclEntry::get_grant_script("myuser=r*w/owner", "TABLE", "public.my_table");
    assert_eq!(
        script,
        "GRANT UPDATE ON TABLE public.my_table TO myuser;\n\nGRANT SELECT ON TABLE public.my_table TO myuser WITH GRANT OPTION;\n\n"
    );
}

#[test]
fn test_grant_script_all_grant_option() {
    let script = AclEntry::get_grant_script("myuser=r*w*/owner", "TABLE", "public.my_table");
    assert_eq!(
        script,
        "GRANT SELECT, UPDATE ON TABLE public.my_table TO myuser WITH GRANT OPTION;\n\n"
    );
}

#[test]
fn test_revoke_script() {
    let script = AclEntry::get_revoke_script("myuser=r/owner", "TABLE", "public.my_table");
    assert_eq!(
        script,
        "REVOKE SELECT ON TABLE public.my_table FROM myuser;\n\n"
    );
}

#[test]
fn test_revoke_script_with_grant_option() {
    let script = AclEntry::get_revoke_script("myuser=r*w/owner", "TABLE", "public.my_table");
    assert_eq!(
        script,
        "REVOKE GRANT OPTION FOR SELECT ON TABLE public.my_table FROM myuser;\n\nREVOKE SELECT, UPDATE ON TABLE public.my_table FROM myuser;\n\n"
    );
}

#[test]
fn test_diff_acls_grant_option_upgrade() {
    // FROM: plain SELECT; TO: SELECT with grant option → upgrade, no revoke
    let from = vec!["user1=r/owner".to_string()];
    let to = vec!["user1=r*/owner".to_string()];
    let diffs = diff_acls(&from, &to, true, "TABLE", &[], &[]);
    assert_eq!(diffs.len(), 1);
    assert_eq!(diffs[0].grantee, "user1");
    assert_eq!(diffs[0].grants_with_option, vec!["SELECT"]);
    assert!(diffs[0].grants_plain.is_empty());
    assert!(diffs[0].revoke_option_for.is_empty());
    assert!(diffs[0].revokes.is_empty());
}

#[test]
fn test_diff_acls_grant_option_downgrade_full() {
    // FROM: SELECT with grant option; TO: plain SELECT → revoke grant option
    let from = vec!["user1=r*/owner".to_string()];
    let to = vec!["user1=r/owner".to_string()];
    let diffs = diff_acls(&from, &to, true, "TABLE", &[], &[]);
    assert_eq!(diffs.len(), 1);
    assert_eq!(diffs[0].revoke_option_for, vec!["SELECT"]);
    assert!(diffs[0].revokes.is_empty());
}

#[test]
fn test_diff_acls_addonly() {
    let from = vec!["user1=r/owner".to_string()];
    let to = vec!["user1=r/owner".to_string(), "user2=rw/owner".to_string()];
    let diffs = diff_acls(&from, &to, false, "TABLE", &[], &[]);
    assert_eq!(diffs.len(), 1);
    assert_eq!(diffs[0].grantee, "user2");
    assert_eq!(diffs[0].grants_plain, vec!["SELECT", "UPDATE"]);
}

#[test]
fn test_diff_acls_full() {
    let from = vec!["user1=r/owner".to_string(), "user3=d/owner".to_string()];
    let to = vec!["user1=r/owner".to_string(), "user2=rw/owner".to_string()];
    let diffs = diff_acls(&from, &to, true, "TABLE", &[], &[]);
    assert_eq!(diffs.len(), 2);
    let user2 = diffs.iter().find(|d| d.grantee == "user2").unwrap();
    assert_eq!(user2.grants_plain, vec!["SELECT", "UPDATE"]);
    let user3 = diffs.iter().find(|d| d.grantee == "user3").unwrap();
    assert_eq!(user3.revokes, vec!["DELETE"]);
}

#[test]
fn test_diff_acls_ignores_grantor() {
    // Same grantee and privileges, different grantor → no diff
    let from = vec!["user1=rw/owner1".to_string()];
    let to = vec!["user1=rw/owner2".to_string()];
    let diffs = diff_acls(&from, &to, true, "TABLE", &[], &[]);
    assert!(diffs.is_empty());
}

#[test]
fn test_diff_acls_merges_multiple_grantors() {
    // Same grantee, privileges split across grantors → merged, no diff
    let from = vec!["user1=r/owner1".to_string(), "user1=w/owner2".to_string()];
    let to = vec!["user1=rw/owner3".to_string()];
    let diffs = diff_acls(&from, &to, true, "TABLE", &[], &[]);
    assert!(diffs.is_empty());
}

#[test]
fn test_parse_privileges_mixed() {
    let items = AclEntry::parse_privileges("r*wadD", "TABLE");
    assert_eq!(items.len(), 5);
    assert_eq!(items[0].name, "SELECT");
    assert!(items[0].grant_option);
    assert_eq!(items[1].name, "UPDATE");
    assert!(!items[1].grant_option);
    assert_eq!(items[2].name, "INSERT");
    assert!(!items[2].grant_option);
}

#[test]
fn test_public_grant_with_grant_option() {
    let script = AclEntry::get_grant_script("=r*/owner", "TABLE", "public.t");
    assert_eq!(
        script,
        "GRANT SELECT ON TABLE public.t TO PUBLIC WITH GRANT OPTION;\n\n"
    );
}

#[test]
fn test_diff_acls_excludes_owners() {
    // Current owner entries are skipped as implicit owner privileges.
    let from = vec![
        "owner_b=arwdDxt/owner_b".to_string(),
        "reader=r/owner_b".to_string(),
    ];
    let to = vec![
        "owner_b=arwdDxt/owner_b".to_string(),
        "reader=r/owner_b".to_string(),
    ];
    let diffs = diff_acls(&from, &to, true, "TABLE", &["owner_b"], &["owner_b"]);
    assert!(
        diffs.is_empty(),
        "Owner grantees must be excluded, got: {diffs:?}"
    );
}

#[test]
fn test_diff_acls_grants_to_former_owner_when_explicit_in_to() {
    // Realistic owner-change scenario: FROM's relacl carries owner_a's
    // implicit-owner entry (PG materialises it once any GRANT exists).
    // After ALTER OWNER TO owner_b, that entry vanishes — so
    // `from_owners` filters owner_a out of from_acl, modelling the
    // post-migration FROM. TO has an explicit grant to owner_a as a
    // regular grantee, which must show up in the diff.
    let from = vec![
        "owner_a=arwdDxt/owner_a".to_string(),
        "reader=r/owner_a".to_string(),
    ];
    let to = vec![
        "owner_b=arwdDxt/owner_b".to_string(),
        "owner_a=ar/owner_b".to_string(),
        "reader=r/owner_b".to_string(),
    ];
    let diffs = diff_acls(&from, &to, true, "TABLE", &["owner_a"], &["owner_b"]);
    assert_eq!(diffs.len(), 1);
    assert_eq!(diffs[0].grantee, "owner_a");
    assert_eq!(diffs[0].grants_plain, vec!["INSERT", "SELECT"]);
    assert!(diffs[0].grants_with_option.is_empty());
    assert!(diffs[0].revoke_option_for.is_empty());
    assert!(diffs[0].revokes.is_empty());
}

/// Regression for the user-reported non-idempotent diff: under the
/// previous symmetric `to_owners` filter, a former owner with no
/// explicit grant in TO would compare its implicit-owner privileges
/// in FROM against an empty set in TO and emit a long REVOKE list;
/// after applying ALTER OWNER + that REVOKE, the former owner has
/// nothing in FROM and the next compare run keeps oscillating. With
/// asymmetric `from_owners` / `to_owners` filters that model
/// `ALTER OWNER`'s effect, the diff is empty when both ACLs already
/// agree on explicit grants.
#[test]
fn test_diff_acls_owner_change_without_explicit_grant_to_former_owner_is_idempotent() {
    let from = vec![
        "owner_a=arwdDxt/owner_a".to_string(),
        "reader=r/owner_a".to_string(),
    ];
    let to = vec![
        "owner_b=arwdDxt/owner_b".to_string(),
        "reader=r/owner_b".to_string(),
    ];
    let diffs = diff_acls(&from, &to, true, "TABLE", &["owner_a"], &["owner_b"]);
    assert!(
        diffs.is_empty(),
        "Owner-change with no explicit grant to former owner must produce an empty diff (ALTER OWNER alone is enough), got: {diffs:?}"
    );
}

#[test]
fn test_diff_acls_excludes_owner_keeps_others() {
    let from = vec![
        "theowner=arwdDxt/theowner".to_string(),
        "old_reader=r/theowner".to_string(),
    ];
    let to = vec![
        "theowner=arwdDxt/theowner".to_string(),
        "new_reader=r/theowner".to_string(),
    ];
    let diffs = diff_acls(&from, &to, true, "TABLE", &["theowner"], &["theowner"]);
    assert_eq!(diffs.len(), 2);
    let added = diffs.iter().find(|d| d.grantee == "new_reader").unwrap();
    assert_eq!(added.grants_plain, vec!["SELECT"]);
    let removed = diffs.iter().find(|d| d.grantee == "old_reader").unwrap();
    assert_eq!(removed.revokes, vec!["SELECT"]);
}

#[test]
fn test_parse_quoted_grantee_with_equals() {
    // PostgreSQL escapes role names containing `=` by quoting; the parser
    // must split on the unquoted `=`, not the one inside the quoted name.
    let entry = AclEntry::parse("\"weird=name\"=r/owner").unwrap();
    assert_eq!(entry.grantee, "\"weird=name\"");
    assert_eq!(entry.privileges, "r");
    assert_eq!(entry.grantor, "owner");
}

#[test]
fn test_parse_quoted_grantee_with_slash() {
    // Same idea for `/` — it can appear inside a quoted role name and
    // must not be mistaken for the grantor separator.
    let entry = AclEntry::parse("\"weird/name\"=r/owner").unwrap();
    assert_eq!(entry.grantee, "\"weird/name\"");
    assert_eq!(entry.privileges, "r");
    assert_eq!(entry.grantor, "owner");
}

#[test]
fn test_parse_quoted_grantor_with_separators() {
    let entry = AclEntry::parse("user=r/\"weird=grantor\"").unwrap();
    assert_eq!(entry.grantee, "user");
    assert_eq!(entry.privileges, "r");
    assert_eq!(entry.grantor, "\"weird=grantor\"");
}

#[test]
fn test_parse_quoted_grantee_with_escaped_quote() {
    // `""` inside a quoted region is an escaped literal `"`; it must not
    // close the quoted region prematurely.
    let entry = AclEntry::parse("\"qu\"\"ote\"=r/owner").unwrap();
    assert_eq!(entry.grantee, "\"qu\"\"ote\"");
    assert_eq!(entry.privileges, "r");
    assert_eq!(entry.grantor, "owner");
}

#[test]
fn test_diff_acls_owner_unchanged_skips_explicit_owner_grant() {
    // Owner unchanged. TO has an explicit entry for the owner role
    // (which PG sometimes emits) — it must be filtered as implicit.
    let from = vec!["reader=r/owner_a".to_string()];
    let to = vec![
        "owner_a=arwdDxt/owner_a".to_string(),
        "reader=r/owner_a".to_string(),
    ];
    let diffs = diff_acls(&from, &to, true, "TABLE", &["owner_a"], &["owner_a"]);
    assert!(
        diffs.is_empty(),
        "Owner-as-grantee entry must be filtered when owner is unchanged, got: {diffs:?}"
    );
}

#[test]
fn test_foreign_table_uses_full_table_privileges() {
    // FOREIGN TABLE shares the same privilege set as TABLE, including
    // MAINTAIN (PG17+). Before v1.0.18 it fell through to the empty
    // default set and silently dropped privileges from the diff.
    let from = vec![];
    let to = vec!["reader=rwm/owner".to_string()];
    let diffs = diff_acls(&from, &to, false, "FOREIGN TABLE", &["owner"], &["owner"]);
    assert_eq!(diffs.len(), 1);
    let mut grants = diffs[0].grants_plain.clone();
    grants.sort();
    assert_eq!(grants, vec!["MAINTAIN", "SELECT", "UPDATE"]);
}

#[test]
fn test_sequence_drops_maintain_privilege() {
    // MAINTAIN ('m') is not a valid privilege on sequences in PG17+; the
    // parser must drop it silently rather than emit an invalid GRANT.
    let from = vec![];
    let to = vec!["reader=Urm/owner".to_string()];
    let diffs = diff_acls(&from, &to, false, "SEQUENCE", &["owner"], &["owner"]);
    assert_eq!(diffs.len(), 1);
    let mut grants = diffs[0].grants_plain.clone();
    grants.sort();
    assert_eq!(grants, vec!["SELECT", "USAGE"]);
}

#[test]
fn test_generate_grants_script_excludes_owner() {
    let from = vec!["owner_b=X/owner_b".to_string()];
    let to = vec!["owner_b=X/owner_b".to_string(), "app=X/owner_b".to_string()];
    let script = generate_grants_script(
        &from,
        &to,
        true,
        "FUNCTION",
        "public.my_func()",
        &["owner_b"],
        &["owner_b"],
    );
    assert!(
        script.contains("GRANT EXECUTE ON FUNCTION public.my_func() TO app;"),
        "Must grant to non-owner, got: {script}"
    );
    assert!(
        !script.contains("owner_b"),
        "Must not reference owner, got: {script}"
    );
}
