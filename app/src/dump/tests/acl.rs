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

// --- Grantor-qualified revokes (`generate_grants_scripts`) ---

fn acl(items: &[&str]) -> Vec<String> {
    items.iter().map(|s| s.to_string()).collect()
}

const TABLE_T: AclTarget<'static> = AclTarget::Object {
    kind: "TABLE",
    name: "app.t",
};

fn grantors(style: RevokeStyle) -> GrantorRevokes<'static> {
    GrantorRevokes {
        owners: &["owner"],
        style,
        parent_acl: &[],
        parent_revokes: &[],
        replay_guard: false,
    }
}

fn table_scripts(from: &[&str], to: &[&str], style: RevokeStyle) -> GrantsScripts {
    generate_grants_scripts(
        &acl(from),
        &acl(to),
        true,
        TABLE_T,
        &["owner"],
        &["owner"],
        Some(grantors(style)),
    )
}

#[test]
fn test_grantor_revoke_granted_by_style() {
    let scripts = table_scripts(
        &["owner=arwd/owner", "delegate=r*/owner", "reader=r/delegate"],
        &["owner=arwd/owner", "delegate=r*/owner"],
        RevokeStyle::GrantedBy,
    );
    assert_eq!(
        scripts.grantor_revokes,
        "REVOKE SELECT ON TABLE app.t FROM reader GRANTED BY delegate;\n\n"
    );
    assert_eq!(scripts.owner_level, "");
}

#[test]
fn test_grantor_revoke_set_role_style() {
    let scripts = table_scripts(
        &["delegate=r*/owner", "reader=r/delegate"],
        &["delegate=r*/owner"],
        RevokeStyle::SetRole,
    );
    assert_eq!(
        scripts.grantor_revokes,
        "SET ROLE delegate;\n\nREVOKE SELECT ON TABLE app.t FROM reader;\n\nRESET ROLE;\n\n"
    );
    assert_eq!(scripts.owner_level, "");
}

#[test]
fn test_grantor_revoke_owner_granted_entries_are_byte_identical() {
    // Every entry granted by the owner: the plain script, whatever the style.
    let from = acl(&["owner=arwd/owner", "reader=r*w/owner", "writer=a/owner"]);
    let to = acl(&["owner=arwd/owner", "reader=r/owner", "other=d/owner"]);
    let plain = generate_grants_script(&from, &to, true, "TABLE", "app.t", &["owner"], &["owner"]);
    assert!(!plain.is_empty());
    for style in [RevokeStyle::GrantedBy, RevokeStyle::SetRole] {
        let scripts = generate_grants_scripts(
            &from,
            &to,
            true,
            TABLE_T,
            &["owner"],
            &["owner"],
            Some(grantors(style)),
        );
        assert_eq!(scripts.grantor_revokes, "");
        assert_eq!(scripts.owner_level, plain);
    }
}

#[test]
fn test_grantor_revoke_none_or_addonly_keeps_the_plain_script() {
    let from = acl(&["delegate=r*/owner", "reader=r/delegate"]);
    let to = acl(&["delegate=r*/owner"]);
    let plain = generate_grants_script(&from, &to, true, "TABLE", "app.t", &["owner"], &["owner"]);
    // No grantor information: the pre-1.1 output (a plain, ineffective REVOKE).
    let scripts = generate_grants_scripts(&from, &to, true, TABLE_T, &["owner"], &["owner"], None);
    assert_eq!(scripts.grantor_revokes, "");
    assert_eq!(scripts.owner_level, plain);
    assert_eq!(
        plain, "REVOKE SELECT ON TABLE app.t FROM reader;\n\n",
        "pins the pre-fix output the grantor-aware path replaces"
    );
    // addonly never revokes.
    let scripts = generate_grants_scripts(
        &from,
        &to,
        false,
        TABLE_T,
        &["owner"],
        &["owner"],
        Some(grantors(RevokeStyle::GrantedBy)),
    );
    assert_eq!(scripts, GrantsScripts::default());
}

#[test]
fn test_grantor_revoke_unknown_owner_keeps_the_plain_script() {
    let from = acl(&["delegate=r*/owner", "reader=r/delegate"]);
    let to = acl(&["delegate=r*/owner"]);
    let scripts = generate_grants_scripts(
        &from,
        &to,
        true,
        TABLE_T,
        &[],
        &[],
        Some(GrantorRevokes {
            owners: &["", ""],
            style: RevokeStyle::GrantedBy,
            parent_acl: &[],
            parent_revokes: &[],
            replay_guard: false,
        }),
    );
    assert_eq!(scripts.grantor_revokes, "");
    assert_eq!(
        scripts.owner_level,
        generate_grants_script(&from, &to, true, "TABLE", "app.t", &[], &[])
    );
}

#[test]
fn test_grantor_only_difference_emits_nothing() {
    // TO still wants reader's SELECT: the grantor is irrelevant.
    let scripts = table_scripts(
        &["delegate=r*/owner", "reader=r/delegate"],
        &["delegate=r*/owner", "reader=r/owner"],
        RevokeStyle::GrantedBy,
    );
    assert_eq!(scripts, GrantsScripts::default());
}

#[test]
fn test_grant_option_downgrade_revokes_dependents_first_and_regrants() {
    // TO takes the delegate's grant option away but keeps reader's SELECT:
    // the dependent entry must go before REVOKE GRANT OPTION FOR (which
    // would fail with "dependent privileges exist"), and SELECT comes back
    // from the owner.
    let scripts = table_scripts(
        &["delegate=r*/owner", "reader=r/delegate"],
        &["delegate=r/owner", "reader=r/owner"],
        RevokeStyle::GrantedBy,
    );
    assert_eq!(
        scripts.grantor_revokes,
        "REVOKE SELECT ON TABLE app.t FROM reader GRANTED BY delegate;\n\n"
    );
    assert_eq!(
        scripts.owner_level,
        "REVOKE GRANT OPTION FOR SELECT ON TABLE app.t FROM delegate;\n\n\
         GRANT SELECT ON TABLE app.t TO reader;\n\n"
    );
}

#[test]
fn test_three_level_chain_is_revoked_leaf_first() {
    let scripts = table_scripts(
        &["delegate=r*/owner", "sub=r*/delegate", "reader=r/sub"],
        &["delegate=r/owner", "reader=r/owner"],
        RevokeStyle::SetRole,
    );
    assert_eq!(
        scripts.grantor_revokes,
        "SET ROLE sub;\n\nREVOKE SELECT ON TABLE app.t FROM reader;\n\nRESET ROLE;\n\n\
         SET ROLE delegate;\n\nREVOKE SELECT ON TABLE app.t FROM sub;\n\nRESET ROLE;\n\n"
    );
    assert_eq!(
        scripts.owner_level,
        "REVOKE GRANT OPTION FOR SELECT ON TABLE app.t FROM delegate;\n\n\
         GRANT SELECT ON TABLE app.t TO reader;\n\n"
    );
}

#[test]
fn test_delegate_losing_its_privilege_takes_its_grants_along() {
    // TO drops the delegate entirely and keeps reader (granted by the delegate).
    let scripts = table_scripts(
        &["delegate=r*w*/owner", "reader=rw/delegate"],
        &["reader=r/owner"],
        RevokeStyle::GrantedBy,
    );
    assert_eq!(
        scripts.grantor_revokes,
        "REVOKE SELECT, UPDATE ON TABLE app.t FROM reader GRANTED BY delegate;\n\n"
    );
    assert_eq!(
        scripts.owner_level,
        "REVOKE GRANT OPTION FOR SELECT, UPDATE ON TABLE app.t FROM delegate;\n\n\
         REVOKE SELECT, UPDATE ON TABLE app.t FROM delegate;\n\n\
         GRANT SELECT ON TABLE app.t TO reader;\n\n"
    );
}

#[test]
fn test_delegate_keeping_the_option_from_another_grantor_keeps_dependents() {
    // The delegate loses the owner's grant option but still holds one from
    // `other`: PostgreSQL keeps reader's entry, and so does pgc.
    let scripts = table_scripts(
        &[
            "other=r*/owner",
            "delegate=r*/owner",
            "delegate=r*/other",
            "reader=r/delegate",
        ],
        &["other=r*/owner", "delegate=r*/owner", "reader=r/owner"],
        RevokeStyle::GrantedBy,
    );
    assert_eq!(scripts, GrantsScripts::default());
}

#[test]
fn test_grant_option_downgrade_of_a_delegated_entry() {
    let scripts = table_scripts(
        &["delegate=r*/owner", "reader=r*/delegate"],
        &["delegate=r*/owner", "reader=r/owner"],
        RevokeStyle::GrantedBy,
    );
    assert_eq!(
        scripts.grantor_revokes,
        "REVOKE GRANT OPTION FOR SELECT ON TABLE app.t FROM reader GRANTED BY delegate;\n\n"
    );
    assert_eq!(scripts.owner_level, "");
}

#[test]
fn test_grantor_revoke_mixed_owner_and_delegate_entries() {
    // reader holds SELECT from the owner and from the delegate: both go, the
    // delegate's by name, the owner's with the plain REVOKE.
    let scripts = table_scripts(
        &["delegate=r*/owner", "reader=r/owner", "reader=r/delegate"],
        &["delegate=r*/owner"],
        RevokeStyle::GrantedBy,
    );
    assert_eq!(
        scripts.grantor_revokes,
        "REVOKE SELECT ON TABLE app.t FROM reader GRANTED BY delegate;\n\n"
    );
    assert_eq!(
        scripts.owner_level,
        "REVOKE SELECT ON TABLE app.t FROM reader;\n\n"
    );
}

#[test]
fn test_grantor_revoke_public_grantee() {
    let scripts = table_scripts(
        &["delegate=r*/owner", "=r/delegate"],
        &["delegate=r*/owner"],
        RevokeStyle::GrantedBy,
    );
    assert_eq!(
        scripts.grantor_revokes,
        "REVOKE SELECT ON TABLE app.t FROM PUBLIC GRANTED BY delegate;\n\n"
    );
}

#[test]
fn test_grantor_revoke_after_owner_change_is_plain() {
    // ALTER ... OWNER TO rewrites the old owner to the new one in every
    // aclitem, so entries either owner granted take the plain REVOKE.
    let scripts = generate_grants_scripts(
        &acl(&["reader=r/owner_a", "writer=w/owner_b"]),
        &acl(&[]),
        true,
        TABLE_T,
        &["owner_a"],
        &["owner_b"],
        Some(GrantorRevokes {
            owners: &["owner_a", "owner_b"],
            style: RevokeStyle::GrantedBy,
            parent_acl: &[],
            parent_revokes: &[],
            replay_guard: false,
        }),
    );
    assert_eq!(scripts.grantor_revokes, "");
    assert_eq!(
        scripts.owner_level,
        "REVOKE SELECT ON TABLE app.t FROM reader;\n\nREVOKE UPDATE ON TABLE app.t FROM writer;\n\n"
    );
}

#[test]
fn test_grantor_revoke_matches_quoted_owner() {
    // The dump spells owners with quote_ident; aclitem text leaves a
    // mixed-case name bare. They are the same role.
    let scripts = generate_grants_scripts(
        &acl(&["reader=r/Owner"]),
        &acl(&[]),
        true,
        TABLE_T,
        &["\"Owner\""],
        &["\"Owner\""],
        Some(GrantorRevokes {
            owners: &["\"Owner\"", "\"Owner\""],
            style: RevokeStyle::GrantedBy,
            parent_acl: &[],
            parent_revokes: &[],
            replay_guard: false,
        }),
    );
    assert_eq!(scripts.grantor_revokes, "");
    assert_eq!(
        scripts.owner_level,
        "REVOKE SELECT ON TABLE app.t FROM reader;\n\n"
    );
}

#[test]
fn test_grantor_revoke_quotes_role_names() {
    let scripts = table_scripts(
        &[
            "Delegate=r*/owner",
            "\"odd role\"=r*/owner",
            "user=r*/owner",
            "MixedReader=r/Delegate",
            "reader=r/\"odd role\"",
            "writer=r/user",
        ],
        &[
            "Delegate=r*/owner",
            "\"odd role\"=r*/owner",
            "user=r*/owner",
        ],
        RevokeStyle::GrantedBy,
    );
    // Ordered by the grantor's aclitem spelling within a chain depth.
    assert_eq!(
        scripts.grantor_revokes,
        "REVOKE SELECT ON TABLE app.t FROM reader GRANTED BY \"odd role\";\n\n\
         REVOKE SELECT ON TABLE app.t FROM \"MixedReader\" GRANTED BY \"Delegate\";\n\n\
         REVOKE SELECT ON TABLE app.t FROM writer GRANTED BY \"user\";\n\n"
    );
}

#[test]
fn test_quote_acl_role() {
    assert_eq!(quote_acl_role(""), "PUBLIC");
    assert_eq!(quote_acl_role("reader_1"), "reader_1");
    assert_eq!(quote_acl_role("Reader"), "\"Reader\"");
    assert_eq!(quote_acl_role("1st"), "\"1st\"");
    assert_eq!(quote_acl_role("select"), "\"select\"");
    assert_eq!(quote_acl_role("\"odd role\""), "\"odd role\"");
    assert_eq!(quote_acl_role("\"say \"\"hi\"\"\""), "\"say \"\"hi\"\"\"");
}

#[test]
fn test_grantor_revoke_column_uses_parent_table_grant_option() {
    // The delegate's grant option is table-level; the column grant it made
    // is revoked in its name.
    let parent = acl(&["owner=arwd/owner", "delegate=r*/owner"]);
    let scripts = generate_grants_scripts(
        &acl(&["reader=r/delegate"]),
        &acl(&[]),
        true,
        AclTarget::Column {
            table: "app.t",
            column: "note",
        },
        &[],
        &["owner"],
        Some(GrantorRevokes {
            owners: &["owner", "owner"],
            style: RevokeStyle::GrantedBy,
            parent_acl: &parent,
            parent_revokes: &[],
            replay_guard: false,
        }),
    );
    assert_eq!(
        scripts.grantor_revokes,
        "REVOKE SELECT (note) ON TABLE app.t FROM reader GRANTED BY delegate;\n\n"
    );
    assert_eq!(scripts.owner_level, "");
}

#[test]
fn test_grantor_revoke_column_set_role_style() {
    let scripts = generate_grants_scripts(
        &acl(&["delegate=r*/owner", "reader=r/delegate"]),
        &acl(&["delegate=r*/owner"]),
        true,
        AclTarget::Column {
            table: "app.t",
            column: "note",
        },
        &[],
        &["owner"],
        Some(GrantorRevokes {
            owners: &["owner", "owner"],
            style: RevokeStyle::SetRole,
            parent_acl: &[],
            parent_revokes: &[],
            replay_guard: false,
        }),
    );
    assert_eq!(
        scripts.grantor_revokes,
        "SET ROLE delegate;\n\nREVOKE SELECT (note) ON TABLE app.t FROM reader;\n\nRESET ROLE;\n\n"
    );
}

#[test]
fn test_grantor_revoke_orphaned_entry_is_left_to_the_plain_script() {
    // The delegate no longer holds any grant option (PostgreSQL leaves such a
    // column entry behind when the table-level option is revoked): no
    // grantor-qualified revoke can work, so the old output stands.
    let from = acl(&["reader=r/delegate"]);
    let scripts = generate_grants_scripts(
        &from,
        &acl(&[]),
        true,
        AclTarget::Column {
            table: "app.t",
            column: "note",
        },
        &[],
        &["owner"],
        Some(GrantorRevokes {
            owners: &["owner", "owner"],
            style: RevokeStyle::GrantedBy,
            parent_acl: &acl(&["delegate=r/owner"]),
            parent_revokes: &[],
            replay_guard: false,
        }),
    );
    assert_eq!(scripts.grantor_revokes, "");
    assert_eq!(
        scripts.owner_level,
        generate_column_grants_script(&from, &[], true, "app.t", "note", &[], &["owner"])
    );
}

#[test]
fn test_grantor_revoke_other_object_kinds() {
    // (kind, name, FROM ACL, expected revoke); TO keeps only the delegate.
    let cases = [
        (
            "SEQUENCE",
            "app.s",
            ["delegate=U*/owner", "reader=U/delegate"],
            "REVOKE USAGE ON SEQUENCE app.s FROM reader GRANTED BY delegate;\n\n",
        ),
        (
            "FUNCTION",
            "app.f()",
            ["delegate=X*/owner", "reader=X/delegate"],
            "REVOKE EXECUTE ON FUNCTION app.f() FROM reader GRANTED BY delegate;\n\n",
        ),
        (
            "PROCEDURE",
            "app.p()",
            ["delegate=X*/owner", "reader=X/delegate"],
            "REVOKE EXECUTE ON PROCEDURE app.p() FROM reader GRANTED BY delegate;\n\n",
        ),
        (
            "SCHEMA",
            "app",
            ["delegate=UC*/owner", "reader=C/delegate"],
            "REVOKE CREATE ON SCHEMA app FROM reader GRANTED BY delegate;\n\n",
        ),
        (
            "TYPE",
            "app.ty",
            ["delegate=U*/owner", "reader=U/delegate"],
            "REVOKE USAGE ON TYPE app.ty FROM reader GRANTED BY delegate;\n\n",
        ),
    ];
    for (kind, name, from, expected) in cases {
        let scripts = generate_grants_scripts(
            &acl(&from),
            &acl(&from[..1]),
            true,
            AclTarget::Object { kind, name },
            &["owner"],
            &["owner"],
            Some(grantors(RevokeStyle::GrantedBy)),
        );
        assert_eq!(scripts.grantor_revokes, expected, "{kind}");
        assert_eq!(scripts.owner_level, "", "{kind}");
    }
}

#[test]
fn test_grantor_revoke_keeps_unparseable_and_invalid_items() {
    // Items the diff skips stay in the remaining ACL untouched.
    let scripts = table_scripts(
        &["garbage", "delegate=r*X/owner", "reader=rX/delegate"],
        &["delegate=r*/owner"],
        RevokeStyle::GrantedBy,
    );
    assert_eq!(
        scripts.grantor_revokes,
        "REVOKE SELECT ON TABLE app.t FROM reader GRANTED BY delegate;\n\n"
    );
    assert_eq!(scripts.owner_level, "");
}

// --- Replay guard (production output) ---

fn guarded(style: RevokeStyle) -> GrantorRevokes<'static> {
    GrantorRevokes {
        replay_guard: true,
        ..grantors(style)
    }
}

#[test]
fn test_replay_guard_granted_by_table() {
    let scripts = generate_grants_scripts(
        &acl(&["delegate=r*w*/owner", "reader=rw/delegate"]),
        &acl(&["delegate=r*w*/owner"]),
        true,
        TABLE_T,
        &["owner"],
        &["owner"],
        Some(guarded(RevokeStyle::GrantedBy)),
    );
    assert_eq!(
        scripts.grantor_revokes,
        "DO $pgc$\n\
         BEGIN\n    \
         IF EXISTS (SELECT 1 FROM pg_catalog.aclexplode((SELECT c.relacl FROM pg_catalog.pg_class c WHERE c.oid = pg_catalog.to_regclass('app.t'))) AS a\n               \
         WHERE pg_catalog.pg_get_userbyid(a.grantor) = 'delegate'\n                 \
         AND pg_catalog.pg_get_userbyid(a.grantee) = 'reader'\n                 \
         AND a.privilege_type IN ('SELECT', 'UPDATE')) THEN\n        \
         REVOKE SELECT, UPDATE ON TABLE app.t FROM reader GRANTED BY delegate;\n    \
         END IF;\n\
         END\n\
         $pgc$;\n\n"
    );
    assert_eq!(scripts.owner_level, "");
}

#[test]
fn test_replay_guard_set_role_switches_inside_the_block() {
    let scripts = generate_grants_scripts(
        &acl(&["delegate=r*/owner", "reader=r/delegate"]),
        &acl(&["delegate=r*/owner"]),
        true,
        TABLE_T,
        &["owner"],
        &["owner"],
        Some(guarded(RevokeStyle::SetRole)),
    );
    assert!(
        scripts.grantor_revokes.contains(
            "AND a.privilege_type IN ('SELECT')) THEN\n        \
             SET ROLE delegate;\n        \
             REVOKE SELECT ON TABLE app.t FROM reader;\n        \
             RESET ROLE;\n    \
             END IF;\n"
        ),
        "got: {}",
        scripts.grantor_revokes
    );
    assert!(!scripts.grantor_revokes.contains("GRANTED BY"));
    // One self-contained block per statement: no role switch outside it.
    assert!(
        !scripts
            .grantor_revokes
            .lines()
            .any(|l| l.starts_with("SET ROLE") || l.starts_with("RESET ROLE"))
    );
}

#[test]
fn test_replay_guard_public_grantee_and_grant_option() {
    // PUBLIC is grantee 0 in aclexplode; a grant-option revoke only runs
    // while the option is still recorded.
    let scripts = generate_grants_scripts(
        &acl(&["delegate=r*/owner", "=r*/delegate"]),
        &acl(&["delegate=r*/owner", "=r/owner"]),
        true,
        TABLE_T,
        &["owner"],
        &["owner"],
        Some(guarded(RevokeStyle::GrantedBy)),
    );
    assert!(
        scripts.grantor_revokes.contains(
            "AND a.grantee = 0\n                 \
             AND a.privilege_type IN ('SELECT') AND a.is_grantable) THEN\n        \
             REVOKE GRANT OPTION FOR SELECT ON TABLE app.t FROM PUBLIC GRANTED BY delegate;\n"
        ),
        "got: {}",
        scripts.grantor_revokes
    );
}

#[test]
fn test_replay_guard_names_roles_raw_and_escapes_literals() {
    // Role names compare raw in the guard and are quoted in the statement;
    // a quote inside an identifier is doubled in the literal.
    let scripts = generate_grants_scripts(
        &acl(&["Delegate=r*/owner", "\"it's\"=r/Delegate"]),
        &acl(&["Delegate=r*/owner"]),
        true,
        AclTarget::Object {
            kind: "TABLE",
            name: "app.\"o'k\"",
        },
        &["owner"],
        &["owner"],
        Some(guarded(RevokeStyle::GrantedBy)),
    );
    let script = &scripts.grantor_revokes;
    assert!(
        script.contains("pg_catalog.to_regclass('app.\"o''k\"')"),
        "got: {script}"
    );
    assert!(
        script.contains("pg_catalog.pg_get_userbyid(a.grantor) = 'Delegate'"),
        "got: {script}"
    );
    assert!(
        script.contains("pg_catalog.pg_get_userbyid(a.grantee) = 'it''s'"),
        "got: {script}"
    );
    assert!(
        script
            .contains("REVOKE SELECT ON TABLE app.\"o'k\" FROM \"it's\" GRANTED BY \"Delegate\";"),
        "got: {script}"
    );
}

#[test]
fn test_replay_guard_acl_lookup_per_target() {
    // (target, FROM ACL, lookup); TO keeps only the delegate.
    let cases = [
        (
            AclTarget::Object {
                kind: "SEQUENCE",
                name: "app.s",
            },
            ["delegate=U*/owner", "reader=U/delegate"],
            "(SELECT c.relacl FROM pg_catalog.pg_class c WHERE c.oid = pg_catalog.to_regclass('app.s'))",
        ),
        (
            AclTarget::Object {
                kind: "FUNCTION",
                name: "\"a.b\".f(p_id integer, VARIADIC p_rest text[])",
            },
            ["delegate=X*/owner", "reader=X/delegate"],
            "(SELECT p.proacl FROM pg_catalog.pg_proc p WHERE p.pronamespace = pg_catalog.to_regnamespace('\"a.b\"') AND pg_catalog.format('%I(%s)', p.proname, pg_catalog.pg_get_function_identity_arguments(p.oid)) = 'f(p_id integer, VARIADIC p_rest text[])')",
        ),
        (
            AclTarget::Object {
                kind: "PROCEDURE",
                name: "app.\"P\"()",
            },
            ["delegate=X*/owner", "reader=X/delegate"],
            "(SELECT p.proacl FROM pg_catalog.pg_proc p WHERE p.pronamespace = pg_catalog.to_regnamespace('app') AND pg_catalog.format('%I(%s)', p.proname, pg_catalog.pg_get_function_identity_arguments(p.oid)) = '\"P\"()')",
        ),
        (
            AclTarget::Object {
                kind: "SCHEMA",
                name: "\"App\"",
            },
            ["delegate=U*/owner", "reader=U/delegate"],
            "(SELECT n.nspacl FROM pg_catalog.pg_namespace n WHERE n.oid = pg_catalog.to_regnamespace('\"App\"'))",
        ),
        (
            AclTarget::Object {
                kind: "TYPE",
                name: "app.ty",
            },
            ["delegate=U*/owner", "reader=U/delegate"],
            "(SELECT t.typacl FROM pg_catalog.pg_type t WHERE t.oid = pg_catalog.to_regtype('app.ty'))",
        ),
        (
            AclTarget::Column {
                table: "app.t",
                column: "\"Note\"",
            },
            ["delegate=r*/owner", "reader=r/delegate"],
            "(SELECT c.attacl FROM pg_catalog.pg_attribute c WHERE c.attrelid = pg_catalog.to_regclass('app.t') AND pg_catalog.quote_ident(c.attname) = '\"Note\"')",
        ),
    ];
    for (target, from, lookup) in cases {
        let scripts = generate_grants_scripts(
            &acl(&from),
            &acl(&from[..1]),
            true,
            target,
            &["owner"],
            &["owner"],
            Some(guarded(RevokeStyle::GrantedBy)),
        );
        assert!(
            scripts
                .grantor_revokes
                .contains(&format!("pg_catalog.aclexplode({lookup}) AS a\n")),
            "{target:?}: got: {}",
            scripts.grantor_revokes
        );
    }
}

#[test]
fn test_dollar_quote_tag_avoids_the_body() {
    assert_eq!(
        dollar_quote_tag("REVOKE SELECT ON TABLE a.b FROM c;"),
        "$pgc$"
    );
    assert_eq!(dollar_quote_tag("x \"$pgc$\" y"), "$pgc1$");
    assert_eq!(dollar_quote_tag("\"$pgc$\" \"$pgc1$\""), "$pgc2$");
}

#[test]
fn test_replay_guard_leaves_owner_level_statements_alone() {
    let from = acl(&["delegate=r*/owner", "reader=r/delegate", "writer=w/owner"]);
    let to = acl(&["delegate=r*/owner"]);
    let plain = generate_grants_scripts(
        &from,
        &to,
        true,
        TABLE_T,
        &["owner"],
        &["owner"],
        Some(grantors(RevokeStyle::GrantedBy)),
    );
    let guarded = generate_grants_scripts(
        &from,
        &to,
        true,
        TABLE_T,
        &["owner"],
        &["owner"],
        Some(guarded(RevokeStyle::GrantedBy)),
    );
    assert_eq!(guarded.owner_level, plain.owner_level);
    assert_eq!(
        guarded.owner_level,
        "REVOKE UPDATE ON TABLE app.t FROM writer;\n\n"
    );
    assert_eq!(guarded.revoked, plain.revoked);
}

// --- Table-level revokes applied to columns (`parent_revokes`) ---

fn revoked(
    grantee: &str,
    grantors: &[&str],
    privilege: &str,
    option_only: bool,
) -> RevokedPrivilege {
    RevokedPrivilege {
        grantee: grantee.to_string(),
        grantors: grantors.iter().map(|s| s.to_string()).collect(),
        privilege: privilege.to_string(),
        option_only,
    }
}

fn column_scripts(
    from: &[&str],
    to: &[&str],
    parent_revokes: &[RevokedPrivilege],
) -> GrantsScripts {
    generate_grants_scripts(
        &acl(from),
        &acl(to),
        true,
        AclTarget::Column {
            table: "app.t",
            column: "note",
        },
        &[],
        &["owner"],
        Some(GrantorRevokes {
            owners: &["owner", "owner"],
            style: RevokeStyle::GrantedBy,
            parent_acl: &[],
            parent_revokes,
            replay_guard: false,
        }),
    )
}

#[test]
fn test_table_revokes_list_what_they_take() {
    let scripts = table_scripts(
        &["delegate=r*w/owner", "reader=r/delegate", "writer=w/owner"],
        &["delegate=r/owner"],
        RevokeStyle::GrantedBy,
    );
    assert_eq!(
        scripts.revoked,
        vec![
            revoked("reader", &["delegate"], "SELECT", false),
            revoked("delegate", &["owner"], "SELECT", true),
            revoked("delegate", &["owner"], "UPDATE", false),
            revoked("writer", &["owner"], "UPDATE", false),
        ]
    );
    // Both owners (deduplicated, raw) act for an owner-level statement.
    let scripts = generate_grants_scripts(
        &acl(&["reader=r/Owner_A"]),
        &acl(&[]),
        true,
        TABLE_T,
        &["\"Owner_A\""],
        &["owner_b"],
        Some(GrantorRevokes {
            owners: &["\"Owner_A\"", "owner_b"],
            style: RevokeStyle::GrantedBy,
            parent_acl: &[],
            parent_revokes: &[],
            replay_guard: false,
        }),
    );
    assert_eq!(
        scripts.revoked,
        vec![revoked("reader", &["Owner_A", "owner_b"], "SELECT", false)]
    );
    // Without grantor information (a recreated table) nothing is listed.
    let scripts = generate_grants_scripts(
        &acl(&["reader=r/owner"]),
        &acl(&[]),
        true,
        TABLE_T,
        &["owner"],
        &["owner"],
        None,
    );
    assert!(scripts.revoked.is_empty());
}

#[test]
fn test_column_regrants_what_a_table_revoke_strips() {
    // The table's REVOKE SELECT FROM reader (as the owner) also removes the
    // owner's column grant, which TO keeps.
    let parent = [revoked("reader", &["owner"], "SELECT", false)];
    let scripts = column_scripts(&["reader=r/owner"], &["reader=r/owner"], &parent);
    assert_eq!(scripts.grantor_revokes, "");
    assert_eq!(
        scripts.owner_level,
        "GRANT SELECT (note) ON TABLE app.t TO reader;\n\n"
    );
    // Without the parent revoke: nothing to do, as before.
    assert_eq!(
        column_scripts(&["reader=r/owner"], &["reader=r/owner"], &[]),
        GrantsScripts::default()
    );
}

#[test]
fn test_column_regrant_after_a_grantor_named_table_revoke() {
    let parent = [revoked("reader", &["delegate"], "SELECT", false)];
    let scripts = column_scripts(
        &["delegate=r*/owner", "reader=r/delegate"],
        &["delegate=r*/owner", "reader=r/owner"],
        &parent,
    );
    // The table statement removes the column entry itself: no column revoke.
    assert_eq!(scripts.grantor_revokes, "");
    assert_eq!(
        scripts.owner_level,
        "GRANT SELECT (note) ON TABLE app.t TO reader;\n\n"
    );
}

#[test]
fn test_column_table_revoke_only_reaches_its_own_grantor() {
    // reader also holds the column privilege from the delegate, which the
    // owner's table-level REVOKE does not touch: nothing to grant back.
    let parent = [revoked("reader", &["owner"], "SELECT", false)];
    let scripts = column_scripts(
        &["delegate=r*/owner", "reader=r/owner", "reader=r/delegate"],
        &["delegate=r*/owner", "reader=r/owner"],
        &parent,
    );
    assert_eq!(scripts, GrantsScripts::default());
}

#[test]
fn test_column_regrants_a_grant_option_a_table_revoke_strips() {
    let parent = [revoked("delegate", &["owner"], "SELECT", true)];
    let scripts = column_scripts(&["delegate=r*/owner"], &["delegate=r*/owner"], &parent);
    assert_eq!(
        scripts.owner_level,
        "GRANT SELECT (note) ON TABLE app.t TO delegate WITH GRANT OPTION;\n\n"
    );
    // TO wants the plain privilege only: the existing downgrade statement
    // stays, and nothing is added.
    let scripts = column_scripts(&["delegate=r*/owner"], &["delegate=r/owner"], &parent);
    assert_eq!(
        scripts.owner_level,
        "REVOKE GRANT OPTION FOR SELECT (note) ON TABLE app.t FROM delegate;\n\n"
    );
}

#[test]
fn test_column_dependents_of_a_table_level_option_revoke_go_first() {
    // The table's REVOKE GRANT OPTION FOR SELECT FROM delegate also takes the
    // delegate's column option, so the column entry it granted under it must
    // be revoked before — else "dependent privileges exist" — and both come
    // back from the owner.
    let parent = [revoked("delegate", &["owner"], "SELECT", true)];
    let scripts = column_scripts(
        &["delegate=r*/owner", "reader=r/delegate"],
        &["delegate=r*/owner", "reader=r/owner"],
        &parent,
    );
    assert_eq!(
        scripts.grantor_revokes,
        "REVOKE SELECT (note) ON TABLE app.t FROM reader GRANTED BY delegate;\n\n"
    );
    assert_eq!(
        scripts.owner_level,
        "GRANT SELECT (note) ON TABLE app.t TO reader;\n\n\
         GRANT SELECT (note) ON TABLE app.t TO delegate WITH GRANT OPTION;\n\n"
    );
}

#[test]
fn test_column_parent_revoke_of_another_privilege_changes_nothing() {
    let parent = [revoked("reader", &["owner"], "UPDATE", false)];
    let scripts = column_scripts(&["reader=r/owner"], &["reader=r/owner"], &parent);
    assert_eq!(scripts, GrantsScripts::default());
}

#[test]
fn test_strip_revoked() {
    let stripped = strip_revoked(
        &acl(&[
            "garbage",
            "reader=rw*/owner",
            "reader=r/delegate",
            "\"Odd\"=r*/owner",
            "=r/owner",
        ]),
        &[
            revoked("reader", &["owner"], "SELECT", false),
            revoked("reader", &["owner"], "UPDATE", true),
            revoked("Odd", &["owner"], "SELECT", true),
            revoked("", &["owner"], "SELECT", false),
        ],
        "COLUMN",
    );
    assert_eq!(
        stripped,
        acl(&[
            "garbage",
            "reader=w/owner",
            "reader=r/delegate",
            "\"Odd\"=r/owner"
        ])
    );
}

// ---- pre-cascade: statements that reach TO from any of several ACLs -------

#[test]
fn from_any_with_one_state_is_the_plain_script() {
    let target = AclTarget::Object {
        kind: "TABLE",
        name: "app.v",
    };
    let cases: [(&[&str], &[&str]); 4] = [
        (
            &["owner=arwdDxt/owner", "a=r/owner"],
            &["owner=arwdDxt/owner", "b=rw/owner"],
        ),
        (&["a=r*w/owner", "=r/owner"], &["a=rw/owner"]),
        (&[], &["a=r/owner", "b=a*/owner", "=r/owner"]),
        (&["a=r/owner", "a=w/other"], &[]),
    ];
    for (from, to) in cases {
        let (from, to) = (acl(from), acl(to));
        for full in [true, false] {
            assert_eq!(
                generate_grants_script_from_any(
                    &[(from.as_slice(), &["owner"])],
                    &to,
                    full,
                    target,
                    &["owner"],
                ),
                generate_grants_script(&from, &to, full, "TABLE", "app.v", &["owner"], &["owner"]),
                "{from:?} -> {to:?} (full: {full})"
            );
        }
    }
}

#[test]
fn from_any_grants_what_a_state_lacks_and_revokes_what_a_state_has() {
    let target = AclTarget::Object {
        kind: "FUNCTION",
        name: "app.f()",
    };
    // Created by a role whose defaults revoke PUBLIC and grant d, or by one
    // without defaults (the built-in default: PUBLIC).
    let with_defaults = acl(&["app=X/app", "d=X/app"]);
    let builtin = acl(&["=X/owner"]);
    let to = acl(&["owner=X/owner", "=X/owner", "r=X/owner"]);
    let states: [(&[String], &[&str]); 2] = [
        (with_defaults.as_slice(), &["app", "owner"]),
        (builtin.as_slice(), &["owner"]),
    ];
    assert_eq!(
        generate_grants_script_from_any(&states, &to, true, target, &["owner"]),
        "GRANT EXECUTE ON FUNCTION app.f() TO PUBLIC;\n\n\
         REVOKE EXECUTE ON FUNCTION app.f() FROM d;\n\n\
         GRANT EXECUTE ON FUNCTION app.f() TO r;\n\n"
    );
    // Addonly only grants.
    assert_eq!(
        generate_grants_script_from_any(&states, &to, false, target, &["owner"]),
        "GRANT EXECUTE ON FUNCTION app.f() TO PUBLIC;\n\n\
         GRANT EXECUTE ON FUNCTION app.f() TO r;\n\n"
    );
}

#[test]
fn from_any_takes_a_grant_option_away_before_granting_the_privilege() {
    let target = AclTarget::Object {
        kind: "TABLE",
        name: "app.v",
    };
    let fresh: Vec<String> = Vec::new();
    let kept = acl(&["a=r*/owner", "b=r/owner"]);
    let to = acl(&["a=r/owner", "b=r/owner"]);
    // Re-created: a and b need SELECT. Kept: a loses the grant option.
    assert_eq!(
        generate_grants_script_from_any(
            &[(fresh.as_slice(), &[]), (kept.as_slice(), &["owner"])],
            &to,
            true,
            target,
            &["owner"],
        ),
        "REVOKE GRANT OPTION FOR SELECT ON TABLE app.v FROM a;\n\n\
         GRANT SELECT ON TABLE app.v TO a;\n\n\
         GRANT SELECT ON TABLE app.v TO b;\n\n"
    );
}

// ==== fix:pre-grants tests ====

#[test]
fn test_grant_and_revoke_scripts_quote_the_grantee() {
    // aclitem text leaves Reader and user bare: written as-is, `TO Reader`
    // names reader and `TO user` does not parse.
    assert_eq!(
        AclEntry::get_grant_script("Reader=r/owner", "TABLE", "app.t"),
        "GRANT SELECT ON TABLE app.t TO \"Reader\";\n\n"
    );
    assert_eq!(
        AclEntry::get_grant_script("user=r*/owner", "TABLE", "app.t"),
        "GRANT SELECT ON TABLE app.t TO \"user\" WITH GRANT OPTION;\n\n"
    );
    assert_eq!(
        AclEntry::get_revoke_script("\"a b\"=r*/owner", "TABLE", "app.t"),
        "REVOKE GRANT OPTION FOR SELECT ON TABLE app.t FROM \"a b\";\n\n\
         REVOKE SELECT ON TABLE app.t FROM \"a b\";\n\n"
    );
    // Lowercase names and PUBLIC are spelled as before.
    assert_eq!(
        AclEntry::get_grant_script("reader=r/owner", "TABLE", "app.t"),
        "GRANT SELECT ON TABLE app.t TO reader;\n\n"
    );
    assert_eq!(
        AclEntry::get_revoke_script("=X/owner", "FUNCTION", "app.f()"),
        "REVOKE EXECUTE ON FUNCTION app.f() FROM PUBLIC;\n\n"
    );
}

#[test]
fn test_owner_level_diff_quotes_the_grantee() {
    // The finding's chain: the delegate's own grant (from the owner) goes
    // through the owner-level statements, which now spell it like the
    // grantor-named revoke does.
    let scripts = table_scripts(
        &["owner=arwdDxt/owner", "Deleg=r*/owner", "Reader=r/Deleg"],
        &["owner=arwdDxt/owner"],
        RevokeStyle::GrantedBy,
    );
    assert_eq!(
        scripts.grantor_revokes,
        "REVOKE SELECT ON TABLE app.t FROM \"Reader\" GRANTED BY \"Deleg\";\n\n"
    );
    assert_eq!(
        scripts.owner_level,
        "REVOKE GRANT OPTION FOR SELECT ON TABLE app.t FROM \"Deleg\";\n\n\
         REVOKE SELECT ON TABLE app.t FROM \"Deleg\";\n\n"
    );
    let added = generate_grants_script(
        &[],
        &acl(&["Writer=r/owner", "select=a/owner"]),
        true,
        "TABLE",
        "app.t2",
        &["owner"],
        &["owner"],
    );
    assert_eq!(
        added,
        "GRANT SELECT ON TABLE app.t2 TO \"Writer\";\n\n\
         GRANT INSERT ON TABLE app.t2 TO \"select\";\n\n"
    );
}

#[test]
fn test_column_grants_quote_the_grantee() {
    assert_eq!(
        generate_column_grants_script(
            &[],
            &acl(&["Reader=r/owner"]),
            true,
            "app.t",
            "note",
            &[],
            &["owner"],
        ),
        "GRANT SELECT (note) ON TABLE app.t TO \"Reader\";\n\n"
    );
}

#[test]
fn test_mixed_case_owner_entries_are_skipped() {
    // The dump spells the owner `"Owner"`, its own aclitem says `Owner`: the
    // owner's implicit entry is not a grantee. Compared as different, a NULL
    // TO ACL used to revoke the owner's own privileges once the grantee was
    // quoted.
    let owners = ["\"Owner\""];
    assert_eq!(
        generate_grants_script(
            &acl(&["Owner=arwdDxt/Owner", "reader=r/Owner"]),
            &[],
            true,
            "TABLE",
            "app.t",
            &owners,
            &owners,
        ),
        "REVOKE SELECT ON TABLE app.t FROM reader;\n\n"
    );
    assert_eq!(
        generate_grants_script(
            &[],
            &acl(&["\"we/ird\"=arwdDxt/\"we/ird\""]),
            true,
            "TABLE",
            "app.t",
            &["\"we/ird\""],
            &["\"we/ird\""],
        ),
        ""
    );
    assert!(is_owner(&["\"Owner\""], "Owner"));
    assert!(is_owner(&["owner"], "owner"));
    assert!(!is_owner(&["\"Owner\""], "owner"));
    assert!(!is_owner(&[""], ""));
}

#[test]
fn test_public_default_privileges() {
    assert_eq!(public_default_privilege("FUNCTION"), Some('X'));
    assert_eq!(public_default_privilege("PROCEDURE"), Some('X'));
    assert_eq!(public_default_privilege("TYPE"), Some('U'));
    for kind in ["TABLE", "SEQUENCE", "SCHEMA", "COLUMN", "FOREIGN TABLE"] {
        assert_eq!(public_default_privilege(kind), None, "{kind}");
    }
    assert_eq!(
        public_default_entry("TYPE", "\"Owner\"").as_deref(),
        Some("=U/\"Owner\"")
    );
    assert_eq!(public_default_entry("TABLE", "owner"), None);
}

#[test]
fn test_effective_acl_models_a_null_acl_as_its_default() {
    let explicit = acl(&["owner=X/owner"]);
    // NULL: PUBLIC's default privilege, for routines and types only.
    assert_eq!(
        effective_acl(&[], false, "FUNCTION", "owner").as_ref(),
        acl(&["=X/owner"]).as_slice()
    );
    assert!(effective_acl(&[], false, "SCHEMA", "owner").is_empty());
    // `{}`: everything revoked.
    assert!(effective_acl(&[], true, "TYPE", "owner").is_empty());
    // Explicit ACLs are borrowed as they are.
    assert!(matches!(
        effective_acl(&explicit, false, "FUNCTION", "owner"),
        Cow::Borrowed(_)
    ));
}

#[test]
fn test_null_acl_against_a_public_revoke_is_migrated() {
    // FROM NULL, TO with PUBLIC revoked: the REVOKE that used to be lost.
    let from = effective_acl(&[], false, "FUNCTION", "owner");
    let to = acl(&["owner=X/owner"]);
    assert_eq!(
        generate_grants_script(
            &from,
            &to,
            true,
            "FUNCTION",
            "app.secret()",
            &["owner"],
            &["owner"]
        ),
        "REVOKE EXECUTE ON FUNCTION app.secret() FROM PUBLIC;\n\n"
    );
    // The reverse: PUBLIC revoked in FROM, NULL in TO.
    let to = effective_acl(&[], false, "TYPE", "owner");
    assert_eq!(
        generate_grants_script(
            &acl(&["owner=U/owner"]),
            &to,
            true,
            "TYPE",
            "app.dom",
            &["owner"],
            &["owner"]
        ),
        "GRANT USAGE ON TYPE app.dom TO PUBLIC;\n\n"
    );
    // Both NULL, or both explicit: nothing, as before.
    let null = effective_acl(&[], false, "FUNCTION", "owner");
    assert_eq!(
        generate_grants_script(
            &null,
            &null,
            true,
            "FUNCTION",
            "app.f()",
            &["owner"],
            &["owner"]
        ),
        ""
    );
    // A FROM ACL that keeps PUBLIC, against a NULL TO: only the other
    // grantee's privilege goes (PUBLIC's was revoked too).
    assert_eq!(
        generate_grants_script(
            &acl(&["=X/owner", "owner=X/owner", "app=X/owner"]),
            &null,
            true,
            "FUNCTION",
            "app.f()",
            &["owner"],
            &["owner"]
        ),
        "REVOKE EXECUTE ON FUNCTION app.f() FROM app;\n\n"
    );
}

// ---- fix:pre-grants review: objects the migration creates ----

fn items(acl: &[&str]) -> Vec<String> {
    acl.iter().map(|a| a.to_string()).collect()
}

#[test]
fn created_object_grants_equal_the_plain_diff_when_nothing_is_uncertain() {
    let target = AclTarget::Object {
        kind: "FUNCTION",
        name: "s.f()",
    };
    let start = items(&["=X/o"]);
    for to in [
        items(&[]),
        items(&["=X/o"]),
        items(&["o=X/o"]),
        items(&["o=X/o", "r=X*/o", "\"Mixed\"=X/o"]),
    ] {
        for full in [true, false] {
            let created = CreatedAcl {
                certain: start.clone(),
                possible: start.clone(),
            };
            assert_eq!(
                generate_created_object_grants(&created, &to, full, target, &["o"]),
                generate_grants_scripts(&start, &to, full, target, &["o"], &["o"], None),
                "to = {to:?}, full = {full}"
            );
        }
    }
}

#[test]
fn created_object_grants_revoke_the_possible_and_grant_the_uncertain() {
    let target = AclTarget::Object {
        kind: "TYPE",
        name: "s.t",
    };
    let created = CreatedAcl {
        certain: vec![],
        possible: items(&["=U/app", "r=U*/app", "w=U/app"]),
    };
    // r keeps USAGE without the grant option; w loses it; x is new; PUBLIC
    // stays and is granted in case the creator's defaults removed it.
    let to = items(&["=U/o", "r=U/o", "x=U/o"]);
    assert_eq!(
        generate_created_object_grants(&created, &to, true, target, &["o"]).owner_level,
        "GRANT USAGE ON TYPE s.t TO PUBLIC;\n\n\
         REVOKE GRANT OPTION FOR USAGE ON TYPE s.t FROM r;\n\n\
         GRANT USAGE ON TYPE s.t TO r;\n\n\
         REVOKE USAGE ON TYPE s.t FROM w;\n\n\
         GRANT USAGE ON TYPE s.t TO x;\n\n"
    );
    // Add-only revokes nothing.
    assert_eq!(
        generate_created_object_grants(&created, &to, false, target, &["o"]).owner_level,
        "GRANT USAGE ON TYPE s.t TO PUBLIC;\n\n\
         GRANT USAGE ON TYPE s.t TO r;\n\n\
         GRANT USAGE ON TYPE s.t TO x;\n\n"
    );
    // The owner is never a grantee, on either side.
    let owner_granted = CreatedAcl {
        certain: vec![],
        possible: items(&["o=U/app"]),
    };
    assert_eq!(
        generate_created_object_grants(&owner_granted, &items(&["o=U/o"]), true, target, &["o"])
            .owner_level,
        ""
    );
}

#[test]
fn r3c_foreign_server_usage_is_a_valid_privilege() {
    // A server the migration creates again gets TO's `srvacl` back.
    let script = AclEntry::get_grant_script("reader=U*/postgres", "FOREIGN SERVER", "\"Srv X\"");
    assert_eq!(
        script,
        "GRANT USAGE ON FOREIGN SERVER \"Srv X\" TO reader WITH GRANT OPTION;\n\n"
    );
    // Nothing else is: a stray letter is ignored, as for any kind.
    assert_eq!(
        AclEntry::get_grant_script("reader=r/postgres", "FOREIGN SERVER", "srv"),
        ""
    );
}
