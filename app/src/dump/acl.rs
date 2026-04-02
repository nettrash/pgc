/// Represents a single parsed PostgreSQL ACL entry.
///
/// PostgreSQL ACL items have the format: `grantee=privileges/grantor`
/// where privileges are single-character codes:
///   r = SELECT, a = INSERT, w = UPDATE, d = DELETE, D = TRUNCATE,
///   x = REFERENCES, t = TRIGGER, X = EXECUTE, U = USAGE, C = CREATE,
///   c = CONNECT, T = TEMPORARY
/// A `*` after a privilege character indicates WITH GRANT OPTION.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AclEntry {
    pub grantee: String,
    pub privileges: String,
    pub grantor: String,
}

/// A single privilege with its grant-option flag.
#[derive(Debug, Clone, PartialEq, Eq)]
struct PrivilegeItem {
    name: String,
    grant_option: bool,
}

impl AclEntry {
    /// Parse a single ACL item string like `"user=arwdDxt/owner"`.
    pub fn parse(acl_item: &str) -> Option<Self> {
        let eq_pos = acl_item.find('=')?;
        let slash_pos = acl_item.find('/')?;
        if slash_pos <= eq_pos {
            return None;
        }
        let grantee = acl_item[..eq_pos].to_string();
        let privileges = acl_item[eq_pos + 1..slash_pos].to_string();
        let grantor = acl_item[slash_pos + 1..].to_string();
        if privileges.is_empty() {
            return None;
        }
        Some(AclEntry {
            grantee,
            privileges,
            grantor,
        })
    }

    /// Map a single privilege character to its SQL keyword.
    fn priv_char_to_name(ch: char) -> Option<&'static str> {
        match ch {
            'r' => Some("SELECT"),
            'a' => Some("INSERT"),
            'w' => Some("UPDATE"),
            'd' => Some("DELETE"),
            'D' => Some("TRUNCATE"),
            'x' => Some("REFERENCES"),
            't' => Some("TRIGGER"),
            'X' => Some("EXECUTE"),
            'U' => Some("USAGE"),
            'C' => Some("CREATE"),
            'c' => Some("CONNECT"),
            'T' => Some("TEMPORARY"),
            _ => None,
        }
    }

    /// Valid privilege names for a given object kind.
    fn valid_privileges(object_kind: &str) -> &'static [&'static str] {
        match object_kind {
            "TABLE" => &[
                "SELECT",
                "INSERT",
                "UPDATE",
                "DELETE",
                "TRUNCATE",
                "REFERENCES",
                "TRIGGER",
            ],
            "SEQUENCE" => &["USAGE", "SELECT", "UPDATE"],
            "FUNCTION" | "PROCEDURE" => &["EXECUTE"],
            "SCHEMA" => &["USAGE", "CREATE"],
            _ => &[],
        }
    }

    /// Parse privilege string into items, each with a grant-option flag.
    /// E.g. `"r*wad"` → SELECT(grant_option), UPDATE, INSERT, DELETE.
    fn parse_privileges(privs: &str, object_kind: &str) -> Vec<PrivilegeItem> {
        let valid = Self::valid_privileges(object_kind);
        let chars: Vec<char> = privs.chars().collect();
        let mut result = Vec::new();
        let mut i = 0;
        while i < chars.len() {
            if let Some(name) = Self::priv_char_to_name(chars[i]) {
                let grant_option = i + 1 < chars.len() && chars[i + 1] == '*';
                if grant_option {
                    i += 1; // skip the '*'
                }
                if valid.contains(&name) {
                    result.push(PrivilegeItem {
                        name: name.to_string(),
                        grant_option,
                    });
                }
            }
            i += 1;
        }
        result
    }

    fn format_grantee(grantee: &str) -> String {
        if grantee.is_empty() {
            "PUBLIC".to_string()
        } else {
            grantee.to_string()
        }
    }

    /// Generate GRANT statement(s) for this ACL entry on the given object.
    /// Privileges with and without GRANT OPTION are emitted as separate statements.
    pub fn get_grant_script(acl_item: &str, object_kind: &str, object_name: &str) -> String {
        let entry = match AclEntry::parse(acl_item) {
            Some(e) => e,
            None => return String::new(),
        };
        let items = Self::parse_privileges(&entry.privileges, object_kind);
        if items.is_empty() {
            return String::new();
        }
        let grantee = Self::format_grantee(&entry.grantee);
        let mut script = String::new();

        // Collect privileges without grant option
        let plain: Vec<&str> = items
            .iter()
            .filter(|p| !p.grant_option)
            .map(|p| p.name.as_str())
            .collect();
        if !plain.is_empty() {
            script.push_str(&format!(
                "GRANT {} ON {} {} TO {};\n",
                plain.join(", "),
                object_kind,
                object_name,
                grantee
            ));
        }

        // Collect privileges with grant option
        let with_go: Vec<&str> = items
            .iter()
            .filter(|p| p.grant_option)
            .map(|p| p.name.as_str())
            .collect();
        if !with_go.is_empty() {
            script.push_str(&format!(
                "GRANT {} ON {} {} TO {} WITH GRANT OPTION;\n",
                with_go.join(", "),
                object_kind,
                object_name,
                grantee
            ));
        }

        script
    }

    /// Generate REVOKE statement(s) for this ACL entry on the given object.
    /// When revoking privileges that had GRANT OPTION, emits REVOKE GRANT OPTION FOR first,
    /// then REVOKE for the privilege itself along with plain privileges.
    pub fn get_revoke_script(acl_item: &str, object_kind: &str, object_name: &str) -> String {
        let entry = match AclEntry::parse(acl_item) {
            Some(e) => e,
            None => return String::new(),
        };
        let items = Self::parse_privileges(&entry.privileges, object_kind);
        if items.is_empty() {
            return String::new();
        }
        let grantee = Self::format_grantee(&entry.grantee);
        let mut script = String::new();

        // For grant-option privileges, first revoke the grant option
        let with_go: Vec<&str> = items
            .iter()
            .filter(|p| p.grant_option)
            .map(|p| p.name.as_str())
            .collect();
        if !with_go.is_empty() {
            script.push_str(&format!(
                "REVOKE GRANT OPTION FOR {} ON {} {} FROM {};\n",
                with_go.join(", "),
                object_kind,
                object_name,
                grantee
            ));
        }

        // Revoke all privileges (both plain and grant-option ones)
        let all: Vec<&str> = items.iter().map(|p| p.name.as_str()).collect();
        script.push_str(&format!(
            "REVOKE {} ON {} {} FROM {};\n",
            all.join(", "),
            object_kind,
            object_name,
            grantee
        ));

        script
    }
}

/// Represents the per-grantee privilege diff between two ACL lists.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AclDiffEntry {
    pub grantee: String,
    pub grants_plain: Vec<String>,
    pub grants_with_option: Vec<String>,
    pub revoke_option_for: Vec<String>,
    pub revokes: Vec<String>,
}

/// Build per-grantee privilege map: grantee → {privilege_name → has_grant_option}.
/// Multiple ACL entries for the same grantee (from different grantors) are merged;
/// `grant_option = true` wins when the same privilege appears in multiple entries.
fn build_privilege_map(
    acl: &[String],
    object_kind: &str,
) -> std::collections::HashMap<String, std::collections::HashMap<String, bool>> {
    use std::collections::HashMap;
    let mut map: HashMap<String, HashMap<String, bool>> = HashMap::new();
    for item in acl {
        if let Some(entry) = AclEntry::parse(item) {
            let privs = AclEntry::parse_privileges(&entry.privileges, object_kind);
            let grantee_map = map.entry(entry.grantee).or_default();
            for p in privs {
                let existing = grantee_map.entry(p.name).or_insert(false);
                if p.grant_option {
                    *existing = true;
                }
            }
        }
    }
    map
}

/// Compute the per-grantee privilege diff between two ACL lists.
///
/// Compares on a normalised representation (grantee + privilege set + grant-option),
/// **ignoring the grantor** field entirely.  Returns one [`AclDiffEntry`] per grantee
/// that has at least one action.  Revoke entries are only produced when `full` is `true`.
///
/// Grantees listed in `owners` are skipped entirely — PostgreSQL object owners have
/// implicit full privileges, so GRANT/REVOKE targeting them is meaningless.
pub fn diff_acls(
    from_acl: &[String],
    to_acl: &[String],
    full: bool,
    object_kind: &str,
    owners: &[&str],
) -> Vec<AclDiffEntry> {
    use std::collections::BTreeSet;

    let from_map = build_privilege_map(from_acl, object_kind);
    let to_map = build_privilege_map(to_acl, object_kind);

    let empty_privs: std::collections::HashMap<String, bool> = std::collections::HashMap::new();

    let mut all_grantees: BTreeSet<&str> = BTreeSet::new();
    for g in from_map.keys() {
        all_grantees.insert(g.as_str());
    }
    for g in to_map.keys() {
        all_grantees.insert(g.as_str());
    }

    // Remove object owners — they have implicit full privileges
    for owner in owners {
        all_grantees.remove(owner);
    }

    let mut result = Vec::new();

    for grantee in &all_grantees {
        let from_privs = from_map.get(*grantee).unwrap_or(&empty_privs);
        let to_privs = to_map.get(*grantee).unwrap_or(&empty_privs);

        let mut grants_plain = Vec::new();
        let mut grants_with_option = Vec::new();
        let mut revoke_option_for = Vec::new();
        let mut revokes = Vec::new();

        let mut all_privs: BTreeSet<&str> = BTreeSet::new();
        for p in from_privs.keys() {
            all_privs.insert(p.as_str());
        }
        for p in to_privs.keys() {
            all_privs.insert(p.as_str());
        }

        for priv_name in &all_privs {
            let from_go = from_privs.get(*priv_name);
            let to_go = to_privs.get(*priv_name);

            match (from_go, to_go) {
                // New privilege
                (None, Some(false)) => grants_plain.push(priv_name.to_string()),
                (None, Some(true)) => grants_with_option.push(priv_name.to_string()),
                // Upgrade: plain → with grant option
                (Some(false), Some(true)) => grants_with_option.push(priv_name.to_string()),
                // Downgrade: with grant option → plain (full mode only)
                (Some(true), Some(false)) if full => {
                    revoke_option_for.push(priv_name.to_string());
                }
                // Removed entirely (full mode only)
                (Some(go), None) if full => {
                    if *go {
                        revoke_option_for.push(priv_name.to_string());
                    }
                    revokes.push(priv_name.to_string());
                }
                _ => {}
            }
        }

        if !grants_plain.is_empty()
            || !grants_with_option.is_empty()
            || !revoke_option_for.is_empty()
            || !revokes.is_empty()
        {
            result.push(AclDiffEntry {
                grantee: grantee.to_string(),
                grants_plain,
                grants_with_option,
                revoke_option_for,
                revokes,
            });
        }
    }

    result
}

/// Generate the combined GRANT/REVOKE script for an object.
///
/// `owners` lists role names that own the object (from/to); their ACL entries are skipped.
pub fn generate_grants_script(
    from_acl: &[String],
    to_acl: &[String],
    full: bool,
    object_kind: &str,
    object_name: &str,
    owners: &[&str],
) -> String {
    let diffs = diff_acls(from_acl, to_acl, full, object_kind, owners);
    let mut script = String::new();

    for entry in &diffs {
        let grantee = AclEntry::format_grantee(&entry.grantee);

        if !entry.revoke_option_for.is_empty() {
            script.push_str(&format!(
                "REVOKE GRANT OPTION FOR {} ON {} {} FROM {};\n",
                entry.revoke_option_for.join(", "),
                object_kind,
                object_name,
                grantee
            ));
        }
        if !entry.revokes.is_empty() {
            script.push_str(&format!(
                "REVOKE {} ON {} {} FROM {};\n",
                entry.revokes.join(", "),
                object_kind,
                object_name,
                grantee
            ));
        }
        if !entry.grants_plain.is_empty() {
            script.push_str(&format!(
                "GRANT {} ON {} {} TO {};\n",
                entry.grants_plain.join(", "),
                object_kind,
                object_name,
                grantee
            ));
        }
        if !entry.grants_with_option.is_empty() {
            script.push_str(&format!(
                "GRANT {} ON {} {} TO {} WITH GRANT OPTION;\n",
                entry.grants_with_option.join(", "),
                object_kind,
                object_name,
                grantee
            ));
        }
    }

    script
}

/// Generate GRANT statements for a new object (all ACL entries from "to").
///
/// `owners` lists role names that own the object; their ACL entries are skipped.
pub fn generate_new_object_grants(
    to_acl: &[String],
    object_kind: &str,
    object_name: &str,
    owners: &[&str],
) -> String {
    generate_grants_script(&[], to_acl, false, object_kind, object_name, owners)
}

#[cfg(test)]
mod tests {
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
            "GRANT SELECT, UPDATE ON TABLE public.my_table TO myuser;\n"
        );
    }

    #[test]
    fn test_grant_script_with_grant_option() {
        let script = AclEntry::get_grant_script("myuser=r*w/owner", "TABLE", "public.my_table");
        assert_eq!(
            script,
            "GRANT UPDATE ON TABLE public.my_table TO myuser;\nGRANT SELECT ON TABLE public.my_table TO myuser WITH GRANT OPTION;\n"
        );
    }

    #[test]
    fn test_grant_script_all_grant_option() {
        let script = AclEntry::get_grant_script("myuser=r*w*/owner", "TABLE", "public.my_table");
        assert_eq!(
            script,
            "GRANT SELECT, UPDATE ON TABLE public.my_table TO myuser WITH GRANT OPTION;\n"
        );
    }

    #[test]
    fn test_revoke_script() {
        let script = AclEntry::get_revoke_script("myuser=r/owner", "TABLE", "public.my_table");
        assert_eq!(
            script,
            "REVOKE SELECT ON TABLE public.my_table FROM myuser;\n"
        );
    }

    #[test]
    fn test_revoke_script_with_grant_option() {
        let script = AclEntry::get_revoke_script("myuser=r*w/owner", "TABLE", "public.my_table");
        assert_eq!(
            script,
            "REVOKE GRANT OPTION FOR SELECT ON TABLE public.my_table FROM myuser;\nREVOKE SELECT, UPDATE ON TABLE public.my_table FROM myuser;\n"
        );
    }

    #[test]
    fn test_diff_acls_grant_option_upgrade() {
        // FROM: plain SELECT; TO: SELECT with grant option → upgrade, no revoke
        let from = vec!["user1=r/owner".to_string()];
        let to = vec!["user1=r*/owner".to_string()];
        let diffs = diff_acls(&from, &to, true, "TABLE", &[]);
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
        let diffs = diff_acls(&from, &to, true, "TABLE", &[]);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].revoke_option_for, vec!["SELECT"]);
        assert!(diffs[0].revokes.is_empty());
    }

    #[test]
    fn test_diff_acls_addonly() {
        let from = vec!["user1=r/owner".to_string()];
        let to = vec!["user1=r/owner".to_string(), "user2=rw/owner".to_string()];
        let diffs = diff_acls(&from, &to, false, "TABLE", &[]);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].grantee, "user2");
        assert_eq!(diffs[0].grants_plain, vec!["SELECT", "UPDATE"]);
    }

    #[test]
    fn test_diff_acls_full() {
        let from = vec!["user1=r/owner".to_string(), "user3=d/owner".to_string()];
        let to = vec!["user1=r/owner".to_string(), "user2=rw/owner".to_string()];
        let diffs = diff_acls(&from, &to, true, "TABLE", &[]);
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
        let diffs = diff_acls(&from, &to, true, "TABLE", &[]);
        assert!(diffs.is_empty());
    }

    #[test]
    fn test_diff_acls_merges_multiple_grantors() {
        // Same grantee, privileges split across grantors → merged, no diff
        let from = vec!["user1=r/owner1".to_string(), "user1=w/owner2".to_string()];
        let to = vec!["user1=rw/owner3".to_string()];
        let diffs = diff_acls(&from, &to, true, "TABLE", &[]);
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
            "GRANT SELECT ON TABLE public.t TO PUBLIC WITH GRANT OPTION;\n"
        );
    }

    #[test]
    fn test_diff_acls_excludes_owners() {
        // Owner in FROM has explicit entry, absent in TO → should NOT produce a REVOKE
        let from = vec![
            "owner_a=arwdDxt/owner_a".to_string(),
            "reader=r/owner_a".to_string(),
        ];
        let to = vec![
            "owner_b=arwdDxt/owner_b".to_string(),
            "reader=r/owner_b".to_string(),
        ];
        let diffs = diff_acls(&from, &to, true, "TABLE", &["owner_a", "owner_b"]);
        assert!(
            diffs.is_empty(),
            "Owner grantees must be excluded, got: {diffs:?}"
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
        let diffs = diff_acls(&from, &to, true, "TABLE", &["theowner"]);
        assert_eq!(diffs.len(), 2);
        let added = diffs.iter().find(|d| d.grantee == "new_reader").unwrap();
        assert_eq!(added.grants_plain, vec!["SELECT"]);
        let removed = diffs.iter().find(|d| d.grantee == "old_reader").unwrap();
        assert_eq!(removed.revokes, vec!["SELECT"]);
    }

    #[test]
    fn test_generate_grants_script_excludes_owner() {
        let from = vec!["owner_a=X/owner_a".to_string()];
        let to = vec!["owner_b=X/owner_b".to_string(), "app=X/owner_b".to_string()];
        let script = generate_grants_script(
            &from,
            &to,
            true,
            "FUNCTION",
            "public.my_func()",
            &["owner_a", "owner_b"],
        );
        assert!(
            script.contains("GRANT EXECUTE ON FUNCTION public.my_func() TO app;"),
            "Must grant to non-owner, got: {script}"
        );
        assert!(
            !script.contains("owner_a"),
            "Must not reference old owner, got: {script}"
        );
        assert!(
            !script.contains("owner_b"),
            "Must not reference new owner, got: {script}"
        );
    }
}
