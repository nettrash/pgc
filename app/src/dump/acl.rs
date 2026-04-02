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

/// Compute grants to add and optionally revoke between two ACL lists.
///
/// Returns `(grants_to_add, grants_to_revoke)` where each is a list of raw ACL item strings.
/// `grants_to_revoke` is empty when not in full mode.
pub fn diff_acls(from_acl: &[String], to_acl: &[String], full: bool) -> (Vec<String>, Vec<String>) {
    use std::collections::HashSet;

    let from_set: HashSet<&str> = from_acl.iter().map(|s| s.as_str()).collect();
    let to_set: HashSet<&str> = to_acl.iter().map(|s| s.as_str()).collect();

    // ACL items to add: present in "to" but not in "from"
    let to_add: Vec<String> = to_acl
        .iter()
        .filter(|item| !from_set.contains(item.as_str()))
        .cloned()
        .collect();

    // ACL items to revoke (only in full mode): present in "from" but not in "to"
    let to_revoke: Vec<String> = if full {
        from_acl
            .iter()
            .filter(|item| !to_set.contains(item.as_str()))
            .cloned()
            .collect()
    } else {
        Vec::new()
    };

    (to_add, to_revoke)
}

/// Generate the combined GRANT/REVOKE script for an object.
pub fn generate_grants_script(
    from_acl: &[String],
    to_acl: &[String],
    full: bool,
    object_kind: &str,
    object_name: &str,
) -> String {
    let (to_add, to_revoke) = diff_acls(from_acl, to_acl, full);
    let mut script = String::new();

    for item in &to_revoke {
        script.push_str(&AclEntry::get_revoke_script(item, object_kind, object_name));
    }
    for item in &to_add {
        script.push_str(&AclEntry::get_grant_script(item, object_kind, object_name));
    }

    script
}

/// Generate GRANT statements for a new object (all ACL entries from "to").
pub fn generate_new_object_grants(
    to_acl: &[String],
    object_kind: &str,
    object_name: &str,
) -> String {
    let mut script = String::new();
    for item in to_acl {
        script.push_str(&AclEntry::get_grant_script(item, object_kind, object_name));
    }
    script
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
    fn test_diff_acls_grant_option_change() {
        // FROM: plain SELECT; TO: SELECT with grant option
        let from = vec!["user1=r/owner".to_string()];
        let to = vec!["user1=r*/owner".to_string()];
        let (add, revoke) = diff_acls(&from, &to, true);
        assert_eq!(add, vec!["user1=r*/owner"]);
        assert_eq!(revoke, vec!["user1=r/owner"]);
    }

    #[test]
    fn test_diff_acls_addonly() {
        let from = vec!["user1=r/owner".to_string()];
        let to = vec!["user1=r/owner".to_string(), "user2=rw/owner".to_string()];
        let (add, revoke) = diff_acls(&from, &to, false);
        assert_eq!(add, vec!["user2=rw/owner"]);
        assert!(revoke.is_empty());
    }

    #[test]
    fn test_diff_acls_full() {
        let from = vec!["user1=r/owner".to_string(), "user3=d/owner".to_string()];
        let to = vec!["user1=r/owner".to_string(), "user2=rw/owner".to_string()];
        let (add, revoke) = diff_acls(&from, &to, true);
        assert_eq!(add, vec!["user2=rw/owner"]);
        assert_eq!(revoke, vec!["user3=d/owner"]);
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
}
