/// Represents a single parsed PostgreSQL ACL entry.
///
/// PostgreSQL ACL items have the format: `grantee=privileges/grantor`
/// where privileges are single-character codes:
///   r = SELECT, a = INSERT, w = UPDATE, d = DELETE, D = TRUNCATE,
///   x = REFERENCES, t = TRIGGER, X = EXECUTE, U = USAGE, C = CREATE,
///   c = CONNECT, T = TEMPORARY
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AclEntry {
    pub grantee: String,
    pub privileges: String,
    pub grantor: String,
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

    /// Returns the SQL object type keyword for GRANT/REVOKE based on the privilege characters.
    fn privilege_descriptions(privs: &str, object_kind: &str) -> Vec<String> {
        let mut result = Vec::new();
        for ch in privs.chars() {
            let desc = match ch {
                'r' => "SELECT",
                'a' => "INSERT",
                'w' => "UPDATE",
                'd' => "DELETE",
                'D' => "TRUNCATE",
                'x' => "REFERENCES",
                't' => "TRIGGER",
                'X' => "EXECUTE",
                'U' => "USAGE",
                'C' => "CREATE",
                'c' => "CONNECT",
                'T' => "TEMPORARY",
                '*' => continue, // grant option marker, skip
                _ => continue,
            };
            result.push(desc.to_string());
        }
        // Filter to only valid privileges for this object kind
        let valid: &[&str] = match object_kind {
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
        };
        result.retain(|p| valid.contains(&p.as_str()));
        result
    }

    fn format_grantee(grantee: &str) -> String {
        if grantee.is_empty() {
            "PUBLIC".to_string()
        } else {
            grantee.to_string()
        }
    }

    /// Generate GRANT statement for this ACL entry on the given object.
    pub fn get_grant_script(acl_item: &str, object_kind: &str, object_name: &str) -> String {
        let entry = match AclEntry::parse(acl_item) {
            Some(e) => e,
            None => return String::new(),
        };
        let privs = Self::privilege_descriptions(&entry.privileges, object_kind);
        if privs.is_empty() {
            return String::new();
        }
        let on_keyword = match object_kind {
            "FUNCTION" | "PROCEDURE" => object_kind.to_string(),
            _ => object_kind.to_string(),
        };
        format!(
            "GRANT {} ON {} {} TO {};\n",
            privs.join(", "),
            on_keyword,
            object_name,
            Self::format_grantee(&entry.grantee)
        )
    }

    /// Generate REVOKE statement for this ACL entry on the given object.
    pub fn get_revoke_script(acl_item: &str, object_kind: &str, object_name: &str) -> String {
        let entry = match AclEntry::parse(acl_item) {
            Some(e) => e,
            None => return String::new(),
        };
        let privs = Self::privilege_descriptions(&entry.privileges, object_kind);
        if privs.is_empty() {
            return String::new();
        }
        let on_keyword = match object_kind {
            "FUNCTION" | "PROCEDURE" => object_kind.to_string(),
            _ => object_kind.to_string(),
        };
        format!(
            "REVOKE {} ON {} {} FROM {};\n",
            privs.join(", "),
            on_keyword,
            object_name,
            Self::format_grantee(&entry.grantee)
        )
    }
}

/// Compute grants to add and optionally revoke between two ACL lists.
///
/// Returns `(grants_to_add, grants_to_revoke)` where each is a list of raw ACL item strings.
/// `grants_to_revoke` is empty when not in full mode.
pub fn diff_acls(from_acl: &[String], to_acl: &[String], full: bool) -> (Vec<String>, Vec<String>) {
    let mut to_add = Vec::new();
    let mut to_revoke = Vec::new();

    // ACL items to add: present in "to" but not in "from"
    for item in to_acl {
        if !from_acl.contains(item) {
            to_add.push(item.clone());
        }
    }

    // ACL items to revoke (only in full mode): present in "from" but not in "to"
    if full {
        for item in from_acl {
            if !to_acl.contains(item) {
                to_revoke.push(item.clone());
            }
        }
    }

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
    fn test_revoke_script() {
        let script = AclEntry::get_revoke_script("myuser=r/owner", "TABLE", "public.my_table");
        assert_eq!(
            script,
            "REVOKE SELECT ON TABLE public.my_table FROM myuser;\n"
        );
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
}
