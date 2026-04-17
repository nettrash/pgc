use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::utils::string_extensions::StringExt;

/// A PostgreSQL default ACL entry (pg_default_acl).
/// Represents one `ALTER DEFAULT PRIVILEGES ... GRANT ... TO ...` statement.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DefaultPrivilege {
    /// Role whose future objects get these privileges ("" = current role)
    pub role_name: String,
    /// Schema scope ("" = all schemas / database-wide)
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub schema_name: String,
    /// Object type: 'r' = table, 'S' = sequence, 'f' = function/procedure, 'T' = type, 'n' = schema
    pub object_type: String,
    /// ACL entries (same format as pg_class.relacl)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub acl: Vec<String>,
    pub hash: Option<String>,
}

impl DefaultPrivilege {
    pub fn hash(&mut self) {
        let mut hasher = Sha256::new();
        hasher.update(self.role_name.as_bytes());
        hasher.update(self.schema_name.as_bytes());
        hasher.update(self.object_type.as_bytes());
        hasher.update((self.acl.len() as u32).to_be_bytes());
        for entry in &self.acl {
            hasher.update((entry.len() as u32).to_be_bytes());
            hasher.update(entry.as_bytes());
        }
        self.hash = Some(format!("{:x}", hasher.finalize()));
    }

    fn object_type_name(&self) -> &str {
        match self.object_type.as_str() {
            "r" => "TABLES",
            "S" => "SEQUENCES",
            "f" => "FUNCTIONS",
            "T" => "TYPES",
            "n" => "SCHEMAS",
            other => other,
        }
    }

    /// Generate the full ALTER DEFAULT PRIVILEGES script from ACL entries.
    /// Uses a simple diff: revokes everything for grantees no longer present,
    /// grants everything for new/changed entries.
    pub fn get_script(&self) -> String {
        let mut script = String::new();
        let object_type_name = self.object_type_name();
        let for_clause = if self.role_name.is_empty() {
            String::new()
        } else {
            format!("FOR ROLE {} ", self.role_name)
        };
        let in_schema_clause = if self.schema_name.is_empty() {
            String::new()
        } else {
            format!("IN SCHEMA {} ", self.schema_name)
        };

        for entry in &self.acl {
            if let Some(grants) = parse_acl_to_grant(entry, object_type_name) {
                script.append_block(&format!(
                    "ALTER DEFAULT PRIVILEGES {}{}GRANT {} ON {} TO {};",
                    for_clause,
                    in_schema_clause,
                    grants.privileges,
                    object_type_name,
                    grants.grantee
                ));
            }
        }

        script
    }

    pub fn get_revoke_script(&self) -> String {
        let mut script = String::new();
        let object_type_name = self.object_type_name();
        let for_clause = if self.role_name.is_empty() {
            String::new()
        } else {
            format!("FOR ROLE {} ", self.role_name)
        };
        let in_schema_clause = if self.schema_name.is_empty() {
            String::new()
        } else {
            format!("IN SCHEMA {} ", self.schema_name)
        };

        for entry in &self.acl {
            if let Some(grants) = parse_acl_to_grant(entry, object_type_name) {
                script.append_block(&format!(
                    "ALTER DEFAULT PRIVILEGES {}{}REVOKE {} ON {} FROM {};",
                    for_clause,
                    in_schema_clause,
                    grants.privileges,
                    object_type_name,
                    grants.grantee
                ));
            }
        }

        script
    }
}

struct GrantInfo {
    grantee: String,
    privileges: String,
}

/// Parse a PostgreSQL ACL string like "alice=rw/bob" into a GrantInfo.
fn parse_acl_to_grant(acl: &str, object_type: &str) -> Option<GrantInfo> {
    // Format: grantee=privileges/grantor
    let slash_pos = acl.rfind('/')?;
    let grantee_priv = &acl[..slash_pos];
    let eq_pos = grantee_priv.find('=')?;
    let grantee_raw = &grantee_priv[..eq_pos];
    let priv_chars = &grantee_priv[eq_pos + 1..];

    let grantee = if grantee_raw.is_empty() {
        "PUBLIC".to_string()
    } else {
        grantee_raw.to_string()
    };

    let privs = expand_privilege_chars(priv_chars, object_type);
    if privs.is_empty() {
        return None;
    }

    Some(GrantInfo {
        grantee,
        privileges: privs.join(", "),
    })
}

fn expand_privilege_chars(chars: &str, object_type: &str) -> Vec<String> {
    let mut result = Vec::new();
    for c in chars.chars() {
        let priv_name = match (c, object_type) {
            ('r', _) => Some("SELECT"),
            ('a', _) => Some("INSERT"),
            ('w', _) => Some("UPDATE"),
            ('d', _) => Some("DELETE"),
            ('D', _) => Some("TRUNCATE"),
            ('x', _) => Some("REFERENCES"),
            ('t', _) => Some("TRIGGER"),
            ('U', _) => Some("USAGE"),
            ('C', _) => Some("CREATE"),
            ('c', _) => Some("CONNECT"),
            ('T', _) => Some("TEMPORARY"),
            ('X', _) => Some("EXECUTE"),
            _ => None,
        };
        if let Some(p) = priv_name {
            result.push(p.to_string());
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_populates() {
        let mut dp = DefaultPrivilege {
            role_name: "alice".into(),
            schema_name: "public".into(),
            object_type: "r".into(),
            acl: vec!["bob=rw/alice".into()],
            hash: None,
        };
        dp.hash();
        assert!(dp.hash.is_some());
    }

    #[test]
    fn test_get_script_contains_alter_default_privileges() {
        let mut dp = DefaultPrivilege {
            role_name: "alice".into(),
            schema_name: "public".into(),
            object_type: "r".into(),
            acl: vec!["bob=r/alice".into()],
            hash: None,
        };
        dp.hash();
        let s = dp.get_script();
        assert!(s.contains("ALTER DEFAULT PRIVILEGES"));
        assert!(s.contains("FOR ROLE alice"));
        assert!(s.contains("IN SCHEMA public"));
        assert!(s.contains("GRANT SELECT ON TABLES TO bob"));
    }

    #[test]
    fn test_object_type_name() {
        let dp = DefaultPrivilege {
            role_name: "".into(),
            schema_name: "".into(),
            object_type: "f".into(),
            acl: vec![],
            hash: None,
        };
        assert_eq!(dp.object_type_name(), "FUNCTIONS");
    }
}
