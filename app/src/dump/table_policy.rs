use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::{Error, Row, postgres::PgRow};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TablePolicy {
    pub schema: String,   // Schema name
    pub table: String,    // Table name
    pub name: String,     // Policy name
    pub command: String,  // ALL, SELECT, INSERT, UPDATE, DELETE
    pub permissive: bool, // true = PERMISSIVE, false = RESTRICTIVE
    #[serde(default)]
    pub roles: Vec<String>, // Roles the policy applies to; empty means PUBLIC
    pub using_clause: Option<String>, // USING (predicate)
    pub check_clause: Option<String>, // WITH CHECK (predicate)
}

impl TablePolicy {
    #[allow(clippy::too_many_arguments)] // Helper used for construction/mapping in tests and from_row
    pub fn from_parts(
        schema: String,
        table: String,
        name: String,
        polcmd: &str,
        permissive: bool,
        roles: Vec<String>,
        using_clause: Option<String>,
        check_clause: Option<String>,
    ) -> Self {
        let command = match polcmd {
            "r" => "select".to_string(),
            "a" => "insert".to_string(),
            "w" => "update".to_string(),
            "d" => "delete".to_string(),
            _ => "all".to_string(),
        };

        let mut sorted_roles = roles;
        sorted_roles.sort_unstable();

        Self {
            schema,
            table,
            name,
            command,
            permissive,
            roles: sorted_roles,
            using_clause,
            check_clause,
        }
    }

    pub fn from_row(row: &PgRow) -> Result<Self, Error> {
        Ok(Self::from_parts(
            row.get("schemaname"),
            row.get("tablename"),
            row.get("polname"),
            row.get::<String, _>("polcmd").as_str(),
            row.get::<bool, _>("polpermissive"),
            row.get::<Option<Vec<String>>, _>("roles")
                .unwrap_or_default(),
            row.get("using_clause"),
            row.get("check_clause"),
        ))
    }

    pub fn add_to_hasher(&self, hasher: &mut Sha256) {
        hasher.update(self.schema.as_bytes());
        hasher.update(self.table.as_bytes());
        hasher.update(self.name.as_bytes());
        hasher.update(self.command.as_bytes());
        hasher.update(self.permissive.to_string().as_bytes());
        for role in &self.roles {
            hasher.update(role.as_bytes());
        }
        if let Some(using_clause) = &self.using_clause {
            hasher.update(using_clause.as_bytes());
        }
        if let Some(check_clause) = &self.check_clause {
            hasher.update(check_clause.as_bytes());
        }
    }

    pub fn get_script(&self) -> String {
        let mut script = String::new();
        let escaped_name = self.name.replace('"', "\"\"");
        let escaped_schema = self.schema.replace('"', "\"\"");
        let escaped_table = self.table.replace('"', "\"\"");
        script.push_str(&format!(
            "create policy \"{}\" on \"{}\".\"{}\"",
            escaped_name, escaped_schema, escaped_table
        ));

        if !self.permissive {
            script.push_str(" as restrictive");
        }

        script.push_str(&format!(" for {}", self.command));

        let role_clause = if self.roles.is_empty() {
            "public".to_string()
        } else {
            self.roles
                .iter()
                .map(|r| format!("\"{}\"", r.replace('"', "\"\"")))
                .collect::<Vec<_>>()
                .join(", ")
        };
        script.push_str(&format!(" to {}", role_clause));

        if let Some(using_clause) = &self.using_clause {
            let trimmed = using_clause.trim();
            if trimmed.starts_with('(') && trimmed.ends_with(')') {
                script.push_str(&format!(" using {}", trimmed));
            } else {
                script.push_str(&format!(" using ({})", trimmed));
            }
        }

        if let Some(check_clause) = &self.check_clause {
            let trimmed = check_clause.trim();
            if trimmed.starts_with('(') && trimmed.ends_with(')') {
                script.push_str(&format!(" with check {}", trimmed));
            } else {
                script.push_str(&format!(" with check ({})", trimmed));
            }
        }

        script.push_str(";\n");
        script
    }
}

impl PartialEq for TablePolicy {
    fn eq(&self, other: &Self) -> bool {
        self.schema == other.schema
            && self.table == other.table
            && self.name == other.name
            && self.command == other.command
            && self.permissive == other.permissive
            && self.roles == other.roles
            && self.using_clause == other.using_clause
            && self.check_clause == other.check_clause
    }
}

#[cfg(test)]
mod tests {
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
            using_clause: Some(
                "(tenant_id = current_setting('app.current_tenant')::int)".to_string(),
            ),
            check_clause: None,
        }
    }

    #[test]
    fn test_get_script() {
        let script = sample_policy().get_script();
        assert!(script.contains("create policy \"p_users_select\""));
        assert!(script.contains("for select"));
        assert!(script.contains("to \"analyst\", \"auditor\""));
        assert!(script.contains("using (tenant_id = current_setting"));
        assert!(script.ends_with(";\n"));
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
        assert!(script.ends_with(";\n"));
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
}
