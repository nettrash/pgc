use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::utils::string_extensions::StringExt;

/// Parse a `key=value` option string into `(key, value)`. The catalog stores
/// options in `key=value` form; PostgreSQL DDL requires `key 'value'`.
fn parse_option(opt: &str) -> (&str, &str) {
    if let Some(pos) = opt.find('=') {
        (&opt[..pos], &opt[pos + 1..])
    } else {
        (opt, "")
    }
}

/// Format a list of catalog options as `DROP key, DROP key, ...`
fn format_drop_options(options: &[String]) -> String {
    options
        .iter()
        .map(|o| format!("DROP {}", parse_option(o).0))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Format a list of catalog options as `ADD key 'value', ADD key 'value', ...`
fn format_add_options(options: &[String]) -> String {
    options
        .iter()
        .map(|o| {
            let (k, v) = parse_option(o);
            format!("ADD {} '{}'", k, v.replace('\'', "''"))
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// Format a list of catalog options as `key 'value', key 'value', ...` (for CREATE)
fn format_options(options: &[String]) -> String {
    options
        .iter()
        .map(|o| {
            let (k, v) = parse_option(o);
            format!("{} '{}'", k, v.replace('\'', "''"))
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// A PostgreSQL foreign-data wrapper (pg_foreign_data_wrapper).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForeignDataWrapper {
    pub name: String,
    pub owner: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub handler_func: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub validator_func: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub options: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    pub hash: Option<String>,
}

impl ForeignDataWrapper {
    pub fn hash(&mut self) {
        let mut hasher = Sha256::new();
        hasher.update(self.name.as_bytes());
        hasher.update(self.owner.as_bytes());
        if let Some(h) = &self.handler_func {
            hasher.update(h.as_bytes());
        }
        if let Some(v) = &self.validator_func {
            hasher.update(v.as_bytes());
        }
        hasher.update((self.options.len() as u32).to_be_bytes());
        for o in &self.options {
            hasher.update((o.len() as u32).to_be_bytes());
            hasher.update(o.as_bytes());
        }
        if let Some(c) = &self.comment {
            hasher.update(c.as_bytes());
        }
        self.hash = Some(format!("{:x}", hasher.finalize()));
    }

    pub fn get_script(&self) -> String {
        let mut parts: Vec<String> = Vec::new();
        if let Some(h) = &self.handler_func {
            parts.push(format!("HANDLER {}", h));
        } else {
            parts.push("NO HANDLER".to_string());
        }
        if let Some(v) = &self.validator_func {
            parts.push(format!("VALIDATOR {}", v));
        } else {
            parts.push("NO VALIDATOR".to_string());
        }
        if !self.options.is_empty() {
            parts.push(format!("OPTIONS ({})", format_options(&self.options)));
        }

        let mut script = format!(
            "CREATE FOREIGN DATA WRAPPER {} {};",
            self.name,
            parts.join(" ")
        )
        .with_empty_lines();

        if !self.owner.is_empty() {
            script.append_block(&format!(
                "ALTER FOREIGN DATA WRAPPER {} OWNER TO {};",
                self.name, self.owner
            ));
        }

        if let Some(comment) = &self.comment {
            script.append_block(&format!(
                "COMMENT ON FOREIGN DATA WRAPPER {} IS '{}';",
                self.name,
                comment.replace('\'', "''")
            ));
        }

        script
    }

    pub fn get_drop_script(&self) -> String {
        format!("DROP FOREIGN DATA WRAPPER IF EXISTS {} CASCADE;", self.name).with_empty_lines()
    }

    pub fn get_alter_script(&self, target: &ForeignDataWrapper, use_drop: bool) -> String {
        let mut script = String::new();

        let definition_changed = self.handler_func != target.handler_func
            || self.validator_func != target.validator_func
            || self.options != target.options;

        if definition_changed {
            let mut parts: Vec<String> = Vec::new();
            if let Some(h) = &target.handler_func {
                parts.push(format!("HANDLER {}", h));
            } else if self.handler_func.is_some() {
                parts.push("NO HANDLER".to_string());
            }
            if let Some(v) = &target.validator_func {
                parts.push(format!("VALIDATOR {}", v));
            } else if self.validator_func.is_some() {
                parts.push("NO VALIDATOR".to_string());
            }
            if !parts.is_empty() {
                script.append_block(&format!(
                    "ALTER FOREIGN DATA WRAPPER {} {};",
                    target.name,
                    parts.join(" ")
                ));
            }
            if self.options != target.options {
                // Drop old options and add new ones
                if !self.options.is_empty() {
                    script.append_block(&format!(
                        "ALTER FOREIGN DATA WRAPPER {} OPTIONS ({});",
                        target.name,
                        format_drop_options(&self.options)
                    ));
                }
                if !target.options.is_empty() {
                    script.append_block(&format!(
                        "ALTER FOREIGN DATA WRAPPER {} OPTIONS ({});",
                        target.name,
                        format_add_options(&target.options)
                    ));
                }
            }
        }

        if self.owner != target.owner && !target.owner.is_empty() {
            script.append_block(&format!(
                "ALTER FOREIGN DATA WRAPPER {} OWNER TO {};",
                target.name, target.owner
            ));
        }

        if self.comment != target.comment {
            match &target.comment {
                Some(c) => {
                    script.append_block(&format!(
                        "COMMENT ON FOREIGN DATA WRAPPER {} IS '{}';",
                        target.name,
                        c.replace('\'', "''")
                    ));
                }
                None if use_drop => {
                    script.append_block(&format!(
                        "COMMENT ON FOREIGN DATA WRAPPER {} IS NULL;",
                        target.name
                    ));
                }
                _ => {}
            }
        }

        script
    }
}

/// A PostgreSQL foreign server (pg_foreign_server).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForeignServer {
    pub name: String,
    pub owner: String,
    pub fdw_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server_version: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub options: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    pub hash: Option<String>,
}

impl ForeignServer {
    pub fn hash(&mut self) {
        let mut hasher = Sha256::new();
        hasher.update(self.name.as_bytes());
        hasher.update(self.owner.as_bytes());
        hasher.update(self.fdw_name.as_bytes());
        if let Some(v) = &self.server_type {
            hasher.update(v.as_bytes());
        }
        if let Some(v) = &self.server_version {
            hasher.update(v.as_bytes());
        }
        hasher.update((self.options.len() as u32).to_be_bytes());
        for o in &self.options {
            hasher.update((o.len() as u32).to_be_bytes());
            hasher.update(o.as_bytes());
        }
        if let Some(c) = &self.comment {
            hasher.update(c.as_bytes());
        }
        self.hash = Some(format!("{:x}", hasher.finalize()));
    }

    pub fn get_script(&self) -> String {
        let type_clause = self
            .server_type
            .as_ref()
            .map(|t| format!(" TYPE '{}'", t.replace('\'', "''")))
            .unwrap_or_default();
        let version_clause = self
            .server_version
            .as_ref()
            .map(|v| format!(" VERSION '{}'", v.replace('\'', "''")))
            .unwrap_or_default();
        let options_clause = if self.options.is_empty() {
            String::new()
        } else {
            format!(" OPTIONS ({})", format_options(&self.options))
        };

        let mut script = format!(
            "CREATE SERVER {}{}{} FOREIGN DATA WRAPPER {}{};",
            self.name, type_clause, version_clause, self.fdw_name, options_clause
        )
        .with_empty_lines();

        if !self.owner.is_empty() {
            script.append_block(&format!(
                "ALTER SERVER {} OWNER TO {};",
                self.name, self.owner
            ));
        }

        if let Some(comment) = &self.comment {
            script.append_block(&format!(
                "COMMENT ON SERVER {} IS '{}';",
                self.name,
                comment.replace('\'', "''")
            ));
        }

        script
    }

    pub fn get_drop_script(&self) -> String {
        format!("DROP SERVER IF EXISTS {} CASCADE;", self.name).with_empty_lines()
    }

    pub fn get_alter_script(&self, target: &ForeignServer, use_drop: bool) -> String {
        let mut script = String::new();

        let definition_changed = self.fdw_name != target.fdw_name;

        if definition_changed {
            // FDW name can't be changed; drop+recreate
            if use_drop {
                script.append_block(&self.get_drop_script());
            } else {
                let drop = self.get_drop_script();
                script.push_str(
                    &drop
                        .lines()
                        .map(|l| format!("-- {}\n", l))
                        .collect::<String>(),
                );
            }
            script.push_str(&target.get_script());
            return script;
        }

        if self.server_version != target.server_version {
            let ver_clause = target
                .server_version
                .as_ref()
                .map(|v| format!("VERSION '{}'", v.replace('\'', "''")))
                .unwrap_or_else(|| "NO VERSION".to_string());
            script.append_block(&format!("ALTER SERVER {} {};", target.name, ver_clause));
        }

        if self.options != target.options {
            if !self.options.is_empty() {
                script.append_block(&format!(
                    "ALTER SERVER {} OPTIONS ({});",
                    target.name,
                    format_drop_options(&self.options)
                ));
            }
            if !target.options.is_empty() {
                script.append_block(&format!(
                    "ALTER SERVER {} OPTIONS ({});",
                    target.name,
                    format_add_options(&target.options)
                ));
            }
        }

        if self.owner != target.owner && !target.owner.is_empty() {
            script.append_block(&format!(
                "ALTER SERVER {} OWNER TO {};",
                target.name, target.owner
            ));
        }

        if self.comment != target.comment {
            match &target.comment {
                Some(c) => {
                    script.append_block(&format!(
                        "COMMENT ON SERVER {} IS '{}';",
                        target.name,
                        c.replace('\'', "''")
                    ));
                }
                None if use_drop => {
                    script.append_block(&format!("COMMENT ON SERVER {} IS NULL;", target.name));
                }
                _ => {}
            }
        }

        script
    }
}

/// A PostgreSQL user mapping (pg_user_mapping).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserMapping {
    pub server_name: String,
    /// Username, or "PUBLIC" for the PUBLIC mapping
    pub username: String,
    /// Options (may contain credentials — only included if non-sensitive)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub options: Vec<String>,
    pub hash: Option<String>,
}

impl UserMapping {
    pub fn hash(&mut self) {
        let mut hasher = Sha256::new();
        hasher.update(self.server_name.as_bytes());
        hasher.update(self.username.as_bytes());
        hasher.update((self.options.len() as u32).to_be_bytes());
        for o in &self.options {
            hasher.update((o.len() as u32).to_be_bytes());
            hasher.update(o.as_bytes());
        }
        self.hash = Some(format!("{:x}", hasher.finalize()));
    }

    pub fn get_script(&self) -> String {
        let options_clause = if self.options.is_empty() {
            String::new()
        } else {
            format!(" OPTIONS ({})", format_options(&self.options))
        };

        format!(
            "CREATE USER MAPPING FOR {} SERVER {}{};",
            self.username, self.server_name, options_clause
        )
        .with_empty_lines()
    }

    pub fn get_drop_script(&self) -> String {
        format!(
            "DROP USER MAPPING IF EXISTS FOR {} SERVER {};",
            self.username, self.server_name
        )
        .with_empty_lines()
    }

    pub fn get_alter_script(&self, target: &UserMapping) -> String {
        let mut script = String::new();

        if self.options != target.options {
            if !self.options.is_empty() {
                script.append_block(&format!(
                    "ALTER USER MAPPING FOR {} SERVER {} OPTIONS ({});",
                    target.username,
                    target.server_name,
                    format_drop_options(&self.options)
                ));
            }
            if !target.options.is_empty() {
                script.append_block(&format!(
                    "ALTER USER MAPPING FOR {} SERVER {} OPTIONS ({});",
                    target.username,
                    target.server_name,
                    format_add_options(&target.options)
                ));
            }
        }

        script
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fdw_hash_populates() {
        let mut f = ForeignDataWrapper {
            name: "\"myfdw\"".into(),
            owner: "postgres".into(),
            handler_func: Some("my_handler".into()),
            validator_func: None,
            options: vec![],
            comment: None,
            hash: None,
        };
        f.hash();
        assert!(f.hash.is_some());
    }

    #[test]
    fn test_fdw_get_script() {
        let mut f = ForeignDataWrapper {
            name: "\"myfdw\"".into(),
            owner: "postgres".into(),
            handler_func: Some("my_handler".into()),
            validator_func: None,
            options: vec![],
            comment: None,
            hash: None,
        };
        f.hash();
        let s = f.get_script();
        assert!(s.contains("CREATE FOREIGN DATA WRAPPER"));
        assert!(s.contains("HANDLER my_handler"));
    }

    #[test]
    fn test_server_get_script() {
        let mut s = ForeignServer {
            name: "\"myserver\"".into(),
            owner: "postgres".into(),
            fdw_name: "myfdw".into(),
            server_type: None,
            server_version: Some("1.0".into()),
            options: vec!["host 'localhost'".into()],
            comment: None,
            hash: None,
        };
        s.hash();
        let script = s.get_script();
        assert!(script.contains("CREATE SERVER"));
        assert!(script.contains("FOREIGN DATA WRAPPER myfdw"));
    }

    #[test]
    fn test_user_mapping_get_script() {
        let mut m = UserMapping {
            server_name: "myserver".into(),
            username: "alice".into(),
            options: vec!["user 'alice_remote'".into()],
            hash: None,
        };
        m.hash();
        let s = m.get_script();
        assert!(s.contains("CREATE USER MAPPING FOR alice SERVER myserver"));
    }
}
