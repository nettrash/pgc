use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::utils::string_extensions::StringExt;

/// A PostgreSQL publication (pg_publication).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Publication {
    pub name: String,
    pub owner: String,
    /// True if publication publishes all tables
    pub all_tables: bool,
    /// Published operations: comma-separated (insert, update, delete, truncate)
    pub publish: String,
    /// Tables in the publication (schema.table), empty if all_tables
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tables: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    pub hash: Option<String>,
}

impl Publication {
    pub fn hash(&mut self) {
        let mut hasher = Sha256::new();
        hasher.update(self.name.as_bytes());
        hasher.update(self.owner.as_bytes());
        hasher.update([self.all_tables as u8]);
        hasher.update(self.publish.as_bytes());
        hasher.update((self.tables.len() as u32).to_be_bytes());
        for t in &self.tables {
            hasher.update((t.len() as u32).to_be_bytes());
            hasher.update(t.as_bytes());
        }
        if let Some(c) = &self.comment {
            hasher.update(c.as_bytes());
        }
        self.hash = Some(format!("{:x}", hasher.finalize()));
    }

    pub fn get_script(&self) -> String {
        let for_clause = if self.all_tables {
            " FOR ALL TABLES".to_string()
        } else if !self.tables.is_empty() {
            format!(" FOR TABLE {}", self.tables.join(", "))
        } else {
            String::new()
        };

        let mut script = format!(
            "CREATE PUBLICATION {}{} WITH (publish = '{}');",
            self.name, for_clause, self.publish
        )
        .with_empty_lines();

        if !self.owner.is_empty() {
            script.append_block(&format!(
                "ALTER PUBLICATION {} OWNER TO {};",
                self.name, self.owner
            ));
        }

        if let Some(comment) = &self.comment {
            script.append_block(&format!(
                "COMMENT ON PUBLICATION {} IS '{}';",
                self.name,
                comment.replace('\'', "''")
            ));
        }

        script
    }

    pub fn get_drop_script(&self) -> String {
        format!("DROP PUBLICATION IF EXISTS {};", self.name).with_empty_lines()
    }

    pub fn get_alter_script(&self, target: &Publication, use_drop: bool) -> String {
        let mut script = String::new();

        if self.all_tables != target.all_tables || self.tables != target.tables {
            // Table membership changes need SET TABLE or FOR ALL TABLES
            if target.all_tables {
                // There's no ALTER to switch to FOR ALL TABLES, must drop+recreate
                if use_drop {
                    script = self.get_drop_script();
                    script.push_str(&target.get_script());
                    return script;
                }
                let drop = self.get_drop_script();
                let create = target.get_script();
                let commented_drop = drop
                    .lines()
                    .map(|l| format!("-- {}\n", l))
                    .collect::<String>();
                let commented_create = create
                    .lines()
                    .map(|l| format!("-- {}\n", l))
                    .collect::<String>();
                return format!(
                    "-- use_drop=false: publication {} requires drop+recreate to switch to FOR ALL TABLES; statements commented out\n{}{}",
                    self.name, commented_drop, commented_create
                );
            } else if !target.tables.is_empty() {
                script.append_block(&format!(
                    "ALTER PUBLICATION {} SET TABLE {};",
                    target.name,
                    target.tables.join(", ")
                ));
            }
        }

        if self.publish != target.publish {
            script.append_block(&format!(
                "ALTER PUBLICATION {} SET (publish = '{}');",
                target.name, target.publish
            ));
        }

        if self.owner != target.owner && !target.owner.is_empty() {
            script.append_block(&format!(
                "ALTER PUBLICATION {} OWNER TO {};",
                target.name, target.owner
            ));
        }

        if self.comment != target.comment {
            match &target.comment {
                Some(c) => {
                    script.append_block(&format!(
                        "COMMENT ON PUBLICATION {} IS '{}';",
                        target.name,
                        c.replace('\'', "''")
                    ));
                }
                None if use_drop => {
                    script
                        .append_block(&format!("COMMENT ON PUBLICATION {} IS NULL;", target.name));
                }
                _ => {}
            }
        }

        script
    }
}

/// A PostgreSQL subscription (pg_subscription).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Subscription {
    pub name: String,
    pub owner: String,
    /// Connection string
    pub connection: String,
    /// Publication names to subscribe to
    pub publications: Vec<String>,
    /// Enabled state
    pub enabled: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    pub hash: Option<String>,
}

impl Subscription {
    pub fn hash(&mut self) {
        let mut hasher = Sha256::new();
        hasher.update(self.name.as_bytes());
        hasher.update(self.owner.as_bytes());
        hasher.update(self.connection.as_bytes());
        hasher.update((self.publications.len() as u32).to_be_bytes());
        for p in &self.publications {
            hasher.update((p.len() as u32).to_be_bytes());
            hasher.update(p.as_bytes());
        }
        hasher.update([self.enabled as u8]);
        if let Some(c) = &self.comment {
            hasher.update(c.as_bytes());
        }
        self.hash = Some(format!("{:x}", hasher.finalize()));
    }

    pub fn get_script(&self) -> String {
        let enabled_clause = if self.enabled {
            ""
        } else {
            " WITH (enabled = false)"
        };

        let mut script = format!(
            "CREATE SUBSCRIPTION {} CONNECTION '{}' PUBLICATION {}{};",
            self.name,
            self.connection.replace('\'', "''"),
            self.publications.join(", "),
            enabled_clause
        )
        .with_empty_lines();

        if !self.owner.is_empty() {
            script.append_block(&format!(
                "ALTER SUBSCRIPTION {} OWNER TO {};",
                self.name, self.owner
            ));
        }

        if let Some(comment) = &self.comment {
            script.append_block(&format!(
                "COMMENT ON SUBSCRIPTION {} IS '{}';",
                self.name,
                comment.replace('\'', "''")
            ));
        }

        script
    }

    pub fn get_drop_script(&self) -> String {
        format!("DROP SUBSCRIPTION IF EXISTS {};", self.name).with_empty_lines()
    }

    pub fn get_alter_script(&self, target: &Subscription, use_drop: bool) -> String {
        let mut script = String::new();

        if self.connection != target.connection {
            script.append_block(&format!(
                "ALTER SUBSCRIPTION {} CONNECTION '{}';",
                target.name,
                target.connection.replace('\'', "''")
            ));
        }

        if self.publications != target.publications {
            script.append_block(&format!(
                "ALTER SUBSCRIPTION {} SET PUBLICATION {};",
                target.name,
                target.publications.join(", ")
            ));
        }

        if self.enabled != target.enabled {
            let action = if target.enabled { "ENABLE" } else { "DISABLE" };
            script.append_block(&format!("ALTER SUBSCRIPTION {} {};", target.name, action));
        }

        if self.owner != target.owner && !target.owner.is_empty() {
            script.append_block(&format!(
                "ALTER SUBSCRIPTION {} OWNER TO {};",
                target.name, target.owner
            ));
        }

        if self.comment != target.comment {
            match &target.comment {
                Some(c) => {
                    script.append_block(&format!(
                        "COMMENT ON SUBSCRIPTION {} IS '{}';",
                        target.name,
                        c.replace('\'', "''")
                    ));
                }
                None if use_drop => {
                    script
                        .append_block(&format!("COMMENT ON SUBSCRIPTION {} IS NULL;", target.name));
                }
                _ => {}
            }
        }

        script
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_publication_hash_populates() {
        let mut p = Publication {
            name: "\"my_pub\"".into(),
            owner: "postgres".into(),
            all_tables: false,
            publish: "insert,update,delete".into(),
            tables: vec!["public.orders".into()],
            comment: None,
            hash: None,
        };
        p.hash();
        assert!(p.hash.is_some());
    }

    #[test]
    fn test_publication_get_script() {
        let mut p = Publication {
            name: "\"my_pub\"".into(),
            owner: "postgres".into(),
            all_tables: false,
            publish: "insert,update,delete".into(),
            tables: vec!["public.orders".into()],
            comment: None,
            hash: None,
        };
        p.hash();
        let s = p.get_script();
        assert!(s.contains("CREATE PUBLICATION"));
        assert!(s.contains("FOR TABLE public.orders"));
    }

    #[test]
    fn test_subscription_get_script() {
        let mut s = Subscription {
            name: "\"my_sub\"".into(),
            owner: "postgres".into(),
            connection: "host=primary dbname=mydb".into(),
            publications: vec!["my_pub".into()],
            enabled: true,
            comment: None,
            hash: None,
        };
        s.hash();
        let script = s.get_script();
        assert!(script.contains("CREATE SUBSCRIPTION"));
        assert!(script.contains("my_pub"));
    }

    #[test]
    fn alter_to_all_tables_use_drop_true_drops_and_recreates() {
        let from = Publication {
            name: "my_pub".into(),
            owner: "postgres".into(),
            all_tables: false,
            publish: "insert,update,delete".into(),
            tables: vec!["public.orders".into()],
            comment: None,
            hash: None,
        };
        let to = Publication {
            name: "my_pub".into(),
            owner: "postgres".into(),
            all_tables: true,
            publish: "insert,update,delete".into(),
            tables: vec![],
            comment: None,
            hash: None,
        };
        let script = from.get_alter_script(&to, true);
        assert!(
            script.contains("DROP PUBLICATION IF EXISTS my_pub;"),
            "use_drop=true must DROP, got: {script}"
        );
        assert!(
            script.contains("CREATE PUBLICATION my_pub FOR ALL TABLES"),
            "use_drop=true must CREATE with FOR ALL TABLES, got: {script}"
        );
    }

    #[test]
    fn alter_to_all_tables_use_drop_false_comments_out() {
        let from = Publication {
            name: "my_pub".into(),
            owner: "postgres".into(),
            all_tables: false,
            publish: "insert,update,delete".into(),
            tables: vec!["public.orders".into()],
            comment: None,
            hash: None,
        };
        let to = Publication {
            name: "my_pub".into(),
            owner: "postgres".into(),
            all_tables: true,
            publish: "insert,update,delete".into(),
            tables: vec![],
            comment: None,
            hash: None,
        };
        let script = from.get_alter_script(&to, false);
        assert!(
            script.contains("-- use_drop=false"),
            "must include use_drop=false warning, got: {script}"
        );
        assert!(
            script.contains("-- DROP PUBLICATION IF EXISTS my_pub;"),
            "must have commented-out DROP, got: {script}"
        );
        assert!(
            script.contains("-- CREATE PUBLICATION my_pub FOR ALL TABLES"),
            "must have commented-out CREATE, got: {script}"
        );
        // Must NOT have uncommented DROP or CREATE
        let has_active_drop = script.lines().any(|l| {
            let t = l.trim_start();
            t.starts_with("DROP PUBLICATION") && !t.starts_with("--")
        });
        assert!(!has_active_drop, "must not have active DROP, got: {script}");
    }
}
