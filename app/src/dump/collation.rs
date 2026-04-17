use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::utils::string_extensions::StringExt;

/// Represents a PostgreSQL collation (from pg_collation).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Collation {
    pub schema: String,
    pub name: String,
    pub owner: String,
    /// Collation provider: 'c' = libc, 'i' = icu, 'd' = default
    pub provider: String,
    /// locale (for libc: collcollate and collctype must match, or use lc_collate/lc_ctype)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub locale: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lc_collate: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lc_ctype: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub icu_locale: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub icu_rules: Option<String>,
    pub deterministic: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    pub hash: Option<String>,
}

impl Collation {
    pub fn hash(&mut self) {
        let mut hasher = Sha256::new();
        hasher.update(self.schema.as_bytes());
        hasher.update(self.name.as_bytes());
        hasher.update(self.owner.as_bytes());
        hasher.update(self.provider.as_bytes());
        if let Some(v) = &self.locale {
            hasher.update(b"L");
            hasher.update(v.as_bytes());
        }
        if let Some(v) = &self.lc_collate {
            hasher.update(b"C");
            hasher.update(v.as_bytes());
        }
        if let Some(v) = &self.lc_ctype {
            hasher.update(b"T");
            hasher.update(v.as_bytes());
        }
        if let Some(v) = &self.icu_locale {
            hasher.update(b"I");
            hasher.update(v.as_bytes());
        }
        if let Some(v) = &self.icu_rules {
            hasher.update(b"R");
            hasher.update(v.as_bytes());
        }
        hasher.update([self.deterministic as u8]);
        if let Some(c) = &self.comment {
            hasher.update(c.as_bytes());
        }
        self.hash = Some(format!("{:x}", hasher.finalize()));
    }

    pub fn get_script(&self) -> String {
        let mut opts: Vec<String> = Vec::new();

        let provider_name = match self.provider.as_str() {
            "i" => "icu",
            "d" => "default",
            _ => "libc",
        };
        opts.push(format!("PROVIDER = {}", provider_name));

        if let Some(locale) = &self.locale {
            opts.push(format!("LOCALE = '{}'", locale.replace('\'', "''")));
        } else {
            if let Some(lcc) = &self.lc_collate {
                opts.push(format!("LC_COLLATE = '{}'", lcc.replace('\'', "''")));
            }
            if let Some(lct) = &self.lc_ctype {
                opts.push(format!("LC_CTYPE = '{}'", lct.replace('\'', "''")));
            }
        }
        if let Some(il) = &self.icu_locale {
            opts.push(format!("ICU_LOCALE = '{}'", il.replace('\'', "''")));
        }
        if let Some(ir) = &self.icu_rules {
            opts.push(format!("ICU_RULES = '{}'", ir.replace('\'', "''")));
        }
        if !self.deterministic {
            opts.push("DETERMINISTIC = false".to_string());
        }

        let mut script = format!(
            "CREATE COLLATION {}.{} ({});",
            self.schema,
            self.name,
            opts.join(", ")
        )
        .with_empty_lines();

        if !self.owner.is_empty() {
            script.append_block(&format!(
                "ALTER COLLATION {}.{} OWNER TO {};",
                self.schema, self.name, self.owner
            ));
        }

        if let Some(comment) = &self.comment {
            script.append_block(&format!(
                "COMMENT ON COLLATION {}.{} IS '{}';",
                self.schema,
                self.name,
                comment.replace('\'', "''")
            ));
        }

        script
    }

    pub fn get_drop_script(&self) -> String {
        format!(
            "DROP COLLATION IF EXISTS {}.{} CASCADE;",
            self.schema, self.name
        )
        .with_empty_lines()
    }

    pub fn get_alter_script(&self, target: &Collation, use_drop: bool) -> String {
        let mut script = String::new();

        // Collations can't really be altered (provider/locale are immutable);
        // if anything changed, drop and recreate.
        let definition_changed = self.provider != target.provider
            || self.locale != target.locale
            || self.lc_collate != target.lc_collate
            || self.lc_ctype != target.lc_ctype
            || self.icu_locale != target.icu_locale
            || self.icu_rules != target.icu_rules
            || self.deterministic != target.deterministic;

        if definition_changed {
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

        if self.owner != target.owner && !target.owner.is_empty() {
            script.append_block(&format!(
                "ALTER COLLATION {}.{} OWNER TO {};",
                target.schema, target.name, target.owner
            ));
        }

        if self.comment != target.comment {
            match &target.comment {
                Some(c) => {
                    script.append_block(&format!(
                        "COMMENT ON COLLATION {}.{} IS '{}';",
                        target.schema,
                        target.name,
                        c.replace('\'', "''")
                    ));
                }
                None if use_drop => {
                    script.append_block(&format!(
                        "COMMENT ON COLLATION {}.{} IS NULL;",
                        target.schema, target.name
                    ));
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

    fn make_collation() -> Collation {
        Collation {
            schema: "public".into(),
            name: "\"my_coll\"".into(),
            owner: "postgres".into(),
            provider: "i".into(),
            locale: None,
            lc_collate: None,
            lc_ctype: None,
            icu_locale: Some("en-US".into()),
            icu_rules: None,
            deterministic: true,
            comment: None,
            hash: None,
        }
    }

    #[test]
    fn test_hash_populates() {
        let mut c = make_collation();
        c.hash();
        assert!(c.hash.is_some());
    }

    #[test]
    fn test_hash_changes_with_icu_locale() {
        let mut c = make_collation();
        c.hash();
        let h1 = c.hash.clone();
        c.icu_locale = Some("fr-FR".into());
        c.hash();
        assert_ne!(h1, c.hash);
    }

    #[test]
    fn test_get_script_contains_create_collation() {
        let mut c = make_collation();
        c.hash();
        let s = c.get_script();
        assert!(s.contains("CREATE COLLATION"));
        assert!(s.contains("en-US"));
    }

    #[test]
    fn test_get_drop_script() {
        let mut c = make_collation();
        c.hash();
        assert!(c.get_drop_script().contains("DROP COLLATION"));
    }
}
