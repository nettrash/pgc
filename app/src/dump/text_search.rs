use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::utils::string_extensions::StringExt;

/// A PostgreSQL text search configuration (pg_ts_config).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TextSearchConfig {
    pub schema: String,
    pub name: String,
    pub owner: String,
    /// Parser: fully-qualified name, e.g. "pg_catalog.default"
    pub parser: String,
    /// Mappings: list of "token_type:dict1,dict2" strings
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub mappings: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    pub hash: Option<String>,
}

impl TextSearchConfig {
    pub fn hash(&mut self) {
        let mut hasher = Sha256::new();
        hasher.update(self.schema.as_bytes());
        hasher.update(self.name.as_bytes());
        hasher.update(self.owner.as_bytes());
        hasher.update(self.parser.as_bytes());
        hasher.update((self.mappings.len() as u32).to_be_bytes());
        for m in &self.mappings {
            hasher.update((m.len() as u32).to_be_bytes());
            hasher.update(m.as_bytes());
        }
        if let Some(c) = &self.comment {
            hasher.update(c.as_bytes());
        }
        self.hash = Some(format!("{:x}", hasher.finalize()));
    }

    pub fn get_script(&self) -> String {
        let mut script = format!(
            "CREATE TEXT SEARCH CONFIGURATION {}.{} (PARSER = {});",
            self.schema, self.name, self.parser
        )
        .with_empty_lines();

        for mapping in &self.mappings {
            // mapping format: "token_type:dict1,dict2"
            if let Some((token_type, dicts)) = mapping.split_once(':') {
                script.append_block(&format!(
                    "ALTER TEXT SEARCH CONFIGURATION {}.{} ADD MAPPING FOR {} WITH {};",
                    self.schema, self.name, token_type, dicts
                ));
            }
        }

        if !self.owner.is_empty() {
            script.append_block(&format!(
                "ALTER TEXT SEARCH CONFIGURATION {}.{} OWNER TO {};",
                self.schema, self.name, self.owner
            ));
        }

        if let Some(comment) = &self.comment {
            script.append_block(&format!(
                "COMMENT ON TEXT SEARCH CONFIGURATION {}.{} IS '{}';",
                self.schema,
                self.name,
                comment.replace('\'', "''")
            ));
        }

        script
    }

    pub fn get_drop_script(&self) -> String {
        format!(
            "DROP TEXT SEARCH CONFIGURATION IF EXISTS {}.{} CASCADE;",
            self.schema, self.name
        )
        .with_empty_lines()
    }

    pub fn get_alter_script(&self, target: &TextSearchConfig, use_drop: bool) -> String {
        let mut script = String::new();

        let parser_changed = self.parser != target.parser;
        let mappings_changed = self.mappings != target.mappings;

        if parser_changed {
            // Parser can't be changed with ALTER, must drop and recreate
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

        if mappings_changed {
            // Remove all old mappings and add new ones
            script.append_block(&format!(
                "ALTER TEXT SEARCH CONFIGURATION {}.{} DROP MAPPING IF EXISTS FOR {};",
                target.schema,
                target.name,
                self.mappings
                    .iter()
                    .filter_map(|m| m.split_once(':').map(|(t, _)| t.to_string()))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
            for mapping in &target.mappings {
                if let Some((token_type, dicts)) = mapping.split_once(':') {
                    script.append_block(&format!(
                        "ALTER TEXT SEARCH CONFIGURATION {}.{} ADD MAPPING FOR {} WITH {};",
                        target.schema, target.name, token_type, dicts
                    ));
                }
            }
        }

        if self.owner != target.owner && !target.owner.is_empty() {
            script.append_block(&format!(
                "ALTER TEXT SEARCH CONFIGURATION {}.{} OWNER TO {};",
                target.schema, target.name, target.owner
            ));
        }

        if self.comment != target.comment {
            match &target.comment {
                Some(c) => {
                    script.append_block(&format!(
                        "COMMENT ON TEXT SEARCH CONFIGURATION {}.{} IS '{}';",
                        target.schema,
                        target.name,
                        c.replace('\'', "''")
                    ));
                }
                None if use_drop => {
                    script.append_block(&format!(
                        "COMMENT ON TEXT SEARCH CONFIGURATION {}.{} IS NULL;",
                        target.schema, target.name
                    ));
                }
                _ => {}
            }
        }

        script
    }
}

/// A PostgreSQL text search dictionary (pg_ts_dict).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TextSearchDict {
    pub schema: String,
    pub name: String,
    pub owner: String,
    /// Template: fully-qualified name, e.g. "pg_catalog.simple"
    pub template: String,
    /// Options as "key=value" strings
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub options: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    pub hash: Option<String>,
}

impl TextSearchDict {
    pub fn hash(&mut self) {
        let mut hasher = Sha256::new();
        hasher.update(self.schema.as_bytes());
        hasher.update(self.name.as_bytes());
        hasher.update(self.owner.as_bytes());
        hasher.update(self.template.as_bytes());
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
        let mut parts = vec![format!("TEMPLATE = {}", self.template)];
        parts.extend(self.options.iter().cloned());

        let mut script = format!(
            "CREATE TEXT SEARCH DICTIONARY {}.{} ({});",
            self.schema,
            self.name,
            parts.join(", ")
        )
        .with_empty_lines();

        if !self.owner.is_empty() {
            script.append_block(&format!(
                "ALTER TEXT SEARCH DICTIONARY {}.{} OWNER TO {};",
                self.schema, self.name, self.owner
            ));
        }

        if let Some(comment) = &self.comment {
            script.append_block(&format!(
                "COMMENT ON TEXT SEARCH DICTIONARY {}.{} IS '{}';",
                self.schema,
                self.name,
                comment.replace('\'', "''")
            ));
        }

        script
    }

    pub fn get_drop_script(&self) -> String {
        format!(
            "DROP TEXT SEARCH DICTIONARY IF EXISTS {}.{} CASCADE;",
            self.schema, self.name
        )
        .with_empty_lines()
    }

    pub fn get_alter_script(&self, target: &TextSearchDict, use_drop: bool) -> String {
        let mut script = String::new();

        let definition_changed = self.template != target.template || self.options != target.options;

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
                "ALTER TEXT SEARCH DICTIONARY {}.{} OWNER TO {};",
                target.schema, target.name, target.owner
            ));
        }

        if self.comment != target.comment {
            match &target.comment {
                Some(c) => {
                    script.append_block(&format!(
                        "COMMENT ON TEXT SEARCH DICTIONARY {}.{} IS '{}';",
                        target.schema,
                        target.name,
                        c.replace('\'', "''")
                    ));
                }
                None if use_drop => {
                    script.append_block(&format!(
                        "COMMENT ON TEXT SEARCH DICTIONARY {}.{} IS NULL;",
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

    #[test]
    fn test_ts_config_hash() {
        let mut c = TextSearchConfig {
            schema: "public".into(),
            name: "\"my_config\"".into(),
            owner: "postgres".into(),
            parser: "pg_catalog.default".into(),
            mappings: vec!["word:english_stem".into()],
            comment: None,
            hash: None,
        };
        c.hash();
        assert!(c.hash.is_some());
    }

    #[test]
    fn test_ts_config_get_script() {
        let mut c = TextSearchConfig {
            schema: "public".into(),
            name: "\"my_config\"".into(),
            owner: "postgres".into(),
            parser: "pg_catalog.default".into(),
            mappings: vec!["word:english_stem".into()],
            comment: None,
            hash: None,
        };
        c.hash();
        let s = c.get_script();
        assert!(s.contains("CREATE TEXT SEARCH CONFIGURATION"));
        assert!(s.contains("ADD MAPPING FOR word WITH english_stem"));
    }

    #[test]
    fn test_ts_dict_get_script() {
        let mut d = TextSearchDict {
            schema: "public".into(),
            name: "\"my_dict\"".into(),
            owner: "postgres".into(),
            template: "pg_catalog.simple".into(),
            options: vec!["STOPWORDS = english".into()],
            comment: None,
            hash: None,
        };
        d.hash();
        let s = d.get_script();
        assert!(s.contains("CREATE TEXT SEARCH DICTIONARY"));
        assert!(s.contains("STOPWORDS = english"));
    }
}
