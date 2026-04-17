use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::utils::string_extensions::StringExt;

/// A PostgreSQL cast (pg_cast).
/// Casts are global (not schema-scoped).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Cast {
    /// Source type, fully-qualified (e.g. "pg_catalog.int4")
    pub source_type: String,
    /// Target type, fully-qualified
    pub target_type: String,
    /// Cast method: 'f' = function, 'b' = binary-coercible, 'i' = I/O coercion
    pub cast_method: String,
    /// Fully-qualified function name (if method = 'f')
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub function_name: Option<String>,
    /// Cast context: 'e' = explicit only, 'a' = assignment, 'i' = implicit
    pub cast_context: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    pub hash: Option<String>,
}

impl Cast {
    pub fn hash(&mut self) {
        let mut hasher = Sha256::new();
        hasher.update(self.source_type.as_bytes());
        hasher.update(self.target_type.as_bytes());
        hasher.update(self.cast_method.as_bytes());
        if let Some(f) = &self.function_name {
            hasher.update(f.as_bytes());
        }
        hasher.update(self.cast_context.as_bytes());
        if let Some(c) = &self.comment {
            hasher.update(c.as_bytes());
        }
        self.hash = Some(format!("{:x}", hasher.finalize()));
    }

    fn context_clause(&self) -> &str {
        match self.cast_context.as_str() {
            "a" => " AS ASSIGNMENT",
            "i" => " AS IMPLICIT",
            _ => "",
        }
    }

    pub fn get_script(&self) -> String {
        let func_clause = match self.cast_method.as_str() {
            "f" => {
                if let Some(f) = &self.function_name {
                    format!("WITH FUNCTION {}", f)
                } else {
                    "WITHOUT FUNCTION".to_string()
                }
            }
            "b" => "WITHOUT FUNCTION".to_string(),
            _ => "WITH INOUT".to_string(),
        };

        let mut script = format!(
            "CREATE CAST ({} AS {}) {}{};",
            self.source_type,
            self.target_type,
            func_clause,
            self.context_clause()
        )
        .with_empty_lines();

        if let Some(comment) = &self.comment {
            script.append_block(&format!(
                "COMMENT ON CAST ({} AS {}) IS '{}';",
                self.source_type,
                self.target_type,
                comment.replace('\'', "''")
            ));
        }

        script
    }

    pub fn get_drop_script(&self) -> String {
        format!(
            "DROP CAST IF EXISTS ({} AS {}) CASCADE;",
            self.source_type, self.target_type
        )
        .with_empty_lines()
    }

    pub fn get_alter_script(&self, target: &Cast, use_drop: bool) -> String {
        let mut script = String::new();

        let definition_changed = self.cast_method != target.cast_method
            || self.function_name != target.function_name
            || self.cast_context != target.cast_context;

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

        if self.comment != target.comment {
            match &target.comment {
                Some(c) => {
                    script.append_block(&format!(
                        "COMMENT ON CAST ({} AS {}) IS '{}';",
                        target.source_type,
                        target.target_type,
                        c.replace('\'', "''")
                    ));
                }
                None if use_drop => {
                    script.append_block(&format!(
                        "COMMENT ON CAST ({} AS {}) IS NULL;",
                        target.source_type, target.target_type
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

    fn make_cast() -> Cast {
        Cast {
            source_type: "text".into(),
            target_type: "integer".into(),
            cast_method: "f".into(),
            function_name: Some("pg_catalog.int4(text)".into()),
            cast_context: "e".into(),
            comment: None,
            hash: None,
        }
    }

    #[test]
    fn test_hash_populates() {
        let mut c = make_cast();
        c.hash();
        assert!(c.hash.is_some());
    }

    #[test]
    fn test_get_script_with_function() {
        let mut c = make_cast();
        c.hash();
        let s = c.get_script();
        assert!(s.contains("CREATE CAST"));
        assert!(s.contains("WITH FUNCTION"));
        assert!(s.contains("pg_catalog.int4(text)"));
    }

    #[test]
    fn test_get_script_implicit() {
        let mut c = make_cast();
        c.cast_context = "i".into();
        c.hash();
        let s = c.get_script();
        assert!(s.contains("AS IMPLICIT"));
    }

    #[test]
    fn test_get_drop_script() {
        let mut c = make_cast();
        c.hash();
        assert!(c.get_drop_script().contains("DROP CAST"));
    }
}
