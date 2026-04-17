use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::utils::string_extensions::StringExt;

/// A PostgreSQL operator (pg_operator).
/// Operators are schema-scoped.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Operator {
    pub schema: String,
    pub name: String,
    pub owner: String,
    /// Left operand type (NULL for prefix/right-unary)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub left_type: Option<String>,
    /// Right operand type (NULL for postfix/left-unary, but postfix is removed in PG14+)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub right_type: Option<String>,
    /// Result type
    pub result_type: String,
    /// Implementing function (fully qualified)
    pub procedure: String,
    /// Commutator operator (optional)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commutator: Option<String>,
    /// Negator operator (optional)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub negator: Option<String>,
    /// Restriction selectivity function
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restrict: Option<String>,
    /// Join selectivity function
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub join: Option<String>,
    pub is_hashes: bool,
    pub is_merges: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    pub hash: Option<String>,
}

impl Operator {
    pub fn hash(&mut self) {
        let mut hasher = Sha256::new();
        hasher.update(self.schema.as_bytes());
        hasher.update(self.name.as_bytes());
        hasher.update(self.owner.as_bytes());
        if let Some(v) = &self.left_type {
            hasher.update(v.as_bytes());
        }
        if let Some(v) = &self.right_type {
            hasher.update(v.as_bytes());
        }
        hasher.update(self.result_type.as_bytes());
        hasher.update(self.procedure.as_bytes());
        if let Some(v) = &self.commutator {
            hasher.update(v.as_bytes());
        }
        if let Some(v) = &self.negator {
            hasher.update(v.as_bytes());
        }
        if let Some(v) = &self.restrict {
            hasher.update(v.as_bytes());
        }
        if let Some(v) = &self.join {
            hasher.update(v.as_bytes());
        }
        hasher.update([self.is_hashes as u8, self.is_merges as u8]);
        if let Some(c) = &self.comment {
            hasher.update(c.as_bytes());
        }
        self.hash = Some(format!("{:x}", hasher.finalize()));
    }

    fn operand_signature(&self) -> String {
        match (&self.left_type, &self.right_type) {
            (Some(l), Some(r)) => format!("{}, {}", l, r),
            (Some(l), None) => format!("{}, NONE", l),
            (None, Some(r)) => format!("NONE, {}", r),
            (None, None) => "NONE, NONE".to_string(),
        }
    }

    pub fn get_script(&self) -> String {
        let mut opts: Vec<String> = vec![
            format!("PROCEDURE = {}", self.procedure),
            format!("LEFTARG = {}", self.left_type.as_deref().unwrap_or("NONE")),
            format!(
                "RIGHTARG = {}",
                self.right_type.as_deref().unwrap_or("NONE")
            ),
        ];
        if let Some(c) = &self.commutator {
            opts.push(format!("COMMUTATOR = {}", c));
        }
        if let Some(n) = &self.negator {
            opts.push(format!("NEGATOR = {}", n));
        }
        if let Some(r) = &self.restrict {
            opts.push(format!("RESTRICT = {}", r));
        }
        if let Some(j) = &self.join {
            opts.push(format!("JOIN = {}", j));
        }
        if self.is_hashes {
            opts.push("HASHES".to_string());
        }
        if self.is_merges {
            opts.push("MERGES".to_string());
        }

        let mut script = format!(
            "CREATE OPERATOR {}.{} ({});",
            self.schema,
            self.name,
            opts.join(", ")
        )
        .with_empty_lines();

        if !self.owner.is_empty() {
            script.append_block(&format!(
                "ALTER OPERATOR {}.{}({}) OWNER TO {};",
                self.schema,
                self.name,
                self.operand_signature(),
                self.owner
            ));
        }

        if let Some(comment) = &self.comment {
            script.append_block(&format!(
                "COMMENT ON OPERATOR {}.{}({}) IS '{}';",
                self.schema,
                self.name,
                self.operand_signature(),
                comment.replace('\'', "''")
            ));
        }

        script
    }

    pub fn get_drop_script(&self) -> String {
        format!(
            "DROP OPERATOR IF EXISTS {}.{}({}) CASCADE;",
            self.schema,
            self.name,
            self.operand_signature()
        )
        .with_empty_lines()
    }

    pub fn get_alter_script(&self, target: &Operator, use_drop: bool) -> String {
        let mut script = String::new();

        let definition_changed = self.left_type != target.left_type
            || self.right_type != target.right_type
            || self.result_type != target.result_type
            || self.procedure != target.procedure
            || self.commutator != target.commutator
            || self.negator != target.negator
            || self.restrict != target.restrict
            || self.join != target.join
            || self.is_hashes != target.is_hashes
            || self.is_merges != target.is_merges;

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
                "ALTER OPERATOR {}.{}({}) OWNER TO {};",
                target.schema,
                target.name,
                target.operand_signature(),
                target.owner
            ));
        }

        if self.comment != target.comment {
            match &target.comment {
                Some(c) => {
                    script.append_block(&format!(
                        "COMMENT ON OPERATOR {}.{}({}) IS '{}';",
                        target.schema,
                        target.name,
                        target.operand_signature(),
                        c.replace('\'', "''")
                    ));
                }
                None if use_drop => {
                    script.append_block(&format!(
                        "COMMENT ON OPERATOR {}.{}({}) IS NULL;",
                        target.schema,
                        target.name,
                        target.operand_signature()
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

    fn make_op() -> Operator {
        Operator {
            schema: "public".into(),
            name: "===".into(),
            owner: "postgres".into(),
            left_type: Some("integer".into()),
            right_type: Some("integer".into()),
            result_type: "boolean".into(),
            procedure: "public.my_eq".into(),
            commutator: None,
            negator: None,
            restrict: None,
            join: None,
            is_hashes: false,
            is_merges: false,
            comment: None,
            hash: None,
        }
    }

    #[test]
    fn test_hash_populates() {
        let mut op = make_op();
        op.hash();
        assert!(op.hash.is_some());
    }

    #[test]
    fn test_get_script_contains_create_operator() {
        let mut op = make_op();
        op.hash();
        let s = op.get_script();
        assert!(s.contains("CREATE OPERATOR"));
        assert!(s.contains("PROCEDURE = public.my_eq"));
    }

    #[test]
    fn test_get_drop_script() {
        let mut op = make_op();
        op.hash();
        assert!(op.get_drop_script().contains("DROP OPERATOR"));
    }
}
