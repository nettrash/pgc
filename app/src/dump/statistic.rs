use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::utils::string_extensions::StringExt;

/// Information about a PostgreSQL extended statistics object (CREATE STATISTICS).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Statistic {
    pub schema: String,
    pub name: String,
    pub owner: String,
    /// The target table schema.
    pub table_schema: String,
    /// The target table name.
    pub table_name: String,
    /// The statistics kinds (e.g., "ndistinct", "dependencies", "mcv").
    pub kinds: Vec<String>,
    /// The column/expression definitions.
    pub columns: Vec<String>,
    /// The full definition as returned by pg_get_statisticsobjdef.
    pub definition: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    /// Statistics target (number of most-common-values entries)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stxstattarget: Option<i32>,
    pub hash: Option<String>,
}

impl Statistic {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        schema: String,
        name: String,
        owner: String,
        table_schema: String,
        table_name: String,
        kinds: Vec<String>,
        columns: Vec<String>,
        definition: String,
    ) -> Self {
        let mut stat = Self {
            schema,
            name,
            owner,
            table_schema,
            table_name,
            kinds,
            columns,
            definition,
            comment: None,
            stxstattarget: None,
            hash: None,
        };
        stat.hash();
        stat
    }

    pub fn hash(&mut self) {
        let mut hasher = Sha256::new();
        hasher.update(self.schema.as_bytes());
        hasher.update(self.name.as_bytes());
        hasher.update(self.owner.as_bytes());
        hasher.update(self.table_schema.as_bytes());
        hasher.update(self.table_name.as_bytes());

        hasher.update((self.kinds.len() as u32).to_be_bytes());
        for kind in &self.kinds {
            hasher.update(kind.as_bytes());
        }

        hasher.update((self.columns.len() as u32).to_be_bytes());
        for col in &self.columns {
            hasher.update(col.as_bytes());
        }

        if let Some(comment) = &self.comment {
            hasher.update((comment.len() as u32).to_be_bytes());
            hasher.update(comment.as_bytes());
        }

        hasher.update(self.definition.as_bytes());

        if let Some(target) = self.stxstattarget {
            hasher.update(target.to_be_bytes());
        }

        self.hash = Some(format!("{:x}", hasher.finalize()));
    }

    /// Returns a CREATE STATISTICS script.
    pub fn get_script(&self) -> String {
        let mut result = format!(
            "create statistics {}.{} ({}) on {} from {}.{};",
            self.schema,
            self.name,
            self.kinds.join(", "),
            self.columns.join(", "),
            self.table_schema,
            self.table_name
        )
        .with_empty_lines();

        if !self.owner.is_empty() {
            result.push_str(
                &format!(
                    "alter statistics {}.{} owner to {};",
                    self.schema, self.name, self.owner
                )
                .with_empty_lines(),
            );
        }

        if let Some(target) = self.stxstattarget
            && target >= 0
        {
            result.push_str(
                &format!(
                    "alter statistics {}.{} set statistics {};",
                    self.schema, self.name, target
                )
                .with_empty_lines(),
            );
        }

        if let Some(comment) = &self.comment {
            result.push_str(
                &format!(
                    "comment on statistics {}.{} is '{}';",
                    self.schema,
                    self.name,
                    comment.replace('\'', "''")
                )
                .with_empty_lines(),
            );
        }

        result
    }

    /// Returns a DROP STATISTICS script.
    pub fn get_drop_script(&self) -> String {
        format!("drop statistics if exists {}.{};", self.schema, self.name).with_empty_lines()
    }

    /// Returns an ALTER script to transform self into target.
    /// Extended statistics objects can't be altered in place for definition changes;
    /// they must be dropped and recreated.
    pub fn get_alter_script(&self, target: &Statistic, use_drop: bool) -> String {
        let mut statements = Vec::new();

        // Owner change
        if self.owner != target.owner && !target.owner.is_empty() {
            statements.push(
                format!(
                    "alter statistics {}.{} owner to {};",
                    target.schema, target.name, target.owner
                )
                .with_empty_lines(),
            );
        }

        // If the structural definition changed, drop and recreate
        if self.kinds != target.kinds
            || self.columns != target.columns
            || self.table_schema != target.table_schema
            || self.table_name != target.table_name
        {
            let drop = self.get_drop_script();
            let create = target.get_script();
            if use_drop {
                return format!("{}{}", drop, create);
            } else {
                let commented_drop = drop
                    .lines()
                    .map(|l| format!("-- {}\n", l))
                    .collect::<String>();
                let commented_create = create
                    .lines()
                    .map(|l| format!("-- {}\n", l))
                    .collect::<String>();
                return format!(
                    "-- use_drop=false: statistics {}.{} requires drop+recreate; statements commented out\n{}{}",
                    self.schema, self.name, commented_drop, commented_create
                );
            }
        }

        // Comment change
        if self.comment != target.comment {
            if let Some(comment) = &target.comment {
                statements.push(
                    format!(
                        "comment on statistics {}.{} is '{}';",
                        target.schema,
                        target.name,
                        comment.replace('\'', "''")
                    )
                    .with_empty_lines(),
                );
            } else {
                statements.push(
                    format!(
                        "comment on statistics {}.{} is null;",
                        target.schema, target.name
                    )
                    .with_empty_lines(),
                );
            }
        }

        // Statistics target change
        if self.stxstattarget != target.stxstattarget {
            if let Some(t) = target.stxstattarget {
                if t >= 0 {
                    statements.push(
                        format!(
                            "alter statistics {}.{} set statistics {};",
                            target.schema, target.name, t
                        )
                        .with_empty_lines(),
                    );
                }
            } else {
                // Reset to default (-1)
                statements.push(
                    format!(
                        "alter statistics {}.{} set statistics -1;",
                        target.schema, target.name
                    )
                    .with_empty_lines(),
                );
            }
        }

        statements.join("")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_statistic() -> Statistic {
        Statistic::new(
            "public".to_string(),
            "my_stat".to_string(),
            "postgres".to_string(),
            "public".to_string(),
            "my_table".to_string(),
            vec!["ndistinct".to_string(), "dependencies".to_string()],
            vec!["col1".to_string(), "col2".to_string()],
            "CREATE STATISTICS public.my_stat (ndistinct, dependencies) ON col1, col2 FROM public.my_table".to_string(),
        )
    }

    #[test]
    fn hash_populates_hash_field() {
        let stat = make_statistic();
        assert!(stat.hash.is_some());
    }

    #[test]
    fn hash_is_consistent() {
        let s1 = make_statistic();
        let s2 = make_statistic();
        assert_eq!(s1.hash, s2.hash);
    }

    #[test]
    fn hash_differs_with_different_name() {
        let s1 = make_statistic();
        let mut s2 = make_statistic();
        s2.name = "other_stat".to_string();
        s2.hash();
        assert_ne!(s1.hash, s2.hash);
    }

    #[test]
    fn get_script_creates_statistics() {
        let stat = make_statistic();
        let script = stat.get_script();
        assert!(script.contains("create statistics public.my_stat"));
        assert!(script.contains("(ndistinct, dependencies)"));
        assert!(script.contains("on col1, col2"));
        assert!(script.contains("from public.my_table"));
    }

    #[test]
    fn get_script_includes_owner() {
        let stat = make_statistic();
        let script = stat.get_script();
        assert!(script.contains("alter statistics public.my_stat owner to postgres;"));
    }

    #[test]
    fn get_script_includes_comment() {
        let mut stat = make_statistic();
        stat.comment = Some("test comment".to_string());
        let script = stat.get_script();
        assert!(script.contains("comment on statistics public.my_stat is 'test comment';"));
    }

    #[test]
    fn get_drop_script() {
        let stat = make_statistic();
        let script = stat.get_drop_script();
        assert!(script.contains("drop statistics if exists public.my_stat;"));
    }

    #[test]
    fn get_alter_script_owner_change() {
        let s1 = make_statistic();
        let mut s2 = make_statistic();
        s2.owner = "new_owner".to_string();
        let script = s1.get_alter_script(&s2, true);
        assert!(script.contains("alter statistics public.my_stat owner to new_owner;"));
    }

    #[test]
    fn get_alter_script_definition_change_drops_recreates() {
        let s1 = make_statistic();
        let mut s2 = make_statistic();
        s2.definition =
            "CREATE STATISTICS public.my_stat (mcv) ON col1, col2, col3 FROM public.my_table"
                .to_string();
        s2.kinds = vec!["mcv".to_string()];
        s2.columns = vec!["col1".to_string(), "col2".to_string(), "col3".to_string()];
        let script = s1.get_alter_script(&s2, true);
        assert!(script.contains("drop statistics if exists public.my_stat;"));
        assert!(script.contains("create statistics public.my_stat"));
    }

    #[test]
    fn get_alter_script_comment_change() {
        let s1 = make_statistic();
        let mut s2 = make_statistic();
        s2.comment = Some("new comment".to_string());
        let script = s1.get_alter_script(&s2, true);
        assert!(script.contains("comment on statistics public.my_stat is 'new comment';"));
    }

    #[test]
    fn get_alter_script_no_changes() {
        let s1 = make_statistic();
        let s2 = make_statistic();
        let script = s1.get_alter_script(&s2, true);
        assert!(script.is_empty());
    }

    #[test]
    fn get_alter_script_definition_change_use_drop_false_comments_out() {
        let s1 = make_statistic();
        let mut s2 = make_statistic();
        s2.kinds = vec!["mcv".to_string()];
        s2.columns = vec!["col1".to_string(), "col2".to_string(), "col3".to_string()];
        let script = s1.get_alter_script(&s2, false);
        assert!(script.contains("-- use_drop=false"));
        assert!(script.contains("-- drop statistics"));
        assert!(script.contains("-- create statistics"));
        assert!(!script.contains("\ndrop statistics"));
    }
}
