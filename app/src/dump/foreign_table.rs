use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::utils::string_extensions::StringExt;

/// A column in a foreign table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForeignTableColumn {
    pub name: String,
    pub data_type: String,
    pub is_nullable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub column_default: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub options: Vec<String>,
}

/// Information about a PostgreSQL foreign table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForeignTable {
    pub schema: String,
    pub name: String,
    pub server: String,
    pub owner: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub options: Vec<String>,
    pub columns: Vec<ForeignTableColumn>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    pub hash: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub acl: Vec<String>,
}

impl ForeignTable {
    pub fn new(
        schema: String,
        name: String,
        server: String,
        owner: String,
        options: Vec<String>,
        columns: Vec<ForeignTableColumn>,
    ) -> Self {
        let mut ft = Self {
            schema,
            name,
            server,
            owner,
            options,
            columns,
            comment: None,
            hash: None,
            acl: Vec::new(),
        };
        ft.hash();
        ft
    }

    pub fn hash(&mut self) {
        let mut hasher = Sha256::new();
        hasher.update(self.schema.as_bytes());
        hasher.update(self.name.as_bytes());
        hasher.update(self.server.as_bytes());
        hasher.update(self.owner.as_bytes());

        hasher.update((self.options.len() as u32).to_be_bytes());
        for opt in &self.options {
            hasher.update(opt.as_bytes());
        }

        hasher.update((self.columns.len() as u32).to_be_bytes());
        for col in &self.columns {
            hasher.update(col.name.as_bytes());
            hasher.update(col.data_type.as_bytes());
            hasher.update([col.is_nullable as u8]);
            if let Some(def) = &col.column_default {
                hasher.update(def.as_bytes());
            }
            hasher.update((col.options.len() as u32).to_be_bytes());
            for opt in &col.options {
                hasher.update(opt.as_bytes());
            }
        }

        if let Some(comment) = &self.comment {
            hasher.update((comment.len() as u32).to_be_bytes());
            hasher.update(comment.as_bytes());
        }

        self.hash = Some(format!("{:x}", hasher.finalize()));
    }

    /// Returns a CREATE FOREIGN TABLE script.
    pub fn get_script(&self) -> String {
        let mut script = format!("create foreign table {}.{} (\n", self.schema, self.name);

        for (i, col) in self.columns.iter().enumerate() {
            script.push_str(&format!("    {} {}", col.name, col.data_type));
            if !col.is_nullable {
                script.push_str(" not null");
            }
            if let Some(def) = &col.column_default {
                script.push_str(&format!(" default {}", def));
            }
            if !col.options.is_empty() {
                script.push_str(&format!(" options ({})", col.options.join(", ")));
            }
            if i < self.columns.len() - 1 {
                script.push(',');
            }
            script.push('\n');
        }

        script.push_str(&format!(")\nserver {}", self.server));

        if !self.options.is_empty() {
            script.push_str(&format!("\noptions ({})", self.options.join(", ")));
        }

        script.push(';');
        let mut result = script.with_empty_lines();

        if !self.owner.is_empty() {
            result.push_str(
                &format!(
                    "alter foreign table {}.{} owner to {};",
                    self.schema, self.name, self.owner
                )
                .with_empty_lines(),
            );
        }

        if let Some(comment) = &self.comment {
            result.push_str(
                &format!(
                    "comment on foreign table {}.{} is '{}';",
                    self.schema,
                    self.name,
                    comment.replace('\'', "''")
                )
                .with_empty_lines(),
            );
        }

        result
    }

    /// Returns a DROP FOREIGN TABLE script.
    pub fn get_drop_script(&self) -> String {
        format!(
            "drop foreign table if exists {}.{};",
            self.schema, self.name
        )
        .with_empty_lines()
    }

    /// Returns an ALTER script to transform self into target.
    pub fn get_alter_script(&self, target: &ForeignTable, use_drop: bool) -> String {
        let mut statements = Vec::new();

        // Owner change
        if self.owner != target.owner && !target.owner.is_empty() {
            statements.push(
                format!(
                    "alter foreign table {}.{} owner to {};",
                    target.schema, target.name, target.owner
                )
                .with_empty_lines(),
            );
        }

        // Server change requires drop+recreate
        if self.server != target.server {
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
                    "-- use_drop=false: foreign table {}.{} requires drop+recreate; statements commented out\n{}{}",
                    self.schema, self.name, commented_drop, commented_create
                );
            }
        }

        // Table options change
        if self.options != target.options {
            if target.options.is_empty() {
                // Reset all options — need to drop each individually
                for opt in &self.options {
                    if let Some(key) = opt.split_whitespace().next() {
                        statements.push(
                            format!(
                                "alter foreign table {}.{} options (drop {});",
                                target.schema, target.name, key
                            )
                            .with_empty_lines(),
                        );
                    }
                }
            } else {
                // Use SET for simplicity — drop all old, add all new
                statements.push(
                    format!(
                        "alter foreign table {}.{} options ({});",
                        target.schema,
                        target.name,
                        target.options.join(", ")
                    )
                    .with_empty_lines(),
                );
            }
        }

        // Column changes: detect added, dropped, and altered columns
        let self_cols: std::collections::HashMap<&str, &ForeignTableColumn> =
            self.columns.iter().map(|c| (c.name.as_str(), c)).collect();
        let target_cols: std::collections::HashMap<&str, &ForeignTableColumn> = target
            .columns
            .iter()
            .map(|c| (c.name.as_str(), c))
            .collect();

        // Drop columns that no longer exist
        for col in &self.columns {
            if !target_cols.contains_key(col.name.as_str()) {
                statements.push(
                    format!(
                        "alter foreign table {}.{} drop column {};",
                        target.schema, target.name, col.name
                    )
                    .with_empty_lines(),
                );
            }
        }

        // Add new columns
        for col in &target.columns {
            if !self_cols.contains_key(col.name.as_str()) {
                let mut col_def = format!("{} {}", col.name, col.data_type);
                if !col.is_nullable {
                    col_def.push_str(" not null");
                }
                if let Some(def) = &col.column_default {
                    col_def.push_str(&format!(" default {}", def));
                }
                statements.push(
                    format!(
                        "alter foreign table {}.{} add column {};",
                        target.schema, target.name, col_def
                    )
                    .with_empty_lines(),
                );
                if !col.options.is_empty() {
                    statements.push(
                        format!(
                            "alter foreign table {}.{} alter column {} options (add {});",
                            target.schema,
                            target.name,
                            col.name,
                            col.options.join(", ")
                        )
                        .with_empty_lines(),
                    );
                }
            }
        }

        // Alter existing columns
        for col in &target.columns {
            if let Some(existing) = self_cols.get(col.name.as_str()) {
                if col.data_type != existing.data_type {
                    statements.push(
                        format!(
                            "alter foreign table {}.{} alter column {} type {};",
                            target.schema, target.name, col.name, col.data_type
                        )
                        .with_empty_lines(),
                    );
                }
                if col.is_nullable != existing.is_nullable {
                    if col.is_nullable {
                        statements.push(
                            format!(
                                "alter foreign table {}.{} alter column {} drop not null;",
                                target.schema, target.name, col.name
                            )
                            .with_empty_lines(),
                        );
                    } else {
                        statements.push(
                            format!(
                                "alter foreign table {}.{} alter column {} set not null;",
                                target.schema, target.name, col.name
                            )
                            .with_empty_lines(),
                        );
                    }
                }
                if col.column_default != existing.column_default {
                    if let Some(def) = &col.column_default {
                        statements.push(
                            format!(
                                "alter foreign table {}.{} alter column {} set default {};",
                                target.schema, target.name, col.name, def
                            )
                            .with_empty_lines(),
                        );
                    } else {
                        statements.push(
                            format!(
                                "alter foreign table {}.{} alter column {} drop default;",
                                target.schema, target.name, col.name
                            )
                            .with_empty_lines(),
                        );
                    }
                }
            }
        }

        // Comment change
        if self.comment != target.comment {
            if let Some(comment) = &target.comment {
                statements.push(
                    format!(
                        "comment on foreign table {}.{} is '{}';",
                        target.schema,
                        target.name,
                        comment.replace('\'', "''")
                    )
                    .with_empty_lines(),
                );
            } else {
                statements.push(
                    format!(
                        "comment on foreign table {}.{} is null;",
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

    fn make_column(name: &str, data_type: &str) -> ForeignTableColumn {
        ForeignTableColumn {
            name: name.to_string(),
            data_type: data_type.to_string(),
            is_nullable: true,
            column_default: None,
            options: Vec::new(),
        }
    }

    fn make_foreign_table() -> ForeignTable {
        ForeignTable::new(
            "public".to_string(),
            "ft_test".to_string(),
            "remote_server".to_string(),
            "postgres".to_string(),
            Vec::new(),
            vec![make_column("id", "integer"), make_column("name", "text")],
        )
    }

    #[test]
    fn hash_populates_hash_field() {
        let ft = make_foreign_table();
        assert!(ft.hash.is_some());
    }

    #[test]
    fn hash_is_consistent() {
        let ft1 = make_foreign_table();
        let ft2 = make_foreign_table();
        assert_eq!(ft1.hash, ft2.hash);
    }

    #[test]
    fn hash_differs_with_different_server() {
        let ft1 = make_foreign_table();
        let mut ft2 = make_foreign_table();
        ft2.server = "other_server".to_string();
        ft2.hash();
        assert_ne!(ft1.hash, ft2.hash);
    }

    #[test]
    fn get_script_creates_foreign_table() {
        let ft = make_foreign_table();
        let script = ft.get_script();
        assert!(script.contains("create foreign table public.ft_test"));
        assert!(script.contains("server remote_server"));
        assert!(script.contains("id integer"));
        assert!(script.contains("name text"));
    }

    #[test]
    fn get_script_includes_not_null() {
        let mut ft = make_foreign_table();
        ft.columns[0].is_nullable = false;
        let script = ft.get_script();
        assert!(script.contains("id integer not null"));
    }

    #[test]
    fn get_script_includes_column_options() {
        let mut ft = make_foreign_table();
        ft.columns[0].options = vec!["column_name 'remote_id'".to_string()];
        let script = ft.get_script();
        assert!(script.contains("options (column_name 'remote_id')"));
    }

    #[test]
    fn get_script_includes_table_options() {
        let ft = ForeignTable::new(
            "public".to_string(),
            "ft_test".to_string(),
            "remote_server".to_string(),
            "postgres".to_string(),
            vec![
                "schema_name 'remote_schema'".to_string(),
                "table_name 'remote_table'".to_string(),
            ],
            vec![make_column("id", "integer")],
        );
        let script = ft.get_script();
        assert!(
            script.contains("options (schema_name 'remote_schema', table_name 'remote_table')")
        );
    }

    #[test]
    fn get_script_includes_owner() {
        let ft = make_foreign_table();
        let script = ft.get_script();
        assert!(script.contains("alter foreign table public.ft_test owner to postgres;"));
    }

    #[test]
    fn get_script_includes_comment() {
        let mut ft = make_foreign_table();
        ft.comment = Some("test comment".to_string());
        let script = ft.get_script();
        assert!(script.contains("comment on foreign table public.ft_test is 'test comment';"));
    }

    #[test]
    fn get_drop_script() {
        let ft = make_foreign_table();
        let script = ft.get_drop_script();
        assert!(script.contains("drop foreign table if exists public.ft_test;"));
    }

    #[test]
    fn get_alter_script_owner_change() {
        let ft1 = make_foreign_table();
        let mut ft2 = make_foreign_table();
        ft2.owner = "new_owner".to_string();
        let script = ft1.get_alter_script(&ft2, true);
        assert!(script.contains("alter foreign table public.ft_test owner to new_owner;"));
    }

    #[test]
    fn get_alter_script_server_change_drops_recreates() {
        let ft1 = make_foreign_table();
        let mut ft2 = make_foreign_table();
        ft2.server = "new_server".to_string();
        let script = ft1.get_alter_script(&ft2, true);
        assert!(script.contains("drop foreign table if exists public.ft_test;"));
        assert!(script.contains("create foreign table public.ft_test"));
        assert!(script.contains("server new_server"));
    }

    #[test]
    fn get_alter_script_add_column() {
        let ft1 = make_foreign_table();
        let mut ft2 = make_foreign_table();
        ft2.columns.push(make_column("email", "text"));
        let script = ft1.get_alter_script(&ft2, true);
        assert!(script.contains("alter foreign table public.ft_test add column email text;"));
    }

    #[test]
    fn get_alter_script_drop_column() {
        let ft1 = make_foreign_table();
        let mut ft2 = make_foreign_table();
        ft2.columns.retain(|c| c.name != "name");
        let script = ft1.get_alter_script(&ft2, true);
        assert!(script.contains("alter foreign table public.ft_test drop column name;"));
    }

    #[test]
    fn get_alter_script_change_column_type() {
        let ft1 = make_foreign_table();
        let mut ft2 = make_foreign_table();
        ft2.columns[0].data_type = "bigint".to_string();
        let script = ft1.get_alter_script(&ft2, true);
        assert!(script.contains("alter foreign table public.ft_test alter column id type bigint;"));
    }

    #[test]
    fn get_alter_script_set_not_null() {
        let ft1 = make_foreign_table();
        let mut ft2 = make_foreign_table();
        ft2.columns[0].is_nullable = false;
        let script = ft1.get_alter_script(&ft2, true);
        assert!(
            script.contains("alter foreign table public.ft_test alter column id set not null;")
        );
    }

    #[test]
    fn get_alter_script_drop_not_null() {
        let mut ft1 = make_foreign_table();
        ft1.columns[0].is_nullable = false;
        let ft2 = make_foreign_table();
        let script = ft1.get_alter_script(&ft2, true);
        assert!(
            script.contains("alter foreign table public.ft_test alter column id drop not null;")
        );
    }

    #[test]
    fn get_alter_script_comment_change() {
        let ft1 = make_foreign_table();
        let mut ft2 = make_foreign_table();
        ft2.comment = Some("new comment".to_string());
        let script = ft1.get_alter_script(&ft2, true);
        assert!(script.contains("comment on foreign table public.ft_test is 'new comment';"));
    }

    #[test]
    fn get_alter_script_no_changes() {
        let ft1 = make_foreign_table();
        let ft2 = make_foreign_table();
        let script = ft1.get_alter_script(&ft2, true);
        assert!(script.is_empty());
    }

    #[test]
    fn get_alter_script_server_change_use_drop_false_comments_out() {
        let ft1 = make_foreign_table();
        let mut ft2 = make_foreign_table();
        ft2.server = "new_server".to_string();
        let script = ft1.get_alter_script(&ft2, false);
        assert!(script.contains("-- use_drop=false"));
        assert!(script.contains("-- drop foreign table"));
        assert!(script.contains("-- create foreign table"));
        assert!(!script.contains("\ndrop foreign table"));
    }
}
