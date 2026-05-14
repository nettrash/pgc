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
#[path = "foreign_table_tests.rs"]
mod tests;
