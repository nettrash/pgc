//! Full-text search objects: [`TextSearchConfig`] (`pg_ts_config`) and
//! [`TextSearchDict`] (`pg_ts_dict`).

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::utils::string_extensions::{StringExt, comment_out};

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

    /// Whether one of this configuration's mappings uses the dictionary
    /// `schema.name` (both in `quote_ident` form, as the mappings spell it).
    pub fn maps_dictionary(&self, schema: &str, name: &str) -> bool {
        let dictionary = format!("{schema}.{name}");
        self.mappings.iter().any(|mapping| {
            mapping.split_once(':').is_some_and(|(_, dicts)| {
                split_dictionary_list(dicts).contains(&dictionary.as_str())
            })
        })
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
                script.push_str(&comment_out(&drop));
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

    /// Whether turning this dictionary into `target` takes a new dictionary:
    /// its template changes, which no `ALTER` can make, or its options change
    /// in a way the in-place `ALTER` cannot reproduce (see
    /// [`TextSearchDict::get_alter_script`]).
    pub fn is_recreated_as(&self, target: &TextSearchDict) -> bool {
        self.template != target.template
            || (self.options != target.options && self.get_options_alter_script(target).is_none())
    }

    /// Replaces this dictionary with `target` (see
    /// [`TextSearchDict::is_recreated_as`]) without taking anything along.
    ///
    /// `DROP TEXT SEARCH DICTIONARY ... CASCADE` would silently drop every
    /// configuration that maps the dictionary, and through those every
    /// generated `tsvector` column and expression index built on them. So
    /// `target` is created under a temporary name, each of `configurations`
    /// — the FROM configurations mapping this dictionary — is re-pointed to
    /// it (`ALTER MAPPING REPLACE`), this dictionary is dropped without
    /// `CASCADE`, and the new one takes its name. The configurations keep
    /// their OIDs, so what depends on them is left in place; anything else
    /// still using the dictionary (a configuration in a schema the dump
    /// does not cover, a `regdictionary` constant) makes the drop fail
    /// instead of going with it.
    ///
    /// The statements run as one `DO` block, which is a single statement:
    /// when that drop fails, the temporary dictionary and the re-pointed
    /// mappings go with it, whether or not the migration runs in a
    /// transaction, and the same script runs again once the blocker is
    /// gone. Run again after it succeeded, it replaces the dictionary once
    /// more. The block is dollar-quoted with a tag its body does not
    /// contain (see `dollar_quote_tag`).
    ///
    /// Like the in-place `ALTER`, this never re-processes stored data.
    pub fn get_recreate_script(
        &self,
        target: &TextSearchDict,
        configurations: &[&TextSearchConfig],
    ) -> String {
        let temporary = self.temporary_name();
        let mut options = vec![format!("TEMPLATE = {}", target.template)];
        options.extend(target.options.iter().cloned());
        let mut statements = vec![format!(
            "CREATE TEXT SEARCH DICTIONARY {}.{} ({});",
            target.schema,
            temporary,
            options.join(", ")
        )];
        for configuration in configurations {
            statements.push(format!(
                "ALTER TEXT SEARCH CONFIGURATION {}.{} ALTER MAPPING REPLACE {}.{} WITH {}.{};",
                configuration.schema,
                configuration.name,
                self.schema,
                self.name,
                target.schema,
                temporary
            ));
        }
        statements.push(format!(
            "DROP TEXT SEARCH DICTIONARY {}.{};",
            self.schema, self.name
        ));
        statements.push(format!(
            "ALTER TEXT SEARCH DICTIONARY {}.{} RENAME TO {};",
            target.schema, temporary, target.name
        ));
        if !target.owner.is_empty() {
            statements.push(format!(
                "ALTER TEXT SEARCH DICTIONARY {}.{} OWNER TO {};",
                target.schema, target.name, target.owner
            ));
        }
        if let Some(comment) = &target.comment {
            statements.push(format!(
                "COMMENT ON TEXT SEARCH DICTIONARY {}.{} IS '{}';",
                target.schema,
                target.name,
                comment.replace('\'', "''")
            ));
        }
        let mut body = String::from("\nBEGIN\n");
        for statement in &statements {
            body.push_str("    ");
            body.push_str(statement);
            body.push('\n');
        }
        body.push_str("END\n");
        let tag = crate::dump::acl::dollar_quote_tag(&body);
        let mut script = String::from(
            "-- Re-created (its template or options cannot be altered in place) without CASCADE: the \
             text search configurations mapping it move to the new dictionary. Stored data is not \
             re-processed: rebuild the indexes and stored tsvector columns built through them if \
             its output changed.\n",
        );
        script.push_str(&format!("DO {tag}{body}{tag};").with_empty_lines());
        script
    }

    /// The name [`TextSearchDict::get_recreate_script`] creates the new
    /// dictionary under: `pgc_new_` and 16 hex digits of a hash of this
    /// dictionary's `schema.name` — a plain identifier, the same on every
    /// run, and well within PostgreSQL's 63 bytes.
    fn temporary_name(&self) -> String {
        let mut hasher = Sha256::new();
        hasher.update(self.schema.as_bytes());
        hasher.update(b".");
        hasher.update(self.name.as_bytes());
        let digest = format!("{:x}", hasher.finalize());
        format!("pgc_new_{}", &digest[..16])
    }

    /// Whether turning this dictionary into `target` is an in-place
    /// `ALTER TEXT SEARCH DICTIONARY ... (...)` of its options: the template
    /// is unchanged, the options differ, and the statement can be built (see
    /// [`TextSearchDict::get_alter_script`]). The comparer emits these before
    /// the tables, so a generated column or index created in the same
    /// migration is computed with the new options.
    pub fn alters_options_in_place(&self, target: &TextSearchDict) -> bool {
        self.template == target.template
            && self.options != target.options
            && self.get_options_alter_script(target).is_some()
    }

    /// Returns a script that turns this dictionary into `target`.
    ///
    /// An options-only change (for example PostgreSQL 19's rewritten Dutch
    /// stemmer, where keeping the old stemming means switching
    /// `language = 'dutch'` to `'dutch_porter'`) is applied in place with
    /// `ALTER TEXT SEARCH DICTIONARY ... (...)`. Dropping the dictionary
    /// instead needs `CASCADE`, which silently takes every text search
    /// configuration that maps it, and through those every generated
    /// `tsvector` column and expression index built on them.
    ///
    /// `TEMPLATE` cannot be altered (PostgreSQL rejects it as an unrecognized
    /// dictionary parameter), so a template change is a drop and recreate
    /// here, commented out when `use_drop` is off. With `use_drop` the
    /// comparer replaces such a dictionary with
    /// [`TextSearchDict::get_recreate_script`] instead, which takes nothing
    /// along.
    pub fn get_alter_script(&self, target: &TextSearchDict, use_drop: bool) -> String {
        let mut script = String::new();

        let template_changed = self.template != target.template;
        let options_alter = if !template_changed && self.options != target.options {
            self.get_options_alter_script(target)
        } else {
            None
        };
        let definition_changed =
            template_changed || (self.options != target.options && options_alter.is_none());

        if definition_changed {
            if use_drop {
                script.append_block(&self.get_drop_script());
            } else {
                let drop = self.get_drop_script();
                script.push_str(&comment_out(&drop));
            }
            script.push_str(&target.get_script());
            return script;
        }

        if let Some(alter) = options_alter {
            script.push_str(&alter);
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

    /// The in-place `ALTER TEXT SEARCH DICTIONARY` that turns this
    /// dictionary's options into `target`'s, or `None` when it cannot be
    /// built reliably and the caller must fall back to drop and recreate.
    ///
    /// PostgreSQL (`AlterTSDictionary`) processes the statement's options in
    /// order: each one first removes every existing option of the same name,
    /// then, if it carries a value, is appended. So the statement lists the
    /// bare names of the options `target` no longer has, followed by every
    /// `target` option in `target` order, which leaves `dictinitoption`
    /// byte-equal to `target`'s and the dictionary hash converged.
    ///
    /// `None` when an option has no parseable name, or when `target` repeats
    /// a name (a later entry would replace the earlier one, so the stored
    /// list could not come out equal).
    fn get_options_alter_script(&self, target: &TextSearchDict) -> Option<String> {
        let from_keys = self
            .options
            .iter()
            .map(|o| dict_option_name(o))
            .collect::<Option<Vec<_>>>()?;
        let to_keys = target
            .options
            .iter()
            .map(|o| dict_option_name(o))
            .collect::<Option<Vec<_>>>()?;
        if to_keys
            .iter()
            .enumerate()
            .any(|(i, k)| to_keys[..i].contains(k))
        {
            return None;
        }

        let mut items: Vec<&str> = Vec::new();
        for key in from_keys {
            if !to_keys.contains(&key) && !items.contains(&key) {
                items.push(key);
            }
        }
        items.extend(target.options.iter().map(|o| o.trim()));
        if items.is_empty() {
            return None;
        }

        let mut script = String::new();
        script.push_str(
            "-- ALTER does not re-process stored data: rebuild the indexes and stored tsvector \
             columns built through configurations that use this dictionary if its output changed.\n",
        );
        script.append_block(&format!(
            "ALTER TEXT SEARCH DICTIONARY {}.{} ({});",
            target.schema,
            target.name,
            items.join(", ")
        ));
        Some(script)
    }
}

/// The dictionaries of one mapping's `dict1,dict2` list, as the dump writes
/// it: `quote_ident`-ed `schema.name` entries joined by commas. A comma
/// inside a quoted identifier does not split.
fn split_dictionary_list(dicts: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut start = 0;
    let mut quoted = false;
    for (i, c) in dicts.char_indices() {
        match c {
            '"' => quoted = !quoted,
            ',' if !quoted => {
                parts.push(dicts[start..i].trim());
                start = i + 1;
            }
            _ => {}
        }
    }
    parts.push(dicts[start..].trim());
    parts
}

/// The option name of one `dictinitoption` entry, as PostgreSQL's
/// `serialize_deflist` writes it: `quote_identifier(name) = value`. A quoted
/// name keeps its quotes, so the result can be re-emitted verbatim as the
/// bare-name (option removal) form of `ALTER TEXT SEARCH DICTIONARY`.
///
/// `None` when the entry is not of the `name = value` shape.
fn dict_option_name(option: &str) -> Option<&str> {
    let option = option.trim_start();
    let end = if let Some(rest) = option.strip_prefix('"') {
        let bytes = rest.as_bytes();
        let mut i = 0;
        loop {
            match bytes.get(i) {
                None => return None,
                Some(b'"') if bytes.get(i + 1) == Some(&b'"') => i += 2,
                // Opening quote plus the closing one at `rest[i]`.
                Some(b'"') => break i + 2,
                Some(_) => i += 1,
            }
        }
    } else {
        option.find(|c: char| c == '=' || c.is_whitespace())?
    };
    let name = &option[..end];
    if name.is_empty() || !option[end..].trim_start().starts_with('=') {
        return None;
    }
    Some(name)
}

#[cfg(test)]
#[path = "tests/text_search.rs"]
mod tests;
