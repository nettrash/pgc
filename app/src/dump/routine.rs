use serde::{Deserialize, Serialize};
use sqlx::postgres::types::Oid;

// This is an information about a PostgreSQL routine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Routine {
    /// The schema name of the routine.
    pub schema: String,
    /// The object identifier of the routine.
    pub oid: Oid,
    /// The name of the routine.
    pub name: String,
    /// The language of the routine (e.g., 'plpgsql', 'sql').
    pub lang: String,
    /// The kind of the routine (e.g., 'function', 'procedure').
    pub kind: String,
    /// The return type of the routine (e.g., 'void', 'integer').
    pub return_type: String,
    /// The arguments of the routine, formatted as a string.
    pub arguments: String,
    /// The default values for the arguments, formatted as a string.
    pub arguments_defaults: Option<String>,
    /// Optional comment on the routine.
    #[serde(default)]
    pub comment: Option<String>,
    /// The description of the routine.
    pub source_code: String,
    /// The hash of the routine.
    pub hash: Option<String>,
}

impl Routine {
    /// Creates a new Routine instance.
    #[allow(clippy::too_many_arguments)] // Routine metadata naturally includes these fields from pg_proc.
    pub fn new(
        schema: String,
        oid: Oid,
        name: String,
        lang: String,
        kind: String,
        return_type: String,
        arguments: String,
        arguments_defaults: Option<String>,
        comment: Option<String>,
        source_code: String,
    ) -> Self {
        let mut routine = Routine {
            schema,
            oid,
            name,
            lang,
            kind,
            return_type,
            arguments,
            arguments_defaults,
            comment,
            source_code,
            hash: None,
        };
        routine.hash();
        routine
    }

    /// Hash
    pub fn hash(&mut self) {
        let src = format!(
            "{}.{}.{}.{}.{}.{}.{}.{}",
            self.schema,
            self.name,
            self.lang,
            self.kind,
            self.return_type,
            self.arguments,
            self.comment.clone().unwrap_or_default(),
            self.source_code
        );
        self.hash = Some(format!("{:x}", md5::compute(src)));
    }

    /// Returns a string to create the routine.
    pub fn get_script(&self) -> String {
        let kind = self.kind.to_lowercase();
        let delimiter = if self.source_code.contains("$$") {
            self.generate_dollar_delimiter()
        } else {
            "$$".to_string()
        };

        let script_body = match kind.as_str() {
            "procedure" => format!(
                "create or replace procedure \"{}\".\"{}\"({}) language {} as {d}{body}{d};\n",
                self.schema,
                self.name,
                self.arguments,
                self.lang,
                d = delimiter,
                body = self.source_code
            ),
            _ => format!(
                "create or replace {} \"{}\".\"{}\"({}) returns {} language {} as {d}{body}{d};\n",
                kind,
                self.schema,
                self.name,
                self.arguments,
                self.return_type,
                self.lang,
                d = delimiter,
                body = self.source_code
            ),
        };

        let mut script = script_body;

        if let Some(defaults) = &self.arguments_defaults {
            script.push_str(&format!("-- Defaults: {defaults}\n"));
        }

        if let Some(comment) = &self.comment {
            let object_kind = match kind.as_str() {
                "procedure" => "procedure",
                _ => "function",
            };
            script.push_str(&format!(
                "comment on {object_kind} \"{}\".\"{}\"({}) is '{}';\n",
                self.schema,
                self.name,
                self.arguments,
                comment.replace('\'', "''")
            ));
        }

        script
    }

    /// Returns a string to drop the routine.
    pub fn get_drop_script(&self) -> String {
        format!(
            "drop {} if exists \"{}\".\"{}\" ({});\n",
            self.kind.to_lowercase(),
            self.schema,
            self.name,
            self.arguments
        )
    }

    fn generate_dollar_delimiter(&self) -> String {
        let mut base = format!(
            "pgc_{}_body",
            self.name
                .chars()
                .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
                .collect::<String>()
        );
        if base.is_empty() {
            base = "pgc_body".to_string();
        }

        let mut idx = 0;
        loop {
            let candidate = if idx == 0 {
                base.clone()
            } else {
                format!("{}_{}", base, idx)
            };
            let delimiter = format!("${}$", candidate);
            if !self.source_code.contains(&delimiter) {
                return delimiter;
            }
            idx += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build_function_routine() -> Routine {
        Routine::new(
            "public".to_string(),
            Oid(42),
            "add".to_string(),
            "plpgsql".to_string(),
            "FUNCTION".to_string(),
            "integer".to_string(),
            "a integer".to_string(),
            Some("DEFAULT 1".to_string()),
            None,
            "BEGIN RETURN a + 1; END".to_string(),
        )
    }

    fn build_procedure_routine() -> Routine {
        Routine::new(
            "public".to_string(),
            Oid(7),
            "do_something".to_string(),
            "sql".to_string(),
            "Procedure".to_string(),
            "void".to_string(),
            "a integer".to_string(),
            None,
            None,
            "SELECT a;".to_string(),
        )
    }

    #[test]
    fn new_initializes_fields_and_hash() {
        let schema = "public";
        let name = "add";
        let lang = "plpgsql";
        let kind = "FUNCTION";
        let return_type = "integer";
        let arguments = "a integer";
        let defaults = Some("DEFAULT 1".to_string());
        let source_code = "BEGIN RETURN a + 1; END";

        let routine = Routine::new(
            schema.to_string(),
            Oid(42),
            name.to_string(),
            lang.to_string(),
            kind.to_string(),
            return_type.to_string(),
            arguments.to_string(),
            defaults.clone(),
            None,
            source_code.to_string(),
        );

        assert_eq!(routine.schema, schema);
        assert_eq!(routine.oid, Oid(42));
        assert_eq!(routine.name, name);
        assert_eq!(routine.lang, lang);
        assert_eq!(routine.kind, kind);
        assert_eq!(routine.return_type, return_type);
        assert_eq!(routine.arguments, arguments);
        assert_eq!(routine.arguments_defaults, defaults);
        assert_eq!(routine.source_code, source_code);

        let expected_src = format!(
            "{}.{}.{}.{}.{}.{}.{}.{}",
            schema, name, lang, kind, return_type, arguments, "", source_code
        );
        let expected_hash = format!("{:x}", md5::compute(expected_src));
        assert_eq!(routine.hash.as_ref(), Some(&expected_hash));
    }

    #[test]
    fn hash_reflects_source_code_changes() {
        let mut routine = build_function_routine();
        let original_hash = routine.hash.clone().expect("hash should be initialized");

        routine.source_code = "BEGIN RETURN a + 2; END".to_string();
        routine.hash();

        let updated_hash = routine.hash.clone().expect("hash should be recomputed");
        assert_ne!(updated_hash, original_hash);
    }

    #[test]
    fn get_script_uses_custom_delimiter_when_body_contains_dollar_dollar() {
        let routine = Routine::new(
            "public".to_string(),
            Oid(99),
            "echo".to_string(),
            "plpgsql".to_string(),
            "FUNCTION".to_string(),
            "void".to_string(),
            "".to_string(),
            None,
            None,
            "BEGIN PERFORM $$nested$$; END".to_string(),
        );

        let script = routine.get_script();

        assert!(script.contains("create or replace function"));
        assert!(!script.contains("$$BEGIN PERFORM $$nested$$; END$$"));
        assert!(script.contains("$pgc_echo_body$BEGIN PERFORM $$nested$$; END$pgc_echo_body$"));
        assert!(!routine.source_code.contains("$pgc_echo_body$"));
    }

    #[test]
    fn hash_does_not_include_argument_defaults() {
        let mut routine = build_function_routine();
        let original_hash = routine.hash.clone();

        routine.arguments_defaults = Some("DEFAULT 99".to_string());
        routine.hash();

        assert_eq!(routine.hash, original_hash);
    }

    #[test]
    fn get_script_for_function_includes_defaults() {
        let routine = build_function_routine();
        let script = routine.get_script();

        let expected = "create or replace function \"public\".\"add\"(a integer) returns integer language plpgsql as $$BEGIN RETURN a + 1; END$$;\n-- Defaults: DEFAULT 1\n";
        assert_eq!(script, expected);
    }

    #[test]
    fn get_script_for_procedure_omits_returns() {
        let routine = build_procedure_routine();
        let script = routine.get_script();

        let expected = "create or replace procedure \"public\".\"do_something\"(a integer) language sql as $$SELECT a;$$;\n";
        assert_eq!(script, expected);
    }

    #[test]
    fn get_drop_script_matches_kind() {
        let routine = build_function_routine();
        let drop_script = routine.get_drop_script();

        let expected = "drop function if exists \"public\".\"add\" (a integer);\n";
        assert_eq!(drop_script, expected);
    }

    #[test]
    fn get_script_handles_returns_table() {
        let routine = Routine::new(
            "data".to_string(),
            Oid(100),
            "test".to_string(),
            "plpgsql".to_string(),
            "FUNCTION".to_string(),
            "TABLE(row_to_json json)".to_string(),
            "fetching_id bigint, fetching_event_id character varying".to_string(),
            None,
            None,
            "BEGIN RETURN QUERY SELECT row_to_json(t) FROM t; END".to_string(),
        );
        let script = routine.get_script();

        let expected = "create or replace function \"data\".\"test\"(fetching_id bigint, fetching_event_id character varying) returns TABLE(row_to_json json) language plpgsql as $$BEGIN RETURN QUERY SELECT row_to_json(t) FROM t; END$$;\n";
        assert_eq!(script, expected);
    }
}
