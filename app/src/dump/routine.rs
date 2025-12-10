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
            source_code,
            hash: None,
        };
        routine.hash();
        routine
    }

    /// Hash
    pub fn hash(&mut self) {
        let src = format!(
            "{}.{}.{}.{}.{}.{}.{}",
            self.schema,
            self.name,
            self.lang,
            self.kind,
            self.return_type,
            self.arguments,
            self.source_code
        );
        self.hash = Some(format!("{:x}", md5::compute(src)));
    }

    /// Returns a string to create the routine.
    pub fn get_script(&self) -> String {
        let kind = self.kind.to_lowercase();
        let script_body = match kind.as_str() {
            "procedure" => format!(
                "create or replace procedure \"{}\".\"{}\"({}) language {} as $${}$$;\n",
                self.schema, self.name, self.arguments, self.lang, self.source_code
            ),
            _ => format!(
                "create or replace {} \"{}\".\"{}\"({}) returns {} language {} as $${}$$;\n",
                kind,
                self.schema,
                self.name,
                self.arguments,
                self.return_type,
                self.lang,
                self.source_code
            ),
        };

        let mut script = script_body;

        if let Some(defaults) = &self.arguments_defaults {
            script.push_str(&format!("-- Defaults: {defaults}\n"));
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
            "{}.{}.{}.{}.{}.{}.{}",
            schema, name, lang, kind, return_type, arguments, source_code
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
    fn hash_ignores_argument_defaults() {
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
}
