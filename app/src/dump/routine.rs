use serde::{Deserialize, Serialize};
use sqlx::postgres::types::Oid;

/// Information about a PostgreSQL aggregate function from pg_aggregate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregateInfo {
    /// Transition function (SFUNC)
    pub sfunc: String,
    /// State data type (STYPE)
    pub stype: String,
    /// State data space (SSPACE), if non-zero
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sspace: Option<i32>,
    /// Final function (FINALFUNC)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finalfunc: Option<String>,
    /// Whether FINALFUNC receives extra arguments (FINALFUNC_EXTRA)
    #[serde(default)]
    pub finalfunc_extra: bool,
    /// FINALFUNC_MODIFY behaviour: r=read_only, s=shareable, w=read_write
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finalfunc_modify: Option<String>,
    /// Combine function (COMBINEFUNC)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub combinefunc: Option<String>,
    /// Serialization function (SERIALFUNC)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub serialfunc: Option<String>,
    /// Deserialization function (DESERIALFUNC)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deserialfunc: Option<String>,
    /// Initial condition (INITCOND)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub initcond: Option<String>,
    /// Moving-aggregate transition function (MSFUNC)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub msfunc: Option<String>,
    /// Moving-aggregate inverse transition function (MINVFUNC)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub minvfunc: Option<String>,
    /// Moving-aggregate state type (MSTYPE)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mstype: Option<String>,
    /// Moving-aggregate state space (MSSPACE), if non-zero
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub msspace: Option<i32>,
    /// Moving-aggregate final function (MFINALFUNC)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mfinalfunc: Option<String>,
    /// Whether MFINALFUNC receives extra arguments (MFINALFUNC_EXTRA)
    #[serde(default)]
    pub mfinalfunc_extra: bool,
    /// MFINALFUNC_MODIFY behaviour: r=read_only, s=shareable, w=read_write
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mfinalfunc_modify: Option<String>,
    /// Moving-aggregate initial condition (MINITCOND)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub minitcond: Option<String>,
    /// Sort operator (SORTOP)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sortop: Option<String>,
    /// Aggregate kind: 'n' for normal, 'o' for ordered-set, 'h' for hypothetical-set
    #[serde(default = "AggregateInfo::default_kind")]
    pub kind: char,
    /// Number of direct (non-aggregated) arguments for ordered-set/hypothetical-set aggregates.
    /// For normal aggregates this is 0.
    #[serde(default)]
    pub num_direct_args: i16,
}

impl AggregateInfo {
    fn default_kind() -> char {
        'n'
    }

    /// Generates the body of CREATE AGGREGATE (the parenthesized option list).
    pub fn get_options_body(&self) -> String {
        let mut opts: Vec<String> = Vec::new();
        opts.push(format!("    SFUNC = {}", self.sfunc));
        opts.push(format!("    STYPE = {}", self.stype));
        if let Some(ref v) = self.sspace
            && *v > 0
        {
            opts.push(format!("    SSPACE = {}", v));
        }
        if let Some(ref v) = self.finalfunc {
            opts.push(format!("    FINALFUNC = {}", v));
        }
        if self.finalfunc_extra {
            opts.push("    FINALFUNC_EXTRA".to_string());
        }
        if let Some(ref v) = self.finalfunc_modify {
            let label = match v.as_str() {
                "s" => "SHAREABLE",
                "w" => "READ_WRITE",
                _ => "READ_ONLY",
            };
            opts.push(format!("    FINALFUNC_MODIFY = {}", label));
        }
        if let Some(ref v) = self.combinefunc {
            opts.push(format!("    COMBINEFUNC = {}", v));
        }
        if let Some(ref v) = self.serialfunc {
            opts.push(format!("    SERIALFUNC = {}", v));
        }
        if let Some(ref v) = self.deserialfunc {
            opts.push(format!("    DESERIALFUNC = {}", v));
        }
        if let Some(ref v) = self.initcond {
            opts.push(format!("    INITCOND = '{}'", v.replace('\'', "''")));
        }
        if let Some(ref v) = self.msfunc {
            opts.push(format!("    MSFUNC = {}", v));
        }
        if let Some(ref v) = self.minvfunc {
            opts.push(format!("    MINVFUNC = {}", v));
        }
        if let Some(ref v) = self.mstype {
            opts.push(format!("    MSTYPE = {}", v));
        }
        if let Some(ref v) = self.msspace
            && *v > 0
        {
            opts.push(format!("    MSSPACE = {}", v));
        }
        if let Some(ref v) = self.mfinalfunc {
            opts.push(format!("    MFINALFUNC = {}", v));
        }
        if self.mfinalfunc_extra {
            opts.push("    MFINALFUNC_EXTRA".to_string());
        }
        if let Some(ref v) = self.mfinalfunc_modify {
            let label = match v.as_str() {
                "s" => "SHAREABLE",
                "w" => "READ_WRITE",
                _ => "READ_ONLY",
            };
            opts.push(format!("    MFINALFUNC_MODIFY = {}", label));
        }
        if let Some(ref v) = self.minitcond {
            opts.push(format!("    MINITCOND = '{}'", v.replace('\'', "''")));
        }
        if let Some(ref v) = self.sortop {
            opts.push(format!("    SORTOP = {}", v));
        }
        if self.kind == 'h' {
            opts.push("    HYPOTHETICAL".to_string());
        }
        opts.join(",\n")
    }
}

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
    /// The owner of the routine.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub owner: String,
    /// Optional comment on the routine.
    #[serde(default)]
    pub comment: Option<String>,
    /// The description of the routine.
    pub source_code: String,
    /// Volatility category: "volatile", "stable", or "immutable".
    #[serde(default = "Routine::default_volatility")]
    pub volatility: String,
    /// Whether the function is strict (RETURNS NULL ON NULL INPUT).
    #[serde(default)]
    pub is_strict: bool,
    /// Whether the function is leak-proof.
    #[serde(default)]
    pub is_leakproof: bool,
    /// Parallel safety: "unsafe", "restricted", or "safe".
    #[serde(default = "Routine::default_parallel")]
    pub parallel: String,
    /// Whether the routine runs with definer's privileges (SECURITY DEFINER).
    #[serde(default)]
    pub security_definer: bool,
    /// For aggregate functions: the aggregate definition details.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aggregate_info: Option<AggregateInfo>,
    /// The hash of the routine.
    pub hash: Option<String>,
}

impl Routine {
    fn default_volatility() -> String {
        "volatile".to_string()
    }

    fn default_parallel() -> String {
        "unsafe".to_string()
    }

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
            owner: String::new(),
            comment,
            source_code,
            volatility: "volatile".to_string(),
            is_strict: false,
            is_leakproof: false,
            parallel: "unsafe".to_string(),
            security_definer: false,
            aggregate_info: None,
            hash: None,
        };
        routine.hash();
        routine
    }

    /// Hash
    pub fn hash(&mut self) {
        let src = format!(
            "{}.{}.{}.{}.{}.{}.{}.{}.{}.{}.{}.{}.{}.{}",
            self.schema,
            self.name,
            self.lang,
            self.kind,
            self.return_type,
            self.arguments,
            self.owner,
            self.comment.clone().unwrap_or_default(),
            self.source_code,
            self.volatility,
            self.is_strict,
            self.is_leakproof,
            self.parallel,
            self.security_definer,
        );
        self.hash = Some(format!("{:x}", md5::compute(src)));
    }

    /// Builds the flags clause for functions/window functions.
    fn get_flags_clause(&self) -> String {
        let kind = self.kind.to_lowercase();
        let mut flags = Vec::new();

        // Volatility (only meaningful for functions/window)
        if kind != "procedure" {
            match self.volatility.as_str() {
                "immutable" => flags.push("IMMUTABLE".to_string()),
                "stable" => flags.push("STABLE".to_string()),
                _ => flags.push("VOLATILE".to_string()),
            }
        }

        // STRICT (not valid for procedures)
        if kind != "procedure" && self.is_strict {
            flags.push("STRICT".to_string());
        }

        // LEAKPROOF
        if kind != "procedure" && self.is_leakproof {
            flags.push("LEAKPROOF".to_string());
        }

        // PARALLEL safety (only meaningful for functions/window)
        if kind != "procedure" {
            match self.parallel.as_str() {
                "safe" => flags.push("PARALLEL SAFE".to_string()),
                "restricted" => flags.push("PARALLEL RESTRICTED".to_string()),
                _ => flags.push("PARALLEL UNSAFE".to_string()),
            }
        }

        // SECURITY DEFINER
        if self.security_definer {
            flags.push("SECURITY DEFINER".to_string());
        }

        // WINDOW
        if kind == "window" {
            flags.push("WINDOW".to_string());
        }

        if flags.is_empty() {
            String::new()
        } else {
            format!(" {}", flags.join(" "))
        }
    }

    /// Returns a string to create the routine.
    pub fn get_script(&self) -> String {
        let kind = self.kind.to_lowercase();

        // Aggregate functions use a completely different CREATE syntax.
        if kind == "aggregate" {
            return self.get_aggregate_script();
        }

        let delimiter = if self.source_code.contains("$$") {
            self.generate_dollar_delimiter()
        } else {
            "$$".to_string()
        };

        let arguments_with_defaults = self.arguments_with_defaults();
        let flags = self.get_flags_clause();

        // For window functions, use CREATE FUNCTION (WINDOW is a flag, not a kind)
        let create_kind = if kind == "window" { "function" } else { &kind };

        let script_body = match kind.as_str() {
            "procedure" => format!(
                "create or replace procedure \"{}\".\"{}\"({}) language {}{flags} as {d}{body}{d};\n",
                self.schema,
                self.name,
                arguments_with_defaults,
                self.lang,
                flags = flags,
                d = delimiter,
                body = self.source_code
            ),
            _ => format!(
                "create or replace {create_kind} \"{}\".\"{}\"({}) returns {} language {}{flags} as {d}{body}{d};\n",
                self.schema,
                self.name,
                arguments_with_defaults,
                self.return_type,
                self.lang,
                create_kind = create_kind,
                flags = flags,
                d = delimiter,
                body = self.source_code
            ),
        };

        let mut script = script_body;

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

        script.push_str(&self.get_owner_script());

        script
    }

    /// Returns the argument list with default values embedded.
    ///
    /// PostgreSQL stores defaults separately from the argument list in pg_proc.
    /// `pg_get_function_identity_arguments()` returns arguments without defaults,
    /// while `pg_get_expr(proargdefaults, 0)` returns a comma-separated list of
    /// default expressions that apply to the **last N** arguments.
    ///
    /// This method merges them back so `CREATE OR REPLACE` includes the defaults
    /// and doesn't fail with "cannot remove parameter defaults from existing function".
    fn arguments_with_defaults(&self) -> String {
        let defaults_str = match &self.arguments_defaults {
            Some(d) if !d.is_empty() => d,
            _ => return self.arguments.clone(),
        };

        if self.arguments.is_empty() {
            return self.arguments.clone();
        }

        // Split arguments respecting parenthesized type expressions (e.g. "numeric(10,2)")
        let args = Self::split_arguments(&self.arguments);
        // Split defaults — these are simple expressions, but may contain commas inside
        // function calls; use the same splitter for safety.
        let defaults = Self::split_arguments(defaults_str);

        if defaults.is_empty() || defaults.len() > args.len() {
            return self.arguments.clone();
        }

        // Defaults apply to the last N arguments
        let first_default_idx = args.len() - defaults.len();
        let mut result_parts: Vec<String> = Vec::with_capacity(args.len());

        for (i, arg) in args.iter().enumerate() {
            if i >= first_default_idx {
                let default_val = defaults[i - first_default_idx].trim();
                if default_val.to_uppercase().starts_with("DEFAULT ") {
                    result_parts.push(format!("{} {}", arg.trim(), default_val));
                } else {
                    result_parts.push(format!("{} DEFAULT {}", arg.trim(), default_val));
                }
            } else {
                result_parts.push(arg.trim().to_string());
            }
        }

        result_parts.join(", ")
    }

    /// Splits a comma-separated string respecting parenthesized groups.
    /// E.g. "a numeric(10,2), b text" → ["a numeric(10,2)", " b text"]
    fn split_arguments(s: &str) -> Vec<String> {
        let mut parts = Vec::new();
        let mut depth = 0;
        let mut current = String::new();

        for ch in s.chars() {
            match ch {
                '(' => {
                    depth += 1;
                    current.push(ch);
                }
                ')' => {
                    depth -= 1;
                    current.push(ch);
                }
                ',' if depth == 0 => {
                    parts.push(current.clone());
                    current.clear();
                }
                _ => {
                    current.push(ch);
                }
            }
        }

        if !current.is_empty() {
            parts.push(current);
        }

        parts
    }

    /// Returns the CREATE AGGREGATE script.
    fn get_aggregate_script(&self) -> String {
        let mut script = String::new();

        if let Some(ref agg) = self.aggregate_info {
            let args = if self.arguments.is_empty() {
                "*".to_string()
            } else if (agg.kind == 'o' || agg.kind == 'h') && agg.num_direct_args >= 0 {
                // Ordered-set and hypothetical-set aggregates use the syntax:
                //   CREATE AGGREGATE name(direct_args ORDER BY sorted_args) (...)
                // pg_get_function_identity_arguments returns all args comma-separated;
                // we split at num_direct_args to insert ORDER BY.
                let all_args = Self::split_arguments(&self.arguments);
                let n_direct = agg.num_direct_args as usize;
                if n_direct == 0 {
                    // No direct args: ORDER BY sorted_args
                    format!(
                        "ORDER BY {}",
                        all_args
                            .iter()
                            .map(|a| a.trim().to_string())
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                } else if n_direct < all_args.len() {
                    let direct: Vec<String> = all_args[..n_direct]
                        .iter()
                        .map(|a| a.trim().to_string())
                        .collect();
                    let sorted: Vec<String> = all_args[n_direct..]
                        .iter()
                        .map(|a| a.trim().to_string())
                        .collect();
                    format!("{} ORDER BY {}", direct.join(", "), sorted.join(", "))
                } else {
                    // All args are direct (edge case) – fall back to normal syntax
                    self.arguments.clone()
                }
            } else {
                self.arguments.clone()
            };

            script.push_str(&format!(
                "create aggregate \"{}\".\"{}\"({}) (\n{}\n);\n",
                self.schema,
                self.name,
                args,
                agg.get_options_body()
            ));
        } else {
            // Fallback: no aggregate_info available, emit a comment
            script.push_str(&format!(
                "/* aggregate \"{}\".\"{}\"({}) — aggregate details unavailable */\n",
                self.schema, self.name, self.arguments
            ));
        }

        if let Some(comment) = &self.comment {
            script.push_str(&format!(
                "comment on aggregate \"{}\".\"{}\"({}) is '{}';\n",
                self.schema,
                self.name,
                self.arguments,
                comment.replace('\'', "''")
            ));
        }

        script.push_str(&self.get_owner_script());
        script
    }

    /// Returns a string to drop the routine.
    pub fn get_drop_script(&self) -> String {
        let drop_kind = match self.kind.to_lowercase().as_str() {
            "window" => "function".to_string(),
            other => other.to_string(),
        };
        format!(
            "drop {} if exists \"{}\".\"{}\" ({});\n",
            drop_kind, self.schema, self.name, self.arguments
        )
    }

    pub fn get_owner_script(&self) -> String {
        if self.owner.is_empty() {
            return String::new();
        }

        let object_kind = match self.kind.to_lowercase().as_str() {
            "procedure" => "procedure",
            "aggregate" => "aggregate",
            _ => "function",
        };

        format!(
            "alter {} \"{}\".\"{}\"({}) owner to \"{}\";\n",
            object_kind,
            self.schema,
            self.name,
            self.arguments,
            self.owner.replace('"', "\"\"")
        )
    }

    /// Generates a unique dollar-quoted delimiter tag for the routine body.
    ///
    /// The base tag is derived from the routine name by keeping only ASCII
    /// alphanumeric characters and replacing all others with underscores,
    /// prefixed with `pgc_` and suffixed with `_body`. If the derived base
    /// is empty, a default `pgc_body` base is used instead. A numeric suffix
    /// is then appended (starting with no suffix) until a `$tag$` delimiter
    /// is found that does not occur anywhere in `self.source_code`, ensuring
    /// the chosen delimiter does not conflict with existing dollar quotes in
    /// the source.
    fn generate_dollar_delimiter(&self) -> String {
        let sanitized = self
            .name
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
            .collect::<String>();
        let base = if sanitized.is_empty() {
            "pgc_body".to_string()
        } else {
            format!("pgc_{}_body", sanitized)
        };

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
        assert_eq!(routine.volatility, "volatile");
        assert!(!routine.is_strict);
        assert!(!routine.is_leakproof);
        assert_eq!(routine.parallel, "unsafe");
        assert!(!routine.security_definer);
        assert!(routine.aggregate_info.is_none());

        let expected_src = format!(
            "{}.{}.{}.{}.{}.{}.{}.{}.{}.{}.{}.{}.{}.{}",
            schema,
            name,
            lang,
            kind,
            return_type,
            arguments,
            "",
            "",
            source_code,
            "volatile",
            false,
            false,
            "unsafe",
            false,
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

        let expected = "create or replace function \"public\".\"add\"(a integer DEFAULT 1) returns integer language plpgsql VOLATILE PARALLEL UNSAFE as $$BEGIN RETURN a + 1; END$$;\n";
        assert_eq!(script, expected);
    }

    #[test]
    fn get_script_includes_owner_when_present() {
        let mut routine = build_function_routine();
        routine.owner = "pgc_owner".to_string();
        routine.hash();

        let expected = "create or replace function \"public\".\"add\"(a integer DEFAULT 1) returns integer language plpgsql VOLATILE PARALLEL UNSAFE as $$BEGIN RETURN a + 1; END$$;\nalter function \"public\".\"add\"(a integer) owner to \"pgc_owner\";\n";
        assert_eq!(routine.get_script(), expected);
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

        let expected = "create or replace function \"data\".\"test\"(fetching_id bigint, fetching_event_id character varying) returns TABLE(row_to_json json) language plpgsql VOLATILE PARALLEL UNSAFE as $$BEGIN RETURN QUERY SELECT row_to_json(t) FROM t; END$$;\n";
        assert_eq!(script, expected);
    }

    #[test]
    fn get_script_for_immutable_strict_function() {
        let mut routine = build_function_routine();
        routine.volatility = "immutable".to_string();
        routine.is_strict = true;
        routine.parallel = "safe".to_string();
        routine.hash();

        let script = routine.get_script();
        assert!(script.contains("IMMUTABLE STRICT PARALLEL SAFE"));
        assert!(!script.contains("VOLATILE"));
    }

    #[test]
    fn get_script_for_stable_leakproof_security_definer() {
        let mut routine = build_function_routine();
        routine.volatility = "stable".to_string();
        routine.is_leakproof = true;
        routine.security_definer = true;
        routine.hash();

        let script = routine.get_script();
        assert!(script.contains("STABLE LEAKPROOF PARALLEL UNSAFE SECURITY DEFINER"));
    }

    #[test]
    fn get_script_procedure_only_emits_security_definer() {
        let mut routine = build_procedure_routine();
        routine.security_definer = true;
        routine.hash();

        let script = routine.get_script();
        assert!(script.contains("SECURITY DEFINER"));
        assert!(!script.contains("VOLATILE"));
        assert!(!script.contains("PARALLEL"));
        assert!(!script.contains("LEAKPROOF"));
    }

    #[test]
    fn get_script_for_aggregate() {
        let mut routine = Routine::new(
            "public".to_string(),
            Oid(200),
            "my_sum".to_string(),
            "internal".to_string(),
            "aggregate".to_string(),
            "bigint".to_string(),
            "integer".to_string(),
            None,
            None,
            "-".to_string(),
        );
        routine.aggregate_info = Some(AggregateInfo {
            sfunc: "int4_sum".to_string(),
            stype: "bigint".to_string(),
            sspace: None,
            finalfunc: None,
            finalfunc_extra: false,
            finalfunc_modify: None,
            combinefunc: None,
            serialfunc: None,
            deserialfunc: None,
            initcond: Some("0".to_string()),
            msfunc: None,
            minvfunc: None,
            mstype: None,
            msspace: None,
            mfinalfunc: None,
            mfinalfunc_extra: false,
            mfinalfunc_modify: None,
            minitcond: None,
            sortop: None,
            kind: 'n',
            num_direct_args: 0,
        });
        routine.hash();

        let script = routine.get_script();
        assert!(script.contains("create aggregate \"public\".\"my_sum\"(integer)"));
        assert!(script.contains("SFUNC = int4_sum"));
        assert!(script.contains("STYPE = bigint"));
        assert!(script.contains("INITCOND = '0'"));
    }

    #[test]
    fn get_drop_script_for_window_uses_function() {
        let routine = Routine::new(
            "public".to_string(),
            Oid(300),
            "my_window".to_string(),
            "plpgsql".to_string(),
            "window".to_string(),
            "integer".to_string(),
            "integer".to_string(),
            None,
            None,
            "BEGIN END".to_string(),
        );
        let script = routine.get_drop_script();
        assert!(script.starts_with("drop function"));
    }

    #[test]
    fn hash_changes_when_volatility_changes() {
        let mut routine = build_function_routine();
        let h1 = routine.hash.clone();

        routine.volatility = "immutable".to_string();
        routine.hash();
        let h2 = routine.hash.clone();

        assert_ne!(h1, h2);
    }

    #[test]
    fn get_aggregate_script_ordered_set() {
        let mut routine = Routine::new(
            "public".to_string(),
            Oid(500),
            "my_percentile".to_string(),
            "internal".to_string(),
            "aggregate".to_string(),
            "double precision".to_string(),
            "double precision, double precision".to_string(),
            None,
            None,
            "-".to_string(),
        );
        routine.aggregate_info = Some(AggregateInfo {
            sfunc: "ordered_set_transition".to_string(),
            stype: "internal".to_string(),
            sspace: None,
            finalfunc: Some("percentile_disc_final".to_string()),
            finalfunc_extra: true,
            finalfunc_modify: None,
            combinefunc: None,
            serialfunc: None,
            deserialfunc: None,
            initcond: None,
            msfunc: None,
            minvfunc: None,
            mstype: None,
            msspace: None,
            mfinalfunc: None,
            mfinalfunc_extra: false,
            mfinalfunc_modify: None,
            minitcond: None,
            sortop: None,
            kind: 'o',
            num_direct_args: 1,
        });
        routine.hash();

        let script = routine.get_script();
        assert!(
            script.contains("create aggregate \"public\".\"my_percentile\"(double precision ORDER BY double precision)"),
            "Expected ordered-set syntax with ORDER BY, got: {}",
            script
        );
        assert!(script.contains("SFUNC = ordered_set_transition"));
        assert!(script.contains("FINALFUNC = percentile_disc_final"));
        assert!(script.contains("FINALFUNC_EXTRA"));
    }

    #[test]
    fn get_aggregate_script_hypothetical_set() {
        let mut routine = Routine::new(
            "public".to_string(),
            Oid(501),
            "my_rank".to_string(),
            "internal".to_string(),
            "aggregate".to_string(),
            "bigint".to_string(),
            "\"any\", \"any\"".to_string(),
            None,
            None,
            "-".to_string(),
        );
        routine.aggregate_info = Some(AggregateInfo {
            sfunc: "hypothetical_rank_sfunc".to_string(),
            stype: "internal".to_string(),
            sspace: None,
            finalfunc: Some("hypothetical_rank_final".to_string()),
            finalfunc_extra: true,
            finalfunc_modify: None,
            combinefunc: None,
            serialfunc: None,
            deserialfunc: None,
            initcond: None,
            msfunc: None,
            minvfunc: None,
            mstype: None,
            msspace: None,
            mfinalfunc: None,
            mfinalfunc_extra: false,
            mfinalfunc_modify: None,
            minitcond: None,
            sortop: None,
            kind: 'h',
            num_direct_args: 1,
        });
        routine.hash();

        let script = routine.get_script();
        assert!(
            script.contains("create aggregate \"public\".\"my_rank\"(\"any\" ORDER BY \"any\")"),
            "Expected hypothetical-set syntax with ORDER BY, got: {}",
            script
        );
        assert!(script.contains("HYPOTHETICAL"));
    }

    #[test]
    fn get_aggregate_script_ordered_set_no_direct_args() {
        let mut routine = Routine::new(
            "public".to_string(),
            Oid(502),
            "my_mode".to_string(),
            "internal".to_string(),
            "aggregate".to_string(),
            "anyelement".to_string(),
            "anyelement".to_string(),
            None,
            None,
            "-".to_string(),
        );
        routine.aggregate_info = Some(AggregateInfo {
            sfunc: "ordered_set_transition".to_string(),
            stype: "internal".to_string(),
            sspace: None,
            finalfunc: Some("mode_final".to_string()),
            finalfunc_extra: true,
            finalfunc_modify: None,
            combinefunc: None,
            serialfunc: None,
            deserialfunc: None,
            initcond: None,
            msfunc: None,
            minvfunc: None,
            mstype: None,
            msspace: None,
            mfinalfunc: None,
            mfinalfunc_extra: false,
            mfinalfunc_modify: None,
            minitcond: None,
            sortop: None,
            kind: 'o',
            num_direct_args: 0,
        });
        routine.hash();

        let script = routine.get_script();
        assert!(
            script.contains("create aggregate \"public\".\"my_mode\"(ORDER BY anyelement)"),
            "Expected ordered-set syntax with no direct args, got: {}",
            script
        );
    }
}
