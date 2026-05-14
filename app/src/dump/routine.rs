use serde::{Deserialize, Serialize};
use sqlx::postgres::types::Oid;

use crate::utils::string_extensions::StringExt;

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
    /// Configuration parameters set via SET (e.g. search_path, lock_timeout).
    /// Each entry is in "name=value" format, matching pg_proc.proconfig.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub config: Vec<String>,
    /// For aggregate functions: the aggregate definition details.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aggregate_info: Option<AggregateInfo>,
    /// Estimated execution cost (per row) — COST clause.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost: Option<f64>,
    /// Estimated number of result rows — ROWS clause (set-returning functions only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rows: Option<f64>,
    /// Planner support function — SUPPORT clause.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub support_function: Option<String>,
    /// Transform types — TRANSFORM FOR TYPE clause.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub transform_types: Vec<String>,
    /// The hash of the routine.
    pub hash: Option<String>,
    /// ACL (grant) entries for this routine
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub acl: Vec<String>,
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
            source_code: crate::utils::string_extensions::normalize_line_endings(source_code),
            volatility: "volatile".to_string(),
            is_strict: false,
            is_leakproof: false,
            parallel: "unsafe".to_string(),
            security_definer: false,
            config: Vec::new(),
            aggregate_info: None,
            cost: None,
            rows: None,
            support_function: None,
            transform_types: Vec::new(),
            hash: None,
            acl: Vec::new(),
        };
        routine.hash();
        routine
    }

    /// Hash
    pub fn hash(&mut self) {
        let agg_repr = match &self.aggregate_info {
            Some(agg) => format!(
                "{}.{}.{}",
                agg.kind,
                agg.num_direct_args,
                agg.get_options_body()
            ),
            None => String::new(),
        };
        let config_repr = self.config.join(",");
        let cost_repr = self.cost.map_or(String::new(), |c| c.to_string());
        let rows_repr = self.rows.map_or(String::new(), |r| r.to_string());
        let support_repr = self.support_function.clone().unwrap_or_default();
        let transform_repr = self.transform_types.join(",");
        // `arguments_defaults` participates in the hash so a
        // defaults-only diff is *detected* — `Comparer::emit_routine_
        // diff` keys off `hashes_differ` to decide whether to emit
        // anything at all. The actual migration uses `CREATE OR
        // REPLACE FUNCTION`, NOT `DROP FUNCTION ... CASCADE`:
        // PostgreSQL accepts default-argument changes via the
        // OR REPLACE form when the identity argument types and return
        // type are unchanged. PR #187 review (C11) corrected the
        // earlier wording here that misleadingly framed defaults as
        // a DROP+CREATE requirement.
        let defaults_repr = self.arguments_defaults.clone().unwrap_or_default();
        let src = format!(
            "{}.{}.{}.{}.{}.{}.{}.{}.{}.{}.{}.{}.{}.{}.{}.{}.{}.{}.{}.{}.{}",
            self.schema,
            self.name,
            self.lang,
            self.kind,
            self.return_type,
            self.arguments,
            defaults_repr,
            self.owner,
            self.comment.clone().unwrap_or_default(),
            self.source_code,
            self.volatility,
            self.is_strict,
            self.is_leakproof,
            self.parallel,
            self.security_definer,
            config_repr,
            agg_repr,
            cost_repr,
            rows_repr,
            support_repr,
            transform_repr,
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

        // COST (not valid for procedures)
        if kind != "procedure"
            && let Some(cost) = self.cost
        {
            // Default cost is 1 for C/internal, 100 for others; always emit if set
            flags.push(format!("COST {cost}"));
        }

        // ROWS (only for set-returning functions, not procedures)
        if kind != "procedure"
            && let Some(rows) = self.rows
            && rows > 0.0
        {
            flags.push(format!("ROWS {rows}"));
        }

        // SUPPORT function (not valid for procedures)
        if kind != "procedure"
            && let Some(ref support) = self.support_function
        {
            flags.push(format!("SUPPORT {support}"));
        }

        if flags.is_empty() {
            String::new()
        } else {
            format!(" {}", flags.join(" "))
        }
    }

    /// Builds the SET configuration clauses from proconfig entries.
    fn get_config_clause(&self) -> String {
        if self.config.is_empty() {
            return String::new();
        }
        let mut parts = Vec::new();
        for entry in &self.config {
            if let Some(pos) = entry.find('=') {
                let name = entry[..pos].trim();
                let value = entry[pos + 1..].trim();
                // For list-valued GUCs (e.g. search_path), proconfig stores values
                // with double-quote delimiters (e.g. "public, pg_temp"). These must
                // NOT be wrapped in single quotes because that would turn them into
                // a string literal, changing the semantics (the comma becomes part of
                // a single identifier instead of separating list elements).
                // Use the value verbatim when it contains double quotes; otherwise
                // wrap in single quotes as a safe string literal.
                if value.contains('"') {
                    parts.push(format!(" SET {name} = {value}"));
                } else {
                    parts.push(format!(" SET {name} = '{}'", value.replace('\'', "''")));
                }
            }
        }
        parts.join("")
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
        let config = self.get_config_clause();
        let transform = if self.transform_types.is_empty() {
            String::new()
        } else {
            format!(
                " TRANSFORM {}",
                self.transform_types
                    .iter()
                    .map(|t| format!("FOR TYPE {t}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };

        // For window functions, use CREATE FUNCTION (WINDOW is a flag, not a kind)
        let create_kind = if kind == "window" { "function" } else { &kind };

        let script_body = match kind.as_str() {
            "procedure" => format!(
                "create or replace procedure {}.{}({}) language {}{transform}{flags}{config} as {d}{body}{d};",
                self.schema,
                self.name,
                arguments_with_defaults,
                self.lang,
                transform = transform,
                flags = flags,
                config = config,
                d = delimiter,
                body = self.source_code
            ).with_empty_lines(),
            _ => format!(
                "create or replace {create_kind} {}.{}({}) returns {} language {}{transform}{flags}{config} as {d}{body}{d};",
                self.schema,
                self.name,
                arguments_with_defaults,
                self.return_type,
                self.lang,
                create_kind = create_kind,
                transform = transform,
                flags = flags,
                config = config,
                d = delimiter,
                body = self.source_code
            ).with_empty_lines(),
        };

        let mut script = script_body;

        if let Some(comment) = &self.comment {
            let object_kind = match kind.as_str() {
                "procedure" => "procedure",
                _ => "function",
            };
            script.append_block(&format!(
                "comment on {object_kind} {}.{}({}) is '{}';",
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

    /// Splits a comma-separated string respecting parenthesized groups and
    /// single-quoted string literals.
    /// E.g. "a numeric(10,2), b text"    → ["a numeric(10,2)", " b text"]
    /// E.g. "','::character varying, 0"  → ["','::character varying", " 0"]
    ///
    /// Dollar-quoted strings (`$$...$$`) are intentionally not handled: both
    /// call sites receive output from `pg_get_function_identity_arguments` or
    /// `pg_get_expr(proargdefaults, 0)`, which always produce single-quoted
    /// expressions and never emit dollar-quotes.
    fn split_arguments(s: &str) -> Vec<String> {
        let mut parts = Vec::new();
        let mut depth = 0;
        let mut in_quote = false;
        let mut current = String::new();
        let mut iter = s.chars().peekable();

        while let Some(ch) = iter.next() {
            if in_quote {
                // Inside a single-quoted string literal.
                // Two consecutive single quotes represent an escaped quote ('').
                if ch == '\'' {
                    if iter.peek() == Some(&'\'') {
                        // Escaped quote: consume both characters and stay in-quote.
                        current.push('\'');
                        current.push(iter.next().unwrap()); // safe: peek() confirmed Some above
                    } else {
                        // Closing quote.
                        in_quote = false;
                        current.push(ch);
                    }
                } else {
                    current.push(ch);
                }
                continue;
            }

            // Outside any string literal.
            match ch {
                '\'' => {
                    in_quote = true;
                    current.push(ch);
                }
                '(' => {
                    depth += 1;
                    current.push(ch);
                }
                ')' => {
                    depth -= 1;
                    current.push(ch);
                }
                ',' if depth == 0 => {
                    parts.push(std::mem::take(&mut current));
                }
                _ => {
                    current.push(ch);
                }
            }
        }

        if !current.is_empty() {
            parts.push(current);
        }
        debug_assert!(
            !in_quote,
            "split_arguments: unclosed single-quote in input: {s:?}"
        );
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

            script.append_block(&format!(
                "create aggregate {}.{}({}) (\n{}\n);",
                self.schema,
                self.name,
                args,
                agg.get_options_body()
            ));
        } else {
            // Fallback: no aggregate_info available, emit a comment
            script.push_str(&format!(
                "/* aggregate {}.{}({}) — aggregate details unavailable */\n",
                self.schema, self.name, self.arguments
            ));
        }

        if let Some(comment) = &self.comment {
            script.append_block(&format!(
                "comment on aggregate {}.{}({}) is '{}';",
                self.schema,
                self.name,
                self.signature_args(),
                comment.replace('\'', "''")
            ));
        }

        script.push_str(&self.get_owner_script());
        script
    }

    /// Returns the argument signature suitable for SQL statements.
    ///
    /// For aggregates with an empty argument list PostgreSQL requires `(*)`
    /// rather than `()`, so this helper centralises that logic.
    fn signature_args(&self) -> String {
        if self.kind.to_lowercase() == "aggregate" && self.arguments.is_empty() {
            "*".to_string()
        } else {
            self.arguments.clone()
        }
    }

    /// Returns a string to drop the routine.
    pub fn get_drop_script(&self) -> String {
        let drop_kind = match self.kind.to_lowercase().as_str() {
            "window" => "function".to_string(),
            other => other.to_string(),
        };
        format!(
            "drop {} if exists {}.{} ({}) cascade;",
            drop_kind,
            self.schema,
            self.name,
            self.signature_args()
        )
        .with_empty_lines()
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
            "alter {} {}.{}({}) owner to {};",
            object_kind,
            self.schema,
            self.name,
            self.signature_args(),
            self.owner
        )
        .with_empty_lines()
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
#[path = "routine_tests.rs"]
mod tests;
