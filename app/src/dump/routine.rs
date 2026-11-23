//! Functions, procedures and aggregates (`pg_proc`).
//!
//! Routines are compared by their full signature, not just their name: overloads
//! are distinct objects, and an argument- or return-type change forces a drop and
//! recreate rather than `CREATE OR REPLACE`. Because `DROP FUNCTION … CASCADE`
//! silently takes dependent indexes, constraints, generated columns, defaults,
//! policies, views, rules, triggers, domain constraints and extended statistics
//! with it — and routines whose argument defaults call it, and, through a domain
//! default calling it, the domain itself — the comparer re-emits those afterwards,
//! detaching such a domain default first — see
//! [`column_dependent`](super::column_dependent).
//!
//! A body is either a string (`AS $$ … $$`, kept in `pg_proc.prosrc`) or, from
//! PostgreSQL 14 on, a SQL-standard body (`BEGIN ATOMIC … END` / `RETURN expr`),
//! which the server parses when the routine is created and stores as a query
//! tree, leaving `prosrc` empty. The latter is dumped as `pg_get_function_sqlbody`
//! deparses it under an empty `search_path` and emitted verbatim after every
//! option, as `pg_dump` does — see [`Routine::is_sql_body`]. Unlike a string
//! body it is a hard dependent of everything it names, which
//! [`SqlBodyDependencies`] records.

use serde::{Deserialize, Serialize};
use sqlx::postgres::types::Oid;

use crate::utils::string_extensions::{StringExt, block_comment_safe, quote_identifier};

/// GUC parameters flagged `GUC_LIST_QUOTE` in PostgreSQL. Their `proconfig` value is
/// stored as an already-quoted comma-separated list rather than a plain scalar, so a
/// `SET` clause must not wrap it in a single string literal — see
/// `Routine::list_quote_set_clause` for how it is emitted instead.
///
/// This mirrors the `GUC_LIST_QUOTE` set of PostgreSQL 19's `guc_parameters.dat`,
/// checked live on 14, 18 and 19 by storing `'A b', 'c'` and reading back
/// `"A b", c`. `output_plugin_libraries` (`PGC_SUSET`) is also in the current minor
/// releases of 14–18. `oauth_validator_libraries` exists from 18 on and is
/// `PGC_SIGHUP` ("cannot be changed now" in a `SET` clause), so no routine can carry
/// it; it is listed so the classification does not depend on that.
const LIST_QUOTE_GUCS: [&str; 8] = [
    "search_path",
    "temp_tablespaces",
    "session_preload_libraries",
    "shared_preload_libraries",
    "local_preload_libraries",
    "unix_socket_directories",
    "output_plugin_libraries",
    "oauth_validator_libraries",
];

/// `NAMEDATALEN - 1`: the byte length at which PostgreSQL's lexer truncates an
/// identifier, quoted or not.
const MAX_IDENTIFIER_BYTES: usize = 63;

/// First `server_version_num` that accepts `SET <list GUC> TO NULL` — the only
/// spelling of an empty list, stored in `proconfig` as a bare `name=` (PostgreSQL 19).
pub const SET_LIST_TO_NULL_MIN_VERSION: i32 = 190_000;

/// First `server_version_num` that stores an aggregate's planner support
/// function (`CREATE AGGREGATE ... (SUPPORT = ...)`, PostgreSQL 19). Older
/// servers accept the option with a `WARNING` and drop it.
pub const AGGREGATE_SUPPORT_MIN_VERSION: i32 = 190_000;

/// Whether `name` is a GUC whose `proconfig` value is a quote-preserving list.
/// GUC names are case-insensitive, matching PostgreSQL's own lookup.
fn is_list_quote_guc(name: &str) -> bool {
    LIST_QUOTE_GUCS
        .iter()
        .any(|guc| guc.eq_ignore_ascii_case(name))
}

/// One element of a `GUC_LIST_QUOTE` value, as `split_guc_list` isolates it.
#[derive(Debug, PartialEq, Eq)]
struct GucListElement {
    /// The element's text, with the surrounding double quotes removed and `""`
    /// collapsed to `"`. Never downcased or truncated.
    value: String,
    /// Whether the element was double-quoted in the stored list.
    quoted: bool,
}

impl GucListElement {
    /// Whether this element survives being pasted back into a `SET` clause as the
    /// token it was stored as — the verbatim form pgc has always emitted — and
    /// is stored back identically.
    ///
    /// `flatten_set_variable_args` stores each element of a list either
    /// double-quoted (a string, run through `quote_identifier`), as a bare
    /// identifier, or as the bare text of a numeric constant: `SET search_path =
    /// public, 42` stores `public, 42`. Pasted back, a quoted or bare identifier is
    /// re-lexed as an identifier, and a number as the same number.
    ///
    /// Three things break that, and each needs the string-literal form instead:
    /// the lexer rejects a zero-length quoted identifier (`""`), it truncates every
    /// identifier (but not a number) to [`MAX_IDENTIFIER_BYTES`], and an unquoted
    /// element that is neither an identifier nor a number (`$user`, `my-schema`,
    /// which only `SET … FROM CURRENT` can store) is a syntax error. Keywords are
    /// not checked: when the value is stored, `quote_identifier` already
    /// double-quotes every keyword that is not unreserved.
    fn survives_verbatim(&self) -> bool {
        if self.quoted {
            !self.value.is_empty() && self.value.len() <= MAX_IDENTIFIER_BYTES
        } else {
            is_numeric_constant(&self.value)
                || (self.value.len() <= MAX_IDENTIFIER_BYTES && is_bare_identifier(&self.value))
        }
    }
}

/// Whether `s` is a numeric constant as `flatten_set_variable_args` stores one: the
/// `%d` of an integer, or a float's text exactly as the lexer read it (an integer
/// too large for `int4` is a float too), optionally negated with a leading `-`.
///
/// Accepts every form of `scan.l`'s `decinteger`, `numeric` and `real`, and, for
/// the text of an out-of-range integer, `hexinteger`, `octinteger` and
/// `bininteger` — including the `_` digit separators of PostgreSQL 16. Only a
/// server whose lexer produced such text can have stored it, so the same text
/// re-lexes to the same constant there.
fn is_numeric_constant(s: &str) -> bool {
    let s = s.strip_prefix('-').unwrap_or(s);
    let prefixed_radix = match s.get(..2) {
        Some("0x" | "0X") => Some(16),
        Some("0o" | "0O") => Some(8),
        Some("0b" | "0B") => Some(2),
        _ => None,
    };
    if let Some(radix) = prefixed_radix {
        // `0[xX](_?{hexdigit})+`: a separator may follow the prefix.
        let digits = &s[2..];
        let len = scan_digits(digits, radix, true);
        return len > 0 && len == digits.len();
    }

    // `{decinteger}`, `{numeric}` = `{decinteger}\.{decinteger}?` | `\.{decinteger}`,
    // each optionally followed by the `[Ee][-+]?{decinteger}` exponent of `{real}`.
    let integer = scan_digits(s, 10, false);
    let mut end = integer;
    if s[end..].starts_with('.') {
        let fraction = scan_digits(&s[end + 1..], 10, false);
        if integer == 0 && fraction == 0 {
            return false;
        }
        end += 1 + fraction;
    } else if integer == 0 {
        return false;
    }
    if s[end..].starts_with(['e', 'E']) {
        end += 1;
        if s[end..].starts_with(['+', '-']) {
            end += 1;
        }
        let exponent = scan_digits(&s[end..], 10, false);
        if exponent == 0 {
            return false;
        }
        end += exponent;
    }
    end == s.len()
}

/// The byte length of the longest prefix of `s` made of `radix` digits with single
/// `_` separators between them, as `scan.l` writes `{decdigit}(_?{decdigit})*`; with
/// `leading_separator`, `(_?{digit})+`. 0 when `s` does not start with one.
fn scan_digits(s: &str, radix: u32, leading_separator: bool) -> usize {
    let bytes = s.as_bytes();
    let mut end = 0;
    loop {
        let mut next = end;
        if bytes.get(next) == Some(&b'_') && (end > 0 || leading_separator) {
            next += 1;
        }
        match bytes.get(next) {
            Some(&b) if char::from(b).is_digit(radix) => end = next + 1,
            _ => return end,
        }
    }
}

/// Whether `s` lexes as one unquoted SQL identifier (`scan.l`'s `ident_start`
/// followed by `ident_cont`). Case is not checked: the lexer downcases an unquoted
/// identifier, exactly as `search_path` itself would when it is parsed.
fn is_bare_identifier(s: &str) -> bool {
    let mut chars = s.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    (first.is_ascii_alphabetic() || first == '_' || !first.is_ascii())
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '$' || !c.is_ascii())
}

/// Whitespace as PostgreSQL's `scanner_isspace` defines it.
fn is_scanner_space(c: char) -> bool {
    matches!(c, ' ' | '\t' | '\n' | '\r' | '\u{0b}' | '\u{0c}')
}

/// Splits a `GUC_LIST_QUOTE` value the way PostgreSQL's `SplitGUCList` does: a
/// comma-separated list whose elements are either double-quoted (with `""` standing
/// for one `"`) or run to the next comma or whitespace, never downcased or
/// truncated. Whitespace around elements and commas is skipped.
///
/// An empty or all-whitespace value is the empty list. Returns `None` where
/// `SplitGUCList` fails: an unterminated quote, an empty unquoted element (`a,,b`)
/// or anything but a comma after an element.
fn split_guc_list(value: &str) -> Option<Vec<GucListElement>> {
    let mut elements = Vec::new();
    let mut chars = value.chars().peekable();
    while chars.next_if(|&c| is_scanner_space(c)).is_some() {}
    if chars.peek().is_none() {
        return Some(elements);
    }
    loop {
        let element = if chars.next_if_eq(&'"').is_some() {
            let mut text = String::new();
            loop {
                match chars.next()? {
                    '"' if chars.next_if_eq(&'"').is_some() => text.push('"'),
                    '"' => break,
                    c => text.push(c),
                }
            }
            GucListElement {
                value: text,
                quoted: true,
            }
        } else {
            let mut text = String::new();
            while let Some(c) = chars.next_if(|&c| c != ',' && !is_scanner_space(c)) {
                text.push(c);
            }
            if text.is_empty() {
                return None;
            }
            GucListElement {
                value: text,
                quoted: false,
            }
        };
        elements.push(element);
        while chars.next_if(|&c| is_scanner_space(c)).is_some() {}
        match chars.next() {
            None => return Some(elements),
            Some(',') => while chars.next_if(|&c| is_scanner_space(c)).is_some() {},
            Some(_) => return None,
        }
    }
}

/// `s` as a single-quoted SQL string literal.
fn quote_literal(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// Information about a PostgreSQL aggregate function from pg_aggregate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregateInfo {
    /// Transition function (SFUNC)
    pub sfunc: String,
    /// State data type (STYPE)
    pub stype: String,
    /// State data space (SSPACE, `aggtransspace`). 0 means "use the default
    /// estimate"; a negative value marks a state that can grow without bound, which
    /// the PostgreSQL 19 planner reads (earlier versions accept and store it too).
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
    /// Moving-aggregate state space (MSSPACE, `aggmtransspace`). 0 means the
    /// default; any other value, negative included, is emitted as stored.
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

    /// Generates the option list of the aggregate as [`Routine::hash`] folds it in.
    ///
    /// Because it feeds the hash, this text must only change when a stored value
    /// would otherwise be lost, and it is not quite the list `CREATE AGGREGATE`
    /// gets: the script builder writes `SORTOP` as `OPERATOR(…)` (see
    /// `get_script_options_body`) and appends `SUPPORT` and `PARALLEL`, which the
    /// hash already covers through their own `Routine` fields. `SSPACE` /
    /// `MSSPACE` are written whenever non-zero, as `pg_dump` does — a negative
    /// value (an unbounded state, meaningful from PostgreSQL 19) used to be
    /// dropped, so only aggregates carrying one hash differently from pgc 1.0.x.
    pub fn get_options_body(&self) -> String {
        self.options_body(false)
    }

    /// The parenthesized option list of `CREATE AGGREGATE`: [`get_options_body`]
    /// with `SORTOP` spelled `OPERATOR(schema.op)`. The grammar takes a bare
    /// operator there but not a schema-qualified one, and `regoper` output is
    /// qualified whenever the name alone is ambiguous (`pg_catalog.>`), so the
    /// plain form was a syntax error.
    ///
    /// [`get_options_body`]: AggregateInfo::get_options_body
    fn get_script_options_body(&self) -> String {
        self.options_body(true)
    }

    fn options_body(&self, sortop_as_operator: bool) -> String {
        let mut opts: Vec<String> = Vec::new();
        opts.push(format!("    SFUNC = {}", self.sfunc));
        opts.push(format!("    STYPE = {}", self.stype));
        if let Some(ref v) = self.sspace
            && *v != 0
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
            && *v != 0
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
            if sortop_as_operator {
                opts.push(format!("    SORTOP = OPERATOR({v})"));
            } else {
                opts.push(format!("    SORTOP = {v}"));
            }
        }
        if self.kind == 'h' {
            opts.push("    HYPOTHETICAL".to_string());
        }
        opts.join(",\n")
    }
}

/// What a SQL-standard body (`BEGIN ATOMIC … END` / `RETURN expr`) uses, as
/// `pg_depend` records it: the `deptype = 'n'` rows of the routine.
///
/// PostgreSQL resolves such a body when the routine is created and keeps a
/// hard dependency on every relation, column, routine and operator in it, so
/// none of them can be dropped, and no column of it retyped, while the
/// routine exists — and none of them can be missing when it is created, even
/// with `check_function_bodies` off. The comparer reads these lists to drop
/// such a routine before the objects it uses change and re-create it after,
/// and to create it after the routines and operators it uses; the `clear`
/// script reads them to drop it before the relations it reads.
///
/// Empty for a string body, which records none of this, and in dumps written
/// before the field existed. It never enters [`Routine::hash`]: it only
/// decides what else a detected change drags with it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqlBodyDependencies {
    /// Relations the body reads or writes — tables, partitioned tables, views,
    /// materialized views, foreign tables and sequences (`nextval('s'::regclass)`)
    /// — as unquoted `schema.relation`, the form of
    /// [`View::table_relation`](super::view::View::table_relation).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub relations: Vec<String>,
    /// The individual columns among them, as unquoted `schema.relation.column`,
    /// the form of [`View::column_relation`](super::view::View::column_relation).
    /// PostgreSQL refuses to drop or retype exactly these.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub columns: Vec<String>,
    /// Routines the body calls, including the function behind a cast it
    /// applies, as `schema.name(identity arguments)` in the routine query's
    /// own quoting — the form [`Routine::dependency_key`] builds and a cast's
    /// `function_name` carries.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub routines: Vec<String>,
    /// Operators the body applies, as `schema.name(left, right)` with `NONE`
    /// for a missing operand — the key the comparer matches operators by.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub operators: Vec<String>,
}

impl SqlBodyDependencies {
    /// `true` when nothing was recorded (a string body, or a dump that
    /// predates the field).
    pub fn is_empty(&self) -> bool {
        self.relations.is_empty()
            && self.columns.is_empty()
            && self.routines.is_empty()
            && self.operators.is_empty()
    }
}

/// This is an information about a PostgreSQL routine.
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
    /// The routine body: `pg_proc.prosrc` for a string body (`AS $$ … $$`), or,
    /// when [`is_sql_body`](Routine::is_sql_body) is set, the SQL-standard body
    /// as `pg_get_function_sqlbody` deparses it (`BEGIN ATOMIC … END` /
    /// `RETURN expr`). Either way it is the text the dependency scans read.
    pub source_code: String,
    /// `true` when [`source_code`](Routine::source_code) is a SQL-standard body
    /// (`pg_proc.prosqlbody`, PostgreSQL 14+) rather than a string body. Such a
    /// routine has an empty `prosrc`; its body is emitted verbatim after every
    /// option, with no `AS`, and is not CRLF-normalised: the deparser lays it
    /// out with `\n` only, so a carriage return in it belongs to a literal.
    /// `false` in dumps written by pgc 1.0.x, which dumped such bodies empty.
    ///
    /// The body is deparsed with an empty `search_path`, as `pg_dump` does, so
    /// every name outside `pg_catalog` in it is schema-qualified — including
    /// `public` ones — whatever the dump session's or the database's
    /// `search_path` is.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub is_sql_body: bool,
    /// What the SQL-standard body uses (see [`SqlBodyDependencies`]); empty
    /// unless [`is_sql_body`](Routine::is_sql_body) is set. Not hashed.
    #[serde(default, skip_serializing_if = "SqlBodyDependencies::is_empty")]
    pub sql_body_dependencies: SqlBodyDependencies,
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
    /// `proacl` is an empty array rather than NULL: every privilege was
    /// revoked, the owner's own included. Both dump as an empty
    /// [`Routine::acl`], but NULL stands for the default privileges, which give
    /// PUBLIC `EXECUTE`, so the grants comparison has to tell them apart
    /// (`acl::effective_acl`). Not hashed: [`Routine::hash`] leaves the ACL to
    /// that comparison. Absent (`false`) from dumps written by pgc 1.0.x,
    /// which could not tell the two apart.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub acl_explicitly_empty: bool,
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
            is_sql_body: false,
            sql_body_dependencies: SqlBodyDependencies::default(),
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
            acl_explicitly_empty: false,
        };
        routine.hash();
        routine
    }

    /// `schema.name(identity arguments)`: the form in which
    /// [`SqlBodyDependencies::routines`] and a cast's `function_name` name
    /// this routine.
    pub fn dependency_key(&self) -> String {
        format!("{}.{}({})", self.schema, self.name, self.arguments)
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
        let mut src = format!(
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
        // Folded in only when set, so the hash of every string-body routine is
        // the one pgc 1.0.x computed. It tells a SQL-standard body apart from a
        // string body of the same text, a switch `CREATE OR REPLACE` performs.
        if self.is_sql_body {
            src.push_str(".sqlbody");
        }
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
    ///
    /// `set_list_to_null` says whether the migrated server accepts
    /// `SET <list GUC> TO NULL` (see [`Routine::get_script_for_server`]).
    fn get_config_clause(&self, set_list_to_null: bool) -> String {
        let mut clause = String::new();
        for entry in &self.config {
            // Only the first '=' separates the name: values may contain one too.
            let Some((name, value)) = entry.split_once('=') else {
                continue;
            };
            let (name, value) = (name.trim(), value.trim());
            if is_list_quote_guc(name) {
                clause.push_str(&Self::list_quote_set_clause(name, value, set_list_to_null));
            } else {
                // Any other GUC holds a plain scalar — or, for a list GUC without
                // GUC_LIST_QUOTE (DateStyle, log_min_messages on 19, …), a list that
                // PostgreSQL stores unquoted — so one string literal re-stores it
                // exactly. That is also correct for a value containing a double
                // quote, and for an empty one.
                clause.push_str(&format!(" SET {name} = {}", quote_literal(value)));
            }
        }
        clause
    }

    /// The `SET` clause for the stored `value` of a `GUC_LIST_QUOTE` parameter
    /// (`search_path`, `temp_tablespaces`, …), whose `proconfig` text is already a
    /// canonical comma-separated list with each element quoted as needed.
    ///
    /// Wrapping that whole list in one string literal would make `t, pg_temp` a
    /// single schema named "t, pg_temp" (issue #217), so the list is emitted
    /// verbatim — `SET search_path = t, pg_temp`, pgc's long-standing output —
    /// whenever every element survives being re-lexed as the identifier or number
    /// it was stored as. When one does not (the `""` that `SET search_path = ''`
    /// stores, an identifier longer than 63 bytes), each element becomes a string
    /// literal instead, much as `pg_get_functiondef` prints it: `SET search_path
    /// TO '', 'public'`. Unlike there, a numeric element stays a bare number
    /// (`TO '', 42`), because a literal would be stored back as `"42"`.
    ///
    /// The empty list is stored as a bare `name=` and can only be written
    /// `SET name TO NULL`, which PostgreSQL 19 introduced. Where the migrated server
    /// is not known to accept that, the clause falls back to `= ''`, the closest
    /// form every version accepts (it stores `""`, which behaves the same).
    fn list_quote_set_clause(name: &str, value: &str, set_list_to_null: bool) -> String {
        match split_guc_list(value) {
            Some(elements) if elements.is_empty() => {
                if set_list_to_null {
                    format!(" SET {name} TO NULL")
                } else {
                    format!(" SET {name} = ''")
                }
            }
            Some(elements) if !elements.iter().all(GucListElement::survives_verbatim) => {
                // A number stays bare: as a literal it would be stored back
                // double-quoted (`"42"`, not `42`).
                let literals: Vec<String> = elements
                    .iter()
                    .map(|e| {
                        if !e.quoted && is_numeric_constant(&e.value) {
                            e.value.clone()
                        } else {
                            quote_literal(&e.value)
                        }
                    })
                    .collect();
                format!(" SET {name} TO {}", literals.join(", "))
            }
            // Every element survives verbatim — or the value is not a list
            // SplitGUCList accepts (only `SET … FROM CURRENT` can store one), where
            // pasting it back is the best that can be done.
            _ => format!(" SET {name} = {value}"),
        }
    }

    /// Returns a string to create the routine, for a migrated server that is not
    /// known to accept PostgreSQL 19 syntax. Same as
    /// [`get_script_for_server(false)`](Routine::get_script_for_server).
    pub fn get_script(&self) -> String {
        self.get_script_for_server(false)
    }

    /// Returns a string to create the routine.
    ///
    /// `set_list_to_null` says whether the server the script will run on accepts
    /// `SET <list GUC> TO NULL` — `server_version_num` at least
    /// [`SET_LIST_TO_NULL_MIN_VERSION`]. It only matters for a routine whose
    /// `proconfig` holds an empty list; every other routine gets the same script
    /// either way.
    pub fn get_script_for_server(&self, set_list_to_null: bool) -> String {
        let kind = self.kind.to_lowercase();

        // Aggregate functions use a completely different CREATE syntax.
        if kind == "aggregate" {
            return self.get_aggregate_script();
        }

        // A SQL-standard body is part of the statement, not a string: it goes
        // after every option, verbatim and without `AS` (as pg_dump emits it).
        // A string body is dollar-quoted, with a tag the body does not contain.
        let body_clause = if self.is_sql_body {
            format!(" {}", self.source_code)
        } else {
            let delimiter = if self.source_code.contains("$$") {
                self.generate_dollar_delimiter()
            } else {
                "$$".to_string()
            };
            format!(" as {d}{body}{d}", d = delimiter, body = self.source_code)
        };

        let arguments_with_defaults = self.arguments_with_defaults();
        let flags = self.get_flags_clause();
        let config = self.get_config_clause(set_list_to_null);
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
        // The dump stores `pg_language.lanname` raw; every language PostgreSQL
        // ships (`sql`, `plpgsql`, `c`, `internal`) stays bare.
        let language = quote_identifier(&self.lang);

        let script_body = match kind.as_str() {
            "procedure" => format!(
                "create or replace procedure {}.{}({}) language {}{transform}{flags}{config}{body_clause};",
                self.schema,
                self.name,
                arguments_with_defaults,
                language,
                transform = transform,
                flags = flags,
                config = config,
                body_clause = body_clause
            ).with_empty_lines(),
            _ => format!(
                "create or replace {create_kind} {}.{}({}) returns {} language {}{transform}{flags}{config}{body_clause};",
                self.schema,
                self.name,
                arguments_with_defaults,
                self.return_type,
                language,
                create_kind = create_kind,
                transform = transform,
                flags = flags,
                config = config,
                body_clause = body_clause
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

    /// Returns the CREATE OR REPLACE AGGREGATE script.
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

            // SUPPORT and PARALLEL live on the aggregate's own pg_proc row, not in
            // pg_aggregate, and are hashed through their Routine fields — so they are
            // appended here rather than in `get_options_body`, whose text feeds the
            // hash and must stay stable. Omitting either resets it: CREATE OR REPLACE
            // AGGREGATE rewrites every option it is not given.
            let mut options = agg.get_script_options_body();
            if let Some(ref support) = self.support_function {
                // PostgreSQL 19+ (`AGGREGATE_SUPPORT_MIN_VERSION`); older servers
                // accept the statement with a WARNING and ignore the option, and
                // never store one to begin with. `Comparer` leaves it out, with a
                // note, when it knows the migrated server is older.
                options.push_str(&format!(",\n    SUPPORT = {support}"));
            }
            match self.parallel.as_str() {
                "safe" => options.push_str(",\n    PARALLEL = SAFE"),
                "restricted" => options.push_str(",\n    PARALLEL = RESTRICTED"),
                _ => {}
            }

            // OR REPLACE (PostgreSQL 12+) so a changed aggregate is updated in place,
            // keeping its OID and so every view that uses it. It cannot change the
            // routine kind, aggregate kind, direct-argument count, argument types or
            // result type; `Comparer` drops the aggregate first when one of those
            // differs.
            script.append_block(&format!(
                "create or replace aggregate {}.{}({}) (\n{}\n);",
                self.schema, self.name, args, options
            ));
        } else {
            // Fallback: no aggregate_info available, emit a comment
            script.push_str(&format!(
                "/* aggregate {}.{}({}) — aggregate details unavailable */\n",
                block_comment_safe(&self.schema),
                block_comment_safe(&self.name),
                block_comment_safe(&self.arguments)
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

    /// This aggregate as a server older than PostgreSQL 19 can hold it: the
    /// same routine without its planner support function, rehashed. `None`
    /// for anything but an aggregate that has one — the only routine an older
    /// server cannot represent in full (see [`AGGREGATE_SUPPORT_MIN_VERSION`]).
    pub fn without_aggregate_support(&self) -> Option<Routine> {
        if self.aggregate_info.is_none() || self.support_function.is_none() {
            return None;
        }
        let mut stripped = self.clone();
        stripped.support_function = None;
        stripped.hash();
        Some(stripped)
    }

    /// This routine as a server older than PostgreSQL 19 holds it: every
    /// `GUC_LIST_QUOTE` parameter set to the empty list — a bare `name=`,
    /// which only `SET name TO NULL` can store — holds `name=""` instead,
    /// which the `SET name = ''` fallback of
    /// [`Routine::get_script_for_server`] stores and which behaves the same;
    /// rehashed. `None` when no `SET` clause holds an empty list.
    ///
    /// The comparer compares a FROM routine on such a server with this, not
    /// with the TO routine itself, whose bare `name=` the server can never
    /// store: otherwise the routine would be re-created by every compare.
    pub fn with_empty_lists_as_empty_strings(&self) -> Option<Routine> {
        let mut changed = false;
        let config = self
            .config
            .iter()
            .map(|entry| match entry.split_once('=') {
                Some((name, value))
                    if is_list_quote_guc(name.trim())
                        && split_guc_list(value.trim()).is_some_and(|e| e.is_empty()) =>
                {
                    changed = true;
                    format!("{name}=\"\"")
                }
                _ => entry.clone(),
            })
            .collect();
        if !changed {
            return None;
        }
        let mut portable = self.clone();
        portable.config = config;
        portable.hash();
        Some(portable)
    }

    /// The `GUC_LIST_QUOTE` parameters this routine sets to the empty list
    /// (see [`Routine::with_empty_lists_as_empty_strings`]), in `SET` order.
    pub fn empty_list_settings(&self) -> Vec<&str> {
        self.config
            .iter()
            .filter_map(|entry| entry.split_once('='))
            .filter(|(name, value)| {
                is_list_quote_guc(name.trim())
                    && split_guc_list(value.trim()).is_some_and(|e| e.is_empty())
            })
            .map(|(name, _)| name.trim())
            .collect()
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

    /// Returns a `DROP ROUTINE` script, which drops a function, procedure, window
    /// function or ordinary aggregate alike. Used when the routine is replaced by
    /// one of another kind, so that re-running the migration — after the
    /// replacement already happened — still drops what it finds instead of failing
    /// with "is an aggregate function". It takes the identity arguments, so it is
    /// not for ordered-set aggregates (`ORDER BY` is only valid in DROP AGGREGATE),
    /// which cannot change kind without changing those arguments anyway.
    pub fn get_drop_routine_script(&self) -> String {
        format!(
            "drop routine if exists {}.{} ({}) cascade;",
            self.schema, self.name, self.arguments
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
#[path = "tests/routine.rs"]
mod tests;
