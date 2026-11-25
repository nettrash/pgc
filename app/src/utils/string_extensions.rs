//! Identifier quoting and script-assembly helpers.
//!
//! [`unquote_ident`] reverses PostgreSQL's `quote_ident()` rendering, recovering
//! the raw catalog name. Case-sensitivity is the reason this matters: `"Foo"` and
//! `foo` are different relations, and a dump stores both spellings.
//!
//! The [`StringExt`] trait adds the small append helpers the script builders use.
//!
//! # Values inside SQL comments
//!
//! The generated migration is full of comments that name the objects they are
//! about — `/* Table: s.t*/` headers, `-- use_drop=false: view s.v …` notes and
//! whole statements commented out for review. Those names come from the
//! catalog, and PostgreSQL lets an identifier hold any character: a line break
//! ends a `--` comment, `*/` ends a block comment and `/*` opens a nested one,
//! so a name interpolated as-is turns the rest of itself into executable SQL
//! (a view named `"v⏎drop table t; --"` drops `t` when the migration runs).
//! Every value that reaches a comment therefore goes through one of:
//!
//! - [`line_comment_safe`] — for a value inside a `-- …` line;
//! - [`block_comment_safe`] — for a value inside a `/* … */` comment;
//! - [`comment_out`] / [`comment_out_keep_blank_lines`] — for a statement
//!   emitted commented out, one `-- ` per physical line.
//!
//! The first two return the input borrowed when it needs no change, so the
//! output for ordinary names is byte-for-byte what it was before.

use std::borrow::Cow;

const EMPTY_LINES: &str = "\n\n";

pub trait StringExt {
    fn with_empty_lines(self) -> String;

    fn append_block(&mut self, other: &str);
}

/// Reverses `quote_ident` for one identifier, yielding the raw catalog name:
/// strips the surrounding double quotes and unescapes any doubled quote inside.
/// Input that carries no surrounding quotes is returned unchanged.
///
/// This exists because a dump stores a regular view's schema and name as
/// `quote_ident` renders them (`"MyView"`) but a `table_relation` entry as the raw
/// catalog name (`MyView`), so the two only compare equal once the quoting is undone.
///
/// Scope, so callers do not mistake this for more than it is:
///
/// - It takes exactly **one** identifier, as `quote_ident` would render it — not a
///   qualified `schema.name`, not arbitrary SQL. Unquote each part, then join.
/// - Case is preserved, and so is every character inside the quotes. PostgreSQL
///   identifiers are case-sensitive and may legally contain `"`, `'` or backticks,
///   so `"A"` and `a` are different relations and must not collapse to one key.
/// - The one input it cannot round-trip is a raw, never-quoted name that itself
///   begins and ends with a double quote; such a name is indistinguishable from a
///   quoted rendering of its own interior.
/// # Examples
///
/// ```
/// use pgc::utils::string_extensions::unquote_ident;
///
/// // A quoted identifier gives back its raw catalog name…
/// assert_eq!(unquote_ident("\"MyView\""), "MyView");
/// // …and an unquoted one is already raw.
/// assert_eq!(unquote_ident("myview"), "myview");
///
/// // Doubled quotes inside a quoted identifier collapse to one.
/// assert_eq!(unquote_ident("\"say \"\"hi\"\"\""), "say \"hi\"");
///
/// // Case matters: these are two different relations.
/// assert_ne!(unquote_ident("\"Foo\""), unquote_ident("foo"));
/// ```
pub fn unquote_ident(part: &str) -> String {
    let trimmed = part.trim();
    match trimmed
        .strip_prefix('"')
        .and_then(|inner| inner.strip_suffix('"'))
    {
        Some(inner) => inner.replace("\"\"", "\""),
        None => trimmed.to_string(),
    }
}

/// The keywords PostgreSQL's `quote_ident()` quotes: every keyword that is not
/// unreserved (the reserved, column-name and type/function-name categories of
/// `pg_get_keywords()`), as the union of PostgreSQL 14–19 lists them, sorted.
/// No keyword changed category across those versions; 16 and 17 only added
/// some.
const QUOTED_KEYWORDS: &[&str] = &[
    "all",
    "analyse",
    "analyze",
    "and",
    "any",
    "array",
    "as",
    "asc",
    "asymmetric",
    "authorization",
    "between",
    "bigint",
    "binary",
    "bit",
    "boolean",
    "both",
    "case",
    "cast",
    "char",
    "character",
    "check",
    "coalesce",
    "collate",
    "collation",
    "column",
    "concurrently",
    "constraint",
    "create",
    "cross",
    "current_catalog",
    "current_date",
    "current_role",
    "current_schema",
    "current_time",
    "current_timestamp",
    "current_user",
    "dec",
    "decimal",
    "default",
    "deferrable",
    "desc",
    "distinct",
    "do",
    "else",
    "end",
    "except",
    "exists",
    "extract",
    "false",
    "fetch",
    "float",
    "for",
    "foreign",
    "freeze",
    "from",
    "full",
    "grant",
    "greatest",
    "group",
    "grouping",
    "having",
    "ilike",
    "in",
    "initially",
    "inner",
    "inout",
    "int",
    "integer",
    "intersect",
    "interval",
    "into",
    "is",
    "isnull",
    "join",
    "json",
    "json_array",
    "json_arrayagg",
    "json_exists",
    "json_object",
    "json_objectagg",
    "json_query",
    "json_scalar",
    "json_serialize",
    "json_table",
    "json_value",
    "lateral",
    "leading",
    "least",
    "left",
    "like",
    "limit",
    "localtime",
    "localtimestamp",
    "merge_action",
    "national",
    "natural",
    "nchar",
    "none",
    "normalize",
    "not",
    "notnull",
    "null",
    "nullif",
    "numeric",
    "offset",
    "on",
    "only",
    "or",
    "order",
    "out",
    "outer",
    "overlaps",
    "overlay",
    "placing",
    "position",
    "precision",
    "primary",
    "real",
    "references",
    "returning",
    "right",
    "row",
    "select",
    "session_user",
    "setof",
    "similar",
    "smallint",
    "some",
    "substring",
    "symmetric",
    "system_user",
    "table",
    "tablesample",
    "then",
    "time",
    "timestamp",
    "to",
    "trailing",
    "treat",
    "trim",
    "true",
    "union",
    "unique",
    "user",
    "using",
    "values",
    "varchar",
    "variadic",
    "verbose",
    "when",
    "where",
    "window",
    "with",
    "xmlattributes",
    "xmlconcat",
    "xmlelement",
    "xmlexists",
    "xmlforest",
    "xmlnamespaces",
    "xmlparse",
    "xmlpi",
    "xmlroot",
    "xmlserialize",
    "xmltable",
];

/// Spells a raw catalog name as an SQL identifier, the way PostgreSQL's
/// `quote_ident()` does: bare when it is a lowercase ASCII letter or `_`
/// followed by lowercase letters, digits and `_`, and not a keyword that
/// `quote_ident()` quotes; otherwise in double quotes, with every `"` doubled.
///
/// The input is the raw name, as a `name[]` or `text[]` catalog column holds
/// it (`pg_subscription.subpublications`), not an already-quoted rendering —
/// the reverse of [`unquote_ident`]. Returns `name` borrowed when it stays
/// bare.
///
/// ```
/// use pgc::utils::string_extensions::quote_identifier;
///
/// assert_eq!(quote_identifier("orders_pub"), "orders_pub");
/// assert_eq!(quote_identifier("MyPub"), "\"MyPub\"");
/// assert_eq!(quote_identifier("pub two"), "\"pub two\"");
/// assert_eq!(quote_identifier("select"), "\"select\"");
/// assert_eq!(quote_identifier("say \"hi\""), "\"say \"\"hi\"\"\"");
/// assert!(matches!(quote_identifier("p1"), std::borrow::Cow::Borrowed("p1")));
/// ```
pub fn quote_identifier(name: &str) -> Cow<'_, str> {
    quote_identifier_unless(name, |word| QUOTED_KEYWORDS.binary_search(&word).is_ok())
}

/// The keywords of `QUOTED_KEYWORDS` that `quote_ident()` leaves bare on
/// PostgreSQL 14 and 15, each with the `server_version_num` from which it
/// quotes them: PostgreSQL 16 made the first five column-name keywords, 17
/// the rest. Every other keyword there is quoted by every supported version.
/// Sorted by keyword.
const KEYWORDS_QUOTED_SINCE: &[(&str, i32)] = &[
    ("json", 170000),
    ("json_array", 160000),
    ("json_arrayagg", 160000),
    ("json_exists", 170000),
    ("json_object", 160000),
    ("json_objectagg", 160000),
    ("json_query", 170000),
    ("json_scalar", 170000),
    ("json_serialize", 170000),
    ("json_table", 170000),
    ("json_value", 170000),
    ("merge_action", 170000),
    ("system_user", 160000),
];

/// Spells a raw catalog name the way `quote_ident()` does on the server
/// whose `server_version_num` is given: [`quote_identifier`], except that a
/// keyword a version added (`json_object` from PostgreSQL 16, `json` and
/// `merge_action` from 17) stays bare on an older server. `None` stands for
/// an unknown version and gives the spelling of the oldest supported one,
/// PostgreSQL 14, which quotes only the keywords every supported version
/// quotes.
///
/// This is for matching a name with one the server quoted itself — a dump
/// stores names as `quote_ident()` returned them — not for writing SQL,
/// where the extra quotes [`quote_identifier`] adds are always accepted.
///
/// ```
/// use pgc::utils::string_extensions::quote_identifier_for_server;
///
/// assert_eq!(quote_identifier_for_server("json_object", Some(150000)), "json_object");
/// assert_eq!(quote_identifier_for_server("json_object", Some(160000)), "\"json_object\"");
/// assert_eq!(quote_identifier_for_server("json", Some(160000)), "json");
/// assert_eq!(quote_identifier_for_server("json", Some(170000)), "\"json\"");
/// // Unknown: PostgreSQL 14's spelling.
/// assert_eq!(quote_identifier_for_server("merge_action", None), "merge_action");
/// assert_eq!(quote_identifier_for_server("left", None), "\"left\"");
/// assert_eq!(quote_identifier_for_server("MyMv", None), "\"MyMv\"");
/// ```
pub fn quote_identifier_for_server(name: &str, server_version_num: Option<i32>) -> Cow<'_, str> {
    quote_identifier_unless(name, |word| {
        QUOTED_KEYWORDS.binary_search(&word).is_ok()
            && match KEYWORDS_QUOTED_SINCE.binary_search_by(|(keyword, _)| keyword.cmp(&word)) {
                Ok(i) => server_version_num.is_some_and(|v| v >= KEYWORDS_QUOTED_SINCE[i].1),
                Err(_) => true,
            }
    })
}

/// `name` bare when it is a lowercase ASCII letter or `_` followed by
/// lowercase letters, digits and `_`, and `is_quoted_keyword` says it is not
/// a keyword to quote; otherwise in double quotes, with every `"` doubled.
fn quote_identifier_unless(name: &str, is_quoted_keyword: impl Fn(&str) -> bool) -> Cow<'_, str> {
    let bare = name
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_lowercase() || c == '_')
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
        && !is_quoted_keyword(name);
    if bare {
        Cow::Borrowed(name)
    } else {
        Cow::Owned(format!("\"{}\"", name.replace('"', "\"\"")))
    }
}

/// Normalize CRLF line endings to LF, returning the original string
/// unchanged (no allocation) when it contains no `\r\n`.
pub fn normalize_line_endings(s: String) -> String {
    if s.contains("\r\n") {
        s.replace("\r\n", "\n")
    } else {
        s
    }
}

/// Makes `value` safe to interpolate into a `--` line comment.
///
/// PostgreSQL's lexer ends a line comment at a line feed **or** a bare
/// carriage return (`non_newline = [^\n\r]`), and psql does the same, so
/// each `\r` and `\n` becomes a space — what `pg_dump` does to the names in
/// its own `-- Name: …` headers. Nothing else can end a line comment.
///
/// Returns `value` borrowed when it holds neither character.
///
/// ```
/// use pgc::utils::string_extensions::line_comment_safe;
///
/// assert_eq!(line_comment_safe("s.\"v\ndrop table t; --\""), "s.\"v drop table t; --\"");
/// assert_eq!(line_comment_safe("a\r\nb"), "a  b");
/// // An ordinary name is handed back untouched, without allocating.
/// assert!(matches!(line_comment_safe("s.t"), std::borrow::Cow::Borrowed("s.t")));
/// ```
pub fn line_comment_safe(value: &str) -> Cow<'_, str> {
    if value.contains(['\r', '\n']) {
        Cow::Owned(value.replace(['\r', '\n'], " "))
    } else {
        Cow::Borrowed(value)
    }
}

/// Makes `value` safe to interpolate into a `/* … */` block comment.
///
/// A `*/` inside the value would close the comment early and a `/*` would open
/// a nested one (PostgreSQL block comments nest), which swallows the rest of
/// the migration. Both are split with a space (`* /`, `/ *`). A trailing `/`
/// gets a space after it too, because the headers put the value directly
/// before their closing `*/` (`/* Table: {}*/`), where `x/` would fuse into
/// `x/*/` and open a nested comment. Line breaks become spaces, as in
/// [`line_comment_safe`], so the header stays on one line.
///
/// Returns `value` borrowed when none of that applies.
///
/// ```
/// use pgc::utils::string_extensions::block_comment_safe;
///
/// assert_eq!(block_comment_safe("\"x*/ drop table t; /*\""), "\"x* / drop table t; / *\"");
/// assert_eq!(block_comment_safe("\"a/\""), "\"a/\""); // ends in `"`, not `/`
/// assert_eq!(block_comment_safe("a/"), "a/ ");
/// assert!(matches!(block_comment_safe("s.t"), std::borrow::Cow::Borrowed("s.t")));
/// ```
pub fn block_comment_safe(value: &str) -> Cow<'_, str> {
    if !value.contains(['\r', '\n'])
        && !value.contains("*/")
        && !value.contains("/*")
        && !value.ends_with('/')
    {
        return Cow::Borrowed(value);
    }
    let mut out = String::with_capacity(value.len() + 4);
    let mut prev: Option<char> = None;
    for c in value.chars() {
        let c = if c == '\r' || c == '\n' { ' ' } else { c };
        if matches!((prev, c), (Some('/'), '*') | (Some('*'), '/')) {
            out.push(' ');
        }
        out.push(c);
        prev = Some(c);
    }
    if out.ends_with('/') {
        out.push(' ');
    }
    Cow::Owned(out)
}

/// Splits `text` into physical lines the way PostgreSQL's lexer sees them:
/// a line ends at `\r\n`, `\n` or a bare `\r`. Each item is the line's
/// content and whether a terminator followed it. Unlike [`str::lines`], a bare
/// `\r` is a terminator here — psql ends a `--` comment at one.
fn physical_lines(text: &str) -> impl Iterator<Item = (&str, bool)> {
    let mut rest = Some(text);
    std::iter::from_fn(move || {
        let current = rest?;
        match current.find(['\r', '\n']) {
            Some(at) => {
                let skip = if current[at..].starts_with("\r\n") {
                    2
                } else {
                    1
                };
                rest = Some(&current[at + skip..]);
                Some((&current[..at], true))
            }
            None => {
                rest = None;
                Some((current, false))
            }
        }
    })
}

/// Comments out `sql` for review: every physical line becomes `-- <line>\n`.
///
/// Byte-for-byte the `sql.lines().map(|l| format!("-- {l}\n"))` idiom it
/// replaces, except that a bare `\r` also ends a line. `str::lines` does not
/// split on one, but psql ends a `--` comment there, so the text after it —
/// part of a quoted name, say — would run as SQL.
///
/// ```
/// use pgc::utils::string_extensions::comment_out;
///
/// assert_eq!(comment_out("drop view s.v;\n\n"), "-- drop view s.v;\n-- \n");
/// assert_eq!(comment_out("drop view s.\"v\rdrop table t; --\";"),
///            "-- drop view s.\"v\n-- drop table t; --\";\n");
/// assert_eq!(comment_out(""), "");
/// ```
pub fn comment_out(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len() + 8);
    for (line, terminated) in physical_lines(sql) {
        // `str::lines` yields no empty final line after a trailing terminator.
        if line.is_empty() && !terminated {
            break;
        }
        out.push_str("-- ");
        out.push_str(line);
        out.push('\n');
    }
    out
}

/// Comments out one statement block the way `format!("-- {sql}")` does for a
/// single-line statement — `-- ` before the first line, the trailing blank
/// lines of a [`StringExt::with_empty_lines`] block kept as they are — but also
/// before every later line that has content, so a statement whose quoted name
/// holds a line break stays commented out to its end. Line breaks (`\r\n`,
/// `\n`, bare `\r`) are written as `\n`.
///
/// ```
/// use pgc::utils::string_extensions::comment_out_keep_blank_lines;
///
/// assert_eq!(comment_out_keep_blank_lines("drop index s.i;\n\n"), "-- drop index s.i;\n\n");
/// assert_eq!(comment_out_keep_blank_lines("drop index s.\"i\ndrop table t; --\";\n\n"),
///            "-- drop index s.\"i\n-- drop table t; --\";\n\n");
/// ```
pub fn comment_out_keep_blank_lines(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len() + 8);
    for (index, (line, terminated)) in physical_lines(sql).enumerate() {
        if index == 0 || !line.is_empty() {
            out.push_str("-- ");
        }
        out.push_str(line);
        if terminated {
            out.push('\n');
        }
    }
    out
}

impl StringExt for String {
    fn with_empty_lines(mut self) -> String {
        self.push_str(EMPTY_LINES);
        self
    }

    fn append_block(&mut self, other: &str) {
        self.push_str(other);
        self.push_str(EMPTY_LINES);
    }
}

#[cfg(test)]
#[path = "tests/string_extensions.rs"]
mod tests;
