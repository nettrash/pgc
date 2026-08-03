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

/// Normalize CRLF line endings to LF, returning the original string
/// unchanged (no allocation) when it contains no `\r\n`.
pub fn normalize_line_endings(s: String) -> String {
    if s.contains("\r\n") {
        s.replace("\r\n", "\n")
    } else {
        s
    }
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
