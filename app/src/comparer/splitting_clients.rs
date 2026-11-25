//! Output for SQL clients that split a script at every `;`
//! (`--output-for-splitting-clients`).
//!
//! psql sends a script to the server one statement at a time, and it knows
//! where each statement ends: not at a `;` inside parentheses, such as the
//! ones between the actions of a rule (`DO ALSO (INSERT …; INSERT …)`), and
//! not at a `;` inside the `BEGIN ATOMIC … END` body of a SQL-standard
//! routine. Many other clients, the VS Code PostgreSQL extension among them,
//! split at every `;` outside quotes and comments instead. They send such a
//! statement in pieces, and the server rejects the first piece with `syntax
//! error at end of input`. The same clients take a positional parameter
//! (`$1`) outside quotes for a bind parameter and stop to ask for its value,
//! and PostgreSQL deparses a SQL-standard body over unnamed arguments with
//! exactly those.
//!
//! [`wrap_for_splitting_clients`] runs every statement that has either
//! through a `DO` block, which such a client sends whole:
//!
//! ```sql
//! DO $pgc_client$
//! BEGIN
//!     EXECUTE $pgc_sql$
//! create or replace function s.f() returns integer language sql BEGIN ATOMIC
//!  SELECT 1;
//! END
//! $pgc_sql$;
//! END
//! $pgc_client$;
//! ```
//!
//! The statement is copied byte for byte, and a dollar-quoted string is taken
//! literally, so `EXECUTE` hands the server exactly the text psql would have
//! sent, in the same session and transaction. A tag that occurs anywhere in
//! the statement gets a number (`$pgc_sql1$`), so it cannot end the string
//! early.
//!
//! A carriage return is the one byte that is not copied. Such clients run the
//! script from an editor, and an editor that gives a file one kind of line
//! ending, as VS Code does when it opens one, turns a CR inside a literal or
//! a routine body (the `E'a\r\nb'` of a function, the CR LF line ends of a
//! body written on Windows) into a LF, so the migration would create the
//! object a little different from the one it was compared with. A statement
//! with a CR is therefore wrapped too, cut at each CR, and `EXECUTE` joins the
//! pieces with `chr(13)` again:
//!
//! ```sql
//! DO $pgc_client$
//! BEGIN
//!     EXECUTE pg_catalog.array_to_string(ARRAY[$pgc_sql$
//! create or replace function s.f() returns text language sql BEGIN ATOMIC
//!  SELECT 'line1$pgc_sql$, $pgc_sql$
//! line2'::text;
//! END
//! $pgc_sql$], pg_catalog.chr(13));
//! END
//! $pgc_client$;
//! ```
//!
//! Every other statement is left as it is.
//!
//! Statements are delimited the way psql delimits them, so a statement is
//! wrapped whole: a `;` ends one unless it is inside parentheses, or, in a
//! `CREATE [OR REPLACE] FUNCTION | PROCEDURE` statement, inside a block that
//! psql counts with `BEGIN`, `CASE` and `END`. String literals (`'…'`, `E'…'`,
//! `U&'…'`), quoted identifiers, dollar-quoted strings and comments are
//! skipped. The scanner is its own, rather than the shared `scanner` module's,
//! because it has to follow PostgreSQL's lexer where that one need not: a
//! dollar-quote tag never starts with a digit (`$1` is a parameter), and a `$`
//! inside an identifier (`a$1`) is part of it.

use crate::comparer::scanner::is_line_end;

/// The tag of the `DO` block's body, numbered when the statement contains it.
const OUTER_TAG: &str = "pgc_client";
/// The tag of the string `EXECUTE` runs, numbered when the statement contains it.
const INNER_TAG: &str = "pgc_sql";

/// Wrap every statement of `script` that a client splitting at each `;` would
/// cut, would ask a bind parameter for, or whose carriage returns an editor
/// would turn into line feeds, in a `DO` block that `EXECUTE`s it unchanged.
/// Everything else, comments and blank lines included, is copied as it is, so
/// a script without such a statement comes back unchanged.
///
/// # Examples
///
/// ```
/// use pgc::comparer::splitting_clients::wrap_for_splitting_clients;
///
/// let script = "create table t (id integer);\n\n\
///               create function f() returns integer language sql BEGIN ATOMIC\n SELECT 1;\nEND;\n";
/// assert_eq!(
///     wrap_for_splitting_clients(script),
///     "create table t (id integer);\n\n\
///      DO $pgc_client$\nBEGIN\n    EXECUTE $pgc_sql$\n\
///      create function f() returns integer language sql BEGIN ATOMIC\n SELECT 1;\nEND\n\
///      $pgc_sql$;\nEND\n$pgc_client$;\n"
/// );
/// ```
pub fn wrap_for_splitting_clients(script: &str) -> String {
    let src = script.as_bytes();
    let len = src.len();
    let mut out = String::with_capacity(len);
    // `script[..copied]` is in `out` already.
    let mut copied = 0;
    let mut statement = Statement::default();
    let mut i = 0;
    while i < len {
        let byte = src[i];
        if byte.is_ascii_whitespace() {
            i += 1;
            continue;
        }
        if src[i..].starts_with(b"--") {
            while i < len && !is_line_end(src[i]) {
                i += 1;
            }
            continue;
        }
        if src[i..].starts_with(b"/*") {
            i = block_comment_end(src, i);
            continue;
        }
        // Leading blank lines and comments stay outside the wrapper.
        let start = *statement.start.get_or_insert(i);
        match byte {
            b'\'' | b'"' => i = quoted_end(src, i + 1, byte, false),
            b'$' => {
                if let Some(tag_len) = dollar_quote_at(src, i) {
                    i = dollar_quoted_end(src, i, tag_len);
                } else if src.get(i + 1).is_some_and(u8::is_ascii_digit) {
                    statement.needs_wrap = true;
                    i += 1;
                    while i < len && src[i].is_ascii_digit() {
                        i += 1;
                    }
                } else {
                    i += 1;
                }
            }
            b'(' => {
                statement.paren_depth += 1;
                i += 1;
            }
            b')' => {
                statement.paren_depth = statement.paren_depth.saturating_sub(1);
                i += 1;
            }
            b';' if statement.paren_depth == 0 && statement.begin_depth == 0 => {
                if statement.needs_wrap || script[start..i].contains('\r') {
                    out.push_str(&script[copied..start]);
                    push_wrapped(&mut out, &script[start..i]);
                    // The statement's own `;` now ends the `DO`.
                    copied = i;
                }
                statement = Statement::default();
                i += 1;
            }
            b';' => {
                statement.needs_wrap = true;
                i += 1;
            }
            _ if is_identifier_start(byte) => {
                let end = identifier_end(src, i);
                let word = &src[i..end];
                let next = src.get(end).copied();
                // `U&'…'` needs nothing of its own: past the `U&` it is a
                // standard literal, where `\` escapes no quote.
                if word.eq_ignore_ascii_case(b"e") && next == Some(b'\'') {
                    i = quoted_end(src, end + 1, b'\'', true);
                } else {
                    statement.identifier(word);
                    i = end;
                }
            }
            _ => i += 1,
        }
    }
    out.push_str(&script[copied..]);
    out
}

/// What the scan knows about the statement it is in.
#[derive(Default)]
struct Statement {
    /// Byte offset of its first token; `None` until one is seen.
    start: Option<usize>,
    paren_depth: usize,
    /// psql's `begin_depth`: open `BEGIN` / `CASE` blocks of a routine body.
    begin_depth: usize,
    /// psql's `identifiers`: the lowercased first letter of each of the first
    /// four identifiers that is `create`, `function`, `procedure`, `or` or
    /// `replace`, and 0 for any other.
    head: [u8; 4],
    /// Identifiers seen so far.
    identifiers: usize,
    /// It holds a `;` that does not end it, or a positional parameter.
    needs_wrap: bool,
}

impl Statement {
    /// Account for an unquoted identifier (keywords included), as psql does
    /// to tell where a `CREATE FUNCTION` or `CREATE PROCEDURE` with a
    /// `BEGIN ATOMIC` body ends.
    fn identifier(&mut self, word: &[u8]) {
        const HEAD_WORDS: [&[u8]; 5] = [b"create", b"function", b"procedure", b"or", b"replace"];
        if self.identifiers < self.head.len()
            && HEAD_WORDS.iter().any(|w| word.eq_ignore_ascii_case(w))
        {
            self.head[self.identifiers] = word[0].to_ascii_lowercase();
        }
        self.identifiers += 1;
        if self.paren_depth > 0 || !self.creates_routine() {
            return;
        }
        if word.eq_ignore_ascii_case(b"begin") {
            self.begin_depth += 1;
        } else if word.eq_ignore_ascii_case(b"case") {
            // CASE ends with END too, which only matters inside a BEGIN.
            if self.begin_depth > 0 {
                self.begin_depth += 1;
            }
        } else if word.eq_ignore_ascii_case(b"end") {
            self.begin_depth = self.begin_depth.saturating_sub(1);
        }
    }

    /// `CREATE FUNCTION`, `CREATE PROCEDURE`, or either with `OR REPLACE`.
    fn creates_routine(&self) -> bool {
        matches!(
            self.head,
            [b'c', b'f' | b'p', _, _] | [b'c', b'o', b'r', b'f' | b'p']
        )
    }
}

/// Append `statement` (without its terminating `;`) wrapped in a `DO` block,
/// up to the closing tag of the block; the caller adds the `;`. A statement
/// with a carriage return is written without it: as the pieces between them,
/// which `EXECUTE` joins with `chr(13)`. An array rather than a chain of `||`,
/// so a body with many CR LF line ends does not nest an expression as deep.
fn push_wrapped(out: &mut String, statement: &str) {
    let outer = unused_tag(statement, OUTER_TAG);
    let inner = unused_tag(statement, INNER_TAG);
    let text = if statement.contains('\r') {
        let pieces = statement.replace('\r', &format!("{inner}, {inner}"));
        format!("pg_catalog.array_to_string(ARRAY[{inner}\n{pieces}\n{inner}], pg_catalog.chr(13))")
    } else {
        format!("{inner}\n{statement}\n{inner}")
    };
    out.push_str(&format!(
        "DO {outer}\nBEGIN\n    EXECUTE {text};\nEND\n{outer}"
    ));
}

/// `$name$`, or `$name1$`, `$name2$`, … for the first one `statement` does not
/// contain anywhere.
fn unused_tag(statement: &str, name: &str) -> String {
    let mut tag = format!("${name}$");
    let mut n = 0u32;
    while statement.contains(&tag) {
        n += 1;
        tag = format!("${name}{n}$");
    }
    tag
}

/// PostgreSQL's `ident_start`: a letter, `_` or any byte of a multi-byte
/// character.
fn is_identifier_start(byte: u8) -> bool {
    byte.is_ascii_alphabetic() || byte == b'_' || byte >= 0x80
}

/// The end of the identifier starting at `i` (`ident_cont` adds digits and `$`).
fn identifier_end(src: &[u8], mut i: usize) -> usize {
    while i < src.len()
        && (is_identifier_start(src[i]) || src[i].is_ascii_digit() || src[i] == b'$')
    {
        i += 1;
    }
    i
}

/// The length of the dollar-quote tag at `i` (`$$` or `$tag$`, the tag not
/// starting with a digit), or `None` when `i` does not open one.
fn dollar_quote_at(src: &[u8], i: usize) -> Option<usize> {
    let mut j = i + 1;
    if j < src.len() && is_identifier_start(src[j]) {
        j += 1;
        while j < src.len() && (is_identifier_start(src[j]) || src[j].is_ascii_digit()) {
            j += 1;
        }
    }
    (j < src.len() && src[j] == b'$').then_some(j - i + 1)
}

/// Just past the tag that closes the dollar-quoted string opened at `i`, or
/// the end of `src` when nothing closes it.
fn dollar_quoted_end(src: &[u8], i: usize, tag_len: usize) -> usize {
    let tag = &src[i..i + tag_len];
    let body = i + tag_len;
    src[body..]
        .windows(tag_len)
        .position(|w| w == tag)
        .map_or(src.len(), |at| body + at + tag_len)
}

/// Just past the `delimiter` that closes the literal or quoted identifier
/// whose body starts at `i`. A doubled delimiter is part of the body, and
/// with `backslash_escapes` (an `E'…'` string) so is the byte after a `\`.
fn quoted_end(src: &[u8], mut i: usize, delimiter: u8, backslash_escapes: bool) -> usize {
    while i < src.len() {
        if backslash_escapes && src[i] == b'\\' {
            i += 2;
        } else if src[i] == delimiter {
            if src.get(i + 1) == Some(&delimiter) {
                i += 2;
            } else {
                return i + 1;
            }
        } else {
            i += 1;
        }
    }
    src.len()
}

/// Just past the end of the block comment opened at `i`, which may nest.
fn block_comment_end(src: &[u8], mut i: usize) -> usize {
    let mut depth = 0usize;
    while i < src.len() {
        if src[i..].starts_with(b"/*") {
            depth += 1;
            i += 2;
        } else if src[i..].starts_with(b"*/") {
            depth -= 1;
            i += 2;
            if depth == 0 {
                return i;
            }
        } else {
            i += 1;
        }
    }
    src.len()
}

#[cfg(test)]
#[path = "tests/splitting_clients.rs"]
mod tests;
