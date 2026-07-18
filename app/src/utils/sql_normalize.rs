//! Canonicalization of catalog-stored SQL expressions so semantically identical
//! forms compare equal.
//!
//! PostgreSQL's expression deparsing (`pg_get_viewdef`, `pg_get_indexdef`,
//! `pg_get_constraintdef`, generated-column expressions, …) is not idempotent for
//! `IN (literal_list)`: the first deparse of `col IN ('FOO','BAR')` yields an
//! array-level cast
//!
//! ```text
//! (col)::text = ANY ((ARRAY['FOO'::character varying, 'BAR'::character varying])::text[])
//! ```
//!
//! and re-parsing that form deparses to an element-level cast
//!
//! ```text
//! (col)::text = ANY (ARRAY[('FOO'::character varying)::text, ('BAR'::character varying)::text])
//! ```
//!
//! The two are equivalent but textually different, so pgc — which hashes/compares
//! the deparsed text — would emit `DROP`+`CREATE` on every run (issue #226). The
//! element-level form is PostgreSQL's fixed point (re-parsing it is stable), so
//! [`canonicalize_definition`] rewrites the array-level cast into it and both forms
//! collapse to the same string.

/// Canonicalize a catalog expression/definition for comparison and hashing.
///
/// Distributes the array-level `IN`-list cast into the element-level fixed-point
/// form, then lowercases everything outside string literals and quoted identifiers
/// and collapses the redundant `::character varying::text` / `]::text[]` casts that
/// the paren-free (CHECK-constraint) rendering leaves behind. Content inside
/// `'...'` literals and `"..."` identifiers is preserved verbatim, including case.
pub fn canonicalize_definition(s: &str) -> String {
    lowercase_and_collapse(&distribute_array_casts(s))
}

fn is_ident_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

/// If a quoted region begins at `chars[at]`, return its length (both delimiters
/// included); otherwise `None`. Recognizes every form PostgreSQL can render a string
/// literal or identifier as, so their content is always skipped verbatim:
///
/// * standard `'...'` literals and `"..."` identifiers, with the doubled-delimiter
///   (`''` / `""`) escape,
/// * E-strings `E'...'` / `e'...'`, with backslash escapes (`\'` does not close),
/// * dollar-quoted strings `$tag$...$tag$` (and `$$...$$`).
///
/// PostgreSQL's expression deparser emits only the standard single-quoted form, but
/// handling the others keeps the canonicalizer correct if one ever reaches it (e.g.
/// under `standard_conforming_strings = off`) rather than mis-scanning literal content.
fn quoted_prefix_len(chars: &[char], at: usize) -> Option<usize> {
    let c = chars[at];
    // E-string: E'…' / e'…', only when the E stands alone (not the tail of an
    // identifier like `some_value'…'`, which is an identifier followed by a literal).
    if (c == 'E' || c == 'e')
        && chars.get(at + 1) == Some(&'\'')
        && (at == 0 || !is_ident_char(chars[at - 1]))
    {
        return Some(2 + escaped_literal_body_len(chars, at + 2));
    }
    if c == '\'' || c == '"' {
        return Some(standard_quoted_len(chars, at));
    }
    if c == '$' {
        return dollar_quoted_len(chars, at);
    }
    None
}

/// Length of a standard `'...'` / `"..."` region starting at `chars[start]` (the
/// opening delimiter), including both delimiters and honoring the doubled-delimiter
/// escape.
fn standard_quoted_len(chars: &[char], start: usize) -> usize {
    let quote = chars[start];
    let mut i = start + 1;
    while i < chars.len() {
        if chars[i] == quote {
            if chars.get(i + 1) == Some(&quote) {
                i += 2; // doubled-delimiter escape stays inside the run
                continue;
            }
            return i - start + 1; // closing delimiter
        }
        i += 1;
    }
    chars.len() - start // unterminated: consume the rest
}

/// Length of an E-string body starting at `body` (just past the opening `'`),
/// including the closing `'`. A backslash escapes the next char, and `''` is a
/// doubled-quote escape.
fn escaped_literal_body_len(chars: &[char], body: usize) -> usize {
    let mut i = body;
    while i < chars.len() {
        match chars[i] {
            '\\' => i += 2, // escaped char — never terminates the literal
            '\'' => {
                if chars.get(i + 1) == Some(&'\'') {
                    i += 2; // doubled-quote escape
                    continue;
                }
                return i - body + 1; // closing quote
            }
            _ => i += 1,
        }
    }
    chars.len() - body
}

/// If a dollar-quote `$tag$...$tag$` begins at `chars[at]` (a `$`), return its total
/// length. `None` when `chars[at]` does not open a dollar-quote or the closing tag is
/// absent (so a stray `$` — e.g. in `col$1` — is treated as an ordinary character).
fn dollar_quoted_len(chars: &[char], at: usize) -> Option<usize> {
    let tag_len = dollar_tag_len(chars, at)?;
    let tag = &chars[at..at + tag_len];
    let mut i = at + tag_len;
    while i + tag_len <= chars.len() {
        if chars[i] == '$'
            && dollar_tag_len(chars, i) == Some(tag_len)
            && &chars[i..i + tag_len] == tag
        {
            return Some(i + tag_len - at);
        }
        i += 1;
    }
    None
}

/// Length of a dollar-quote tag `$[alnum_|_]*$` starting at `chars[pos]`, or `None`.
fn dollar_tag_len(chars: &[char], pos: usize) -> Option<usize> {
    if chars.get(pos) != Some(&'$') {
        return None;
    }
    let mut j = pos + 1;
    while j < chars.len() && is_ident_char(chars[j]) {
        j += 1;
    }
    if chars.get(j) == Some(&'$') {
        Some(j - pos + 1)
    } else {
        None
    }
}

/// Index of the `]` matching the `[` at `chars[open]`, respecting nested brackets
/// and skipping quoted regions. Returns `None` when unbalanced.
fn matching_bracket(chars: &[char], open: usize) -> Option<usize> {
    let mut depth = 0i32;
    let mut i = open;
    while i < chars.len() {
        if let Some(len) = quoted_prefix_len(chars, i) {
            i += len;
            continue;
        }
        match chars[i] {
            '[' => depth += 1,
            ']' => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

/// Split `chars` at top-level commas, respecting nested `()`/`[]` and quoted regions.
/// Each returned element is trimmed.
fn split_top_level_commas(chars: &[char]) -> Vec<String> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut depth = 0i32;
    let mut i = 0;
    while i < chars.len() {
        if let Some(len) = quoted_prefix_len(chars, i) {
            current.extend(&chars[i..i + len]);
            i += len;
            continue;
        }
        let c = chars[i];
        match c {
            '(' | '[' => {
                depth += 1;
                current.push(c);
            }
            ')' | ']' => {
                depth -= 1;
                current.push(c);
            }
            ',' if depth == 0 => {
                parts.push(current.trim().to_string());
                current.clear();
            }
            _ => current.push(c),
        }
        i += 1;
    }
    if !current.trim().is_empty() {
        parts.push(current.trim().to_string());
    }
    parts
}

/// Rewrite every `(ARRAY[e1, …, en])::T[]` (array-level cast) into
/// `ARRAY[(e1)::baseT, …, (en)::baseT]` (element-level cast), where `baseT` is `T`
/// with the trailing `[]` removed. This is PostgreSQL's re-parsed fixed point, so a
/// definition already in that form is left unchanged and the two renderings collapse
/// to the same text. Quoted literals and identifiers are never inspected or altered.
fn distribute_array_casts(input: &str) -> String {
    let chars: Vec<char> = input.chars().collect();
    let mut out = String::with_capacity(input.len());
    let mut i = 0;
    while i < chars.len() {
        if let Some(len) = quoted_prefix_len(&chars, i) {
            out.extend(&chars[i..i + len]);
            i += len;
            continue;
        }
        if let Some(consumed) = try_rewrite_array_cast(&chars, i) {
            out.push_str(&consumed.0);
            i = consumed.1;
            continue;
        }
        out.push(chars[i]);
        i += 1;
    }
    out
}

/// If `chars[at..]` begins with `(ARRAY[ ... ])::TYPE[]`, return the distributed
/// replacement and the index just past the consumed region. `None` otherwise.
fn try_rewrite_array_cast(chars: &[char], at: usize) -> Option<(String, usize)> {
    // Case-insensitive match of the literal prefix `(array[`.
    const PREFIX: &str = "(array[";
    if at + PREFIX.len() > chars.len() {
        return None;
    }
    let head: String = chars[at..at + PREFIX.len()].iter().collect();
    if !head.eq_ignore_ascii_case(PREFIX) {
        return None;
    }
    let bracket_open = at + PREFIX.len() - 1; // index of '['
    let bracket_close = matching_bracket(chars, bracket_open)?;

    // Expect `)::TYPE[]` immediately after the closing bracket.
    let mut j = bracket_close + 1;
    if j >= chars.len() || chars[j] != ')' {
        return None;
    }
    j += 1;
    if j + 1 >= chars.len() || chars[j] != ':' || chars[j + 1] != ':' {
        return None;
    }
    j += 2;
    let type_start = j;
    // A type name: identifier chars plus spaces (e.g. `character varying`).
    while j < chars.len()
        && (chars[j].is_ascii_alphanumeric() || chars[j] == '_' || chars[j] == ' ')
    {
        j += 1;
    }
    let base_type: String = chars[type_start..j].iter().collect();
    let base_type = base_type.trim();
    // Must be followed by the array marker `[]`.
    if base_type.is_empty() || j + 1 >= chars.len() || chars[j] != '[' || chars[j + 1] != ']' {
        return None;
    }
    let end = j + 2;

    let content: Vec<char> = chars[bracket_open + 1..bracket_close].to_vec();
    let elements = split_top_level_commas(&content);
    if elements.is_empty() {
        return None;
    }
    let distributed: Vec<String> = elements
        .iter()
        .map(|e| format!("({})::{}", distribute_array_casts(e), base_type))
        .collect();
    Some((format!("array[{}]", distributed.join(", ")), end))
}

/// Lowercase everything outside single-quoted literals and double-quoted
/// identifiers, then collapse the redundant paren-free `IN`-list casts that only ever
/// occur inside an `array[...]` literal. Quoted regions (both kinds) are copied
/// verbatim so literal values and case-sensitive identifiers survive.
fn lowercase_and_collapse(s: &str) -> String {
    collapse_array_literal_casts(&lowercase_outside_quotes(s))
}

/// Lowercase every character outside single-quoted literals and double-quoted
/// identifiers, copying quoted regions (both kinds) verbatim.
fn lowercase_outside_quotes(s: &str) -> String {
    let chars: Vec<char> = s.chars().collect();
    let mut out = String::with_capacity(s.len());
    let mut i = 0;
    while i < chars.len() {
        if let Some(len) = quoted_prefix_len(&chars, i) {
            out.extend(&chars[i..i + len]);
            i += len;
            continue;
        }
        for lc in chars[i].to_lowercase() {
            out.push(lc);
        }
        i += 1;
    }
    out
}

/// Collapse the redundant casts PostgreSQL leaves on the paren-free `IN`-list
/// rendering — but only where they occur, inside an `array[...]` literal. For a match
/// `array[<content>]::T[]`, the array-level cast `::T[]` is dropped and each element's
/// `::character varying::text` collapses to `::character varying`, so the array-level
/// and element-level renderings converge. A `::text[]` cast on anything else (an array
/// subscript or slice such as `col[1:2]::text[]`) and a standalone `x::character
/// varying::text` double cast are outside any `array[...]` literal and are left intact
/// — collapsing them would drop a real cast and hide a diff. Input must already be
/// lowercased outside quotes.
fn collapse_array_literal_casts(s: &str) -> String {
    let chars: Vec<char> = s.chars().collect();
    let mut out = String::with_capacity(s.len());
    let mut i = 0;
    while i < chars.len() {
        if let Some(len) = quoted_prefix_len(&chars, i) {
            out.extend(&chars[i..i + len]);
            i += len;
            continue;
        }
        if let Some((replacement, end)) = try_collapse_array_literal(&chars, i) {
            out.push_str(&replacement);
            i = end;
            continue;
        }
        out.push(chars[i]);
        i += 1;
    }
    out
}

/// If `chars[at..]` starts an `array[...]` literal (optionally cast with `::T[]`),
/// return its collapsed rendering and the index just past the consumed region.
fn try_collapse_array_literal(chars: &[char], at: usize) -> Option<(String, usize)> {
    const PREFIX: &str = "array[";
    if at + PREFIX.len() > chars.len() {
        return None;
    }
    if chars[at..at + PREFIX.len()].iter().collect::<String>() != PREFIX {
        return None;
    }
    // `array` must stand alone, not be the tail of another identifier (e.g. `x_array[`).
    if at > 0 && (chars[at - 1].is_ascii_alphanumeric() || chars[at - 1] == '_') {
        return None;
    }
    let bracket_open = at + PREFIX.len() - 1; // index of '['
    let bracket_close = matching_bracket(chars, bracket_open)?;
    let content = &chars[bracket_open + 1..bracket_close];

    // Drop a trailing array-level cast `::TYPE[]` if present.
    let mut end = bracket_close + 1;
    if end + 1 < chars.len() && chars[end] == ':' && chars[end + 1] == ':' {
        let mut j = end + 2;
        let type_start = j;
        while j < chars.len()
            && (chars[j].is_ascii_alphanumeric() || chars[j] == '_' || chars[j] == ' ')
        {
            j += 1;
        }
        if j > type_start && j + 1 < chars.len() && chars[j] == '[' && chars[j + 1] == ']' {
            end = j + 2;
        }
    }

    Some((format!("array[{}]", collapse_element_casts(content)), end))
}

/// Collapse `::character varying::text` to `::character varying` in array-literal
/// content, outside quoted regions so literal values are never rewritten.
fn collapse_element_casts(content: &[char]) -> String {
    let mut out = String::with_capacity(content.len());
    let mut buf = String::new();
    let mut i = 0;
    while i < content.len() {
        if let Some(len) = quoted_prefix_len(content, i) {
            out.push_str(&buf.replace("::character varying::text", "::character varying"));
            buf.clear();
            out.extend(&content[i..i + len]);
            i += len;
            continue;
        }
        buf.push(content[i]);
        i += 1;
    }
    out.push_str(&buf.replace("::character varying::text", "::character varying"));
    out
}

#[cfg(test)]
#[path = "sql_normalize_tests.rs"]
mod tests;
