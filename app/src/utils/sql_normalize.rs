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

/// Length of the quoted region starting at `chars[start]` (a `'` or `"`), including
/// both delimiters and honoring the doubled-quote escape. `chars[start]` must be the
/// opening quote. Returns the number of chars the region spans.
fn quoted_run_len(chars: &[char], start: usize) -> usize {
    let quote = chars[start];
    let mut i = start + 1;
    while i < chars.len() {
        if chars[i] == quote {
            if i + 1 < chars.len() && chars[i + 1] == quote {
                i += 2; // doubled-quote escape stays inside the run
                continue;
            }
            return i - start + 1; // closing quote
        }
        i += 1;
    }
    chars.len() - start // unterminated: consume the rest
}

/// Index of the `]` matching the `[` at `chars[open]`, respecting nested brackets
/// and skipping quoted regions. Returns `None` when unbalanced.
fn matching_bracket(chars: &[char], open: usize) -> Option<usize> {
    let mut depth = 0i32;
    let mut i = open;
    while i < chars.len() {
        match chars[i] {
            '\'' | '"' => {
                i += quoted_run_len(chars, i);
                continue;
            }
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
        let c = chars[i];
        match c {
            '\'' | '"' => {
                let len = quoted_run_len(chars, i);
                current.extend(&chars[i..i + len]);
                i += len;
                continue;
            }
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
        if chars[i] == '\'' || chars[i] == '"' {
            let len = quoted_run_len(&chars, i);
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
/// identifiers, then collapse the redundant paren-free `IN`-list casts. Quoted
/// regions (both kinds) are copied verbatim so literal values and case-sensitive
/// identifiers survive.
fn lowercase_and_collapse(s: &str) -> String {
    let chars: Vec<char> = s.chars().collect();
    let mut out = String::with_capacity(s.len());
    let mut buf = String::new();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c == '\'' || c == '"' {
            flush_collapsed(&mut buf, &mut out);
            let len = quoted_run_len(&chars, i);
            out.extend(&chars[i..i + len]);
            i += len;
            continue;
        }
        for lc in c.to_lowercase() {
            buf.push(lc);
        }
        i += 1;
    }
    flush_collapsed(&mut buf, &mut out);
    out
}

/// Collapse redundant casts in `buf` (guaranteed to hold no quoted content), append
/// to `out`, and clear `buf`.
fn flush_collapsed(buf: &mut String, out: &mut String) {
    if buf.is_empty() {
        return;
    }
    let collapsed = buf
        .replace("::character varying::text", "::character varying")
        .replace("]::text[]", "]");
    out.push_str(&collapsed);
    buf.clear();
}

#[cfg(test)]
#[path = "sql_normalize_tests.rs"]
mod tests;
