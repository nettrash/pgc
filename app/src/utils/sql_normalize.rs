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
/// Every rendering PostgreSQL uses for a varchar `IN`-list array — parenthesized or
/// paren-free, array-level or element-level cast — is rewritten to the one
/// element-level fixed point `array[('v'::character varying)::text, …]`, and
/// everything outside string literals and quoted identifiers is lowercased. Both
/// rewrites preserve semantics exactly (cast distribution is what PostgreSQL performs
/// on re-parse; the rest is parenthesization), so no type information is ever dropped:
/// a real array cast (`::integer[]`, `::bigint[]`, an array subscript's `::text[]`)
/// and a plain `character varying[]` literal keep their own distinct canonical keys.
/// Content inside `'...'` literals and `"..."` identifiers is preserved verbatim,
/// including case.
///
/// Leading and trailing whitespace is stripped, so a definition is canonicalized to
/// the same string whether or not a catalog rendering left surrounding whitespace —
/// every caller (view hash and alter comparison, index, constraint, generated column)
/// therefore agrees regardless of trimming at the call site.
pub fn canonicalize_definition(s: &str) -> String {
    lowercase_and_collapse(&distribute_array_casts(s.trim()))
}

fn is_ident_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

/// Whether `chars[at..]` starts with the ASCII `prefix`, compared
/// case-insensitively, without allocating. The array-literal matchers run this at
/// every scan position, so it must stay allocation-free.
fn starts_with_ascii_ci(chars: &[char], at: usize, prefix: &str) -> bool {
    at + prefix.len() <= chars.len()
        && chars[at..at + prefix.len()]
            .iter()
            .zip(prefix.chars())
            .all(|(c, p)| c.eq_ignore_ascii_case(&p))
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
/// PostgreSQL's expression deparser emits only the standard single-quoted form,
/// under **both** settings of `standard_conforming_strings`: quotes are always
/// escaped by doubling (never `\'`), and backslashes are emitted bare when the
/// setting is `on` or doubled to `\\` when it is `off` (ruleutils'
/// `simple_quote_literal`, verified live on PostgreSQL 16 in both modes). E-strings
/// and dollar-quotes therefore never appear in catalog deparse output; handling them
/// keeps the canonicalizer from mis-scanning literal content if hand-written SQL
/// ever reaches it.
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
/// opening delimiter), including both delimiters. The only escape is the doubled
/// delimiter (`''` / `""`); a backslash is an ordinary character, so `\'` ends the
/// literal at the quote.
///
/// That is exactly the contract of PostgreSQL's deparser output: it never renders a
/// quote as `\'`, and under `standard_conforming_strings = on` a string ending in a
/// backslash legitimately renders as `'x\'` — the quote after the backslash IS the
/// terminator (under `= off` the deparser doubles the backslash instead). Treating
/// `\'` as an escape here would mis-scan that trailing-backslash rendering and
/// swallow everything after it into the literal, so a backslash escape must never
/// be added to this function. Backslash escapes exist only in the E-string scanner
/// ([`escaped_literal_body_len`]), whose `E'...'` form the deparser never emits.
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

/// Rewrite `(ARRAY[e1, …, en])::T[]` (array-level cast) into
/// `ARRAY[(e1)::baseT, …, (en)::baseT]` (element-level cast), where `baseT` is `T`
/// with the trailing `[]` removed. This is PostgreSQL's re-parsed fixed point, so a
/// definition already in that form is left unchanged and the two renderings collapse
/// to the same text. Quoted literals and identifiers are never inspected or altered.
///
/// The matcher is deliberately exact, mirroring what the deparser emits for this
/// pattern: `)` immediately followed by `::` (the deparser never puts whitespace
/// around a cast), and `T` an unqualified, unquoted, typmod-free type name
/// (ASCII alphanumerics, `_` and spaces — e.g. `text`, `character varying`)
/// immediately followed by `[]`. In the `IN`-list deparse this exists for, `T` is
/// always `text`. Wider spellings — schema-qualified `::s.dom[]`, quoted `::"T"[]`,
/// typmod `::character varying(10)[]` — do not match and the expression passes
/// through untouched, which fails safe: an unmatched rendering can at worst cause
/// churn for a form that has never been observed to flip, while an unsound rewrite
/// could fuse genuinely different expressions.
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
/// See [`distribute_array_casts`] for the exact matcher contract this implements.
fn try_rewrite_array_cast(chars: &[char], at: usize) -> Option<(String, usize)> {
    // Case-insensitive match of the literal prefix `(array[`.
    const PREFIX: &str = "(array[";
    if !starts_with_ascii_ci(chars, at, PREFIX) {
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
/// identifiers, then rewrite the paren-free `IN`-list array renderings to the
/// element-level fixed point. Quoted regions (both kinds) are copied verbatim so
/// literal values and case-sensitive identifiers survive.
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

/// Rewrite the paren-free `IN`-list renderings of a `text[]` array literal into the
/// element-level fixed point, so they compare equal to each other and to the
/// parenthesized family handled by [`distribute_array_casts`]. Everything that is not
/// exactly one of those two renderings — a plain varchar array, any other element
/// type, any other trailing cast, an array subscript's `::text[]`, a standalone
/// `x::character varying::text` double cast — is left byte-for-byte intact. Input
/// must already be lowercased outside quotes.
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

/// If `chars[at..]` starts an `array[...]` literal in one of the two paren-free
/// renderings PostgreSQL uses for a varchar `IN`-list, return the element-level
/// fixed-point rendering and the index just past the consumed region.
///
/// The two redundant renderings of the same `text[]` array are
///
/// * array-level cast:   `array['v'::character varying, …]::text[]`
/// * element-level cast: `array['v'::character varying::text, …]`
///
/// Both become `array[('v'::character varying)::text, …]` — the same fixed point the
/// parenthesized `(array[…])::text[]` family reaches via [`distribute_array_casts`] —
/// so every rendering of one `IN`-list compares equal, even across a dump taken on a
/// server that renders one family and a dump that renders the other. Each rewrite
/// preserves semantics exactly: distributing an array-level cast over the elements is
/// what PostgreSQL itself does on re-parse, and turning `X::character varying::text`
/// into `(X::character varying)::text` only parenthesizes a left-associative cast
/// chain. Because the `::text` marker is kept rather than stripped, a plain
/// `character varying[]` literal canonicalizes to a *different* key than any of the
/// `text[]` renderings — the two types can never be conflated. Any literal that is
/// not exactly one of the two renderings (mixed elements, other element types, any
/// other trailing cast) is returned as `None` and left untouched.
fn try_collapse_array_literal(chars: &[char], at: usize) -> Option<(String, usize)> {
    // Input is already lowercased outside quotes, so the case-insensitive compare is
    // equivalent to an exact match here — and allocation-free.
    const PREFIX: &str = "array[";
    if !starts_with_ascii_ci(chars, at, PREFIX) {
        return None;
    }
    // `array` must stand alone, not be the tail of another identifier (e.g. `x_array[`).
    if at > 0 && (chars[at - 1].is_ascii_alphanumeric() || chars[at - 1] == '_') {
        return None;
    }
    let bracket_open = at + PREFIX.len() - 1; // index of '['
    let bracket_close = matching_bracket(chars, bracket_open)?;
    let content = &chars[bracket_open + 1..bracket_close];
    let elements = split_top_level_commas(content);
    if elements.is_empty() {
        return None;
    }

    // Array-level rendering: a trailing `::text[]` over all-varchar elements.
    if let Some((base_type, after)) = trailing_array_cast(chars, bracket_close + 1) {
        if base_type == "text" && elements.iter().all(|e| ends_with_varchar_cast(e)) {
            return Some((element_level_fixed_point(&elements), after));
        }
        return None; // real array cast — leave the literal untouched
    }

    // Element-level rendering: every element carries the paren-free chained cast.
    if elements
        .iter()
        .all(|e| e.strip_suffix("::text").is_some_and(ends_with_varchar_cast))
    {
        return Some((element_level_fixed_point(&elements), bracket_close + 1));
    }
    None
}

/// Whether an array element ends with a varchar cast — `::character varying`,
/// optionally carrying a typmod, e.g. `::character varying(10)`. The typmod spelling
/// survives PostgreSQL's re-parse (an explicit `'A'::varchar(10)` inside an `IN` list
/// deparses as `'A'::character varying(10)` in both the array-level and element-level
/// renderings), so it participates in the same #226 flip as the bare form and must be
/// gated in for the renderings to converge. Input is already lowercased outside quotes.
fn ends_with_varchar_cast(element: &str) -> bool {
    strip_trailing_typmod(element).ends_with("::character varying")
}

/// Strip a trailing `(digits[, digits…])` typmod from a cast spelling, returning the
/// prefix; input without one is returned unchanged.
fn strip_trailing_typmod(e: &str) -> &str {
    if !e.ends_with(')') {
        return e;
    }
    let Some(open) = e.rfind('(') else {
        return e;
    };
    let inner = &e[open + 1..e.len() - 1];
    if !inner.is_empty()
        && inner
            .chars()
            .all(|c| c.is_ascii_digit() || c == ',' || c == ' ')
    {
        &e[..open]
    } else {
        e
    }
}

/// Rebuild an array literal at the element-level fixed point: each element becomes
/// `(<element>)::text`, with the element-level rendering's own trailing `::text`
/// removed first so both paren-free renderings produce byte-identical output. Nested
/// array literals inside an element are canonicalized recursively.
fn element_level_fixed_point(elements: &[String]) -> String {
    let rebuilt: Vec<String> = elements
        .iter()
        .map(|e| {
            let base = e.strip_suffix("::text").unwrap_or(e);
            format!("({})::text", collapse_array_literal_casts(base))
        })
        .collect();
    format!("array[{}]", rebuilt.join(", "))
}

/// A trailing array-level cast `::TYPE[]` starting at `chars[pos]`: the base type name
/// (`TYPE`) and the index just past the closing `[]`. `None` when there is no such cast.
fn trailing_array_cast(chars: &[char], pos: usize) -> Option<(String, usize)> {
    if chars.get(pos) != Some(&':') || chars.get(pos + 1) != Some(&':') {
        return None;
    }
    let type_start = pos + 2;
    let mut j = type_start;
    while j < chars.len()
        && (chars[j].is_ascii_alphanumeric() || chars[j] == '_' || chars[j] == ' ')
    {
        j += 1;
    }
    if j == type_start || chars.get(j) != Some(&'[') || chars.get(j + 1) != Some(&']') {
        return None;
    }
    let base_type: String = chars[type_start..j].iter().collect();
    Some((base_type.trim().to_string(), j + 2))
}

#[cfg(test)]
#[path = "sql_normalize_tests.rs"]
mod tests;
