//! SQL-aware scanner that strips comments and collapses excess blank lines
//! while preserving the contents of all quoted literals verbatim.
//!
//! Recognised token types (copied byte-for-byte without interpretation):
//!   - dollar-quoted strings:      `$$` … `$$` or `$tag$` … `$tag$`
//!   - single-quoted literals:     `'` … `'`   (`''` is the escape for a literal quote)
//!   - E-string literals:          `E'` … `'`  (backslash sequences and `''` are handled)
//!   - double-quoted identifiers:  `"` … `"`   (`""` is the escape for a literal quote)

/// Single-pass scanner: strip SQL comments (`--` line and `/* */` block),
/// collapse runs of 3+ newlines into 2, and pass all quoted literals
/// through verbatim so that their content is never altered.
///
/// Operates on bytes to avoid corrupting multi-byte UTF-8 sequences.
pub(crate) fn strip_comments_and_collapse(script: &str) -> String {
    let src = script.as_bytes();
    let len = src.len();
    let mut result: Vec<u8> = Vec::with_capacity(len);
    let mut newline_count = 0u32;
    let mut i = 0;

    while i < len {
        // Dollar-quoting: $$ or $tag$ — pass through verbatim until matching closer
        if src[i] == b'$'
            && let Some(tag_len) = dollar_tag_at(src, i)
        {
            let tag = &src[i..i + tag_len];
            result.extend_from_slice(tag);
            i += tag_len;
            newline_count = 0;
            loop {
                if i >= len {
                    break;
                }
                if src[i] == b'$'
                    && let Some(close_len) = dollar_tag_at(src, i)
                    && close_len == tag_len
                    && &src[i..i + close_len] == tag
                {
                    result.extend_from_slice(&src[i..i + close_len]);
                    i += close_len;
                    break;
                }
                result.push(src[i]);
                i += 1;
            }
            continue;
        }
        // E-string literal E'...' or e'...' — pass through verbatim.
        if (src[i] == b'E' || src[i] == b'e') && i + 1 < len && src[i + 1] == b'\'' {
            result.push(src[i]);
            result.push(b'\'');
            i += 2;
            copy_quoted_literal(src, &mut result, &mut i, b'\'', true);
            newline_count = 0;
            continue;
        }
        // Single-quoted string — pass through verbatim (handle '' escapes)
        if src[i] == b'\'' {
            result.push(b'\'');
            i += 1;
            copy_quoted_literal(src, &mut result, &mut i, b'\'', false);
            newline_count = 0;
            continue;
        }
        // Double-quoted identifier — pass through verbatim (handle "" escapes)
        if src[i] == b'"' {
            result.push(b'"');
            i += 1;
            copy_quoted_literal(src, &mut result, &mut i, b'"', false);
            newline_count = 0;
            continue;
        }
        // Block comment /* ... */ — strip.
        // PostgreSQL allows arbitrarily nested block comments; track depth.
        if i + 1 < len && src[i] == b'/' && src[i + 1] == b'*' {
            i += 2;
            let mut depth: usize = 1;
            while i + 1 < len && depth > 0 {
                if src[i] == b'/' && src[i + 1] == b'*' {
                    depth += 1;
                    i += 2;
                } else if src[i] == b'*' && src[i + 1] == b'/' {
                    depth -= 1;
                    i += 2;
                } else {
                    i += 1;
                }
            }
            if depth > 0 {
                i = len;
            }
            continue;
        }
        // Line comment -- ... — strip
        if i + 1 < len && src[i] == b'-' && src[i + 1] == b'-' {
            i += 2;
            while i < len && src[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        // Regular byte — apply newline collapsing
        if src[i] == b'\n' {
            newline_count += 1;
            if newline_count <= 2 {
                result.push(b'\n');
            }
        } else {
            newline_count = 0;
            result.push(src[i]);
        }
        i += 1;
    }

    // Safety: result is built entirely from slices of `script` (valid UTF-8),
    // so it is guaranteed to be valid UTF-8.
    String::from_utf8(result).expect("output must be valid UTF-8")
}

/// Copies a quoted literal body (everything after the opening delimiter has
/// already been pushed) into `result`, advancing `i` past the closing
/// delimiter.  Two quoting conventions are supported:
///
/// * **Standard** (`backslash_escapes = false`): a doubled delimiter (`''`
///   or `""`) is an escape sequence; any other occurrence of the delimiter
///   ends the literal.
/// * **E-string** (`backslash_escapes = true`): additionally, a backslash
///   followed by any byte is copied as a unit without re-inspecting the
///   second byte, so `\'` never terminates the literal.
///
/// In both modes every byte is copied verbatim — the scanner does not
/// interpret the content, only tracks enough structure to know when the
/// literal ends.
pub(crate) fn copy_quoted_literal(
    src: &[u8],
    result: &mut Vec<u8>,
    i: &mut usize,
    delimiter: u8,
    backslash_escapes: bool,
) {
    let len = src.len();
    while *i < len {
        if backslash_escapes && src[*i] == b'\\' {
            // Copy the backslash and the following byte as a unit.
            result.push(b'\\');
            *i += 1;
            if *i < len {
                result.push(src[*i]);
                *i += 1;
            }
        } else if src[*i] == delimiter {
            result.push(delimiter);
            *i += 1;
            if *i < len && src[*i] == delimiter {
                // Doubled-delimiter escape — copy the second one and keep scanning.
                result.push(delimiter);
                *i += 1;
            } else {
                break; // closing delimiter
            }
        } else {
            result.push(src[*i]);
            *i += 1;
        }
    }
}

/// Checks if a dollar-quote tag starts at position `pos` in `src`.
/// A tag is `$` followed by zero or more alphanumeric/underscore chars, followed by `$`.
/// Returns the total length of the tag (including both `$` signs) or None.
pub(crate) fn dollar_tag_at(src: &[u8], pos: usize) -> Option<usize> {
    if pos >= src.len() || src[pos] != b'$' {
        return None;
    }
    let mut j = pos + 1;
    while j < src.len() && (src[j].is_ascii_alphanumeric() || src[j] == b'_') {
        j += 1;
    }
    if j < src.len() && src[j] == b'$' {
        Some(j - pos + 1) // include closing $
    } else {
        None
    }
}

#[cfg(test)]
#[path = "scanner_tests.rs"]
mod tests;
