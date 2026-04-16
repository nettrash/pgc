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
fn copy_quoted_literal(
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
fn dollar_tag_at(src: &[u8], pos: usize) -> Option<usize> {
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
mod tests {
    use super::*;

    #[test]
    fn collapses_triple_newlines_outside_dollar_quotes() {
        let out = strip_comments_and_collapse("SELECT 1;\n\n\n\nSELECT 2;\n");
        assert_eq!(out, "SELECT 1;\n\nSELECT 2;\n");
    }

    #[test]
    fn preserves_triple_newlines_inside_dollar_quotes() {
        let input = concat!(
            "CREATE OR REPLACE PROCEDURE public.test_proc() LANGUAGE plpgsql AS $$\n",
            "BEGIN\n",
            "  RAISE NOTICE 'block 1';\n",
            "\n\n\n",
            "  RAISE NOTICE 'block 2';\n",
            "END;\n",
            "$$;\n",
        );
        let out = strip_comments_and_collapse(input);
        assert!(
            out.contains("'block 1';\n\n\n\n  RAISE NOTICE 'block 2'"),
            "blank lines inside $$ body must be preserved, got:\n{}",
            out
        );
    }

    #[test]
    fn preserves_newlines_inside_tagged_dollar_quotes() {
        let input = concat!(
            "CREATE FUNCTION f() RETURNS void LANGUAGE plpgsql AS $body$\n",
            "BEGIN\n",
            "\n\n\n",
            "  NULL;\n",
            "END;\n",
            "$body$;\n",
        );
        let out = strip_comments_and_collapse(input);
        assert!(
            out.contains("BEGIN\n\n\n\n  NULL;"),
            "blank lines inside $body$ must be preserved, got:\n{}",
            out
        );
    }

    #[test]
    fn collapses_newlines_between_dollar_quoted_blocks() {
        let out = strip_comments_and_collapse("$$body1$$;\n\n\n\n$$body2$$;\n");
        assert_eq!(out, "$$body1$$;\n\n$$body2$$;\n");
    }

    #[test]
    fn mixed_dollar_quote_and_outside_newlines() {
        let input = concat!(
            "SELECT 1;\n\n\n\n",
            "CREATE FUNCTION f() RETURNS void AS $$\n",
            "BEGIN\n",
            "\n\n\n",
            "  NULL;\n",
            "END;\n",
            "$$;\n",
            "\n\n\n\n",
            "SELECT 2;\n",
        );
        let out = strip_comments_and_collapse(input);
        assert!(
            !out.contains("SELECT 1;\n\n\n"),
            "newlines before $$ block should be collapsed"
        );
        assert!(
            !out.contains("$$;\n\n\n"),
            "newlines after $$ block should be collapsed"
        );
        assert!(
            out.contains("BEGIN\n\n\n\n  NULL;"),
            "blank lines inside $$ must be preserved, got:\n{}",
            out
        );
    }

    #[test]
    fn strips_line_comment() {
        let out = strip_comments_and_collapse("-- comment\nSELECT 1;\n");
        assert!(!out.contains("-- comment"));
        assert!(out.contains("SELECT 1;"));
    }

    #[test]
    fn strips_block_comment() {
        let out = strip_comments_and_collapse("/* block */SELECT 1;\n");
        assert!(!out.contains("block"));
        assert!(out.contains("SELECT 1;"));
    }

    #[test]
    fn strips_nested_block_comment() {
        let out = strip_comments_and_collapse("/* outer /* inner */ still */SELECT 1;\n");
        assert!(!out.contains("outer"));
        assert!(out.contains("SELECT 1;"));
    }

    #[test]
    fn preserves_comment_inside_dollar_quote() {
        let input = "$$-- not a comment\n/* also not */$$;\n";
        let out = strip_comments_and_collapse(input);
        assert!(out.contains("-- not a comment"));
        assert!(out.contains("/* also not */"));
    }

    #[test]
    fn preserves_comment_inside_single_quoted_string() {
        let input = "SELECT '-- not a comment';\n";
        let out = strip_comments_and_collapse(input);
        assert!(out.contains("'-- not a comment'"));
    }

    #[test]
    fn preserves_comment_inside_e_string() {
        let input = "SELECT E'/* not block */';\n";
        let out = strip_comments_and_collapse(input);
        assert!(out.contains("E'/* not block */'"));
    }

    #[test]
    fn preserves_comment_inside_double_quoted_identifier() {
        let input = "SELECT \"-- col\";\n";
        let out = strip_comments_and_collapse(input);
        assert!(out.contains("\"-- col\""));
    }

    #[test]
    fn preserves_newlines_inside_single_quoted_string() {
        let out = strip_comments_and_collapse("COMMENT ON TABLE t IS 'line1\n\n\n\nline5';\n");
        assert!(
            out.contains("'line1\n\n\n\nline5'"),
            "newlines inside single-quoted string must be preserved, got:\n{}",
            out
        );
    }

    #[test]
    fn preserves_newlines_inside_e_string() {
        let out = strip_comments_and_collapse("SELECT E'first\n\n\n\nlast';\n");
        assert!(
            out.contains("E'first\n\n\n\nlast'"),
            "newlines inside E-string must be preserved, got:\n{}",
            out
        );
    }

    #[test]
    fn preserves_newlines_inside_double_quoted_identifier() {
        let out = strip_comments_and_collapse("SELECT \"col\n\n\n\nname\";\n");
        assert!(
            out.contains("\"col\n\n\n\nname\""),
            "newlines inside double-quoted identifier must be preserved, got:\n{}",
            out
        );
    }

    #[test]
    fn collapses_newlines_between_single_quoted_strings() {
        let out = strip_comments_and_collapse("SELECT 'a';\n\n\n\nSELECT 'b';\n");
        assert_eq!(out, "SELECT 'a';\n\nSELECT 'b';\n");
    }

    #[test]
    fn e_string_with_escaped_quote_and_newlines() {
        let out = strip_comments_and_collapse("SELECT E'it\\'s\n\n\n\nfine';\n");
        assert!(
            out.contains("E'it\\'s\n\n\n\nfine'"),
            "E-string with escaped quote and newlines must be preserved, got:\n{}",
            out
        );
    }

    #[test]
    fn single_quoted_doubled_quote_and_newlines() {
        let out = strip_comments_and_collapse("SELECT 'it''s\n\n\n\nfine';\n");
        assert!(out.contains("'it''s\n\n\n\nfine'"), "got:\n{}", out);
    }

    #[test]
    fn lowercase_e_string_preserves_newlines() {
        let out = strip_comments_and_collapse("SELECT e'first\n\n\n\nlast';\n");
        assert!(out.contains("e'first\n\n\n\nlast'"), "got:\n{}", out);
    }

    #[test]
    fn unterminated_single_quoted_string_copies_to_end() {
        let out = strip_comments_and_collapse("SELECT 'unterminated\n\n\n\nstring\n");
        assert!(out.contains("\n\n\n\n"));
    }

    #[test]
    fn unterminated_e_string_copies_to_end() {
        let out = strip_comments_and_collapse("SELECT E'unterminated\n\n\n\nstring\n");
        assert!(out.contains("\n\n\n\n"));
    }

    #[test]
    fn unterminated_double_quoted_copies_to_end() {
        let out = strip_comments_and_collapse("SELECT \"unterminated\n\n\n\nident\n");
        assert!(out.contains("\n\n\n\n"));
    }

    #[test]
    fn empty_single_quoted_string_no_corruption() {
        let out = strip_comments_and_collapse("SELECT '';\n\n\n\nSELECT 1;\n");
        assert_eq!(out, "SELECT '';\n\nSELECT 1;\n");
    }

    #[test]
    fn empty_dollar_body_not_corrupted() {
        let out = strip_comments_and_collapse("CREATE FUNCTION f() AS $$$$;\n");
        assert!(out.contains("$$$$"));
    }

    #[test]
    fn unterminated_dollar_quote_copies_to_end() {
        let input = "CREATE FUNCTION f() AS $$\nBEGIN\n\n\n\n  NULL;\n";
        let out = strip_comments_and_collapse(input);
        assert!(out.contains("\n\n\n\n"));
    }

    #[test]
    fn newline_count_resets_after_single_quoted_literal() {
        let out = strip_comments_and_collapse("A;\n\n'inside\n\n\n\ntext';\n\nB;\n");
        assert!(out.contains("'inside\n\n\n\ntext'"));
        assert!(out.contains("A;\n\n'inside"));
        assert!(out.contains("';\n\nB;"));
    }

    #[test]
    fn collapses_after_quoted_literal_with_excess_newlines() {
        let out = strip_comments_and_collapse("SELECT 'hello';\n\n\n\nSELECT 'world';\n");
        assert_eq!(out, "SELECT 'hello';\n\nSELECT 'world';\n");
    }

    #[test]
    fn e_string_escaped_backslash_then_newlines() {
        let out = strip_comments_and_collapse("SELECT E'foo\\\\';\n\n\n\nSELECT 1;\n");
        assert_eq!(out, "SELECT E'foo\\\\';\n\nSELECT 1;\n");
    }

    #[test]
    fn double_quoted_doubled_escape_and_newlines() {
        let out = strip_comments_and_collapse("SELECT \"col\"\"\n\n\n\nname\";\n");
        assert!(out.contains("\"col\"\"\n\n\n\nname\""));
    }

    #[test]
    fn comment_stripped_but_single_quoted_newlines_preserved() {
        let input = "-- strip this\nCOMMENT ON TABLE t IS 'multi\n\n\n\nline';\n";
        let out = strip_comments_and_collapse(input);
        assert!(!out.contains("-- strip this"));
        assert!(out.contains("'multi\n\n\n\nline'"));
    }

    #[test]
    fn block_comment_stripped_but_e_string_newlines_preserved() {
        let input = "/* block comment */\nSELECT E'keep\n\n\n\nme';\n";
        let out = strip_comments_and_collapse(input);
        assert!(!out.contains("block comment"));
        assert!(out.contains("E'keep\n\n\n\nme'"));
    }

    #[test]
    fn adjacent_single_quoted_strings_both_preserved() {
        let out = strip_comments_and_collapse("SELECT 'a\n\n\n\nb' || 'c\n\n\n\nd';\n");
        assert!(out.contains("'a\n\n\n\nb'"));
        assert!(out.contains("'c\n\n\n\nd'"));
    }

    #[test]
    fn dollar_body_containing_single_quoted_newlines() {
        let input = concat!(
            "CREATE FUNCTION f() AS $$\n",
            "BEGIN\n",
            "  RAISE NOTICE 'msg\n\n\n\nend';\n",
            "END;\n",
            "$$;\n",
        );
        let out = strip_comments_and_collapse(input);
        assert!(out.contains("'msg\n\n\n\nend'"));
    }

    #[test]
    fn single_quoted_string_without_excess_newlines_unchanged() {
        let out = strip_comments_and_collapse("SELECT 'a\n\nb';\n");
        assert_eq!(out, "SELECT 'a\n\nb';\n");
    }

    #[test]
    fn e_identifier_not_confused_with_e_string() {
        let out = strip_comments_and_collapse("SELECT E = 1;\n\n\n\nSELECT 2;\n");
        assert_eq!(out, "SELECT E = 1;\n\nSELECT 2;\n");
    }

    #[test]
    fn multiple_e_strings_on_same_line() {
        let out = strip_comments_and_collapse("SELECT E'x\n\n\n\ny', E'a\n\n\n\nb';\n");
        assert!(out.contains("E'x\n\n\n\ny'"));
        assert!(out.contains("E'a\n\n\n\nb'"));
    }

    #[test]
    fn mixed_literal_types_with_newlines() {
        let input = concat!(
            "COMMENT ON TABLE t IS 'line1\n\n\n\nline5';\n",
            "\n\n\n\n",
            "SELECT E'a\n\n\n\nb';\n",
            "\n\n\n\n",
            "SELECT \"id\n\n\n\ncol\";\n",
            "\n\n\n\n",
            "CREATE FUNCTION f() AS $$\nBEGIN\n\n\n\n  NULL;\nEND;\n$$;\n",
        );
        let out = strip_comments_and_collapse(input);
        assert!(out.contains("'line1\n\n\n\nline5'"));
        assert!(out.contains("E'a\n\n\n\nb'"));
        assert!(out.contains("\"id\n\n\n\ncol\""));
        assert!(out.contains("BEGIN\n\n\n\n  NULL;"));
    }
}
