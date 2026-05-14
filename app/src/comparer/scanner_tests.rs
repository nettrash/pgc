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
