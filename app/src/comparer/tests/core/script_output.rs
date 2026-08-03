//! Tests for the final script rendering performed by `get_script`:
//! `use_single_transaction` framing, comment stripping under
//! `use_comments = false`, and newline collapsing — all of which must leave
//! string literals, E-strings, quoted identifiers and dollar-quoted bodies
//! byte-for-byte intact.

use crate::comparer::core::*;
use super::helpers::*;
use crate::config::dump_config::DumpConfig;
use crate::config::grants_mode::GrantsMode;
use crate::dump::table::Table;

#[tokio::test]
async fn use_single_transaction_should_add_begin_commit() {
    let from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut new_table = Table::new(
        "public".to_string(),
        "\"my-table\"".to_string(),
        "public".to_string(),
        "my-table".to_string(),
        "postgres".to_string(),
        None,
        vec![int_column("public", "\"my-table\"", "id", 1)],
        vec![],
        vec![],
        vec![],
        None,
    );

    new_table.hash();

    to_dump.tables.push(new_table);

    let mut comparer = Comparer::new(from_dump, to_dump, false, true, true, GrantsMode::Ignore);

    comparer.compare().await.unwrap();

    let script = comparer.get_script();

    const SCRIPT_BODY_START_PATTERN: &str = "*/\n\n";

    let script_body_start_index = script
        .find(SCRIPT_BODY_START_PATTERN)
        .map(|index| index + SCRIPT_BODY_START_PATTERN.len())
        .expect("Script header was not found");

    let script_body = &script[script_body_start_index..];

    assert!(script_body.starts_with("begin;\n\n"));
    assert!(script_body.ends_with("\ncommit;"));
}

#[tokio::test]
async fn use_comments_false_strips_block_and_line_comments() {
    let from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());
    let mut comparer = Comparer::new(from_dump, to_dump, false, false, false, GrantsMode::Ignore);
    comparer.script =
        "/* header comment */\nCREATE TABLE t1 (id int); -- inline comment\n/* trailing */\n"
            .to_string();
    let result = comparer.get_script();
    assert_eq!(result, "CREATE TABLE t1 (id int);\n");
}

#[tokio::test]
async fn use_comments_false_strips_singly_nested_block_comment() {
    let from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());
    let mut comparer = Comparer::new(from_dump, to_dump, false, false, false, GrantsMode::Ignore);
    // /* outer /* inner */ still outer */ must all be stripped.
    comparer.script = "SELECT /* outer /* inner */ still outer */ 1;\n".to_string();
    let result = comparer.get_script();
    assert_eq!(result, "SELECT  1;\n");
}

#[tokio::test]
async fn use_comments_false_strips_deeply_nested_block_comment() {
    let from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());
    let mut comparer = Comparer::new(from_dump, to_dump, false, false, false, GrantsMode::Ignore);
    // Three levels of nesting.
    comparer.script = "SELECT /* a /* b /* c */ b */ a */ 1;\n".to_string();
    let result = comparer.get_script();
    assert_eq!(result, "SELECT  1;\n");
}

#[tokio::test]
async fn use_comments_false_strips_adjacent_nested_block_comments() {
    let from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());
    let mut comparer = Comparer::new(from_dump, to_dump, false, false, false, GrantsMode::Ignore);
    // Two independent outer comments each with their own inner comment.
    // Any space left before SELECT after stripping the first comment is removed by get_script()'s trim().
    comparer.script = "/* a /* b */ a */ SELECT /* c /* d */ c */ 1;\n".to_string();
    let result = comparer.get_script();
    assert_eq!(result, "SELECT  1;\n");
}

#[tokio::test]
async fn use_comments_false_nested_block_comment_before_statement() {
    let from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());
    let mut comparer = Comparer::new(from_dump, to_dump, false, false, false, GrantsMode::Ignore);
    // Nested comment as a header; SQL that follows must be preserved intact.
    comparer.script = "/* header /* nested */ end */\nCREATE TABLE t (id int);\n".to_string();
    let result = comparer.get_script();
    assert_eq!(result, "CREATE TABLE t (id int);\n");
}

#[tokio::test]
async fn use_comments_false_nested_comment_only_script_returns_empty() {
    let from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());
    let mut comparer = Comparer::new(from_dump, to_dump, false, false, false, GrantsMode::Ignore);
    // A script consisting only of a nested block comment produces no output.
    comparer.script = "/* outer /* inner */ outer */\n".to_string();
    let result = comparer.get_script();
    assert_eq!(result, "");
}

#[tokio::test]
async fn use_comments_false_nested_comment_sql_between_levels() {
    let from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());
    let mut comparer = Comparer::new(from_dump, to_dump, false, false, false, GrantsMode::Ignore);
    // Text that sits between the outer /* and its matching */ must be stripped
    // even when inner comment pairs appear in the middle.
    comparer.script = "SELECT /* before /* mid */ after */ 42;\n".to_string();
    let result = comparer.get_script();
    assert_eq!(result, "SELECT  42;\n");
}

#[tokio::test]
async fn use_comments_false_nested_comment_immediately_after_keyword() {
    let from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());
    let mut comparer = Comparer::new(from_dump, to_dump, false, false, false, GrantsMode::Ignore);
    // No space between the keyword and the nested comment; scanner must not
    // be confused by the /* that immediately follows non-comment text.
    comparer.script = "SELECT/* /* nested */ */1;\n".to_string();
    let result = comparer.get_script();
    assert_eq!(result, "SELECT1;\n");
}

#[tokio::test]
async fn use_comments_false_nested_comment_followed_by_line_comment() {
    let from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());
    let mut comparer = Comparer::new(from_dump, to_dump, false, false, false, GrantsMode::Ignore);
    // After the nested block comment closes, a line comment on the same line
    // must also be stripped.
    comparer.script = "SELECT /* a /* b */ a */ 1; -- strip me\n".to_string();
    let result = comparer.get_script();
    assert_eq!(result, "SELECT  1;\n");
}

#[tokio::test]
async fn use_comments_false_nested_comment_inside_single_quoted_string_not_stripped() {
    let from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());
    let mut comparer = Comparer::new(from_dump, to_dump, false, false, false, GrantsMode::Ignore);
    // Nested-comment-like sequences inside a string literal must be preserved.
    comparer.script = "SELECT '/* outer /* inner */ outer */' AS val;\n".to_string();
    let result = comparer.get_script();
    assert_eq!(result, "SELECT '/* outer /* inner */ outer */' AS val;\n");
}

#[tokio::test]
async fn use_comments_false_nested_comment_inside_e_string_not_stripped() {
    let from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());
    let mut comparer = Comparer::new(from_dump, to_dump, false, false, false, GrantsMode::Ignore);
    // Nested-comment-like sequences inside an E-string must also be preserved.
    comparer.script = "SELECT E'/* outer /* inner */ outer */' AS val;\n".to_string();
    let result = comparer.get_script();
    assert_eq!(result, "SELECT E'/* outer /* inner */ outer */' AS val;\n");
}

#[tokio::test]
async fn use_comments_false_nested_comment_inside_double_quoted_identifier_not_stripped() {
    let from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());
    let mut comparer = Comparer::new(from_dump, to_dump, false, false, false, GrantsMode::Ignore);
    // Nested-comment-like sequences inside a double-quoted identifier must be preserved.
    comparer.script = "SELECT 1 AS \"/* outer /* inner */ outer */\";\n".to_string();
    let result = comparer.get_script();
    assert_eq!(result, "SELECT 1 AS \"/* outer /* inner */ outer */\";\n");
}

#[tokio::test]
async fn use_comments_false_preserves_dollar_quoted_comments() {
    let from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());
    let mut comparer = Comparer::new(from_dump, to_dump, false, false, false, GrantsMode::Ignore);
    comparer.script = "CREATE FUNCTION f() RETURNS void AS $$\n-- inside body\n/* also inside */\n$$ LANGUAGE plpgsql;\n".to_string();
    let result = comparer.get_script();
    assert_eq!(
        result,
        "CREATE FUNCTION f() RETURNS void AS $$\n-- inside body\n/* also inside */\n$$ LANGUAGE plpgsql;\n"
    );
}

#[tokio::test]
async fn use_comments_false_preserves_named_dollar_tag_comments() {
    let from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());
    let mut comparer = Comparer::new(from_dump, to_dump, false, false, false, GrantsMode::Ignore);
    comparer.script =
        "CREATE FUNCTION g() RETURNS void AS $body$\n-- comment inside\n$body$ LANGUAGE plpgsql;\n"
            .to_string();
    let result = comparer.get_script();
    assert_eq!(
        result,
        "CREATE FUNCTION g() RETURNS void AS $body$\n-- comment inside\n$body$ LANGUAGE plpgsql;\n"
    );
}

#[tokio::test]
async fn use_comments_false_preserves_single_quoted_comments() {
    let from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());
    let mut comparer = Comparer::new(from_dump, to_dump, false, false, false, GrantsMode::Ignore);
    comparer.script = "SELECT '-- not a comment' AS val, '/* also not */' AS val2;\n".to_string();
    let result = comparer.get_script();
    assert_eq!(
        result,
        "SELECT '-- not a comment' AS val, '/* also not */' AS val2;\n"
    );
}

#[tokio::test]
async fn use_comments_false_preserves_e_string_backslash_escaped_quote() {
    let from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());
    let mut comparer = Comparer::new(from_dump, to_dump, false, false, false, GrantsMode::Ignore);
    // \' inside E'...' is an escaped quote and must NOT terminate the string.
    // The comment-like content after it must be preserved, not stripped.
    comparer.script = "SELECT E'it\\'s fine -- not a comment' AS val; -- strip\n".to_string();
    let result = comparer.get_script();
    assert_eq!(result, "SELECT E'it\\'s fine -- not a comment' AS val;\n");
}

#[tokio::test]
async fn use_comments_false_preserves_e_string_block_comment_lookalike() {
    let from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());
    let mut comparer = Comparer::new(from_dump, to_dump, false, false, false, GrantsMode::Ignore);
    // /* ... */ inside an E-string must not be treated as a block comment.
    comparer.script = "SELECT E'/* not a comment */' AS val; /* strip */\n".to_string();
    let result = comparer.get_script();
    assert_eq!(result, "SELECT E'/* not a comment */' AS val;\n");
}

#[tokio::test]
async fn use_comments_false_preserves_e_string_backslash_backslash_then_quote() {
    let from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());
    let mut comparer = Comparer::new(from_dump, to_dump, false, false, false, GrantsMode::Ignore);
    // \\\' is an escaped backslash (\\) followed by an escaped quote (\').
    // The string should continue after that sequence.
    comparer.script =
        "SELECT E'backslash\\\\\\'quote -- still inside' AS val; -- strip\n".to_string();
    let result = comparer.get_script();
    assert_eq!(
        result,
        "SELECT E'backslash\\\\\\'quote -- still inside' AS val;\n"
    );
}

#[tokio::test]
async fn use_comments_false_preserves_e_string_doubled_quote_escape() {
    let from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());
    let mut comparer = Comparer::new(from_dump, to_dump, false, false, false, GrantsMode::Ignore);
    // '' inside E'...' is also a valid quote escape; must not terminate string early.
    comparer.script = "SELECT E'it''s fine -- not a comment' AS val; -- strip\n".to_string();
    let result = comparer.get_script();
    assert_eq!(result, "SELECT E'it''s fine -- not a comment' AS val;\n");
}

#[tokio::test]
async fn use_comments_false_preserves_lowercase_e_string() {
    let from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());
    let mut comparer = Comparer::new(from_dump, to_dump, false, false, false, GrantsMode::Ignore);
    // Lowercase e'...' prefix must be handled identically to E'...'.
    comparer.script = "SELECT e'it\\'s fine -- not a comment' AS val; -- strip\n".to_string();
    let result = comparer.get_script();
    assert_eq!(result, "SELECT e'it\\'s fine -- not a comment' AS val;\n");
}

#[tokio::test]
async fn use_comments_false_does_not_treat_standalone_e_as_e_string() {
    let from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());
    let mut comparer = Comparer::new(from_dump, to_dump, false, false, false, GrantsMode::Ignore);
    // A bare column/alias named "e" followed immediately by a plain string
    // literal must not be misidentified as an E-string prefix.
    // Here "e" is a table alias and 'text' is a separate literal.
    comparer.script = "SELECT e, 'text -- not a comment' FROM t; -- strip\n".to_string();
    let result = comparer.get_script();
    assert_eq!(result, "SELECT e, 'text -- not a comment' FROM t;\n");
}

#[tokio::test]
async fn use_comments_false_does_not_treat_uppercase_e_identifier_as_e_string() {
    let from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());
    let mut comparer = Comparer::new(from_dump, to_dump, false, false, false, GrantsMode::Ignore);
    // Uppercase E as a standalone identifier (column name), not followed by
    // a quote, must not be confused with an E-string prefix.
    comparer.script = "SELECT E FROM t; -- strip\n".to_string();
    let result = comparer.get_script();
    assert_eq!(result, "SELECT E FROM t;\n");
}

#[tokio::test]
async fn use_comments_false_preserves_empty_e_string() {
    let from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());
    let mut comparer = Comparer::new(from_dump, to_dump, false, false, false, GrantsMode::Ignore);
    // An empty E-string E'' must not confuse the state machine.
    comparer.script = "SELECT E'' AS val; /* strip */\n".to_string();
    let result = comparer.get_script();
    assert_eq!(result, "SELECT E'' AS val;\n");
}

#[tokio::test]
async fn use_comments_false_preserves_e_string_with_other_backslash_sequences() {
    let from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());
    let mut comparer = Comparer::new(from_dump, to_dump, false, false, false, GrantsMode::Ignore);
    // \n and \t are everyday escape sequences; the scanner must copy them
    // verbatim and must not mistake the character after the backslash for
    // anything other than the second byte of the pair.
    comparer.script =
        "SELECT E'line1\\nline2\\ttabbed -- not a comment' AS val; -- strip\n".to_string();
    let result = comparer.get_script();
    assert_eq!(
        result,
        "SELECT E'line1\\nline2\\ttabbed -- not a comment' AS val;\n"
    );
}

#[tokio::test]
async fn use_comments_false_preserves_multiple_e_strings_in_one_statement() {
    let from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());
    let mut comparer = Comparer::new(from_dump, to_dump, false, false, false, GrantsMode::Ignore);
    // Multiple E-strings in one statement; real trailing comment stripped.
    comparer.script = "INSERT INTO t VALUES (E'val\\'1 -- x', E'val/*2*/'); -- strip\n".to_string();
    let result = comparer.get_script();
    assert_eq!(
        result,
        "INSERT INTO t VALUES (E'val\\'1 -- x', E'val/*2*/');\n"
    );
}

#[tokio::test]
async fn use_comments_false_preserves_e_string_adjacent_to_double_quoted_identifier() {
    let from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());
    let mut comparer = Comparer::new(from_dump, to_dump, false, false, false, GrantsMode::Ignore);
    // E-string and double-quoted identifier in the same statement; both
    // preserved, real trailing comment stripped.
    comparer.script =
        "INSERT INTO \"my--table\" (col) VALUES (E'it\\'s -- ok'); -- strip\n".to_string();
    let result = comparer.get_script();
    assert_eq!(
        result,
        "INSERT INTO \"my--table\" (col) VALUES (E'it\\'s -- ok');\n"
    );
}

#[tokio::test]
async fn use_comments_false_preserves_double_quoted_identifier_comments() {
    let from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());
    let mut comparer = Comparer::new(from_dump, to_dump, false, false, false, GrantsMode::Ignore);
    // Double-quoted identifiers containing sequences that look like comment
    // starters must be passed through verbatim and must NOT be stripped.
    comparer.script =
        "SELECT 1 AS \"col--name\", 2 AS \"/*not a comment*/\"; -- real comment\n".to_string();
    let result = comparer.get_script();
    assert_eq!(
        result,
        "SELECT 1 AS \"col--name\", 2 AS \"/*not a comment*/\";\n"
    );
}

#[tokio::test]
async fn use_comments_false_preserves_double_quoted_identifier_with_escaped_quote() {
    let from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());
    let mut comparer = Comparer::new(from_dump, to_dump, false, false, false, GrantsMode::Ignore);
    // A doubled double-quote inside a quoted identifier is an escape sequence
    // and must survive comment stripping intact.
    comparer.script =
        "ALTER TABLE t RENAME COLUMN \"col\"\"--name\" TO new_name; /* drop this */\n".to_string();
    let result = comparer.get_script();
    assert_eq!(
        result,
        "ALTER TABLE t RENAME COLUMN \"col\"\"--name\" TO new_name;\n"
    );
}

#[tokio::test]
async fn use_comments_false_preserves_multiple_double_quoted_identifiers() {
    let from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());
    let mut comparer = Comparer::new(from_dump, to_dump, false, false, false, GrantsMode::Ignore);
    // Several double-quoted identifiers in one statement, each containing
    // comment-like sequences; only the trailing real comment should be stripped.
    comparer.script = "SELECT \"a--b\", \"c/*d*/e\", \"f--g\" FROM t; -- strip me\n".to_string();
    let result = comparer.get_script();
    assert_eq!(result, "SELECT \"a--b\", \"c/*d*/e\", \"f--g\" FROM t;\n");
}

#[tokio::test]
async fn use_comments_false_preserves_qualified_double_quoted_name() {
    let from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());
    let mut comparer = Comparer::new(from_dump, to_dump, false, false, false, GrantsMode::Ignore);
    // Quoted schema + quoted table, both containing comment-like sequences.
    comparer.script =
        "CREATE TABLE \"my--schema\".\"my/*table*/\" (id int); /* strip */\n".to_string();
    let result = comparer.get_script();
    assert_eq!(
        result,
        "CREATE TABLE \"my--schema\".\"my/*table*/\" (id int);\n"
    );
}

#[tokio::test]
async fn use_comments_false_preserves_empty_double_quoted_identifier() {
    let from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());
    let mut comparer = Comparer::new(from_dump, to_dump, false, false, false, GrantsMode::Ignore);
    // "" is a valid (if unusual) quoted identifier; must not confuse the state machine.
    comparer.script = "ALTER INDEX \"\" RENAME TO x; /* strip */\n".to_string();
    let result = comparer.get_script();
    assert_eq!(result, "ALTER INDEX \"\" RENAME TO x;\n");
}

#[tokio::test]
async fn use_comments_false_strips_comment_after_double_quoted_identifier() {
    let from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());
    let mut comparer = Comparer::new(from_dump, to_dump, false, false, false, GrantsMode::Ignore);
    // The parser must exit the double-quote state correctly so the real block
    // comment that follows is still stripped.
    comparer.script = "SELECT \"col\" /* strip this */ FROM t;\n".to_string();
    let result = comparer.get_script();
    assert_eq!(result, "SELECT \"col\"  FROM t;\n");
}

#[tokio::test]
async fn use_comments_false_mixed_double_and_single_quoted_with_comment() {
    let from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());
    let mut comparer = Comparer::new(from_dump, to_dump, false, false, false, GrantsMode::Ignore);
    // Double-quoted identifier and single-quoted string both containing
    // comment-like bytes; trailing real comment must still be stripped.
    comparer.script =
        "INSERT INTO \"my--table\" (col) VALUES ('/* not */ a -- val'); -- strip\n".to_string();
    let result = comparer.get_script();
    assert_eq!(
        result,
        "INSERT INTO \"my--table\" (col) VALUES ('/* not */ a -- val');\n"
    );
}

#[tokio::test]
async fn use_comments_false_returns_empty_for_comment_only_script() {
    let from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());
    let mut comparer = Comparer::new(from_dump, to_dump, false, false, false, GrantsMode::Ignore);
    comparer.script = "/* only a comment */\n-- another comment\n".to_string();
    let result = comparer.get_script();
    assert_eq!(result, "");
}

#[tokio::test]
async fn use_comments_false_collapses_excess_newlines() {
    let from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());
    let mut comparer = Comparer::new(from_dump, to_dump, false, false, false, GrantsMode::Ignore);
    comparer.script =
        "CREATE TABLE t1 (id int);\n/* removed */\n\n\n\nCREATE TABLE t2 (id int);\n".to_string();
    let result = comparer.get_script();
    assert_eq!(
        result,
        "CREATE TABLE t1 (id int);\n\nCREATE TABLE t2 (id int);\n"
    );
}

#[tokio::test]
async fn use_comments_true_preserves_all_comments() {
    let from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());
    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Ignore);
    comparer.script = "/* header */\nCREATE TABLE t1 (id int); -- inline\n".to_string();
    let result = comparer.get_script();
    assert_eq!(
        result,
        "/* header */\nCREATE TABLE t1 (id int); -- inline\n"
    );
}

#[tokio::test]
async fn use_comments_false_preserves_utf8() {
    let from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());
    let mut comparer = Comparer::new(from_dump, to_dump, false, false, false, GrantsMode::Ignore);
    comparer.script =
        "/* comment */\nCOMMENT ON TABLE t IS '数据表 — 描述';\nSELECT $$函数体$$;\n".to_string();
    let result = comparer.get_script();
    assert_eq!(
        result,
        "COMMENT ON TABLE t IS '数据表 — 描述';\nSELECT $$函数体$$;\n"
    );
}

/// Helper: build a Comparer with use_comments=false and a given script body.
fn comparer_with_script(script: &str) -> Comparer {
    let from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());
    let mut c = Comparer::new(from_dump, to_dump, false, false, false, GrantsMode::Ignore);
    c.script = script.to_string();
    c
}

#[test]
fn get_script_collapses_triple_newlines_outside_dollar_quotes() {
    let input = "SELECT 1;\n\n\n\nSELECT 2;\n";
    let c = comparer_with_script(input);
    let out = c.get_script();
    // 4 newlines should be collapsed to 2
    assert_eq!(out, "SELECT 1;\n\nSELECT 2;\n");
}

#[test]
fn get_script_preserves_triple_newlines_inside_dollar_quotes() {
    let input = concat!(
        "CREATE OR REPLACE PROCEDURE public.test_proc() LANGUAGE plpgsql AS $$\n",
        "BEGIN\n",
        "  RAISE NOTICE 'block 1';\n",
        "\n",
        "\n",
        "\n",
        "  RAISE NOTICE 'block 2';\n",
        "END;\n",
        "$$;\n",
    );
    let c = comparer_with_script(input);
    let out = c.get_script();
    // The three consecutive newlines inside $$ must survive
    assert!(
        out.contains("'block 1';\n\n\n\n  RAISE NOTICE 'block 2'"),
        "blank lines inside $$ body must be preserved, got:\n{}",
        out
    );
}

#[test]
fn get_script_preserves_newlines_inside_tagged_dollar_quotes() {
    let input = concat!(
        "CREATE FUNCTION f() RETURNS void LANGUAGE plpgsql AS $body$\n",
        "BEGIN\n",
        "\n",
        "\n",
        "\n",
        "  NULL;\n",
        "END;\n",
        "$body$;\n",
    );
    let c = comparer_with_script(input);
    let out = c.get_script();
    assert!(
        out.contains("BEGIN\n\n\n\n  NULL;"),
        "blank lines inside $body$ must be preserved, got:\n{}",
        out
    );
}

#[test]
fn get_script_collapses_newlines_between_dollar_quoted_blocks() {
    // Newlines *outside* dollar-quoted blocks should still be collapsed
    let input = "$$body1$$;\n\n\n\n$$body2$$;\n";
    let c = comparer_with_script(input);
    let out = c.get_script();
    assert_eq!(out, "$$body1$$;\n\n$$body2$$;\n");
}

#[test]
fn get_script_mixed_dollar_quote_and_outside_newlines() {
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
    let c = comparer_with_script(input);
    let out = c.get_script();

    // Outside: collapsed
    assert!(
        !out.contains("SELECT 1;\n\n\n"),
        "newlines before $$ block should be collapsed"
    );
    assert!(
        !out.contains("$$;\n\n\n"),
        "newlines after $$ block should be collapsed"
    );
    // Inside: preserved
    assert!(
        out.contains("BEGIN\n\n\n\n  NULL;"),
        "blank lines inside $$ must be preserved, got:\n{}",
        out
    );
}

#[test]
fn get_script_with_use_comments_true_returns_verbatim() {
    let input = "SELECT 1;\n\n\n\nSELECT 2;\n";
    let from_dump = Dump::new(DumpConfig::default());
    let to_dump = Dump::new(DumpConfig::default());
    let mut c = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Ignore);
    c.script = input.to_string();
    let out = c.get_script();
    // With use_comments=true, script is returned as-is
    assert_eq!(out, input);
}

#[test]
fn get_script_strips_comments_but_preserves_dollar_body_newlines() {
    let input = concat!(
        "-- a comment\n",
        "CREATE FUNCTION f() RETURNS void AS $$\n",
        "BEGIN\n",
        "\n\n\n",
        "  NULL;\n",
        "END;\n",
        "$$;\n",
    );
    let c = comparer_with_script(input);
    let out = c.get_script();
    // Comment removed
    assert!(
        !out.contains("-- a comment"),
        "line comment should be removed"
    );
    // Dollar body preserved
    assert!(
        out.contains("BEGIN\n\n\n\n  NULL;"),
        "blank lines inside $$ body must be preserved, got:\n{}",
        out
    );
}

#[test]
fn get_script_empty_dollar_body_not_corrupted() {
    let input = "CREATE FUNCTION f() AS $$$$;\n";
    let c = comparer_with_script(input);
    let out = c.get_script();
    assert!(out.contains("$$$$"), "empty dollar body must be preserved");
}

#[test]
fn get_script_unterminated_dollar_quote_copies_to_end() {
    // Unterminated dollar-quote: everything after opening tag should be
    // copied verbatim (same as the comment-stripping pass behaviour).
    let input = "CREATE FUNCTION f() AS $$\nBEGIN\n\n\n\n  NULL;\n";
    let c = comparer_with_script(input);
    let out = c.get_script();
    assert!(
        out.contains("\n\n\n\n"),
        "unterminated $$ body newlines must be preserved, got:\n{}",
        out
    );
}

#[test]
fn get_script_preserves_newlines_inside_single_quoted_string() {
    // Multi-line COMMENT body in a single-quoted literal must not be collapsed.
    let input = "COMMENT ON TABLE t IS 'line1\n\n\n\nline5';\n";
    let c = comparer_with_script(input);
    let out = c.get_script();
    assert!(
        out.contains("'line1\n\n\n\nline5'"),
        "newlines inside single-quoted string must be preserved, got:\n{}",
        out
    );
}

#[test]
fn get_script_preserves_newlines_inside_e_string() {
    // E-string literal with multi-line content.
    let input = "SELECT E'first\n\n\n\nlast';\n";
    let c = comparer_with_script(input);
    let out = c.get_script();
    assert!(
        out.contains("E'first\n\n\n\nlast'"),
        "newlines inside E-string must be preserved, got:\n{}",
        out
    );
}

#[test]
fn get_script_preserves_newlines_inside_double_quoted_identifier() {
    // Unusual but legal: double-quoted identifiers can contain newlines.
    let input = "SELECT \"col\n\n\n\nname\";\n";
    let c = comparer_with_script(input);
    let out = c.get_script();
    assert!(
        out.contains("\"col\n\n\n\nname\""),
        "newlines inside double-quoted identifier must be preserved, got:\n{}",
        out
    );
}

#[test]
fn get_script_collapses_newlines_between_single_quoted_strings() {
    // Newlines *outside* quoted strings should still be collapsed.
    let input = "SELECT 'a';\n\n\n\nSELECT 'b';\n";
    let c = comparer_with_script(input);
    let out = c.get_script();
    assert_eq!(out, "SELECT 'a';\n\nSELECT 'b';\n");
}

#[test]
fn get_script_e_string_with_escaped_quote_and_newlines() {
    // E-string with \' inside — must not terminate early.
    let input = "SELECT E'it\\'s\n\n\n\nfine';\n";
    let c = comparer_with_script(input);
    let out = c.get_script();
    assert!(
        out.contains("E'it\\'s\n\n\n\nfine'"),
        "E-string with escaped quote and newlines must be preserved, got:\n{}",
        out
    );
}

#[test]
fn get_script_single_quoted_doubled_quote_and_newlines() {
    // Standard single-quoted string with '' escape and embedded newlines.
    let input = "SELECT 'it''s\n\n\n\nfine';\n";
    let c = comparer_with_script(input);
    let out = c.get_script();
    assert!(
        out.contains("'it''s\n\n\n\nfine'"),
        "single-quoted string with doubled quote and newlines must be preserved, got:\n{}",
        out
    );
}

#[test]
fn get_script_lowercase_e_string_preserves_newlines() {
    // Lowercase e should be recognised as an E-string opener too.
    let input = "SELECT e'first\n\n\n\nlast';\n";
    let c = comparer_with_script(input);
    let out = c.get_script();
    assert!(
        out.contains("e'first\n\n\n\nlast'"),
        "newlines inside lowercase e-string must be preserved, got:\n{}",
        out
    );
}

#[test]
fn get_script_unterminated_single_quoted_string_copies_to_end() {
    let input = "SELECT 'unterminated\n\n\n\nstring\n";
    let c = comparer_with_script(input);
    let out = c.get_script();
    assert!(
        out.contains("\n\n\n\n"),
        "unterminated single-quoted string newlines must be preserved, got:\n{}",
        out
    );
}

#[test]
fn get_script_unterminated_e_string_copies_to_end() {
    let input = "SELECT E'unterminated\n\n\n\nstring\n";
    let c = comparer_with_script(input);
    let out = c.get_script();
    assert!(
        out.contains("\n\n\n\n"),
        "unterminated E-string newlines must be preserved, got:\n{}",
        out
    );
}

#[test]
fn get_script_unterminated_double_quoted_identifier_copies_to_end() {
    let input = "SELECT \"unterminated\n\n\n\nident\n";
    let c = comparer_with_script(input);
    let out = c.get_script();
    assert!(
        out.contains("\n\n\n\n"),
        "unterminated double-quoted identifier newlines must be preserved, got:\n{}",
        out
    );
}

#[test]
fn get_script_empty_single_quoted_string_no_corruption() {
    // Empty string '' should not confuse the scanner.
    let input = "SELECT '';\n\n\n\nSELECT 1;\n";
    let c = comparer_with_script(input);
    let out = c.get_script();
    assert_eq!(out, "SELECT '';\n\nSELECT 1;\n");
}

#[test]
fn get_script_newline_count_resets_after_single_quoted_literal() {
    // Two newlines before a quoted literal, then two newlines after it —
    // neither run alone exceeds 2 so nothing should be collapsed.
    let input = "A;\n\n'inside\n\n\n\ntext';\n\nB;\n";
    let c = comparer_with_script(input);
    let out = c.get_script();
    assert!(
        out.contains("'inside\n\n\n\ntext'"),
        "newlines inside literal must be preserved, got:\n{}",
        out
    );
    // The two newlines before and after the literal should survive.
    assert!(
        out.contains("A;\n\n'inside"),
        "two newlines before literal should survive, got:\n{}",
        out
    );
    assert!(
        out.contains("';\n\nB;"),
        "two newlines after literal should survive, got:\n{}",
        out
    );
}

#[test]
fn get_script_collapses_after_quoted_literal_with_excess_newlines() {
    // Excess newlines *after* a quoted literal should still be collapsed.
    let input = "SELECT 'hello';\n\n\n\nSELECT 'world';\n";
    let c = comparer_with_script(input);
    let out = c.get_script();
    assert_eq!(out, "SELECT 'hello';\n\nSELECT 'world';\n");
}

#[test]
fn get_script_mixed_literal_types_with_newlines() {
    // Mix of dollar-quoted, single-quoted, E-string, and double-quoted
    // literals each containing newlines that must be preserved, separated
    // by excessive newlines that should be collapsed.
    let input = concat!(
        "COMMENT ON TABLE t IS 'line1\n\n\n\nline5';\n",
        "\n\n\n\n",
        "SELECT E'a\n\n\n\nb';\n",
        "\n\n\n\n",
        "SELECT \"id\n\n\n\ncol\";\n",
        "\n\n\n\n",
        "CREATE FUNCTION f() AS $$\nBEGIN\n\n\n\n  NULL;\nEND;\n$$;\n",
    );
    let c = comparer_with_script(input);
    let out = c.get_script();
    // Inside literals: preserved
    assert!(
        out.contains("'line1\n\n\n\nline5'"),
        "single-quoted newlines must be preserved"
    );
    assert!(
        out.contains("E'a\n\n\n\nb'"),
        "E-string newlines must be preserved"
    );
    assert!(
        out.contains("\"id\n\n\n\ncol\""),
        "double-quoted newlines must be preserved"
    );
    assert!(
        out.contains("BEGIN\n\n\n\n  NULL;"),
        "dollar-quoted newlines must be preserved"
    );
    // Outside literals: collapsed (no run of 3+ newlines between statements)
    let between_stmts = out
        .split("'line1\n\n\n\nline5';")
        .nth(1)
        .unwrap()
        .split("E'a\n\n\n\nb'")
        .next()
        .unwrap();
    assert!(
        !between_stmts.contains("\n\n\n"),
        "newlines between statements should be collapsed, got segment: {:?}",
        between_stmts
    );
}

#[test]
fn get_script_e_string_escaped_backslash_then_newlines() {
    // E'foo\\' — the \\\\ is an escaped backslash, so the next ' closes
    // the string.  Newlines outside should be collapsed.
    let input = "SELECT E'foo\\\\';\n\n\n\nSELECT 1;\n";
    let c = comparer_with_script(input);
    let out = c.get_script();
    assert_eq!(out, "SELECT E'foo\\\\';\n\nSELECT 1;\n");
}

#[test]
fn get_script_double_quoted_doubled_escape_and_newlines() {
    // Double-quoted identifier with "" escape and embedded newlines.
    let input = "SELECT \"col\"\"\n\n\n\nname\";\n";
    let c = comparer_with_script(input);
    let out = c.get_script();
    assert!(
        out.contains("\"col\"\"\n\n\n\nname\""),
        "double-quoted identifier with escaped quote and newlines must be preserved, got:\n{}",
        out
    );
}

#[test]
fn get_script_comment_stripped_but_single_quoted_newlines_preserved() {
    // The comment-stripping pass runs first; the collapsing pass must
    // still preserve newlines inside single-quoted strings.
    let input = concat!(
        "-- strip this\n",
        "COMMENT ON TABLE t IS 'multi\n\n\n\nline';\n",
    );
    let c = comparer_with_script(input);
    let out = c.get_script();
    assert!(!out.contains("-- strip this"), "comment should be removed");
    assert!(
        out.contains("'multi\n\n\n\nline'"),
        "single-quoted newlines must survive comment stripping + collapsing, got:\n{}",
        out
    );
}

#[test]
fn get_script_block_comment_stripped_but_e_string_newlines_preserved() {
    let input = concat!("/* block comment */\n", "SELECT E'keep\n\n\n\nme';\n",);
    let c = comparer_with_script(input);
    let out = c.get_script();
    assert!(
        !out.contains("block comment"),
        "block comment should be removed"
    );
    assert!(
        out.contains("E'keep\n\n\n\nme'"),
        "E-string newlines must survive after block comment stripping, got:\n{}",
        out
    );
}

#[test]
fn get_script_adjacent_single_quoted_strings_both_preserved() {
    // Two single-quoted strings back-to-back, each with internal newlines.
    let input = "SELECT 'a\n\n\n\nb' || 'c\n\n\n\nd';\n";
    let c = comparer_with_script(input);
    let out = c.get_script();
    assert!(
        out.contains("'a\n\n\n\nb'"),
        "first literal newlines must be preserved, got:\n{}",
        out
    );
    assert!(
        out.contains("'c\n\n\n\nd'"),
        "second literal newlines must be preserved, got:\n{}",
        out
    );
}

#[test]
fn get_script_dollar_body_containing_single_quoted_newlines() {
    // A dollar-quoted body that itself contains a single-quoted string
    // with many newlines — everything inside $$ is already copied verbatim
    // by the dollar-quote branch, so the inner literal is preserved too.
    let input = concat!(
        "CREATE FUNCTION f() AS $$\n",
        "BEGIN\n",
        "  RAISE NOTICE 'msg\n\n\n\nend';\n",
        "END;\n",
        "$$;\n",
    );
    let c = comparer_with_script(input);
    let out = c.get_script();
    assert!(
        out.contains("'msg\n\n\n\nend'"),
        "single-quoted literal inside dollar body must be preserved, got:\n{}",
        out
    );
}

#[test]
fn get_script_single_quoted_string_without_excess_newlines_unchanged() {
    // A single-quoted string with exactly 2 newlines (not excess) — should
    // pass through without any modification.
    let input = "SELECT 'a\n\nb';\n";
    let c = comparer_with_script(input);
    let out = c.get_script();
    assert_eq!(out, "SELECT 'a\n\nb';\n");
}

#[test]
fn get_script_e_identifier_not_confused_with_e_string() {
    // A column named "E" followed by a comparison — the E is followed by
    // a space, not a quote, so it must not be mistaken for an E-string.
    let input = "SELECT E = 1;\n\n\n\nSELECT 2;\n";
    let c = comparer_with_script(input);
    let out = c.get_script();
    assert_eq!(out, "SELECT E = 1;\n\nSELECT 2;\n");
}

#[test]
fn get_script_multiple_e_strings_on_same_line() {
    let input = "SELECT E'x\n\n\n\ny', E'a\n\n\n\nb';\n";
    let c = comparer_with_script(input);
    let out = c.get_script();
    assert!(
        out.contains("E'x\n\n\n\ny'"),
        "first E-string newlines must be preserved, got:\n{}",
        out
    );
    assert!(
        out.contains("E'a\n\n\n\nb'"),
        "second E-string newlines must be preserved, got:\n{}",
        out
    );
}
