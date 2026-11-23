use super::*;
use std::borrow::Cow;

#[test]
fn with_empty_lines_appends_two_newlines() {
    let s = "abc".to_string();
    assert_eq!(s.with_empty_lines(), "abc\n\n");
}

#[test]
fn append_block_appends_content_and_two_newlines() {
    let mut s = "a".to_string();
    s.append_block("b");
    assert_eq!(s, "ab\n\n");
}

#[test]
fn unquote_ident_reverses_quote_ident() {
    // Unquoted input is already the catalog name.
    assert_eq!(unquote_ident("base"), "base");
    // quote_ident only wraps the name; the interior is the catalog name verbatim.
    assert_eq!(unquote_ident("\"MyBase\""), "MyBase");
    assert_eq!(unquote_ident("\"select\""), "select");
    // A doubled quote inside the wrapper is one literal quote in the catalog.
    assert_eq!(unquote_ident("\"has\"\"dq\""), "has\"dq");
    // Characters that are legal inside an identifier survive untouched.
    assert_eq!(unquote_ident("\"it's\""), "it's");
    assert_eq!(unquote_ident("\"a`b\""), "a`b");
    // A lone quote is not a quoted rendering and must not be mangled.
    assert_eq!(unquote_ident("\""), "\"");
    assert_eq!(unquote_ident("\"\""), "");
}

#[test]
fn unquote_ident_keeps_case_distinct_relations_apart() {
    // PostgreSQL stores "A" and a as different relations; folding case here would
    // fuse them into one key and misdirect view dependency edges.
    assert_ne!(unquote_ident("\"A\""), unquote_ident("a"));
    assert_eq!(unquote_ident("\"A\""), "A");
    assert_eq!(unquote_ident("a"), "a");
}

// ---- Values inside SQL comments -------------------------------------------

/// The `str::lines` comment-out idiom `comment_out` replaced, kept here as
/// the reference its output must match for every input without a bare `\r`.
fn lines_idiom(sql: &str) -> String {
    sql.lines().map(|l| format!("-- {l}\n")).collect()
}

#[test]
fn line_comment_safe_borrows_an_ordinary_value() {
    for value in ["", "s.t", "\"My Table\"", "a -- b", "x*/y", "/* z */"] {
        assert!(
            matches!(line_comment_safe(value), Cow::Borrowed(v) if v == value),
            "{value:?} must come back borrowed and unchanged"
        );
    }
}

#[test]
fn line_comment_safe_turns_every_line_break_into_a_space() {
    assert_eq!(
        line_comment_safe("\"v\ndrop table t; --\""),
        "\"v drop table t; --\""
    );
    // A bare CR ends a `--` comment in PostgreSQL's lexer and in psql.
    assert_eq!(line_comment_safe("a\rb"), "a b");
    // One space per character, as pg_dump's sanitize_line does.
    assert_eq!(line_comment_safe("a\r\nb"), "a  b");
    assert_eq!(line_comment_safe("\n\r"), "  ");
}

#[test]
fn block_comment_safe_borrows_an_ordinary_value() {
    for value in [
        "",
        "s.t",
        "\"My Table\"",
        "a -- b",
        "a*b",
        "a/b",
        "*a",
        "\"x/\"",
    ] {
        assert!(
            matches!(block_comment_safe(value), Cow::Borrowed(v) if v == value),
            "{value:?} must come back borrowed and unchanged"
        );
    }
}

#[test]
fn block_comment_safe_splits_comment_delimiters() {
    assert_eq!(
        block_comment_safe("\"x*/ create table t(); /*\""),
        "\"x* / create table t(); / *\""
    );
    assert_eq!(block_comment_safe("\"nest/*ed\""), "\"nest/ *ed\"");
    // Overlapping delimiters are split one by one.
    assert_eq!(block_comment_safe("/*/"), "/ * / ");
    assert_eq!(block_comment_safe("*/*"), "* / *");
    assert_eq!(block_comment_safe("**//"), "** // ");
    // Line breaks go too, so a header stays on one line.
    assert_eq!(block_comment_safe("a\r\nb\rc"), "a  b c");
}

#[test]
fn block_comment_safe_keeps_a_trailing_slash_off_the_closing_delimiter() {
    // `/* Table: {}*/` with `x/` would read `x/*/`: a nested comment opener.
    assert_eq!(block_comment_safe("x/"), "x/ ");
    assert_eq!(format!("/* {}*/", block_comment_safe("x/")), "/* x/ */");
}

#[test]
fn block_comment_safe_output_never_holds_a_delimiter() {
    let nasty = [
        "*/", "/*", "/*/", "*/*", "a*/b/*c", "//**", "*//*", "x/", "\n*/\r/*", "/***/",
    ];
    for value in nasty {
        let safe = block_comment_safe(value);
        assert!(
            !safe.contains("*/") && !safe.contains("/*") && !safe.contains(['\r', '\n']),
            "{value:?} -> {safe:?}"
        );
        // Nor may it fuse with the delimiters a header wraps it in.
        let header = format!("/* {safe}*/");
        assert_eq!(header.matches("/*").count(), 1, "{header:?}");
        assert!(header.ends_with("*/") && header.find("*/") == Some(header.len() - 2));
    }
}

#[test]
fn comment_out_matches_the_lines_idiom_without_a_bare_cr() {
    for sql in [
        "",
        "\n",
        "\n\n",
        "drop view s.v;",
        "drop view s.v;\n",
        "drop view s.v;\n\n",
        "create view s.v as\n select 1;\n\n",
        "a\r\nb\r\n",
        "a\n\nb",
        "  indented\n\ttabbed\n",
    ] {
        assert_eq!(comment_out(sql), lines_idiom(sql), "{sql:?}");
    }
}

#[test]
fn comment_out_ends_a_line_at_a_bare_cr() {
    // `str::lines` would have left `drop table t; --"` live after the CR.
    assert_eq!(
        comment_out("drop view s.\"v\rdrop table t; --\";\n\n"),
        "-- drop view s.\"v\n-- drop table t; --\";\n-- \n"
    );
    assert_eq!(comment_out("a\r"), "-- a\n");
    assert_eq!(comment_out("a\r\rb"), "-- a\n-- \n-- b\n");
}

#[test]
fn comment_out_leaves_no_line_uncommented() {
    for sql in ["a\nb", "a\rb", "a\r\nb", "\ra", "a\n\rb\r\n\nc\r"] {
        let out = comment_out(sql);
        for line in out.split(['\n', '\r']).filter(|l| !l.is_empty()) {
            assert!(line.starts_with("-- "), "{sql:?} -> {out:?}");
        }
    }
}

#[test]
fn comment_out_keep_blank_lines_matches_the_prefix_form_for_one_statement() {
    // What `format!("-- {}", drop_cmd)` produced for a one-line statement block.
    for sql in [
        "",
        "drop index s.i;",
        "drop index s.i;\n",
        "drop index s.i;\n\n",
    ] {
        assert_eq!(
            comment_out_keep_blank_lines(sql),
            format!("-- {sql}"),
            "{sql:?}"
        );
    }
}

#[test]
fn comment_out_keep_blank_lines_comments_every_later_line_with_content() {
    assert_eq!(
        comment_out_keep_blank_lines("drop trigger \"t\ndrop table x; --\" on s.t;\n\n"),
        "-- drop trigger \"t\n-- drop table x; --\" on s.t;\n\n"
    );
    assert_eq!(
        comment_out_keep_blank_lines("drop trigger \"t\rdrop table x; --\" on s.t;\n\n"),
        "-- drop trigger \"t\n-- drop table x; --\" on s.t;\n\n"
    );
    assert_eq!(
        comment_out_keep_blank_lines("a\r\n\r\nb\n\n"),
        "-- a\n\n-- b\n\n"
    );
}

// ==== fix:pre-grants tests ====

#[test]
fn quoted_keywords_are_sorted_for_binary_search() {
    assert!(QUOTED_KEYWORDS.windows(2).all(|w| w[0] < w[1]));
}

#[test]
fn quote_identifier_matches_postgresql_quote_ident() {
    // Bare: lowercase letters, digits and `_`, not starting with a digit.
    assert_eq!(quote_identifier("pub_1"), "pub_1");
    assert_eq!(quote_identifier("_p"), "_p");
    // Unreserved keywords stay bare, as quote_ident leaves them.
    assert_eq!(quote_identifier("publication"), "publication");
    assert_eq!(quote_identifier("name"), "name");
    // Every other keyword category is quoted: reserved, column-name and
    // type/function-name (PostgreSQL 17 added `json_table`).
    assert_eq!(quote_identifier("select"), "\"select\"");
    assert_eq!(quote_identifier("int"), "\"int\"");
    assert_eq!(quote_identifier("left"), "\"left\"");
    assert_eq!(quote_identifier("json_table"), "\"json_table\"");
    // Case, spaces, a leading digit, non-ASCII and quotes.
    assert_eq!(quote_identifier("MyPub"), "\"MyPub\"");
    assert_eq!(quote_identifier("pub two"), "\"pub two\"");
    assert_eq!(quote_identifier("1st"), "\"1st\"");
    assert_eq!(quote_identifier("café"), "\"café\"");
    assert_eq!(quote_identifier("a\"b"), "\"a\"\"b\"");
    assert_eq!(quote_identifier(""), "\"\"");
    // Borrowed when nothing changes.
    assert!(matches!(quote_identifier("p"), Cow::Borrowed("p")));
    // Round trip with unquote_ident.
    for raw in ["MyPub", "pub two", "a\"b", "select", "plain"] {
        assert_eq!(unquote_ident(&quote_identifier(raw)), raw);
    }
}

// ==== fix:r3-ddl tests ====

#[test]
fn keywords_quoted_since_are_sorted_quoted_keywords_of_16_and_17() {
    assert!(KEYWORDS_QUOTED_SINCE.windows(2).all(|w| w[0].0 < w[1].0));
    for (keyword, since) in KEYWORDS_QUOTED_SINCE {
        assert!(QUOTED_KEYWORDS.binary_search(keyword).is_ok(), "{keyword}");
        assert!([160000, 170000].contains(since), "{keyword}: {since}");
    }
}

/// `quote_ident()` of each version, verified live on PostgreSQL 14–19:
/// 14 and 15 leave every keyword PostgreSQL 16 and 17 added bare; 16 quotes
/// `json_array`, `json_arrayagg`, `json_object`, `json_objectagg` and
/// `system_user`; 17 and later quote all 13.
#[test]
fn quote_identifier_for_server_follows_the_version_keyword_list() {
    let added_in_16 = [
        "json_array",
        "json_arrayagg",
        "json_object",
        "json_objectagg",
        "system_user",
    ];
    let added_in_17 = [
        "json",
        "json_exists",
        "json_query",
        "json_scalar",
        "json_serialize",
        "json_table",
        "json_value",
        "merge_action",
    ];
    for version in [None, Some(140000), Some(150013)] {
        for word in added_in_16.iter().chain(&added_in_17) {
            assert!(
                matches!(quote_identifier_for_server(word, version), Cow::Borrowed(w) if w == *word),
                "{word} on {version:?}"
            );
        }
    }
    for version in [160000, 160009] {
        for word in added_in_16 {
            assert_eq!(
                quote_identifier_for_server(word, Some(version)),
                format!("\"{word}\"")
            );
        }
        for word in added_in_17 {
            assert_eq!(quote_identifier_for_server(word, Some(version)), word);
        }
    }
    for version in [170000, 180000, 190000] {
        for word in added_in_16.iter().chain(&added_in_17) {
            assert_eq!(
                quote_identifier_for_server(word, Some(version)),
                format!("\"{word}\"")
            );
            // The newest version's spelling is `quote_identifier`'s.
            assert_eq!(
                quote_identifier_for_server(word, Some(version)),
                quote_identifier(word)
            );
        }
    }
    // Every other keyword is quoted on every version, and the rest of the
    // rule is `quote_identifier`'s.
    for version in [None, Some(140000), Some(190000)] {
        for word in [
            "select", "left", "int", "position", "MyMv", "a\"b", "1st", "",
        ] {
            assert_eq!(
                quote_identifier_for_server(word, version),
                quote_identifier(word),
                "{word:?} on {version:?}"
            );
        }
        for word in ["plain_mv", "publication", "name"] {
            assert_eq!(quote_identifier_for_server(word, version), word);
        }
    }
}
