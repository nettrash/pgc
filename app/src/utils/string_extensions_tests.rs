use super::*;

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
