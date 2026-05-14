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
