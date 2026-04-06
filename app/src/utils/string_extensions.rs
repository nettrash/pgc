const EMPTY_LINES: &str = "\n\n";

pub trait StringExt {
    fn with_empty_lines(self) -> String;

    fn append_block(&mut self, other: &str);
}

impl StringExt for String {
    fn with_empty_lines(mut self) -> String {
        self.push_str(EMPTY_LINES);
        self
    }

    fn append_block(&mut self, other: &str) {
        self.push_str(other);
        self.push_str(EMPTY_LINES);
    }
}

#[cfg(test)]
mod tests {
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
}
