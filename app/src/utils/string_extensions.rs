const EMPTY_LINES: &str = "\n\n";

pub trait StringExt {
    fn with_empty_lines(self) -> String;

    fn append_block(&mut self, other: &str);
}

/// Normalize CRLF line endings to LF, returning the original string
/// unchanged (no allocation) when it contains no `\r\n`.
pub fn normalize_line_endings(s: String) -> String {
    if s.contains("\r\n") {
        s.replace("\r\n", "\n")
    } else {
        s
    }
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
#[path = "string_extensions_tests.rs"]
mod tests;
