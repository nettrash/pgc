use serde::{Deserialize, Serialize};
use std::fmt;

/// Controls how grants (privileges) are handled during comparison.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum GrantsMode {
    /// Ignore grants entirely (default, current behaviour).
    #[default]
    Ignore,
    /// Only add grants that exist in "to" but not in "from" (additive).
    AddOnly,
    /// Make grants identical: add missing and revoke extra.
    Full,
}

impl fmt::Display for GrantsMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ignore => write!(f, "ignore"),
            Self::AddOnly => write!(f, "addonly"),
            Self::Full => write!(f, "full"),
        }
    }
}

impl GrantsMode {
    /// Parse from a case-insensitive string. Panics on invalid value.
    pub fn from_str_or_panic(s: &str) -> Self {
        match s.to_uppercase().as_str() {
            "IGNORE" => Self::Ignore,
            "ADDONLY" | "ADD_ONLY" => Self::AddOnly,
            "FULL" => Self::Full,
            _ => panic!(
                "Invalid value for GRANTS_MODE: '{}'. Expected: ignore, addonly, full.",
                s
            ),
        }
    }
}
