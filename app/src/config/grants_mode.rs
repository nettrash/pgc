use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

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

impl FromStr for GrantsMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "ignore" => Ok(Self::Ignore),
            "addonly" | "add_only" | "add-only" => Ok(Self::AddOnly),
            "full" => Ok(Self::Full),
            _ => Err(format!(
                "invalid grants mode '{}'; valid values: ignore, addonly, add_only, add-only, full",
                s
            )),
        }
    }
}

#[cfg(test)]
#[path = "grants_mode_tests.rs"]
mod tests;
