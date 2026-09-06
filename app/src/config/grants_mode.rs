//! How privilege differences are handled — the `--grants-mode` flag.

use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

/// Controls how grants (privileges) are handled during comparison.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
///
/// Parsed from the `--grants-mode` flag or the `GRANTS_MODE` config key, and
/// rendered back with [`Display`](std::fmt::Display).
///
/// ```
/// use pgc::config::grants_mode::GrantsMode;
///
/// assert_eq!("ignore".parse(), Ok(GrantsMode::Ignore));
/// assert_eq!("full".parse(), Ok(GrantsMode::Full));
///
/// // The additive mode accepts three spellings.
/// for spelling in ["addonly", "add_only", "add-only"] {
///     assert_eq!(spelling.parse(), Ok(GrantsMode::AddOnly));
/// }
///
/// // Matching is case-insensitive, and Display round-trips.
/// assert_eq!("FULL".parse::<GrantsMode>().unwrap().to_string(), "full");
///
/// assert!("sometimes".parse::<GrantsMode>().is_err());
/// ```
///
/// The default is [`GrantsMode::Ignore`] — privilege diffs are not emitted
/// unless asked for.
///
/// ```
/// # use pgc::config::grants_mode::GrantsMode;
/// assert_eq!(GrantsMode::default(), GrantsMode::Ignore);
/// ```
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
#[path = "tests/grants_mode.rs"]
mod tests;
