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
mod tests {
    use super::*;

    #[test]
    fn from_str_ignore() {
        assert_eq!("ignore".parse::<GrantsMode>().unwrap(), GrantsMode::Ignore);
        assert_eq!("IGNORE".parse::<GrantsMode>().unwrap(), GrantsMode::Ignore);
        assert_eq!("Ignore".parse::<GrantsMode>().unwrap(), GrantsMode::Ignore);
    }

    #[test]
    fn from_str_addonly_aliases() {
        assert_eq!(
            "addonly".parse::<GrantsMode>().unwrap(),
            GrantsMode::AddOnly
        );
        assert_eq!(
            "ADDONLY".parse::<GrantsMode>().unwrap(),
            GrantsMode::AddOnly
        );
        assert_eq!(
            "add_only".parse::<GrantsMode>().unwrap(),
            GrantsMode::AddOnly
        );
        assert_eq!(
            "ADD_ONLY".parse::<GrantsMode>().unwrap(),
            GrantsMode::AddOnly
        );
        assert_eq!(
            "add-only".parse::<GrantsMode>().unwrap(),
            GrantsMode::AddOnly
        );
        assert_eq!(
            "ADD-ONLY".parse::<GrantsMode>().unwrap(),
            GrantsMode::AddOnly
        );
        assert_eq!(
            "Add-Only".parse::<GrantsMode>().unwrap(),
            GrantsMode::AddOnly
        );
    }

    #[test]
    fn from_str_full() {
        assert_eq!("full".parse::<GrantsMode>().unwrap(), GrantsMode::Full);
        assert_eq!("FULL".parse::<GrantsMode>().unwrap(), GrantsMode::Full);
        assert_eq!("Full".parse::<GrantsMode>().unwrap(), GrantsMode::Full);
    }

    #[test]
    fn from_str_invalid() {
        assert!("bogus".parse::<GrantsMode>().is_err());
        assert!("".parse::<GrantsMode>().is_err());
    }
}
