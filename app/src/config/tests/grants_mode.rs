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
