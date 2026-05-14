use crate::utils::string_extensions::StringExt;

/// Represents a single parsed PostgreSQL ACL entry.
///
/// PostgreSQL ACL items have the format: `grantee=privileges/grantor`
/// where privileges are single-character codes:
///   r = SELECT, a = INSERT, w = UPDATE, d = DELETE, D = TRUNCATE,
///   x = REFERENCES, t = TRIGGER, X = EXECUTE, U = USAGE, C = CREATE,
///   c = CONNECT, T = TEMPORARY
/// A `*` after a privilege character indicates WITH GRANT OPTION.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AclEntry {
    pub grantee: String,
    pub privileges: String,
    pub grantor: String,
}

/// A single privilege with its grant-option flag.
#[derive(Debug, Clone, PartialEq, Eq)]
struct PrivilegeItem {
    name: String,
    grant_option: bool,
}

/// Find the first byte position of `target` in `s` that lies outside any
/// double-quoted region. PostgreSQL aclitem text uses `"..."` to escape role
/// names containing special characters (`=`, `/`, whitespace, etc.) and `""`
/// as a literal quote inside a quoted region. A naive `str::find` on the
/// raw separator misparses entries like `"weird=name"=r/owner`.
fn find_unquoted(s: &str, target: u8) -> Option<usize> {
    let bytes = s.as_bytes();
    let mut i = 0;
    let mut in_quotes = false;
    while i < bytes.len() {
        let c = bytes[i];
        if in_quotes {
            if c == b'"' {
                if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                    i += 2;
                    continue;
                }
                in_quotes = false;
            }
        } else if c == b'"' {
            in_quotes = true;
        } else if c == target {
            return Some(i);
        }
        i += 1;
    }
    None
}

impl AclEntry {
    /// Parse a single ACL item string like `"user=arwdDxt/owner"`. Quoted
    /// role names with embedded `=` or `/` (e.g. `"weird=name"=r/owner`) are
    /// handled correctly via [`find_unquoted`].
    pub fn parse(acl_item: &str) -> Option<Self> {
        let eq_pos = find_unquoted(acl_item, b'=')?;
        // Search for `/` only after the `=` so a slash inside the grantee
        // name does not confuse us.
        let slash_rel = find_unquoted(&acl_item[eq_pos + 1..], b'/')?;
        let slash_pos = eq_pos + 1 + slash_rel;
        let grantee = acl_item[..eq_pos].to_string();
        let privileges = acl_item[eq_pos + 1..slash_pos].to_string();
        let grantor = acl_item[slash_pos + 1..].to_string();
        if privileges.is_empty() {
            return None;
        }
        Some(AclEntry {
            grantee,
            privileges,
            grantor,
        })
    }

    /// Map a single privilege character to its SQL keyword.
    fn priv_char_to_name(ch: char) -> Option<&'static str> {
        match ch {
            'r' => Some("SELECT"),
            'a' => Some("INSERT"),
            'w' => Some("UPDATE"),
            'd' => Some("DELETE"),
            'D' => Some("TRUNCATE"),
            'x' => Some("REFERENCES"),
            't' => Some("TRIGGER"),
            'X' => Some("EXECUTE"),
            'U' => Some("USAGE"),
            'C' => Some("CREATE"),
            'c' => Some("CONNECT"),
            'T' => Some("TEMPORARY"),
            'm' => Some("MAINTAIN"),
            _ => None,
        }
    }

    /// Valid privilege names for a given object kind.
    fn valid_privileges(object_kind: &str) -> &'static [&'static str] {
        match object_kind {
            "TABLE" | "FOREIGN TABLE" => &[
                "SELECT",
                "INSERT",
                "UPDATE",
                "DELETE",
                "TRUNCATE",
                "REFERENCES",
                "TRIGGER",
                "MAINTAIN",
            ],
            // PG17+ added MAINTAIN as a privilege bit, but it is only valid
            // on tables, views, materialised views, and foreign tables — not
            // on sequences. Listing only the valid set here causes
            // `parse_privileges` to silently drop a stray `m` if it ever
            // appears in a sequence ACL, which is the correct behaviour.
            "SEQUENCE" => &["USAGE", "SELECT", "UPDATE"],
            "FUNCTION" | "PROCEDURE" => &["EXECUTE"],
            "SCHEMA" => &["USAGE", "CREATE"],
            "TYPE" => &["USAGE"],
            "COLUMN" => &["SELECT", "INSERT", "UPDATE", "REFERENCES"],
            _ => &[],
        }
    }

    /// Parse privilege string into items, each with a grant-option flag.
    /// E.g. `"r*wad"` → SELECT(grant_option), UPDATE, INSERT, DELETE.
    fn parse_privileges(privs: &str, object_kind: &str) -> Vec<PrivilegeItem> {
        let valid = Self::valid_privileges(object_kind);
        let chars: Vec<char> = privs.chars().collect();
        let mut result = Vec::new();
        let mut i = 0;
        while i < chars.len() {
            if let Some(name) = Self::priv_char_to_name(chars[i]) {
                let grant_option = i + 1 < chars.len() && chars[i + 1] == '*';
                if grant_option {
                    i += 1; // skip the '*'
                }
                if valid.contains(&name) {
                    result.push(PrivilegeItem {
                        name: name.to_string(),
                        grant_option,
                    });
                }
            }
            i += 1;
        }
        result
    }

    fn format_grantee(grantee: &str) -> String {
        if grantee.is_empty() {
            "PUBLIC".to_string()
        } else {
            grantee.to_string()
        }
    }

    /// Generate GRANT statement(s) for this ACL entry on the given object.
    /// Privileges with and without GRANT OPTION are emitted as separate statements.
    pub fn get_grant_script(acl_item: &str, object_kind: &str, object_name: &str) -> String {
        let entry = match AclEntry::parse(acl_item) {
            Some(e) => e,
            None => return String::new(),
        };
        let items = Self::parse_privileges(&entry.privileges, object_kind);
        if items.is_empty() {
            return String::new();
        }
        let grantee = Self::format_grantee(&entry.grantee);
        let mut script = String::new();

        // Collect privileges without grant option
        let plain: Vec<&str> = items
            .iter()
            .filter(|p| !p.grant_option)
            .map(|p| p.name.as_str())
            .collect();
        if !plain.is_empty() {
            script.append_block(&format!(
                "GRANT {} ON {} {} TO {};",
                plain.join(", "),
                object_kind,
                object_name,
                grantee
            ));
        }

        // Collect privileges with grant option
        let with_go: Vec<&str> = items
            .iter()
            .filter(|p| p.grant_option)
            .map(|p| p.name.as_str())
            .collect();
        if !with_go.is_empty() {
            script.append_block(&format!(
                "GRANT {} ON {} {} TO {} WITH GRANT OPTION;",
                with_go.join(", "),
                object_kind,
                object_name,
                grantee
            ));
        }

        script
    }

    /// Generate REVOKE statement(s) for this ACL entry on the given object.
    /// When revoking privileges that had GRANT OPTION, emits REVOKE GRANT OPTION FOR first,
    /// then REVOKE for the privilege itself along with plain privileges.
    pub fn get_revoke_script(acl_item: &str, object_kind: &str, object_name: &str) -> String {
        let entry = match AclEntry::parse(acl_item) {
            Some(e) => e,
            None => return String::new(),
        };
        let items = Self::parse_privileges(&entry.privileges, object_kind);
        if items.is_empty() {
            return String::new();
        }
        let grantee = Self::format_grantee(&entry.grantee);
        let mut script = String::new();

        // For grant-option privileges, first revoke the grant option
        let with_go: Vec<&str> = items
            .iter()
            .filter(|p| p.grant_option)
            .map(|p| p.name.as_str())
            .collect();
        if !with_go.is_empty() {
            script.append_block(&format!(
                "REVOKE GRANT OPTION FOR {} ON {} {} FROM {};",
                with_go.join(", "),
                object_kind,
                object_name,
                grantee
            ));
        }

        // Revoke all privileges (both plain and grant-option ones)
        let all: Vec<&str> = items.iter().map(|p| p.name.as_str()).collect();
        script.append_block(&format!(
            "REVOKE {} ON {} {} FROM {};",
            all.join(", "),
            object_kind,
            object_name,
            grantee
        ));

        script
    }
}

/// Represents the per-grantee privilege diff between two ACL lists.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AclDiffEntry {
    pub grantee: String,
    pub grants_plain: Vec<String>,
    pub grants_with_option: Vec<String>,
    pub revoke_option_for: Vec<String>,
    pub revokes: Vec<String>,
}

/// Build per-grantee privilege map: grantee → {privilege_name → has_grant_option}.
/// Multiple ACL entries for the same grantee (from different grantors) are merged;
/// `grant_option = true` wins when the same privilege appears in multiple entries.
fn build_privilege_map(
    acl: &[String],
    object_kind: &str,
    owners: &[&str],
) -> std::collections::HashMap<String, std::collections::HashMap<String, bool>> {
    use std::collections::HashMap;
    let mut map: HashMap<String, HashMap<String, bool>> = HashMap::new();
    for item in acl {
        let Some(entry) = AclEntry::parse(item) else {
            // Surfacing unparseable ACL items prevents silent data loss when
            // a future PostgreSQL version introduces an aclitem syntax we do
            // not yet recognise — without this warning the offending grantee
            // would simply disappear from the diff.
            eprintln!("Warning: skipping unparseable ACL item ({object_kind}): {item:?}");
            continue;
        };
        if owners.contains(&entry.grantee.as_str()) {
            continue;
        }
        let privs = AclEntry::parse_privileges(&entry.privileges, object_kind);
        let grantee_map = map.entry(entry.grantee).or_default();
        for p in privs {
            let existing = grantee_map.entry(p.name).or_insert(false);
            if p.grant_option {
                *existing = true;
            }
        }
    }
    map
}

/// Compute the per-grantee privilege diff between two ACL lists.
///
/// Compares on a normalised representation (grantee + privilege set + grant-option),
/// **ignoring the grantor** field entirely.  Returns one [`AclDiffEntry`] per grantee
/// that has at least one action.  Revoke entries are only produced when `full` is `true`.
///
/// Owner filtering is asymmetric to model the effect of `ALTER ... OWNER TO`:
///
/// * `from_owners` — roles whose implicit-owner ACL entries in `from_acl`
///   will be REMOVED by the migration (typically the FROM-side owner when
///   ownership is changing).
/// * `to_owners` — roles whose implicit-owner ACL entries in `to_acl`
///   are AUTOMATIC after the migration (typically the TO-side owner) and
///   therefore need no GRANT to materialise.
///
/// When the owner does not change, callers pass the same single role in
/// both lists and the behaviour collapses to "skip the owner everywhere."
/// When the owner changes, the FROM owner is filtered from `from_acl`
/// only — so a former owner that appears in `to_acl` as an explicit
/// grantee shows up correctly in the diff, and the FROM owner's
/// implicit-owner privileges (which `ALTER OWNER` will strip) are not
/// mistakenly compared against the TO ACL.
pub fn diff_acls(
    from_acl: &[String],
    to_acl: &[String],
    full: bool,
    object_kind: &str,
    from_owners: &[&str],
    to_owners: &[&str],
) -> Vec<AclDiffEntry> {
    use std::collections::BTreeSet;

    let from_map = build_privilege_map(from_acl, object_kind, from_owners);
    let to_map = build_privilege_map(to_acl, object_kind, to_owners);

    let empty_privs: std::collections::HashMap<String, bool> = std::collections::HashMap::new();

    let mut all_grantees: BTreeSet<&str> = BTreeSet::new();
    for g in from_map.keys() {
        all_grantees.insert(g.as_str());
    }
    for g in to_map.keys() {
        all_grantees.insert(g.as_str());
    }

    let mut result = Vec::new();

    for grantee in &all_grantees {
        let from_privs = from_map.get(*grantee).unwrap_or(&empty_privs);
        let to_privs = to_map.get(*grantee).unwrap_or(&empty_privs);

        let mut grants_plain = Vec::new();
        let mut grants_with_option = Vec::new();
        let mut revoke_option_for = Vec::new();
        let mut revokes = Vec::new();

        let mut all_privs: BTreeSet<&str> = BTreeSet::new();
        for p in from_privs.keys() {
            all_privs.insert(p.as_str());
        }
        for p in to_privs.keys() {
            all_privs.insert(p.as_str());
        }

        for priv_name in &all_privs {
            let from_go = from_privs.get(*priv_name);
            let to_go = to_privs.get(*priv_name);

            match (from_go, to_go) {
                // New privilege
                (None, Some(false)) => grants_plain.push(priv_name.to_string()),
                (None, Some(true)) => grants_with_option.push(priv_name.to_string()),
                // Upgrade: plain → with grant option
                (Some(false), Some(true)) => grants_with_option.push(priv_name.to_string()),
                // Downgrade: with grant option → plain (full mode only)
                (Some(true), Some(false)) if full => {
                    revoke_option_for.push(priv_name.to_string());
                }
                // Removed entirely (full mode only)
                (Some(go), None) if full => {
                    if *go {
                        revoke_option_for.push(priv_name.to_string());
                    }
                    revokes.push(priv_name.to_string());
                }
                _ => {}
            }
        }

        if !grants_plain.is_empty()
            || !grants_with_option.is_empty()
            || !revoke_option_for.is_empty()
            || !revokes.is_empty()
        {
            result.push(AclDiffEntry {
                grantee: grantee.to_string(),
                grants_plain,
                grants_with_option,
                revoke_option_for,
                revokes,
            });
        }
    }

    result
}

/// Generate the combined GRANT/REVOKE script for an object.
///
/// See [`diff_acls`] for the meaning of `from_owners` / `to_owners`. When
/// the owner does not change between `from` and `to`, callers pass the
/// same single role in both lists; when it does change, `from_owners`
/// holds the FROM owner (whose implicit-owner ACL entry will be removed
/// by `ALTER ... OWNER`) and `to_owners` holds the TO owner (whose
/// implicit-owner ACL entry will be created by it).
pub fn generate_grants_script(
    from_acl: &[String],
    to_acl: &[String],
    full: bool,
    object_kind: &str,
    object_name: &str,
    from_owners: &[&str],
    to_owners: &[&str],
) -> String {
    let diffs = diff_acls(from_acl, to_acl, full, object_kind, from_owners, to_owners);
    let mut script = String::new();

    for entry in &diffs {
        let grantee = AclEntry::format_grantee(&entry.grantee);

        if !entry.revoke_option_for.is_empty() {
            script.append_block(&format!(
                "REVOKE GRANT OPTION FOR {} ON {} {} FROM {};",
                entry.revoke_option_for.join(", "),
                object_kind,
                object_name,
                grantee
            ));
        }
        if !entry.revokes.is_empty() {
            script.append_block(&format!(
                "REVOKE {} ON {} {} FROM {};",
                entry.revokes.join(", "),
                object_kind,
                object_name,
                grantee
            ));
        }
        if !entry.grants_plain.is_empty() {
            script.append_block(&format!(
                "GRANT {} ON {} {} TO {};",
                entry.grants_plain.join(", "),
                object_kind,
                object_name,
                grantee
            ));
        }
        if !entry.grants_with_option.is_empty() {
            script.append_block(&format!(
                "GRANT {} ON {} {} TO {} WITH GRANT OPTION;",
                entry.grants_with_option.join(", "),
                object_kind,
                object_name,
                grantee
            ));
        }
    }

    script
}

/// Generate GRANT statements for a new object (all ACL entries from "to").
///
/// `owners` lists role names that own the object; their ACL entries are
/// skipped from `to_acl`. There is no `from_acl`, so the FROM owner list
/// is empty.
pub fn generate_new_object_grants(
    to_acl: &[String],
    object_kind: &str,
    object_name: &str,
    owners: &[&str],
) -> String {
    generate_grants_script(&[], to_acl, false, object_kind, object_name, &[], owners)
}

/// Generate column-level GRANT/REVOKE statements.
///
/// Column privileges use the format:
///   `GRANT SELECT (col) ON TABLE schema.table TO grantee;`
///
/// `from_owners` / `to_owners` follow the same convention as
/// [`generate_grants_script`]. When the parent table's owner does not
/// change, callers pass the same single role in both lists. When it
/// does, `from_owners` is the FROM owner (whose implicit-column-grant
/// entries, if any, vanish under `ALTER TABLE ... OWNER TO`) and
/// `to_owners` is the TO owner.
pub fn generate_column_grants_script(
    from_acl: &[String],
    to_acl: &[String],
    full: bool,
    table_name: &str,
    column_name: &str,
    from_owners: &[&str],
    to_owners: &[&str],
) -> String {
    let diffs = diff_acls(from_acl, to_acl, full, "COLUMN", from_owners, to_owners);
    let mut script = String::new();

    for entry in &diffs {
        let grantee = AclEntry::format_grantee(&entry.grantee);
        if !entry.revoke_option_for.is_empty() {
            script.append_block(&format!(
                "REVOKE GRANT OPTION FOR {} ({}) ON TABLE {} FROM {};",
                entry.revoke_option_for.join(", "),
                column_name,
                table_name,
                grantee
            ));
        }
        if !entry.revokes.is_empty() {
            script.append_block(&format!(
                "REVOKE {} ({}) ON TABLE {} FROM {};",
                entry.revokes.join(", "),
                column_name,
                table_name,
                grantee
            ));
        }
        if !entry.grants_plain.is_empty() {
            script.append_block(&format!(
                "GRANT {} ({}) ON TABLE {} TO {};",
                entry.grants_plain.join(", "),
                column_name,
                table_name,
                grantee
            ));
        }
        if !entry.grants_with_option.is_empty() {
            script.append_block(&format!(
                "GRANT {} ({}) ON TABLE {} TO {} WITH GRANT OPTION;",
                entry.grants_with_option.join(", "),
                column_name,
                table_name,
                grantee
            ));
        }
    }

    script
}

#[cfg(test)]
#[path = "acl_tests.rs"]
mod tests;
