//! Access control lists — parsing PostgreSQL `aclitem` strings and turning the
//! difference between two of them into `GRANT` / `REVOKE` statements.
//!
//! An `aclitem` renders as `grantee=privileges/grantor`, where each privilege is
//! a single character (`r` SELECT, `a` INSERT, `w` UPDATE, `d` DELETE,
//! `D` TRUNCATE, `x` REFERENCES, `t` TRIGGER, `X` EXECUTE, `U` USAGE, `C` CREATE,
//! `c` CONNECT, `T` TEMPORARY) and a trailing `*` means WITH GRANT OPTION. An
//! empty grantee means `PUBLIC`.
//!
//! What actually gets emitted depends on
//! [`GrantsMode`](crate::config::grants_mode::GrantsMode).
//!
//! # Grantors
//!
//! The comparison is grantor-insensitive: a grantee's privileges are merged
//! over every entry that grants them, and a missing privilege is added with a
//! plain `GRANT`, which records the owner as grantor. The one place the grantor
//! matters is the `REVOKE` side of full mode. PostgreSQL removes only the
//! entries recorded under the role a `REVOKE` acts as — the owner, when a
//! superuser or the owner runs it — so an entry a delegate granted (a role
//! holding `WITH GRANT OPTION`; PostgreSQL 19 also records the role named by
//! `GRANT … GRANTED BY`) survives a plain `REVOKE` without an error, and
//! revoking a grant option such entries were granted under fails with
//! "dependent privileges exist". [`generate_grants_scripts`] therefore revokes
//! those entries by naming their grantor, leaf first, before any owner-level
//! statement runs; see [`RevokeStyle`] for the two spellings.
//!
//! # Table revokes reach the columns
//!
//! PostgreSQL applies a table-level `REVOKE` to the ACL of every column as
//! well, removing the column entries the same grantor gave the same grantee.
//! A table's [`GrantsScripts::revoked`] lists what its statements take away,
//! and each column of that table is planned with it
//! ([`GrantorRevokes::parent_revokes`]): a column privilege the target still
//! wants is granted back, and a column entry granted under a column grant
//! option the table statement removes is revoked before it.

use crate::utils::string_extensions::{StringExt, unquote_ident};
use std::borrow::Cow;
use std::cmp::Reverse;
use std::collections::{BTreeMap, HashMap, HashSet};

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
    /// handled correctly via the private `find_unquoted` scanner.
    /// # Examples
    ///
    /// ```
    /// use pgc::dump::acl::AclEntry;
    ///
    /// let entry = AclEntry::parse("alice=arwd/postgres").unwrap();
    /// assert_eq!(entry.grantee, "alice");
    /// assert_eq!(entry.privileges, "arwd");
    /// assert_eq!(entry.grantor, "postgres");
    ///
    /// // An empty grantee is PUBLIC.
    /// assert_eq!(AclEntry::parse("=r/postgres").unwrap().grantee, "");
    ///
    /// // Separators inside a quoted role name do not confuse the parser.
    /// let quoted = AclEntry::parse("\"weird=name\"=r/postgres").unwrap();
    /// assert_eq!(quoted.grantee, "\"weird=name\"");
    /// assert_eq!(quoted.privileges, "r");
    ///
    /// // An item granting nothing is not an entry.
    /// assert!(AclEntry::parse("alice=/postgres").is_none());
    /// ```
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
            "TYPE" | "FOREIGN SERVER" => &["USAGE"],
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

    /// The grantee as a `GRANT` / `REVOKE` names it: `PUBLIC` for the empty
    /// name, otherwise the role quoted as SQL needs it. aclitem text leaves
    /// `Reader` or `user` bare, and written as-is they would name `reader` or
    /// fail to parse (see `quote_acl_role`).
    fn format_grantee(grantee: &str) -> String {
        quote_acl_role(grantee)
    }

    /// Generate GRANT statement(s) for this ACL entry on the given object.
    /// Privileges with and without GRANT OPTION are emitted as separate statements.
    /// # Examples
    ///
    /// ```
    /// use pgc::dump::acl::AclEntry;
    ///
    /// let script = AclEntry::get_grant_script("alice=rw/postgres", "TABLE", "app.orders");
    /// assert!(script.contains("GRANT SELECT, UPDATE ON TABLE app.orders TO alice;"));
    ///
    /// // Privileges valid for one object kind are dropped for another: a
    /// // sequence has no TRIGGER privilege, so `t` is ignored.
    /// let seq = AclEntry::get_grant_script("alice=rt/postgres", "SEQUENCE", "app.s");
    /// assert!(seq.contains("SELECT"));
    /// assert!(!seq.contains("TRIGGER"));
    /// ```
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
    privilege_map(acl, object_kind, owners, true)
}

/// [`build_privilege_map`], optionally without the unparseable-item warning
/// (for a second pass over an ACL that has already been warned about).
fn privilege_map(
    acl: &[String],
    object_kind: &str,
    owners: &[&str],
    warn: bool,
) -> std::collections::HashMap<String, std::collections::HashMap<String, bool>> {
    use std::collections::HashMap;
    let mut map: HashMap<String, HashMap<String, bool>> = HashMap::new();
    for item in acl {
        let Some(entry) = AclEntry::parse(item) else {
            // Surfacing unparseable ACL items prevents silent data loss when
            // a future PostgreSQL version introduces an aclitem syntax we do
            // not yet recognise — without this warning the offending grantee
            // would simply disappear from the diff.
            if warn {
                eprintln!("Warning: skipping unparseable ACL item ({object_kind}): {item:?}");
            }
            continue;
        };
        if is_owner(owners, &entry.grantee) {
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
/// [`generate_grants_scripts`] adds the grantor-qualified revokes a plain `REVOKE`
/// cannot perform.
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
    let target = AclTarget::Object {
        kind: object_kind,
        name: object_name,
    };
    render_diffs(target, &diffs)
}

/// Generate the owner-level GRANT/REVOKE statements that turn an object into
/// `to_acl` from whichever of `from_states` it is in when they run.
///
/// For an object whose ACL at that point the migration cannot know exactly:
/// one it creates, which starts from the default privileges of whichever
/// role runs the migration ([`CreatedAcl::states`]), and a view a routine's
/// `DROP … CASCADE` may or may not have taken along (matched by name, so an
/// overload can stand in for the routine it calls): either created, or still
/// holding its FROM ACL. Each state is an ACL with the owners whose entries
/// it skips, as `from_acl` / `from_owners` in [`diff_acls`].
///
/// Every privilege `to_acl` has that some state lacks is granted, and in full
/// mode every privilege some state has that `to_acl` lacks is revoked (and
/// likewise for grant options). PostgreSQL treats granting a privilege that
/// is already held, and revoking one that is not, as no-ops, so the result
/// reaches `to_acl` from each of the states. With a single state it is
/// exactly [`generate_grants_script`]'s output.
///
/// The revokes are owner-level only: an entry recorded under another
/// grantor (see [`generate_grants_scripts`]) is left in place, since naming
/// a grantor on an object that was re-created would fail.
///
/// # Examples
///
/// ```
/// use pgc::dump::acl::{AclTarget, generate_grants_script_from_any};
///
/// // Either the view survived with its FROM ACL, or it was re-created with
/// // the schema's default privileges (none here).
/// let from = vec!["owner=arwdDxt/owner".to_string(), "old=r/owner".to_string()];
/// let fresh: Vec<String> = Vec::new();
/// let to = vec!["owner=arwdDxt/owner".to_string(), "reader=r/owner".to_string()];
/// let script = generate_grants_script_from_any(
///     &[(fresh.as_slice(), &[]), (from.as_slice(), &["owner"])],
///     &to,
///     true,
///     AclTarget::Object { kind: "TABLE", name: "app.v" },
///     &["owner"],
/// );
/// assert_eq!(
///     script,
///     "REVOKE SELECT ON TABLE app.v FROM old;\n\nGRANT SELECT ON TABLE app.v TO reader;\n\n"
/// );
/// ```
pub fn generate_grants_script_from_any(
    from_states: &[(&[String], &[&str])],
    to_acl: &[String],
    full: bool,
    target: AclTarget<'_>,
    to_owners: &[&str],
) -> String {
    use std::collections::BTreeSet;

    let kind = target.privilege_kind();
    // Per grantee: grants, grants with option, revokes of the option, revokes.
    let mut merged: BTreeMap<String, [BTreeSet<String>; 4]> = BTreeMap::new();
    for (from_acl, from_owners) in from_states {
        for entry in diff_acls(from_acl, to_acl, full, kind, from_owners, to_owners) {
            let slot = merged.entry(entry.grantee).or_default();
            slot[0].extend(entry.grants_plain);
            slot[1].extend(entry.grants_with_option);
            slot[2].extend(entry.revoke_option_for);
            slot[3].extend(entry.revokes);
        }
    }
    let diffs: Vec<AclDiffEntry> = merged
        .into_iter()
        .map(
            |(grantee, [plain, with_option, option_for, revokes])| AclDiffEntry {
                grantee,
                grants_plain: plain.into_iter().collect(),
                grants_with_option: with_option.into_iter().collect(),
                revoke_option_for: option_for.into_iter().collect(),
                revokes: revokes.into_iter().collect(),
            },
        )
        .collect();
    render_diffs(target, &diffs)
}

/// Render per-grantee diffs as the owner-level `REVOKE` / `GRANT` statements
/// on `target`, in the order pgc has always emitted them: per grantee,
/// `REVOKE GRANT OPTION FOR`, `REVOKE`, `GRANT`, `GRANT … WITH GRANT OPTION`.
fn render_diffs(target: AclTarget<'_>, diffs: &[AclDiffEntry]) -> String {
    let mut script = String::new();
    for entry in diffs {
        let grantee = AclEntry::format_grantee(&entry.grantee);
        if !entry.revoke_option_for.is_empty() {
            script.append_block(&format!(
                "REVOKE GRANT OPTION FOR {} FROM {grantee};",
                target.privileges_on(&entry.revoke_option_for)
            ));
        }
        if !entry.revokes.is_empty() {
            script.append_block(&format!(
                "REVOKE {} FROM {grantee};",
                target.privileges_on(&entry.revokes)
            ));
        }
        if !entry.grants_plain.is_empty() {
            script.append_block(&format!(
                "GRANT {} TO {grantee};",
                target.privileges_on(&entry.grants_plain)
            ));
        }
        if !entry.grants_with_option.is_empty() {
            script.append_block(&format!(
                "GRANT {} TO {grantee} WITH GRANT OPTION;",
                target.privileges_on(&entry.grants_with_option)
            ));
        }
    }
    script
}

/// The privilege `acldefault()` gives PUBLIC on an object of `GRANT` kind
/// `object_kind`, as an aclitem character: `EXECUTE` (`X`) on functions and
/// procedures, `USAGE` (`U`) on types, domains included. `None` for every
/// other kind pgc compares — schemas, tables, views, sequences, foreign
/// tables, columns — whose default grants PUBLIC nothing.
pub fn public_default_privilege(object_kind: &str) -> Option<char> {
    match object_kind {
        "FUNCTION" | "PROCEDURE" => Some('X'),
        "TYPE" => Some('U'),
        _ => None,
    }
}

/// PUBLIC's entry of `acldefault()` for an object of `object_kind` owned by
/// `owner` (e.g. `=X/owner`), or `None` when the default grants PUBLIC
/// nothing ([`public_default_privilege`]).
pub fn public_default_entry(object_kind: &str, owner: &str) -> Option<String> {
    public_default_privilege(object_kind).map(|privilege| format!("={privilege}/{owner}"))
}

/// The ACL a dumped `acl` stands for, in the terms [`generate_grants_scripts`]
/// compares.
///
/// The dump reads a NULL catalog ACL as an empty list, but PostgreSQL reads
/// NULL as `acldefault(kind, owner)`: the owner's entry, which the comparison
/// skips, and for routines and types PUBLIC's `EXECUTE` / `USAGE`. Compared
/// as empty, a NULL ACL on one side and `{owner=X/owner}` on the other looked
/// equal, and `REVOKE EXECUTE ... FROM PUBLIC` was never migrated. So an empty
/// `acl` of such a kind becomes PUBLIC's default entry, unless
/// `explicitly_empty` says the catalog ACL is an empty array (everything
/// revoked, the owner's own privileges included). Any other ACL is returned
/// as it is.
/// # Examples
///
/// ```
/// use pgc::dump::acl::effective_acl;
///
/// assert_eq!(effective_acl(&[], false, "FUNCTION", "app").as_ref(), ["=X/app"]);
/// assert_eq!(effective_acl(&[], false, "TYPE", "app").as_ref(), ["=U/app"]);
/// // Tables grant PUBLIC nothing by default, and `{}` revokes everything.
/// assert!(effective_acl(&[], false, "TABLE", "app").is_empty());
/// assert!(effective_acl(&[], true, "FUNCTION", "app").is_empty());
/// // An explicit ACL is left alone.
/// let acl = vec!["app=X/app".to_string()];
/// assert_eq!(effective_acl(&acl, false, "FUNCTION", "app").as_ref(), acl.as_slice());
/// ```
pub fn effective_acl<'a>(
    acl: &'a [String],
    explicitly_empty: bool,
    object_kind: &str,
    owner: &str,
) -> Cow<'a, [String]> {
    if !acl.is_empty() || explicitly_empty {
        return Cow::Borrowed(acl);
    }
    match public_default_entry(object_kind, owner) {
        Some(entry) => Cow::Owned(vec![entry]),
        None => Cow::Borrowed(acl),
    }
}

/// What an object holds right after the migration creates it — a new object, or
/// one it drops and recreates — and sets its owner, when the role that runs the
/// `CREATE` is unknown. Every object the migration creates has its grants
/// planned from one, whatever the kind and whatever made the migration create
/// it; `Comparer` computes it from the FROM default privileges.
///
/// PostgreSQL starts a new object from the creating role's default privileges:
/// its database-wide `pg_default_acl` row for the kind replaces `acldefault()`,
/// and its row for the object's schema adds to it. A migration usually runs as
/// a superuser, while the rows usually belong to an application role
/// (`ALTER DEFAULT PRIVILEGES FOR ROLE app ...`), so which rows apply cannot
/// be told from the dumps. Hence two ACLs, compared by
/// [`generate_created_object_grants`]: what the object holds whoever creates
/// it, and everything it may hold.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CreatedAcl {
    /// Entries the object holds whichever role creates it; every `GRANT` the
    /// target needs beyond these is issued. For a routine or type at most
    /// PUBLIC's default entry ([`public_default_entry`]), and only while every
    /// role's default privileges keep it: a role without any gets exactly
    /// `acldefault()`. For a relation in full mode, also what the FROM rows
    /// for its schema grant — pgc's long-standing assumption that those rows
    /// apply to the migration.
    pub certain: Vec<String>,
    /// Entries the object may hold, depending on the creating role: `certain`
    /// plus PUBLIC's default entry plus what any role's default privileges
    /// for the kind grant, database-wide or in the object's schema. In full
    /// mode, each one the target lacks is revoked — a no-op when the object
    /// does not hold it. Always a superset of `certain`.
    pub possible: Vec<String>,
}

impl CreatedAcl {
    /// The object's two starting states for
    /// [`generate_grants_script_from_any`], each skipping the entries of
    /// `owners`: `certain` and `possible`. Since `possible` holds all of
    /// `certain`, the grants that reach a target from both are those it needs
    /// beyond `certain`, and the revokes those of `possible` it lacks. A
    /// caller that cannot tell whether the object was created at all adds the
    /// state it has otherwise.
    pub fn states<'a>(&'a self, owners: &'a [&'a str]) -> [(&'a [String], &'a [&'a str]); 2] {
        [
            (self.certain.as_slice(), owners),
            (self.possible.as_slice(), owners),
        ]
    }
}

/// The owner-level `GRANT` / `REVOKE` statements that give an object the
/// migration creates the `to_acl` privileges, whichever role runs its `CREATE`
/// (see [`CreatedAcl`]): revokes are computed against
/// [`CreatedAcl::possible`] and grants against [`CreatedAcl::certain`], so a
/// privilege the target lacks is revoked even if it may be absent, and one the
/// target holds is granted even if it may be present — both no-ops then. It
/// is [`generate_grants_script_from_any`] over [`CreatedAcl::states`].
///
/// `owners` is the object's TO owner, whose entries neither side counts. There
/// are no grantor-named revokes: the object is new, so every entry was
/// recorded by its creation or by the owner. With `certain == possible` the
/// result is what [`generate_grants_scripts`] makes of the pair without a
/// [`GrantorRevokes`].
/// # Examples
///
/// ```
/// use pgc::dump::acl::{AclTarget, CreatedAcl, generate_created_object_grants};
///
/// // Some role's default privileges revoke PUBLIC's EXECUTE: PUBLIC may or may
/// // not hold it once the function exists.
/// let created = CreatedAcl {
///     certain: vec![],
///     possible: vec!["=X/app".to_string()],
/// };
/// let target = AclTarget::Object { kind: "FUNCTION", name: "app.f()" };
/// // A target without PUBLIC revokes it; one with PUBLIC grants it.
/// let hardened = vec!["app=X/app".to_string()];
/// assert_eq!(
///     generate_created_object_grants(&created, &hardened, true, target, &["app"]).owner_level,
///     "REVOKE EXECUTE ON FUNCTION app.f() FROM PUBLIC;\n\n"
/// );
/// let open = vec!["=X/app".to_string()];
/// assert_eq!(
///     generate_created_object_grants(&created, &open, true, target, &["app"]).owner_level,
///     "GRANT EXECUTE ON FUNCTION app.f() TO PUBLIC;\n\n"
/// );
/// ```
pub fn generate_created_object_grants(
    created: &CreatedAcl,
    to_acl: &[String],
    full: bool,
    target: AclTarget<'_>,
    owners: &[&str],
) -> GrantsScripts {
    GrantsScripts {
        owner_level: generate_grants_script_from_any(
            &created.states(owners),
            to_acl,
            full,
            target,
            owners,
        ),
        ..GrantsScripts::default()
    }
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
    let target = AclTarget::Column {
        table: table_name,
        column: column_name,
    };
    render_diffs(target, &diffs)
}

/// First `server_version_num` whose `REVOKE … GRANTED BY` may name a role other
/// than the current user. PostgreSQL 14–18 reject that with "grantor must be
/// current user"; 19 revokes exactly the entries recorded under the named role.
pub const GRANTED_BY_MIN_VERSION: i32 = 190000;

/// How a full-mode revoke removes an ACL entry recorded under a grantor that is
/// neither the object's FROM owner nor its TO owner (see the module docs for
/// why a plain `REVOKE` cannot).
///
/// Neither form is safe to replay on its own. Once the entry is gone, a
/// grantor that holds no privilege at all on the object any more (the common
/// case: the target drops a delegate together with what it granted) makes the
/// same statement fail with "permission denied", where a plain `REVOKE` run by
/// the owner never does. Production output therefore wraps each one in a
/// guard ([`GrantorRevokes::replay_guard`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RevokeStyle {
    /// `REVOKE … FROM grantee GRANTED BY grantor;`, for a migration applied to
    /// PostgreSQL 19 or later ([`GRANTED_BY_MIN_VERSION`]). The migrating role
    /// must be a superuser or inherit the grantor's privileges. The object is
    /// looked up as the migrating role, and no session state is changed.
    GrantedBy,
    /// `SET ROLE grantor;`, the plain `REVOKE`, then `RESET ROLE;` — accepted by
    /// every supported major, so it is used whenever the migrated server is
    /// older than 19 or of unknown version. The migrating role must be allowed
    /// to `SET ROLE` to the grantor, and the grantor needs `USAGE` on the
    /// object's schema to look it up.
    ///
    /// It cannot reach an entry whose grantor has since become a superuser:
    /// PostgreSQL 14–18 run any `GRANT` / `REVOKE` issued by a superuser as the
    /// object's owner, so the statement silently removes nothing and the next
    /// compare emits it again. Only PostgreSQL 19's `GRANTED BY` names such a
    /// grantor, and the dump does not record role attributes to warn about it.
    SetRole,
}

/// What an ACL belongs to, and so how its privileges are spelled in a
/// `GRANT` / `REVOKE`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AclTarget<'a> {
    /// A whole object: `… ON {kind} {name}`. `kind` is the `GRANT` object
    /// keyword (`TABLE`, `SEQUENCE`, `FUNCTION`, `PROCEDURE`, `SCHEMA`, `TYPE`),
    /// which also selects the privileges that are valid for it.
    Object {
        /// `GRANT` object keyword.
        kind: &'a str,
        /// Qualified object name, as the statement spells it (the dump's
        /// `quote_ident` forms; a routine's is `schema.name(identity arguments)`).
        name: &'a str,
    },
    /// One column of a table: `… ({column}) ON TABLE {table}`.
    Column {
        /// Qualified table name.
        table: &'a str,
        /// Column name, in `quote_ident` form.
        column: &'a str,
    },
}

impl AclTarget<'_> {
    /// The object kind `AclEntry::parse_privileges` validates privileges against.
    fn privilege_kind(&self) -> &str {
        match self {
            AclTarget::Object { kind, .. } => kind,
            AclTarget::Column { .. } => "COLUMN",
        }
    }

    /// `SELECT, UPDATE ON TABLE s.t` / `SELECT, UPDATE (c) ON TABLE s.t`.
    fn privileges_on<S: AsRef<str>>(&self, privileges: &[S]) -> String {
        let privileges: Vec<&str> = privileges.iter().map(AsRef::as_ref).collect();
        match self {
            AclTarget::Object { kind, name } => {
                format!("{} ON {kind} {name}", privileges.join(", "))
            }
            AclTarget::Column { table, column } => {
                format!("{} ({column}) ON TABLE {table}", privileges.join(", "))
            }
        }
    }

    /// A scalar subquery yielding the target's current `aclitem[]` (NULL when
    /// it holds the default privileges or no longer exists), resolving the
    /// name the way the statement itself does. Used by the replay guard.
    fn current_acl_sql(&self) -> String {
        match self {
            AclTarget::Column { table, column } => format!(
                "(SELECT c.attacl FROM pg_catalog.pg_attribute c \
                 WHERE c.attrelid = pg_catalog.to_regclass({}) \
                 AND pg_catalog.quote_ident(c.attname) = {})",
                sql_literal(table),
                sql_literal(column)
            ),
            AclTarget::Object { kind, name } => match *kind {
                "SCHEMA" => format!(
                    "(SELECT n.nspacl FROM pg_catalog.pg_namespace n \
                     WHERE n.oid = pg_catalog.to_regnamespace({}))",
                    sql_literal(name)
                ),
                "TYPE" => format!(
                    "(SELECT t.typacl FROM pg_catalog.pg_type t \
                     WHERE t.oid = pg_catalog.to_regtype({}))",
                    sql_literal(name)
                ),
                // `to_regprocedure` cannot parse the argument names that
                // `pg_get_function_identity_arguments` includes, so match the
                // routine the way the dump spelled it.
                "FUNCTION" | "PROCEDURE" => {
                    let (namespace, routine) = match find_unquoted(name, b'.') {
                        Some(dot) => (
                            format!(
                                "p.pronamespace = pg_catalog.to_regnamespace({}) AND ",
                                sql_literal(&name[..dot])
                            ),
                            &name[dot + 1..],
                        ),
                        None => (String::new(), *name),
                    };
                    format!(
                        "(SELECT p.proacl FROM pg_catalog.pg_proc p WHERE {namespace}\
                         pg_catalog.format('%I(%s)', p.proname, \
                         pg_catalog.pg_get_function_identity_arguments(p.oid)) = {})",
                        sql_literal(routine)
                    )
                }
                _ => format!(
                    "(SELECT c.relacl FROM pg_catalog.pg_class c \
                     WHERE c.oid = pg_catalog.to_regclass({}))",
                    sql_literal(name)
                ),
            },
        }
    }
}

/// What a full-mode revoke needs to know to remove the ACL entries of one
/// object that a role other than its owner granted.
#[derive(Debug, Clone, Copy)]
pub struct GrantorRevokes<'a> {
    /// The object's FROM owner and TO owner, as the dump spells them
    /// (`quote_ident` form; an empty string is ignored). A plain `REVOKE`, run
    /// by a superuser or the owner, removes the entries these roles granted —
    /// `ALTER … OWNER TO` rewrites the old owner to the new one in every
    /// aclitem, column ACLs included — so those entries keep the owner-level
    /// statements pgc has always emitted.
    pub owners: &'a [&'a str],
    /// How to name any other grantor.
    pub style: RevokeStyle,
    /// For a [`AclTarget::Column`], the parent table's FROM ACL. A role holding a
    /// table-level grant option can grant the column privilege, and so is the
    /// grantor a revoke has to act as. Empty for any other target.
    pub parent_acl: &'a [String],
    /// For a [`AclTarget::Column`], what the parent table's statements revoke
    /// (its [`GrantsScripts::revoked`]); PostgreSQL applies each of them to the
    /// column too. Empty for any other target.
    pub parent_revokes: &'a [RevokedPrivilege],
    /// Wrap each grantor-named revoke in a `DO` block that runs it only while
    /// the entry it removes still exists, so a replayed migration does not
    /// fail (see [`RevokeStyle`]). Set for production output; the `SET ROLE` of
    /// the pre-19 form then moves inside the block, where a failing `REVOKE`
    /// rolls it back.
    pub replay_guard: bool,
}

/// One privilege a `REVOKE` statement takes from one grantee. PostgreSQL applies
/// a table-level `REVOKE` to every column's ACL as well, removing the column
/// entries the same grantor gave the same grantee.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RevokedPrivilege {
    /// Raw role name of the grantee; empty for PUBLIC.
    pub grantee: String,
    /// Raw names of the roles whose entries the statement removes: the named
    /// grantor, or both owners for an owner-level statement.
    pub grantors: Vec<String>,
    /// Privilege keyword, e.g. `SELECT`.
    pub privilege: String,
    /// `REVOKE GRANT OPTION FOR`: only the grant option goes.
    pub option_only: bool,
}

/// The GRANT/REVOKE statements of one object or column, split by when they
/// must run.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GrantsScripts {
    /// Revokes of entries recorded under a grantor other than the owner,
    /// dependents first. They only remove privileges, and a grantor can revoke
    /// only while it still holds the grant option, so they must run before
    /// `owner_level` and before any statement that takes such a grant option
    /// away — including one on the parent table, whose grant options also cover
    /// its columns. `Comparer` therefore emits them for every object at the top
    /// of the grants section, columns first.
    pub grantor_revokes: String,
    /// The owner-level statements: what [`generate_grants_script`] /
    /// [`generate_column_grants_script`] emit, computed against the ACL that
    /// `grantor_revokes` leaves behind, followed for a column by the `GRANT`s
    /// that restore what the parent table's `REVOKE`s strip from it
    /// ([`GrantorRevokes::parent_revokes`]). Byte-identical to their output
    /// when no entry needs a grantor-qualified revoke and no table revoke
    /// strips a column privilege the target keeps.
    pub owner_level: String,
    /// Every privilege the two scripts revoke, grantor-named and owner-level.
    /// Filled only in full mode with [`GrantorRevokes`]; a table's list becomes
    /// its columns' [`GrantorRevokes::parent_revokes`].
    pub revoked: Vec<RevokedPrivilege>,
}

/// Generate the GRANT/REVOKE statements that turn `from_acl` into `to_acl` for
/// one object or column, revoking entries granted by a role other than the
/// owner by naming that role.
///
/// `from_owners` / `to_owners` filter grantees exactly as in [`diff_acls`].
/// `grantors` is `None` for an ACL that is not the object's catalog ACL (the
/// default privileges a new or recreated object is created with) or whose owner
/// is unknown; the result is then the plain grantor-insensitive script. With
/// `grantors`, in full mode, every FROM entry recorded under another grantor is
/// revoked by naming it when the TO side lacks the privilege (or the grant
/// option), and so is every entry granted under a grant option this migration
/// removes — PostgreSQL would otherwise refuse with "dependent privileges
/// exist". A privilege the TO side still wants is then granted again by the
/// owner, keeping the GRANT side grantor-insensitive.
///
/// An entry whose grantor no longer holds the grant option (PostgreSQL leaves a
/// column grant behind when the table-level grant option it was made under is
/// revoked) cannot be revoked by anyone; it is reported on stderr and left to
/// the owner-level statements, as before.
/// # Examples
///
/// ```
/// use pgc::dump::acl::{AclTarget, GrantorRevokes, RevokeStyle, generate_grants_scripts};
///
/// // A delegate holding the grant option granted SELECT to a reader.
/// let from = vec![
///     "delegate=r*/owner".to_string(),
///     "reader=r/delegate".to_string(),
/// ];
/// let to = vec!["delegate=r*/owner".to_string()];
/// let target = AclTarget::Object { kind: "TABLE", name: "app.t" };
/// let grantors = GrantorRevokes {
///     owners: &["owner"],
///     style: RevokeStyle::GrantedBy,
///     parent_acl: &[],
///     parent_revokes: &[],
///     replay_guard: false,
/// };
/// let scripts = generate_grants_scripts(&from, &to, true, target, &["owner"], &["owner"], Some(grantors));
/// assert_eq!(
///     scripts.grantor_revokes,
///     "REVOKE SELECT ON TABLE app.t FROM reader GRANTED BY delegate;\n\n"
/// );
/// assert!(scripts.owner_level.is_empty());
/// assert_eq!(scripts.revoked[0].grantee, "reader");
/// assert_eq!(scripts.revoked[0].grantors, ["delegate"]);
/// ```
pub fn generate_grants_scripts(
    from_acl: &[String],
    to_acl: &[String],
    full: bool,
    target: AclTarget<'_>,
    from_owners: &[&str],
    to_owners: &[&str],
    grantors: Option<GrantorRevokes<'_>>,
) -> GrantsScripts {
    let kind = target.privilege_kind();
    // Addonly never revokes, so it never needs a grantor.
    let grantors = grantors.filter(|_| full);
    let plan = grantors.as_ref().and_then(|grantors| {
        GrantorRevokePlan::build(from_acl, to_acl, target, from_owners, to_owners, grantors)
    });
    let remaining: &[String] = plan
        .as_ref()
        .map_or(from_acl, |plan| plan.remaining_acl.as_slice());
    let owner_diffs = diff_acls(remaining, to_acl, full, kind, from_owners, to_owners);
    let mut owner_level = render_diffs(target, &owner_diffs);
    let Some(grantors) = grantors else {
        return GrantsScripts {
            owner_level,
            ..GrantsScripts::default()
        };
    };

    // The parent table's REVOKEs strip column entries too: grant back what
    // the TO side keeps (the owner-level diff above assumed they survive).
    // Only GRANTs are added, so the statements above stay as they were.
    if !grantors.parent_revokes.is_empty() {
        let stripped = strip_revoked(remaining, grantors.parent_revokes, kind);
        if stripped.as_slice() != remaining {
            let after = diff_acls(&stripped, to_acl, full, kind, from_owners, to_owners);
            owner_level.push_str(&render_diffs(target, &regrants(&owner_diffs, &after)));
        }
    }

    let mut revoked = Vec::new();
    let grantor_revokes = match &plan {
        Some(plan) => {
            for revoke in &plan.revokes {
                for privilege in &revoke.privileges {
                    revoked.push(RevokedPrivilege {
                        grantee: unquote_ident(&revoke.grantee),
                        grantors: vec![unquote_ident(&revoke.grantor)],
                        privilege: privilege.clone(),
                        option_only: revoke.option_only,
                    });
                }
            }
            plan.render(target, grantors.style, grantors.replay_guard)
        }
        None => String::new(),
    };
    let mut owner_roles: Vec<String> = Vec::new();
    for owner in grantors.owners.iter().filter(|o| !o.is_empty()) {
        let owner = unquote_ident(owner);
        if !owner_roles.contains(&owner) {
            owner_roles.push(owner);
        }
    }
    for diff in &owner_diffs {
        let grantee = unquote_ident(&diff.grantee);
        let downgrades = diff.revoke_option_for.iter().map(|p| (p, true));
        let removals = diff.revokes.iter().map(|p| (p, false));
        for (privilege, option_only) in downgrades.chain(removals) {
            revoked.push(RevokedPrivilege {
                grantee: grantee.clone(),
                grantors: owner_roles.clone(),
                privilege: privilege.clone(),
                option_only,
            });
        }
    }

    GrantsScripts {
        grantor_revokes,
        owner_level,
        revoked,
    }
}

/// `acl` as it is left once PostgreSQL has applied `revoked` to it: each entry
/// the same grantor gave the same grantee loses the revoked privilege, or only
/// its grant option. Unparseable items are kept as they are.
fn strip_revoked(acl: &[String], revoked: &[RevokedPrivilege], object_kind: &str) -> Vec<String> {
    acl.iter()
        .filter_map(|item| {
            let Some(entry) = AclEntry::parse(item) else {
                return Some(item.clone());
            };
            let grantee = unquote_ident(&entry.grantee);
            let grantor = unquote_ident(&entry.grantor);
            rebuild_aclitem(&entry, object_kind, |privilege| {
                revoked
                    .iter()
                    .filter(|r| {
                        r.privilege == privilege
                            && r.grantee == grantee
                            && r.grantors.contains(&grantor)
                    })
                    .map(|r| {
                        if r.option_only {
                            Fate::DropOption
                        } else {
                            Fate::Drop
                        }
                    })
                    .max()
                    .unwrap_or(Fate::Keep)
            })
        })
        .collect()
}

/// The `GRANT`s `after` has that `before` lacks, per grantee.
fn regrants(before: &[AclDiffEntry], after: &[AclDiffEntry]) -> Vec<AclDiffEntry> {
    after
        .iter()
        .filter_map(|entry| {
            let earlier = before.iter().find(|e| e.grantee == entry.grantee);
            let missing = |wanted: &[String], granted: Option<&Vec<String>>| -> Vec<String> {
                wanted
                    .iter()
                    .filter(|p| !granted.is_some_and(|g| g.contains(p)))
                    .cloned()
                    .collect()
            };
            let grants_plain = missing(&entry.grants_plain, earlier.map(|e| &e.grants_plain));
            let grants_with_option = missing(
                &entry.grants_with_option,
                earlier.map(|e| &e.grants_with_option),
            );
            (!grants_plain.is_empty() || !grants_with_option.is_empty()).then(|| AclDiffEntry {
                grantee: entry.grantee.clone(),
                grants_plain,
                grants_with_option,
                revoke_option_for: Vec::new(),
                revokes: Vec::new(),
            })
        })
        .collect()
}

/// Rebuild `entry` with `fate_of` applied to each of its privileges that is
/// valid for `object_kind` (the rest are kept), or `None` when nothing remains.
fn rebuild_aclitem(
    entry: &AclEntry,
    object_kind: &str,
    fate_of: impl Fn(&str) -> Fate,
) -> Option<String> {
    let valid = AclEntry::valid_privileges(object_kind);
    let chars: Vec<char> = entry.privileges.chars().collect();
    let mut kept = String::new();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        let grant_option = chars.get(i + 1) == Some(&'*');
        let fate = AclEntry::priv_char_to_name(c)
            .filter(|name| valid.contains(name))
            .map_or(Fate::Keep, &fate_of);
        match fate {
            Fate::Keep => {
                kept.push(c);
                if grant_option {
                    kept.push('*');
                }
            }
            Fate::DropOption => kept.push(c),
            Fate::Drop => {}
        }
        i += if grant_option { 2 } else { 1 };
    }
    (!kept.is_empty()).then(|| format!("{}={}/{}", entry.grantee, kept, entry.grantor))
}

/// What the migration does to one privilege of a FROM-side ACL entry, ordered
/// by how much of it goes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Fate {
    Keep,
    /// `REVOKE GRANT OPTION FOR`: the privilege stays, its grant option goes.
    DropOption,
    /// `REVOKE`: the privilege and its grant option go.
    Drop,
}

/// A parsed FROM-side ACL entry and the fate of each of its privileges.
struct HeldEntry {
    /// Grantee and grantor as the aclitem spells them (`""` is PUBLIC).
    grantee: String,
    grantor: String,
    /// The aclitem's privilege characters, for rebuilding what survives.
    privilege_chars: String,
    /// Raw role names, comparable with the dump's `quote_ident`-form owners.
    grantee_role: String,
    grantor_role: String,
    /// The grantor is the FROM or TO owner, so a plain `REVOKE` reaches it.
    by_owner: bool,
    privileges: Vec<PrivilegeItem>,
    /// Parallel to `privileges`: what this ACL's own statements do.
    fates: Vec<Fate>,
    /// Parallel to `privileges`: what the parent table's `REVOKE`s do to a
    /// column entry ([`GrantorRevokes::parent_revokes`]); `Keep` elsewhere.
    parent: Vec<Fate>,
}

impl HeldEntry {
    fn fate_of(&self, privilege: &str) -> Fate {
        self.privileges
            .iter()
            .position(|p| p.name == privilege)
            .map_or(Fate::Keep, |i| self.fates[i])
    }

    /// The aclitem that remains once the non-`Keep` fates are applied, or
    /// `None` when nothing does.
    fn remaining_item(&self, object_kind: &str) -> Option<String> {
        let entry = AclEntry {
            grantee: self.grantee.clone(),
            privileges: self.privilege_chars.clone(),
            grantor: self.grantor.clone(),
        };
        rebuild_aclitem(&entry, object_kind, |name| self.fate_of(name))
    }

    /// Whether the entry still carries the grant option on privilege `i` once
    /// both this ACL's statements and the parent table's have run.
    fn keeps_option(&self, i: usize) -> bool {
        self.privileges[i].grant_option && self.fates[i].max(self.parent[i]) == Fate::Keep
    }
}

/// One grantor-qualified `REVOKE [GRANT OPTION FOR]` statement.
#[derive(Debug)]
struct QualifiedRevoke {
    grantee: String,
    grantor: String,
    option_only: bool,
    privileges: Vec<String>,
}

/// The grantor-qualified revokes of one ACL, in execution order, and the ACL
/// the owner-level diff then starts from.
struct GrantorRevokePlan {
    revokes: Vec<QualifiedRevoke>,
    remaining_acl: Vec<String>,
}

impl GrantorRevokePlan {
    /// `None` when no entry needs a grantor-qualified revoke — the common case,
    /// which then keeps the owner-level script byte-identical.
    fn build(
        from_acl: &[String],
        to_acl: &[String],
        target: AclTarget<'_>,
        from_owners: &[&str],
        to_owners: &[&str],
        grantors: &GrantorRevokes<'_>,
    ) -> Option<Self> {
        let kind = target.privilege_kind();
        let owners: HashSet<String> = grantors
            .owners
            .iter()
            .filter(|o| !o.is_empty())
            .map(|o| unquote_ident(o))
            .collect();
        if owners.is_empty() {
            return None;
        }

        // Parse the FROM entries the owner-level diff also looks at; `slots`
        // maps each FROM item to its parsed entry, if any.
        let mut held: Vec<HeldEntry> = Vec::new();
        let mut slots: Vec<Option<usize>> = Vec::with_capacity(from_acl.len());
        for item in from_acl {
            let parsed = AclEntry::parse(item).filter(|e| !is_owner(from_owners, &e.grantee));
            let Some(entry) = parsed else {
                slots.push(None);
                continue;
            };
            let privileges = AclEntry::parse_privileges(&entry.privileges, kind);
            let grantor_role = unquote_ident(&entry.grantor);
            slots.push(Some(held.len()));
            held.push(HeldEntry {
                by_owner: owners.contains(&grantor_role),
                grantee_role: unquote_ident(&entry.grantee),
                grantor_role,
                fates: vec![Fate::Keep; privileges.len()],
                parent: vec![Fate::Keep; privileges.len()],
                privileges,
                grantee: entry.grantee,
                grantor: entry.grantor,
                privilege_chars: entry.privileges,
            });
        }
        if held.iter().all(|e| e.by_owner) {
            return None;
        }

        // What the TO side wants, merged over grantors, as `diff_acls` sees it.
        let wanted = privilege_map(to_acl, kind, to_owners, false);
        for entry in &mut held {
            let grantee_wants = wanted.get(&entry.grantee);
            for (i, p) in entry.privileges.iter().enumerate() {
                entry.fates[i] = match grantee_wants.and_then(|w| w.get(&p.name)) {
                    None => Fate::Drop,
                    Some(false) if p.grant_option => Fate::DropOption,
                    _ => Fate::Keep,
                };
            }
        }

        // A grantor can revoke only while it holds the grant option itself
        // (directly: PostgreSQL resolves `GRANTED BY` without role
        // membership). One that does not is left with an orphaned entry no
        // REVOKE reaches — say so and leave it to the owner-level no-op.
        let mut option_holders: HashSet<(String, String)> = HashSet::new();
        for entry in &held {
            for p in entry.privileges.iter().filter(|p| p.grant_option) {
                option_holders.insert((entry.grantee_role.clone(), p.name.clone()));
            }
        }
        for item in grantors.parent_acl {
            if let Some(entry) = AclEntry::parse(item) {
                for p in AclEntry::parse_privileges(&entry.privileges, "TABLE") {
                    if p.grant_option {
                        option_holders.insert((unquote_ident(&entry.grantee), p.name));
                    }
                }
            }
        }
        for entry in held.iter_mut().filter(|e| !e.by_owner) {
            for (i, p) in entry.privileges.iter().enumerate() {
                if entry.fates[i] != Fate::Keep
                    && !option_holders.contains(&(entry.grantor_role.clone(), p.name.clone()))
                {
                    eprintln!(
                        "Warning: cannot revoke {} from {}: its grantor {} no longer holds the grant option, so no REVOKE can remove it.",
                        target.privileges_on(&[p.name.as_str()]),
                        AclEntry::format_grantee(&entry.grantee),
                        entry.grantor
                    );
                    entry.fates[i] = Fate::Keep;
                }
            }
        }

        // A column entry also loses what the parent table's REVOKEs take from
        // the same grantee as the same grantor. Those statements run after
        // this plan's and need no statement here, but a grant option they
        // remove has dependents like any other.
        for revoked in grantors.parent_revokes {
            for entry in held.iter_mut().filter(|e| {
                e.grantee_role == revoked.grantee && revoked.grantors.contains(&e.grantor_role)
            }) {
                for (i, p) in entry.privileges.iter().enumerate() {
                    if p.name != revoked.privilege {
                        continue;
                    }
                    let fate = match (revoked.option_only, p.grant_option) {
                        (false, _) => Fate::Drop,
                        (true, true) => Fate::DropOption,
                        (true, false) => Fate::Keep,
                    };
                    entry.parent[i] = entry.parent[i].max(fate);
                }
            }
        }

        // Dependent privileges: when a role (never an owner, who always holds
        // every grant option) loses its last grant option on a privilege in
        // this ACL, PostgreSQL refuses to leave the entries it granted under it
        // (RESTRICT). Revoke those too — the owner-level diff grants back
        // whatever the TO side still wants — until nothing more changes.
        let option_holders_before: HashSet<(String, String)> = held
            .iter()
            .filter(|e| !owners.contains(&e.grantee_role))
            .flat_map(|e| {
                e.privileges
                    .iter()
                    .filter(|p| p.grant_option)
                    .map(|p| (e.grantee_role.clone(), p.name.clone()))
            })
            .collect();
        loop {
            let still_held: HashSet<(String, String)> = held
                .iter()
                .flat_map(|e| {
                    (0..e.privileges.len())
                        .filter(|i| e.keeps_option(*i))
                        .map(|i| (e.grantee_role.clone(), e.privileges[i].name.clone()))
                })
                .collect();
            let mut changed = false;
            for entry in &mut held {
                for (i, p) in entry.privileges.iter().enumerate() {
                    let key = (entry.grantor_role.clone(), p.name.clone());
                    if entry.fates[i] != Fate::Drop
                        && option_holders_before.contains(&key)
                        && !still_held.contains(&key)
                    {
                        entry.fates[i] = Fate::Drop;
                        changed = true;
                    }
                }
            }
            if !changed {
                break;
            }
        }

        // One statement per (depth, grantor, grantee, kind), deepest first, so
        // an entry is revoked before the grant option it was granted under.
        let mut depths: HashMap<(String, String), usize> = HashMap::new();
        let mut statements: BTreeMap<(Reverse<usize>, String, String, bool), Vec<String>> =
            BTreeMap::new();
        for entry in held.iter().filter(|e| !e.by_owner) {
            for (p, fate) in entry.privileges.iter().zip(&entry.fates) {
                if *fate == Fate::Keep {
                    continue;
                }
                let depth = chain_depth(
                    &held,
                    &entry.grantee_role,
                    &p.name,
                    &mut depths,
                    &mut HashSet::new(),
                );
                let privileges = statements
                    .entry((
                        Reverse(depth),
                        entry.grantor.clone(),
                        entry.grantee.clone(),
                        *fate == Fate::DropOption,
                    ))
                    .or_default();
                if !privileges.contains(&p.name) {
                    privileges.push(p.name.clone());
                }
            }
        }
        if statements.is_empty() {
            return None;
        }
        let revokes = statements
            .into_iter()
            .map(|((_, grantor, grantee, option_only), mut privileges)| {
                privileges.sort();
                QualifiedRevoke {
                    grantee,
                    grantor,
                    option_only,
                    privileges,
                }
            })
            .collect();

        // Owner-granted entries stay as they are: the owner-level diff revokes
        // them itself.
        let remaining_acl = from_acl
            .iter()
            .zip(&slots)
            .filter_map(|(item, slot)| match slot {
                Some(i)
                    if !held[*i].by_owner && held[*i].fates.iter().any(|f| *f != Fate::Keep) =>
                {
                    held[*i].remaining_item(kind)
                }
                _ => Some(item.clone()),
            })
            .collect();

        Some(GrantorRevokePlan {
            revokes,
            remaining_acl,
        })
    }

    fn render(&self, target: AclTarget<'_>, style: RevokeStyle, replay_guard: bool) -> String {
        if replay_guard {
            return self.render_guarded(target, style);
        }
        let mut script = String::new();
        let mut current_role: Option<&str> = None;
        for revoke in &self.revokes {
            let statement = revoke.statement(target);
            match style {
                RevokeStyle::GrantedBy => script.append_block(&format!(
                    "{statement} GRANTED BY {};",
                    quote_acl_role(&revoke.grantor)
                )),
                RevokeStyle::SetRole => {
                    if current_role != Some(revoke.grantor.as_str()) {
                        if current_role.is_some() {
                            script.append_block("RESET ROLE;");
                        }
                        script.append_block(&format!(
                            "SET ROLE {};",
                            quote_acl_role(&revoke.grantor)
                        ));
                        current_role = Some(revoke.grantor.as_str());
                    }
                    script.append_block(&format!("{statement};"));
                }
            }
        }
        if current_role.is_some() {
            script.append_block("RESET ROLE;");
        }
        script
    }

    /// Each revoke as its own `DO` block that runs it only while an entry it
    /// removes is still recorded, the `SET ROLE` form switching roles inside
    /// the block.
    fn render_guarded(&self, target: AclTarget<'_>, style: RevokeStyle) -> String {
        let acl = target.current_acl_sql();
        let mut script = String::new();
        for revoke in &self.revokes {
            let statement = revoke.statement(target);
            let action = match style {
                RevokeStyle::GrantedBy => format!(
                    "        {statement} GRANTED BY {};\n",
                    quote_acl_role(&revoke.grantor)
                ),
                RevokeStyle::SetRole => format!(
                    "        SET ROLE {};\n        {statement};\n        RESET ROLE;\n",
                    quote_acl_role(&revoke.grantor)
                ),
            };
            let grantee = if revoke.grantee.is_empty() {
                "a.grantee = 0".to_string()
            } else {
                format!(
                    "pg_catalog.pg_get_userbyid(a.grantee) = {}",
                    sql_literal(&unquote_ident(&revoke.grantee))
                )
            };
            let privileges: Vec<String> =
                revoke.privileges.iter().map(|p| sql_literal(p)).collect();
            let body = format!(
                "\nBEGIN\n    IF EXISTS (SELECT 1 FROM pg_catalog.aclexplode({acl}) AS a\n               \
                 WHERE pg_catalog.pg_get_userbyid(a.grantor) = {}\n                 \
                 AND {grantee}\n                 \
                 AND a.privilege_type IN ({}){}) THEN\n\
                 {action}    END IF;\nEND\n",
                sql_literal(&unquote_ident(&revoke.grantor)),
                privileges.join(", "),
                if revoke.option_only {
                    " AND a.is_grantable"
                } else {
                    ""
                },
            );
            let tag = dollar_quote_tag(&body);
            script.append_block(&format!("DO {tag}{body}{tag};"));
        }
        script
    }
}

impl QualifiedRevoke {
    /// `REVOKE [GRANT OPTION FOR] … FROM grantee`, without the grantor or `;`.
    fn statement(&self, target: AclTarget<'_>) -> String {
        let head = if self.option_only {
            "REVOKE GRANT OPTION FOR"
        } else {
            "REVOKE"
        };
        format!(
            "{head} {} FROM {}",
            target.privileges_on(&self.privileges),
            quote_acl_role(&self.grantee)
        )
    }
}

/// `value` as an SQL string literal (standard-conforming: only `'` doubles).
fn sql_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

/// A dollar-quote delimiter (`$pgc$`, else `$pgc1$`, …) that `body` does not
/// contain, so no identifier or literal inside can end the quote early.
pub(crate) fn dollar_quote_tag(body: &str) -> String {
    (0..)
        .map(|n| match n {
            0 => "$pgc$".to_string(),
            n => format!("$pgc{n}$"),
        })
        .find(|tag| !body.contains(tag.as_str()))
        .expect("an unused dollar-quote tag exists")
}

/// How many grants separate `role`'s `privilege` from an owner: 1 for an entry
/// the owner granted, one more per delegate above it, 0 when `role` holds no
/// entry for it. PostgreSQL rejects circular grant options, so the recursion
/// ends; `visiting` guards against a malformed ACL anyway.
fn chain_depth(
    held: &[HeldEntry],
    role: &str,
    privilege: &str,
    memo: &mut HashMap<(String, String), usize>,
    visiting: &mut HashSet<(String, String)>,
) -> usize {
    let key = (role.to_string(), privilege.to_string());
    if let Some(depth) = memo.get(&key) {
        return *depth;
    }
    if !visiting.insert(key.clone()) {
        return 0;
    }
    let mut depth = 0;
    for entry in held
        .iter()
        .filter(|e| e.grantee_role == role && e.privileges.iter().any(|p| p.name == privilege))
    {
        let above = if entry.by_owner {
            0
        } else {
            chain_depth(held, &entry.grantor_role, privilege, memo, visiting)
        };
        depth = depth.max(above + 1);
    }
    visiting.remove(&key);
    memo.insert(key, depth);
    depth
}

/// PostgreSQL's reserved keywords (`pg_get_keywords()` category `R`, 14–19),
/// which a role name cannot be spelled as without quotes.
pub(crate) const RESERVED_KEYWORDS: &[&str] = &[
    "all",
    "analyse",
    "analyze",
    "and",
    "any",
    "array",
    "as",
    "asc",
    "asymmetric",
    "both",
    "case",
    "cast",
    "check",
    "collate",
    "column",
    "constraint",
    "create",
    "current_catalog",
    "current_date",
    "current_role",
    "current_time",
    "current_timestamp",
    "current_user",
    "default",
    "deferrable",
    "desc",
    "distinct",
    "do",
    "else",
    "end",
    "except",
    "false",
    "fetch",
    "for",
    "foreign",
    "from",
    "grant",
    "group",
    "having",
    "in",
    "initially",
    "intersect",
    "into",
    "lateral",
    "leading",
    "limit",
    "localtime",
    "localtimestamp",
    "not",
    "null",
    "offset",
    "on",
    "only",
    "or",
    "order",
    "placing",
    "primary",
    "references",
    "returning",
    "select",
    "session_user",
    "some",
    "symmetric",
    "system_user",
    "table",
    "then",
    "to",
    "trailing",
    "true",
    "union",
    "unique",
    "user",
    "using",
    "variadic",
    "when",
    "where",
    "window",
    "with",
];

/// Whether aclitem grantee `grantee` is one of `owners`, which the dump spells
/// `quote_ident`'ed. aclitem text quotes a name only when it holds a character
/// other than an ASCII letter, digit or `_`, so the owner `"Owner"` appears
/// in its own ACL as `Owner`: the names are compared raw. PUBLIC (the empty
/// grantee) is never an owner.
fn is_owner(owners: &[&str], grantee: &str) -> bool {
    if grantee.is_empty() {
        return false;
    }
    let grantee = unquote_ident(grantee);
    owners
        .iter()
        .any(|owner| !owner.is_empty() && unquote_ident(owner) == grantee)
}

/// Spell a role name taken from aclitem text as an SQL role specification.
///
/// aclitem text quotes a name only when it holds a character other than an
/// ASCII letter, digit or `_`, so `MixedCase` and `user` arrive bare; both
/// need quotes in SQL. An empty name is `PUBLIC`.
pub(crate) fn quote_acl_role(acl_name: &str) -> String {
    if acl_name.is_empty() {
        return "PUBLIC".to_string();
    }
    let raw = unquote_ident(acl_name);
    let bare = raw
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_lowercase() || c == '_')
        && raw
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
        && !RESERVED_KEYWORDS.contains(&raw.as_str());
    if bare {
        raw
    } else {
        format!("\"{}\"", raw.replace('"', "\"\""))
    }
}

#[cfg(test)]
#[path = "tests/acl.rs"]
mod tests;
