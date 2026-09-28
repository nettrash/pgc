//! Logical replication: [`Publication`] (`pg_publication`) and [`Subscription`]
//! (`pg_subscription`).
//!
//! Creating a publication needs the server running with `wal_level = logical`,
//! which is why CI starts PostgreSQL that way for the fixture schemas.
//!
//! # Publication membership
//!
//! A publication publishes exactly one of:
//!
//! - **every object of a kind**: `FOR ALL TABLES` (optionally with
//!   `EXCEPT (TABLE ...)`, PostgreSQL 19+) and/or `FOR ALL SEQUENCES`
//!   (PostgreSQL 19+);
//! - **a list**: `FOR TABLE` members, each with an optional column list and
//!   row filter (PostgreSQL 15+), and/or `FOR TABLES IN SCHEMA` schemas
//!   (PostgreSQL 15+);
//! - **nothing**: an empty publication.
//!
//! No `ALTER PUBLICATION` leaves the all-objects kind, and before PostgreSQL 19
//! none enters it either, so [`Publication::get_alter_script_with`] is a small
//! state machine:
//!
//! | FROM → TO | Emitted |
//! |---|---|
//! | list or empty → list | `SET TABLE ...` / `SET TABLES IN SCHEMA ...` |
//! | list → empty | `DROP TABLE ONLY ...` / `DROP TABLES IN SCHEMA ...` |
//! | all-objects → all-objects (PostgreSQL 19) | `SET ALL ...` |
//! | list or empty → all-objects, migrated server on PostgreSQL 19 | empty the list, then `SET ALL ...` |
//! | list or empty → all-objects before 19; all-objects → list or empty | `DROP PUBLICATION` + `CREATE PUBLICATION`, commented out without `use_drop` |
//!
//! `SET ALL ...` replaces the whole all-objects spec, so it always names every
//! kind the target has: `SET ALL TABLES` on an `ALL TABLES, ALL SEQUENCES`
//! publication turns `ALL SEQUENCES` off.
//!
//! Each [`Publication::tables`] and [`Publication::except_tables`] entry is one
//! `pg_publication_rel` row, so an entry that could recurse is written `ONLY`:
//! every except entry, every member of an emptying `DROP TABLE`, and a
//! `FOR TABLE` / `SET TABLE` member that is a classical-inheritance parent
//! ([`PublicationAlterContext::inheritance_parents`]). Without it,
//! `SET TABLE parent` adds the children as members too, and a parent listed
//! with its children and a row filter is refused as "conflicting or redundant
//! WHERE clauses". Other members are written without `ONLY`, as pgc 1.0.x
//! wrote them.
//!
//! # Table changes that publication state blocks
//!
//! Publications are migrated after the tables, but PostgreSQL refuses some
//! table changes while a `pg_publication_rel` row stands in the way:
//!
//! - `DROP COLUMN` or `ALTER COLUMN ... TYPE` of a column that a member's
//!   column list or row filter uses (PostgreSQL 15+; "cannot alter type of a
//!   column used by a publication WHERE clause"), even when the publication
//!   itself does not change;
//! - `SET UNLOGGED` of a member, or on PostgreSQL 19 of an except entry;
//! - on PostgreSQL 19, `ATTACH PARTITION` of an except entry.
//!
//! The comparer therefore takes those rows out ahead of the table section
//! ([`Publication::get_release_script`]) and records them in
//! [`PublicationAlterContext::released_tables`], from which
//! [`Publication::get_alter_script_with`] puts back whatever the target still
//! lists, with the target's column list and row filter. A publication the
//! migration keeps as it is — one only FROM has, without `use_drop`, or one
//! whose drop+recreate is commented out — is not altered at all, so
//! [`Publication::get_restore_script`] sets its own list again after the
//! tables; what cannot come back (a member made `UNLOGGED`, an EXCEPT entry)
//! gets a note and a warning instead.
//!
//! # Subscriptions and the publisher
//!
//! A migration only contacts a subscription's publisher where the target
//! itself needs it: [`Subscription::get_script_for`] writes `connect = false`
//! for a disabled subscription, or one without a replication slot
//! ([`SubscriptionSlot::NoSlot`], `slot_name = NONE`), so creating it neither
//! needs a reachable publisher nor leaves an unconsumed, WAL-retaining slot
//! there. Only an enabled subscription is created connected — creating its
//! slot, as `CREATE SUBSCRIPTION` does by default. A slot change on an existing
//! subscription is an `ALTER SUBSCRIPTION ... SET (slot_name = ...)`, which
//! never creates or drops a slot on the publisher.
//!
//! The catalog can hold a slot name that PostgreSQL does not accept as a
//! `slot_name` (see [`SlotNameRules`]). Such a name is never written, so the
//! migration still applies. A note in the script and a warning at compare time
//! say which subscriptions keep a slot that differs from the target's.

use std::collections::{BTreeMap, HashMap, HashSet};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::utils::string_extensions::{
    StringExt, comment_out, line_comment_safe, quote_identifier, unquote_ident,
};

/// Column list and row filter of one `FOR TABLE` member of a [`Publication`]
/// (PostgreSQL 15+, `pg_publication_rel.prattrs` and `prqual`).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicationTableSpec {
    /// Published columns, `quote_ident`'ed and sorted by name. Empty means
    /// every column (`prattrs` is NULL).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub columns: Vec<String>,
    /// Row filter as `pg_get_expr(prqual, prrelid)` deparses it, without the
    /// `WHERE ( )` around it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub row_filter: Option<String>,
}

impl PublicationTableSpec {
    /// True when the member publishes every column and every row, so it needs
    /// no [`Publication::table_specs`] entry.
    pub fn is_empty(&self) -> bool {
        self.columns.is_empty() && self.row_filter.is_none()
    }

    /// Whether the column list or the row filter names one of `columns`,
    /// spelled `quote_ident`'ed like [`PublicationTableSpec::columns`] entries.
    /// PostgreSQL then refuses to drop or retype such a column while this
    /// member exists.
    ///
    /// The row filter is matched on its identifier tokens: `pg_get_expr`
    /// quotes a column name exactly as `quote_ident` does, so a token equals
    /// the column's spelling. String literals are skipped. A function or type
    /// name that happens to equal a column name also counts, which only makes
    /// the migration release and restore the member needlessly.
    pub fn uses_any_column(&self, columns: &HashSet<String>) -> bool {
        self.columns.iter().any(|c| columns.contains(c))
            || self.row_filter.as_deref().is_some_and(|filter| {
                expression_identifiers(filter)
                    .iter()
                    .any(|token| columns.contains(*token))
            })
    }
}

/// The identifier tokens of a deparsed expression, as it spells them: a
/// double-quoted identifier with its quotes, or a bare word. Single-quoted
/// literals are skipped (a doubled `''` stays inside the literal).
fn expression_identifiers(expr: &str) -> Vec<&str> {
    let bytes = expr.as_bytes();
    let mut tokens = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i];
        if c == b'\'' || c == b'"' {
            let start = i;
            i += 1;
            while i < bytes.len() {
                if bytes[i] == c {
                    if bytes.get(i + 1) == Some(&c) {
                        i += 2;
                        continue;
                    }
                    break;
                }
                i += 1;
            }
            i = (i + 1).min(bytes.len());
            if c == b'"' {
                tokens.push(&expr[start..i]);
            }
        } else if c.is_ascii_alphabetic() || c == b'_' || c >= 0x80 {
            let start = i;
            while i < bytes.len()
                && (bytes[i].is_ascii_alphanumeric()
                    || bytes[i] == b'_'
                    || bytes[i] == b'$'
                    || bytes[i] >= 0x80)
            {
                i += 1;
            }
            tokens.push(&expr[start..i]);
        } else if c.is_ascii_digit() {
            // A number (and any exponent or suffix letters with it) is not an
            // identifier.
            while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'.') {
                i += 1;
            }
        } else {
            i += 1;
        }
    }
    tokens
}

/// What the rest of the migration does, and what the migrated database looks
/// like, that a publication's CREATE or ALTER has to account for. The comparer
/// builds it; the `Default` value describes a migrated server older than
/// PostgreSQL 19, a migration that drops no table, and no inheritance.
#[derive(Debug, Clone, Default)]
pub struct PublicationAlterContext {
    /// The migrated server accepts PostgreSQL 19's
    /// `ALTER PUBLICATION ... SET ALL TABLES / ALL SEQUENCES`.
    pub set_all_supported: bool,
    /// Tables whose `pg_publication_rel` rows are already gone when the
    /// publications are migrated, because an earlier statement of the
    /// migration dropped them (or dropped and recreated them). Spelled like
    /// [`Publication::tables`] entries: `schema.table`, both parts
    /// `quote_ident`'ed.
    pub gone_tables: HashSet<String>,
    /// Tables that have classical-inheritance children (another table's
    /// `inherits_from` names them) in either dump, spelled like
    /// [`Publication::tables`] entries. Such a member is written
    /// `ONLY` so that `FOR TABLE` / `SET TABLE` adds exactly its own row, not
    /// one for each child as well.
    pub inheritance_parents: HashSet<String>,
    /// Per publication name, the `FOR TABLE` members and EXCEPT tables that an
    /// earlier statement of the migration took out of it
    /// ([`Publication::get_release_script`]) because they would block a table
    /// change. Like [`PublicationAlterContext::gone_tables`], their rows are
    /// gone when the publication is altered, so whatever the target still
    /// lists is put back — but only in that publication.
    pub released_tables: HashMap<String, HashSet<String>>,
}

impl PublicationAlterContext {
    /// Whether an earlier statement of the migration removed one of
    /// `publication`'s `pg_publication_rel` rows, so its membership needs
    /// another look even when the hashes match.
    pub fn touches(&self, publication: &Publication) -> bool {
        publication.references_any_table(&self.gone_tables)
            || self
                .released_tables
                .get(&publication.name)
                .is_some_and(|released| !released.is_empty())
    }
}

/// A PostgreSQL publication (pg_publication).
///
/// The fields added after pgc 1.0.x are left out of the dump while they hold
/// their default and are folded into [`Publication::hash`] only when they do
/// not, so dumps and hashes of publications that do not use them stay as
/// pgc 1.0.x wrote them.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Publication {
    pub name: String,
    pub owner: String,
    /// True if publication publishes all tables (`puballtables`)
    pub all_tables: bool,
    /// `FOR ALL SEQUENCES` (PostgreSQL 19+, `puballsequences`). Sequence
    /// membership is implicit: no sequence is listed in `tables`.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub all_sequences: bool,
    /// Published operations: comma-separated (insert, update, delete, truncate)
    pub publish: String,
    /// `FOR TABLE` members (schema.table), empty for an all-tables publication
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tables: Vec<String>,
    /// Column lists and row filters of `tables` members (PostgreSQL 15+),
    /// keyed by the member's `tables` entry. A member that publishes every
    /// column and row has no entry.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub table_specs: BTreeMap<String, PublicationTableSpec>,
    /// `FOR TABLES IN SCHEMA` schemas, `quote_ident`'ed (PostgreSQL 15+,
    /// `pg_publication_namespace`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub schemas: Vec<String>,
    /// Tables of `FOR ALL TABLES EXCEPT (...)` (PostgreSQL 19+), one per
    /// `pg_publication_rel` row with `prexcept`: an inheritance parent listed
    /// without `ONLY` stores a row for each child as well, and a child created
    /// later gets none. Only meaningful with `all_tables`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub except_tables: Vec<String>,
    /// `publish_via_partition_root` (`pubviaroot`).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub via_partition_root: bool,
    /// `publish_generated_columns` (PostgreSQL 18+, `pubgencols`) as its option
    /// value (`stored`); `None` for the default, `none`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub publish_generated_columns: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    pub hash: Option<String>,
}

/// Feed `s` to `hasher` behind a length prefix, so neighbouring values cannot
/// run into each other.
fn hash_str(hasher: &mut Sha256, s: &str) {
    hasher.update((s.len() as u32).to_be_bytes());
    hasher.update(s.as_bytes());
}

/// Feed a counted list of length-prefixed strings to `hasher`.
fn hash_list(hasher: &mut Sha256, items: &[String]) {
    hasher.update((items.len() as u32).to_be_bytes());
    for item in items {
        hash_str(hasher, item);
    }
}

impl Publication {
    /// Compute [`Publication::hash`].
    ///
    /// The fields pgc 1.0.x knew are hashed exactly as it hashed them, so a
    /// hash stored in an older dump still matches. Every later field is hashed
    /// only when it is not at its default, behind a tag that starts with a NUL
    /// byte: a comment (PostgreSQL `text`) cannot contain one, so no tag can be
    /// mistaken for comment bytes.
    pub fn hash(&mut self) {
        let mut hasher = Sha256::new();
        hasher.update(self.name.as_bytes());
        hasher.update(self.owner.as_bytes());
        hasher.update([self.all_tables as u8]);
        hasher.update(self.publish.as_bytes());
        hasher.update((self.tables.len() as u32).to_be_bytes());
        for t in &self.tables {
            hasher.update((t.len() as u32).to_be_bytes());
            hasher.update(t.as_bytes());
        }
        if self.all_sequences {
            hasher.update(b"\0all_sequences");
        }
        if !self.except_tables.is_empty() {
            hasher.update(b"\0except_tables");
            hash_list(&mut hasher, &self.except_tables);
        }
        if !self.schemas.is_empty() {
            hasher.update(b"\0schemas");
            hash_list(&mut hasher, &self.schemas);
        }
        let specs = self.effective_table_specs();
        if !specs.is_empty() {
            hasher.update(b"\0table_specs");
            hasher.update((specs.len() as u32).to_be_bytes());
            for (table, spec) in specs {
                hash_str(&mut hasher, table);
                hash_list(&mut hasher, &spec.columns);
                match &spec.row_filter {
                    Some(filter) => {
                        hasher.update([1u8]);
                        hash_str(&mut hasher, filter);
                    }
                    None => hasher.update([0u8]),
                }
            }
        }
        if self.via_partition_root {
            hasher.update(b"\0via_partition_root");
        }
        if let Some(gencols) = &self.publish_generated_columns {
            hasher.update(b"\0publish_generated_columns");
            hash_str(&mut hasher, gencols);
        }
        if let Some(c) = &self.comment {
            hasher.update(c.as_bytes());
        }
        self.hash = Some(format!("{:x}", hasher.finalize()));
    }

    /// The `table_specs` entries that restrict something, so an entry holding
    /// an empty spec counts the same as no entry.
    fn effective_table_specs(&self) -> BTreeMap<&str, &PublicationTableSpec> {
        self.table_specs
            .iter()
            .filter(|(_, spec)| !spec.is_empty())
            .map(|(table, spec)| (table.as_str(), spec))
            .collect()
    }

    /// True for `FOR ALL TABLES` and/or `FOR ALL SEQUENCES`.
    fn is_all_objects(&self) -> bool {
        self.all_tables || self.all_sequences
    }

    /// True when the membership uses something only PostgreSQL 19 has, which
    /// proves the dump came from (and so the migration runs on) PostgreSQL 19.
    fn uses_pg19_membership(&self) -> bool {
        self.all_sequences || !self.except_tables.is_empty()
    }

    /// `ALL TABLES [EXCEPT (TABLE ONLY ...)][, ALL SEQUENCES]` or
    /// `ALL SEQUENCES`, as pg_dump 19 writes it; `None` when the publication is
    /// a list or empty.
    ///
    /// Each except entry is one catalog row, so `TABLE ONLY` recreates exactly
    /// those rows; without `ONLY`, an inheritance parent would also pick up
    /// children that were created after the list was set.
    fn all_objects_clause(&self) -> Option<String> {
        let mut parts = Vec::new();
        if self.all_tables {
            let mut clause = "ALL TABLES".to_string();
            if !self.except_tables.is_empty() {
                let except = self
                    .except_tables
                    .iter()
                    .map(|t| format!("TABLE ONLY {t}"))
                    .collect::<Vec<_>>()
                    .join(", ");
                clause.push_str(&format!(" EXCEPT ({except})"));
            }
            parts.push(clause);
        }
        if self.all_sequences {
            parts.push("ALL SEQUENCES".to_string());
        }
        (!parts.is_empty()).then(|| parts.join(", "))
    }

    /// The all-objects kind as messages name it, e.g. `FOR ALL TABLES`.
    fn all_objects_kind(&self) -> &'static str {
        match (self.all_tables, self.all_sequences) {
            (true, true) => "FOR ALL TABLES, ALL SEQUENCES",
            (false, true) => "FOR ALL SEQUENCES",
            _ => "FOR ALL TABLES",
        }
    }

    /// One `FOR TABLE` member with its column list and row filter, `ONLY`
    /// when it is an inheritance parent (see the module docs).
    fn member_clause(&self, table: &str, ctx: &PublicationAlterContext) -> String {
        let mut clause = if ctx.inheritance_parents.contains(table) {
            format!("ONLY {table}")
        } else {
            table.to_string()
        };
        if let Some(spec) = self.table_specs.get(table) {
            if !spec.columns.is_empty() {
                clause.push_str(&format!(" ({})", spec.columns.join(", ")));
            }
            if let Some(filter) = &spec.row_filter {
                // pg_get_expr leaves some expressions (`true`) unparenthesised,
                // and the grammar needs the parentheses; pg_dump adds them too.
                clause.push_str(&format!(" WHERE ({filter})"));
            }
        }
        clause
    }

    /// `TABLE a, b[, TABLES IN SCHEMA s]` or `TABLES IN SCHEMA s`; `None` when
    /// the publication lists nothing.
    fn list_clause(&self, ctx: &PublicationAlterContext) -> Option<String> {
        let mut parts = Vec::new();
        if !self.tables.is_empty() {
            let members = self
                .tables
                .iter()
                .map(|t| self.member_clause(t, ctx))
                .collect::<Vec<_>>()
                .join(", ");
            parts.push(format!("TABLE {members}"));
        }
        if !self.schemas.is_empty() {
            parts.push(format!("TABLES IN SCHEMA {}", self.schemas.join(", ")));
        }
        (!parts.is_empty()).then(|| parts.join(", "))
    }

    /// The `WITH (...)` options: `publish` always (as pgc 1.0.x wrote it), the
    /// others only when they are not at their default.
    fn with_clause(&self) -> String {
        let mut clause = format!("publish = '{}'", self.publish);
        if self.via_partition_root {
            clause.push_str(", publish_via_partition_root = true");
        }
        if let Some(gencols) = &self.publish_generated_columns {
            clause.push_str(&format!(", publish_generated_columns = {gencols}"));
        }
        clause
    }

    /// True when a `tables` or `except_tables` entry is in `gone_tables`, i.e.
    /// the migration removes one of this publication's `pg_publication_rel`
    /// rows as a side effect, so the membership needs another look even when
    /// the hashes match.
    pub fn references_any_table(&self, gone_tables: &HashSet<String>) -> bool {
        self.tables
            .iter()
            .chain(&self.except_tables)
            .any(|t| gone_tables.contains(t))
    }

    /// [`Publication::get_script_with`] for a database without inheritance.
    pub fn get_script(&self) -> String {
        self.get_script_with(&PublicationAlterContext::default())
    }

    /// `CREATE PUBLICATION` with its owner and comment. Only
    /// `ctx.inheritance_parents` matters here: it decides which members are
    /// written `ONLY`.
    pub fn get_script_with(&self, ctx: &PublicationAlterContext) -> String {
        let for_clause = self
            .all_objects_clause()
            .or_else(|| self.list_clause(ctx))
            .map(|clause| format!(" FOR {clause}"))
            .unwrap_or_default();

        let mut script = format!(
            "CREATE PUBLICATION {}{} WITH ({});",
            self.name,
            for_clause,
            self.with_clause()
        )
        .with_empty_lines();

        if !self.owner.is_empty() {
            script.append_block(&format!(
                "ALTER PUBLICATION {} OWNER TO {};",
                self.name, self.owner
            ));
        }

        if let Some(comment) = &self.comment {
            script.append_block(&format!(
                "COMMENT ON PUBLICATION {} IS '{}';",
                self.name,
                comment.replace('\'', "''")
            ));
        }

        script
    }

    pub fn get_drop_script(&self) -> String {
        format!("DROP PUBLICATION IF EXISTS {};", self.name).with_empty_lines()
    }

    /// [`Publication::get_alter_script_with`] for a migration that drops no
    /// table and runs on a server older than PostgreSQL 19, in a database
    /// without inheritance.
    pub fn get_alter_script(&self, target: &Publication, use_drop: bool) -> String {
        self.get_alter_script_with(target, use_drop, &PublicationAlterContext::default())
    }

    /// The `FOR TABLE` members this publication still has when it is migrated
    /// (without the rows `ctx` says an earlier statement removed), and whether
    /// its membership has to be written anew to become `target`'s.
    fn membership_change(
        &self,
        target: &Publication,
        ctx: &PublicationAlterContext,
    ) -> (Vec<String>, bool) {
        let released = ctx.released_tables.get(&self.name);
        let live = |list: &[String]| -> Vec<String> {
            list.iter()
                .filter(|t| {
                    !ctx.gone_tables.contains(*t) && !released.is_some_and(|r| r.contains(*t))
                })
                .cloned()
                .collect()
        };
        let live_tables = live(&self.tables);
        let live_except = live(&self.except_tables);

        let membership_changed = self.all_tables != target.all_tables
            || self.all_sequences != target.all_sequences
            || self.except_tables != target.except_tables
            || self.tables != target.tables
            || self.schemas != target.schemas
            || self.effective_table_specs() != target.effective_table_specs()
            || live_tables != target.tables
            || live_except != target.except_tables;
        (live_tables, membership_changed)
    }

    /// Why no `ALTER PUBLICATION` turns this publication's membership into
    /// `target`'s, so that [`Publication::get_alter_script_with`] drops and
    /// recreates it (see the state machine in the module docs); `None` when
    /// it alters it in place.
    fn recreate_reason(
        &self,
        target: &Publication,
        ctx: &PublicationAlterContext,
    ) -> Option<String> {
        if !self.membership_change(target, ctx).1 {
            return None;
        }
        // PG19-only data on either side means the FROM server is 19 (or
        // the migration cannot apply there anyway).
        let can_set_all =
            ctx.set_all_supported || self.uses_pg19_membership() || target.uses_pg19_membership();
        match (self.is_all_objects(), target.is_all_objects()) {
            (_, true) if can_set_all => None,
            (_, true) => Some(format!("to switch to {}", target.all_objects_kind())),
            (true, false) => Some(format!("to switch from {}", self.all_objects_kind())),
            (false, false) => None,
        }
    }

    /// Whether [`Publication::get_alter_script_with`] turns this publication
    /// into `target` with `DROP PUBLICATION` + `CREATE PUBLICATION` — only
    /// commented out without `use_drop`, which leaves it as it is.
    pub fn alter_recreates(&self, target: &Publication, ctx: &PublicationAlterContext) -> bool {
        self.recreate_reason(target, ctx).is_some()
    }

    /// ALTER script that turns this publication into `target`.
    ///
    /// Membership follows the state machine in the module docs. It is computed
    /// against the membership the publication still has at this point of the
    /// migration: `ctx.gone_tables` rows are already gone, so they are left out
    /// of `DROP TABLE` (which would fail on them) and a gone member the target
    /// still lists is put back. Option changes are one `SET (...)`, ordered
    /// before the membership change when it turns `publish_via_partition_root`
    /// on (a partitioned member's row filter or column list needs it) and after
    /// it otherwise. A drop+recreate returns right away, because the recreated
    /// publication already carries the target's options, owner and comment.
    pub fn get_alter_script_with(
        &self,
        target: &Publication,
        use_drop: bool,
        ctx: &PublicationAlterContext,
    ) -> String {
        let (live_tables, membership_changed) = self.membership_change(target, ctx);

        let mut membership = String::new();
        if membership_changed {
            if let Some(reason) = self.recreate_reason(target, ctx) {
                return self.recreate_script(target, use_drop, &reason, ctx);
            }
            // What is left after `recreate_reason`: into the all-objects kind
            // with `SET ALL`, or from a list (or nothing) to a list.
            if target.is_all_objects() {
                if !self.is_all_objects() {
                    // SET ALL is refused while the publication lists anything.
                    membership.push_str(&self.empty_list_script(&live_tables));
                }
                membership.append_block(&format!(
                    "ALTER PUBLICATION {} SET {};",
                    target.name,
                    target.all_objects_clause().unwrap_or_default()
                ));
            } else {
                match target.list_clause(ctx) {
                    Some(clause) => {
                        membership.append_block(&format!(
                            "ALTER PUBLICATION {} SET {};",
                            target.name, clause
                        ));
                    }
                    None => membership.push_str(&self.empty_list_script(&live_tables)),
                }
            }
        }

        let mut options = Vec::new();
        if self.publish != target.publish {
            options.push(format!("publish = '{}'", target.publish));
        }
        if self.via_partition_root != target.via_partition_root {
            options.push(format!(
                "publish_via_partition_root = {}",
                target.via_partition_root
            ));
        }
        if self.publish_generated_columns != target.publish_generated_columns {
            options.push(format!(
                "publish_generated_columns = {}",
                target
                    .publish_generated_columns
                    .as_deref()
                    .unwrap_or("none")
            ));
        }
        let options_script = if options.is_empty() {
            String::new()
        } else {
            format!(
                "ALTER PUBLICATION {} SET ({});",
                target.name,
                options.join(", ")
            )
            .with_empty_lines()
        };

        let mut script = String::new();
        if target.via_partition_root && !self.via_partition_root {
            script.push_str(&options_script);
            script.push_str(&membership);
        } else {
            script.push_str(&membership);
            script.push_str(&options_script);
        }

        if self.owner != target.owner && !target.owner.is_empty() {
            script.append_block(&format!(
                "ALTER PUBLICATION {} OWNER TO {};",
                target.name, target.owner
            ));
        }

        if self.comment != target.comment {
            match &target.comment {
                Some(c) => {
                    script.append_block(&format!(
                        "COMMENT ON PUBLICATION {} IS '{}';",
                        target.name,
                        c.replace('\'', "''")
                    ));
                }
                None if use_drop => {
                    script
                        .append_block(&format!("COMMENT ON PUBLICATION {} IS NULL;", target.name));
                }
                _ => {}
            }
        }

        script
    }

    /// Statements that remove every listed member still present
    /// (`live_tables`) and every schema, leaving the publication empty.
    /// `ONLY` keeps `DROP TABLE` from recursing into inheritance children
    /// that are not members themselves, which PostgreSQL rejects.
    fn empty_list_script(&self, live_tables: &[String]) -> String {
        let mut script = String::new();
        if !live_tables.is_empty() {
            let members = live_tables
                .iter()
                .map(|t| format!("ONLY {t}"))
                .collect::<Vec<_>>()
                .join(", ");
            script.append_block(&format!(
                "ALTER PUBLICATION {} DROP TABLE {};",
                self.name, members
            ));
        }
        if !self.schemas.is_empty() {
            script.append_block(&format!(
                "ALTER PUBLICATION {} DROP TABLES IN SCHEMA {};",
                self.name,
                self.schemas.join(", ")
            ));
        }
        script
    }

    /// The statements that take `members` (`FOR TABLE` entries) and `except`
    /// (EXCEPT entries, PostgreSQL 19) out of this publication, ahead of the
    /// table changes they would make PostgreSQL refuse: a drop or retype of a
    /// column the member's column list or row filter uses, `SET UNLOGGED` of a
    /// member or except entry, `ATTACH PARTITION` of an except entry. Empty
    /// when both are.
    ///
    /// Members go with `DROP TABLE ONLY`, one row each; except entries with a
    /// `SET ALL ...` that lists the rest. [`Publication::get_alter_script_with`]
    /// puts back what the target keeps once it finds them in
    /// [`PublicationAlterContext::released_tables`].
    pub fn get_release_script(&self, members: &[String], except: &[String]) -> String {
        let mut script = String::new();
        if !members.is_empty() {
            let list = members
                .iter()
                .map(|t| format!("ONLY {t}"))
                .collect::<Vec<_>>()
                .join(", ");
            script.append_block(&format!(
                "ALTER PUBLICATION {} DROP TABLE {};",
                self.name, list
            ));
        }
        if !except.is_empty() {
            let kept = Publication {
                except_tables: self
                    .except_tables
                    .iter()
                    .filter(|t| !except.contains(*t))
                    .cloned()
                    .collect(),
                ..self.clone()
            };
            if let Some(clause) = kept.all_objects_clause() {
                script.append_block(&format!("ALTER PUBLICATION {} SET {};", self.name, clause));
            }
        }
        script
    }

    /// The statements that put back, once the tables are migrated, the rows
    /// [`Publication::get_release_script`] took out of this publication, for a
    /// publication the migration otherwise leaves as it is: a FROM-only one
    /// without `use_drop`, or one whose drop+recreate is commented out
    /// ([`Publication::alter_recreates`]). No `ALTER PUBLICATION` of the
    /// target restores them there, and before this the membership was lost
    /// without a word. `released` are the entries taken out, `unlogged` the
    /// tables (spelled like [`Publication::tables`] entries) the migration
    /// makes `UNLOGGED`.
    ///
    /// The `FOR TABLE` members come back with a `SET` of this publication's
    /// own list, column lists and row filters included — a `SET` rather than
    /// `ADD TABLE`, so that a replayed migration sets the same list again
    /// instead of failing on a member that is already back. Only a retyped
    /// column can have blocked a member here, since columns are dropped only
    /// with `use_drop`; a row filter the new type no longer accepts fails
    /// here, as the table change used to. A member made `UNLOGGED` and an
    /// EXCEPT entry (made `UNLOGGED` or attached as a partition) cannot be put
    /// back — PostgreSQL publishes no unlogged table and takes neither an
    /// unlogged table nor a partition in `EXCEPT` — so each gets a note
    /// instead ([`Publication::released_for_good`]).
    pub fn get_restore_script(
        &self,
        released: &HashSet<String>,
        unlogged: &HashSet<String>,
        ctx: &PublicationAlterContext,
    ) -> String {
        let mut script = String::new();
        let lost = |t: &String| released.contains(t) && unlogged.contains(t);
        if self
            .tables
            .iter()
            .any(|t| released.contains(t) && !unlogged.contains(t))
        {
            let kept = Publication {
                tables: self.tables.iter().filter(|t| !lost(t)).cloned().collect(),
                ..self.clone()
            };
            if let Some(clause) = kept.list_clause(ctx) {
                script.append_block(&format!("ALTER PUBLICATION {} SET {};", self.name, clause));
            }
        }
        for note in self.released_for_good(released, unlogged) {
            script.push_str(&format!("-- {}\n", line_comment_safe(&note)));
        }
        script
    }

    /// What [`Publication::get_restore_script`] cannot put back, one message
    /// per row: the released members made `UNLOGGED` and the released EXCEPT
    /// entries. The comparer also prints them as warnings, since
    /// `--use-comments false` strips the notes from the script.
    pub fn released_for_good(
        &self,
        released: &HashSet<String>,
        unlogged: &HashSet<String>,
    ) -> Vec<String> {
        let members = self
            .tables
            .iter()
            .filter(|t| released.contains(*t) && unlogged.contains(*t))
            .map(|t| {
                format!(
                    "publication {}: member {t} was dropped because the migration makes it UNLOGGED, which a publication cannot hold",
                    self.name
                )
            });
        let except = self
            .except_tables
            .iter()
            .filter(|t| released.contains(*t))
            .map(|t| {
                format!(
                    "publication {}: {t} was taken out of EXCEPT because the migration makes it UNLOGGED or a partition, which EXCEPT cannot hold",
                    self.name
                )
            });
        members.chain(except).collect()
    }

    /// `DROP PUBLICATION` + the target's `CREATE PUBLICATION`, or both
    /// commented out behind a note saying why when `use_drop` is off.
    fn recreate_script(
        &self,
        target: &Publication,
        use_drop: bool,
        reason: &str,
        ctx: &PublicationAlterContext,
    ) -> String {
        let drop = self.get_drop_script();
        let create = target.get_script_with(ctx);
        if use_drop {
            return format!("{drop}{create}");
        }
        format!(
            "-- use_drop=false: publication {} requires drop+recreate {}; statements commented out\n{}{}",
            line_comment_safe(&self.name),
            reason,
            comment_out(&drop),
            comment_out(&create)
        )
    }
}

/// The replication slot a [`Subscription`] streams from
/// (`pg_subscription.subslotname`).
///
/// [`SubscriptionSlot::Own`] — a slot named after the subscription — is
/// `CREATE SUBSCRIPTION`'s default. It is neither serialized nor hashed, so a
/// subscription on its own slot keeps the hash pgc 1.0.x gave it, and a dump
/// written by pgc 1.0.x, which did not read the slot, loads as `Own`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SubscriptionSlot {
    /// A slot named after the subscription (`subslotname = subname`).
    #[default]
    Own,
    /// No slot: `slot_name = NONE` (`subslotname` NULL). PostgreSQL keeps
    /// such a subscription disabled ("cannot enable subscription that does
    /// not have a slot name").
    NoSlot,
    /// A slot with another name, raw as the catalog holds it.
    Named(String),
}

impl SubscriptionSlot {
    /// Builds the slot from the catalog: `is_null` is `subslotname IS NULL`,
    /// `other_name` is `NULLIF(subslotname, subname)` — `None` for a slot
    /// named after the subscription.
    pub fn from_catalog(is_null: bool, other_name: Option<String>) -> Self {
        match (is_null, other_name) {
            (true, _) => SubscriptionSlot::NoSlot,
            (false, None) => SubscriptionSlot::Own,
            (false, Some(name)) => SubscriptionSlot::Named(name),
        }
    }

    /// Whether this is the default, [`SubscriptionSlot::Own`]
    /// (`skip_serializing_if` predicate).
    pub fn is_own(&self) -> bool {
        matches!(self, SubscriptionSlot::Own)
    }
}

/// What the migrated server accepts as the value of a subscription's
/// `slot_name` option, in `CREATE SUBSCRIPTION` and in
/// `ALTER SUBSCRIPTION ... SET`.
///
/// PostgreSQL checks that value as a replication slot name
/// (`ReplicationSlotValidateName`): 1 to 63 bytes of lower-case ASCII letters,
/// digits and underscores. It reads `'none'` as `slot_name = NONE`, and
/// PostgreSQL 19 reserves `pg_conflict_detection` for its conflict detection
/// slot. The catalog can still hold any other name. A subscription created
/// `connect = false` without a `slot_name` records its own name unchecked
/// (`CREATE SUBSCRIPTION "My Sub" ... WITH (connect = false)`), and a
/// `RENAME` keeps the slot name.
///
/// pgc never writes a slot these rules refuse. Such a `SET` or `CREATE` would
/// fail, or for `none` clear the slot, and the next compare would emit it
/// again. [`Subscription::get_script_for`] and
/// [`Subscription::get_alter_script_for`] leave such a slot out, with a `--`
/// note.
///
/// The default holds the rules of PostgreSQL 14 to 18.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SlotNameRules {
    /// `pg_conflict_detection` is reserved (the migrated server is on
    /// PostgreSQL 19 or later).
    pub conflict_slot_reserved: bool,
}

impl SlotNameRules {
    /// The first `server_version_num` that reserves
    /// [`SlotNameRules::CONFLICT_DETECTION_SLOT`].
    pub const CONFLICT_SLOT_RESERVED_MIN_VERSION: i32 = 190000;

    /// The slot PostgreSQL 19 keeps for the conflict detection that
    /// `retain_dead_tuples` needs.
    pub const CONFLICT_DETECTION_SLOT: &'static str = "pg_conflict_detection";

    /// The longest slot name, in bytes (`NAMEDATALEN - 1`).
    const MAX_LEN: usize = 63;

    /// Whether `slot`, raw, is a replication slot name PostgreSQL accepts
    /// (`ReplicationSlotValidateName`): one it can create on a publisher,
    /// and the one a connected `CREATE SUBSCRIPTION` creates there when no
    /// `slot_name` is given.
    pub fn is_valid(&self, slot: &str) -> bool {
        !slot.is_empty()
            && slot.len() <= Self::MAX_LEN
            && slot
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
            && !(self.conflict_slot_reserved && slot == Self::CONFLICT_DETECTION_SLOT)
    }

    /// Whether PostgreSQL takes `slot`, raw, as `slot_name = '...'`: a valid
    /// name ([`SlotNameRules::is_valid`]) other than `none`, which it reads
    /// as `slot_name = NONE`.
    pub fn accepts(&self, slot: &str) -> bool {
        self.is_valid(slot) && slot != "none"
    }
}

/// How the `--` notes and warnings on a slot [`SlotNameRules`] refuse
/// describe the rules.
pub(crate) const SLOT_NAME_RULE: &str = "a slot name has 1 to 63 lower-case letters, digits and \
                              underscores, and is neither none nor, on PostgreSQL 19+, \
                              pg_conflict_detection";

/// A PostgreSQL subscription (pg_subscription).
///
/// The subscription's source is either a libpq conninfo (`connection`) or,
/// on PostgreSQL 19+, a foreign server (`server`, `CREATE SUBSCRIPTION ...
/// SERVER s`) — never both: the catalog keeps `subconninfo` NULL exactly when
/// `subserver` is set.
///
/// Only the options below are modelled. The replication slot and every PG19
/// option are folded into [`Subscription::hash`] only when they differ from
/// their default, so a subscription that does not use them hashes exactly as
/// it did in pgc 1.0.x.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Subscription {
    pub name: String,
    pub owner: String,
    /// Connection string (`subconninfo`). Empty for a `SERVER` subscription,
    /// whose conninfo is built from the server and its user mapping instead.
    #[serde(default)]
    pub connection: String,
    /// Foreign server the subscription takes its connection from
    /// (`pg_subscription.subserver`, PG19+), as `quote_ident(srvname)`. When
    /// `Some`, `connection` is empty and ignored. Always `None` before PG19.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server: Option<String>,
    /// Publication names to subscribe to, raw as `pg_subscription.subpublications`
    /// holds them (not `quote_ident`'ed, unlike every other name in the dump).
    /// Kept raw so the hash of a subscription dumped by pgc 1.0.x still
    /// matches; [`Subscription::publication_list`] quotes them for the DDL.
    pub publications: Vec<String>,
    /// Enabled state
    pub enabled: bool,
    /// The replication slot (`subslotname`). Left out of the dump, and read
    /// back, as [`SubscriptionSlot::Own`] — the default, and all a pgc 1.0.x
    /// dump can say.
    #[serde(default, skip_serializing_if = "SubscriptionSlot::is_own")]
    pub slot_name: SubscriptionSlot,
    /// `retain_dead_tuples` (`subretaindeadtuples`, PG19+): keep the dead
    /// tuples conflict detection needs. Always `false` before PG19.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub retain_dead_tuples: bool,
    /// `max_retention_duration` in milliseconds (`submaxretention`, PG19+);
    /// `0` means no limit and is the default. Always `0` before PG19.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub max_retention_duration: i32,
    /// Per-subscription `wal_receiver_timeout` (`subwalrcvtimeout`, PG19+),
    /// verbatim as the server stores it (`'30s'`, `'45000'` and `'1min'` stay
    /// distinct). `None` when it is `'-1'` (inherit the GUC, the default) or
    /// the server predates PG19.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wal_receiver_timeout: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    pub hash: Option<String>,
}

impl Subscription {
    pub fn hash(&mut self) {
        let mut hasher = Sha256::new();
        hasher.update(self.name.as_bytes());
        hasher.update(self.owner.as_bytes());
        hasher.update(self.connection.as_bytes());
        // PG19 fields: folded in only when set, tagged and length-prefixed,
        // so a subscription without them keeps its pgc 1.0.x hash.
        if let Some(server) = &self.server {
            hasher.update(b"server:");
            hasher.update((server.len() as u32).to_be_bytes());
            hasher.update(server.as_bytes());
        }
        hasher.update((self.publications.len() as u32).to_be_bytes());
        for p in &self.publications {
            hasher.update((p.len() as u32).to_be_bytes());
            hasher.update(p.as_bytes());
        }
        hasher.update([self.enabled as u8]);
        // The slot: folded in only when it is not named after the
        // subscription, tagged (and a name length-prefixed), so a
        // subscription on its own slot — and every one a pgc 1.0.x dump
        // holds — keeps its pgc 1.0.x hash.
        match &self.slot_name {
            SubscriptionSlot::Own => {}
            SubscriptionSlot::NoSlot => hasher.update(b"slot_name=none"),
            SubscriptionSlot::Named(slot) => {
                hasher.update(b"slot_name:");
                hasher.update((slot.len() as u32).to_be_bytes());
                hasher.update(slot.as_bytes());
            }
        }
        if self.retain_dead_tuples {
            hasher.update(b"retain_dead_tuples");
        }
        if self.max_retention_duration != 0 {
            hasher.update(b"max_retention_duration:");
            hasher.update(self.max_retention_duration.to_be_bytes());
        }
        if let Some(timeout) = &self.wal_receiver_timeout {
            hasher.update(b"wal_receiver_timeout:");
            hasher.update((timeout.len() as u32).to_be_bytes());
            hasher.update(timeout.as_bytes());
        }
        if let Some(c) = &self.comment {
            hasher.update(c.as_bytes());
        }
        self.hash = Some(format!("{:x}", hasher.finalize()));
    }

    /// The `SERVER s` / `CONNECTION '...'` clause naming the source.
    fn source_clause(&self) -> String {
        match &self.server {
            Some(server) => format!("SERVER {server}"),
            None => format!("CONNECTION '{}'", self.connection.replace('\'', "''")),
        }
    }

    /// The publications as the `PUBLICATION` clause of `CREATE SUBSCRIPTION` /
    /// `ALTER SUBSCRIPTION ... SET PUBLICATION` lists them: each name quoted
    /// the way `quote_ident()` would, so `"MyPub"` does not fold to `mypub` and
    /// `"pub two"` or `"select"` still parse.
    pub fn publication_list(&self) -> String {
        self.publications
            .iter()
            .map(|p| quote_identifier(p))
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// `wal_receiver_timeout = '...'`, with `'-1'` (inherit the GUC) for `None`.
    /// PostgreSQL has no `ALTER SUBSCRIPTION ... RESET`, so `'-1'` is the only
    /// way back to the default.
    fn wal_receiver_timeout_option(value: Option<&str>) -> String {
        format!(
            "wal_receiver_timeout = '{}'",
            value.unwrap_or("-1").replace('\'', "''")
        )
    }

    /// The raw name of the replication slot, `None` for `slot_name = NONE`.
    pub fn slot(&self) -> Option<String> {
        match &self.slot_name {
            SubscriptionSlot::Own => Some(unquote_ident(&self.name)),
            SubscriptionSlot::NoSlot => None,
            SubscriptionSlot::Named(slot) => Some(slot.clone()),
        }
    }

    /// `slot_name = '...'`, or `slot_name = NONE` without a slot. Only for a
    /// slot [`Subscription::slot_settable`] accepts.
    fn slot_name_option(&self) -> String {
        match self.slot() {
            Some(slot) => format!("slot_name = '{}'", slot.replace('\'', "''")),
            None => "slot_name = NONE".to_string(),
        }
    }

    /// Whether the slot can be written as `slot_name = ...` under `rules`:
    /// always for [`SubscriptionSlot::NoSlot`], otherwise when `rules`
    /// accept the slot's name.
    pub fn slot_settable(&self, rules: SlotNameRules) -> bool {
        self.slot().is_none_or(|slot| rules.accepts(&slot))
    }

    /// Whether a `CREATE SUBSCRIPTION` can give the subscription this slot
    /// under `rules`. A slot named after the subscription is recorded without
    /// a `slot_name` option, so it only needs a valid name
    /// ([`SlotNameRules::is_valid`]), and only for the publisher to create
    /// it. Any other slot needs `slot_name = ...`
    /// ([`Subscription::slot_settable`]).
    fn slot_creatable(&self, rules: SlotNameRules) -> bool {
        match &self.slot_name {
            SubscriptionSlot::Own => rules.is_valid(&unquote_ident(&self.name)),
            _ => self.slot_settable(rules),
        }
    }

    /// Whether the migration cannot give this TO subscription its slot,
    /// because `rules` refuse the slot's name. `from` is the FROM
    /// subscription it is altered from, `None` when it is created.
    ///
    /// For an altered subscription this means the slot differs from FROM's.
    /// For a created one it means the slot is not the subscription's own:
    /// `CREATE SUBSCRIPTION` records that one without checking it.
    pub fn slot_out_of_reach(&self, from: Option<&Subscription>, rules: SlotNameRules) -> bool {
        if self.slot_settable(rules) {
            return false;
        }
        match from {
            Some(from) => from.slot_name != self.slot_name,
            None => !self.slot_name.is_own(),
        }
    }

    /// Whether [`Subscription::get_script_for`] connects to the publisher:
    /// only for an enabled subscription with a replication slot whose name
    /// `rules` accept. The publisher refuses to create a slot under any other
    /// name. Everything else is created `connect = false`.
    pub fn connects_on_create(&self, rules: SlotNameRules) -> bool {
        self.enabled && self.slot_name != SubscriptionSlot::NoSlot && self.slot_creatable(rules)
    }

    /// [`Subscription::get_script_for`] under the slot name rules of
    /// PostgreSQL 14 to 18 ([`SlotNameRules::default`]).
    pub fn get_script(&self) -> String {
        self.get_script_for(SlotNameRules::default())
    }

    /// `CREATE SUBSCRIPTION`, then `OWNER TO` and `COMMENT ON`.
    ///
    /// **Disabled, or without a slot.** The subscription is created
    /// `WITH (connect = false[, slot_name = ...], enabled = false, ...)`.
    /// - The publisher is not contacted, so no slot is created there and no
    ///   table is subscribed yet. A `--` note says so when the subscription
    ///   records a slot.
    /// - `enabled = false` follows from `connect = false`. It is written out
    ///   as the state the migration leaves, which `slot_name = NONE`
    ///   requires.
    ///
    /// **Enabled.** The subscription is created as `CREATE SUBSCRIPTION` does
    /// by default: connected, creating its slot on the publisher and copying
    /// the tables. It gets `slot_name = '...'` when the slot is not named
    /// after the subscription, with a note that an existing slot of that name
    /// is taken over with `create_slot = false`.
    ///
    /// **A slot `rules` refuse.** The subscription is created
    /// `connect = false`, even when enabled, and a `--` note says so:
    /// - a slot named after the subscription whose name is no valid slot
    ///   name ([`SlotNameRules::is_valid`]) is recorded as it is, but the
    ///   publisher would refuse to create it;
    /// - any other slot PostgreSQL does not take as `slot_name = '...'`
    ///   ([`SlotNameRules::accepts`]) is left out, so the subscription is
    ///   created on the slot named after it.
    pub fn get_script_for(&self, rules: SlotNameRules) -> String {
        let connect = self.connects_on_create(rules);
        let mut options: Vec<String> = Vec::new();
        if !connect {
            options.push("connect = false".to_string());
        }
        if !self.slot_name.is_own() && self.slot_settable(rules) {
            options.push(self.slot_name_option());
        }
        if !connect {
            options.push("enabled = false".to_string());
        }
        if self.retain_dead_tuples {
            options.push("retain_dead_tuples = true".to_string());
        }
        if self.max_retention_duration != 0 {
            options.push(format!(
                "max_retention_duration = {}",
                self.max_retention_duration
            ));
        }
        if let Some(timeout) = &self.wal_receiver_timeout {
            options.push(Self::wal_receiver_timeout_option(Some(timeout)));
        }
        let with_clause = if options.is_empty() {
            String::new()
        } else {
            format!(" WITH ({})", options.join(", "))
        };

        let mut script = String::new();
        if let Some(note) = self.create_note(connect, rules) {
            script.append_block(&note);
        }
        script.append_block(&format!(
            "CREATE SUBSCRIPTION {} {} PUBLICATION {}{};",
            self.name,
            self.source_clause(),
            self.publication_list(),
            with_clause
        ));

        if !self.owner.is_empty() {
            script.append_block(&format!(
                "ALTER SUBSCRIPTION {} OWNER TO {};",
                self.name, self.owner
            ));
        }

        if let Some(comment) = &self.comment {
            script.append_block(&format!(
                "COMMENT ON SUBSCRIPTION {} IS '{}';",
                self.name,
                comment.replace('\'', "''")
            ));
        }

        script
    }

    /// The `--` note [`Subscription::get_script_for`] puts ahead of the
    /// `CREATE SUBSCRIPTION` whose slot the migration does not create,
    /// creates under a name of its own, or cannot name: `None` for a
    /// subscription without a slot and for one created connected on a slot
    /// named after it.
    fn create_note(&self, connect: bool, rules: SlotNameRules) -> Option<String> {
        let slot = self.slot()?;
        let name = line_comment_safe(&self.name);
        let slot = line_comment_safe(&slot);
        if !self.slot_creatable(rules) {
            return Some(if self.slot_name.is_own() {
                format!(
                    "-- subscription {name} is created disabled without contacting its publisher \
                     (connect = false): PostgreSQL does not accept its replication slot {slot} as \
                     a slot name ({SLOT_NAME_RULE}), so it needs another slot_name before ALTER \
                     SUBSCRIPTION ... ENABLE"
                )
            } else {
                format!(
                    "-- replication slot {slot} of subscription {name} is left out: PostgreSQL \
                     does not accept it as a slot_name ({SLOT_NAME_RULE}). The subscription is \
                     created disabled without contacting its publisher (connect = false), on the \
                     slot named after it, and needs a slot PostgreSQL accepts before ALTER \
                     SUBSCRIPTION ... ENABLE"
                )
            });
        }
        if !connect {
            Some(format!(
                "-- subscription {name} is created disabled without contacting its publisher \
                 (connect = false): replication slot {slot} is not created there and no table \
                 is subscribed yet. The slot has to exist on the publisher before ALTER \
                 SUBSCRIPTION ... ENABLE, and ALTER SUBSCRIPTION ... REFRESH PUBLICATION then \
                 subscribes the tables"
            ))
        } else if matches!(self.slot_name, SubscriptionSlot::Named(_)) {
            Some(format!(
                "-- subscription {name} connects to its publisher and creates replication slot \
                 {slot} there: if that slot exists already, add create_slot = false to use it"
            ))
        } else {
            None
        }
    }

    /// The `--` note ahead of an `ALTER SUBSCRIPTION ... SET (slot_name = ...)`
    /// that moves `self` to `target`'s slot: the statement neither drops the
    /// old slot on the publisher nor creates the new one.
    fn slot_change_note(&self, target: &Subscription) -> String {
        let name = line_comment_safe(&target.name);
        let mut lines = Vec::new();
        if let Some(old) = self.slot() {
            lines.push(format!(
                "-- replication slot {} stays on the publisher of subscription {name}: ALTER \
                 SUBSCRIPTION does not drop it. Drop it there once nothing uses it, or it keeps \
                 retaining WAL",
                line_comment_safe(&old)
            ));
        }
        if let Some(new) = target.slot() {
            lines.push(format!(
                "-- ALTER SUBSCRIPTION does not create replication slot {}: it has to exist on \
                 the publisher before subscription {name} is enabled",
                line_comment_safe(&new)
            ));
        }
        lines.join("\n")
    }

    /// The `--` note that stands in for the `SET (slot_name = ...)` moving
    /// `self` to a `target` slot whose name PostgreSQL refuses (see
    /// [`Subscription::slot_out_of_reach`]). The subscription keeps its slot,
    /// and when that leaves it without one it cannot be enabled either.
    fn unsettable_slot_note(&self, target: &Subscription) -> String {
        let name = line_comment_safe(&target.name);
        let target_slot = target.slot().unwrap_or_default();
        let slot = line_comment_safe(&target_slot);
        let (kept, not_enabled) = match self.slot() {
            Some(old) => (format!("replication slot {}", line_comment_safe(&old)), ""),
            None if target.enabled => (
                "no replication slot (slot_name = NONE)".to_string(),
                ", and stays disabled: PostgreSQL cannot enable a subscription without a slot",
            ),
            None => ("no replication slot (slot_name = NONE)".to_string(), ""),
        };
        format!(
            "-- replication slot {slot} of subscription {name} is left out: PostgreSQL does not \
             accept it as a slot_name ({SLOT_NAME_RULE}). The subscription stays on {kept}\
             {not_enabled}"
        )
    }

    pub fn get_drop_script(&self) -> String {
        format!("DROP SUBSCRIPTION IF EXISTS {};", self.name).with_empty_lines()
    }

    /// Whether turning `self` into `target` has to wait for the apply worker
    /// to exit: `retain_dead_tuples` changes on an enabled subscription.
    ///
    /// PostgreSQL 19 accepts that `SET` only on a disabled subscription whose
    /// workers are gone ("logical replication worker is still running"), so
    /// [`Subscription::get_alter_script_with`] disables the subscription
    /// first and waits for its workers to exit. A worker only notices the
    /// `DISABLE` once it commits, so inside one transaction the `SET` fails
    /// for as long as the worker runs.
    pub fn restarts_apply_worker(&self, target: &Subscription) -> bool {
        self.enabled && self.retain_dead_tuples != target.retain_dead_tuples
    }

    /// How long [`Subscription::worker_exit_wait_script`] waits for the
    /// workers of a disabled subscription to exit, in seconds. A worker
    /// normally exits within milliseconds of the `DISABLE`; the bound only
    /// keeps a worker that never does from hanging the migration.
    const WORKER_EXIT_WAIT_SECONDS: u32 = 60;

    /// A `DO` block that returns once `pg_stat_subscription` shows no worker
    /// of this subscription (in the current database) any more, or after
    /// [`Subscription::WORKER_EXIT_WAIT_SECONDS`]; the `SET` that follows then
    /// fails with PostgreSQL's own error. Only useful after a committed
    /// `DISABLE`, so never emitted in a single transaction.
    ///
    /// `pg_subscription` is shared by every database of the cluster, hence
    /// the `subdbid` filter; the name is compared raw, as the catalog holds
    /// it. The block is dollar-quoted with a tag its body does not contain.
    fn worker_exit_wait_script(&self) -> String {
        let raw_name = unquote_ident(&self.name).replace('\'', "''");
        let body = format!(
            "\nDECLARE\n    \
                 deadline timestamptz := clock_timestamp() + interval '{secs} seconds';\n\
             BEGIN\n    \
                 WHILE clock_timestamp() < deadline AND EXISTS (\n        \
                     SELECT 1 FROM pg_catalog.pg_stat_subscription st\n        \
                     JOIN pg_catalog.pg_subscription s ON s.oid = st.subid\n        \
                     WHERE s.subname = '{raw_name}'\n          \
                       AND s.subdbid = (SELECT oid FROM pg_catalog.pg_database WHERE datname = current_database())\n          \
                       AND st.pid IS NOT NULL\n    \
                 ) LOOP\n        \
                     PERFORM pg_catalog.pg_sleep(0.1);\n    \
                 END LOOP;\n\
             END\n",
            secs = Self::WORKER_EXIT_WAIT_SECONDS,
        );
        let tag = crate::dump::acl::dollar_quote_tag(&body);
        format!("DO {tag}{body}{tag};")
    }

    /// [`Subscription::get_alter_script_with`] for a migration that is not
    /// wrapped in one transaction, where nothing has to wait for a commit:
    /// its [`SubscriptionAlterScript::script`].
    pub fn get_alter_script(&self, target: &Subscription, use_drop: bool) -> String {
        self.get_alter_script_with(target, use_drop, false).script
    }

    /// [`Subscription::get_alter_script_for`] under the slot name rules of
    /// PostgreSQL 14 to 18 ([`SlotNameRules::default`]).
    pub fn get_alter_script_with(
        &self,
        target: &Subscription,
        use_drop: bool,
        single_transaction: bool,
    ) -> SubscriptionAlterScript {
        self.get_alter_script_for(
            target,
            use_drop,
            single_transaction,
            SlotNameRules::default(),
        )
    }

    /// Emit the `ALTER SUBSCRIPTION` statements that turn `self` into `target`.
    /// Every property is alterable in place, so a subscription is never
    /// dropped and recreated (that would lose its slot and origin progress).
    ///
    /// `SET PUBLICATION` refreshes the subscribed tables by default, which
    /// PostgreSQL refuses on a disabled subscription and inside a transaction
    /// block. So it keeps its default only when the subscription is enabled at
    /// that point and `single_transaction` is off; otherwise it is written
    /// `WITH (refresh = false)`, and when the subscription ends up enabled an
    /// `ALTER SUBSCRIPTION ... REFRESH PUBLICATION` follows — right after the
    /// `ENABLE`, or, in a single transaction, in
    /// [`SubscriptionAlterScript::post_commit`]. A subscription that stays
    /// disabled cannot refresh at all; its tables are picked up by the
    /// `REFRESH PUBLICATION` that has to follow its eventual `ENABLE` anyway.
    ///
    /// Order matters for `retain_dead_tuples`, which PostgreSQL 19 only lets
    /// change on a disabled subscription, and which makes a source change
    /// (`CONNECTION` / `SERVER`) contact the publisher while it is on:
    ///
    /// - turning it **off** runs before the source change, so the new source
    ///   is never contacted;
    /// - turning it **on** (which always contacts the publisher) runs after
    ///   the source change, so the check runs against the new source;
    /// - an enabled subscription (see [`Subscription::restarts_apply_worker`])
    ///   is disabled before anything else, and — outside a single
    ///   transaction — a `DO` block (`worker_exit_wait_script`) waits for
    ///   its workers to exit. Every other change then runs
    ///   while it is disabled: a source change, `SET PUBLICATION` or owner
    ///   switch made while it is enabled restarts its worker, and the `SET`
    ///   would meet the restarted one ("logical replication worker is still
    ///   running"). `SET PUBLICATION` is therefore written
    ///   `WITH (refresh = false)`, and the `REFRESH PUBLICATION` follows the
    ///   `ENABLE`. When the target is disabled, the subscription is enabled
    ///   for that refresh and disabled again (not in a single transaction),
    ///   as a `SET PUBLICATION` on the enabled subscription would have
    ///   refreshed it.
    ///
    /// `max_retention_duration` and `wal_receiver_timeout` ride in the same
    /// `SET (...)`; on their own they are legal on an enabled subscription.
    ///
    /// So does a slot change, `slot_name = '...'` or `slot_name = NONE`, with
    /// a `--` note ahead of the `SET`: it neither drops the old slot on the
    /// publisher nor creates the new one. PostgreSQL takes a named slot on an
    /// enabled subscription, but `slot_name = NONE` only on a disabled one,
    /// so an enabled subscription losing its slot is disabled first (its
    /// target is disabled as well: a subscription without a slot cannot be
    /// enabled). For the same reason a bounced subscription whose target has
    /// no slot is not enabled again for a `REFRESH PUBLICATION`. The `SET`
    /// always precedes the `ENABLE`, which needs the slot name.
    ///
    /// A target slot whose name `rules` refuse is left out of the `SET`, and
    /// a `--` note at the top of the script says so (see [`SlotNameRules`]).
    /// The subscription keeps its slot. If that leaves it without a slot, it
    /// is not enabled either.
    ///
    /// An owner change goes before an `ALTER ... SERVER` switch: that
    /// statement checks the owner's user mapping, `USAGE` and, for a
    /// non-superuser owner, the password, so it has to see the owner the
    /// subscription will connect as. Otherwise `OWNER TO` comes after the
    /// state changes, as it always has: a `CONNECTION` change demands a
    /// password only from a non-superuser owner, so it is best made while the
    /// old owner (usually the superuser that created it) still owns it.
    pub fn get_alter_script_for(
        &self,
        target: &Subscription,
        use_drop: bool,
        single_transaction: bool,
        rules: SlotNameRules,
    ) -> SubscriptionAlterScript {
        let mut script = String::new();
        let mut post_commit = String::new();
        let name = &target.name;

        // The slot the subscription ends on is TO's, unless PostgreSQL
        // refuses its name: then it keeps its own, and it ends enabled only
        // if that leaves it a slot.
        let slot_unreachable = target.slot_out_of_reach(Some(self), rules);
        let end_slot = if slot_unreachable {
            &self.slot_name
        } else {
            &target.slot_name
        };
        let end_enabled = target.enabled && *end_slot != SubscriptionSlot::NoSlot;
        if slot_unreachable {
            script.append_block(&self.unsettable_slot_note(target));
        }

        let rdt_changed = self.retain_dead_tuples != target.retain_dead_tuples;
        let mut options: Vec<String> = Vec::new();
        if rdt_changed {
            options.push(format!(
                "retain_dead_tuples = {}",
                target.retain_dead_tuples
            ));
        }
        if self.max_retention_duration != target.max_retention_duration {
            options.push(format!(
                "max_retention_duration = {}",
                target.max_retention_duration
            ));
        }
        if self.wal_receiver_timeout != target.wal_receiver_timeout {
            options.push(Self::wal_receiver_timeout_option(
                target.wal_receiver_timeout.as_deref(),
            ));
        }
        let slot_changed = self.slot_name != *end_slot;
        let slot_to_none = slot_changed && *end_slot == SubscriptionSlot::NoSlot;
        if slot_changed {
            options.push(target.slot_name_option());
        }
        let set_options = (!options.is_empty())
            .then(|| format!("ALTER SUBSCRIPTION {name} SET ({});", options.join(", ")));
        let slot_note = slot_changed.then(|| self.slot_change_note(target));
        // The SET, after its slot note when it changes the slot.
        let push_set = |script: &mut String| {
            if let Some(set) = &set_options {
                if let Some(note) = &slot_note {
                    script.append_block(note);
                }
                script.append_block(set);
            }
        };
        let publications_changed = self.publications != target.publications;
        let owner_changed = self.owner != target.owner && !target.owner.is_empty();
        let owner_script = format!("ALTER SUBSCRIPTION {name} OWNER TO {};", target.owner);
        let bounce = self.restarts_apply_worker(target);
        // Whether the subscription is enabled at this point of the script.
        let mut enabled_now = self.enabled;
        let set_before_source = rdt_changed && !target.retain_dead_tuples;

        if bounce {
            if single_transaction {
                script.append_block(
                    "-- retain_dead_tuples can only change once the workers of the subscription have \
                     exited, which they do only after this DISABLE commits: run these statements \
                     outside a single transaction",
                );
                script.append_block(&format!("ALTER SUBSCRIPTION {name} DISABLE;"));
            } else {
                script.append_block(
                    "-- retain_dead_tuples can only change once the workers of the subscription have \
                     exited: it is disabled first, and the block below waits for them",
                );
                script.append_block(&format!("ALTER SUBSCRIPTION {name} DISABLE;"));
                script.append_block(&target.worker_exit_wait_script());
            }
            enabled_now = false;
        } else if slot_to_none && enabled_now {
            // PostgreSQL takes slot_name = NONE only on a disabled
            // subscription; the target is disabled anyway.
            script.append_block(&format!("ALTER SUBSCRIPTION {name} DISABLE;"));
            enabled_now = false;
        }

        if set_before_source {
            push_set(&mut script);
        }

        let mut owner_done = false;
        match (&self.server, &target.server) {
            (from, Some(to)) if from.as_ref() != Some(to) => {
                if owner_changed {
                    script.append_block(&owner_script);
                    owner_done = true;
                }
                script.append_block(&format!("ALTER SUBSCRIPTION {name} SERVER {to};"));
            }
            (Some(_), None) => {
                script.append_block(&format!(
                    "ALTER SUBSCRIPTION {name} {};",
                    target.source_clause()
                ));
            }
            (None, None) if self.connection != target.connection => {
                script.append_block(&format!(
                    "ALTER SUBSCRIPTION {name} {};",
                    target.source_clause()
                ));
            }
            _ => {}
        }

        // Whether the SET PUBLICATION below leaves the refresh to a later
        // REFRESH PUBLICATION.
        let mut refresh_later = false;
        if publications_changed {
            if enabled_now && !single_transaction {
                script.append_block(&format!(
                    "ALTER SUBSCRIPTION {name} SET PUBLICATION {};",
                    target.publication_list()
                ));
            } else {
                script.append_block(&format!(
                    "ALTER SUBSCRIPTION {name} SET PUBLICATION {} WITH (refresh = false);",
                    target.publication_list()
                ));
                // Outside a single transaction a bounced subscription was
                // enabled before, so it is refreshed even when it ends
                // disabled, as its SET PUBLICATION would have refreshed it —
                // unless it ends without a slot, which cannot be enabled.
                refresh_later = end_enabled
                    || (bounce && !single_transaction && *end_slot != SubscriptionSlot::NoSlot);
            }
        }

        if !set_before_source {
            push_set(&mut script);
        }

        if refresh_later && !end_enabled {
            script.append_block(&format!("ALTER SUBSCRIPTION {name} ENABLE;"));
            script.append_block(&format!("ALTER SUBSCRIPTION {name} REFRESH PUBLICATION;"));
            script.append_block(&format!("ALTER SUBSCRIPTION {name} DISABLE;"));
        } else {
            if enabled_now != end_enabled {
                let action = if end_enabled { "ENABLE" } else { "DISABLE" };
                script.append_block(&format!("ALTER SUBSCRIPTION {name} {action};"));
            }

            if refresh_later {
                let refresh = format!("ALTER SUBSCRIPTION {name} REFRESH PUBLICATION;");
                if single_transaction {
                    post_commit.append_block(&refresh);
                } else {
                    script.append_block(&refresh);
                }
            }
        }

        if owner_changed && !owner_done {
            script.append_block(&owner_script);
        }

        if self.comment != target.comment {
            match &target.comment {
                Some(c) => {
                    script.append_block(&format!(
                        "COMMENT ON SUBSCRIPTION {} IS '{}';",
                        name,
                        c.replace('\'', "''")
                    ));
                }
                None if use_drop => {
                    script.append_block(&format!("COMMENT ON SUBSCRIPTION {name} IS NULL;"));
                }
                _ => {}
            }
        }

        SubscriptionAlterScript {
            script,
            post_commit,
        }
    }
}

/// The statements [`Subscription::get_alter_script_with`] emits, split by
/// where they run.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SubscriptionAlterScript {
    /// The `ALTER SUBSCRIPTION` statements, in order, at the subscription's
    /// place in the migration.
    pub script: String,
    /// `ALTER SUBSCRIPTION ... REFRESH PUBLICATION`, which cannot run inside a
    /// transaction block: set only for a single-transaction migration, which
    /// emits it after `commit;`. Empty otherwise.
    pub post_commit: String,
}

/// `skip_serializing_if` predicate for integer options whose default is `0`.
fn is_zero(value: &i32) -> bool {
    *value == 0
}

#[cfg(test)]
#[path = "tests/publication.rs"]
mod tests;
