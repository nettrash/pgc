//! Tables (`pg_class`) and everything attached to them.
//!
//! A table aggregates the pieces held by its sibling modules — columns,
//! constraints, indexes, triggers and policies — and hashes them together, so any
//! change below the table surfaces as a change to the table.
//!
//! Partitioning drives most of the ordering rules here: a parent must be created
//! before its partitions and dropped after them, multi-level hierarchies are
//! ordered by depth, and an index on a partitioned parent expands to `ON ONLY`
//! plus a per-partition `ATTACH PARTITION` under `--output-for-production`.
//!
//! [`PgCatalogCaps`] records which catalog columns the connected server actually
//! has, so one binary can introspect PostgreSQL 14 through 19.
//!
//! [`TableAlterCaps`] is the other direction: which in-place ALTER forms the
//! server being *migrated* accepts, so the alter path can prefer them over a
//! drop + re-create. The comparer derives it from the FROM dump.

use crate::{
    dump::{
        table_column::{GeneratedAlterCaps, TableColumn},
        table_constraint::{ConstraintAlterCaps, TableConstraint},
        table_index::TableIndex,
        table_policy::TablePolicy,
        table_trigger::TableTrigger,
    },
    utils::string_extensions::{
        StringExt, block_comment_safe, comment_out, comment_out_keep_blank_lines,
    },
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::{Error, PgPool, Row};
use std::collections::{HashMap, HashSet};

/// Triggers keyed by `(raw_schema, raw_table_name)`, as the bulk fetch
/// distributes them.
type TriggersByTable = HashMap<(String, String), Vec<TableTrigger>>;

/// One partition clone of a parent's row trigger as the bulk fetch reads it,
/// before `Table::fill_all` decides whether the dump records it (see
/// `Table::recorded_trigger_clones`).
#[derive(Debug, Clone)]
pub(crate) struct TriggerCloneRow {
    /// The clone, on its partition.
    pub(crate) trigger: TableTrigger,
    /// Whether its enabled state or comment differs from the trigger it was
    /// cloned from.
    pub(crate) own_state: bool,
    /// `(raw_schema, raw_table_name)` of the table holding the trigger it was
    /// cloned from: the partition's parent.
    pub(crate) parent: (String, String),
}

/// [`TriggerCloneRow`]s keyed by their partition's
/// `(raw_schema, raw_table_name)`.
type TriggerClonesByTable = HashMap<(String, String), Vec<TriggerCloneRow>>;

/// What `Table::fetch_constraints_bulk` reads, keyed by
/// `(raw_schema, raw_table_name)`: the constraints, and the names that go
/// into [`Table::enforced_inherited_checks`].
type ConstraintsByTable = (
    HashMap<(String, String), Vec<TableConstraint>>,
    HashMap<(String, String), Vec<String>>,
);

fn escape_single_quotes(value: &str) -> String {
    value.replace('\'', "''")
}

fn quote_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

/// The key column list of a `PRIMARY KEY (...)` constraint definition as
/// `pg_get_constraintdef` renders it — the text between the first `(` and the
/// `)` that closes it — and what follows that `)` (`INCLUDE (...)`,
/// `DEFERRABLE ...`), trimmed. Quote-aware, so a `(`, `)` or `,` inside a
/// quoted column name (`"Code (x)"`, `"a,b"`) is part of the name. `None`
/// when the definition holds no complete list.
fn primary_key_column_list(definition: &str) -> Option<(&str, &str)> {
    let bytes = definition.as_bytes();
    let open = definition.find('(')?;
    let mut depth = 0usize;
    let mut in_quotes = false;
    for (i, &b) in bytes.iter().enumerate().skip(open) {
        match b {
            // A doubled `"` inside a quoted name toggles twice, so it stays
            // inside the name.
            b'"' => in_quotes = !in_quotes,
            b'(' if !in_quotes => depth += 1,
            b')' if !in_quotes => {
                depth -= 1;
                if depth == 0 {
                    return Some((&definition[open + 1..i], definition[i + 1..].trim()));
                }
            }
            _ => {}
        }
    }
    None
}

/// `list` split at the commas outside double quotes and parentheses, each
/// item trimmed.
fn split_key_columns(list: &str) -> Vec<&str> {
    let mut items = Vec::new();
    let mut depth = 0usize;
    let mut in_quotes = false;
    let mut start = 0;
    for (i, b) in list.bytes().enumerate() {
        match b {
            b'"' => in_quotes = !in_quotes,
            b'(' if !in_quotes => depth += 1,
            b')' if !in_quotes => depth = depth.saturating_sub(1),
            b',' if !in_quotes && depth == 0 => {
                items.push(list[start..i].trim());
                start = i + 1;
            }
            _ => {}
        }
    }
    items.push(list[start..].trim());
    items
}

/// Whether `item` is one identifier as `pg_get_constraintdef` renders it:
/// a quoted name (with its doubled quotes), or a bare one, which PostgreSQL
/// only leaves unquoted when it is lowercase letters, digits and `_`.
fn is_single_identifier(item: &str) -> bool {
    if let Some(inner) = item.strip_prefix('"').and_then(|r| r.strip_suffix('"')) {
        // Every `"` inside must be one of a doubled pair.
        return !inner.replace("\"\"", "").contains('"');
    }
    !item.is_empty()
        && item
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_' || b == b'$')
}

/// The column of a NOT NULL constraint as its definition spells it
/// (`NOT NULL code`, `NOT NULL "Code" NOT VALID`): the `quote_ident` form the
/// dump uses for column names. `None` when the definition does not parse.
fn not_null_constraint_column(constraint: &TableConstraint) -> Option<&str> {
    let rest = TableConstraint::strip_not_null_prefix(constraint.definition.as_deref()?)?;
    if !rest.starts_with('"') {
        return rest.split_whitespace().next();
    }
    // Up to the closing quote; a doubled quote is one escaped quote inside.
    let bytes = rest.as_bytes();
    let mut i = 1;
    while i < bytes.len() {
        if bytes[i] == b'"' {
            if bytes.get(i + 1) == Some(&b'"') {
                i += 2;
                continue;
            }
            return Some(&rest[..=i]);
        }
        i += 1;
    }
    None
}

/// The column of a NOT NULL constraint, normalized as
/// `TableConstraint::normalize_identifier_for_name` does (a quoted name
/// keeps its case, an unquoted one is lowercased); `None` for any other
/// constraint, and when the definition does not parse.
fn not_null_key(constraint: &TableConstraint) -> Option<String> {
    if !constraint.constraint_type.eq_ignore_ascii_case("not null") {
        return None;
    }
    not_null_constraint_column(constraint).map(TableConstraint::normalize_identifier_for_name)
}

/// Extract column identifiers from a `pg_get_partkeydef` output string.
///
/// The input format is `method (ident1, ident2, ...)` where `method` is one of
/// `range`, `list`, or `hash`.  Identifiers may be double-quoted (PostgreSQL
/// style) and unquoted identifiers may contain `$` in addition to alphanumeric
/// characters and underscores.
///
/// Returns lowercased identifier names (with quotes stripped for quoted idents).
fn extract_partition_key_identifiers(pk: &str) -> Vec<String> {
    // Find the content inside the outermost parentheses, skipping the method keyword.
    let start = match pk.find('(') {
        Some(i) => i + 1,
        None => return Vec::new(),
    };
    let end = match pk.rfind(')') {
        Some(i) => i,
        None => pk.len(),
    };
    if start >= end {
        return Vec::new();
    }
    let inner = &pk[start..end];

    let mut identifiers = Vec::new();
    for part in inner.split(',') {
        let trimmed = part.trim();
        if trimmed.is_empty() {
            continue;
        }
        if trimmed.starts_with('"') {
            // Quoted identifier: find matching close quote (doubled quotes "" are escapes).
            let bytes = trimmed.as_bytes();
            let mut i = 1; // skip opening quote
            let mut ident = String::new();
            while i < bytes.len() {
                if bytes[i] == b'"' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                        ident.push('"');
                        i += 2;
                    } else {
                        break; // closing quote
                    }
                } else {
                    ident.push(bytes[i] as char);
                    i += 1;
                }
            }
            identifiers.push(ident.to_lowercase());
        } else {
            // Unquoted identifier: take leading identifier chars (alphanumeric, _, $).
            let ident: String = trimmed
                .chars()
                .take_while(|c| c.is_alphanumeric() || *c == '_' || *c == '$')
                .collect();
            if !ident.is_empty() {
                identifiers.push(ident.to_lowercase());
            }
        }
    }
    identifiers
}

/// Pre-computed catalog capability flags.
///
/// These booleans indicate whether certain columns / features exist in the
/// connected PostgreSQL server's system catalogs.  They are probed **once**
/// per dump run (via [`PgCatalogCaps::detect`]) and then threaded into every
/// per-table fetch function, avoiding a repeated `EXISTS` query per table.
#[derive(Debug, Clone, Copy)]
pub struct PgCatalogCaps {
    /// pg_attribute.attcompression exists (PG 14+)
    pub has_attcompression: bool,
    /// pg_constraint.conenforced exists (PG 18+)
    pub has_conenforced: bool,
    /// The server records what a generated column's expression uses as
    /// dependencies of the column itself (PostgreSQL 14; 15 and later record
    /// them on the column's `pg_attrdef` row), which is what makes its
    /// `ALTER COLUMN ... TYPE` fail (see
    /// [`TableColumn::generation_blocks_retype`]).
    pub generation_deps_on_column: bool,
}

impl PgCatalogCaps {
    /// Probe the catalog once and return the capability flags.
    pub async fn detect(pool: &PgPool, pg_version: i32) -> Self {
        let has_attcompression = pg_version >= 140000
            && sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = 'pg_attribute'::regclass AND attname = 'attcompression' AND NOT attisdropped)",
            )
            .fetch_one(pool)
            .await
            .unwrap_or(false);

        let has_conenforced = pg_version >= 180000
            && sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = 'pg_constraint'::regclass AND attname = 'conenforced' AND NOT attisdropped)",
            )
            .fetch_one(pool)
            .await
            .unwrap_or(false);

        Self {
            has_attcompression,
            has_conenforced,
            generation_deps_on_column: pg_version < 150000,
        }
    }
}

/// In-place ALTER forms the migrated server (the FROM database) accepts for
/// one table, threaded through [`Table::get_alter_script_with_caps`] and its
/// siblings. The [`Default`] is the form every supported server accepts,
/// which is also what the capability-less `get_alter_script*` methods use.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TableAlterCaps {
    /// Constraint changes (CHECK enforceability).
    pub constraint: ConstraintAlterCaps,
    /// Generated-column expression changes (`SET EXPRESSION`).
    pub generated: GeneratedAlterCaps,
}

/// How a classical-inheritance (`INHERITS`) family shares one generated
/// column's drop + add, worked out by the comparer across tables and handed
/// to each member's alter.
///
/// A parent's `DROP COLUMN` recurses: it drops a child's copy that is only
/// inherited and leaves a merged (also local) copy behind as a plain local
/// column. Its `ADD COLUMN` recurses too, and merges into any copy a child
/// still has, keeping that copy's old definition. So a child that follows
/// the parent has to lose whatever copy it still has *between* the two —
/// `DROP COLUMN IF EXISTS` covers both cases — and must not drop and re-add
/// the column itself afterwards: by then it is inherited again, and
/// PostgreSQL refuses ("cannot drop inherited column").
///
/// It also carries `rewritten_by_ancestor`, the other thing a member's alter
/// has to know about the rest of its family (partitions included).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct InheritedRecreates {
    /// Column name → the descendants (`schema.name`, parents first) whose
    /// copy this table's recursing `ADD COLUMN` re-creates; each is dropped
    /// with `DROP COLUMN IF EXISTS` between this table's drop and add.
    pub(crate) descendant_drops: HashMap<String, Vec<String>>,
    /// Columns an ancestor's drop + add re-creates for this table, which
    /// therefore emits neither the drop nor the add for them.
    pub(crate) recreated_by_ancestor: HashSet<String>,
    /// An ancestor's alter, which runs before this table's, rewrites this
    /// table's rows: a partitioned parent's recursing `ALTER COLUMN ... TYPE`
    /// or `SET EXPRESSION`, or an inheritance parent's `ALTER COLUMN ...
    /// TYPE` of a column this table inherits. The rewrite checks every row
    /// against this table's enforced CHECK constraints on the column, so the
    /// local ones this alter relaxes (`NOT ENFORCED` in place) or drops have
    /// to let go before it: this alter leaves them out, and the comparer
    /// runs them ahead of the table alters (see
    /// [`Table::ancestor_rewrite_pre_script`]). Inherited copies follow
    /// their parent's own statement, which runs before its rewrite.
    pub(crate) rewritten_by_ancestor: bool,
    /// CHECK constraints of this table, by name, that its inheritance
    /// family drops and re-adds together: a merged child copy (local, and
    /// inherited too — `coninhcount > 0`) the alter drops, and the copy of
    /// every ancestor it inherits it from. PostgreSQL refuses to drop a
    /// copy that is still inherited ("cannot drop inherited constraint"),
    /// and an ancestor's recursing re-add merges into a copy the child still
    /// has ("already exists", or "conflicts with NOT ENFORCED constraint").
    /// So every member's drop runs in the comparer's pass ahead of the table
    /// alters, parents first (see [`Table::ancestor_rewrite_pre_script`]),
    /// and each member re-adds its TO copy in its own alter — an ancestor
    /// whose copy does not change included; the order of the re-adds does
    /// not matter. Never an in-place `ALTER CONSTRAINT`.
    pub(crate) family_checks: HashSet<String>,
    /// Columns this table inherits (`INHERITS`) from an ancestor whose own
    /// alter retypes them in place: that `ALTER COLUMN ... TYPE` recurses to
    /// this table, which PostgreSQL forbids to retype an inherited column
    /// itself ("cannot alter inherited column"). So this alter leaves their
    /// type alone, and whatever stands in the way of the ancestor's retype
    /// here — a generated column of this table reading one of them (see
    /// [`Table::generated_columns_in_the_way_of_retype`]) — is dropped, or
    /// loses its expression, ahead of the table alters (see
    /// [`Table::ancestor_retype_pre_script`]); a re-created one is added
    /// back in this table's alter.
    pub(crate) retyped_by_ancestor: HashSet<String>,
    /// Foreign keys of this table, by name, that the comparer drops ahead
    /// of the table alters because they reference a column another table's
    /// alter drops and re-adds (see `Comparer::column_drop_blockers`).
    /// A drop this alter emits for one of them says `IF EXISTS`.
    pub(crate) dropped_ahead: HashSet<String>,
    /// This table's own triggers, by name, that its alter drops before it
    /// attaches the table to a partitioned parent (see
    /// [`Table::attaches_to_another_parent`]): those TO no longer has that
    /// share a name with a row trigger of the new parent or one of its
    /// ancestors, which the attach clones onto the table. The trigger
    /// statements after every table leave them out
    /// ([`Table::get_trigger_alter_script_in_family`]).
    pub(crate) triggers_dropped_before_attach: HashSet<String>,
    /// The CHECK constraints this table inherits once its alter attaches it
    /// to a partitioned parent (see [`Table::partition_moves`]), or joins
    /// new `INHERITS` parents (see [`Table::inheritance_moves`]): as this
    /// table's copy of it (see `TableConstraint::inherited_copy`) —
    /// enforced when TO's copy here is — every CHECK the TO parent and its
    /// ancestors hold, and of the new `INHERITS` ancestors' those TO lists
    /// on this table too, as a merged copy (the rest are
    /// [`InheritedRecreates::inherit_readds`]). The dump never shows a
    /// copy a table only inherits, so the comparer works them out. The
    /// alter gets each in shape before the attach and never drops one the
    /// attach merges; the comparer alters such a table after every other,
    /// so its new parents are in their TO shape by then.
    pub(crate) attach_checks: Vec<TableConstraint>,
    /// The CHECK constraints this table inherits from the FROM parents its
    /// alter leaves — its partitioned parent, which it detaches from, and
    /// the `INHERITS` parents it says `NO INHERIT` to — and their
    /// ancestors, as its own copies (see `TableConstraint::inherited_copy`,
    /// with `coninhcount` 0), leaving out those a parent it keeps still
    /// passes down. PostgreSQL leaves them behind as local constraints,
    /// which the dump — showing only local ones — never saw on the table.
    /// The alter diffs them like the table's own from the detach on: TO
    /// keeps, changes or drops each, and a new parent's attach merges it.
    pub(crate) detached_checks: Vec<TableConstraint>,
    /// The TO CHECK constraints, parents first, of the new `INHERITS`
    /// ancestors (see [`Table::inheritance_moves`]) that hold a CHECK TO
    /// has this table only inherit. `INHERIT` needs the table to hold a
    /// matching copy already and then leaves it local, a merged copy a
    /// later compare would turn into an inherited one with a drop + re-add
    /// across the whole family. So the alter drops each of these before its
    /// `INHERIT`, parents first — and the table's own copy, which TO does
    /// not list — and adds them back after it, children first, which
    /// recurses into the table and leaves its copy inherited only, as TO
    /// has it. Planned only with `use_drop`; without it such a CHECK is one
    /// of [`InheritedRecreates::attach_checks`].
    pub(crate) inherit_readds: Vec<TableConstraint>,
}

/// A NOT NULL constraint a table's alter renames (see
/// `Table::not_null_renames`): `from` and `to` in the dump's `quote_ident`
/// form, and the column it is on, normalized as `not_null_key` gives it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct NotNullRename {
    from: String,
    to: String,
    column: String,
}

/// What `Table::build_alter_script_parts` produces.
struct AlterScriptParts {
    /// The alter script.
    script: String,
    /// The `REPLICA IDENTITY USING INDEX` left to the comparer (see
    /// `replica_identity_waits_for_comparer`).
    replica_identity_after: Option<String>,
    /// The local CHECK relaxes and drops left to the comparer's pass ahead of
    /// the table alters (see `InheritedRecreates::rewritten_by_ancestor`).
    ancestor_pre_script: String,
    /// The generated columns taken out of the way of an ancestor's retype,
    /// left to the comparer's pass after it (see
    /// `InheritedRecreates::retyped_by_ancestor`).
    ancestor_retype_pre_script: String,
}

/// This is an information about a PostgreSQL table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Table {
    pub schema: String,
    pub name: String,
    pub raw_schema: String,
    pub raw_name: String,
    /// Owner of the table
    pub owner: String,
    /// Tablespace of the table
    pub space: Option<String>,
    /// Whether the table has indexes
    pub has_indexes: bool,
    /// Whether the table has triggers
    pub has_triggers: bool,
    /// Whether the table has rules
    pub has_rules: bool,
    /// Whether the table has row security
    pub has_rowsecurity: bool,
    /// Column names
    pub columns: Vec<TableColumn>,
    /// Constraint names
    pub constraints: Vec<TableConstraint>,
    /// The CHECK constraints, by name, that this table only inherits
    /// (`pg_constraint.conislocal` false, so [`Table::constraints`] leaves
    /// them out) and whose copy here is ENFORCED although no parent's copy
    /// is. PostgreSQL 18 reaches that state by attaching a table with an
    /// enforced copy under a `NOT ENFORCED` parent, and 19 also by
    /// `ALTER TABLE ... ALTER CONSTRAINT ... ENFORCED` on the inheriting
    /// table. Any other inherited copy follows its parents — ENFORCED as
    /// soon as one of them is — so this is the one state of such a copy the
    /// dump has to record.
    ///
    /// Empty before PostgreSQL 18, which has no `NOT ENFORCED` CHECK, and
    /// in a dump written before pgc 1.1.0. Hashed only when non-empty, so
    /// every other table keeps its hash. The comparer, not the table's
    /// alter, puts a change to it in place, since the parents' statements
    /// reset such a copy (see `Comparer::inherited_check_enforcement`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub enforced_inherited_checks: Vec<String>,
    /// Index names
    pub indexes: Vec<TableIndex>,
    /// The table's own triggers. A partition's clones of its partitioned
    /// parent's row triggers are not among them (see
    /// [`Table::trigger_clones`]).
    pub triggers: Vec<TableTrigger>,
    /// The clones of a partitioned parent's row triggers on this partition
    /// (`pg_trigger.tgparentid <> 0`) whose state differs from the parent
    /// trigger's: enabled in another mode (`ALTER TABLE <partition> DISABLE
    /// TRIGGER ...`), or carrying a comment. A clone in the parent trigger's
    /// state is left out, as `pg_dump` leaves it out — unless the parent is
    /// not in the dump (it lives in a schema the dump does not cover): then
    /// nothing tells that state, and every clone of the partition is
    /// recorded.
    ///
    /// PostgreSQL creates a clone with the parent's trigger — on every
    /// partition, whenever the trigger or a partition is created or attached
    /// — in the parent's enabled state and without a comment, and drops it
    /// with the parent's trigger; a clone cannot be created or dropped on
    /// its own. So the comparer never creates or drops one: it only sets the
    /// state recorded here once the parent's trigger is in place.
    ///
    /// Hashed only when non-empty, so a table without such a clone keeps its
    /// hash. PostgreSQL 15 and later mark a clone `tgisinternal = false`, and
    /// pgc 1.0.x listed every clone there among [`Table::triggers`];
    /// [`Dump::read_from_file`](crate::dump::core::Dump::read_from_file)
    /// moves them here — except on a partition whose parent is not in the
    /// dump, where only a current dump of the same partition tells them
    /// apart, and the comparer moves them (see
    /// `Comparer::separate_legacy_orphan_trigger_clones`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub trigger_clones: Vec<TableTrigger>,
    /// Row-level security policies
    #[serde(default)]
    pub policies: Vec<TablePolicy>,
    /// Table definition (optional)
    pub definition: Option<String>,
    /// Partition key (PARTITION BY ...)
    pub partition_key: Option<String>,
    /// Parent table (PARTITION OF ...)
    pub partition_of: Option<String>,
    /// Partition bound (FOR VALUES ... or DEFAULT)
    pub partition_bound: Option<String>,
    /// Table comment
    #[serde(default)]
    pub comment: Option<String>,
    /// Hash of the table
    pub hash: Option<String>,
    /// ACL (grant) entries for this table
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub acl: Vec<String>,
    /// Table access method (e.g., "heap", custom AM)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub access_method: Option<String>,
    /// Whether the table is UNLOGGED (relpersistence = 'u')
    #[serde(default)]
    pub is_unlogged: bool,
    /// Table-level storage parameters (reloptions)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_parameters: Option<Vec<String>>,
    /// REPLICA IDENTITY setting (d=default, n=nothing, f=full, i=index).
    ///
    /// A table left on `USING INDEX` after that index was dropped
    /// (`relreplident = 'i'` with no `indisreplident` index) is dumped as `n`:
    /// the server treats that state as NOTHING.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replica_identity: Option<String>,
    /// Index behind `REPLICA IDENTITY USING INDEX` — the table's one index
    /// with `pg_index.indisreplident` — in `quote_ident` form. Set only when
    /// `replica_identity` is `i`; a dump written before pgc 1.1.0 did not
    /// record it, and there `i` comes with `None`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replica_identity_index: Option<String>,
    /// Columns of [`Table::replica_identity_index`] (`pg_index.indkey`: key
    /// columns, then any `INCLUDE` columns), in `quote_ident` form. The alter
    /// path uses them to tell whether dropping or changing a column can take
    /// the index, or a NOT NULL it needs, away. Not hashed: they follow from
    /// the index, whose definition is. Empty when there is no index, and in
    /// a dump written before pgc 1.1.0; empty with a named index means
    /// unknown, and then every column counts.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub replica_identity_columns: Vec<String>,
    /// Whether FORCE ROW LEVEL SECURITY is enabled
    #[serde(default)]
    pub force_rowsecurity: bool,
    /// Classical inheritance parents (non-partition)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub inherits_from: Vec<String>,
    /// OF type name for typed tables
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub typed_table_type: Option<String>,
}

/// Structured result of [`Table::index_alter_plan`]: which indexes to create,
/// drop, or merely re-comment when altering a table. Borrows from both the FROM
/// and TO tables. Consumed by the production output path.
#[derive(Debug, Default)]
pub struct IndexAlterPlan<'a> {
    /// Indexes to (re)create — brand-new, or whose definition changed.
    pub create: Vec<&'a TableIndex>,
    /// Old indexes to drop — definition changed (paired with a `create`) or
    /// removed entirely.
    pub drop: Vec<&'a TableIndex>,
    /// Indexes whose comment changed but whose definition did not.
    pub comment_changes: Vec<&'a TableIndex>,
}

/// The two statements an ALTER of a table uses to change its replica
/// identity, from `Table::replica_identity_phases`.
///
/// `USING INDEX` pins an index and the NOT NULLs on its key columns, so it
/// is set last and left first. `DROP NOT NULL` on a replica-identity key
/// column fails on every supported version ("column is in index used as
/// replica identity"), and dropping the index itself leaves the table on
/// `USING INDEX` with no index, which the server treats as NOTHING. In the
/// other direction the index has to exist before `USING INDEX` names it,
/// and PostgreSQL 19 (and 18.7) also refuse it while a key column's NOT
/// NULL is `NOT VALID`.
///
/// Leaving is not free: with `USE_SINGLE_TRANSACTION=false`, or when the
/// return waits for an index built after `COMMIT`, a table without a primary
/// key has no replica identity in between, and UPDATE / DELETE on it fail
/// while it is published. So the table leaves early only when it has to.
#[derive(Debug, Default, PartialEq, Eq)]
struct ReplicaIdentityPhases {
    /// Runs before anything else in the alter: moves a table off its old
    /// `USING INDEX`, to the target setting or, when the target is an index
    /// (the same one about to be rebuilt, or another one while the old one
    /// is disturbed), to DEFAULT. Only when the target is not an index, when
    /// the alter can take the old index or a NOT NULL on one of its key
    /// columns away, or when the old index is unknown (a dump written before
    /// pgc 1.1.0). A switch between two indexes otherwise needs no early
    /// statement: the late `USING INDEX` moves the table in one step.
    early: Option<String>,
    /// Runs after the columns, constraints and indexes: the new setting.
    late: Option<String>,
}

impl Table {
    /// Creates a new Table with the given name
    #[allow(clippy::too_many_arguments)] // Table metadata naturally includes these fields (from pg_class and related catalogs).
    pub fn new(
        schema: String,
        name: String,
        raw_schema: String,
        raw_name: String,
        owner: String,
        space: Option<String>,
        columns: Vec<TableColumn>,
        constraints: Vec<TableConstraint>,
        indexes: Vec<TableIndex>,
        triggers: Vec<TableTrigger>,
        definition: Option<String>,
    ) -> Self {
        let mut table = Self {
            schema,
            name,
            raw_schema,
            raw_name,
            owner,
            space,
            has_indexes: !indexes.is_empty(),
            has_triggers: !triggers.is_empty(),
            has_rules: false,
            has_rowsecurity: false,
            columns,
            constraints,
            enforced_inherited_checks: Vec::new(),
            indexes,
            triggers,
            trigger_clones: Vec::new(),
            policies: Vec::new(),
            definition,
            partition_key: None,
            partition_of: None,
            partition_bound: None,
            comment: None,
            hash: None,
            acl: Vec::new(),
            access_method: None,
            is_unlogged: false,
            storage_parameters: None,
            replica_identity: None,
            replica_identity_index: None,
            replica_identity_columns: Vec::new(),
            force_rowsecurity: false,
            inherits_from: Vec::new(),
            typed_table_type: None,
        };
        table.hash();
        table
    }
    /// Fill metadata for every table in `tables` using **schema-wide** queries.
    ///
    /// Previously each table fired seven `WHERE schema='X' AND name='Y'` queries,
    /// yielding `7 × N` round-trips for `N` tables. This variant fires seven
    /// queries total (one per sub-resource), filtered by the same accessible-
    /// schema list that was used to collect the shell tables, and distributes
    /// rows into the matching `Table` in memory. On high-latency connections
    /// this reduces dump time dramatically.
    ///
    /// `has_tabledef_fn` and `caps` are pre-probed once per dump run so this
    /// function avoids redundant catalog checks.
    pub async fn fill_all(
        tables: &mut [Table],
        pool: &PgPool,
        has_tabledef_fn: bool,
        pg_version: i32,
        caps: PgCatalogCaps,
        schema_filter: &str,
    ) -> Result<(), Error> {
        if tables.is_empty() {
            return Ok(());
        }

        // (raw_schema, raw_name) -> index into `tables`
        let table_idx: HashMap<(String, String), usize> = tables
            .iter()
            .enumerate()
            .map(|(i, t)| ((t.raw_schema.clone(), t.raw_name.clone()), i))
            .collect();

        let (
            columns_by_key,
            indexes_by_key,
            (constraints_by_key, enforced_inherited_by_key),
            (triggers_by_key, clones_by_key),
            (policies_by_key, rowsecurity_by_key),
            partitions_by_key,
            definitions_by_key,
        ) = tokio::try_join!(
            Self::fetch_columns_bulk(pool, schema_filter, caps),
            Self::fetch_indexes_bulk(pool, schema_filter),
            Self::fetch_constraints_bulk(pool, schema_filter, pg_version, caps),
            Self::fetch_triggers_bulk(pool, schema_filter, pg_version),
            Self::fetch_policies_bulk(pool, schema_filter),
            Self::fetch_partition_info_bulk(pool, schema_filter),
            Self::fetch_definitions_bulk(pool, schema_filter, has_tabledef_fn),
        )?;

        for (key, mut cols) in columns_by_key {
            if let Some(&i) = table_idx.get(&key) {
                cols.sort_by_key(|a| a.ordinal_position);
                tables[i].columns = cols;
            }
        }
        for (key, mut idxs) in indexes_by_key {
            if let Some(&i) = table_idx.get(&key) {
                idxs.sort_by_key(|a| a.name.to_lowercase());
                tables[i].indexes = idxs;
            }
        }
        for (key, cons) in constraints_by_key {
            if let Some(&i) = table_idx.get(&key) {
                tables[i].constraints = cons;
            }
        }
        for (key, names) in enforced_inherited_by_key {
            if let Some(&i) = table_idx.get(&key) {
                tables[i].enforced_inherited_checks = names;
            }
        }
        for (key, mut trigs) in triggers_by_key {
            if let Some(&i) = table_idx.get(&key) {
                trigs.sort_by_key(|a| a.name.to_lowercase());
                tables[i].triggers = trigs;
            }
        }
        for (key, rows) in clones_by_key {
            if let Some(&i) = table_idx.get(&key) {
                let mut clones =
                    Self::recorded_trigger_clones(rows, |parent| table_idx.contains_key(parent));
                clones.sort_by_key(|a| a.name.to_lowercase());
                tables[i].trigger_clones = clones;
            }
        }
        for (key, pols) in policies_by_key {
            if let Some(&i) = table_idx.get(&key) {
                tables[i].policies = pols;
            }
        }
        for (key, rs) in rowsecurity_by_key {
            if let Some(&i) = table_idx.get(&key) {
                tables[i].has_rowsecurity = rs;
            }
        }
        for (key, (partition_key, partition_of, partition_bound)) in partitions_by_key {
            if let Some(&i) = table_idx.get(&key) {
                tables[i].partition_key = partition_key;
                tables[i].partition_of = partition_of;
                tables[i].partition_bound = partition_bound;
            }
        }
        for (key, def) in definitions_by_key {
            if let Some(&i) = table_idx.get(&key) {
                tables[i].definition = def;
            }
        }

        Ok(())
    }

    /// The `generation_blocks_retype` column of the columns query (see
    /// [`TableColumn::generation_blocks_retype`]): on a server that records a
    /// generated column's expression dependencies on the column itself
    /// (`caps.generation_deps_on_column`, PostgreSQL 14), whether any of them
    /// is one `ALTER COLUMN ... TYPE` refuses — anything but the column's
    /// own type and collation and the other columns of its table, the only
    /// ones PostgreSQL 14's check lets through. Empty on later servers, where
    /// the flag is always false.
    fn generation_blocks_retype_column(caps: PgCatalogCaps) -> &'static str {
        if !caps.generation_deps_on_column {
            return "";
        }
        ",
                CASE WHEN a.attgenerated <> '' THEN EXISTS (
                    SELECT 1 FROM pg_depend gd
                    WHERE gd.classid = 'pg_class'::regclass
                      AND gd.objid = a.attrelid
                      AND gd.objsubid = a.attnum
                      AND NOT (gd.refclassid = 'pg_type'::regclass AND gd.refobjid = a.atttypid)
                      AND NOT (gd.refclassid = 'pg_collation'::regclass AND gd.refobjid = a.attcollation)
                      AND NOT (gd.refclassid = 'pg_class'::regclass
                               AND gd.refobjid = a.attrelid
                               AND gd.refobjsubid <> 0)
                ) ELSE false END AS generation_blocks_retype"
    }

    /// The columns query. `compression_col` and `generation_col` are the
    /// version-dependent columns read after the column comment: the
    /// `attcompression` one, and the `generation_blocks_retype` one (see
    /// `Table::generation_blocks_retype_column`), each empty when the server
    /// has none.
    fn build_columns_query(
        compression_col: &str,
        generation_col: &str,
        schema_filter: &str,
    ) -> String {
        // Read column metadata directly from pg_catalog instead of
        // information_schema.columns. The standard view filters rows by
        // `pg_has_role(... ) OR has_column_privilege(...)`, so a dump taken
        // by a role without privileges on a table silently produces an empty
        // column list. pg_catalog views have no such filter, which keeps the
        // dump faithful regardless of the connecting role's grants.
        //
        // For the typmod-derived metrics (character/numeric/datetime
        // precisions, interval type) we still call the
        // `information_schema._pg_*` helper functions: they're plain SQL
        // helpers that the standard view itself uses internally, and unlike
        // the view they aren't privilege-filtered.
        //
        // SAFETY of hardcoded fields. The following columns below are
        // returned as constants rather than derived from pg_catalog:
        //
        //   - `interval_precision`, `character_set_catalog`,
        //     `character_set_schema`, `character_set_name`,
        //     `scope_catalog`, `scope_schema`, `scope_name`,
        //     `maximum_cardinality`, `dtd_identifier`  → all `NULL`
        //   - `is_self_referencing`                       → `'NO'`
        //   - `is_updatable`                              → `'YES'`
        //
        // None of these appear in `TableColumn::add_to_hasher`
        // (app/src/dump/table_column.rs ~L313–L353 — the explicit
        // "skip catalog/charset/related_views and other descriptive-only
        // fields" comment), and none are read by
        // `TableColumn::get_alter_script`. They participate in `PartialEq`
        // for round-trip dump equality only — a mismatch on any of them
        // produces zero ALTER SQL because no get_alter_script branch
        // emits a statement for them.
        //
        // Information_schema itself derived several of these (character_set_*,
        // scope_*, maximum_cardinality, dtd_identifier, is_self_referencing)
        // from the SQL standard's character-set / structured-type machinery
        // that PostgreSQL does not implement, so they were already constant
        // on every dump from the old query path; the new path returns the
        // same constants.
        //
        // `is_updatable` was per-row in the old query (info_schema's view
        // computes it from view privileges), but it isn't read anywhere in
        // pgc, so hardcoding 'YES' is safe. Keep this list in sync with
        // `add_to_hasher` if any of these fields ever start driving SQL
        // emission.
        format!(
            "SELECT
                current_database()::text as table_catalog,
                quote_ident(nc.nspname) as table_schema,
                quote_ident(c.relname) as table_name,
                nc.nspname as raw_table_schema,
                c.relname as raw_table_name,
                quote_ident(a.attname) as column_name,
                a.attnum::int4 as ordinal_position,
                CASE
                    WHEN a.attgenerated <> '' OR a.attidentity <> '' THEN NULL
                    ELSE pg_get_expr(ad.adbin, ad.adrelid)
                END as column_default,
                CASE
                    WHEN a.attnotnull OR (t.typtype = 'd' AND t.typnotnull) THEN 'NO'
                    ELSE 'YES'
                END as is_nullable,
                CASE
                    WHEN (
                        (t.typtype = 'd' AND bt.typelem <> 0 AND bt.typlen = -1)
                        OR (t.typtype <> 'd' AND t.typelem <> 0 AND t.typlen = -1)
                    )
                    THEN pg_catalog.format_type(a.atttypid, a.atttypmod)
                    WHEN (
                        (t.typtype = 'd' AND nbt.nspname IS NOT NULL AND nbt.nspname <> 'pg_catalog')
                        OR (t.typtype <> 'd' AND nt.nspname <> 'pg_catalog')
                    )
                    THEN pg_catalog.format_type(a.atttypid, a.atttypmod)
                    WHEN t.typtype = 'd' THEN pg_catalog.format_type(a.atttypid, a.atttypmod)
                    ELSE pg_catalog.format_type(a.atttypid, NULL)
                END AS formatted_data_type,
                information_schema._pg_char_max_length(
                    information_schema._pg_truetypid(a, t),
                    information_schema._pg_truetypmod(a, t)
                )::int4 as character_maximum_length,
                information_schema._pg_char_octet_length(
                    information_schema._pg_truetypid(a, t),
                    information_schema._pg_truetypmod(a, t)
                )::int4 as character_octet_length,
                information_schema._pg_numeric_precision(
                    information_schema._pg_truetypid(a, t),
                    information_schema._pg_truetypmod(a, t)
                )::int4 as numeric_precision,
                information_schema._pg_numeric_precision_radix(
                    information_schema._pg_truetypid(a, t),
                    information_schema._pg_truetypmod(a, t)
                )::int4 as numeric_precision_radix,
                information_schema._pg_numeric_scale(
                    information_schema._pg_truetypid(a, t),
                    information_schema._pg_truetypmod(a, t)
                )::int4 as numeric_scale,
                information_schema._pg_datetime_precision(
                    information_schema._pg_truetypid(a, t),
                    information_schema._pg_truetypmod(a, t)
                )::int4 as datetime_precision,
                information_schema._pg_interval_type(
                    information_schema._pg_truetypid(a, t),
                    information_schema._pg_truetypmod(a, t)
                ) as interval_type,
                NULL::int4 as interval_precision,
                NULL::text as character_set_catalog,
                NULL::text as character_set_schema,
                NULL::text as character_set_name,
                CASE
                    WHEN co.collname IS NOT NULL
                         AND NOT (nco.nspname = 'pg_catalog' AND co.collname = 'default')
                    THEN current_database()
                    ELSE NULL
                END as collation_catalog,
                CASE
                    WHEN co.collname IS NOT NULL
                         AND NOT (nco.nspname = 'pg_catalog' AND co.collname = 'default')
                    THEN nco.nspname
                    ELSE NULL
                END as collation_schema,
                CASE
                    WHEN co.collname IS NOT NULL
                         AND NOT (nco.nspname = 'pg_catalog' AND co.collname = 'default')
                    THEN co.collname
                    ELSE NULL
                END as collation_name,
                CASE WHEN t.typtype = 'd' THEN current_database() ELSE NULL END as domain_catalog,
                CASE WHEN t.typtype = 'd' THEN nt.nspname ELSE NULL END as domain_schema,
                CASE WHEN t.typtype = 'd' THEN t.typname ELSE NULL END as domain_name,
                current_database() as udt_catalog,
                COALESCE(nbt.nspname, nt.nspname) as udt_schema,
                COALESCE(bt.typname, t.typname) as udt_name,
                NULL::text as scope_catalog,
                NULL::text as scope_schema,
                NULL::text as scope_name,
                NULL::int4 as maximum_cardinality,
                NULL::text as dtd_identifier,
                'NO' as is_self_referencing,
                CASE WHEN a.attidentity IN ('a', 'd') THEN 'YES' ELSE 'NO' END as is_identity,
                CASE a.attidentity
                    WHEN 'a' THEN 'ALWAYS'
                    WHEN 'd' THEN 'BY DEFAULT'
                    ELSE NULL
                END as identity_generation,
                CASE WHEN a.attidentity IN ('a', 'd') THEN ps.seqstart::text ELSE NULL END as identity_start,
                CASE WHEN a.attidentity IN ('a', 'd') THEN ps.seqincrement::text ELSE NULL END as identity_increment,
                CASE WHEN a.attidentity IN ('a', 'd') THEN ps.seqmax::text ELSE NULL END as identity_maximum,
                CASE WHEN a.attidentity IN ('a', 'd') THEN ps.seqmin::text ELSE NULL END as identity_minimum,
                CASE
                    WHEN a.attidentity IN ('a', 'd')
                    THEN CASE WHEN ps.seqcycle THEN 'YES' ELSE 'NO' END
                    ELSE 'NO'
                END as identity_cycle,
                CASE WHEN a.attgenerated <> '' THEN 'ALWAYS' ELSE 'NEVER' END as is_generated,
                CASE WHEN a.attgenerated <> '' THEN pg_get_expr(ad.adbin, ad.adrelid) ELSE NULL END as generation_expression,
                a.attgenerated::text as attgenerated,
                a.attstorage::text as col_storage,
                a.attstattarget::int4 as col_stattarget,
                'YES' as is_updatable,
                pd.description as column_comment{compression_col}{generation_col},
                COALESCE(
                    (SELECT array_agg(acl_item::text) FROM unnest(a.attacl) as acl_item),
                    '{{}}'::text[]
                ) as col_acl,
                (
                    SELECT string_agg(DISTINCT quote_ident(vn.nspname) || '.' || quote_ident(vc.relname), ', ')
                    FROM pg_depend dep
                    JOIN pg_rewrite r
                        ON r.oid = dep.objid
                       AND dep.classid = 'pg_rewrite'::regclass
                    JOIN pg_class vc
                        ON vc.oid = r.ev_class
                       AND vc.relkind IN ('v', 'm')
                       AND vc.oid <> a.attrelid
                    JOIN pg_namespace vn ON vn.oid = vc.relnamespace
                    WHERE dep.refclassid = 'pg_class'::regclass
                      AND dep.refobjid = a.attrelid
                      AND dep.refobjsubid = a.attnum
                      AND dep.deptype = 'n'
                ) AS related_views
            FROM pg_attribute a
            JOIN pg_class c ON c.oid = a.attrelid
            JOIN pg_namespace nc ON nc.oid = c.relnamespace
            JOIN pg_type t ON t.oid = a.atttypid
            JOIN pg_namespace nt ON nt.oid = t.typnamespace
            LEFT JOIN pg_type bt ON t.typtype = 'd' AND bt.oid = t.typbasetype
            LEFT JOIN pg_namespace nbt ON nbt.oid = bt.typnamespace
            LEFT JOIN pg_attrdef ad ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum
            LEFT JOIN pg_collation co ON co.oid = a.attcollation
            LEFT JOIN pg_namespace nco ON nco.oid = co.collnamespace
            LEFT JOIN pg_depend ident_dep
                ON ident_dep.refclassid = 'pg_class'::regclass
               AND ident_dep.refobjid = a.attrelid
               AND ident_dep.refobjsubid = a.attnum
               AND ident_dep.classid = 'pg_class'::regclass
               AND ident_dep.deptype = 'i'
            LEFT JOIN pg_class seq ON seq.oid = ident_dep.objid AND seq.relkind = 'S'
            LEFT JOIN pg_sequence ps ON ps.seqrelid = seq.oid
            LEFT JOIN pg_description pd
                ON pd.objoid = c.oid
               AND pd.classoid = 'pg_class'::regclass
               AND pd.objsubid = a.attnum
            WHERE nc.nspname IN {schema_filter}
              AND a.attnum > 0
              AND NOT a.attisdropped
              AND c.relkind IN ('r','v','m','f','p')
            ORDER BY nc.nspname, c.relname, a.attnum"
        )
    }

    /// Fetch columns for every table in the accessible schemas in one query.
    async fn fetch_columns_bulk(
        pool: &PgPool,
        schema_filter: &str,
        caps: PgCatalogCaps,
    ) -> Result<HashMap<(String, String), Vec<TableColumn>>, Error> {
        let compression_col = if caps.has_attcompression {
            ",\n                                a.attcompression::text as col_compression"
        } else {
            ""
        };
        let query = Self::build_columns_query(
            compression_col,
            Self::generation_blocks_retype_column(caps),
            schema_filter,
        );
        let rows = sqlx::query(&query).fetch_all(pool).await?;

        let mut columns_by_key: HashMap<(String, String), Vec<TableColumn>> = HashMap::new();
        if !rows.is_empty() {
            for row in rows {
                let raw_schema: String = row.get("raw_table_schema");
                let raw_name: String = row.get("raw_table_name");
                let table_column = TableColumn {
                    catalog: row.get("table_catalog"),
                    schema: row.get("table_schema"),
                    table: row.get("table_name"),
                    name: row.get("column_name"),
                    ordinal_position: row.get("ordinal_position"),
                    column_default: row.get("column_default"),
                    is_nullable: row.get::<&str, _>("is_nullable") == "YES", // Convert to boolean
                    data_type: row.get("formatted_data_type"),
                    character_maximum_length: row.get("character_maximum_length"),
                    character_octet_length: row.get("character_octet_length"),
                    numeric_precision: row.get("numeric_precision"),
                    numeric_precision_radix: row.get("numeric_precision_radix"),
                    numeric_scale: row.get("numeric_scale"),
                    datetime_precision: row.get("datetime_precision"),
                    interval_type: row.get("interval_type"),
                    interval_precision: row.get("interval_precision"),
                    character_set_catalog: row.get("character_set_catalog"),
                    character_set_schema: row.get("character_set_schema"),
                    character_set_name: row.get("character_set_name"),
                    collation_catalog: row.get("collation_catalog"),
                    collation_schema: row.get("collation_schema"),
                    collation_name: row.get("collation_name"),
                    domain_catalog: row.get("domain_catalog"),
                    domain_schema: row.get("domain_schema"),
                    domain_name: row.get("domain_name"),
                    udt_catalog: row.get("udt_catalog"),
                    udt_schema: row.get("udt_schema"),
                    udt_name: row.get("udt_name"),
                    scope_catalog: row.get("scope_catalog"),
                    scope_schema: row.get("scope_schema"),
                    scope_name: row.get("scope_name"),
                    maximum_cardinality: row.get("maximum_cardinality"),
                    dtd_identifier: row.get("dtd_identifier"),
                    is_self_referencing: row.get::<&str, _>("is_self_referencing") == "YES", // Convert to boolean
                    is_identity: row.get::<&str, _>("is_identity") == "YES", // Convert to boolean
                    identity_generation: row.get("identity_generation"),
                    identity_start: row.get("identity_start"),
                    identity_increment: row.get("identity_increment"),
                    identity_maximum: row.get("identity_maximum"),
                    identity_minimum: row.get("identity_minimum"),
                    identity_cycle: row.get::<&str, _>("identity_cycle") == "YES", // Convert to boolean
                    is_generated: row.get("is_generated"),
                    generation_expression: row.get("generation_expression"),
                    generation_type: {
                        let ag: String = row.get("attgenerated");
                        if ag.is_empty() { None } else { Some(ag) }
                    },
                    is_updatable: row.get::<&str, _>("is_updatable") == "YES", // Convert to boolean
                    related_views: row.get::<Option<String>, _>("related_views").map(|s| {
                        let mut views: Vec<String> =
                            s.split(',').map(|v| v.trim().to_string()).collect();
                        views.sort_unstable();
                        views
                    }),
                    comment: row.get("column_comment"),
                    storage: {
                        let s: String = row.get("col_storage");
                        match s.as_str() {
                            "p" => Some("PLAIN".to_string()),
                            "e" => Some("EXTERNAL".to_string()),
                            "m" => Some("MAIN".to_string()),
                            "x" => Some("EXTENDED".to_string()),
                            _ => None,
                        }
                    },
                    compression: if caps.has_attcompression {
                        let c: String = row.get("col_compression");
                        match c.as_str() {
                            "p" => Some("pglz".to_string()),
                            "l" => Some("lz4".to_string()),
                            _ => None,
                        }
                    } else {
                        None
                    },
                    statistics_target: {
                        let st: Option<i32> = row.get("col_stattarget");
                        st.filter(|&v| v >= 0)
                    },
                    acl: row.get::<Vec<String>, _>("col_acl"),
                    serial_type: None,
                    generation_blocks_retype: caps.generation_deps_on_column
                        && row.get::<bool, _>("generation_blocks_retype"),
                    generation_routine_dropped: false,
                };

                columns_by_key
                    .entry((raw_schema, raw_name))
                    .or_default()
                    .push(table_column);
            }
        }

        Ok(columns_by_key)
    }

    /// Fetch indexes for every table in the accessible schemas in one query.
    async fn fetch_indexes_bulk(
        pool: &PgPool,
        schema_filter: &str,
    ) -> Result<HashMap<(String, String), Vec<TableIndex>>, Error> {
        let query = Self::build_indexes_bulk_query(schema_filter);
        let rows = sqlx::query(&query).fetch_all(pool).await?;

        let mut indexes_by_key: HashMap<(String, String), Vec<TableIndex>> = HashMap::new();
        for row in rows {
            let raw_schema: String = row.get("raw_schemaname");
            let raw_name: String = row.get("raw_tablename");
            let table_index = TableIndex {
                schema: row.get("schemaname"),
                table: row.get("tablename"),
                name: row.get("indexname"),
                catalog: row.get("tablespace"),
                indexdef: row.get("indexdef"),
                is_partition_index: row.get("is_partition_index"),
                comment: row.get("index_comment"),
            };
            indexes_by_key
                .entry((raw_schema, raw_name))
                .or_default()
                .push(table_index);
        }

        Ok(indexes_by_key)
    }

    fn build_indexes_bulk_query(schema_filter: &str) -> String {
        format!(
            "select
                quote_ident(i.schemaname) as schemaname,
                quote_ident(i.tablename) as tablename,
                i.schemaname as raw_schemaname,
                i.tablename as raw_tablename,
                quote_ident(i.indexname) as indexname,
                i.tablespace,
                i.indexdef,
                EXISTS (SELECT 1 FROM pg_inherits inh WHERE inh.inhrelid = ic.oid) AS is_partition_index,
                d.description as index_comment
            from
                pg_indexes i
                join pg_class ic on ic.relname = i.indexname
                join pg_namespace n on n.oid = ic.relnamespace and n.nspname = i.schemaname
                join pg_index idx on idx.indexrelid = ic.oid
                left join pg_constraint puc on puc.conindid = ic.oid and puc.contype in ('p', 'u')
                left join pg_description d
                    on d.objoid = ic.oid
                    and d.classoid = 'pg_class'::regclass
                    and d.objsubid = 0
            where
                idx.indisprimary = false
                and (idx.indisunique = false or puc.oid is null)
                and not exists (
                    select 1 from pg_constraint xc
                    where xc.conindid = ic.oid
                    and xc.contype = 'x'
                )
                and i.schemaname in {schema_filter}
            order by
                i.schemaname,
                i.tablename,
                i.indexname"
        )
    }

    /// The constraints query: every local constraint of every table in
    /// `schema_filter`.
    ///
    /// `caps.has_conenforced` (PostgreSQL 18+) adds `is_enforced`, and
    /// `pg_version` 18+ adds the named `NOT NULL` constraints (`contype = 'n'`)
    /// PostgreSQL 18 started storing. `UNIQUE NULLS NOT DISTINCT` (PostgreSQL
    /// 15+) needs no column of its own: the flag lives in
    /// `pg_index.indnullsnotdistinct` and `pg_get_constraintdef` prints it in
    /// the definition, which is how the comparer sees a change to it (see
    /// [`TableConstraint::nulls_not_distinct`]).
    fn build_constraints_query(
        schema_filter: &str,
        pg_version: i32,
        caps: PgCatalogCaps,
    ) -> String {
        let conenforced_col = if caps.has_conenforced {
            ",\n                c.conenforced AS is_enforced"
        } else {
            ""
        };
        // PG18 stores named NOT NULL constraints in pg_constraint as contype = 'n'.
        let contype_filter = if pg_version >= 180000 {
            "('p','u','f','c','x','n')"
        } else {
            "('p','u','f','c','x')"
        };
        format!(
            "SELECT
                current_database() AS catalog,
                quote_ident(n.nspname) AS schema,
                n.nspname AS raw_schema,
                t.relname AS raw_table_name,
                quote_ident(c.conname) AS constraint_name,
                quote_ident(t.relname) AS table_name,
                CASE c.contype
                    WHEN 'p' THEN 'PRIMARY KEY'
                    WHEN 'f' THEN 'FOREIGN KEY'
                    WHEN 'u' THEN 'UNIQUE'
                    WHEN 'c' THEN 'CHECK'
                    WHEN 'x' THEN 'EXCLUDE'
                    WHEN 'n' THEN 'NOT NULL'
                    ELSE c.contype::text
                END AS constraint_type,
                c.condeferrable AS is_deferrable,
                c.condeferred AS initially_deferred,
                pg_get_constraintdef(c.oid, true) AS definition,
                c.coninhcount::int4 AS coninhcount,\n                c.connoinherit AS no_inherit,\n                d.description AS constraint_comment{conenforced_col}
            FROM
                pg_constraint c
                JOIN pg_class t ON t.oid = c.conrelid
                JOIN pg_namespace n ON n.oid = t.relnamespace
                LEFT JOIN pg_description d ON d.objoid = c.oid AND d.classoid = 'pg_constraint'::regclass AND d.objsubid = 0
                WHERE
                    n.nspname IN {schema_filter} AND
                    c.contype IN {contype_filter} AND
                    c.conislocal
                ORDER BY
                    n.nspname,
                    t.relname,
                    c.conname;"
        )
    }

    /// The query behind [`Table::enforced_inherited_checks`]: every CHECK
    /// copy a table only inherits (`NOT conislocal`) that is ENFORCED while
    /// no copy of the same name on a direct parent (`pg_inherits`, which
    /// holds partitions too) is — wherever that parent lives, dumped or not.
    /// Only for a server with `pg_constraint.conenforced` (PostgreSQL 18+):
    /// before it every CHECK is enforced, and no copy can be one of these.
    fn build_enforced_inherited_checks_query(schema_filter: &str) -> String {
        format!(
            "SELECT
                n.nspname AS raw_schema,
                t.relname AS raw_table_name,
                quote_ident(c.conname) AS constraint_name
            FROM
                pg_constraint c
                JOIN pg_class t ON t.oid = c.conrelid
                JOIN pg_namespace n ON n.oid = t.relnamespace
                WHERE
                    n.nspname IN {schema_filter} AND
                    c.contype = 'c' AND
                    NOT c.conislocal AND
                    c.conenforced AND
                    NOT EXISTS (
                        SELECT 1
                        FROM pg_inherits i
                        JOIN pg_constraint pc ON pc.conrelid = i.inhparent
                        WHERE i.inhrelid = c.conrelid AND
                            pc.contype = 'c' AND
                            pc.conname = c.conname AND
                            pc.conenforced
                    )
                ORDER BY
                    n.nspname,
                    t.relname,
                    c.conname;"
        )
    }

    /// Fetch constraints for every table in the accessible schemas in one
    /// query, and, on a server with `conenforced`, the names that go into
    /// [`Table::enforced_inherited_checks`] in a second one, run after it
    /// on the same branch of `Table::fill_all`'s join.
    async fn fetch_constraints_bulk(
        pool: &PgPool,
        schema_filter: &str,
        pg_version: i32,
        caps: PgCatalogCaps,
    ) -> Result<ConstraintsByTable, Error> {
        let query = Self::build_constraints_query(schema_filter, pg_version, caps);

        let rows = sqlx::query(&query).fetch_all(pool).await?;

        let mut constraints_by_key: HashMap<(String, String), Vec<TableConstraint>> =
            HashMap::new();
        for row in rows {
            let raw_schema: String = row.get("raw_schema");
            let raw_name: String = row.get("raw_table_name");
            let constraint = TableConstraint {
                catalog: row.get("catalog"),
                schema: row.get("schema"),
                name: row.get("constraint_name"),
                table_name: row.get("table_name"),
                constraint_type: row.get("constraint_type"),
                is_deferrable: row.get("is_deferrable"),
                initially_deferred: row.get("initially_deferred"),
                definition: row.get("definition"),
                coninhcount: row.get("coninhcount"),
                is_enforced: row.try_get("is_enforced").unwrap_or(true),
                no_inherit: row.get("no_inherit"),
                nulls_not_distinct: false,
                comment: row.get("constraint_comment"),
            };
            constraints_by_key
                .entry((raw_schema, raw_name))
                .or_default()
                .push(constraint);
        }

        let mut enforced_inherited_by_key: HashMap<(String, String), Vec<String>> = HashMap::new();
        if caps.has_conenforced {
            let query = Self::build_enforced_inherited_checks_query(schema_filter);
            for row in sqlx::query(&query).fetch_all(pool).await? {
                enforced_inherited_by_key
                    .entry((row.get("raw_schema"), row.get("raw_table_name")))
                    .or_default()
                    .push(row.get("constraint_name"));
            }
        }

        Ok((constraints_by_key, enforced_inherited_by_key))
    }

    /// The query behind `fetch_triggers_bulk`: every trigger a table defines
    /// itself, plus the partition clones of a parent's row triggers, with
    /// `is_clone` telling the two apart.
    ///
    /// `pg_trigger.tgparentid` (PostgreSQL 13+) marks a clone of a
    /// partitioned parent's row trigger. PostgreSQL 13 and 14 make the clone
    /// `tgisinternal`, 15 and later do not, so a clone is told by
    /// `tgparentid` alone, on every version, and compared with the trigger
    /// it was cloned from (`u`) — the parent's own, or the parent's clone
    /// one level up: `clone_own_state` says whether its enabled state or
    /// comment is its own, and `parent_raw_schema` / `parent_raw_table_name`
    /// name the table `u` is on. Every clone is read, since whether the dump
    /// records it also depends on whether that table is dumped (see
    /// `Table::recorded_trigger_clones`). The clones of a foreign key's
    /// internal triggers are never read (their names carry OIDs); a
    /// constraint trigger's (`contype = 't'`) are. Before PostgreSQL 13
    /// every clone is internal, so none is read.
    fn build_triggers_bulk_query(schema_filter: &str, pg_version: i32) -> String {
        if pg_version < 130000 {
            return format!(
                "select
                t.oid,
                n.nspname as raw_schema,
                c.relname as raw_table_name,
                quote_ident(t.tgname) as tgname,
                pg_get_triggerdef(t.oid) as tgdef,
                t.tgenabled::text as tgenabled,
                d.description as tgcomment,
                false as is_clone
            from
                pg_trigger t
            join pg_class c on c.oid = t.tgrelid
            join pg_namespace n on n.oid = c.relnamespace
            left join pg_description d on d.objoid = t.oid and d.classoid = 'pg_trigger'::regclass
            where
                n.nspname IN {schema_filter} and
                t.tgisinternal = false
            order by
                n.nspname, c.relname, t.tgname"
            );
        }
        format!(
            "select
                t.oid,
                n.nspname as raw_schema,
                c.relname as raw_table_name,
                quote_ident(t.tgname) as tgname,
                pg_get_triggerdef(t.oid) as tgdef,
                t.tgenabled::text as tgenabled,
                d.description as tgcomment,
                t.tgparentid <> 0 as is_clone,
                coalesce(t.tgenabled <> u.tgenabled or d.description is not null, false)
                    as clone_own_state,
                un.nspname as parent_raw_schema,
                uc.relname as parent_raw_table_name
            from
                pg_trigger t
            join pg_class c on c.oid = t.tgrelid
            join pg_namespace n on n.oid = c.relnamespace
            left join pg_trigger u on u.oid = t.tgparentid
            left join pg_class uc on uc.oid = u.tgrelid
            left join pg_namespace un on un.oid = uc.relnamespace
            left join pg_description d on d.objoid = t.oid and d.classoid = 'pg_trigger'::regclass
            where
                n.nspname IN {schema_filter} and
                (
                    (t.tgparentid = 0 and t.tgisinternal = false)
                    or (
                        t.tgparentid <> 0
                        and not exists (
                            select 1 from pg_constraint k
                            where k.oid = t.tgconstraint and k.contype <> 't'
                        )
                    )
                )
            order by
                n.nspname, c.relname, t.tgname"
        )
    }

    /// Fetch triggers for every table in the accessible schemas in one query:
    /// each table's own triggers, and apart from them every partition clone
    /// of a parent's row trigger, which `fill_all` narrows down to the ones
    /// [`Table::trigger_clones`] records.
    async fn fetch_triggers_bulk(
        pool: &PgPool,
        schema_filter: &str,
        pg_version: i32,
    ) -> Result<(TriggersByTable, TriggerClonesByTable), Error> {
        let query = Self::build_triggers_bulk_query(schema_filter, pg_version);
        let rows = sqlx::query(&query).fetch_all(pool).await?;

        let mut triggers_by_key: TriggersByTable = HashMap::new();
        let mut clones_by_key: TriggerClonesByTable = HashMap::new();
        for row in rows {
            let raw_schema: String = row.get("raw_schema");
            let raw_name: String = row.get("raw_table_name");
            let trig = TableTrigger {
                oid: row.get("oid"),
                name: row.get("tgname"),
                definition: row.get("tgdef"),
                enabled: row.get("tgenabled"),
                comment: row.try_get("tgcomment").ok().flatten(),
            };
            if row.get::<bool, _>("is_clone") {
                clones_by_key
                    .entry((raw_schema, raw_name))
                    .or_default()
                    .push(TriggerCloneRow {
                        trigger: trig,
                        own_state: row.get("clone_own_state"),
                        parent: (
                            row.get::<Option<String>, _>("parent_raw_schema")
                                .unwrap_or_default(),
                            row.get::<Option<String>, _>("parent_raw_table_name")
                                .unwrap_or_default(),
                        ),
                    });
            } else {
                triggers_by_key
                    .entry((raw_schema, raw_name))
                    .or_default()
                    .push(trig);
            }
        }

        Ok((triggers_by_key, clones_by_key))
    }

    /// The clones of one partition that the dump records (see
    /// [`Table::trigger_clones`]), out of every clone `fetch_triggers_bulk`
    /// read for it: one whose enabled state or comment is its own, and every
    /// clone whose parent is not a dumped table (`dumped` answers for a
    /// `(raw_schema, raw_table_name)`). Only the parent's state tells a
    /// clone in that state apart, so without the parent every clone is
    /// recorded, and the comparer compares them with each other.
    pub(crate) fn recorded_trigger_clones(
        rows: Vec<TriggerCloneRow>,
        dumped: impl Fn(&(String, String)) -> bool,
    ) -> Vec<TableTrigger> {
        rows.into_iter()
            .filter(|row| row.own_state || !dumped(&row.parent))
            .map(|row| row.trigger)
            .collect()
    }

    /// Fetch row-level security policies and per-table rowsecurity flag in
    /// two schema-wide queries. Returns separate maps keyed by
    /// `(raw_schema, raw_table_name)`.
    #[allow(clippy::type_complexity)]
    async fn fetch_policies_bulk(
        pool: &PgPool,
        schema_filter: &str,
    ) -> Result<
        (
            HashMap<(String, String), Vec<TablePolicy>>,
            HashMap<(String, String), bool>,
        ),
        Error,
    > {
        let policies_query = format!(
            "SELECT
                    quote_ident(p.polname) as polname,
                    quote_ident(n.nspname) AS schemaname,
                    quote_ident(c.relname) AS tablename,
                    n.nspname AS raw_schema,
                    c.relname AS raw_table_name,
                    p.polcmd::text AS polcmd,
                    p.polpermissive,
                    array(SELECT rolname::text FROM pg_roles r WHERE r.oid = ANY(p.polroles) ORDER BY rolname) AS roles,
                    pg_get_expr(p.polqual, p.polrelid) AS using_clause,
                    pg_get_expr(p.polwithcheck, p.polrelid) AS check_clause
             FROM pg_policy p
             JOIN pg_class c ON c.oid = p.polrelid
             JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname IN {schema_filter}
             ORDER BY n.nspname, c.relname, p.polname"
        );

        // Separate query so we can populate rowsecurity for tables WITHOUT
        // policies too; matches the old per-table semantics.
        let rowsecurity_query = format!(
            "SELECT n.nspname AS raw_schema, c.relname AS raw_table_name, c.relrowsecurity
             FROM pg_class c
             JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname IN {schema_filter} AND c.relkind IN ('r','p')"
        );

        let (policy_rows, rowsecurity_rows) = tokio::try_join!(
            sqlx::query(&policies_query).fetch_all(pool),
            sqlx::query(&rowsecurity_query).fetch_all(pool),
        )?;

        let mut policies_by_key: HashMap<(String, String), Vec<TablePolicy>> = HashMap::new();
        for row in &policy_rows {
            let raw_schema: String = row.get("raw_schema");
            let raw_name: String = row.get("raw_table_name");
            policies_by_key
                .entry((raw_schema, raw_name))
                .or_default()
                .push(TablePolicy::from_row(row)?);
        }

        let mut rowsecurity_by_key: HashMap<(String, String), bool> = HashMap::new();
        for row in &rowsecurity_rows {
            let raw_schema: String = row.get("raw_schema");
            let raw_name: String = row.get("raw_table_name");
            let rs: bool = row.get("relrowsecurity");
            rowsecurity_by_key.insert((raw_schema, raw_name), rs);
        }

        Ok((policies_by_key, rowsecurity_by_key))
    }

    /// Fetch partition information for the given table.
    ///
    /// Returns a map from `(raw_schema, raw_table_name)` to
    /// `(partition_key, partition_of, partition_bound)`.
    #[allow(clippy::type_complexity)]
    async fn fetch_partition_info_bulk(
        pool: &PgPool,
        schema_filter: &str,
    ) -> Result<HashMap<(String, String), (Option<String>, Option<String>, Option<String>)>, Error>
    {
        // `pg_inherits` records BOTH declarative partition children
        // (parent.relkind = 'p') AND classical-inheritance children
        // (parent.relkind = 'r', child created with `INHERITS (...)`).
        // The `partition_of` field is only meaningful for partition
        // children — classical-inheritance parents are tracked
        // separately in `inherits_from`. Without the `p.relkind = 'p'`
        // guard, classical-inheritance children land with both fields
        // set, which (a) makes `build_script` emit invalid
        // `CREATE TABLE … PARTITION OF parent` for a non-partitioned
        // parent, and (b) trips `column_type_change_forces_recreate`
        // into a wholesale drop+recreate for column type changes
        // PostgreSQL would happily ALTER in place. That second effect
        // is what leaked a non-idempotent diff after migration:
        // recreated tables inherit current default privileges, and a
        // second `pgc compare` then emitted REVOKE statements for the
        // newly-attached grants.
        let query = format!(
            "SELECT
                n.nspname AS raw_schema,
                c.relname AS raw_table_name,
                c.relkind::text,
                pg_get_partkeydef(c.oid) AS partition_key,
                pg_get_expr(c.relpartbound, c.oid) AS partition_bound,
                quote_ident(p.relname) AS parent_table,
                quote_ident(pn.nspname) AS parent_schema
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            LEFT JOIN pg_inherits i ON i.inhrelid = c.oid
            LEFT JOIN pg_class p ON p.oid = i.inhparent AND p.relkind = 'p'
            LEFT JOIN pg_namespace pn ON pn.oid = p.relnamespace
            WHERE n.nspname IN {schema_filter} AND c.relkind IN ('r','p')"
        );

        let rows = sqlx::query(&query).fetch_all(pool).await?;

        let mut out: HashMap<(String, String), (Option<String>, Option<String>, Option<String>)> =
            HashMap::new();
        for row in rows {
            let raw_schema: String = row.get("raw_schema");
            let raw_name: String = row.get("raw_table_name");
            let relkind: String = row.get("relkind");

            let partition_key = if relkind == "p" {
                row.get("partition_key")
            } else {
                None
            };

            let (partition_of, partition_bound) = if let Some(parent_table) =
                row.get::<Option<String>, _>("parent_table")
                && let Some(parent_schema) = row.get::<Option<String>, _>("parent_schema")
            {
                (
                    Some(format!("{}.{}", parent_schema, parent_table)),
                    row.get("partition_bound"),
                )
            } else {
                (None, None)
            };

            out.insert(
                (raw_schema, raw_name),
                (partition_key, partition_of, partition_bound),
            );
        }
        Ok(out)
    }

    /// Fetch table definitions via `pg_get_tabledef` for all tables at once.
    /// Returns an empty map when the extension function is absent.
    async fn fetch_definitions_bulk(
        pool: &PgPool,
        schema_filter: &str,
        has_tabledef_fn: bool,
    ) -> Result<HashMap<(String, String), Option<String>>, Error> {
        if !has_tabledef_fn {
            return Ok(HashMap::new());
        }
        let query = format!(
            "select
                n.nspname as raw_schema,
                c.relname as raw_table_name,
                pg_get_tabledef(c.oid) AS definition
            from
                pg_class c
                join pg_namespace n on n.oid = c.relnamespace
            where
                n.nspname IN {schema_filter}
                and c.relkind in ('r','p')"
        );
        let rows = sqlx::query(&query).fetch_all(pool).await?;
        let mut out: HashMap<(String, String), Option<String>> = HashMap::new();
        for row in rows {
            let raw_schema: String = row.get("raw_schema");
            let raw_name: String = row.get("raw_table_name");
            let def: Option<String> = row.get("definition");
            out.insert((raw_schema, raw_name), def);
        }
        Ok(out)
    }

    /// The `[constraint <name>] primary key (...)` line of this table's
    /// `CREATE TABLE`, from the definition `pg_get_constraintdef` gave its
    /// primary key; `None` when it has none, or the definition holds no key
    /// column list.
    ///
    /// The key columns are taken from the definition quote-aware
    /// (`primary_key_column_list`), and each is spelled in double quotes, as
    /// this line has always spelled them: a quoted name is kept as it is and
    /// a bare one is quoted. A list that is not plain column names — a
    /// temporal key's `WITHOUT OVERLAPS` — is kept verbatim. What the
    /// definition holds after the list (`INCLUDE (...)`, `DEFERRABLE
    /// INITIALLY DEFERRED`) follows it verbatim; it used to be left out, so
    /// the table came out with a different key and the next compare dropped
    /// and re-added it.
    fn inline_primary_key_clause(&self) -> Option<String> {
        let pk = self
            .constraints
            .iter()
            .find(|c| c.constraint_type.eq_ignore_ascii_case("primary key"))?;
        let (list, tail) = primary_key_column_list(pk.definition.as_deref()?)?;
        let items = split_key_columns(list);
        let columns = if items.iter().all(|item| is_single_identifier(item)) {
            items
                .iter()
                .map(|item| {
                    if item.starts_with('"') {
                        (*item).to_string()
                    } else {
                        format!("\"{item}\"")
                    }
                })
                .collect::<Vec<_>>()
                .join(", ")
        } else {
            list.to_string()
        };
        let tail = if tail.is_empty() {
            String::new()
        } else {
            format!(" {tail}")
        };
        Some(if pk.name.is_empty() {
            format!("    primary key ({columns}){tail}")
        } else {
            format!("    constraint {} primary key ({columns}){tail}", pk.name)
        })
    }

    /// The column definitions and inline primary key of this table's plain
    /// `CREATE TABLE` (see [`Table::get_script`]), one element each.
    fn column_definitions(&self) -> Vec<String> {
        let mut column_definitions = Vec::new();
        for column in &self.columns {
            let mut col_def = String::new();

            // Column name
            col_def.push_str(&format!("    {} ", column.name));

            // Use standard column definition
            let col_script = column.get_script();
            // Extract just the type and constraints part. `get_script`
            // starts with the (quoted) name and one space; strip exactly
            // that, since a quoted name such as `"Col X"` can itself
            // contain spaces and cutting at the first one would leave an
            // unterminated identifier behind.
            if let Some(col_part) = col_script.strip_prefix(&format!("{} ", column.name)) {
                let mut col_part = col_part.to_string();

                // If there is a named NOT NULL constraint for this column,
                // replace the plain "not null" with "constraint <name> not null"
                // (plus any modifier flags like NO INHERIT / NOT ENFORCED).
                // A NOT NULL constraint is "named" if its name differs from
                // PG's auto-generated default "{table}_{col}_not_null" (or
                // that name with a numeric suffix that PG appends to resolve
                // cross-table collisions). Column names are compared
                // quote-aware (`TableConstraint::parse_pg_identifier`):
                // quoted identifiers preserve case, unquoted ones fold to
                // lowercase, as PG folds them.
                if col_part.ends_with("not null")
                    && let Some(nn_clause) = self.named_not_null_clause(&column.name)
                {
                    col_part.truncate(col_part.len() - "not null".len());
                    col_part.push_str(&nn_clause);
                }

                col_def.push_str(&col_part);
            }

            column_definitions.push(col_def);
        }

        // 4. Add primary key constraint if exists
        if let Some(pk_def) = self.inline_primary_key_clause() {
            column_definitions.push(pk_def);
        }
        column_definitions
    }

    /// The element list of this table's `CREATE TABLE … OF <type> ( … )`,
    /// for a typed table: PostgreSQL takes the columns — names, types and
    /// collations — from the type and refuses a data type there, so each
    /// column is `<name> with options` followed by what the table adds to it,
    /// its default and NOT NULL (a named NOT NULL constraint as `constraint
    /// <name> not null`, see `named_not_null_clause`), and is left out when it
    /// adds neither; then the inline primary key. Identity and generated
    /// columns are not supported on typed tables, and the other constraints
    /// are added after the `CREATE TABLE`, as for any table. Empty when the
    /// table adds nothing, which leaves the list out.
    fn typed_table_elements(&self) -> Vec<String> {
        let mut elements: Vec<String> = self
            .columns
            .iter()
            .filter_map(|column| {
                let mut options = String::new();
                if let Some(default) = &column.column_default {
                    options.push_str(&format!(" default {default}"));
                }
                if !column.is_nullable {
                    match self.named_not_null_clause(&column.name) {
                        Some(clause) => options.push_str(&format!(" {clause}")),
                        None => options.push_str(" not null"),
                    }
                }
                (!options.is_empty()).then(|| format!("    {} with options{options}", column.name))
            })
            .collect();
        if let Some(pk_def) = self.inline_primary_key_clause() {
            elements.push(pk_def);
        }
        elements
    }

    /// Hash the table
    pub fn hash(&mut self) {
        let mut hasher = Sha256::new();
        hasher.update(self.schema.as_bytes());
        hasher.update(self.name.as_bytes());
        hasher.update(self.owner.as_bytes());
        hasher.update(self.has_indexes.to_string().as_bytes());
        hasher.update(self.has_triggers.to_string().as_bytes());
        // has_rules is intentionally excluded from the hash: rules are compared
        // separately via compare_rules() and should not influence view dependency
        // detection or trigger unnecessary table processing.
        hasher.update(self.has_rowsecurity.to_string().as_bytes());

        for column in &self.columns {
            column.add_to_hasher(&mut hasher);
        }

        for constraint in &self.constraints {
            constraint.add_to_hasher(&mut hasher);
        }
        // Only a table with such a copy has any, so every other table keeps
        // the hash pgc 1.0.x gave it.
        if !self.enforced_inherited_checks.is_empty() {
            hasher.update(b"enforced_inherited_checks");
            hasher.update((self.enforced_inherited_checks.len() as u32).to_be_bytes());
            for name in &self.enforced_inherited_checks {
                hasher.update((name.len() as u32).to_be_bytes());
                hasher.update(name.as_bytes());
            }
        }

        for index in &self.indexes {
            index.add_to_hasher(&mut hasher);
        }

        for trigger in &self.triggers {
            trigger.add_to_hasher(&mut hasher);
        }
        // Only a partition with a clone in a state of its own has any, so
        // every other table keeps its hash.
        if !self.trigger_clones.is_empty() {
            hasher.update(b"trigger_clones");
            for clone in &self.trigger_clones {
                clone.add_to_hasher(&mut hasher);
            }
        }

        for policy in &self.policies {
            policy.add_to_hasher(&mut hasher);
        }

        if let Some(pk) = &self.partition_key {
            hasher.update(pk.as_bytes());
        }
        if let Some(po) = &self.partition_of {
            hasher.update(po.as_bytes());
        }
        if let Some(pb) = &self.partition_bound {
            hasher.update(pb.as_bytes());
        }
        if let Some(sp) = &self.space {
            hasher.update(sp.as_bytes());
        }
        if let Some(cmt) = &self.comment {
            hasher.update(cmt.as_bytes());
        }
        if let Some(am) = &self.access_method {
            hasher.update(am.as_bytes());
        }
        hasher.update(self.is_unlogged.to_string().as_bytes());
        if let Some(params) = &self.storage_parameters {
            for p in params {
                hasher.update(p.as_bytes());
            }
        }
        if let Some(ri) = &self.replica_identity {
            hasher.update(ri.as_bytes());
        }
        // Only a `USING INDEX` table has one, so every other table keeps the
        // hash pgc 1.0.x gave it.
        if let Some(index) = &self.replica_identity_index {
            hasher.update(index.as_bytes());
        }
        hasher.update(self.force_rowsecurity.to_string().as_bytes());
        for parent in &self.inherits_from {
            hasher.update(parent.as_bytes());
        }
        if let Some(tt) = &self.typed_table_type {
            hasher.update(tt.as_bytes());
        }

        self.hash = Some(format!("{:x}", hasher.finalize()));
    }

    fn build_script(&self, include_triggers: bool, defer_indexes: bool) -> String {
        let mut script = String::new();

        let unlogged_prefix = if self.is_unlogged { "unlogged " } else { "" };

        if let Some(parent) = &self.partition_of {
            script.push_str(&format!(
                "create {}table {}.{} partition of {}",
                unlogged_prefix, self.schema, self.name, parent
            ));
            if let Some(bound) = &self.partition_bound {
                script.push_str(&format!("\n    {}", bound));
            }

            // A sub-partition is itself partitioned — emit PARTITION BY before tablespace/semicolon
            if let Some(partition_key) = &self.partition_key {
                script.push_str(&format!("\npartition by {}", partition_key));
            }

            // Storage parameters. CREATE TABLE ... PARTITION OF takes WITH (...) just
            // like a plain CREATE TABLE, and the clause has to be here: a partition
            // recreated without it comes back with no reloptions at all, so the
            // migration only reaches the target schema on a second pass.
            if let Some(params) = &self.storage_parameters
                && !params.is_empty()
            {
                script.push_str(&format!("\nwith ({})", params.join(", ")));
            }

            if let Some(space) = &self.space {
                script.push_str(&format!("\ntablespace {}", quote_ident(space)));
            }

            script.append_block(";");
        } else {
            // 1. Build CREATE TABLE statement
            script.push_str(&format!(
                "create {}table {}.{}",
                unlogged_prefix, self.schema, self.name
            ));

            // 2. Add column definitions. A typed table takes its columns
            // from its type: its list holds only what it adds on top of them
            // (see `typed_table_elements`), and is left out when that is
            // nothing, as PostgreSQL refuses an empty one.
            let column_definitions = match &self.typed_table_type {
                Some(type_name) => {
                    script.push_str(&format!(" of {type_name}"));
                    self.typed_table_elements()
                }
                None => self.column_definitions(),
            };
            if self.typed_table_type.is_none() || !column_definitions.is_empty() {
                script.push_str(" (\n");
                script.push_str(&column_definitions.join(",\n"));
                script.push_str("\n)");
            }

            // Classical inheritance
            if !self.inherits_from.is_empty() {
                script.push_str(&format!("\ninherits ({})", self.inherits_from.join(", ")));
            }

            if let Some(partition_key) = &self.partition_key {
                script.push_str(&format!("\npartition by {}", partition_key));
            }

            if let Some(am) = &self.access_method {
                script.push_str(&format!("\nusing {}", quote_ident(am)));
            }

            // Storage parameters
            if let Some(params) = &self.storage_parameters
                && !params.is_empty()
            {
                script.push_str(&format!("\nwith ({})", params.join(", ")));
            }

            if let Some(space) = &self.space {
                script.push_str(&format!("\ntablespace {}", quote_ident(space)));
            }

            script.append_block(";");
        }

        // 5. Add other constraints (excluding primary key, foreign key, and NOT NULL).
        // NOT NULL constraints are already expressed in column definitions
        // (named ones with CONSTRAINT <name>, unnamed ones as plain NOT NULL).
        for constraint in &self.constraints {
            let c_type = constraint.constraint_type.to_lowercase();
            if c_type != "primary key" && c_type != "foreign key" && c_type != "not null" {
                script.push_str(&constraint.get_script());
            }
        }

        // 5a. Emit SET STORAGE / SET COMPRESSION for columns with non-default values
        for column in &self.columns {
            if let Some(storage) = &column.storage {
                // EXTENDED is the default for most varlena types; always emit to be explicit
                script.append_block(&format!(
                    "alter table {}.{} alter column {} set storage {};",
                    self.schema, self.name, column.name, storage
                ));
            }
            if let Some(compression) = &column.compression {
                script.append_block(&format!(
                    "alter table {}.{} alter column {} set compression {};",
                    self.schema, self.name, column.name, compression
                ));
            }
            if let Some(stats) = column.statistics_target {
                script.append_block(&format!(
                    "alter table {}.{} alter column {} set statistics {};",
                    self.schema, self.name, column.name, stats
                ));
            }
        }

        // 6. Add indexes (excluding primary key indexes and partition-inherited indexes).
        // In production output mode the indexes are emitted separately by the
        // comparer (concurrently / partition-aware), so they are deferred here.
        if !defer_indexes {
            for index in &self.indexes {
                if !index.indexdef.to_lowercase().contains("primary key")
                    && !index.is_partition_index
                {
                    script.push_str(&index.get_script());
                }
            }
        }

        // 7. Add triggers
        if include_triggers {
            for trigger in &self.triggers {
                script.push_str(&trigger.get_script(&self.schema, &self.name));
            }
        }

        // 8. Enable row-level security before creating policies
        if self.has_rowsecurity {
            script.append_block(&format!(
                "alter table {}.{} enable row level security;",
                self.schema, self.name
            ));
        }

        // 9. Add row-level security policies
        for policy in &self.policies {
            script.push_str(&policy.get_script());
        }

        // 9a. Force row-level security
        if self.force_rowsecurity {
            script.append_block(&format!(
                "alter table {}.{} force row level security;",
                self.schema, self.name
            ));
        }

        // 9b. Replica identity. `USING INDEX` comes after the constraints (5)
        // and indexes (6), since the index and the NOT NULLs on its columns
        // have to exist first.
        if let Some(ri) = &self.replica_identity {
            match ri.as_str() {
                "n" => script.append_block(&format!(
                    "alter table {}.{} replica identity nothing;",
                    self.schema, self.name
                )),
                "f" => script.append_block(&format!(
                    "alter table {}.{} replica identity full;",
                    self.schema, self.name
                )),
                "i" => {
                    // The comparer runs it itself when the index is only
                    // there later: after the production path's post-commit
                    // build (`replica_identity_create_script_after_indexes`),
                    // or, for a partition's index PostgreSQL creates from the
                    // parent's, after the table section
                    // (`replica_identity_script_after_tables`).
                    let left_to_comparer = self.replica_identity_index_is_inherited()
                        || (defer_indexes && self.replica_identity_index_is_creatable());
                    // `None` for a pre-1.1.0 dump, which never recorded the index.
                    if !left_to_comparer
                        && let Some(stmt) = self.replica_identity_using_index_script()
                    {
                        script.push_str(&stmt);
                    }
                }
                // "d" is the default.
                _ => {}
            }
        }

        // 10. Add table comment (if any) and column comments
        if let Some(comment) = &self.comment {
            script.append_block(&format!(
                "comment on table {}.{} is '{}';",
                self.schema,
                self.name,
                escape_single_quotes(comment)
            ));
        }

        for column in &self.columns {
            if let Some(comment_script) = column.get_comment_script() {
                script.push_str(&comment_script);
            }
        }

        script.push_str(&self.get_owner_script());

        script
    }

    /// Get script for the table
    pub fn get_script(&self) -> String {
        self.build_script(true, false)
    }

    /// Get script for the table without triggers (for deferred trigger creation)
    pub fn get_script_without_triggers(&self) -> String {
        self.build_script(false, false)
    }

    /// Get script for the table without triggers and without index creation.
    /// Used by the production output path, which emits indexes separately so
    /// they can be built concurrently / partition-aware.
    pub fn get_script_without_triggers_no_indexes(&self) -> String {
        self.build_script(false, true)
    }

    /// Indexes that `build_script` would emit for a brand-new table: excludes
    /// primary-key-backing indexes (created via the PRIMARY KEY clause) and
    /// partition-inherited indexes (managed by the partitioned parent). Mirrors
    /// the filter in `build_script` so the production path stays in sync.
    pub fn creatable_indexes(&self) -> Vec<&TableIndex> {
        self.indexes
            .iter()
            .filter(|index| {
                !index.indexdef.to_lowercase().contains("primary key") && !index.is_partition_index
            })
            .collect()
    }

    /// Structured index diff between `self` (FROM) and `to_table` (TO),
    /// mirroring the index handling in `build_alter_script`. The production
    /// output path consumes this so each index change can be rewritten
    /// concurrently / partition-aware instead of emitted inline.
    pub fn index_alter_plan<'a>(&'a self, to_table: &'a Table) -> IndexAlterPlan<'a> {
        let mut plan = IndexAlterPlan::default();

        for new_index in &to_table.indexes {
            if new_index.is_partition_index {
                continue;
            }
            if let Some(old_index) = self.indexes.iter().find(|i| i.name == new_index.name) {
                if old_index != new_index {
                    if !crate::dump::table_index::indexdefs_equivalent(
                        &old_index.indexdef,
                        &new_index.indexdef,
                    ) {
                        // Definition changed: drop the old, build the new.
                        plan.drop.push(old_index);
                        plan.create.push(new_index);
                    } else {
                        // Comment-only change.
                        plan.comment_changes.push(new_index);
                    }
                }
            } else {
                plan.create.push(new_index);
            }
        }

        for old_index in &self.indexes {
            if old_index.is_partition_index {
                continue;
            }
            if !to_table.indexes.iter().any(|i| i.name == old_index.name) {
                plan.drop.push(old_index);
            }
        }

        plan
    }

    /// The index behind this table's `REPLICA IDENTITY USING INDEX`, when
    /// the dump recorded it.
    fn replica_identity_using(&self) -> Option<&str> {
        match self.replica_identity.as_deref() {
            Some("i") => self.replica_identity_index.as_deref(),
            _ => None,
        }
    }

    /// Whether dropping `column`, re-creating it, or clearing its NOT NULL
    /// can take this table's replica-identity index away (or make
    /// PostgreSQL refuse the change): it is one of
    /// [`Table::replica_identity_columns`], or those are unknown.
    fn replica_identity_uses_column(&self, column: &str) -> bool {
        self.replica_identity_columns.is_empty()
            || self.replica_identity_columns.iter().any(|c| c == column)
    }

    /// `alter table ... replica identity using index ...;` for this table.
    /// `None` unless its replica identity is an index the dump names: any
    /// other setting, or `USING INDEX` in a dump written before pgc 1.1.0,
    /// which did not record the index.
    pub fn replica_identity_using_index_script(&self) -> Option<String> {
        self.replica_identity_using().map(|index| {
            format!(
                "alter table {}.{} replica identity using index {};",
                self.schema, self.name, index
            )
            .with_empty_lines()
        })
    }

    /// Whether the replica-identity index is one of
    /// [`Table::creatable_indexes`]: built by a `CREATE INDEX` of its own,
    /// which the production path defers, rather than by the CREATE TABLE's
    /// PRIMARY KEY or a UNIQUE constraint.
    fn replica_identity_index_is_creatable(&self) -> bool {
        self.replica_identity_using()
            .is_some_and(|name| self.creatable_indexes().iter().any(|i| i.name == name))
    }

    /// Whether the replica-identity index is one PostgreSQL creates for this
    /// partition from its parent: a partition index attached to an index of
    /// the parent, or the index of a PRIMARY KEY / UNIQUE constraint the
    /// partition inherits. Neither is this table's to create, and the server
    /// picks the name (`<partition>_<columns>_idx`, `<partition>_pkey`, ...),
    /// except under `--output-for-production`, whose concurrent
    /// per-partition build names a new one `<partition>_<parent index>`.
    fn replica_identity_index_is_inherited(&self) -> bool {
        self.partition_of.is_some()
            && self.replica_identity_using().is_some_and(|name| {
                !self.creatable_indexes().iter().any(|i| i.name == name)
                    && !self
                        .constraints
                        .iter()
                        .any(|c| c.name == name && c.coninhcount == 0)
            })
    }

    /// Whether this table already has an index named `name`: one of its
    /// indexes (partition indexes included), the index of one of its
    /// PRIMARY KEY / UNIQUE constraints, or its recorded replica-identity
    /// index.
    fn has_index_named(&self, name: &str) -> bool {
        self.indexes.iter().any(|i| i.name == name)
            || self.constraints.iter().any(|c| {
                c.name == name
                    && (c.constraint_type.eq_ignore_ascii_case("primary key")
                        || c.constraint_type.eq_ignore_ascii_case("unique"))
            })
            || self.replica_identity_using() == Some(name)
    }

    /// Whether this (TO) partition's `USING INDEX` has to wait until the
    /// whole table section has run: its index is one PostgreSQL creates from
    /// the parent (see `replica_identity_index_is_inherited`), and `from` —
    /// the table before the migration, `None` when the migration creates or
    /// re-creates it — does not have it yet. The index then comes from an
    /// index or constraint the migration adds to the parent, or from
    /// creating the partition, and a new partition is created before any
    /// existing table, its parent included, is altered.
    pub(crate) fn replica_identity_waits_for_parent(&self, from: Option<&Table>) -> bool {
        self.replica_identity_index_is_inherited()
            && self
                .replica_identity_using()
                .is_some_and(|name| from.is_none_or(|from| !from.has_index_named(name)))
    }

    /// The `USING INDEX` of a partition that waits for its parent (see
    /// `replica_identity_waits_for_parent`), which the comparer runs after the
    /// table section; `None` for any other table.
    ///
    /// It is guarded. PostgreSQL names that index itself, so the name the TO
    /// dump recorded is the one it picks only when the TO database was built
    /// the same way, and `--output-for-production` builds a new one
    /// concurrently as `<partition>_<parent index>`. When the partition has no
    /// index of that name, the block leaves the replica identity alone and
    /// raises a WARNING instead of failing the migration.
    ///
    /// The block is dollar-quoted with a tag its body does not contain
    /// (`$pgc$`, else `$pgc1$`, ...): the body carries the partition and index
    /// names, and a name holding `$pgc$` would otherwise end the quote and run
    /// the rest of itself as SQL.
    pub(crate) fn replica_identity_script_after_tables(
        &self,
        from: Option<&Table>,
    ) -> Option<String> {
        if !self.replica_identity_waits_for_parent(from) {
            return None;
        }
        let index = self.replica_identity_using()?;
        let table = format!("{}.{}", self.schema, self.name);
        let qualified_index = format!("{}.{}", self.schema, index);
        let body = format!(
            "\n\
                 begin\n    \
                     if exists (\n        \
                         select 1 from pg_catalog.pg_index\n        \
                         where indexrelid = to_regclass('{index_literal}')\n          \
                           and indrelid = '{table_literal}'::regclass\n    \
                     ) then\n        \
                         alter table {table} replica identity using index {index};\n    \
                     else\n        \
                         raise warning '%', 'pgc: replica identity using index {message_index} not set on {message_table}: the partition has no index of that name';\n    \
                     end if;\n\
                 end\n",
            index_literal = escape_single_quotes(&qualified_index),
            table_literal = escape_single_quotes(&table),
            message_index = escape_single_quotes(index),
            message_table = escape_single_quotes(&table),
        );
        let tag = crate::dump::acl::dollar_quote_tag(&body);
        Some(format!("do {tag}{body}{tag};").with_empty_lines())
    }

    /// For a brand-new table on the production path, which builds its
    /// indexes after commit: the `USING INDEX` statement
    /// [`Table::get_script_without_triggers_no_indexes`] leaves out because
    /// its index is one of those. The caller emits it once the index is built.
    pub fn replica_identity_create_script_after_indexes(&self) -> Option<String> {
        if self.replica_identity_index_is_creatable() {
            self.replica_identity_using_index_script()
        } else {
            None
        }
    }

    /// The columns an in-place alter to `to_table` recreates with
    /// `DROP COLUMN` + `ADD COLUMN` (the generated-column rewrites of
    /// [`TableColumn::would_drop_and_re_add_with_caps`], and the generated
    /// columns of [`Table::columns_recreated_for_retype`]); none without
    /// `use_drop`. That drop takes the column's indexes and constraints with
    /// it until the comparer re-emits them (issue #188).
    fn recreated_columns<'a>(
        &'a self,
        to_table: &'a Table,
        use_drop: bool,
        caps: GeneratedAlterCaps,
    ) -> impl Iterator<Item = &'a str> + 'a {
        let for_retype = self.columns_recreated_for_retype(to_table, use_drop, caps);
        to_table
            .columns
            .iter()
            .filter(move |new_col| {
                use_drop
                    && self
                        .columns
                        .iter()
                        .find(|c| c.name == new_col.name)
                        .is_some_and(|old_col| {
                            new_col.would_drop_and_re_add_with_caps(old_col, caps)
                                || for_retype.contains(new_col.name.as_str())
                        })
            })
            .map(|new_col| new_col.name.as_str())
    }

    /// The FROM columns whose `DROP COLUMN` this in-place alter to
    /// `to_table` runs to rewrite a column rather than to remove it for
    /// good — itself, or through its parent's recursing statement: each
    /// column of `recreated_columns` (a live drop + add), and a generated
    /// column in the way of a retype that TO does not have (see
    /// [`Table::generated_columns_in_the_way_of_retype`]), which is dropped
    /// ahead of the retype. None without `use_drop`.
    ///
    /// The comparer drops what depends on them without going with them — a
    /// policy, a trigger, another table's foreign key — ahead of the table
    /// alters, since PostgreSQL refuses such a drop ("other objects depend
    /// on it"), and puts back the TO side's afterwards.
    pub(crate) fn rewrite_column_drops(
        &self,
        to_table: &Table,
        use_drop: bool,
        caps: GeneratedAlterCaps,
    ) -> Vec<String> {
        let mut columns: Vec<String> = self
            .recreated_columns(to_table, use_drop, caps)
            .map(str::to_string)
            .collect();
        for column in self.generated_columns_in_the_way_of_retype(to_table, use_drop, caps) {
            if !to_table.columns.iter().any(|c| c.name == column) {
                columns.push(column.to_string());
            }
        }
        columns
    }

    /// `constraint <name> not null`, with `no inherit` / `not enforced` as
    /// the constraint has them, for the NOT NULL constraint this table has
    /// on `column` (as the dump spells the column) when its name is not the
    /// one PostgreSQL would choose (see
    /// [`TableConstraint::auto_not_null_column`]); `None` otherwise, and
    /// before PostgreSQL 18, which keeps no NOT NULL constraints. A column
    /// definition says it in place of a plain `not null`, so the constraint
    /// is created under its name: PostgreSQL 18+ refuses a second NOT NULL
    /// constraint on a column that already has an auto-named one.
    fn named_not_null_clause(&self, column: &str) -> Option<String> {
        let column_key = TableConstraint::normalize_identifier_for_name(column);
        let nn = self
            .constraints
            .iter()
            .filter(|c| c.constraint_type.eq_ignore_ascii_case("not null"))
            .filter(|c| c.auto_not_null_column(&self.name).is_none())
            .find(|c| {
                c.definition
                    .as_deref()
                    .and_then(TableConstraint::strip_not_null_prefix)
                    .and_then(TableConstraint::parse_pg_identifier)
                    .is_some_and(|name| name == column_key)
            })?;
        let mut clause = format!("constraint {} not null", nn.name);
        if nn.no_inherit {
            clause.push_str(" no inherit");
        }
        if !nn.is_enforced {
            clause.push_str(" not enforced");
        }
        Some(clause)
    }

    /// The FROM columns, by name, whose type (collation included) differs in
    /// `to_table` and that an in-place alter changes with `ALTER COLUMN ...
    /// TYPE` rather than a drop + add ([`TableColumn::alters_type_in_place`]):
    /// this table's own statement, or — on a partition, or on a column it
    /// inherits — its parent's recursing one.
    ///
    /// PostgreSQL refuses that statement while a policy or a trigger (`UPDATE
    /// OF`, or its `WHEN` clause) depends on the column ("cannot alter type of
    /// a column used in a policy definition" / "... in a trigger
    /// definition"), so the comparer drops those ahead of the table alters
    /// and creates the TO side's again afterwards (see
    /// `Comparer::column_drop_blockers`). The caller decides whether the
    /// table is altered in place at all.
    pub(crate) fn columns_retyped_in_place<'a>(
        &'a self,
        to_table: &Table,
        use_drop: bool,
        caps: GeneratedAlterCaps,
    ) -> Vec<&'a str> {
        to_table
            .columns
            .iter()
            .filter_map(|to_col| {
                let from_col = self.columns.iter().find(|c| c.name == to_col.name)?;
                to_col
                    .alters_type_in_place(from_col, caps, use_drop)
                    .then_some(from_col.name.as_str())
            })
            .collect()
    }

    /// The FROM generated columns that stand in the way of this in-place
    /// alter to `to_table`: each reads, in its FROM expression, a column the
    /// alter retypes in place ([`TableColumn::alters_type_in_place`]).
    /// PostgreSQL refuses `ALTER COLUMN ... TYPE` on such a column ("cannot
    /// alter type of a column used by a generated column") on every
    /// supported version, STORED and VIRTUAL alike, whether or not the
    /// generated column changes too, so the alter takes each one out of the
    /// way first: it drops it, or on its way to a plain column drops its
    /// expression, before the retype (see [`Table::columns_recreated_for_retype`]).
    ///
    /// For a partition in FROM and TO, which leaves its column changes to
    /// its parent, these are the columns its parent's recursing statements
    /// drop (and re-add) on it; its own alter emits nothing for them. Empty
    /// without `use_drop` (a drop is then only a comment), and for a table
    /// whose `DROP TABLE` runs.
    pub(crate) fn generated_columns_in_the_way_of_retype<'a>(
        &'a self,
        to_table: &Table,
        use_drop: bool,
        caps: GeneratedAlterCaps,
    ) -> HashSet<&'a str> {
        let is_target_partition = self.partition_of.is_some() && to_table.partition_of.is_some();
        if !use_drop
            || self.partition_key != to_table.partition_key
            || (!is_target_partition && self.will_be_dropped_and_recreated(to_table))
        {
            return HashSet::new();
        }
        let retyped = self.columns_retyped_in_place(to_table, use_drop, caps);
        if retyped.is_empty() {
            return HashSet::new();
        }
        self.columns
            .iter()
            .filter(|column| {
                retyped
                    .iter()
                    .any(|&base| base != column.name && column.generation_expression_reads(base))
            })
            .map(|column| column.name.as_str())
            .collect()
    }

    /// The generated columns this in-place alter to `to_table` drops and
    /// re-adds from their TO definition because they stand in the way of a
    /// retype (see [`Table::generated_columns_in_the_way_of_retype`]) and
    /// are generated in TO too. The drop runs before the retype and the add
    /// after it, and to the comparer they are re-created columns like any
    /// other: what the drop takes along (indexes, constraints, statistics,
    /// the views over them) is restored. One that is plain in TO keeps its
    /// values instead (`DROP EXPRESSION` ahead of the retype), and one TO
    /// does not have is dropped ahead of it.
    pub(crate) fn columns_recreated_for_retype(
        &self,
        to_table: &Table,
        use_drop: bool,
        caps: GeneratedAlterCaps,
    ) -> HashSet<String> {
        let in_the_way = self.generated_columns_in_the_way_of_retype(to_table, use_drop, caps);
        if in_the_way.is_empty() {
            return HashSet::new();
        }
        to_table
            .columns
            .iter()
            .filter(|c| in_the_way.contains(c.name.as_str()) && c.is_generated_column())
            .map(|c| c.name.clone())
            .collect()
    }

    /// The CHECK constraints, by name, that an in-place alter to `to_table`
    /// drops: those TO does not have, and those it changes in a way no
    /// in-place `ALTER CONSTRAINT` the migrated server accepts (`caps`) can
    /// make, which it re-adds. None without `use_drop` (a drop is then only
    /// a comment) and for a table the alter drops and re-creates; a
    /// partition's inherited copies are left to its parent, as the alter
    /// leaves them. The comparer reads it to find a merged inheritance copy
    /// among them (see [`InheritedRecreates::family_checks`]).
    pub(crate) fn dropped_checks(
        &self,
        to_table: &Table,
        use_drop: bool,
        caps: TableAlterCaps,
    ) -> Vec<&TableConstraint> {
        if !use_drop || self.will_be_dropped_and_recreated(to_table) {
            return Vec::new();
        }
        let is_target_partition = self.partition_of.is_some() && to_table.partition_of.is_some();
        let constraint_caps = if self.recreates_a_column(to_table, use_drop, caps.generated) {
            ConstraintAlterCaps::default()
        } else {
            caps.constraint
        };
        self.constraints
            .iter()
            .filter(|c| c.constraint_type.eq_ignore_ascii_case("check"))
            .filter(|c| !(is_target_partition && c.coninhcount > 0))
            .filter(
                |c| match to_table.constraints.iter().find(|t| t.name == c.name) {
                    None => true,
                    Some(target) => {
                        *c != target
                            && c.get_alter_script_with_caps(target, constraint_caps)
                                .is_none()
                    }
                },
            )
            .collect()
    }

    /// Whether an in-place alter to `to_table` recreates any column (see
    /// `recreated_columns`).
    fn recreates_a_column(
        &self,
        to_table: &Table,
        use_drop: bool,
        caps: GeneratedAlterCaps,
    ) -> bool {
        self.recreated_columns(to_table, use_drop, caps)
            .next()
            .is_some()
    }

    /// Whether the `USING INDEX` an in-place alter to `to_table` ends with
    /// has to wait for something the comparer emits after the alter script:
    /// the production path's post-commit build of that very index
    /// (`defer_indexes`), or the re-creation of an index a column drop + add
    /// took down, when a recreated column is one of the index's. The alter
    /// then hands the statement to the comparer instead of emitting it (see
    /// [`Table::get_alter_script_in_family`]).
    fn replica_identity_waits_for_comparer(
        &self,
        to_table: &Table,
        use_drop: bool,
        defer_indexes: bool,
        caps: GeneratedAlterCaps,
    ) -> bool {
        let Some(index) = to_table.replica_identity_using() else {
            return false;
        };
        // Without `use_drop` a rebuilt index is never dropped, so it is there.
        let built_after_commit = defer_indexes
            && self
                .index_alter_plan(to_table)
                .create
                .iter()
                .any(|i| i.name == index)
            && (use_drop || !self.indexes.iter().any(|i| i.name == index));
        built_after_commit
            || self
                .recreated_columns(to_table, use_drop, caps)
                .any(|column| to_table.replica_identity_uses_column(column))
    }

    /// How an alter to `to_table` changes the replica identity: an early
    /// statement, first in the alter, that moves the table off its old
    /// `USING INDEX`, and a late one, last in the alter, that sets the new
    /// value. See [`ReplicaIdentityPhases`].
    ///
    /// `disturbs_index` says whether the alter drops the FROM
    /// replica-identity index (or the constraint behind it), or a column it
    /// is on, or clears a NOT NULL one of its key columns needs. The table
    /// then leaves the index first, and returns to it last even when the
    /// setting itself does not change.
    fn replica_identity_phases(
        &self,
        to_table: &Table,
        disturbs_index: bool,
    ) -> ReplicaIdentityPhases {
        let from_ri = self.replica_identity.as_deref();
        let to_ri = to_table.replica_identity.as_deref();
        // A TO dump written before pgc 1.1.0 says `USING INDEX` without naming
        // the index. Rather than guess, the replica identity is left alone.
        if to_ri == Some("i") && to_table.replica_identity_index.is_none() {
            return ReplicaIdentityPhases::default();
        }
        let set = |setting: &str| {
            format!(
                "alter table {}.{} replica identity {setting};",
                to_table.schema, to_table.name
            )
            .with_empty_lines()
        };
        // DEFAULT also stands in when the target is an index.
        let plain_target = || {
            set(match to_ri {
                Some("n") => "nothing",
                Some("f") => "full",
                _ => "default",
            })
        };
        let from_on_index = from_ri == Some("i");
        let to_on_index = to_ri == Some("i");
        let changes = from_ri != to_ri
            || (to_on_index && self.replica_identity_index != to_table.replica_identity_index);
        // An index the FROM dump did not name (pgc before 1.1.0) cannot be
        // checked for being disturbed, so a change leaves it first.
        let from_index_unknown = from_on_index && self.replica_identity_index.is_none();
        let leaves_early =
            from_on_index && (disturbs_index || (changes && (!to_on_index || from_index_unknown)));

        let early = leaves_early.then(plain_target);
        let late = if to_on_index {
            if changes || leaves_early {
                to_table.replica_identity_using_index_script()
            } else {
                None
            }
        } else if changes && !from_on_index {
            Some(plain_target())
        } else {
            None
        };
        ReplicaIdentityPhases { early, late }
    }

    /// Get trigger creation scripts only
    pub fn get_trigger_script(&self) -> String {
        let mut script = String::new();
        for trigger in &self.triggers {
            script.push_str(&trigger.get_script(&self.schema, &self.name));
        }
        script
    }

    /// Get drop script for the table
    pub fn get_drop_script(&self) -> String {
        format!("drop table if exists {}.{};", self.schema, self.name).with_empty_lines()
    }

    pub fn get_owner_script(&self) -> String {
        if self.owner.is_empty() {
            return String::new();
        }

        format!(
            "alter table {}.{} owner to {};",
            self.schema, self.name, self.owner
        )
        .with_empty_lines()
    }

    /// Get script for creating foreign keys
    pub fn get_foreign_key_script(&self) -> String {
        let mut script = String::new();
        for constraint in &self.constraints {
            if constraint.constraint_type.to_lowercase() == "foreign key" {
                script.push_str(&constraint.get_script());
            }
        }
        script
    }

    /// Get script for altering foreign keys
    pub fn get_foreign_key_alter_script(&self, to_table: &Table) -> String {
        let mut script = String::new();
        for new_constraint in &to_table.constraints {
            if new_constraint.constraint_type.to_lowercase() != "foreign key" {
                continue;
            }

            if let Some(old_constraint) = self
                .constraints
                .iter()
                .find(|c| c.name == new_constraint.name)
            {
                if old_constraint != new_constraint {
                    if let Some(alter_script) = old_constraint.get_alter_script(new_constraint) {
                        script.push_str(&alter_script);
                    } else {
                        // Drop is handled in the table's get_alter_script, so just add the new FK here.
                        script.push_str(&new_constraint.get_script());
                    }
                }
            } else {
                // New FK
                script.push_str(&new_constraint.get_script());
            }
        }
        script
    }

    /// Like [`Table::get_foreign_key_alter_script`] but returns the in-place FK
    /// modifications as a script and the brand-new foreign keys separately, so
    /// the production output path can add the new ones `NOT VALID` and validate
    /// them after the transaction commits. Mirrors the branch structure of
    /// `get_foreign_key_alter_script` so the two cannot drift apart.
    pub fn foreign_key_alter_split<'a>(
        &self,
        to_table: &'a Table,
    ) -> (String, Vec<&'a TableConstraint>) {
        let mut alters = String::new();
        let mut new_fks: Vec<&TableConstraint> = Vec::new();
        for new_constraint in &to_table.constraints {
            if new_constraint.constraint_type.to_lowercase() != "foreign key" {
                continue;
            }
            if let Some(old_constraint) = self
                .constraints
                .iter()
                .find(|c| c.name == new_constraint.name)
            {
                if old_constraint != new_constraint {
                    if let Some(alter_script) = old_constraint.get_alter_script(new_constraint) {
                        alters.push_str(&alter_script);
                    } else {
                        new_fks.push(new_constraint);
                    }
                }
            } else {
                new_fks.push(new_constraint);
            }
        }
        (alters, new_fks)
    }

    fn build_alter_script(
        &self,
        to_table: &Table,
        use_drop: bool,
        include_triggers: bool,
        defer_indexes: bool,
        caps: TableAlterCaps,
        inherited: &InheritedRecreates,
    ) -> String {
        self.build_alter_script_parts(
            to_table,
            use_drop,
            include_triggers,
            defer_indexes,
            caps,
            inherited,
        )
        .script
    }

    /// `build_alter_script`, plus what it leaves out because the comparer
    /// has to run it elsewhere: the `REPLICA IDENTITY USING INDEX` it runs
    /// later (see `replica_identity_waits_for_comparer`), and the CHECK
    /// relaxes and drops it runs ahead of an ancestor's rewrite (see
    /// `InheritedRecreates::rewritten_by_ancestor`).
    fn build_alter_script_parts(
        &self,
        to_table: &Table,
        use_drop: bool,
        include_triggers: bool,
        defer_indexes: bool,
        caps: TableAlterCaps,
        inherited: &InheritedRecreates,
    ) -> AlterScriptParts {
        // If partition key changes (e.g. from LIST to RANGE, or different column), we must recreate the table.
        // Also if table changes from partitioned to non-partitioned or vice versa.
        if self.partition_key != to_table.partition_key {
            let drop_script = if use_drop {
                self.get_drop_script()
            } else {
                comment_out(&self.get_drop_script())
            };
            return AlterScriptParts {
                script: format!(
                    "/* Partition key changed. Table must be recreated. Data loss will occur! */\n{}{}",
                    drop_script,
                    to_table.get_script()
                ),
                replica_identity_after: None,
                ancestor_pre_script: String::new(),
                ancestor_retype_pre_script: String::new(),
            };
        }

        let mut constraint_pre_script = String::new();
        // See `InheritedRecreates::rewritten_by_ancestor`.
        let mut ancestor_pre_script = String::new();
        // What gets generated columns out of the way of an ancestor's retype
        // (see `InheritedRecreates::retyped_by_ancestor`). The comparer runs
        // it after every table's `ancestor_pre_script`, as
        // `retype_pre_script` follows `constraint_pre_script` below.
        let mut ancestor_retype_pre_script = String::new();
        // What has to get out of the way of a retype first (see
        // `generated_columns_in_the_way_of_retype`): runs after the
        // constraint drops, before `column_alter_script`.
        let mut retype_pre_script = String::new();
        let mut column_alter_script = String::new();
        // Generated-column statements that may read a column this alter adds
        // (`SET EXPRESSION`, and the drop + add of a re-created column): they
        // run after every `ADD COLUMN`, and before any column is dropped.
        let mut generated_script = String::new();
        let mut column_drop_script = String::new();
        let mut constraint_post_script = String::new();
        let mut index_script = String::new();
        let mut index_drop_script = String::new();
        let mut detach_script = String::new();
        let mut attach_script = String::new();
        let mut policy_script = String::new();
        let mut policy_drop_script = String::new();
        let mut row_security_script = String::new();

        // Handle partition changes
        if self.partition_of != to_table.partition_of
            || (self.partition_of.is_some() && self.partition_bound != to_table.partition_bound)
        {
            // If it was a partition, detach it
            if let Some(old_parent) = &self.partition_of {
                let detach_cmd = format!(
                    "alter table {} detach partition {}.{};",
                    old_parent, self.schema, self.name
                )
                .with_empty_lines();
                if use_drop {
                    detach_script.push_str(&detach_cmd);
                } else {
                    detach_script.push_str(&comment_out_keep_blank_lines(&detach_cmd));
                }
            }

            // If it is now a partition, attach it
            if let Some(new_parent) = &to_table.partition_of
                && let Some(bound) = &to_table.partition_bound
            {
                attach_script.append_block(&format!(
                    "alter table {} attach partition {}.{} {};",
                    new_parent, self.schema, self.name, bound
                ));
            }
        }

        // Classical inheritance (`INHERITS`): the parents after the ones
        // both sides list in the same order are left (`NO INHERIT`, which
        // like a detach is only commented out without `use_drop`) and the
        // TO ones joined (`INHERIT`), in TO order, since PostgreSQL appends
        // each new parent to the list.
        let (left_parents, joined_parents) = self.inheritance_moves(to_table);
        for parent in left_parents {
            let no_inherit_cmd = format!(
                "alter table {}.{} no inherit {};",
                self.schema, self.name, parent
            )
            .with_empty_lines();
            if use_drop {
                detach_script.push_str(&no_inherit_cmd);
            } else {
                detach_script.push_str(&comment_out_keep_blank_lines(&no_inherit_cmd));
            }
        }
        for parent in joined_parents {
            attach_script.append_block(&format!(
                "alter table {}.{} inherit {};",
                self.schema, self.name, parent
            ));
        }

        // Partition children inherit column structure (add/drop/alter) and
        // non-FK constraints from their parent table.  Generating the same DDL
        // for both parent and partition causes errors (ADD/DROP COLUMN) or
        // redundant operations (SET NOT NULL, SET DEFAULT, constraint changes).
        // Only suppress when the table is a partition child in both FROM and TO.
        // If it is transitioning from standalone to partition (attach), structural
        // changes may be required to make it compatible with the parent first.
        let is_target_partition = self.partition_of.is_some() && to_table.partition_of.is_some();

        // A typed table's columns are its type's attributes: `ALTER TABLE`
        // can neither add, drop nor retype one ("cannot add column to typed
        // table"). The type's `ALTER TYPE … ADD / DROP / ALTER ATTRIBUTE …
        // CASCADE` does all three for it (`compare_types`; the comparer runs
        // it after the view drops and before the tables), so only what a
        // typed table keeps of its own — a default, NOT NULL, storage,
        // statistics, a comment — is altered here. When that type is not
        // compared, the comparer warns (`typed_column_changes`).
        let typed_by_type =
            self.typed_table_type.is_some() && self.typed_table_type == to_table.typed_table_type;

        // `SET EXPRESSION` recurses unless it says `ONLY`. An inheritance
        // child is diffed on its own, and may keep an expression of its own
        // (PostgreSQL 16+), so a table that is not partitioned uses `ONLY`; a
        // partitioned table recurses, since its partitions' columns are never
        // diffed. (A partition-key change returned above, so both sides agree.)
        let set_expression_only = to_table.partition_key.is_none();

        // A live drop + add of a column CASCADE-drops the CHECK constraints on
        // it before `constraint_post_script` runs, so an in-place `ALTER
        // CONSTRAINT ... [NOT] ENFORCED` there would find nothing to alter.
        // Such a table keeps the drop + re-add, which is what Path B restores
        // anyway. Partitions count too: their parent's drop recurses to them.
        let recreates_a_column = self.recreates_a_column(to_table, use_drop, caps.generated);
        let constraint_caps = if recreates_a_column {
            ConstraintAlterCaps::default()
        } else {
            caps.constraint
        };
        // For a partition, and for a column an ancestor's drop + add
        // re-creates (see `InheritedRecreates`), the parent's recursing
        // `DROP COLUMN` has already run when this table is altered, and it took
        // this table's own constraints on the column with it. Their drops say
        // `IF EXISTS`; the TO side's constraints are re-added below either way.
        // Likewise for a typed table whose type drops an attribute: the
        // type's `DROP ATTRIBUTE … CASCADE` has run by then.
        let constraints_may_be_gone = (recreates_a_column
            && (is_target_partition || !inherited.recreated_by_ancestor.is_empty()))
            || (typed_by_type
                && self
                    .columns
                    .iter()
                    .any(|old| !to_table.columns.iter().any(|c| c.name == old.name)));
        let constraint_drop_script = |constraint: &TableConstraint| {
            if constraints_may_be_gone || inherited.dropped_ahead.contains(&constraint.name) {
                constraint.get_drop_if_exists_script()
            } else {
                constraint.get_drop_script()
            }
        };
        // A local CHECK whose relax or drop has to run before an ancestor's
        // rewrite of this table (see `InheritedRecreates::rewritten_by_ancestor`),
        // and a CHECK the inheritance family drops and re-adds together (see
        // `InheritedRecreates::family_checks`).
        let in_family = |constraint: &TableConstraint| {
            constraint.constraint_type.eq_ignore_ascii_case("check")
                && inherited.family_checks.contains(&constraint.name)
        };
        // A copy the detach leaves behind (see
        // `InheritedRecreates::detached_checks`) is still inherited before
        // the detach, so nothing about it can run ahead of it.
        let detached_copy = |constraint: &TableConstraint| {
            inherited
                .detached_checks
                .iter()
                .any(|copy| std::ptr::eq(copy, constraint))
        };
        let ahead_of_ancestor = |constraint: &TableConstraint| {
            !detached_copy(constraint)
                && ((inherited.rewritten_by_ancestor
                    && constraint.coninhcount == 0
                    && constraint.constraint_type.eq_ignore_ascii_case("check"))
                    || in_family(constraint))
        };
        // A constraint drop that can take the FROM replica-identity index
        // down with it (a PRIMARY KEY / UNIQUE constraint shares its index's
        // name) or clear a NOT NULL one of its key columns needs. Without
        // `use_drop` the drop is only commented out. See
        // `replica_identity_phases`.
        let from_ri_index = self.replica_identity_using();
        let drop_disturbs_ri_index = |constraint: &TableConstraint| {
            use_drop
                && from_ri_index.is_some_and(|index| {
                    constraint.name == index
                        || (constraint.constraint_type.eq_ignore_ascii_case("not null")
                            && not_null_constraint_column(constraint)
                                .is_none_or(|column| self.replica_identity_uses_column(column)))
                })
        };
        let mut ri_index_disturbed = false;
        // Generated columns a retype needs out of the way, and those of them
        // re-created from TO (see `columns_recreated_for_retype`).
        let in_the_way =
            self.generated_columns_in_the_way_of_retype(to_table, use_drop, caps.generated);
        let recreated_for_retype =
            self.columns_recreated_for_retype(to_table, use_drop, caps.generated);

        // A generated column in the way of an ancestor's recursing retype
        // (see `InheritedRecreates::retyped_by_ancestor`) has to be out of
        // the way before that ancestor's alter runs, so its statement goes
        // to the pass ahead of the table alters.
        let before_ancestor_retype = |column: &TableColumn| {
            inherited
                .retyped_by_ancestor
                .iter()
                .any(|base| *base != column.name && column.generation_expression_reads(base))
        };

        // Collect column additions or alterations
        for new_col in &to_table.columns {
            if let Some(old_col) = self.columns.iter().find(|c| c.name == new_col.name) {
                // An ancestor's `ALTER COLUMN ... TYPE` retypes this column.
                // So does a typed table's type (see `typed_by_type`).
                let retyped_elsewhere;
                let new_col =
                    if typed_by_type || inherited.retyped_by_ancestor.contains(&new_col.name) {
                        retyped_elsewhere = new_col.with_type_of(old_col);
                        &retyped_elsewhere
                    } else {
                        new_col
                    };
                let ahead_of_retype = in_the_way.contains(old_col.name.as_str());
                let retype_pre_script = if ahead_of_retype && before_ancestor_retype(old_col) {
                    &mut ancestor_retype_pre_script
                } else {
                    &mut retype_pre_script
                };
                if old_col != new_col || ahead_of_retype {
                    // Skip structural column changes for partition children;
                    // only emit column comment changes (not inherited).
                    if is_target_partition {
                        if new_col.comment != old_col.comment {
                            if let Some(comment_script) = new_col.get_comment_script() {
                                column_alter_script.push_str(&comment_script);
                            } else {
                                column_alter_script.push_str(&format!(
                                    "comment on column {}.{}.{} is null;\n",
                                    new_col.schema, new_col.table, new_col.name
                                ));
                            }
                        }
                        continue;
                    }

                    if self.column_type_change_forces_recreate(old_col, new_col) {
                        let drop_script = if use_drop {
                            self.get_drop_script()
                        } else {
                            comment_out(&self.get_drop_script())
                        };
                        // PostgreSQL retypes no partition-key column, even
                        // for a new collation only; that case says so.
                        let change = if old_col.only_collation_differs(new_col) {
                            "collation change"
                        } else {
                            "type change"
                        };
                        return AlterScriptParts {
                            script: format!(
                                "/* Column {} participates in partitioning; {change} requires table recreation. Data loss will occur! */\n{}{}",
                                block_comment_safe(&new_col.name),
                                drop_script,
                                to_table.get_script()
                            ),
                            replica_identity_after: None,
                            ancestor_pre_script: String::new(),
                            ancestor_retype_pre_script: String::new(),
                        };
                    }

                    let parts = new_col.alter_parts_recreating(
                        old_col,
                        use_drop,
                        caps.generated,
                        set_expression_only,
                        recreated_for_retype.contains(&new_col.name),
                    );
                    if ahead_of_retype {
                        // On its way to a plain column (`DROP EXPRESSION`),
                        // or the (empty) inline part of a re-created one.
                        retype_pre_script.push_str(&parts.inline);
                    } else {
                        column_alter_script.push_str(&parts.inline);
                    }
                    if !parts.is_deferred() {
                        continue;
                    }
                    if !parts.drop.is_empty()
                        && inherited.recreated_by_ancestor.contains(&new_col.name)
                    {
                        // An ancestor's drop + add re-creates this column (see
                        // `InheritedRecreates`). What its `ADD COLUMN` does not
                        // carry over is still this table's to set: the
                        // storage/compression/statistics changes, and the
                        // comment, which a recursing `ADD COLUMN` never copies.
                        generated_script.push_str(&parts.post);
                        if let Some(comment_script) = new_col.get_comment_script() {
                            generated_script.push_str(&comment_script);
                        }
                        continue;
                    }
                    // A column in the way of a retype is dropped ahead of it.
                    let drop_script = if ahead_of_retype {
                        retype_pre_script
                    } else {
                        &mut generated_script
                    };
                    drop_script.push_str(&parts.drop);
                    if !parts.drop.is_empty()
                        && let Some(descendants) = inherited.descendant_drops.get(&new_col.name)
                    {
                        for descendant in descendants {
                            drop_script.push_str(
                                &format!(
                                    "alter table {descendant} drop column if exists {};",
                                    new_col.name
                                )
                                .with_empty_lines(),
                            );
                        }
                    }
                    // A live re-add creates a NOT NULL constraint under the
                    // name TO gives it, not under PostgreSQL's own, which
                    // would clash with its re-add (see `named_not_null_clause`).
                    // A rewrite (`TableColumn::plain_needs_rewrite`) renames
                    // its new column instead of adding one: that only happens
                    // on PostgreSQL 14, which names no NOT NULL constraint.
                    match to_table.named_not_null_clause(&new_col.name) {
                        Some(clause)
                            if !parts.drop.is_empty() && !new_col.plain_needs_rewrite(old_col) =>
                        {
                            generated_script
                                .push_str(&new_col.get_add_script_with_not_null(&clause))
                        }
                        _ => generated_script.push_str(&parts.deferred),
                    }
                    generated_script.push_str(&parts.post);
                    generated_script.push_str(&parts.comment);
                }
            } else if typed_by_type {
                // The type's `ADD ATTRIBUTE … CASCADE` adds it as a bare
                // column, NULL in every row; what TO gives it on top of that
                // is set here.
                let added_by_type = TableColumn {
                    column_default: None,
                    is_nullable: true,
                    ..new_col.clone()
                };
                let alter = |to: &TableColumn, from: &TableColumn| {
                    to.alter_parts_recreating(
                        from,
                        use_drop,
                        caps.generated,
                        set_expression_only,
                        false,
                    )
                    .inline
                };
                if new_col.column_default.is_some()
                    && !new_col.is_identity
                    && !new_col.is_generated_column()
                {
                    // `ADD COLUMN … DEFAULT` gives the existing rows the
                    // default, and an ordinary table gets exactly that. The
                    // typed table gets it as `ADD COLUMN` would: the default
                    // first, then the rows `ADD ATTRIBUTE` left NULL filled
                    // with it, then NOT NULL, which those NULLs refused on
                    // every run ("column … contains null values").
                    let with_default = TableColumn {
                        is_nullable: true,
                        ..new_col.clone()
                    };
                    column_alter_script.push_str(&alter(&with_default, &added_by_type));
                    column_alter_script.push_str(
                        &format!(
                            "update {}.{} set {} = default where {} is null;",
                            new_col.schema, new_col.table, new_col.name, new_col.name
                        )
                        .with_empty_lines(),
                    );
                    column_alter_script.push_str(&alter(new_col, &with_default));
                } else {
                    column_alter_script.push_str(&alter(new_col, &added_by_type));
                }
                if let Some(comment_script) = new_col.get_comment_script() {
                    column_alter_script.push_str(&comment_script);
                }
            } else if !is_target_partition {
                column_alter_script.push_str(&new_col.get_add_script());
            }
        }

        // Collect column drops separately so they happen after constraint
        // drops. A typed table's type drops them (see `typed_by_type`).
        if !is_target_partition && !typed_by_type {
            let dropped: Vec<&TableColumn> = self
                .columns
                .iter()
                .filter(|old_col| !to_table.columns.iter().any(|c| c.name == old_col.name))
                .collect();
            // A generated column goes before the columns it reads: from
            // PostgreSQL 15 on, dropping one of those first fails ("other
            // objects depend on it"); on 14 it takes the generated column
            // along, and the drop of that then fails. Every other drop keeps
            // its column order.
            let reads_a_dropped = |col: &TableColumn| {
                dropped.iter().any(|other| {
                    other.name != col.name && col.generation_expression_reads(&other.name)
                })
            };
            let (first, rest): (Vec<&TableColumn>, Vec<&TableColumn>) =
                dropped.iter().partition(|col| reads_a_dropped(col));
            for old_col in first.into_iter().chain(rest) {
                let drop_cmd = old_col.get_drop_script();
                if !use_drop {
                    column_drop_script.push_str(&comment_out(&drop_cmd));
                } else if in_the_way.contains(old_col.name.as_str()) {
                    if before_ancestor_retype(old_col) {
                        ancestor_retype_pre_script.push_str(&drop_cmd);
                    } else {
                        retype_pre_script.push_str(&drop_cmd);
                    }
                } else {
                    column_drop_script.push_str(&drop_cmd);
                }
            }
        }

        // The table's constraints as the statements below find them: its
        // own, plus — once its detach or `NO INHERIT` has run — the CHECK
        // copies it inherited from the parents it leaves, which PostgreSQL
        // leaves behind as local constraints (see
        // `InheritedRecreates::detached_checks`).
        let detaches = use_drop && !detach_script.is_empty();
        let from_constraints: Vec<&TableConstraint> =
            self.constraints
                .iter()
                .chain(inherited.detached_checks.iter().filter(|copy| {
                    detaches && !self.constraints.iter().any(|c| c.name == copy.name)
                }))
                .collect();

        // Match constraints by name, except NOT NULL constraints, which are
        // matched by their column: a column has at most one (PostgreSQL
        // 18+), so the two are the same constraint whatever their names. An
        // auto-generated name differs between databases (collision
        // suffixes), and so does one that drifted — PostgreSQL keeps a NOT
        // NULL constraint's name through a column or table rename, and a
        // detached partition keeps its parent's — or that moved to another
        // column (see `not_null_renames`). A NOT NULL constraint is never
        // paired with a constraint of another kind that shares its name: the
        // one is dropped and the other added.
        let find_old = |new_c: &TableConstraint| -> Option<&TableConstraint> {
            match not_null_key(new_c) {
                Some(column) => from_constraints
                    .iter()
                    .find(|c| not_null_key(c).as_deref() == Some(column.as_str()))
                    .copied(),
                None => from_constraints
                    .iter()
                    .find(|c| c.name == new_c.name && not_null_key(c).is_none())
                    .copied(),
            }
        };
        let find_new = |old_c: &TableConstraint| -> Option<&TableConstraint> {
            match not_null_key(old_c) {
                Some(column) => to_table
                    .constraints
                    .iter()
                    .find(|c| not_null_key(c).as_deref() == Some(column.as_str())),
                None => to_table
                    .constraints
                    .iter()
                    .find(|c| c.name == old_c.name && not_null_key(c).is_none()),
            }
        };
        // A FROM NOT NULL constraint whose column TO keeps NOT NULL under a
        // NOT NULL constraint the dump does not show, as the column only
        // inherits it: TO's table is a partition (one the alter attaches,
        // say) or an inheritance child. Dropping it would leave the column
        // nullable, and no column diff puts that back; the attach merges it
        // into the inherited one instead.
        let to_inherits = to_table.partition_of.is_some() || !to_table.inherits_from.is_empty();
        let not_null_kept_by_to_column = |old_c: &TableConstraint| {
            to_inherits
                && not_null_key(old_c).is_some_and(|column| {
                    to_table.columns.iter().any(|c| {
                        !c.is_nullable
                            && TableConstraint::normalize_identifier_for_name(&c.name) == column
                    })
                })
        };
        // Columns this alter drops and re-adds, itself or through an
        // ancestor: the drop takes their NOT NULL constraint along, and the
        // re-add creates it under TO's name (see `named_not_null_clause`).
        let recreated_column_keys: HashSet<String> = self
            .recreated_columns(to_table, use_drop, caps.generated)
            .map(str::to_string)
            .chain(inherited.recreated_by_ancestor.iter().cloned())
            .map(|c| TableConstraint::normalize_identifier_for_name(&c))
            .collect();

        // The CHECK constraints the table's attach or `INHERIT` needs (see
        // `InheritedRecreates::attach_checks`). PostgreSQL refuses both
        // while the table lacks one of them ("child table is missing
        // constraint") or has one of its name with another definition, and
        // merges a matching one into the copy the table inherits, which can
        // no longer be dropped. So each is put in shape before the attach —
        // added, re-added, or given the enforceability TO's copy has — and a
        // merged one is never dropped. The shape is TO's own copy where TO
        // lists one (a merged copy, which `INHERIT` leaves local), with its
        // comment; the TO loop below leaves such a copy alone.
        let mut attach_pre_script = String::new();
        let attach_check_names: HashSet<&str> = if attach_script.is_empty() {
            HashSet::new()
        } else {
            inherited
                .attach_checks
                .iter()
                .map(|c| c.name.as_str())
                .collect()
        };
        if !attach_script.is_empty() {
            for copy in &inherited.attach_checks {
                let to_local = to_table.constraints.iter().find(|c| {
                    c.name == copy.name && c.constraint_type.eq_ignore_ascii_case("check")
                });
                let target = to_local.unwrap_or(copy);
                let Some(&local) = from_constraints.iter().find(|c| c.name == copy.name) else {
                    attach_pre_script.push_str(&target.get_script());
                    continue;
                };
                let mut uncommented = local.clone();
                uncommented.comment.clone_from(&target.comment);
                if uncommented == *target {
                    // The attach merges it as it is. A comment TO's own copy
                    // changes is set once the rest is done.
                    if to_local.is_some()
                        && local.comment != target.comment
                        && let Some(comment) = local.get_alter_script(target)
                    {
                        constraint_post_script.push_str(&comment);
                    }
                    continue;
                }
                let mut shaped = target.clone();
                if to_local.is_none() {
                    shaped.comment.clone_from(&local.comment);
                }
                if let Some(alter) = local.get_alter_script_with_caps(&shaped, constraint_caps) {
                    attach_pre_script.push_str(&alter);
                } else {
                    let drop_cmd = local.get_drop_script();
                    if use_drop {
                        attach_pre_script.push_str(&drop_cmd);
                    } else {
                        attach_pre_script.push_str(&comment_out(&drop_cmd));
                    }
                    attach_pre_script.push_str(&target.get_script());
                }
            }
        }
        let merged_on_attach = |constraint: &TableConstraint| {
            constraint.constraint_type.eq_ignore_ascii_case("check")
                && attach_check_names.contains(constraint.name.as_str())
        };
        // The CHECKs of new `INHERITS` ancestors that TO has this table only
        // inherit (see `InheritedRecreates::inherit_readds`): `INHERIT` would
        // leave the copy it needs local, so each holder drops its copy before
        // the `INHERIT`, parents first — the table drops its own below, as TO
        // lacks it — and adds it back after it, children first, which
        // recurses into the table.
        let mut inherit_pre_script = String::new();
        let mut inherit_post_script = String::new();
        if !attach_script.is_empty() {
            for holder_copy in &inherited.inherit_readds {
                inherit_pre_script.push_str(&holder_copy.get_drop_script());
            }
            for holder_copy in inherited.inherit_readds.iter().rev() {
                inherit_post_script.push_str(&holder_copy.get_script());
            }
        }
        // NOT NULL constraints paired by column under another name that
        // take TO's (see `not_null_renames`).
        let mut nn_renames: Vec<NotNullRename> = Vec::new();

        // Collect constraint changes; drop statements run before column drops
        for new_constraint in &to_table.constraints {
            let is_fk = new_constraint.constraint_type.to_lowercase() == "foreign key";
            // Skip inherited non-FK constraints for partition children;
            // these are managed by the parent table.  Truly-local partition
            // constraints (coninhcount == 0) are still diffed.
            if is_target_partition && !is_fk && new_constraint.coninhcount > 0 {
                continue;
            }
            // A CHECK the attach or `INHERIT` merges went into shape before
            // it (see `attach_pre_script`).
            if merged_on_attach(new_constraint) {
                continue;
            }
            if let Some(old_constraint) = find_old(new_constraint) {
                // Auto-named NOT NULL on the same column is semantically a no-op
                // regardless of the numeric suffix PG chose for the name.
                //
                // The field set checked here intentionally mirrors
                // `TableConstraint::PartialEq` *minus* the schema/name/table_name
                // identifiers (which we have already established describe the
                // same constraint via `find_old`) and the `definition` string
                // (whose only NOT NULL payload is the column name, also already
                // matched).
                //
                // `coninhcount` is deliberately NOT included: it is not part of
                // `TableConstraint::PartialEq` or `add_to_hasher` either, and
                // inherited NOT NULL constraints on partition children are
                // filtered out earlier by the
                // `is_target_partition && coninhcount > 0` guard, so a change
                // in `coninhcount` cannot reach this branch in the
                // partitioned-table case. For plain `CREATE TABLE ... INHERITS`
                // hierarchies a `coninhcount` flip would still be silently
                // accepted, but that matches the existing behavior for every
                // other constraint type and is intentionally out of scope here.
                let both_auto_nn = old_constraint.auto_not_null_column(&self.name).is_some()
                    && new_constraint
                        .auto_not_null_column(&to_table.name)
                        .is_some();
                let nn_equivalent = both_auto_nn
                    && old_constraint.is_enforced == new_constraint.is_enforced
                    && old_constraint.no_inherit == new_constraint.no_inherit
                    && old_constraint.comment == new_constraint.comment;

                // A NOT NULL constraint paired by column under another name
                // (see `find_old`) that differs in nothing else, or only in
                // its comment. Dropping it would clear the column's NOT NULL
                // — PostgreSQL refuses outright for a primary-key or
                // identity column — and the re-add under TO's name fails
                // while the column still has one. A name TO chose is renamed
                // to, and so is any name whose FROM name another TO
                // constraint takes; a name PostgreSQL generated is anonymous
                // otherwise, and the FROM name stays. A column the alter
                // re-creates gets TO's name from its re-add.
                if let Some(column) = not_null_key(new_constraint)
                    && old_constraint.name != new_constraint.name
                    && old_constraint.definition == new_constraint.definition
                    && old_constraint.is_enforced == new_constraint.is_enforced
                    && old_constraint.no_inherit == new_constraint.no_inherit
                {
                    let name_taken = to_table
                        .constraints
                        .iter()
                        .any(|c| c.name == old_constraint.name);
                    let to_chose = new_constraint
                        .auto_not_null_column(&to_table.name)
                        .is_none();
                    let column_recreated = recreated_column_keys.contains(&column);
                    let renamed = name_taken || (to_chose && !column_recreated);
                    if renamed {
                        nn_renames.push(NotNullRename {
                            from: old_constraint.name.clone(),
                            to: new_constraint.name.clone(),
                            column,
                        });
                    }
                    if !column_recreated && old_constraint.comment != new_constraint.comment {
                        let final_name = if renamed {
                            &new_constraint.name
                        } else {
                            &old_constraint.name
                        };
                        let mut from_named = old_constraint.clone();
                        from_named.name.clone_from(final_name);
                        let mut to_named = new_constraint.clone();
                        to_named.name.clone_from(final_name);
                        if let Some(comment) = from_named.get_alter_script(&to_named) {
                            constraint_post_script.push_str(&comment);
                        }
                    }
                    continue;
                }

                // A CHECK the inheritance family re-creates is dropped and
                // re-added, changed or not (see `InheritedRecreates::family_checks`).
                let family_recreate = in_family(old_constraint);
                if family_recreate || (old_constraint != new_constraint && !nn_equivalent) {
                    if !family_recreate
                        && let Some(alter_script) = old_constraint
                            .get_alter_script_with_caps(new_constraint, constraint_caps)
                    {
                        // A CHECK that stops being enforced lets go first:
                        // an `ALTER COLUMN ... TYPE` or `SET EXPRESSION`
                        // below rewrites the table and would still check
                        // the rows against it. One that becomes enforced
                        // validates the rows, so it waits for their final
                        // values in `constraint_post_script`.
                        if old_constraint.relaxes_enforcement_to(new_constraint) {
                            if ahead_of_ancestor(old_constraint) {
                                ancestor_pre_script.push_str(&alter_script);
                            } else {
                                constraint_pre_script.push_str(&alter_script);
                            }
                        } else if !is_fk {
                            constraint_post_script.push_str(&alter_script);
                        }
                    } else {
                        ri_index_disturbed |= drop_disturbs_ri_index(old_constraint);
                        let drop_cmd = constraint_drop_script(old_constraint);
                        let pre_script = if ahead_of_ancestor(old_constraint) {
                            &mut ancestor_pre_script
                        } else {
                            &mut constraint_pre_script
                        };
                        if use_drop {
                            pre_script.push_str(&drop_cmd);
                        } else {
                            pre_script.push_str(&comment_out(&drop_cmd));
                        }

                        if !is_fk {
                            constraint_post_script.push_str(&new_constraint.get_script());
                        }
                    }
                }
            } else if !is_fk
                && new_constraint
                    .auto_not_null_column(&to_table.name)
                    .is_none()
            {
                constraint_post_script.push_str(&new_constraint.get_script());
            }
            // An auto-named NOT NULL constraint present only on the TO side is not a
            // real object to ADD. PG18 surfaces a column's NOT NULL in pg_constraint as
            // `{table}_{col}_not_null`, but the column itself is already made NOT NULL —
            // by the column diff's `alter column ... set not null`, or by the inline
            // `not null` when the whole column is newly added. Emitting `ADD CONSTRAINT
            // ... NOT NULL` on top is redundant, uses PG18-only syntax that fails on
            // PG14–17, and leaves a named constraint the source never declared, which
            // the next compare would try to drop again.
        }

        for &old_constraint in &from_constraints {
            // Skip inherited non-FK constraint drops for partition children;
            // these are managed by the parent table.
            if is_target_partition
                && !old_constraint
                    .constraint_type
                    .eq_ignore_ascii_case("foreign key")
                && old_constraint.coninhcount > 0
            {
                continue;
            }
            // An auto-named NOT NULL constraint present only on the FROM side is not a
            // real object to DROP. On PG14–17 no such named constraint exists (it lives
            // only as `pg_attribute.attnotnull`), so `DROP CONSTRAINT {table}_{col}_not_null`
            // errors with "constraint does not exist"; on PG18 it exists but dropping it
            // is redundant — either the column is being dropped (which removes its NOT
            // NULL) or the column diff already emits `alter column ... drop not null`.
            // Nor is one whose column TO keeps NOT NULL (see
            // `not_null_kept_by_to_column`), or a CHECK the table's attach
            // merges into the copy it inherits (see `attach_pre_script`).
            if find_new(old_constraint).is_none()
                && old_constraint.auto_not_null_column(&self.name).is_none()
                && !not_null_kept_by_to_column(old_constraint)
                && !merged_on_attach(old_constraint)
            {
                ri_index_disturbed |= drop_disturbs_ri_index(old_constraint);
                let drop_cmd = constraint_drop_script(old_constraint);
                let pre_script = if ahead_of_ancestor(old_constraint) {
                    &mut ancestor_pre_script
                } else {
                    &mut constraint_pre_script
                };
                if use_drop {
                    pre_script.push_str(&drop_cmd);
                } else {
                    pre_script.push_str(&comment_out(&drop_cmd));
                }
            }
        }
        // After the drops, which free the names a rename may take.
        constraint_pre_script.push_str(&self.not_null_renames(nn_renames));

        // Table comment changes
        if self.comment != to_table.comment {
            let comment_stmt = if let Some(cmt) = &to_table.comment {
                format!(
                    "comment on table {}.{} is '{}';",
                    to_table.schema,
                    to_table.name,
                    escape_single_quotes(cmt)
                )
            } else {
                format!(
                    "comment on table {}.{} is null;",
                    to_table.schema, to_table.name
                )
            };
            constraint_post_script.append_block(&comment_stmt);
        }

        // Collect index updates (skip partition-inherited indexes, managed by parent)
        for new_index in &to_table.indexes {
            if new_index.is_partition_index {
                continue;
            }
            if let Some(old_index) = self.indexes.iter().find(|i| i.name == new_index.name) {
                if old_index != new_index {
                    // Check if only the comment changed (no need to drop+recreate)
                    let def_changed = !crate::dump::table_index::indexdefs_equivalent(
                        &old_index.indexdef,
                        &new_index.indexdef,
                    );
                    if def_changed {
                        let drop_cmd = format!(
                            "drop index if exists {}.{};",
                            new_index.schema, new_index.name
                        )
                        .with_empty_lines();
                        if use_drop {
                            index_drop_script.push_str(&drop_cmd);
                        } else {
                            index_drop_script.push_str(&comment_out_keep_blank_lines(&drop_cmd));
                        }
                        index_script.push_str(&new_index.get_script());
                    } else {
                        // Only comment changed
                        if let Some(ref cmt) = new_index.comment {
                            index_script.append_block(&format!(
                                "comment on index {}.{} is '{}';",
                                new_index.schema,
                                new_index.name,
                                cmt.replace('\'', "''")
                            ));
                        } else if old_index.comment.is_some() {
                            index_script.append_block(&format!(
                                "comment on index {}.{} is null;",
                                new_index.schema, new_index.name
                            ));
                        }
                    }
                }
            } else {
                index_script.push_str(&new_index.get_script());
            }
        }

        // Collect policy updates
        for new_policy in &to_table.policies {
            if let Some(old_policy) = self.policies.iter().find(|p| p.name == new_policy.name) {
                if old_policy != new_policy {
                    let drop_cmd = format!(
                        "drop policy if exists {} on {}.{};",
                        old_policy.name, self.schema, self.name
                    )
                    .with_empty_lines();
                    if use_drop {
                        policy_drop_script.push_str(&drop_cmd);
                    } else {
                        policy_drop_script.push_str(&comment_out_keep_blank_lines(&drop_cmd));
                    }
                    policy_script.push_str(&new_policy.get_script());
                }
            } else {
                policy_script.push_str(&new_policy.get_script());
            }
        }

        for old_index in &self.indexes {
            if old_index.is_partition_index {
                continue;
            }
            if !to_table.indexes.iter().any(|i| i.name == old_index.name) {
                let drop_cmd = format!(
                    "drop index if exists {}.{};",
                    old_index.schema, old_index.name
                )
                .with_empty_lines();
                if use_drop {
                    index_drop_script.push_str(&drop_cmd);
                } else {
                    index_drop_script.push_str(&comment_out_keep_blank_lines(&drop_cmd));
                }
            }
        }

        for old_policy in &self.policies {
            if !to_table.policies.iter().any(|p| p.name == old_policy.name) {
                let drop_cmd = format!(
                    "drop policy if exists {} on {}.{};",
                    old_policy.name, self.schema, self.name
                )
                .with_empty_lines();
                if use_drop {
                    policy_drop_script.push_str(&drop_cmd);
                } else {
                    policy_drop_script.push_str(&comment_out_keep_blank_lines(&drop_cmd));
                }
            }
        }

        if self.has_rowsecurity != to_table.has_rowsecurity {
            let stmt = if to_table.has_rowsecurity {
                format!(
                    "alter table {}.{} enable row level security;",
                    self.schema, self.name
                )
            } else {
                format!(
                    "alter table {}.{} disable row level security;",
                    self.schema, self.name
                )
            };
            row_security_script.append_block(&stmt);
        }

        let (trigger_drop_script, trigger_script) = if include_triggers {
            self.get_trigger_alter_parts(
                to_table,
                use_drop,
                &inherited.triggers_dropped_before_attach,
            )
        } else {
            (String::new(), String::new())
        };

        // Anything else that can take the FROM replica-identity index down or
        // clear a NOT NULL on one of its key columns: the index's own drop or
        // rebuild, and one of its columns that is dropped, recreated or loses
        // NOT NULL. Each needs `use_drop`: without it those statements are
        // only commented out. A needless leave + return is not free (see
        // `ReplicaIdentityPhases`), so only the index's own columns count,
        // and every column only when they are unknown.
        if use_drop && let Some(index) = from_ri_index {
            ri_index_disturbed = ri_index_disturbed
                || self
                    .index_alter_plan(to_table)
                    .drop
                    .iter()
                    .any(|i| i.name == index)
                || self
                    .recreated_columns(to_table, use_drop, caps.generated)
                    .any(|column| self.replica_identity_uses_column(column))
                || self.columns.iter().any(|old_col| {
                    self.replica_identity_uses_column(&old_col.name)
                        && to_table
                            .columns
                            .iter()
                            .find(|c| c.name == old_col.name)
                            .is_none_or(|new_col| !old_col.is_nullable && new_col.is_nullable)
                });
        }
        let ri_phases = self.replica_identity_phases(to_table, ri_index_disturbed);

        let mut script = String::new();
        if let Some(early) = &ri_phases.early {
            script.push_str(early);
        }
        // See `InheritedRecreates::triggers_dropped_before_attach`.
        if self.attaches_to_another_parent(to_table) {
            script.push_str(&self.dropped_trigger_drops(to_table, use_drop, |name| {
                inherited.triggers_dropped_before_attach.contains(name)
            }));
        }
        script.push_str(&detach_script);
        script.push_str(&constraint_pre_script);
        script.push_str(&retype_pre_script);
        script.push_str(&column_alter_script);
        script.push_str(&generated_script);
        if !defer_indexes {
            script.push_str(&index_drop_script);
        }
        if include_triggers {
            script.push_str(&trigger_drop_script);
        }
        script.push_str(&policy_drop_script);
        script.push_str(&column_drop_script);
        // `ATTACH PARTITION` and `INHERIT` want the table's columns to match
        // the parent's — every one of them, NOT NULL where the parent's is,
        // and for a partition nothing more — so they wait for the column
        // statements above, and for the constraint drops that free the
        // names and the NOT NULLs those change.
        script.push_str(&inherit_pre_script);
        script.push_str(&attach_pre_script);
        script.push_str(&attach_script);
        script.push_str(&inherit_post_script);
        script.push_str(&constraint_post_script);
        if !defer_indexes {
            script.push_str(&index_script);
        }
        if include_triggers {
            script.push_str(&trigger_script);
        }

        // When enabling row security, PostgreSQL requires row security to be
        // enabled before policies are created. Adjust the order accordingly.
        if !self.has_rowsecurity && to_table.has_rowsecurity {
            script.push_str(&row_security_script);
            script.push_str(&policy_script);
        } else {
            script.push_str(&policy_script);
            script.push_str(&row_security_script);
        }

        if self.owner != to_table.owner {
            script.push_str(&to_table.get_owner_script());
        }

        if self.access_method != to_table.access_method {
            if let Some(am) = &to_table.access_method {
                script.append_block(&format!(
                    "alter table {}.{} set access method {};",
                    to_table.schema,
                    to_table.name,
                    quote_ident(am)
                ));
            } else {
                script.append_block(&format!(
                    "alter table {}.{} set access method heap;",
                    to_table.schema, to_table.name
                ));
            }
        }

        if self.space != to_table.space
            && let Some(new_space) = &to_table.space
        {
            script.push_str(&format!(
                "alter table {}.{} set tablespace {};\n",
                to_table.schema,
                to_table.name,
                quote_ident(new_space)
            ));
        }

        // UNLOGGED / LOGGED change is emitted by the comparer in a
        // separate FK-ordered phase (see issue #180). Doing it inline
        // here would interleave SET UNLOGGED/LOGGED statements with the
        // alphabetical table order PostgreSQL rejects:
        //   * SET UNLOGGED fails if any LOGGED table references this
        //     one — referencers (leaves) must be converted first.
        //   * SET LOGGED   fails if this table references any UNLOGGED
        //     one — referenced tables (roots) must be converted first.
        // Both directions therefore need a topological pass that this
        // per-table function can't perform. The comparer collects
        // `Self::persistence_change_for(&to_table)` results from every
        // pair and emits them in the right order at end of compare.

        // Storage parameters change
        if self.storage_parameters != to_table.storage_parameters {
            // Reset old parameters
            if let Some(old_params) = &self.storage_parameters
                && !old_params.is_empty()
            {
                let param_names: Vec<&str> = old_params
                    .iter()
                    .filter_map(|p| p.split('=').next())
                    .collect();
                if !param_names.is_empty() {
                    script.append_block(&format!(
                        "alter table {}.{} reset ({});",
                        to_table.schema,
                        to_table.name,
                        param_names.join(", ")
                    ));
                }
            }
            // Set new parameters
            if let Some(new_params) = &to_table.storage_parameters
                && !new_params.is_empty()
            {
                script.append_block(&format!(
                    "alter table {}.{} set ({});",
                    to_table.schema,
                    to_table.name,
                    new_params.join(", ")
                ));
            }
        }

        // Replica identity change. This is after every column, constraint and
        // index statement above, so a `USING INDEX` finds its index built and
        // the NOT NULLs on its key columns added and validated. Leaving an
        // index is the early statement at the top of the script instead.
        // When its index is only there after statements the comparer emits
        // later, the comparer runs it: after this table's own (post-commit
        // index builds, re-created column dependents), or, for a partition's
        // index PostgreSQL creates from the parent, after the table section
        // (`replica_identity_script_after_tables`).
        let mut replica_identity_after = None;
        if let Some(late) = ri_phases.late
            && !to_table.replica_identity_waits_for_parent(Some(self))
        {
            if self.replica_identity_waits_for_comparer(
                to_table,
                use_drop,
                defer_indexes,
                caps.generated,
            ) {
                replica_identity_after = Some(late);
            } else {
                script.push_str(&late);
            }
        }

        // Force row-level security change
        if self.force_rowsecurity != to_table.force_rowsecurity {
            let stmt = if to_table.force_rowsecurity {
                format!(
                    "alter table {}.{} force row level security;",
                    to_table.schema, to_table.name
                )
            } else {
                format!(
                    "alter table {}.{} no force row level security;",
                    to_table.schema, to_table.name
                )
            };
            script.append_block(&stmt);
        }

        AlterScriptParts {
            script,
            replica_identity_after,
            ancestor_pre_script,
            ancestor_retype_pre_script,
        }
    }

    /// Get script for altering the table (including triggers)
    ///
    /// Uses only the in-place forms every supported server accepts; see
    /// [`Table::get_alter_script_with_caps`].
    pub fn get_alter_script(&self, to_table: &Table, use_drop: bool) -> String {
        self.get_alter_script_with_caps(to_table, use_drop, TableAlterCaps::default())
    }

    /// Like [`Table::get_alter_script`], for a migrated server with the
    /// in-place capabilities in `caps`.
    pub fn get_alter_script_with_caps(
        &self,
        to_table: &Table,
        use_drop: bool,
        caps: TableAlterCaps,
    ) -> String {
        self.build_alter_script(
            to_table,
            use_drop,
            true,
            false,
            caps,
            &InheritedRecreates::default(),
        )
    }

    /// Get script for altering the table without triggers (for deferred trigger creation)
    pub fn get_alter_script_without_triggers(&self, to_table: &Table, use_drop: bool) -> String {
        self.get_alter_script_without_triggers_with_caps(
            to_table,
            use_drop,
            TableAlterCaps::default(),
        )
    }

    /// Like [`Table::get_alter_script_without_triggers`], for a migrated
    /// server with the in-place capabilities in `caps`.
    pub fn get_alter_script_without_triggers_with_caps(
        &self,
        to_table: &Table,
        use_drop: bool,
        caps: TableAlterCaps,
    ) -> String {
        self.build_alter_script(
            to_table,
            use_drop,
            false,
            false,
            caps,
            &InheritedRecreates::default(),
        )
    }

    /// Like [`Table::get_alter_script_without_triggers`] but omits index
    /// creation/drop statements. Used by the production output path, which
    /// emits index changes separately (concurrently / partition-aware) via
    /// [`Table::index_alter_plan`].
    pub fn get_alter_script_without_triggers_no_indexes(
        &self,
        to_table: &Table,
        use_drop: bool,
    ) -> String {
        self.get_alter_script_without_triggers_no_indexes_with_caps(
            to_table,
            use_drop,
            TableAlterCaps::default(),
        )
    }

    /// Like [`Table::get_alter_script_without_triggers_no_indexes`], for a
    /// migrated server with the in-place capabilities in `caps`.
    pub fn get_alter_script_without_triggers_no_indexes_with_caps(
        &self,
        to_table: &Table,
        use_drop: bool,
        caps: TableAlterCaps,
    ) -> String {
        self.build_alter_script(
            to_table,
            use_drop,
            false,
            true,
            caps,
            &InheritedRecreates::default(),
        )
    }

    /// The comparer's form of [`Table::get_alter_script_without_triggers_with_caps`]
    /// (or, with `defer_indexes`, of its `_no_indexes` sibling): the same
    /// script, with a classical-inheritance family's shared generated-column
    /// drop + add laid out by `inherited` (see [`InheritedRecreates`]), and
    /// without the CHECK statements [`Table::ancestor_rewrite_pre_script`]
    /// returns when `inherited` says an ancestor rewrites this table first,
    /// or that its inheritance family drops a CHECK ahead of the alters.
    ///
    /// Also returns the `REPLICA IDENTITY USING INDEX` the script leaves out
    /// because its index is only there after statements the comparer emits
    /// for this table later: the post-commit build of that index
    /// (`defer_indexes`), or the re-creation of what a column drop + add took
    /// down. The comparer runs it after those.
    pub(crate) fn get_alter_script_in_family(
        &self,
        to_table: &Table,
        use_drop: bool,
        defer_indexes: bool,
        caps: TableAlterCaps,
        inherited: &InheritedRecreates,
    ) -> (String, Option<String>) {
        let parts = self.build_alter_script_parts(
            to_table,
            use_drop,
            false,
            defer_indexes,
            caps,
            inherited,
        );
        (parts.script, parts.replica_identity_after)
    }

    /// The statements [`Table::get_alter_script_in_family`] leaves out
    /// when `inherited` says an ancestor's alter rewrites this table's rows
    /// first (see [`InheritedRecreates::rewritten_by_ancestor`]): the
    /// relaxing in-place `ALTER CONSTRAINT ... NOT ENFORCED` and the `DROP
    /// CONSTRAINT` of each local CHECK constraint the alter changes or
    /// removes. Also the `DROP CONSTRAINT` of each CHECK the inheritance
    /// family drops and re-adds together (see
    /// [`InheritedRecreates::family_checks`]). The comparer runs them ahead
    /// of the table alters. Empty otherwise, and for a table the alter drops
    /// and recreates.
    pub(crate) fn ancestor_rewrite_pre_script(
        &self,
        to_table: &Table,
        use_drop: bool,
        caps: TableAlterCaps,
        inherited: &InheritedRecreates,
    ) -> String {
        self.build_alter_script_parts(to_table, use_drop, false, false, caps, inherited)
            .ancestor_pre_script
    }

    /// The statements [`Table::get_alter_script_in_family`] leaves out
    /// when `inherited` says an inheritance ancestor's alter retypes a
    /// column of this table (see [`InheritedRecreates::retyped_by_ancestor`]):
    /// the `DROP COLUMN` — or, on its way to a plain column, the `DROP
    /// EXPRESSION` — of each generated column of this table that reads such
    /// a column. PostgreSQL refuses the ancestor's recursing `ALTER COLUMN
    /// ... TYPE` while it is there. The comparer runs them ahead of the
    /// table alters, after every table's
    /// [`Table::ancestor_rewrite_pre_script`]; this table's alter adds back
    /// the ones TO keeps. Empty otherwise.
    pub(crate) fn ancestor_retype_pre_script(
        &self,
        to_table: &Table,
        use_drop: bool,
        caps: TableAlterCaps,
        inherited: &InheritedRecreates,
    ) -> String {
        self.build_alter_script_parts(to_table, use_drop, false, false, caps, inherited)
            .ancestor_retype_pre_script
    }

    /// The columns a table typed by the same type in FROM (`self`) and TO
    /// (`to_table`) adds, drops or retypes, as `added x`, `dropped y` and
    /// `retyped z (integer -> bigint)`, in TO's and then FROM's column
    /// order; empty for any other table. `ALTER TABLE` can make none of
    /// these on a typed table, and `build_alter_script` leaves them to the
    /// type's `ALTER TYPE … CASCADE`, so the comparer warns when that type
    /// is not compared.
    pub fn typed_column_changes(&self, to_table: &Table) -> Vec<String> {
        if self.typed_table_type.is_none() || self.typed_table_type != to_table.typed_table_type {
            return Vec::new();
        }
        let mut changes = Vec::new();
        for column in &to_table.columns {
            match self.columns.iter().find(|c| c.name == column.name) {
                None => changes.push(format!("added {}", column.name)),
                Some(old) if old.type_differs(column) => changes.push(format!(
                    "retyped {} ({} -> {})",
                    column.name,
                    old.render_type_clause(),
                    column.render_type_clause()
                )),
                Some(_) => {}
            }
        }
        for column in &self.columns {
            if !to_table.columns.iter().any(|c| c.name == column.name) {
                changes.push(format!("dropped {}", column.name));
            }
        }
        changes
    }

    /// True when comparing `self` (FROM) against `to_table` (TO) would
    /// force `build_alter_script` down a wholesale `DROP TABLE` +
    /// `CREATE TABLE` path rather than an in-place ALTER. The comparer
    /// uses this to drive the `recreated_tables` flag for grants
    /// (issue #180) and to skip Path B column-dependent restoration
    /// (issue #188) — when the table is recreated wholesale, all
    /// dependents are already re-emitted as part of `CREATE TABLE`,
    /// so the column-dependent graph walk would just duplicate work.
    ///
    /// Conditions mirror the early-return branches in
    /// `build_alter_script`. `column_type_change_forces_recreate`
    /// keeps the partition-key column check in a single helper so the
    /// predicate and the SQL emission cannot drift apart.
    pub fn will_be_dropped_and_recreated(&self, to_table: &Table) -> bool {
        if self.partition_key != to_table.partition_key {
            return true;
        }
        for new_col in &to_table.columns {
            if let Some(old_col) = self.columns.iter().find(|c| c.name == new_col.name)
                && self.column_type_change_forces_recreate(old_col, new_col)
            {
                return true;
            }
        }
        false
    }

    /// True when a column's type changed *and* the column either
    /// participates in this table's partition key or the table is a
    /// partition child — both cases require dropping and recreating
    /// the table (PostgreSQL forbids in-place type changes on
    /// partition-key columns and on inherited columns of partition
    /// children).
    ///
    /// Shared between `build_alter_script`'s emission path and
    /// `will_be_dropped_and_recreated`'s predicate so the recreate
    /// decision lives in exactly one place.
    fn column_type_change_forces_recreate(
        &self,
        old_col: &TableColumn,
        new_col: &TableColumn,
    ) -> bool {
        if !old_col.type_differs(new_col) {
            return false;
        }
        let is_partition_child = self.partition_of.is_some();
        let in_partition_key = self.partition_key.as_ref().is_some_and(|pk| {
            let col_lower = new_col.name.to_lowercase();
            extract_partition_key_identifiers(pk).contains(&col_lower)
        });
        is_partition_child || in_partition_key
    }

    /// Returns `Some(target_is_unlogged)` when this table's logging
    /// status differs from `to_table`. The comparer collects these per
    /// (FROM, TO) pair and emits the resulting `ALTER TABLE ... SET
    /// LOGGED|UNLOGGED` statements in FK-topological order (see issue
    /// #180): inline emission in `build_alter_script` would honour the
    /// alphabetical table iteration order, which PostgreSQL rejects
    /// whenever a referencing-table / referenced-table pair flip
    /// persistence in the same migration.
    pub fn persistence_change_for(&self, to_table: &Table) -> Option<bool> {
        if self.is_unlogged != to_table.is_unlogged {
            Some(to_table.is_unlogged)
        } else {
            None
        }
    }

    fn get_trigger_alter_parts(
        &self,
        to_table: &Table,
        use_drop: bool,
        dropped_before_attach: &HashSet<String>,
    ) -> (String, String) {
        let mut trigger_script = String::new();
        let mut trigger_drop_script = String::new();

        for new_trigger in &to_table.triggers {
            if let Some(old_trigger) = self.triggers.iter().find(|t| t.name == new_trigger.name) {
                if old_trigger != new_trigger {
                    if old_trigger.definition != new_trigger.definition {
                        // Definition changed: emit drop in drop section, new trigger in create section
                        let drop_cmd = format!(
                            "drop trigger if exists {} on {}.{};",
                            old_trigger.name, self.schema, self.name
                        )
                        .with_empty_lines();
                        if use_drop {
                            trigger_drop_script.push_str(&drop_cmd);
                        } else {
                            trigger_drop_script.push_str(&comment_out_keep_blank_lines(&drop_cmd));
                        }
                        trigger_script
                            .push_str(&new_trigger.get_script(&to_table.schema, &to_table.name));
                        // Handle enabled/comment changes too
                        let alter = old_trigger.get_alter_script(
                            new_trigger,
                            &self.schema,
                            &self.name,
                            use_drop,
                        );
                        // get_alter_script already emitted drop + definition; skip those lines,
                        // only take the enable/comment parts (lines after the new definition)
                        // by using a fresh get_alter_script call that ignores definition changes:
                        // Actually, easier: only emit enabled/comment changes here
                        let _ = alter; // already handled above
                        if old_trigger.enabled != new_trigger.enabled {
                            let stmt = match new_trigger.enabled.as_str() {
                                "D" => format!(
                                    "alter table {}.{} disable trigger {};",
                                    to_table.schema, to_table.name, new_trigger.name
                                ),
                                "R" => format!(
                                    "alter table {}.{} enable replica trigger {};",
                                    to_table.schema, to_table.name, new_trigger.name
                                ),
                                "A" => format!(
                                    "alter table {}.{} enable always trigger {};",
                                    to_table.schema, to_table.name, new_trigger.name
                                ),
                                _ => format!(
                                    "alter table {}.{} enable trigger {};",
                                    to_table.schema, to_table.name, new_trigger.name
                                ),
                            };
                            trigger_script.append_block(&stmt);
                        }
                        if old_trigger.comment != new_trigger.comment {
                            if let Some(ref comment) = new_trigger.comment {
                                trigger_script.append_block(&format!(
                                    "comment on trigger {} on {}.{} is '{}';",
                                    new_trigger.name,
                                    to_table.schema,
                                    to_table.name,
                                    comment.replace('\'', "''")
                                ));
                            } else {
                                trigger_script.append_block(&format!(
                                    "comment on trigger {} on {}.{} is null;",
                                    new_trigger.name, to_table.schema, to_table.name
                                ));
                            }
                        }
                    } else {
                        // Only enabled/comment changed — no drop needed
                        let alter = old_trigger.get_alter_script(
                            new_trigger,
                            &self.schema,
                            &self.name,
                            use_drop,
                        );
                        trigger_script.push_str(&alter);
                    }
                }
            } else {
                trigger_script.push_str(&new_trigger.get_script(&to_table.schema, &to_table.name));
            }
        }

        // Those the alter drops before an attach are not dropped again (see
        // `InheritedRecreates::triggers_dropped_before_attach`).
        let attaches = self.attaches_to_another_parent(to_table);
        trigger_drop_script.push_str(&self.dropped_trigger_drops(to_table, use_drop, |name| {
            !(attaches && dropped_before_attach.contains(name))
        }));

        (trigger_drop_script, trigger_script)
    }

    /// `drop trigger if exists` for each of this table's own triggers that
    /// `to_table` does not have and whose name `pick` accepts, commented out
    /// without `use_drop`.
    fn dropped_trigger_drops(
        &self,
        to_table: &Table,
        use_drop: bool,
        pick: impl Fn(&String) -> bool,
    ) -> String {
        let mut script = String::new();
        for old_trigger in &self.triggers {
            if pick(&old_trigger.name)
                && !to_table.triggers.iter().any(|t| t.name == old_trigger.name)
            {
                let drop_cmd = format!(
                    "drop trigger if exists {} on {}.{};",
                    old_trigger.name, self.schema, self.name
                )
                .with_empty_lines();
                if use_drop {
                    script.push_str(&drop_cmd);
                } else {
                    script.push_str(&comment_out_keep_blank_lines(&drop_cmd));
                }
            }
        }
        script
    }

    /// Whether this in-place alter attaches the table to a partitioned
    /// parent it is not a partition of in FROM: a table becoming a
    /// partition, or a partition moving to another parent.
    ///
    /// `ATTACH PARTITION` clones the parent's row triggers onto the table,
    /// and fails ("trigger ... already exists") while the table still has
    /// a trigger of its own by one of their names. Such a trigger — TO no
    /// longer lists it: a partition cannot have one besides the clone — is
    /// therefore dropped ahead of the attach, in the table's alter, rather
    /// than with the other trigger statements after every table, by when
    /// the name belongs to the clone, whose drop PostgreSQL refuses. The
    /// comparer names them (see
    /// [`InheritedRecreates::triggers_dropped_before_attach`]).
    pub(crate) fn attaches_to_another_parent(&self, to_table: &Table) -> bool {
        to_table.partition_of.is_some()
            && to_table.partition_bound.is_some()
            && self.partition_of != to_table.partition_of
    }

    /// Whether the in-place alter to `to_table` detaches this table from
    /// its partitioned parent, and whether it attaches it to one: a table
    /// becoming or ceasing to be a partition, moving to another parent, or
    /// changing its bound, which is a detach and an attach to the same
    /// parent.
    pub(crate) fn partition_moves(&self, to_table: &Table) -> (bool, bool) {
        let moves = self.partition_of != to_table.partition_of
            || (self.partition_of.is_some() && self.partition_bound != to_table.partition_bound);
        (
            moves && self.partition_of.is_some(),
            moves && to_table.partition_of.is_some() && to_table.partition_bound.is_some(),
        )
    }

    /// The classical-inheritance (`INHERITS`) parents the in-place alter to
    /// `to_table` leaves (`NO INHERIT`) and joins (`INHERIT`): everything
    /// after the longest run of parents both sides list in the same order.
    /// PostgreSQL appends a parent `INHERIT` adds to the end of the list, so
    /// a parent that only moves in it is left and joined again.
    pub(crate) fn inheritance_moves<'t>(
        &'t self,
        to_table: &'t Table,
    ) -> (&'t [String], &'t [String]) {
        let kept = self
            .inherits_from
            .iter()
            .zip(&to_table.inherits_from)
            .take_while(|(from, to)| from == to)
            .count();
        (&self.inherits_from[kept..], &to_table.inherits_from[kept..])
    }

    /// The statements renaming this table's NOT NULL constraints as
    /// `renames` asks, in an order where no rename takes a name another one
    /// has yet to give up: a cycle, such as two columns swapping their
    /// constraints' names, goes through a temporary name (see
    /// `Table::temporary_not_null_name`). Each statement is guarded (see
    /// `Table::guarded_not_null_rename`).
    fn not_null_renames(&self, mut renames: Vec<NotNullRename>) -> String {
        let mut script = String::new();
        while !renames.is_empty() {
            let ready = renames
                .iter()
                .position(|r| !renames.iter().any(|other| other.from == r.to));
            let step = match ready {
                Some(i) => renames.remove(i),
                None => {
                    let temporary = self.temporary_not_null_name(&renames[0].from);
                    let step = NotNullRename {
                        from: renames[0].from.clone(),
                        to: temporary.clone(),
                        column: renames[0].column.clone(),
                    };
                    renames[0].from = temporary;
                    step
                }
            };
            script.push_str(&self.guarded_not_null_rename(&step));
        }
        script
    }

    /// The name `Table::not_null_renames` moves NOT NULL constraint `name`
    /// out of the way under: `pgc_nn_` and 16 hex digits of a hash of the
    /// table and `name` — a plain identifier, the same on every run.
    fn temporary_not_null_name(&self, name: &str) -> String {
        let mut hasher = Sha256::new();
        for part in [&self.schema, &self.name, name] {
            hasher.update((part.len() as u32).to_be_bytes());
            hasher.update(part.as_bytes());
        }
        let digest = format!("{:x}", hasher.finalize());
        format!("pgc_nn_{}", &digest[..16])
    }

    /// `ALTER TABLE ... RENAME CONSTRAINT` for one of `Table::not_null_renames`,
    /// in a `DO` block that runs it only while the table's NOT NULL
    /// constraint on the column still has the old name. Re-running a partly
    /// (or wholly) applied migration then skips it, where a bare rename
    /// would fail ("constraint ... does not exist") or, after a cycle, take
    /// a name from the wrong column. The block is dollar-quoted with a tag
    /// its body does not contain (see `dollar_quote_tag`).
    fn guarded_not_null_rename(&self, rename: &NotNullRename) -> String {
        let table = format!("{}.{}", self.schema, self.name);
        let body = format!(
            "\n\
             begin\n    \
                 if exists (\n        \
                     select 1 from pg_catalog.pg_constraint c\n        \
                     join pg_catalog.pg_attribute a\n          \
                       on a.attrelid = c.conrelid and a.attnum = c.conkey[1]\n        \
                     where c.conrelid = '{table_literal}'::regclass\n          \
                       and c.contype = 'n'\n          \
                       and c.conname = '{name_literal}'\n          \
                       and a.attname = '{column_literal}'\n    \
                 ) then\n        \
                     alter table {table} rename constraint {from} to {to};\n    \
                 end if;\n\
             end\n",
            table_literal = escape_single_quotes(&table),
            name_literal = escape_single_quotes(&TableConstraint::normalize_identifier_for_name(
                &rename.from
            )),
            column_literal = escape_single_quotes(&rename.column),
            from = rename.from,
            to = rename.to,
        );
        let tag = crate::dump::acl::dollar_quote_tag(&body);
        format!("do {tag}{body}{tag};").with_empty_lines()
    }

    /// Trigger-only alter script
    pub fn get_trigger_alter_script(&self, to_table: &Table, use_drop: bool) -> String {
        let (drop_part, create_part) =
            self.get_trigger_alter_parts(to_table, use_drop, &HashSet::new());
        format!("{}{}", drop_part, create_part)
    }

    /// The comparer's form of [`Table::get_trigger_alter_script`]: without
    /// the drops the table's alter runs before it attaches the table (see
    /// [`InheritedRecreates::triggers_dropped_before_attach`]).
    pub(crate) fn get_trigger_alter_script_in_family(
        &self,
        to_table: &Table,
        use_drop: bool,
        inherited: &InheritedRecreates,
    ) -> String {
        let (drop_part, create_part) = self.get_trigger_alter_parts(
            to_table,
            use_drop,
            &inherited.triggers_dropped_before_attach,
        );
        format!("{drop_part}{create_part}")
    }
}

#[cfg(test)]
#[path = "tests/table.rs"]
mod tests;
