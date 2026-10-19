//! The [`Comparer`] — reads two [`Dump`]s and emits the
//! migration SQL that makes `FROM` equal to `TO`.
//!
//! Output is assembled from several ordered buffers rather than one string,
//! because PostgreSQL dependency rules do not match the order objects are
//! compared in. They are concatenated as:
//!
//! ```text
//! script  →  sequence_post  →  domain_constraint_post  →  type_post  →  enum_post  →  trigger_post
//! ```
//!
//! With `--use-single-transaction`, `enum_add_value` is spliced in *before*
//! the `begin;`: a value `ALTER TYPE … ADD VALUE` adds inside a transaction
//! block cannot be used until that transaction commits.
//!
//! The comparer also tracks cross-cutting state that individual passes need:
//! `dropped_views`, `predropped_routines` and `recreated_tables` coordinate
//! drop/recreate sequencing, `rules_to_recreate` and
//! `views_recreated_by_cascade` tell the rules pass which rules the migration
//! removed on the way (dropped ahead of the view and table changes, taken
//! along with the table they sit on, or taken along with a view a routine's
//! `CASCADE` dropped), and `serial_columns` keeps owned sequences from being
//! emitted independently of their table.
//!
//! A table whose alter has to drop and recreate it (a partition-key change,
//! say) takes along every partition, the sequences its columns own and the
//! foreign keys referencing it. `recreated_families` works out those tables
//! and partitions once, for every pass that prepares for the drop: the
//! partitions are created again from TO whether they changed or not, the
//! sequences are detached first and owned again after, and the foreign keys
//! are dropped first (`recreate_fk_predrops`) and added back after. A table
//! that TO puts under a recreated one — attached, moved, or itself recreated
//! — is altered only once that one is back (`late_table_keys`), or the `DROP
//! TABLE` would take it along.
//!
//! Routines with a SQL-standard body are hard dependents of what they use
//! ([`Routine::sql_body_dependencies`]): `drop_views` drops the ones in the way
//! of a relation, routine or operator change ahead of it, and the ones that
//! need an operator or cast created later are held back until after
//! `compare_casts` (`deferred_routine_script`). Rules are hard dependents too:
//! `drop_views` drops the ones in the way first, and `compare_rules`, which
//! runs after the held-back routines, recreates them. What the migration takes
//! away underneath all three — views, SQL-body routines and rules — is worked
//! out once, by `relation_changes`.
//!
//! A routine dropped with `CASCADE` — ahead of the relation DDL by
//! `drop_views`, or in the routine phase for a return-type, argument or kind
//! change — takes its dependents along, and `recreate_routine_dependents`
//! puts back every kind pgc models; a domain default calling it is detached
//! first, since PostgreSQL would drop the whole domain with it. A view calling
//! a routine the routine phase drops goes too, and with it everything reading
//! the view; when something does, `drop_views` drops the view ahead of the
//! relation DDL instead (`views_cascaded_with_readers`), so its readers are
//! dropped with it and all of them re-created in dependency order. The routines
//! the routine phase's `CASCADE` takes through their argument defaults or
//! bodies (`cascade_victim_routines`) are re-created after it. What the table
//! phase would create calling a routine `drop_views` dropped is left out of it
//! (`enter_table_phase_view`) and created once the routine is back. A stored
//! generated column calling such a routine goes with it too, and so does
//! everything depending on the column (`cascade_readded`): `drop_views` drops
//! what reads it first, the table phase leaves its type to the re-add, and
//! the routine phase re-adds it and then restores the rest. A routine
//! whose `CASCADE` would take something pgc cannot re-create (a partition key,
//! a range or base type, a foreign-data wrapper, an operator or a cast), or
//! that the table changes need in place, is not dropped ahead of anything, and
//! the routine phase drops one such without `CASCADE`: the statement that
//! needs it gone then fails, as it did before pgc dropped such routines at
//! all.

use crate::comparer::production::{self, ChildRef, PartitionContext};
use crate::config::grants_mode::GrantsMode;
use crate::dump::acl;
use crate::dump::column_dependent::{ColumnDependent, ColumnDependentKind};
use crate::dump::foreign_table::ForeignTable;
use crate::dump::pg_type::{CompositeAttribute, CompositeUsage, PgType};
use crate::dump::publication::{PublicationAlterContext, SLOT_NAME_RULE, SlotNameRules};
use crate::dump::routine::{AGGREGATE_SUPPORT_MIN_VERSION, SET_LIST_TO_NULL_MIN_VERSION};
use crate::dump::sequence::{Sequence, UNLOGGED_SEQUENCE_MIN_VERSION};
use crate::dump::statistic::Statistic;
use crate::dump::table::{IndexAlterPlan, InheritedRecreates, TableAlterCaps};
use crate::dump::table_column::{GeneratedAlterCaps, TableColumn};
use crate::dump::table_constraint::{ConstraintAlterCaps, TableConstraint};
use crate::dump::table_index::TableIndex;
use crate::dump::table_policy::TablePolicy;
use crate::dump::table_trigger::TableTrigger;
use crate::dump::{core::Dump, routine::Routine, table::Table, view::View};
use crate::utils::string_extensions::{
    StringExt, block_comment_safe, comment_out, comment_out_keep_blank_lines, line_comment_safe,
    unquote_ident,
};
use sqlx::postgres::types::Oid;
use std::{
    borrow::Cow,
    cmp::Reverse,
    collections::{BinaryHeap, HashMap, HashSet, VecDeque},
    fs::File,
    io::{Error, Write},
};

/// This is a Dump comparer that generates a script comparing two PostgreSQL dumps.
/// The result script, if it will be applied on "from" dump database, will make it equal to "to" dump database.
pub struct Comparer {
    // The dump to compare from
    from: Dump,
    // The dump to compare to
    to: Dump,
    // Whether to use DROP statements in the output
    use_drop: bool,
    // True - if explicit begin...commit statement has to be added; False - otherwise
    use_single_transaction: bool,
    // Whether to include comments in the output script
    use_comments: bool,
    // How to handle grants (privileges) during comparison
    grants_mode: GrantsMode,
    // Whether to emit a migration script that is safe/convenient to run on a
    // live production database (concurrent index builds, partition-aware index
    // creation, NOT VALID + VALIDATE for foreign keys, concurrent index drops).
    output_for_production: bool,
    /// Emit `set check_function_bodies = false;` as the migration's first
    /// statement (issue #240). Off by default; when off the output is
    /// byte-for-byte what it always was.
    guard_sql_routine_bodies: bool,

    // The script that will be generated
    script: String,
    // Statements that must run after the main transaction commits, because
    // CONCURRENTLY / VALIDATE CONSTRAINT / ATTACH PARTITION cannot run inside a
    // transaction block. Only populated when `output_for_production` is set.
    production_post_script: String,
    enum_pre_script: String,
    enum_post_script: String,
    // `alter type … add value` statements of a single-transaction migration,
    // spliced in before its `begin;` (see `compare_enums`).
    enum_add_value_script: String,
    type_post_script: String,
    // Composite type alterations (`ALTER TYPE … ADD / DROP / ALTER ATTRIBUTE`),
    // run after `drop_views` and `release_publication_blockers` rather than
    // in the types section (see `compare_types`).
    composite_alter_script: String,
    // Domain `add constraint` / `validate constraint` statements, run after
    // the tables, routines, operators and casts (see `compare_types`).
    domain_constraint_post_script: String,
    sequence_post_script: String,
    trigger_post_script: String,
    dropped_views: HashMap<String, bool>,
    // FROM routines with a SQL-standard body that `drop_views` dropped ahead of
    // the relation DDL, keyed (schema, name, arguments) as dumped. The routine
    // phase re-creates the ones TO still has, as new routines, and does not
    // drop them a second time.
    predropped_routines: HashSet<(String, String, String)>,
    // FROM routines, keyed like `predropped_routines`, that the routine
    // phase's `DROP … CASCADE` of another routine takes along although TO
    // keeps them: an argument default or a SQL-standard body calls the
    // dropped routine, or an aggregate is built on it (see
    // `Comparer::plan_cascade_victims`). Re-created, like
    // `predropped_routines`, right after the routine they call.
    cascade_victim_routines: HashSet<(String, String, String)>,
    // Routines and views `compare_routines_and_views` held back because a
    // SQL-standard body among them uses an operator or cast that
    // `compare_operators` / `compare_casts` only create later; emitted right
    // after those by `emit_deferred_routines`.
    deferred_routine_script: String,
    // Tracks tables that are dropped and recreated during the migration
    // (e.g. partition key change), plus the partition children described
    // below, which are not.
    recreated_tables: HashSet<String>,
    // The subset of `recreated_tables` (same `table_key` form) whose
    // `DROP TABLE` the migration really runs, so everything that depended on
    // the table — its extended statistics, its publication rows, its table
    // and column privileges — is gone and must be created again (a new table
    // receives the default privileges instead). `recreated_tables` also holds
    // partition children whose column type changes:
    // `Table::will_be_dropped_and_recreated` answers true for them, but the
    // alter path leaves those changes to the parent and never drops the child.
    dropped_and_recreated_tables: HashSet<String>,
    // Columns (`Table::schema`, `Table::name`, `TableColumn::name`) that the
    // migration drops and re-adds, with `use_drop`, in a table it does not
    // drop: a generated-column change `ALTER COLUMN` cannot make (see
    // `TableColumn::would_drop_and_re_add_with_caps`), including the copies
    // in partitions and `INHERITS` followers that the parent's recursing
    // drop + add re-creates, and the generated columns a routine's `DROP ...
    // CASCADE` takes and `recreate_routine_dependents` re-adds (see
    // `cascade_readded`). The new column starts with an empty `attacl`, so
    // `compare_grants` plans its column privileges from nothing. Columns of a
    // table in `dropped_and_recreated_tables` are listed only through
    // `cascade_readded`.
    readded_columns: HashSet<(String, String, String)>,
    // `CREATE STATISTICS` for materialized views that are created or
    // recreated during the migration. `compare_statistics` runs before
    // `compare_routines_and_views` creates them, so these are emitted after
    // it instead.
    view_statistics_post_script: String,
    // Rules present in both dumps that the table and view DDL takes away:
    // `drop_rules` dropped them ahead of a change to something they depend
    // on, or the table they sit on is dropped or recreated. `compare_rules`
    // recreates them.
    rules_to_recreate: HashSet<RuleKey>,
    // Views `recreate_routine_dependents` re-creates after a routine's
    // `DROP ... CASCADE` took them (normalized keys). Their rules, and the
    // rules reading them, went with them, so `compare_rules` recreates those
    // too, and their privileges went as well, so `compare_grants` plans them
    // from the default privileges. The match is by name, so the view may not
    // have been dropped at all: the routines it calls
    // (`View::routine_dependencies`) tell, and when a dump predates them the
    // grants are planned from either state.
    views_recreated_by_cascade: HashSet<String>,
    // Tables `compare_tables` re-creates from TO because their partitioned
    // parent is re-created, in `table_key` form. Their triggers are created
    // by `trigger_post_script`, after every routine, so a routine's `DROP ...
    // CASCADE` cannot take them and `recreate_routine_dependents` leaves them
    // alone. (A table the alter path re-creates gets its triggers inline,
    // before the routine phase.)
    tables_with_late_triggers: HashSet<String>,
    // FROM foreign keys that reference a table the table phase drops and
    // recreates, from a table that stays (see
    // `Comparer::plan_recreate_fk_predrops`). `compare_tables` drops them
    // ahead of every table alter; the table phase does not see them on the
    // FROM side, so `compare_foreign_keys` adds the TO ones back as new.
    recreate_fk_predrops: Vec<TableConstraint>,
    // FROM foreign keys that reference a table the migration drops, or a
    // partition ancestor of one, from another table (see
    // `Comparer::plan_dropped_table_fk_predrops`), in the order `compare_tables`
    // drops them ahead of the `DROP TABLE`s. With `use_drop` the table phase
    // does not see them on the FROM side either, so it does not drop them a
    // second time, and `compare_foreign_keys` adds back the ones TO keeps.
    dropped_table_fk_predrops: Vec<TableConstraint>,
    // Triggers, as `table_key` + `.` + trigger name, that `compare_tables`
    // drops ahead of the table alters because they block a column's drop +
    // add, and creates again at the end of `trigger_post_script` (see
    // `Comparer::column_drop_blockers`). A partitioned table's trigger among
    // them clones afresh onto every partition.
    triggers_dropped_ahead: HashSet<String>,
    // Domains, as `(schema, typname)`, whose default the migration detached
    // right before a routine it calls was dropped with `CASCADE` (see
    // `Comparer::domain_default_detach`), in the order they were detached;
    // `recreate_routine_dependents` sets the TO default again.
    detached_domain_defaults: Vec<(String, String)>,
    // Tracks columns that should use serial/bigserial/smallserial type.
    // Key: (schema, table, column); Value: "serial", "bigserial", or "smallserial".
    // The tuple form avoids ambiguity when any identifier contains a literal '.'.
    serial_columns: HashMap<(String, String, String), String>,
    // `ALTER SUBSCRIPTION ... REFRESH PUBLICATION` statements of a
    // single-transaction migration, which cannot run inside a transaction
    // block and are emitted after `commit;` (see
    // `Subscription::get_alter_script_with`).
    subscription_post_commit_script: String,
    // Per FROM publication name, the member and EXCEPT tables
    // `release_publication_blockers` removed ahead of the table changes they
    // would block, spelled like `Publication::tables` entries.
    // `compare_publications` puts back whatever the TO publication keeps.
    released_publication_tables: HashMap<String, HashSet<String>>,
    // The tables among those that the migration makes UNLOGGED, which no
    // publication can hold any more.
    unlogged_publication_tables: HashSet<String>,
    // Generated columns that a routine's `DROP ... CASCADE` takes and
    // `recreate_routine_dependents` re-adds from TO, with the copies in the
    // partitions below them, worked out by `drop_views` once it knows which
    // routines are dropped where (see `Comparer::cascade_readded_columns`).
    cascade_readded: Vec<CascadeReadd>,
    // Foreign servers (`quote_ident` names) whose wrapper changes, which the
    // migration drops without `CASCADE` and creates again, with their user
    // mappings and foreign tables (see `Comparer::plan_server_recreates`).
    recreated_servers: HashSet<String>,
    // Foreign servers whose wrapper changes but which a subscription the
    // migration keeps uses, with those subscriptions: the migration leaves
    // them alone and says why (see `Comparer::plan_server_recreates`).
    refused_servers: HashMap<String, Vec<String>>,
    // Per server in `recreated_servers`, the `CREATE FOREIGN TABLE`s of the
    // TO foreign tables on it, which `compare_foreign_tables` leaves for
    // `compare_servers` to run once the server is back.
    foreign_tables_after_servers: HashMap<String, String>,
    // Foreign tables, as `(schema, name)`, that the migration drops and
    // creates again (a server change, or a server in `recreated_servers`):
    // they start from the default privileges, so `compare_grants` plans
    // them as new.
    recreated_foreign_tables: HashSet<(String, String)>,
    // Identity sequences the migration renames, TO `(schema, name)` to FROM
    // `(schema, name)`: the same sequence of the same identity column under
    // a name that drifted (see `Comparer::plan_identity_sequence_renames`).
    // `compare_grants` compares their privileges across the rename.
    renamed_identity_sequences: HashMap<(String, String), (String, String)>,
    // Policies dropped ahead of the composite section because they block a
    // typed table's column retype, by name, keyed by their FROM table
    // `(schema, name)`; `compare_tables` creates them from TO after every
    // table alter (see `Comparer::typed_retype_pre_section`).
    typed_retype_policies: HashMap<(String, String), HashSet<String>>,
    // Triggers, as `table_key` + `.` + trigger name, dropped ahead of the
    // composite section likewise; added to `triggers_dropped_ahead`.
    typed_retype_triggers: HashSet<String>,
    // FROM tables, as `(schema, name)`, whose
    // `Table::ancestor_retype_pre_script` ran ahead of the composite section
    // (see `Comparer::typed_retype_pre_section`), so the table phase does
    // not run it again.
    typed_hoisted_tables: HashSet<(String, String)>,
}

/// What `Comparer::partition_move_checks` plans for one table (see
/// [`InheritedRecreates`]).
#[derive(Debug, Default)]
struct MoveChecks {
    /// [`InheritedRecreates::detached_checks`].
    detached: Vec<TableConstraint>,
    /// [`InheritedRecreates::attach_checks`].
    attached: Vec<TableConstraint>,
    /// [`InheritedRecreates::inherit_readds`].
    readds: Vec<TableConstraint>,
}

/// The per-table answer of `Comparer::table_alter_caps`, keyed by FROM table
/// `(schema, name)`.
type TableAlterCapsMap = HashMap<(String, String), TableAlterCaps>;

/// What `Comparer::set_expression_check_hazards` plans: the CHECK
/// constraints that have to be out of the way of an `ALTER TABLE ONLY ...
/// SET EXPRESSION`. All three are keyed by FROM table `(schema, name)`.
#[derive(Debug, Default)]
pub(crate) struct SetExpressionHazards {
    /// The tables whose `SET EXPRESSION` is at risk. Without `use_drop`
    /// nothing can be dropped ahead, so `Comparer::table_alter_caps` turns
    /// the in-place form off for them: the expression change becomes a
    /// drop + add, commented out like every other drop.
    pub(crate) tables: HashSet<(String, String)>,
    /// The CHECKs each holder drops ahead of the table alters, parents
    /// first: added to [`InheritedRecreates::family_checks`].
    pub(crate) drops: HashMap<(String, String), HashSet<String>>,
    /// The CHECKs, as TO has them on each table, whose add waits for every
    /// table alter: [`InheritedRecreates::checks_after_alters`].
    pub(crate) deferred: HashMap<(String, String), HashSet<String>>,
}

/// The statement `--guard-sql-routine-bodies` emits. `SET`, not `SET LOCAL`,
/// so it also covers the post-commit section (see
/// `Comparer::set_guard_sql_routine_bodies`).
const FUNCTION_BODY_GUARD: &str = "set check_function_bodies = false;";

/// The statement a migration starts with when the migrated server defaults to
/// `standard_conforming_strings = off`, or may, as far as a FROM dump written
/// by pgc 1.0.x can tell (see `Comparer::needs_standard_strings_guard`).
/// `SET`, not `SET LOCAL`, for the same reason as [`FUNCTION_BODY_GUARD`], and
/// it goes before `begin;`, as in a `pg_dump` script.
const STANDARD_STRINGS_GUARD: &str = "set standard_conforming_strings = on;";

/// The error [`Comparer::check_dump_versions`] returns for a FROM dump written
/// by pgc 1.1+ and a TO dump written by pgc 1.0.x.
const TO_DUMP_WRITTEN_BY_PGC_1_0: &str = "refusing to compare: the 'to' dump was written by \
     pgc 1.0.x and the 'from' dump by pgc 1.1 or later. A pgc 1.0.x dump leaves out what that \
     version could not read (tables whose names start with pg but not pg_, SQL-standard \
     routine bodies, publication row filters, column lists, schemas and options) and holds \
     the subscriptions of other databases, so the migration would drop, empty, strip or \
     create them in the 'from' database. Re-dump 'to' with pgc 1.1 or later and compare \
     again.";

/// The stderr warning [`Comparer::compare`] prints for a FROM dump written by
/// pgc 1.0.x and a TO dump written by pgc 1.1+ (see
/// `Comparer::legacy_from_against_current_to`).
const FROM_DUMP_WRITTEN_BY_PGC_1_0_WARNING: &str = "Warning: the 'from' dump was written by \
     pgc 1.0.x and the 'to' dump by pgc 1.1 or later. The migration re-applies what pgc \
     1.0.x could not read: SQL-standard routines are re-created, publications and replica \
     identities are set again, and literals holding a backslash are rewritten (so the \
     migration starts with set standard_conforming_strings = on;). It also creates every \
     'to' table whose name starts with pg but not pg_, which pgc 1.0.x could not see, and \
     every 'to' subscription, because pgc 1.0.x dumped the subscriptions of every database \
     in the cluster; where 'from' already has one, its CREATE fails with 'already exists'. \
     Production output skips the CREATE TABLE of such a table instead, but not the \
     constraints added to it separately, which fail with 'already exists' as well, and any \
     other difference in that table is silently left unapplied. Re-dump 'from' with pgc 1.1 \
     or later to compare without these.";

/// The comment a migration opens with when the FROM dump was written by pgc
/// 1.0.x and the TO dump by pgc 1.1+ (see `Comparer::legacy_from_against_current_to`).
/// Fixed text, so no dumped name can end it early.
const FROM_DUMP_WRITTEN_BY_PGC_1_0_COMMENT: &str = "\
/* Warning: the FROM dump was written by pgc 1.0.x and the TO dump by pgc 1.1 or later.
   pgc 1.0.x did not read everything pgc 1.1 does, so this migration re-applies objects
   FROM may already have: SQL-standard routines, publication filters and options, replica
   identities and literals holding a backslash. It also creates every table whose name
   starts with pg but not pg_, and every subscription: where FROM has one, its CREATE
   fails with \"already exists\". Production output skips such a CREATE TABLE, but not the
   constraints added to the table separately, which fail the same way, and any other
   difference in that table is left unapplied. Re-dump FROM with pgc 1.1 or later for an
   exact migration. */";

impl Comparer {
    /// Creates a new Comparer with the given dumps
    pub fn new(
        from: Dump,
        to: Dump,
        use_drop: bool,
        use_single_transaction: bool,
        use_comments: bool,
        grants_mode: GrantsMode,
    ) -> Self {
        let mut comparer = Self {
            from,
            to,
            use_drop,
            use_single_transaction,
            use_comments,
            grants_mode,
            output_for_production: false,
            guard_sql_routine_bodies: false,
            script: String::new(),
            production_post_script: String::new(),
            enum_pre_script: String::new(),
            enum_post_script: String::new(),
            enum_add_value_script: String::new(),
            type_post_script: String::new(),
            composite_alter_script: String::new(),
            domain_constraint_post_script: String::new(),
            sequence_post_script: String::new(),
            trigger_post_script: String::new(),
            dropped_views: HashMap::new(),
            predropped_routines: HashSet::new(),
            cascade_victim_routines: HashSet::new(),
            deferred_routine_script: String::new(),
            recreated_tables: HashSet::new(),
            dropped_and_recreated_tables: HashSet::new(),
            readded_columns: HashSet::new(),
            view_statistics_post_script: String::new(),
            rules_to_recreate: HashSet::new(),
            views_recreated_by_cascade: HashSet::new(),
            tables_with_late_triggers: HashSet::new(),
            recreate_fk_predrops: Vec::new(),
            dropped_table_fk_predrops: Vec::new(),
            triggers_dropped_ahead: HashSet::new(),
            detached_domain_defaults: Vec::new(),
            serial_columns: HashMap::new(),
            subscription_post_commit_script: String::new(),
            released_publication_tables: HashMap::new(),
            unlogged_publication_tables: HashSet::new(),
            cascade_readded: Vec::new(),
            recreated_servers: HashSet::new(),
            refused_servers: HashMap::new(),
            foreign_tables_after_servers: HashMap::new(),
            recreated_foreign_tables: HashSet::new(),
            renamed_identity_sequences: HashMap::new(),
            typed_retype_policies: HashMap::new(),
            typed_retype_triggers: HashSet::new(),
            typed_hoisted_tables: HashSet::new(),
        };

        comparer.script.push_str("/*\n");
        comparer
            .script
            .append_block("Script generated by PostgreSQL Comparer");
        comparer.script.push_str(" From dump:\n");
        comparer.script.push_str(&comparer.from.get_info());
        comparer.script.push_str("\n\n To dump:\n");
        comparer.script.push_str(&comparer.to.get_info());
        comparer.script.push_str(&format!(
            "\n\n Comparison results (use_drop: {}, grants_mode: {}):\n",
            comparer.use_drop, comparer.grants_mode
        ));
        comparer.script.append_block("*/");

        comparer
    }

    /// Whether the migrated server — the FROM database, which the migration
    /// is applied to — is known to be at least `server_version_num` `min` (e.g. `190000`).
    ///
    /// `false` when the FROM dump predates [`Dump::server_version_num`], so
    /// version-specific DDL falls back to the form every supported server
    /// accepts.
    pub(crate) fn migrated_server_at_least(&self, min: i32) -> bool {
        self.from.server_version_num.is_some_and(|v| v >= min)
    }

    /// The migrated server's `server_version_num` when it is known to predate
    /// `min`; `None` when it is at least `min`, or unknown (a FROM dump written
    /// by pgc 1.0.x), in which case pgc emits the DDL as it always has.
    ///
    /// The counterpart of [`Comparer::migrated_server_at_least`] for DDL an
    /// older server does not reject but silently ignores, where emitting it
    /// anyway would leave a difference no migration can close.
    pub(crate) fn migrated_server_known_below(&self, min: i32) -> Option<i32> {
        self.from.server_version_num.filter(|&v| v < min)
    }

    /// Refuse a pair of dumps whose migration would destroy what one of them
    /// could not see: a FROM dump written by pgc 1.1 or later compared with a
    /// TO dump written by pgc 1.0.x.
    ///
    /// Every dump pgc 1.1+ writes records [`Dump::server_version_num`] and no
    /// pgc 1.0.x dump does, which is how the two are told apart. A pgc 1.0.x
    /// dump leaves out, or holds wrongly, what that version could not read,
    /// and the comparer takes the TO dump as the state to reach, so every gap
    /// in it becomes a change: tables named `pg` followed by anything but `_`
    /// (`pgbench_accounts`) are dropped, a SQL-standard routine body
    /// (`BEGIN ATOMIC ... END`, `RETURN expr`) is replaced with an empty one,
    /// publication row filters, column lists, `TABLES IN SCHEMA` members and
    /// options are stripped, and the subscriptions of the cluster's other
    /// databases, which pgc 1.0.x wrote into every dump, are created. No safe
    /// migration can be derived from such a pair, so this returns an error
    /// telling the user to re-dump TO.
    ///
    /// The opposite pairing, a pgc 1.0.x FROM dump against a pgc 1.1+ TO
    /// dump, passes: what it re-applies is harmless or fails loudly (except
    /// under production output, which skips the `CREATE TABLE` of a table
    /// FROM already has), and [`compare`](Self::compare) says so on stderr
    /// and in the script. Two dumps written by the same pgc generation
    /// always pass.
    ///
    /// `pgc --command compare` and the `--config` chain call this on the two
    /// dumps they have read, before building the comparer. [`Comparer::new`]
    /// and [`compare`](Self::compare) do not, so an in-memory comparison of
    /// hand-built dumps is not affected.
    ///
    /// # Errors
    ///
    /// [`ErrorKind::InvalidInput`](std::io::ErrorKind::InvalidInput) when
    /// `from` was written by pgc 1.1+ and `to` by pgc 1.0.x.
    ///
    /// # Examples
    ///
    /// ```
    /// # use pgc::comparer::core::Comparer;
    /// # use pgc::config::dump_config::DumpConfig;
    /// # use pgc::dump::core::Dump;
    /// # let config = || DumpConfig {
    /// #     host: "localhost".to_string(), port: "5432".to_string(),
    /// #     user: "postgres".to_string(), password: String::new(),
    /// #     database: "shop".to_string(), scheme: "public".to_string(),
    /// #     ssl: false, file: String::new(),
    /// # };
    /// // Written by pgc 1.1 or later: the dump records its server's version.
    /// let mut current = Dump::new(config());
    /// current.server_version_num = Some(180000);
    /// // Written by pgc 1.0.x: no version.
    /// let legacy = Dump::new(config());
    ///
    /// assert!(Comparer::check_dump_versions(&current, &legacy).is_err());
    /// assert!(Comparer::check_dump_versions(&legacy, &current).is_ok());
    /// assert!(Comparer::check_dump_versions(&legacy, &legacy).is_ok());
    /// assert!(Comparer::check_dump_versions(&current, &current).is_ok());
    /// ```
    pub fn check_dump_versions(from: &Dump, to: &Dump) -> Result<(), Error> {
        if from.server_version_num.is_some() && to.server_version_num.is_none() {
            return Err(Error::new(
                std::io::ErrorKind::InvalidInput,
                TO_DUMP_WRITTEN_BY_PGC_1_0,
            ));
        }
        Ok(())
    }

    /// Whether the FROM dump was written by pgc 1.0.x and the TO dump by pgc
    /// 1.1+ — the one mixed pairing `check_dump_versions` lets through.
    ///
    /// The migration then re-applies what pgc 1.0.x could not read or read
    /// wrongly, all of it harmless or failing loudly: tables named
    /// `pg<letter>...` are created (see `tables_hidden_from_pgc_1_0`), and so
    /// is every TO subscription (see `from_subscriptions_by_name`),
    /// SQL-standard routines re-created, publications and replica identities
    /// set again, and every literal holding a backslash rewritten — from its
    /// doubled form, when the FROM server defaults to
    /// `standard_conforming_strings = off`, which is why
    /// `needs_standard_strings_guard` answers `true` here. `compare` says so
    /// on stderr (`legacy_from_warnings`) and in a comment at the top of the
    /// script.
    ///
    /// One exception to "loudly": production output creates tables with
    /// `if not exists`, so it skips the `CREATE TABLE` of a hidden table
    /// FROM has — but not the constraints added to it separately, which
    /// still fail, nor its other differences, which are then not applied.
    /// The warning says so.
    fn legacy_from_against_current_to(&self) -> bool {
        self.from.server_version_num.is_none() && self.to.server_version_num.is_some()
    }

    /// Quotes, in a dump written by pgc 1.0.x, the materialized-view names
    /// that are keywords only from PostgreSQL 16 or 17 on (`json_object`,
    /// `json`, `merge_action`), when the other dump says its server quotes
    /// them (see [`Dump::requote_legacy_materialized_view_names`]).
    ///
    /// Loading such a dump spells them as PostgreSQL 14 and 15 do, bare,
    /// since it does not record its server's version; the other dump's is
    /// the best guess there is. Without it, a 1.0.x dump of a PostgreSQL 17
    /// view `json` would not match the `"json"` a current dump of the same
    /// view stores, and the view would be dropped and recreated. Nothing
    /// happens when both dumps, or neither, record a version.
    fn requote_legacy_materialized_view_names(&mut self) {
        if let Some(version) = self.to.server_version_num {
            self.from.requote_legacy_materialized_view_names(version);
        }
        if let Some(version) = self.from.server_version_num {
            self.to.requote_legacy_materialized_view_names(version);
        }
    }

    /// Gives each composite type attribute of a FROM dump written by pgc
    /// 1.0.x, which did not record attribute collations, the collation
    /// ([`CompositeAttribute::collation`]) of the TO attribute of the same
    /// type and name when their data types match, and rehashes the types it
    /// changes. Such a dump says nothing about the collation, so the
    /// migration leaves it as it is, as pgc 1.0.x did, rather than retyping
    /// every attribute TO gives one — with its typed tables, and the policies
    /// and triggers in the way dropped and recreated — on the chance that
    /// FROM differs. Nothing happens unless the TO dump records its server
    /// version and the FROM dump does not.
    fn adopt_legacy_composite_collations(&mut self) {
        if !self.legacy_from_against_current_to() {
            return;
        }
        let to_attributes: HashMap<(&str, &str), &[CompositeAttribute]> = self
            .to
            .types
            .iter()
            .filter(|t| {
                (t.typtype as u8 as char) == 'c'
                    && t.composite_attributes.iter().any(|a| a.collation.is_some())
            })
            .map(|t| {
                (
                    (t.schema.as_str(), t.typname.as_str()),
                    t.composite_attributes.as_slice(),
                )
            })
            .collect();
        if to_attributes.is_empty() {
            return;
        }
        for from_type in &mut self.from.types {
            let Some(wanted) =
                to_attributes.get(&(from_type.schema.as_str(), from_type.typname.as_str()))
            else {
                continue;
            };
            let mut changed = false;
            for attribute in &mut from_type.composite_attributes {
                if let Some(to) = wanted.iter().find(|a| {
                    a.name == attribute.name
                        && a.data_type == attribute.data_type
                        && a.collation.is_some()
                }) && attribute.collation.is_none()
                {
                    attribute.collation.clone_from(&to.collation);
                    changed = true;
                }
            }
            if changed {
                from_type.hash();
            }
        }
    }

    /// Moves, in a FROM dump written by pgc 1.0.x, the partition trigger
    /// clones that the TO dump records on the same partition out of the FROM
    /// partition's own triggers into [`Table::trigger_clones`], where a
    /// current dump keeps them, and rehashes that table. Nothing happens for
    /// a FROM dump that records its server version.
    ///
    /// pgc 1.0.x listed a clone among a partition's own triggers from
    /// PostgreSQL 15 on. [`Dump::read_from_file`] sets them apart by the
    /// parent's triggers, which it cannot do for a partition whose parent
    /// the dump does not have (outside its schemas). A current dump records
    /// every clone of such a partition, so a trigger TO records as a clone
    /// on the same partition, whose parent FROM lacks too, is one in FROM
    /// as well: a partition cannot have a trigger of its own by the name of
    /// one of its clones. Left among the own triggers, it would be dropped,
    /// which PostgreSQL refuses ("cannot drop trigger ... because trigger
    /// ... requires it").
    fn separate_legacy_orphan_trigger_clones(&mut self) {
        if self.from.server_version_num.is_some() {
            return;
        }
        let to_keys: HashSet<String> = self
            .to
            .tables
            .iter()
            .map(|t| Self::table_key(&t.schema, &t.name))
            .collect();
        let clone_names: HashMap<String, HashSet<&str>> = self
            .to
            .tables
            .iter()
            .filter(|t| {
                !t.trigger_clones.is_empty()
                    && t.partition_of
                        .as_deref()
                        .is_some_and(|p| !to_keys.contains(&Self::normalise_partition_of(p)))
            })
            .map(|t| {
                (
                    Self::table_key(&t.schema, &t.name),
                    t.trigger_clones.iter().map(|c| c.name.as_str()).collect(),
                )
            })
            .collect();
        if clone_names.is_empty() {
            return;
        }
        let from_keys: HashSet<String> = self
            .from
            .tables
            .iter()
            .map(|t| Self::table_key(&t.schema, &t.name))
            .collect();
        for table in &mut self.from.tables {
            let Some(names) = clone_names.get(&Self::table_key(&table.schema, &table.name)) else {
                continue;
            };
            if table
                .partition_of
                .as_deref()
                .is_none_or(|p| from_keys.contains(&Self::normalise_partition_of(p)))
            {
                continue;
            }
            let (clones, own): (Vec<TableTrigger>, Vec<TableTrigger>) =
                std::mem::take(&mut table.triggers)
                    .into_iter()
                    .partition(|t| names.contains(t.name.as_str()) && t.is_row_level());
            table.triggers = own;
            if clones.is_empty() {
                continue;
            }
            table.trigger_clones.extend(clones);
            table.trigger_clones.sort_by_key(|t| t.name.to_lowercase());
            table.hash();
        }
    }

    /// The TO tables, as `schema.name`, that the migration creates although
    /// the FROM database may already have them, because a pgc 1.0.x FROM dump
    /// could not see them: its tables query filtered on
    /// `tablename not like 'pg_%'`, where `_` matches any character, so it
    /// left out every name of three or more characters that starts with
    /// `pg`, not only the `pg_` prefix it meant. Empty unless
    /// `legacy_from_against_current_to`.
    fn tables_hidden_from_pgc_1_0(&self) -> Vec<String> {
        if !self.legacy_from_against_current_to() {
            return Vec::new();
        }
        let from_tables: HashSet<(&str, &str)> = self
            .from
            .tables
            .iter()
            .map(|t| (t.schema.as_str(), t.name.as_str()))
            .collect();
        self.to
            .tables
            .iter()
            .filter(|t| {
                let mut name = t.raw_name.chars();
                name.next() == Some('p') && name.next() == Some('g') && name.next().is_some()
            })
            .filter(|t| !from_tables.contains(&(t.schema.as_str(), t.name.as_str())))
            .map(|t| format!("{}.{}", t.schema, t.name))
            .collect()
    }

    /// The TO subscriptions the migration creates although the pgc 1.0.x
    /// FROM dump lists one of the same name: that dump holds the
    /// subscriptions of every database in the cluster (`pg_subscription` is
    /// a shared catalog, and pgc 1.0.x did not filter it on `subdbid`), so it
    /// cannot say whether the FROM database is the one that has it. Empty
    /// unless `legacy_from_against_current_to`.
    fn subscriptions_pgc_1_0_cannot_place(&self) -> Vec<&str> {
        if !self.legacy_from_against_current_to() {
            return Vec::new();
        }
        let listed: HashSet<&str> = self
            .from
            .subscriptions
            .iter()
            .map(|s| s.name.as_str())
            .collect();
        self.to
            .subscriptions
            .iter()
            .map(|s| s.name.as_str())
            .filter(|name| listed.contains(name))
            .collect()
    }

    /// The FROM subscriptions each TO subscription is matched against, by
    /// name: a match is altered (or left alone), anything else created.
    /// `from` is the FROM dump's list and `legacy` is
    /// `legacy_from_against_current_to` — an associated function rather than
    /// a method so the map borrows only that list, not the whole comparer
    /// whose script it is used to extend.
    ///
    /// Empty when `legacy`, so every TO subscription is created. A pgc 1.0.x
    /// FROM dump lists the subscriptions of every database in the cluster,
    /// and matching against it would take another database's subscription
    /// for the FROM database's own: one of the same name and definition would
    /// count as present and never be created, one that differs would be
    /// altered from the wrong definition. Created instead, a subscription
    /// FROM already has fails with "already exists", which PostgreSQL checks
    /// before it connects to the publisher. The FROM-only drops
    /// (`drop_subscriptions`) need no such care: they are
    /// `DROP SUBSCRIPTION IF EXISTS`, which does nothing for a name the FROM
    /// database does not have.
    fn from_subscriptions_by_name(
        from: &[crate::dump::publication::Subscription],
        legacy: bool,
    ) -> HashMap<&str, &crate::dump::publication::Subscription> {
        if legacy {
            return HashMap::new();
        }
        from.iter().map(|s| (s.name.as_str(), s)).collect()
    }

    /// The stderr warnings `compare` prints for a FROM dump written by pgc
    /// 1.0.x and a TO dump written by pgc 1.1+: the general one, then one
    /// naming the tables pgc 1.0.x could not see and one naming the
    /// subscriptions it could not place, each only when there are any.
    /// Empty unless `legacy_from_against_current_to`.
    fn legacy_from_warnings(&self) -> Vec<String> {
        if !self.legacy_from_against_current_to() {
            return Vec::new();
        }
        let mut warnings = vec![FROM_DUMP_WRITTEN_BY_PGC_1_0_WARNING.to_string()];
        let hidden = self.tables_hidden_from_pgc_1_0();
        if !hidden.is_empty() {
            warnings.push(format!(
                "Warning: the migration creates these tables, which a pgc 1.0.x dump \
                 cannot see, so 'from' may already have them: {}.",
                hidden.join(", ")
            ));
        }
        let unplaced = self.subscriptions_pgc_1_0_cannot_place();
        if !unplaced.is_empty() {
            warnings.push(format!(
                "Warning: the migration creates these subscriptions, which the pgc 1.0.x \
                 'from' dump lists without saying which database has them, so 'from' may \
                 already have them: {}.",
                unplaced.join(", ")
            ));
        }
        warnings
    }

    /// Enable or disable production-friendly output. When enabled, indexes are
    /// built concurrently (partition-aware), foreign keys are added `NOT VALID`
    /// then validated after commit, and indexes are dropped concurrently — all
    /// post-commit statements are emitted after the main transaction.
    /// Returns `&mut Self` so it can be chained after construction.
    ///
    /// ```
    /// # use pgc::comparer::core::Comparer;
    /// # use pgc::config::dump_config::DumpConfig;
    /// # use pgc::config::grants_mode::GrantsMode;
    /// # use pgc::dump::core::Dump;
    /// # let config = || DumpConfig {
    /// #     host: "localhost".to_string(), port: "5432".to_string(),
    /// #     user: "postgres".to_string(), password: String::new(),
    /// #     database: "shop".to_string(), scheme: "public".to_string(),
    /// #     ssl: false, file: String::new(),
    /// # };
    /// let mut comparer = Comparer::new(
    ///     Dump::new(config()),
    ///     Dump::new(config()),
    ///     true,
    ///     true,
    ///     true,
    ///     GrantsMode::Ignore,
    /// );
    /// comparer.set_output_for_production(true);
    /// ```
    pub fn set_output_for_production(&mut self, value: bool) -> &mut Self {
        self.output_for_production = value;
        self
    }

    /// Ask PostgreSQL not to validate function bodies while the migration
    /// runs, by emitting `set check_function_bodies = false;` as its first
    /// statement.
    ///
    /// A defensive net for the whole class of ordering bug behind issue #240:
    /// pgc infers routine dependencies by reading names out of `prosrc`, and
    /// text can only ever be an approximation of what a body really calls. If
    /// the order still comes out wrong, this makes a `sql`-language routine
    /// create anyway instead of failing on a callee that does not exist yet.
    ///
    /// It cannot do that for a SQL-standard body (`BEGIN ATOMIC … END` /
    /// `RETURN expr`, see [`Routine::is_sql_body`]): PostgreSQL parses such a
    /// body when the routine is created whatever `check_function_bodies` says,
    /// so for those routines the dependency order is the only safeguard. It is
    /// a better-informed one than for a string body: the body is dumped with
    /// an empty `search_path`, so every name in it outside `pg_catalog` is
    /// schema-qualified for the text scan, and PostgreSQL records what it uses
    /// in `pg_depend` ([`Routine::sql_body_dependencies`]), which orders it
    /// after the views and routines it reads or calls, and after the
    /// operators and casts it applies, which are otherwise created after the
    /// routines. A cast that has no function (`WITHOUT FUNCTION`, `WITH
    /// INOUT`) leaves no such record.
    ///
    /// The trade is real, which is why this is opt-in: with body checking off
    /// a genuine typo in a routine body is no longer caught when the migration
    /// runs — it waits until something calls it.
    ///
    /// `check_function_bodies` is `SET` (not `SET LOCAL`), so it holds for the
    /// rest of the session. That is what makes it survive past the `commit;`
    /// into any post-commit section, and it is why a connection reused for
    /// other work after the migration keeps the setting until it is reset or
    /// the session ends. For the usual one-shot `psql -f` replay this does not
    /// arise.
    ///
    /// # Examples
    ///
    /// ```
    /// # use pgc::comparer::core::Comparer;
    /// # use pgc::config::dump_config::DumpConfig;
    /// # use pgc::config::grants_mode::GrantsMode;
    /// # use pgc::dump::core::Dump;
    /// # let config = || DumpConfig {
    /// #     host: "localhost".to_string(), port: "5432".to_string(),
    /// #     user: "postgres".to_string(), password: String::new(),
    /// #     database: "shop".to_string(), scheme: "public".to_string(),
    /// #     ssl: false, file: String::new(),
    /// # };
    /// let mut comparer = Comparer::new(
    ///     Dump::new(config()),
    ///     Dump::new(config()),
    ///     true,
    ///     true,
    ///     true,
    ///     GrantsMode::Ignore,
    /// );
    /// comparer.set_guard_sql_routine_bodies(true);
    /// ```
    pub fn set_guard_sql_routine_bodies(&mut self, value: bool) -> &mut Self {
        self.guard_sql_routine_bodies = value;
        self
    }

    /// Compare dumps and generate the script.
    ///
    /// A FROM dump written by pgc 1.0.x against a TO dump written by pgc 1.1+
    /// is compared, with a warning on stderr and a comment at the top of the
    /// script: the migration re-applies what pgc 1.0.x could not read. The
    /// opposite pairing is not checked here; see
    /// [`check_dump_versions`](Self::check_dump_versions).
    pub async fn compare(&mut self) -> Result<(), Error> {
        if self.legacy_from_against_current_to() {
            for warning in self.legacy_from_warnings() {
                eprintln!("{warning}");
            }
            self.script
                .append_block(FROM_DUMP_WRITTEN_BY_PGC_1_0_COMMENT);
        }
        self.separate_legacy_orphan_trigger_clones();
        self.requote_legacy_materialized_view_names();
        self.adopt_legacy_composite_collations();
        if self.output_for_production {
            // The statements that cannot run inside a transaction block are
            // emitted in a trailing section. With --use-single-transaction the
            // rest of the migration is wrapped in begin/commit, so that section
            // lands after COMMIT; without it no transaction is opened and the
            // section is simply the tail of the script (each statement still
            // runs in its own implicit transaction).
            let tail = if self.use_single_transaction {
                "are emitted after COMMIT"
            } else {
                "are emitted in a separate section at the end"
            };
            self.script.append_block(&format!(
                "/* Output generated for production: indexes are built/dropped concurrently, \
                 foreign keys are validated, and the statements that cannot run inside a \
                 transaction {tail}. */"
            ));
        }
        // Before everything else, so every literal below — in the main body
        // and in the post-commit section — is read in standard form.
        let standard_strings_guard = self.needs_standard_strings_guard();
        if standard_strings_guard {
            self.script.append_block(STANDARD_STRINGS_GUARD);
        }
        // Where `enum_add_value_script` is spliced in: after the standard
        // strings guard (the labels are literals), before `begin;`.
        let before_begin = self.script.len();
        if self.use_single_transaction {
            self.script.append_block("begin;");
        }
        // First statement of the migration, so every routine created below is
        // covered — including any emitted in the post-commit section, since
        // this is a session-scoped SET rather than SET LOCAL.
        if self.guard_sql_routine_bodies {
            self.script.append_block(FUNCTION_BODY_GUARD);
        }

        self.plan_server_recreates();
        // FROM-only subscriptions go first: on PG19 a `SERVER` subscription
        // blocks dropping its server, wrapper or extension even with CASCADE.
        self.drop_subscriptions().await?;
        self.compare_schemas().await?;
        self.compare_extensions().await?;
        self.compare_enums().await?;
        if !self.enum_add_value_script.is_empty() {
            let add_values = std::mem::take(&mut self.enum_add_value_script);
            self.script.insert_str(before_begin, &add_values);
        }
        if !self.enum_pre_script.is_empty() {
            self.script.push_str(&self.enum_pre_script);
            self.enum_pre_script.clear();
        }
        // Before the types and tables that may use them.
        self.create_new_collations();
        self.compare_types().await?;
        // The order of operations here is important due to dependency relationships:
        // - Tables may depend on sequences (e.g., via SERIAL columns).
        // - Sequences may depend on tables (e.g., via ownership).
        // Therefore, we compare sequences and drop views before comparing tables,
        // and defer sequence drops until after tables to avoid dependency errors.
        self.compare_sequences().await?;
        self.mark_serial_columns();
        self.mark_generation_routines_dropped();
        self.drop_views().await?;
        self.release_publication_blockers();
        // After the views, rules and publication members in the way of a
        // retyped or dropped typed-table column are gone (see
        // `compare_types`), before the tables' own alters.
        if !self.composite_alter_script.is_empty() {
            let pre_section = self.typed_retype_pre_section();
            self.script.push_str(&pre_section);
            self.script.append_block(
                "\n/* ---> Composite types: attribute changes (after the view drops) --------------- */",
            );
            let composite = std::mem::take(&mut self.composite_alter_script);
            self.script.push_str(&composite);
        }
        self.compare_ts_dict_options().await?;
        // The foreign keys in the way of a table the table phase drops and
        // recreates, which it drops ahead of every table alter.
        let families = self.recreated_families();
        self.recreate_fk_predrops = if self.use_drop {
            self.plan_recreate_fk_predrops(&families)
        } else {
            Vec::new()
        };
        self.dropped_table_fk_predrops = self.plan_dropped_table_fk_predrops();
        // Tables and foreign tables as the table phase finds them after
        // `drop_views`, and without what it cannot create yet: whatever calls
        // a routine dropped there is created after the routines.
        let table_phase_originals = self.enter_table_phase_view();
        self.compare_tables().await?;
        self.compare_foreign_keys().await?;
        self.compare_foreign_tables().await?;
        self.leave_table_phase_view(table_phase_originals);
        self.compare_statistics().await?;
        self.compare_collations().await?;
        self.compare_ts_dicts().await?;
        self.compare_ts_configs().await?;
        self.compare_fdws().await?;
        self.compare_servers().await?;
        self.compare_user_mappings().await?;
        self.compare_publications().await?;
        self.compare_subscriptions().await?;
        // After the subscriptions have been dropped or moved off them.
        self.drop_foreign_servers_and_fdws().await?;
        if !self.sequence_post_script.is_empty() {
            self.script.push_str(&self.sequence_post_script);
            self.sequence_post_script.clear();
        }
        self.compare_routines_and_views().await?;
        if !self.view_statistics_post_script.is_empty() {
            self.script.push_str(&self.view_statistics_post_script);
            self.view_statistics_post_script.clear();
        }
        // Operators, casts, and event triggers must come after routines, as they reference functions
        self.compare_operators().await?;
        self.compare_casts().await?;
        self.emit_deferred_routines();
        // Rules after everything their actions can name: routines (the ones
        // held back until after the casts included), views, operators and
        // casts. FROM-only rules went in `drop_views`.
        self.compare_rules().await?;
        self.compare_event_triggers().await?;
        // Domain CHECKs are added and validated only now: both scan the
        // stored data, which by now no longer holds the tables and columns
        // this migration drops, and the CHECK may call a routine, operator or
        // cast created above.
        if !self.domain_constraint_post_script.is_empty() {
            self.script.push_str(&self.domain_constraint_post_script);
            self.domain_constraint_post_script.clear();
        }
        if !self.type_post_script.is_empty() {
            self.script.push_str(&self.type_post_script);
            self.type_post_script.clear();
        }
        if !self.enum_post_script.is_empty() {
            self.script.push_str(&self.enum_post_script);
            self.enum_post_script.clear();
        }
        if !self.trigger_post_script.is_empty() {
            self.script.push_str(&self.trigger_post_script);
            self.trigger_post_script.clear();
        }

        self.compare_grants().await?;
        self.compare_default_privileges().await?;

        if self.use_single_transaction {
            self.script.push_str("\ncommit;");
        }

        // Only a single-transaction migration fills this: without one, each
        // REFRESH PUBLICATION runs in place.
        if !self.subscription_post_commit_script.is_empty() {
            self.script.append_block(
                "\n/* ---> Subscriptions post-commit (run outside a transaction): Start --------------- */",
            );
            let post = std::mem::take(&mut self.subscription_post_commit_script);
            self.script.push_str(&post);
            self.script
                .append_block("/* ---> Subscriptions post-commit: End --------------- */");
        }

        // Concurrent index builds/drops, FK validations and partition-index
        // attaches cannot run inside a transaction block, so they are emitted
        // here in a trailing section — after COMMIT when --use-single-transaction
        // wrapped the migration, otherwise just at the end. Either way each runs
        // in its own implicit transaction.
        if !self.production_post_script.is_empty() {
            self.script.append_block(
                "\n/* ---> Production post-commit (run outside a transaction): Start --------------- */",
            );
            let post = std::mem::take(&mut self.production_post_script);
            self.script.push_str(&post);
            self.script
                .append_block("/* ---> Production post-commit: End --------------- */");
        }

        // Production migrations are meant to be re-runnable against a live,
        // possibly partially-migrated database: inject `if [not] exists` /
        // `or replace` guards into every DDL form PostgreSQL supports them for.
        // The pass is literal-/comment-aware so commented-out drops and quoted
        // text are left untouched. Done last so it also covers the post-commit
        // (concurrent index) statements appended above.
        if self.output_for_production {
            self.script = production::make_idempotent(&self.script);
        }

        // A no-op migration has to stay a no-op. Without
        // `--use-single-transaction` an unchanged schema renders as an empty
        // file, and callers read that emptiness as "nothing to apply" — the
        // repo's own round-trip check asserts a second compare is 0 bytes. So
        // the guards are withdrawn when they are the only thing the migration
        // would do, which makes a no-op diff byte-identical with them on and
        // off in both transaction modes.
        let guards: Vec<&str> = [
            (self.guard_sql_routine_bodies, FUNCTION_BODY_GUARD),
            (standard_strings_guard, STANDARD_STRINGS_GUARD),
        ]
        .into_iter()
        .filter_map(|(on, guard)| on.then_some(guard))
        .collect();
        if !guards.is_empty() {
            let mut remainder = super::scanner::strip_comments_and_collapse(&self.script);
            for guard in &guards {
                remainder = remainder.replacen(guard, "", 1);
            }
            // Whitespace-insensitive so this does not silently stop working if
            // the framing's blank lines are ever laid out differently.
            let remainder = remainder.split_whitespace().collect::<Vec<_>>().join(" ");
            if remainder.is_empty() || remainder == "begin; commit;" {
                for guard in &guards {
                    self.script = self.script.replacen(&format!("{guard}\n\n"), "", 1);
                }
            }
        }

        Ok(())
    }

    /// Whether the migration must start with `set standard_conforming_strings = on;`.
    ///
    /// Every literal pgc 1.1+ dumps is in standard form, because its
    /// connections force the setting on. A FROM server whose clients default
    /// to `off` (PostgreSQL 14–18 only) would read such a literal as an
    /// escape string when the migration is applied there — `'C:\temp'`
    /// stores `C:`, a tab and `emp` — so the migration switches the setting
    /// on for its own session, as a `pg_dump` script does.
    ///
    /// Only when the TO dump was written by pgc 1.1+ (it records
    /// `server_version_num`): a pgc 1.0.x dump holds literals in whatever
    /// form its server's setting produced, and that form is unknown, so the
    /// output stays exactly what pgc 1.0.x emitted.
    ///
    /// A FROM dump written by pgc 1.0.x does not record the setting either,
    /// so against a pgc 1.1+ TO dump the guard is emitted whatever the FROM
    /// server's default. There it matters most: such a dump holds every
    /// backslash literal of an `off` server doubled, each one reads as
    /// changed against the standard form in the TO dump, and the migration
    /// rewrites them all. For a server already at `on` the `SET` changes
    /// nothing, and like any guard it is withdrawn from an otherwise empty
    /// migration.
    fn needs_standard_strings_guard(&self) -> bool {
        self.to.server_version_num.is_some()
            && (self.from.standard_conforming_strings_off || self.from.server_version_num.is_none())
    }

    /// Whether `CREATE TABLE` on the migrated server gives the sequence of a
    /// `serial` column an unlogged persistence: never in a logged table, and
    /// in an unlogged one from PostgreSQL 15, which added unlogged sequences.
    /// `None` when the FROM dump does not record the server version.
    fn serial_sequence_unlogged(&self, table_unlogged: bool) -> Option<bool> {
        if !table_unlogged {
            return Some(false);
        }
        self.from
            .server_version_num
            .map(|v| v >= UNLOGGED_SEQUENCE_MIN_VERSION)
    }

    /// The column defaults of the `to` tables this migration creates (those
    /// missing from `from`) that name a relation through a `regclass` cast, as
    /// `(index into to, default)`. `nextval`, `currval` and `setval` of a
    /// sequence are always rendered that way.
    fn new_table_regclass_defaults<'a>(from: &[Table], to: &'a [Table]) -> Vec<(usize, &'a str)> {
        let from_tables: HashSet<(&str, &str)> = from
            .iter()
            .map(|t| (t.schema.as_str(), t.name.as_str()))
            .collect();
        to.iter()
            .enumerate()
            .filter(|(_, t)| !from_tables.contains(&(t.schema.as_str(), t.name.as_str())))
            .flat_map(|(i, t)| {
                t.columns.iter().filter_map(move |c| {
                    c.column_default
                        .as_deref()
                        .filter(|d| d.contains("::regclass"))
                        .map(|d| (i, d))
                })
            })
            .collect()
    }

    /// Whether a default in `new_table_defaults` (see
    /// `new_table_regclass_defaults`) names `sequence` from a new table that
    /// is created before `owner`, the new table whose column owns the
    /// sequence. `creation_rank[i]` is the position of `self.to.tables[i]` in
    /// `ordered_tables`, the order `compare_tables` creates new tables in.
    ///
    /// A `serial` column's sequence only exists once its table is created, so
    /// such a table would fail on it. Every other reference is fine: a new
    /// table created later (which includes each partition and inheritance
    /// child of `owner`, carrying its very default), a table already in FROM
    /// (altered only after every new table is created), and another column
    /// of `owner` itself (`CREATE TABLE` creates a serial column's sequence
    /// before the table).
    ///
    /// The sequence is matched as the `'schema.name'` or bare `'name'` literal
    /// `pg_get_expr` prints; a false match only costs the explicit path, which
    /// is always correct.
    fn sequence_named_by_earlier_new_table(
        new_table_defaults: &[(usize, &str)],
        creation_rank: &[usize],
        sequence: &Sequence,
        owner: usize,
    ) -> bool {
        let literal = |name: String| format!("'{}'", name.replace('\'', "''"));
        let qualified = literal(format!("{}.{}", sequence.schema, sequence.name));
        let bare = literal(sequence.name.clone());
        new_table_defaults.iter().any(|&(table, default)| {
            creation_rank[table] < creation_rank[owner]
                && (default.contains(&qualified) || default.contains(&bare))
        })
    }

    /// The position of every table of `tables` in `ordered_tables(tables)`,
    /// indexed like `tables`.
    fn creation_ranks(tables: &[Table]) -> Vec<usize> {
        let index: HashMap<(&str, &str), usize> = tables
            .iter()
            .enumerate()
            .map(|(i, t)| ((t.schema.as_str(), t.name.as_str()), i))
            .collect();
        let mut ranks = vec![0; tables.len()];
        for (rank, table) in Self::ordered_tables(tables).into_iter().enumerate() {
            ranks[index[&(table.schema.as_str(), table.name.as_str())]] = rank;
        }
        ranks
    }

    /// Mark columns in `self.to.tables` that should be output as serial/bigserial/smallserial.
    /// Called after `compare_sequences()` which populates `self.serial_columns`.
    fn mark_serial_columns(&mut self) {
        // Build index map for O(1) table lookup
        let to_table_idx: HashMap<(&str, &str), usize> = self
            .to
            .tables
            .iter()
            .enumerate()
            .map(|(i, t)| ((t.schema.as_str(), t.name.as_str()), i))
            .collect();

        // Collect indices first to avoid borrow conflict between the
        // shared borrow of `self.serial_columns` and the mutable borrow
        // of `self.to.tables` below.
        let updates: Vec<(usize, String, String)> = self
            .serial_columns
            .iter()
            .filter_map(|((schema, table, column), serial_type)| {
                to_table_idx
                    .get(&(schema.as_str(), table.as_str()))
                    .map(|&idx| (idx, column.clone(), serial_type.clone()))
            })
            .collect();

        for (tidx, column, serial_type) in updates {
            if let Some(to_column) = self.to.tables[tidx]
                .columns
                .iter_mut()
                .find(|c| c.name == column)
            {
                to_column.serial_type = Some(serial_type);
            }
        }
    }

    /// Mark the FROM generated columns that PostgreSQL 14 keeps depending on
    /// what their expression uses ([`TableColumn::generation_blocks_retype`])
    /// and whose expression calls a routine this migration drops with
    /// `DROP ... CASCADE` — one TO lacks, or one
    /// `Comparer::routine_is_recreated` — as
    /// [`TableColumn::generation_routine_dropped`]: TO making such a column
    /// plain has to rewrite it rather than `DROP EXPRESSION` it, or that
    /// `CASCADE` takes the plain column with its data (see
    /// `TableColumn::plain_needs_rewrite`). Only with `use_drop`, which
    /// otherwise comments every routine drop out; called before `drop_views`,
    /// which plans around the columns the table phase rewrites.
    fn mark_generation_routines_dropped(&mut self) {
        if !self.use_drop
            || !self
                .from
                .tables
                .iter()
                .flat_map(|t| t.columns.iter())
                .any(|c| c.generation_blocks_retype)
        {
            return;
        }
        let to_routines: HashMap<(&str, &str, &str), &Routine> = self
            .to
            .routines
            .iter()
            .map(|r| {
                (
                    (r.schema.as_str(), r.name.as_str(), r.arguments.as_str()),
                    r,
                )
            })
            .collect();
        let dropped: HashSet<(String, String)> = self
            .from
            .routines
            .iter()
            .filter(|from| {
                to_routines
                    .get(&(
                        from.schema.as_str(),
                        from.name.as_str(),
                        from.arguments.as_str(),
                    ))
                    .is_none_or(|to| Self::routine_is_recreated(from, to))
            })
            .map(Self::unquoted_routine_name)
            .collect();
        if dropped.is_empty() {
            return;
        }
        for table in &mut self.from.tables {
            for column in &mut table.columns {
                if column.generation_blocks_retype
                    && column.is_generated_column()
                    && column
                        .generation_expression
                        .as_deref()
                        .is_some_and(|expr| Self::definition_references_any(expr, &dropped))
                {
                    column.generation_routine_dropped = true;
                }
            }
        }
    }

    #[inline]
    fn hashes_differ(a: &Option<String>, b: &Option<String>) -> bool {
        match (a, b) {
            (Some(lhs), Some(rhs)) => lhs != rhs,
            (Some(_), None) | (None, Some(_)) => true,
            (None, None) => false,
        }
    }

    /// Whether `compare_types` / `compare_enums` has something to alter to
    /// turn `from` into `to`: their hashes differ, and not only because of
    /// the ACL ([`PgType::differs_beyond_acl`]), which `compare_grants`
    /// migrates on its own. An ACL-only difference used to drop and recreate
    /// a range type, `CASCADE` taking every column of that type with it.
    fn type_definition_differs(from: &PgType, to: &PgType) -> bool {
        Self::hashes_differ(&from.hash, &to.hash) && from.differs_beyond_acl(to)
    }

    /// The warning for table `from` when it is typed by the same type in FROM
    /// and TO (`to`), its columns differ ([`Table::typed_column_changes`]),
    /// and that type is not compared: outside the dumped schemas, or missing
    /// from one dump. The table's alter leaves those columns to the type's
    /// `ALTER TYPE … CASCADE` (`ALTER TABLE` cannot add, drop or retype a
    /// typed table's column), which then never comes.
    fn typed_table_left_to_uncompared_type(
        from_types: &[PgType],
        to_types: &[PgType],
        from: &Table,
        to: &Table,
    ) -> Option<String> {
        let type_name = from.typed_table_type.as_deref()?;
        let changes = from.typed_column_changes(to);
        let compared = from_types.iter().any(|t| t.is_named_by(type_name))
            && to_types.iter().any(|t| t.is_named_by(type_name));
        if changes.is_empty() || compared {
            return None;
        }
        Some(format!(
            "table {}.{} is typed by {type_name}, which is not among the compared types, so its \
             column changes ({}) are not migrated: ALTER TABLE cannot make them on a typed table, \
             and they need ALTER TYPE {type_name} ... CASCADE (manual intervention needed).",
            from.schema,
            from.name,
            changes.join(", ")
        ))
    }

    /// What the migrated database keeps in composite type `from_type` of the
    /// FROM dump ([`CompositeUsage`]): what the dump recorded, or — for a dump
    /// written by pgc 1.0.x, which recorded neither — its typed tables among
    /// the FROM tables, with the columns storing it unknown.
    fn composite_usage(&self, from_type: &PgType) -> CompositeUsage {
        let mut usage = CompositeUsage::recorded(from_type);
        if from_type.typed_tables.is_none() {
            usage.typed_tables = self
                .from
                .tables
                .iter()
                .filter(|t| {
                    t.typed_table_type
                        .as_deref()
                        .is_some_and(|name| from_type.is_named_by(name))
                })
                .map(|t| format!("{}.{}", t.schema, t.name))
                .collect();
        }
        usage
    }

    /// Emit the diff for a single TO-side routine into `script`. Free
    /// associated function (rather than `&mut self` method) so callers
    /// can pass `&Routine` borrows into `self.from` / `self.to` while
    /// also passing `&mut self.script` — disjoint fields allow split
    /// borrows, which lets the dependency-sorted emit loops avoid
    /// cloning each `Routine` (with its potentially large `source_code`
    /// string) just to satisfy the borrow checker. Same rationale as
    /// [`Comparer::emit_drop`].
    ///
    /// `set_list_to_null` is whether the migrated server accepts
    /// `SET <list GUC> TO NULL` (see [`Routine::get_script_for_server`]); callers
    /// pass `migrated_server_at_least(SET_LIST_TO_NULL_MIN_VERSION)`.
    ///
    /// `no_aggregate_support` is the migrated server's version when it is
    /// known to predate aggregate `SUPPORT` (callers pass
    /// `migrated_server_known_below(AGGREGATE_SUPPORT_MIN_VERSION)`). Such a
    /// server takes `SUPPORT = …` with a `WARNING` and drops it, so the
    /// aggregate would be re-created by every later compare too: the TO
    /// aggregate is emitted without it, behind a note saying so, and one that
    /// differs from FROM only in its support function gets just the note.
    ///
    /// `no_set_list_to_null` is the migrated server's version when it is
    /// known to predate `SET <list GUC> TO NULL` (callers pass
    /// `migrated_server_known_below(SET_LIST_TO_NULL_MIN_VERSION)`). Such a
    /// server gets the `SET … = ''` fallback, which it stores as `""`, never
    /// as TO's bare empty list, so the routine is compared in that form
    /// ([`Routine::with_empty_lists_as_empty_strings`]): one FROM already
    /// holds that way gets only a note, and every other one a note besides
    /// its `CREATE OR REPLACE`. Without it the routine was re-created by
    /// every compare. An unknown version (a pgc 1.0.x FROM dump) keeps the
    /// fallback without a note, as before.
    #[allow(clippy::too_many_arguments)] // Each flag is one migrated-server capability; a struct would only rename them.
    fn emit_routine_diff(
        script: &mut String,
        use_drop: bool,
        set_list_to_null: bool,
        no_set_list_to_null: Option<i32>,
        no_aggregate_support: Option<i32>,
        routine: &Routine,
        from_routine: Option<&Routine>,
        cascade: bool,
    ) {
        let stripped = no_aggregate_support
            .filter(|_| routine.hash.is_some())
            .and_then(|_| routine.without_aggregate_support());
        if let (Some(version), Some(stripped)) = (no_aggregate_support, stripped.as_ref()) {
            let support = routine.support_function.as_deref().unwrap_or_default();
            // An aggregate over `*` has no arguments to print.
            let args = if routine.arguments.is_empty() {
                "*"
            } else {
                routine.arguments.as_str()
            };
            script.push_str(&format!(
                "/* Aggregate {}.{}({}): SUPPORT = {} is left out. It needs PostgreSQL 19, \
                 and the migrated server (server_version_num {}) accepts it with a WARNING \
                 but does not store it. */\n",
                block_comment_safe(&routine.schema),
                block_comment_safe(&routine.name),
                block_comment_safe(args),
                block_comment_safe(support),
                version
            ));
            eprintln!(
                "Warning: aggregate {}.{}({}) has SUPPORT = {}, which the migrated server \
                 (server_version_num {}) cannot store; the migration leaves it out.",
                routine.schema, routine.name, args, support, version
            );
            if from_routine.is_some_and(|from| {
                from.hash.is_some() && !Self::hashes_differ(&from.hash, &stripped.hash)
            }) {
                return;
            }
            return Self::emit_routine_diff(
                script,
                use_drop,
                set_list_to_null,
                no_set_list_to_null,
                None,
                stripped,
                from_routine,
                cascade,
            );
        }

        let portable = no_set_list_to_null
            .filter(|_| !set_list_to_null && routine.hash.is_some())
            .and_then(|_| routine.with_empty_lists_as_empty_strings());
        if let (Some(version), Some(portable)) = (no_set_list_to_null, portable.as_ref()) {
            let settings = routine.empty_list_settings().join(", ");
            script.push_str(&format!(
                "/* Routine {}.{}({}): SET {} TO NULL (an empty list) needs PostgreSQL 19; \
                 the migrated server (server_version_num {}) gets the equivalent SET ... = '' \
                 instead, which it stores as \"\". */\n",
                block_comment_safe(&routine.schema),
                block_comment_safe(&routine.name),
                block_comment_safe(&routine.arguments),
                block_comment_safe(&settings),
                version
            ));
            eprintln!(
                "Warning: routine {}.{}({}) sets {} to an empty list (SET ... TO NULL), which \
                 the migrated server (server_version_num {}) cannot store; the migration uses \
                 the equivalent SET ... = '' instead.",
                routine.schema, routine.name, routine.arguments, settings, version
            );
            if from_routine.is_some_and(|from| {
                from.hash.is_some() && !Self::hashes_differ(&from.hash, &portable.hash)
            }) {
                return;
            }
        }

        if routine.hash.is_none() {
            script.push_str(
                format!(
                    "/* Skipping routine {}.{}({}) due to missing hash. */\n",
                    block_comment_safe(&routine.schema),
                    block_comment_safe(&routine.name),
                    block_comment_safe(&routine.arguments)
                )
                .as_str(),
            );
            return;
        }

        if let Some(from_routine) = from_routine {
            if from_routine.hash.is_none() {
                script.push_str(
                    format!(
                        "/* Skipping routine {}.{}({}) due to missing hash. */\n",
                        block_comment_safe(&from_routine.schema),
                        block_comment_safe(&from_routine.name),
                        block_comment_safe(&from_routine.arguments)
                    )
                    .as_str(),
                );
                return;
            }

            if Self::hashes_differ(&from_routine.hash, &routine.hash) {
                // PR #187 review (C10/C11): a default-argument change is
                // applied by `CREATE OR REPLACE` for the same identity
                // signature, so a defaults-only diff (detected because the
                // hash includes `arguments_defaults`) must NOT trigger the
                // CASCADE-drop path; `get_script()` already emits the
                // non-destructive `CREATE OR REPLACE` form. Only the changes
                // OR REPLACE refuses take the drop first.
                if Self::routine_requires_drop(from_routine, routine) {
                    // Across a routine-kind change, DROP ROUTINE: a replay of the
                    // migration then finds the TO-kind object and still drops it,
                    // where DROP FUNCTION on an aggregate (or procedure) errors.
                    let drop_script = if from_routine.kind.eq_ignore_ascii_case(&routine.kind) {
                        from_routine.get_drop_script()
                    } else {
                        from_routine.get_drop_routine_script()
                    };
                    let drop_script = if cascade {
                        drop_script
                    } else {
                        Self::without_cascade(&drop_script)
                    };
                    Self::emit_drop(script, use_drop, &drop_script);
                }
                script.push_str(
                    format!(
                        "/* Routine: {}.{}({})*/\n",
                        block_comment_safe(&routine.schema),
                        block_comment_safe(&routine.name),
                        block_comment_safe(&routine.arguments)
                    )
                    .as_str(),
                );
                script.push_str(routine.get_script_for_server(set_list_to_null).as_str());
            }
        } else {
            script.push_str(
                format!(
                    "/* Routine: {}.{}({})*/\n",
                    block_comment_safe(&routine.schema),
                    block_comment_safe(&routine.name),
                    block_comment_safe(&routine.arguments)
                )
                .as_str(),
            );
            script.push_str(routine.get_script_for_server(set_list_to_null).as_str());
        }
    }

    /// Whether turning `from` into `to` (same schema, name and identity
    /// arguments) needs a `DROP … CASCADE` before the `CREATE OR REPLACE`,
    /// because it changes something `CREATE OR REPLACE` refuses to: the
    /// return type, the identity arguments, the routine kind ("cannot change
    /// routine kind" — function, procedure, aggregate and window function are
    /// distinct), or an aggregate's kind (normal / ordered-set /
    /// hypothetical-set) or number of direct arguments.
    ///
    /// `recreate_routine_dependents` uses the same predicate to decide which
    /// routines' dependents the CASCADE takes with it.
    fn routine_requires_drop(from: &Routine, to: &Routine) -> bool {
        let aggregate_shape_differs = match (&from.aggregate_info, &to.aggregate_info) {
            (Some(f), Some(t)) => f.kind != t.kind || f.num_direct_args != t.num_direct_args,
            _ => false,
        };
        from.return_type != to.return_type
            || from.arguments != to.arguments
            || !from.kind.eq_ignore_ascii_case(&to.kind)
            || aggregate_shape_differs
    }

    /// Whether `emit_routine_diff` turns `from` into `to` with `DROP … CASCADE`
    /// followed by a create, rather than a `CREATE OR REPLACE` in place: the two
    /// differ, and in something [`Comparer::routine_requires_drop`] names.
    fn routine_is_recreated(from: &Routine, to: &Routine) -> bool {
        from.hash.is_some()
            && to.hash.is_some()
            && Self::hashes_differ(&from.hash, &to.hash)
            && Self::routine_requires_drop(from, to)
    }

    /// What an object holds right after the migration creates it and sets its
    /// owner, as the [`acl::CreatedAcl`] every such object's grants are
    /// planned from ([`acl::generate_created_object_grants`]): a new object,
    /// and one the migration drops and recreates — a routine whose return type
    /// or kind changes or that a pre-drop or another routine's `CASCADE`
    /// takes, a view a table change or a routine's `CASCADE` takes, a table
    /// the alter path rebuilds, a range type. `default_type` is the kind's
    /// `pg_default_acl.defaclobjtype` (`r` for tables, views and foreign
    /// tables, `S`, `f`, `T`), `object_kind` its `GRANT` keyword and
    /// `schema` / `owner` the TO object's. The FROM object's own ACL is no
    /// use for a recreated one: the `DROP` removed it.
    ///
    /// PostgreSQL starts the object from `acldefault(kind, owner)` — the
    /// owner, and for routines and types PUBLIC's `EXECUTE` / `USAGE` — unless
    /// the creating role has default privileges: its database-wide row for the
    /// kind replaces `acldefault()`, its row for the object's schema adds to
    /// it, and `ALTER … OWNER TO` hands its own entries to the owner. The role
    /// that runs the migration cannot be known, so the object may start from
    /// the state of any role with FROM rows, or from `acldefault()` for a role
    /// without:
    ///
    /// - `possible` is what any of those states holds: PUBLIC's default entry
    ///   plus whatever a row grants anyone but its own role;
    /// - `certain` is what all of them hold: PUBLIC's default entry, unless a
    ///   role's database-wide row takes it away and none of that role's rows
    ///   for the schema gives it back.
    ///
    /// Grants are computed against `certain` and revokes against `possible`,
    /// so a `REVOKE ... FROM PUBLIC` the target carries is emitted however the
    /// FROM defaults read, and so is every `GRANT` it needs. Without rows both
    /// are PUBLIC's default entry alone (nothing, for a relation) — what a TO
    /// object with a NULL ACL is compared as, so nothing is granted or revoked
    /// for it.
    ///
    /// Relations keep one assumption on the `certain` side: in full mode a new
    /// or recreated table, view, sequence or foreign table has always been
    /// planned as holding what FROM's rows for its schema grant, so such a
    /// privilege is not granted again. Add-only mode, which revokes nothing,
    /// plans them from `acldefault()` alone and grants every TO entry.
    fn created_object_acl(
        &self,
        default_type: &str,
        schema: &str,
        object_kind: &str,
        owner: &str,
    ) -> acl::CreatedAcl {
        let rows: Vec<&crate::dump::default_privilege::DefaultPrivilege> = self
            .from
            .default_privileges
            .iter()
            .filter(|dp| {
                dp.object_type == default_type
                    && (dp.schema_name.is_empty() || dp.schema_name == schema)
            })
            .collect();
        let public_default = acl::public_default_entry(object_kind, owner);
        let public_privilege = acl::public_default_privilege(object_kind);
        let gives_public = |dp: &crate::dump::default_privilege::DefaultPrivilege| {
            public_privilege.is_some_and(|privilege| {
                dp.acl
                    .iter()
                    .filter_map(|item| acl::AclEntry::parse(item))
                    .any(|entry| entry.grantee.is_empty() && entry.privileges.contains(privilege))
            })
        };
        // A role's state holds PUBLIC's default unless its database-wide row
        // replaces acldefault() without it and no row for the schema adds it.
        let public_certain = public_default.is_some()
            && rows.iter().all(|dp| {
                !dp.schema_name.is_empty()
                    || gives_public(dp)
                    || rows.iter().any(|other| {
                        other.role_name == dp.role_name
                            && !other.schema_name.is_empty()
                            && gives_public(other)
                    })
            });
        let relation_defaults_held =
            matches!(default_type, "r" | "S") && self.grants_mode == GrantsMode::Full;
        let mut possible: Vec<String> = public_default.iter().cloned().collect();
        let mut certain: Vec<String> = if public_certain {
            public_default.into_iter().collect()
        } else {
            Vec::new()
        };
        for dp in &rows {
            let role = unquote_ident(&dp.role_name);
            for item in &dp.acl {
                let Some(entry) = acl::AclEntry::parse(item) else {
                    continue;
                };
                // The creating role's own entries become the owner's.
                if !entry.grantee.is_empty() && unquote_ident(&entry.grantee) == role {
                    continue;
                }
                if !possible.contains(item) {
                    possible.push(item.clone());
                }
                if relation_defaults_held && !dp.schema_name.is_empty() && !certain.contains(item) {
                    certain.push(item.clone());
                }
            }
        }
        acl::CreatedAcl { certain, possible }
    }

    /// Append a DROP-shaped script to `script`, gated on `use_drop`. When
    /// `use_drop` is false, every line is line-prefixed with `-- ` so the
    /// SQL is left in the output for review but won't execute. Free
    /// associated function (rather than `&mut self` method) so callers
    /// holding a borrow into another field of `self` (e.g. a `from_map`
    /// keyed by `&self.from.X`) can still emit drops without a borrow
    /// conflict on disjoint fields.
    fn emit_drop(script: &mut String, use_drop: bool, drop_script: &str) {
        if use_drop {
            script.push_str(drop_script);
        } else {
            script.push_str(&comment_out(drop_script));
        }
    }

    /// `drop_script` — a routine's `drop … cascade;`, as
    /// [`Routine::get_drop_script`] writes it — without its `CASCADE`, so the
    /// drop fails while anything still depends on the routine.
    fn without_cascade(drop_script: &str) -> String {
        match drop_script.rfind(" cascade;") {
            Some(pos) => format!(
                "{}{}",
                &drop_script[..pos],
                &drop_script[pos + " cascade".len()..]
            ),
            None => drop_script.to_string(),
        }
    }

    /// Build the partition topology maps the production output path needs:
    /// the set of partitioned parent tables, each parent's direct partitions,
    /// and the set of indexes that live on a partitioned parent (which cannot
    /// be dropped with `DROP INDEX CONCURRENTLY`). All keys use the dump's
    /// `quote_ident`-applied `schema.name` form so they match `partition_of`.
    fn build_partition_context_maps(
        &self,
    ) -> (
        HashSet<String>,
        HashMap<String, Vec<ChildRef>>,
        HashSet<String>,
    ) {
        let mut partitioned_parents: HashSet<String> = HashSet::new();
        let mut partitioned_indexes: HashSet<String> = HashSet::new();
        for table in self.to.tables.iter().chain(self.from.tables.iter()) {
            if table.partition_key.is_some() {
                partitioned_parents.insert(format!("{}.{}", table.schema, table.name));
                for index in &table.indexes {
                    if !index.is_partition_index {
                        partitioned_indexes.insert(format!("{}.{}", index.schema, index.name));
                    }
                }
            }
        }
        // Children come from the TO side — the schema we are migrating toward.
        let mut children: HashMap<String, Vec<ChildRef>> = HashMap::new();
        for table in &self.to.tables {
            if let Some(parent) = &table.partition_of {
                children.entry(parent.clone()).or_default().push(ChildRef {
                    schema: table.schema.clone(),
                    table: table.name.clone(),
                });
            }
        }
        (partitioned_parents, children, partitioned_indexes)
    }

    /// Emit a new table's CREATE (without triggers) plus its indexes, rewriting
    /// every index for production (concurrent / partition-aware). Free function
    /// over the two buffers so it can run while `self.to` is borrowed by the
    /// emission loops.
    ///
    /// A `REPLICA IDENTITY USING INDEX` on one of those indexes goes
    /// post-commit after them, since the index may not exist before then.
    fn emit_new_table_create_prod(
        script: &mut String,
        post_commit: &mut String,
        table: &Table,
        ctx: &PartitionContext,
    ) {
        script.push_str(&table.get_script_without_triggers_no_indexes());
        for index in table.creatable_indexes() {
            let split = production::index_create_split(index, ctx, true);
            script.push_str(&split.in_txn);
            post_commit.push_str(&split.post_commit);
        }
        if let Some(stmt) = table.replica_identity_create_script_after_indexes() {
            post_commit.push_str(&stmt);
        }
    }

    /// Emit the index changes of an [`IndexAlterPlan`] in default (non-production)
    /// mode: drops first, then comment-only changes, then (re)creates — the same
    /// ordering `Table::build_alter_script` uses for a table's own indexes. Used
    /// for a materialized view that is not being recreated, whose indexes
    /// therefore survive and have to be reconciled in place (issue #235).
    fn emit_index_alter_plan(script: &mut String, plan: &IndexAlterPlan, use_drop: bool) {
        for old_index in &plan.drop {
            let drop_cmd = format!(
                "drop index if exists {}.{};",
                old_index.schema, old_index.name
            )
            .with_empty_lines();
            if use_drop {
                script.push_str(&drop_cmd);
            } else {
                script.push_str(&comment_out_keep_blank_lines(&drop_cmd));
            }
        }
        for index in &plan.comment_changes {
            if let Some(comment) = &index.comment {
                script.append_block(&format!(
                    "comment on index {}.{} is '{}';",
                    index.schema,
                    index.name,
                    comment.replace('\'', "''")
                ));
            } else {
                script.append_block(&format!(
                    "comment on index {}.{} is null;",
                    index.schema, index.name
                ));
            }
        }
        for index in &plan.create {
            script.push_str(&index.get_script());
        }
    }

    /// Emit the index changes of an ALTER for production: drops (concurrent
    /// unless on a partitioned table), comment-only changes (in-txn), then
    /// (re)creates (concurrent / partition-aware). Mirrors the ordering of
    /// `build_alter_script` (drops before creates).
    fn emit_index_alter_plan_prod(
        script: &mut String,
        post_commit: &mut String,
        plan: &IndexAlterPlan,
        use_drop: bool,
        ctx: &PartitionContext,
    ) {
        for old_index in &plan.drop {
            let (stmt, post) = production::index_drop_statement(old_index, ctx);
            let block = stmt.with_empty_lines();
            let target = if post {
                &mut *post_commit
            } else {
                &mut *script
            };
            if use_drop {
                target.push_str(&block);
            } else {
                target.push_str(&comment_out_keep_blank_lines(&block));
            }
        }
        for index in &plan.comment_changes {
            if let Some(comment) = &index.comment {
                script.append_block(&format!(
                    "comment on index {}.{} is '{}';",
                    index.schema,
                    index.name,
                    comment.replace('\'', "''")
                ));
            } else {
                script.append_block(&format!(
                    "comment on index {}.{} is null;",
                    index.schema, index.name
                ));
            }
        }
        for index in &plan.create {
            let split = production::index_create_split(index, ctx, false);
            script.push_str(&split.in_txn);
            post_commit.push_str(&split.post_commit);
        }
    }

    fn get_script(&self) -> String {
        if self.use_comments {
            return self.script.clone();
        }
        let result = super::scanner::strip_comments_and_collapse(&self.script);
        let trimmed = result.trim();
        if trimmed.is_empty() {
            return String::new();
        }
        let mut out = trimmed.to_string();
        out.push('\n');
        out
    }

    fn normalized_view_reference(reference: &str) -> String {
        reference
            .trim()
            .chars()
            .filter(|c| !matches!(c, '"' | '\'' | '`'))
            .collect::<String>()
            .to_lowercase()
    }

    fn normalized_view_key(schema: &str, name: &str) -> String {
        let mut result = String::with_capacity(schema.len() + name.len() + 1);
        for c in schema.chars() {
            if !matches!(c, '"' | '\'' | '`') {
                for lc in c.to_lowercase() {
                    result.push(lc);
                }
            }
        }
        result.push('.');
        for c in name.chars() {
            if !matches!(c, '"' | '\'' | '`') {
                for lc in c.to_lowercase() {
                    result.push(lc);
                }
            }
        }
        result
    }

    /// Check whether `lowered` / `unquoted_lowered` (both prelowered flavors
    /// of the haystack text — the second with `"` stripped) contain an
    /// occurrence of `schema_lc.name_lc` as a whole identifier (not as a
    /// substring of a longer identifier). Call sites in quadratic loops
    /// should prelower the haystack once via `prelower_pair` and reuse it
    /// across inner iterations.
    ///
    /// Dump fields populated via `quote_ident` literally embed `"` for
    /// identifiers that need quoting (mixed case, reserved words, etc.), so
    /// the needle pair `(schema_lc, name_lc)` may also contain quotes. We
    /// match the original (possibly-quoted) needle against `lowered` first
    /// so case-sensitive references survive, then fall back to a
    /// quote-stripped needle against `unquoted_lowered` so a quoted
    /// definition reference is still found in source code that did not
    /// quote it (and vice versa).
    #[inline]
    fn text_references_qualified_name_pre(
        lowered: &str,
        unquoted_lowered: &str,
        schema_lc: &str,
        name_lc: &str,
    ) -> bool {
        let mut pattern = String::with_capacity(schema_lc.len() + 1 + name_lc.len());
        pattern.push_str(schema_lc);
        pattern.push('.');
        pattern.push_str(name_lc);
        if Self::has_whole_qualified_name(lowered, &pattern) {
            return true;
        }
        let unquoted_pattern: String = if pattern.contains('"') {
            pattern.chars().filter(|c| *c != '"').collect()
        } else {
            pattern
        };
        Self::has_whole_qualified_name(unquoted_lowered, &unquoted_pattern)
    }

    /// Build `(lowercase, lowercase_with_quotes_stripped)` from `text` in a
    /// single lowercase pass.
    fn prelower_pair(text: &str) -> (String, String) {
        let lower = text.to_lowercase();
        let unquoted: String = lower.chars().filter(|c| *c != '"').collect();
        (lower, unquoted)
    }

    /// [`Comparer::prelower_pair`] for text whose references are about to be
    /// read as **dependency edges** — a routine's `prosrc`, a view's
    /// definition — rather than merely searched.
    ///
    /// Comments and string-literal bodies are blanked first
    /// (`scanner::blank_comments_and_literals`), because neither can hold a
    /// reference PostgreSQL resolves when the object is created: a comment
    /// naming another routine is documentation, and a name inside a quoted
    /// literal is dynamic SQL. Counting either invents an edge, and one
    /// invented edge pointing back along a real one is a cycle Kahn's sort
    /// cannot order — at which point it appends the whole blocked subgraph in
    /// plain name order and the script creates routines before their callees
    /// (issue #240).
    ///
    /// Only the dependency scans use this. Plain `prelower_pair` stays for
    /// haystacks where a literal match is still a match.
    fn prelower_pair_for_dependency_scan(text: &str) -> (String, String) {
        Self::prelower_pair(&super::scanner::blank_comments_and_literals(text))
    }

    /// Check whether a routine's `aggregate_info` references a function
    /// identified by `(schema, name)` (both lowercase).  Aggregate transition
    /// functions, final functions, etc. are stored as qualified or unqualified
    /// names; this helper normalises and compares them.
    pub(crate) fn aggregate_references_function(
        routine: &Routine,
        schema: &str,
        name: &str,
    ) -> bool {
        let agg = match &routine.aggregate_info {
            Some(a) => a,
            None => return false,
        };
        let qualified = format!("{}.{}", schema, name);
        let refs: Vec<&str> = [
            Some(agg.sfunc.as_str()),
            agg.finalfunc.as_deref(),
            agg.combinefunc.as_deref(),
            agg.serialfunc.as_deref(),
            agg.deserialfunc.as_deref(),
            agg.msfunc.as_deref(),
            agg.minvfunc.as_deref(),
            agg.mfinalfunc.as_deref(),
        ]
        .into_iter()
        .flatten()
        .collect();
        for r in refs {
            let lower = r.to_lowercase().replace('"', "");
            if lower == qualified || lower == name {
                return true;
            }
        }
        false
    }

    /// Return `true` when `text` contains `qualified` delimited by
    /// non-identifier characters on both sides.
    fn has_whole_qualified_name(text: &str, qualified: &str) -> bool {
        let bytes = text.as_bytes();
        for (idx, _) in text.match_indices(qualified) {
            if idx > 0 {
                let before = bytes[idx - 1];
                if before.is_ascii_alphanumeric() || before == b'_' {
                    continue;
                }
            }
            let end = idx + qualified.len();
            if end < bytes.len() {
                let after = bytes[end];
                if after.is_ascii_alphanumeric() || after == b'_' || after == b'.' {
                    continue;
                }
            }
            return true;
        }
        false
    }

    /// Name, on stderr, the objects caught in a dependency cycle.
    ///
    /// `kahn_toposort` cannot order a cycle. It appends every node whose
    /// in-degree never reached zero in plain sort-key order, so the script
    /// still contains all of them — in an order that ignores their real
    /// dependencies, and which therefore may not replay. Before issue #240
    /// that happened silently; the reported symptom was a migration failing on
    /// `function ... does not exist` with nothing in the output to suggest
    /// why.
    ///
    /// The set is narrowed to true SCC members (size >= 2) rather than
    /// reported straight from `kahn_toposort_detect_cycle`, whose remainder
    /// also contains everything merely *blocked behind* a cycle — the same
    /// distinction `topo_order_within_subset_detect_cycle` draws. Naming a
    /// routine that is only downstream of the problem sends the reader to the
    /// wrong file.
    ///
    /// stderr rather than a comment in the script: `USE_COMMENTS=false` strips
    /// commented-out DDL from the output, and a diagnostic a config flag can
    /// delete is not a diagnostic.
    fn warn_dependency_cycle(
        what: &str,
        n: usize,
        depends_on: &[HashSet<usize>],
        label: impl Fn(usize) -> String,
    ) {
        let mut cycles: Vec<Vec<String>> = Self::strongly_connected_components(n, depends_on)
            .into_iter()
            .filter(|scc| scc.len() >= 2)
            .map(|scc| {
                let mut names: Vec<String> = scc.into_iter().map(&label).collect();
                names.sort();
                names
            })
            .collect();
        if cycles.is_empty() {
            return;
        }
        // Deterministic order, so the same schemas always warn the same way.
        cycles.sort();
        for names in cycles {
            eprintln!(
                "Warning: dependency cycle among {what}: {}. pgc cannot order these, \
                 so the script emits them in name order and may not replay as written. \
                 A cycle here is either genuine (mutually recursive routines) or an \
                 artefact of the text scan; if the migration fails on a missing \
                 function, --guard-sql-routine-bodies will get it through.",
                names.join(", ")
            );
        }
    }

    /// Kahn's topological sort for an index-based dependency graph.
    /// Returns indices so that dependencies come before dependents.
    /// Ties are broken by `sort_key` for deterministic output.
    /// Cycles are handled by appending remaining nodes in sort_key order.
    fn kahn_toposort<K: Ord>(
        n: usize,
        depends_on: &[HashSet<usize>],
        sort_key: impl Fn(usize) -> K,
    ) -> Vec<usize> {
        Self::kahn_toposort_detect_cycle(n, depends_on, sort_key).0
    }

    /// Like [`Comparer::kahn_toposort`], but also returns the set of nodes that
    /// could not be ordered acyclically — i.e. nodes whose in-degree
    /// never reached zero during the BFS. Those nodes still appear in
    /// the returned `Vec` (appended in `sort_key` order so the result
    /// is deterministic for callers that don't care about cycles), but
    /// the second return value lets a caller act on the cycle —
    /// `emit_persistence_changes` uses it to drop FKs that form a
    /// SET-ordering deadlock and re-add them after the SET statements
    /// (issue #191).
    ///
    /// **Important**: the returned cyclic set contains every node
    /// Kahn was unable to remove — that includes nodes *blocked by*
    /// a cycle (e.g. `C` in `A↔B + A→C`), not only nodes *in* a
    /// cycle. Callers that need to act on *true* cycle members
    /// should pair this with [`Comparer::strongly_connected_components`] —
    /// `topo_order_within_subset_detect_cycle` does exactly that
    /// (PR #198 review).
    fn kahn_toposort_detect_cycle<K: Ord>(
        n: usize,
        depends_on: &[HashSet<usize>],
        sort_key: impl Fn(usize) -> K,
    ) -> (Vec<usize>, HashSet<usize>) {
        let mut in_degree: Vec<usize> = depends_on.iter().map(|d| d.len()).collect();
        let mut reverse_adj: Vec<Vec<usize>> = vec![vec![]; n];
        for (i, deps) in depends_on.iter().enumerate() {
            for &j in deps {
                reverse_adj[j].push(i);
            }
        }

        let mut queue: VecDeque<usize> = {
            let mut zero: Vec<usize> = (0..n).filter(|&i| in_degree[i] == 0).collect();
            zero.sort_by_key(|a| sort_key(*a));
            VecDeque::from(zero)
        };

        let mut sorted = Vec::with_capacity(n);
        while let Some(node) = queue.pop_front() {
            sorted.push(node);
            let mut newly_free: Vec<usize> = Vec::new();
            for &dependent in &reverse_adj[node] {
                in_degree[dependent] -= 1;
                if in_degree[dependent] == 0 {
                    newly_free.push(dependent);
                }
            }
            newly_free.sort_by_key(|a| sort_key(*a));
            queue.extend(newly_free);
        }

        let mut cyclic: HashSet<usize> = HashSet::new();
        if sorted.len() < n {
            let in_sorted: HashSet<usize> = sorted.iter().copied().collect();
            let mut remaining: Vec<usize> = (0..n).filter(|i| !in_sorted.contains(i)).collect();
            remaining.sort_by_key(|a| sort_key(*a));
            cyclic.extend(remaining.iter().copied());
            sorted.extend(remaining);
        }

        (sorted, cyclic)
    }

    /// Tarjan's strongly-connected-components algorithm, iterative
    /// form to avoid stack overflow on adversarial graphs. Returns
    /// each SCC as a `Vec<usize>` of node indices into the same
    /// `0..n` space `depends_on` uses. Singleton SCCs (size 1) are
    /// included; callers that want *cycle* membership specifically
    /// should keep only SCCs of size >= 2 (self-loops never occur
    /// in the FK-adjacency call site, which filters `j != i`).
    ///
    /// Used by `topo_order_within_subset_detect_cycle` to narrow
    /// Kahn's "couldn't-be-ordered" remainder down to nodes that
    /// actually participate in a directed cycle, rather than nodes
    /// merely *blocked by* one (PR #198 review).
    fn strongly_connected_components(n: usize, depends_on: &[HashSet<usize>]) -> Vec<Vec<usize>> {
        // Iterative Tarjan with an explicit work stack. Each work-
        // stack frame is `(node v, neighbours_remaining: Vec<usize>)`
        // — a snapshot of v's outgoing edges, popped one at a time so
        // we can suspend processing of v when we descend into a
        // neighbour and resume exactly where we left off after the
        // neighbour's frame finishes. The post-enter book-keeping
        // (assigning index/lowlink, pushing onto `tarjan_stack`,
        // marking `on_stack`) runs once per node at the point we
        // push its frame onto `work` — either in the outer
        // `for start in 0..n` loop for roots, or in the
        // `index[w].is_none()` branch for descents.
        let mut index_counter: usize = 0;
        let mut tarjan_stack: Vec<usize> = Vec::new();
        let mut on_stack = vec![false; n];
        let mut index: Vec<Option<usize>> = vec![None; n];
        let mut lowlink = vec![0usize; n];
        let mut sccs: Vec<Vec<usize>> = Vec::new();

        // Each work-stack entry is (v, neighbours_remaining).
        // `neighbours_remaining` is a `Vec<usize>` snapshot of the
        // outgoing edges from v, popped one-at-a-time so we can
        // suspend processing of v when we recurse into a neighbour.
        let mut work: Vec<(usize, Vec<usize>)> = Vec::new();

        for start in 0..n {
            if index[start].is_some() {
                continue;
            }
            // Enter `start`.
            index[start] = Some(index_counter);
            lowlink[start] = index_counter;
            index_counter += 1;
            tarjan_stack.push(start);
            on_stack[start] = true;
            work.push((start, depends_on[start].iter().copied().collect()));

            while let Some((v, neighbours)) = work.last_mut() {
                let v = *v;
                if let Some(w) = neighbours.pop() {
                    if index[w].is_none() {
                        // Recurse into w.
                        index[w] = Some(index_counter);
                        lowlink[w] = index_counter;
                        index_counter += 1;
                        tarjan_stack.push(w);
                        on_stack[w] = true;
                        work.push((w, depends_on[w].iter().copied().collect()));
                    } else if on_stack[w] {
                        // Back-edge into the current SCC candidate.
                        let w_idx = index[w].expect("on_stack implies index is set");
                        lowlink[v] = lowlink[v].min(w_idx);
                    }
                    // Tree-edge to a fully-processed node in a
                    // different SCC: ignore (don't update lowlink).
                } else {
                    // All neighbours exhausted; pop and propagate
                    // lowlink up to the parent (if any).
                    let v_lowlink = lowlink[v];
                    let v_index = index[v].expect("entered node has an index");
                    if v_lowlink == v_index {
                        // v is an SCC root; pop the SCC off
                        // `tarjan_stack`.
                        let mut scc = Vec::new();
                        while let Some(w) = tarjan_stack.pop() {
                            on_stack[w] = false;
                            scc.push(w);
                            if w == v {
                                break;
                            }
                        }
                        sccs.push(scc);
                    }
                    work.pop();
                    if let Some((parent, _)) = work.last() {
                        let parent = *parent;
                        lowlink[parent] = lowlink[parent].min(v_lowlink);
                    }
                }
            }
        }

        sccs
    }

    /// Returns the fully-quoted key used to match a table's `partition_of` reference.
    fn table_key(schema: &str, name: &str) -> String {
        format!("\"{}\".\"{}\"", schema, name)
    }

    /// Normalise a `partition_of` reference so it can be compared against `table_key`.
    /// Handles both quoted ("schema"."name") and unquoted (schema.name) forms.
    fn normalise_partition_of(s: &str) -> String {
        s.split('.')
            .map(|part| {
                let trimmed = part.trim().trim_matches('"');
                format!("\"{}\"", trimmed)
            })
            .collect::<Vec<_>>()
            .join(".")
    }

    /// Compute the partition depth of every table (0 = not a partition / root,
    /// 1 = direct child of a partitioned table, 2 = grandchild, …).
    /// This handles arbitrary nesting depths (sub-partitions of sub-partitions).
    fn partition_depths(tables: &[Table]) -> HashMap<String, usize> {
        // Build a map from normalised key → depth, iterating until stable.
        let keys: Vec<String> = tables
            .iter()
            .map(|t| Self::table_key(&t.schema, &t.name))
            .collect();
        let mut depths: HashMap<String, usize> = keys.iter().cloned().map(|k| (k, 0)).collect();

        // Propagate: a table's depth is parent_depth + 1.  Repeat until no change.
        loop {
            let mut changed = false;
            for table in tables {
                if let Some(parent_ref) = &table.partition_of {
                    let parent_key = Self::normalise_partition_of(parent_ref);
                    let parent_depth = depths.get(&parent_key).copied().unwrap_or(0);
                    let child_key = Self::table_key(&table.schema, &table.name);
                    let current = depths.get(&child_key).copied().unwrap_or(0);
                    let required = parent_depth + 1;
                    if required > current {
                        depths.insert(child_key, required);
                        changed = true;
                    }
                }
            }
            if !changed {
                break;
            }
        }
        depths
    }

    /// Order tables for CREATE and ALTER: parents before their partitions
    /// (all levels), and classical-inheritance parents before their children.
    fn ordered_tables(tables: &[Table]) -> Vec<&Table> {
        let depths = Self::partition_depths(tables);
        let mut ordered: Vec<&Table> = tables.iter().collect();
        ordered.sort_by(|a, b| {
            let da = depths
                .get(&Self::table_key(&a.schema, &a.name))
                .copied()
                .unwrap_or(0);
            let db = depths
                .get(&Self::table_key(&b.schema, &b.name))
                .copied()
                .unwrap_or(0);
            da.cmp(&db)
                .then_with(|| a.schema.cmp(&b.schema))
                .then_with(|| a.name.cmp(&b.name))
        });
        Self::order_inheritance(ordered, false)
    }

    /// Order tables for DROP: deepest partitions first (reverse of CREATE
    /// order), and classical-inheritance children before their parents.
    fn ordered_tables_for_drop(tables: &[Table]) -> Vec<&Table> {
        let depths = Self::partition_depths(tables);
        let mut ordered: Vec<&Table> = tables.iter().collect();
        ordered.sort_by(|a, b| {
            let da = depths
                .get(&Self::table_key(&a.schema, &a.name))
                .copied()
                .unwrap_or(0);
            let db = depths
                .get(&Self::table_key(&b.schema, &b.name))
                .copied()
                .unwrap_or(0);
            // Higher depth (deeper child) should come first when dropping
            db.cmp(&da)
                .then_with(|| a.schema.cmp(&b.schema))
                .then_with(|| a.name.cmp(&b.name))
        });
        Self::order_inheritance(ordered, true)
    }

    /// Reorder `ordered` so every classical-inheritance (`INHERITS`) child
    /// comes after each of its parents — or before them, with
    /// `children_first` — while every other table keeps the position
    /// `ordered` gave it.
    ///
    /// Partitions are ordered by depth already, but plain inheritance is not
    /// part of that sort, and PostgreSQL needs it respected: `CREATE TABLE c
    /// (...) INHERITS (p)` fails before `p` exists, `DROP TABLE p` fails while
    /// `c` does (no CASCADE), and on PostgreSQL 19 `ALTER CONSTRAINT ... NOT
    /// ENFORCED` on a child's merged CHECK fails while the parent's copy is
    /// still enforced — the parent's statement recurses and has to run first.
    ///
    /// A stable topological sort that always takes the earliest ready table,
    /// so an order that is already valid comes back unchanged. A cycle cannot
    /// occur in PostgreSQL; any leftovers keep their original order.
    fn order_inheritance(ordered: Vec<&Table>, children_first: bool) -> Vec<&Table> {
        if ordered.iter().all(|t| t.inherits_from.is_empty()) {
            return ordered;
        }
        let index = Self::relation_index(ordered.iter().copied());
        let n = ordered.len();
        // `waiting_on[i]`: how many tables must still be emitted before `i`;
        // `releases[i]`: the tables that wait on `i`.
        let mut waiting_on = vec![0usize; n];
        let mut releases: Vec<Vec<usize>> = vec![Vec::new(); n];
        for (child, table) in ordered.iter().enumerate() {
            for parent_ref in &table.inherits_from {
                let Some(parent) = Self::lookup_relation(&index, parent_ref) else {
                    continue;
                };
                if parent == child {
                    continue;
                }
                let (first, then) = if children_first {
                    (child, parent)
                } else {
                    (parent, child)
                };
                waiting_on[then] += 1;
                releases[first].push(then);
            }
        }
        let mut ready: BinaryHeap<Reverse<usize>> = (0..n)
            .filter(|&i| waiting_on[i] == 0)
            .map(Reverse)
            .collect();
        let mut emitted = vec![false; n];
        let mut out: Vec<&Table> = Vec::with_capacity(n);
        while let Some(Reverse(i)) = ready.pop() {
            emitted[i] = true;
            out.push(ordered[i]);
            for &j in &releases[i] {
                waiting_on[j] -= 1;
                if waiting_on[j] == 0 {
                    ready.push(Reverse(j));
                }
            }
        }
        out.extend((0..n).filter(|&i| !emitted[i]).map(|i| ordered[i]));
        out
    }

    /// Index `tables` by the forms a `partition_of` / `inherits_from`
    /// reference to them can take: the dump's own `schema.name` (both parts
    /// already `quote_ident`-ed) and the fully quoted [`Self::table_key`]
    /// form that [`Self::normalise_partition_of`] produces.
    fn relation_index<'a>(tables: impl Iterator<Item = &'a Table>) -> HashMap<String, usize> {
        let mut index = HashMap::new();
        for (i, table) in tables.enumerate() {
            index.insert(format!("{}.{}", table.schema, table.name), i);
            index
                .entry(Self::table_key(&table.schema, &table.name))
                .or_insert(i);
        }
        index
    }

    /// Resolve a `partition_of` / `inherits_from` reference against a
    /// [`Self::relation_index`].
    fn lookup_relation(index: &HashMap<String, usize>, reference: &str) -> Option<usize> {
        index
            .get(reference)
            .or_else(|| index.get(&Self::normalise_partition_of(reference)))
            .copied()
    }

    /// The in-place ALTER forms the migrated server — the FROM database —
    /// accepts, for every FROM table, keyed by `(schema, name)`. A table
    /// missing from the map gets [`TableAlterCaps::default`], which is also
    /// every table's answer when the FROM dump's server version is unknown.
    ///
    /// * CHECK `ALTER CONSTRAINT ... [NOT] ENFORCED`: PostgreSQL 19+, except
    ///   for a table with a CHECK going `NOT ENFORCED` whose copy in a TO-side
    ///   descendant stays enforced (see `check_flip_would_reach_enforced_copy`).
    /// * STORED `SET EXPRESSION`: PostgreSQL 17+.
    /// * VIRTUAL `SET EXPRESSION`: PostgreSQL 19+, or 18 when no table of the
    ///   family has a CHECK constraint (18 rejects it for "tables with check
    ///   constraints", and inherited CHECKs count). Never when one of them is
    ///   an explicit `FOR TABLE` member of a publication, which 18 and 19 both
    ///   reject; `FOR ALL TABLES` / `TABLES IN SCHEMA` do not block it.
    ///
    /// `SET EXPRESSION` on a partitioned table recurses to its partitions, so
    /// the VIRTUAL answer is computed over the whole inheritance / partition
    /// family and every member gets the same one — a parent and its children
    /// then agree on SET EXPRESSION versus drop + add. The family is taken
    /// from both dumps, CHECKs included: the create loop runs first, so a
    /// TO-only partition or child — and its CHECKs — already exists when the
    /// parent is altered. A family member whose parent is in neither dump
    /// makes the PostgreSQL 18 answer `false`, since that parent's CHECKs
    /// cannot be seen. Publication membership is read from the FROM dump
    /// alone: publications are altered after the tables.
    ///
    /// Computed once per compare, so every caller (the table alter loop, its
    /// Path B dependent restoration, and the view pre-drop in
    /// `dependent_view_keys`) sees the same answer.
    pub(crate) fn table_alter_caps(&self) -> HashMap<(String, String), TableAlterCaps> {
        self.table_alter_caps_and_left_out().0
    }

    /// [`Comparer::table_alter_caps`], and the FROM tables, sorted, whose
    /// `SET EXPRESSION` it turns off because `use_drop` is off and the
    /// statement would corrupt an inherited CHECK constraint (see
    /// `Comparer::set_expression_check_hazards`). Empty with `use_drop`.
    fn table_alter_caps_and_left_out(&self) -> (TableAlterCapsMap, Vec<(String, String)>) {
        let mut caps = self.base_table_alter_caps();
        let mut left_out = Vec::new();
        // Without `use_drop` no CHECK can be dropped out of the way, so such
        // a table gets the drop + add, which `use_drop` off leaves out too.
        if !self.use_drop {
            left_out.extend(self.set_expression_check_hazards(&caps).tables);
            left_out.sort();
            for key in &left_out {
                if let Some(table_caps) = caps.get_mut(key) {
                    table_caps.generated.stored_set_expression = false;
                    table_caps.generated.virtual_set_expression = false;
                }
            }
        }
        (caps, left_out)
    }

    /// [`Comparer::table_alter_caps`] before the `use_drop` adjustment.
    fn base_table_alter_caps(&self) -> HashMap<(String, String), TableAlterCaps> {
        if !self.migrated_server_at_least(170000) {
            return HashMap::new();
        }
        let at_18 = self.migrated_server_at_least(180000);
        let at_19 = self.migrated_server_at_least(190000);

        // Family members: every FROM table, then every TO-only table. A FROM
        // table's TO side contributes its edges and CHECKs too.
        let from_keys: HashSet<(&str, &str)> = self
            .from
            .tables
            .iter()
            .map(|t| (t.schema.as_str(), t.name.as_str()))
            .collect();
        let to_by_key: HashMap<(&str, &str), &Table> = self
            .to
            .tables
            .iter()
            .map(|t| ((t.schema.as_str(), t.name.as_str()), t))
            .collect();
        let members: Vec<&Table> = self
            .from
            .tables
            .iter()
            .chain(
                self.to
                    .tables
                    .iter()
                    .filter(|t| !from_keys.contains(&(t.schema.as_str(), t.name.as_str()))),
            )
            .collect();
        let n = members.len();
        let index = Self::relation_index(members.iter().copied());
        // The sides of member `i`: itself, plus its TO counterpart.
        let sides = |i: usize| {
            let table = members[i];
            let to_side = to_by_key
                .get(&(table.schema.as_str(), table.name.as_str()))
                .copied()
                .filter(|to| !std::ptr::eq(*to, table));
            std::iter::once(table).chain(to_side)
        };

        // Union-find over partition_of / inherits_from edges.
        let mut family: Vec<usize> = (0..n).collect();
        fn root(family: &mut [usize], mut i: usize) -> usize {
            while family[i] != i {
                family[i] = family[family[i]];
                i = family[i];
            }
            i
        }
        let mut parent_outside_dump = vec![false; n];
        for (i, parent_outside) in parent_outside_dump.iter_mut().enumerate() {
            for side in sides(i) {
                for reference in side.partition_of.iter().chain(side.inherits_from.iter()) {
                    match Self::lookup_relation(&index, reference) {
                        Some(j) => {
                            let (ri, rj) = (root(&mut family, i), root(&mut family, j));
                            family[ri] = rj;
                        }
                        None => *parent_outside = true,
                    }
                }
            }
        }

        // Per family: (has a CHECK, is an explicit publication member,
        // has a parent outside the dump).
        let mut traits: HashMap<usize, (bool, bool, bool)> = HashMap::new();
        for i in 0..n {
            let has_check = sides(i).any(|side| {
                side.constraints
                    .iter()
                    .any(|c| c.constraint_type.eq_ignore_ascii_case("check"))
            });
            let in_publication =
                i < self.from.tables.len() && self.is_explicit_publication_member(members[i]);
            let entry = traits.entry(root(&mut family, i)).or_default();
            entry.0 |= has_check;
            entry.1 |= in_publication;
            entry.2 |= parent_outside_dump[i];
        }

        // TO-side children, for `check_flip_would_reach_enforced_copy`.
        let to_children = if at_19 {
            Self::relation_children(&self.to.tables)
        } else {
            Vec::new()
        };

        let mut caps = HashMap::with_capacity(self.from.tables.len());
        for (i, table) in self.from.tables.iter().enumerate() {
            let (has_check, in_publication, parent_outside) = traits
                .get(&root(&mut family, i))
                .copied()
                .unwrap_or_default();
            let virtual_set_expression =
                !in_publication && (at_19 || (at_18 && !has_check && !parent_outside));
            let check_enforceability = at_19
                && !self.check_flip_would_reach_enforced_copy(table, &to_by_key, &to_children);
            caps.insert(
                (table.schema.clone(), table.name.clone()),
                TableAlterCaps {
                    constraint: ConstraintAlterCaps {
                        check_enforceability,
                    },
                    generated: GeneratedAlterCaps {
                        stored_set_expression: true,
                        virtual_set_expression,
                    },
                },
            );
        }
        caps
    }

    /// Whether an in-place `ALTER CONSTRAINT ... NOT ENFORCED` on one of
    /// `table`'s CHECK constraints would flip a copy that has to stay
    /// enforced.
    ///
    /// The statement cannot say `ONLY` (PostgreSQL rejects it on a table with
    /// children), so it recurses to every inheritance child and partition.
    /// PostgreSQL 19 lets a child's copy stay enforced under a `NOT ENFORCED`
    /// parent, and a child whose copy is the same in FROM and TO gets no
    /// statement of its own to put it back — so round 2 would find it flipped.
    /// Such a table keeps the drop + re-add: the re-added `NOT ENFORCED`
    /// constraint merges into the child's surviving local copy and leaves it
    /// enforced (verified live on PostgreSQL 19). Descendants are taken from
    /// the TO dump — a TO-only child is created before the alter loop, with
    /// its copy. Only their local (merged) copies count here: a copy a
    /// descendant only inherits is dropped by the drop + re-add as well, so
    /// the in-place flip loses nothing more, and `inherited_check_enforcement`
    /// puts such a copy that TO keeps enforced back after every table alter
    /// (see [`Table::enforced_inherited_checks`]).
    fn check_flip_would_reach_enforced_copy(
        &self,
        table: &Table,
        to_by_key: &HashMap<(&str, &str), &Table>,
        to_children: &[Vec<usize>],
    ) -> bool {
        let Some(to_table) = to_by_key.get(&(table.schema.as_str(), table.name.as_str())) else {
            return false;
        };
        let relaxed: Vec<&str> = to_table
            .constraints
            .iter()
            .filter(|to_c| to_c.constraint_type.eq_ignore_ascii_case("check") && !to_c.is_enforced)
            .filter(|to_c| {
                table
                    .constraints
                    .iter()
                    .any(|from_c| from_c.name == to_c.name && from_c.is_enforced)
            })
            .map(|to_c| to_c.name.as_str())
            .collect();
        if relaxed.is_empty() {
            return false;
        }
        let Some(start) = self
            .to
            .tables
            .iter()
            .position(|t| std::ptr::eq(t, *to_table))
        else {
            return false;
        };

        let mut seen = vec![false; self.to.tables.len()];
        seen[start] = true;
        let mut queue: VecDeque<usize> = to_children[start].iter().copied().collect();
        while let Some(i) = queue.pop_front() {
            if std::mem::replace(&mut seen[i], true) {
                continue;
            }
            let descendant = &self.to.tables[i];
            if descendant.constraints.iter().any(|c| {
                c.is_enforced
                    && c.constraint_type.eq_ignore_ascii_case("check")
                    && relaxed.contains(&c.name.as_str())
            }) {
                return true;
            }
            queue.extend(to_children[i].iter().copied());
        }
        false
    }

    /// For each of `tables`, the positions of the tables whose
    /// `partition_of` / `inherits_from` names it: its partitions and
    /// inheritance children within the same dump.
    fn relation_children(tables: &[Table]) -> Vec<Vec<usize>> {
        let index = Self::relation_index(tables.iter());
        let mut children: Vec<Vec<usize>> = vec![Vec::new(); tables.len()];
        for (i, t) in tables.iter().enumerate() {
            for reference in t.partition_of.iter().chain(t.inherits_from.iter()) {
                if let Some(parent) = Self::lookup_relation(&index, reference)
                    && parent != i
                {
                    children[parent].push(i);
                }
            }
        }
        children
    }

    /// Lay out the generated-column drop + adds that classical-inheritance
    /// (`INHERITS`) families share, keyed by FROM table `(schema, name)` (see
    /// [`InheritedRecreates`] for why they must be shared).
    ///
    /// For every column a FROM table re-creates (drop + add, per `alter_caps`)
    /// while none of its `INHERITS` parents does, that table is the root. Its
    /// descendants that re-create the same column too, and whose TO column is
    /// exactly what the root's recursing `ADD COLUMN` gives them
    /// (`TableColumn::is_recreated_by_parent_add`), are its followers: the
    /// root drops their copy between its own drop and add, and they skip the
    /// pair. A follower needs every parent that has the column to be the root
    /// or a follower, or the column would still be inherited from elsewhere
    /// when it is dropped. Any other descendant keeps its own statements.
    ///
    /// Empty with `use_drop=false`, where a drop + add is only commented out.
    pub(crate) fn inherited_recreates(
        &self,
        alter_caps: &HashMap<(String, String), TableAlterCaps>,
    ) -> HashMap<(String, String), InheritedRecreates> {
        let mut plans: HashMap<(String, String), InheritedRecreates> = HashMap::new();
        let from = &self.from.tables;
        if !self.use_drop || from.iter().all(|t| t.inherits_from.is_empty()) {
            return plans;
        }
        let to_by_key: HashMap<(&str, &str), &Table> = self
            .to
            .tables
            .iter()
            .map(|t| ((t.schema.as_str(), t.name.as_str()), t))
            .collect();
        let n = from.len();
        let index = Self::relation_index(from.iter());
        let mut parents: Vec<Vec<usize>> = vec![Vec::new(); n];
        let mut children: Vec<Vec<usize>> = vec![Vec::new(); n];
        for (i, table) in from.iter().enumerate() {
            for reference in &table.inherits_from {
                if let Some(j) = Self::lookup_relation(&index, reference)
                    && j != i
                {
                    parents[i].push(j);
                    children[j].push(i);
                }
            }
        }

        // The TO column of every column each FROM table's alter drops and
        // re-adds. Partitions and wholesale-recreated tables never do.
        let recreated: Vec<HashMap<&str, &TableColumn>> = from
            .iter()
            .map(|table| {
                let Some(to_table) = to_by_key.get(&(table.schema.as_str(), table.name.as_str()))
                else {
                    return HashMap::new();
                };
                if table.partition_of.is_some() || table.will_be_dropped_and_recreated(to_table) {
                    return HashMap::new();
                }
                let caps = Self::caps_for(alter_caps, table).generated;
                let for_retype = table.columns_recreated_for_retype(to_table, self.use_drop, caps);
                to_table
                    .columns
                    .iter()
                    .filter(|to_col| {
                        table
                            .columns
                            .iter()
                            .find(|c| c.name == to_col.name)
                            .is_some_and(|from_col| {
                                to_col.would_drop_and_re_add_with_caps(from_col, caps)
                                    || for_retype.contains(&to_col.name)
                            })
                    })
                    .map(|to_col| (to_col.name.as_str(), to_col))
                    .collect()
            })
            .collect();
        let has_column = |i: usize, column: &str| from[i].columns.iter().any(|c| c.name == column);

        for root in 0..n {
            let mut columns: Vec<(&str, &TableColumn)> =
                recreated[root].iter().map(|(c, col)| (*c, *col)).collect();
            columns.sort_by_key(|(c, _)| *c);
            for (column, root_column) in columns {
                if parents[root]
                    .iter()
                    .any(|&p| recreated[p].contains_key(column))
                {
                    continue; // An ancestor is the root.
                }
                // Descendants holding the column, then decided parents-first.
                let mut subtree: Vec<usize> = Vec::new();
                let mut seen = vec![false; n];
                seen[root] = true;
                let mut queue: VecDeque<usize> = children[root].iter().copied().collect();
                while let Some(d) = queue.pop_front() {
                    if std::mem::replace(&mut seen[d], true) || !has_column(d, column) {
                        continue;
                    }
                    subtree.push(d);
                    queue.extend(children[d].iter().copied());
                }
                subtree.sort_unstable();
                let mut handled: HashSet<usize> = HashSet::from([root]);
                let mut rejected: HashSet<usize> = HashSet::new();
                let mut followers: Vec<usize> = Vec::new();
                loop {
                    let mut progressed = false;
                    for &d in &subtree {
                        if handled.contains(&d) || rejected.contains(&d) {
                            continue;
                        }
                        let holders: Vec<usize> = parents[d]
                            .iter()
                            .copied()
                            .filter(|&p| has_column(p, column))
                            .collect();
                        if holders.iter().any(|p| {
                            !handled.contains(p) && !rejected.contains(p) && subtree.contains(p)
                        }) {
                            continue; // A parent is still undecided.
                        }
                        progressed = true;
                        let follows = holders.iter().all(|p| handled.contains(p))
                            && recreated[d].get(column).is_some_and(|to_col| {
                                to_col.is_recreated_by_parent_add(root_column)
                            });
                        if follows {
                            handled.insert(d);
                            followers.push(d);
                        } else {
                            rejected.insert(d);
                        }
                    }
                    if !progressed {
                        break;
                    }
                }
                if followers.is_empty() {
                    continue;
                }
                let root_key = (from[root].schema.clone(), from[root].name.clone());
                plans.entry(root_key).or_default().descendant_drops.insert(
                    column.to_string(),
                    followers
                        .iter()
                        .map(|&d| format!("{}.{}", from[d].schema, from[d].name))
                        .collect(),
                );
                for &d in &followers {
                    plans
                        .entry((from[d].schema.clone(), from[d].name.clone()))
                        .or_default()
                        .recreated_by_ancestor
                        .insert(column.to_string());
                }
            }
        }
        plans
    }

    /// The FROM tables, as `(schema, name)`, whose rows an ancestor's alter
    /// rewrites before their own alter runs (see
    /// [`InheritedRecreates::rewritten_by_ancestor`]):
    ///
    /// * a partition in both dumps with a column that
    ///   [`TableColumn::rewrites_in_place`] changes — a partition leaves its
    ///   column changes to its parent, whose statement recurses;
    /// * a table with a column that [`TableColumn::alters_type_in_place`]
    ///   changes and one of its FROM `INHERITS` parents has too — the parent's
    ///   `ALTER COLUMN ... TYPE` recurses (its `SET EXPRESSION` says `ONLY`).
    ///
    /// Only a table whose hash changes, as nothing else has a CHECK to relax
    /// or drop, and never one the migration creates anew: its own DROP TABLE
    /// runs, or a partition ancestor's does, by the test `compare_tables`
    /// applies before it records a table in `dropped_and_recreated_tables`.
    fn tables_rewritten_by_ancestor(
        &self,
        alter_caps: &HashMap<(String, String), TableAlterCaps>,
    ) -> HashSet<(String, String)> {
        let from = &self.from.tables;
        let mut rewritten = HashSet::new();
        if from
            .iter()
            .all(|t| t.partition_of.is_none() && t.inherits_from.is_empty())
        {
            return rewritten;
        }
        let to_by_key: HashMap<(&str, &str), &Table> = self
            .to
            .tables
            .iter()
            .map(|t| ((t.schema.as_str(), t.name.as_str()), t))
            .collect();
        let index = Self::relation_index(from.iter());
        let drops = Self::alter_drops_table;

        for table in from {
            let Some(&to_table) = to_by_key.get(&(table.schema.as_str(), table.name.as_str()))
            else {
                continue;
            };
            if table.hash.is_none()
                || to_table.hash.is_none()
                || !Self::hashes_differ(&table.hash, &to_table.hash)
                || drops(table, to_table)
            {
                continue;
            }
            let caps = Self::caps_for(alter_caps, table).generated;
            let mut columns = to_table.columns.iter().filter_map(|to_col| {
                table
                    .columns
                    .iter()
                    .find(|c| c.name == to_col.name)
                    .map(|from_col| (from_col, to_col))
            });
            let by_ancestor = if table.partition_of.is_some() && to_table.partition_of.is_some() {
                columns.any(|(from_col, to_col)| {
                    to_col.rewrites_in_place(from_col, caps, self.use_drop)
                })
            } else {
                let parents: Vec<&Table> = table
                    .inherits_from
                    .iter()
                    .filter_map(|reference| Self::lookup_relation(&index, reference))
                    .map(|i| &from[i])
                    .collect();
                columns.any(|(from_col, to_col)| {
                    to_col.alters_type_in_place(from_col, caps, self.use_drop)
                        && parents
                            .iter()
                            .any(|p| p.columns.iter().any(|c| c.name == to_col.name))
                })
            };
            if !by_ancestor {
                continue;
            }

            // Walk up the partition ancestors: the table goes with any of
            // them whose DROP TABLE runs (or that TO does not have).
            let mut ancestor = table.partition_of.as_deref();
            let mut created_anew = false;
            for _ in 0..from.len() {
                let Some(i) = ancestor.and_then(|r| Self::lookup_relation(&index, r)) else {
                    break;
                };
                let parent = &from[i];
                match to_by_key.get(&(parent.schema.as_str(), parent.name.as_str())) {
                    Some(to_parent) if !drops(parent, to_parent) => {}
                    _ => {
                        created_anew = true;
                        break;
                    }
                }
                ancestor = parent.partition_of.as_deref();
            }
            if !created_anew {
                rewritten.insert((table.schema.clone(), table.name.clone()));
            }
        }
        rewritten
    }

    /// Whether the table alter of `from` into `to` drops the table and
    /// creates it again from TO, as `Table::build_alter_script` does for a
    /// partition-key change and for a column type change it cannot make in
    /// place (see [`Table::will_be_dropped_and_recreated`]) — except on a
    /// partition in both dumps, whose column changes are its parent's
    /// statement to make.
    fn alter_drops_table(from: &Table, to: &Table) -> bool {
        from.partition_key != to.partition_key
            || (from.will_be_dropped_and_recreated(to)
                && !(from.partition_of.is_some() && to.partition_of.is_some()))
    }

    /// The FROM tables the table phase drops with `DROP TABLE` and creates
    /// again from TO (see `RecreatedFamilies`), worked out before any of it
    /// runs, so every pass that has to prepare for those drops — the views,
    /// SQL-body routines and rules in the way (`relation_changes`), the
    /// foreign keys referencing them (`plan_recreate_fk_predrops`), the
    /// sequences their columns own, and the partitions created again with
    /// them — sees the same set.
    ///
    /// A root is a table in both dumps whose own alter drops it
    /// (`alter_drops_table`), and whose hash both dumps have and differs,
    /// the test `compare_tables` applies before it alters a table. Its
    /// members are its FROM partitions at every depth: PostgreSQL's `DROP
    /// TABLE` of a partitioned table takes all of them along. A partition
    /// whose own alter would drop it too is a member of the topmost such
    /// ancestor, since that ancestor's `DROP TABLE` runs first. The FROM
    /// partition ancestors of a root that is a partition stay, and are
    /// recorded for the foreign keys referencing them.
    ///
    /// Only tables are members. The dump records no partition membership for
    /// a foreign table, so a foreign-table partition of a root goes with its
    /// `DROP TABLE` unseen, and only a second run creates it again — as the
    /// standalone foreign table pgc knows it as.
    fn recreated_families(&self) -> RecreatedFamilies {
        let from = &self.from.tables;
        let to_by_key: HashMap<(&str, &str), &Table> = self
            .to
            .tables
            .iter()
            .map(|t| ((t.schema.as_str(), t.name.as_str()), t))
            .collect();
        let drops: Vec<bool> = from
            .iter()
            .map(|table| {
                to_by_key
                    .get(&(table.schema.as_str(), table.name.as_str()))
                    .is_some_and(|to_table| {
                        table.hash.is_some()
                            && to_table.hash.is_some()
                            && Self::hashes_differ(&table.hash, &to_table.hash)
                            && Self::alter_drops_table(table, to_table)
                    })
            })
            .collect();
        let mut families = RecreatedFamilies::default();
        if !drops.contains(&true) {
            return families;
        }
        let index = Self::relation_index(from.iter());
        for (i, table) in from.iter().enumerate() {
            // The topmost FROM partition ancestor whose alter drops it.
            let mut topmost = None;
            let mut ancestor = table
                .partition_of
                .as_deref()
                .and_then(|r| Self::lookup_relation(&index, r));
            for _ in 0..from.len() {
                let Some(parent) = ancestor else {
                    break;
                };
                if drops[parent] {
                    topmost = Some(parent);
                }
                ancestor = from[parent]
                    .partition_of
                    .as_deref()
                    .and_then(|r| Self::lookup_relation(&index, r));
            }
            let key = Self::table_key(&table.schema, &table.name);
            match topmost {
                Some(root) => {
                    families
                        .members
                        .insert(key, Self::table_key(&from[root].schema, &from[root].name));
                }
                None if drops[i] => {
                    // Its FROM partition ancestors stay; PostgreSQL copies a
                    // foreign key referencing one onto every partition.
                    let mut ancestor = table
                        .partition_of
                        .as_deref()
                        .and_then(|r| Self::lookup_relation(&index, r));
                    for _ in 0..from.len() {
                        let Some(parent) = ancestor else {
                            break;
                        };
                        families
                            .root_ancestors
                            .entry(Self::table_key(&from[parent].schema, &from[parent].name))
                            .or_default()
                            .insert(key.clone());
                        ancestor = from[parent]
                            .partition_of
                            .as_deref()
                            .and_then(|r| Self::lookup_relation(&index, r));
                    }
                    families.roots.insert(key);
                }
                None => {}
            }
        }
        families
    }

    /// The `table_key` of the table the TO table `table` is a partition of,
    /// resolved against `to_index` (a `relation_index` of `to_tables`), so a
    /// quoted name matches as well as a plain one; the normalised reference
    /// itself when TO has no such table. `None` for a table that is no
    /// partition.
    fn to_parent_key(
        to_tables: &[Table],
        to_index: &HashMap<String, usize>,
        table: &Table,
    ) -> Option<String> {
        let reference = table.partition_of.as_deref()?;
        Some(match Self::lookup_relation(to_index, reference) {
            Some(i) => Self::table_key(&to_tables[i].schema, &to_tables[i].name),
            None => Self::normalise_partition_of(reference),
        })
    }

    /// Every TO parent of `table`: its `partition_of` parent (see
    /// `Comparer::to_parent_key`) and its `INHERITS` parents, as table keys.
    fn to_parent_keys(
        to_tables: &[Table],
        to_index: &HashMap<String, usize>,
        table: &Table,
    ) -> Vec<String> {
        let mut keys: Vec<String> = Self::to_parent_key(to_tables, to_index, table)
            .into_iter()
            .collect();
        keys.extend(table.inherits_from.iter().filter_map(|reference| {
            Self::lookup_relation(to_index, reference)
                .map(|i| Self::table_key(&to_tables[i].schema, &to_tables[i].name))
        }));
        keys
    }

    /// The tables in both dumps whose step in the table alter loop of
    /// `compare_tables` has to wait until the tables it creates or creates
    /// again are all there: the loop takes these last, in TO order, with the
    /// new partitions deferred under a recreated parent (`deferred_new`).
    ///
    /// Taken in FROM order (`ordered_from`), a table that TO puts under a
    /// recreated table (`recreated`: the roots and members of `families`)
    /// would otherwise be attached to it — or, for a root or a member, created
    /// under it — before the recreate's `DROP TABLE` runs, whenever it comes
    /// first: a standalone table named before the partitioned table it joins,
    /// or a partition of another tree moving into a recreated sub-partition
    /// root deeper down. The `DROP TABLE` then takes it along, with its rows,
    /// and no statement fails. So a table waits when its TO parent is a
    /// recreated table not yet created again at its turn, a deferred new
    /// partition, or a table that waits itself; and a member waits when its
    /// root does, as the root's `DROP TABLE` has to run before it is
    /// created.
    ///
    /// Only tables with a step at all count (a root, a member, or a table
    /// whose hash differs, both hashes known). Empty without `use_drop`: the
    /// `DROP TABLE` is commented out and nothing goes with it.
    fn late_table_keys(
        &self,
        ordered_from: &[&Table],
        to_table_map: &HashMap<(&str, &str), usize>,
        to_index: &HashMap<String, usize>,
        families: &RecreatedFamilies,
        recreated: &HashSet<String>,
        deferred_new: &HashSet<String>,
    ) -> HashSet<String> {
        let mut late: HashSet<String> = HashSet::new();
        if !self.use_drop || recreated.is_empty() {
            return late;
        }
        let to_tables = &self.to.tables;
        // Recreated tables whose step has run by the current one.
        let mut back: HashSet<String> = HashSet::new();
        for table in ordered_from {
            let Some(&tidx) = to_table_map.get(&(table.schema.as_str(), table.name.as_str()))
            else {
                continue;
            };
            let to_table = &to_tables[tidx];
            if table.hash.is_none() || to_table.hash.is_none() {
                continue;
            }
            let key = Self::table_key(&table.schema, &table.name);
            let is_recreated = recreated.contains(&key);
            if !is_recreated && !Self::hashes_differ(&table.hash, &to_table.hash) {
                continue;
            }
            let parent_waits =
                Self::to_parent_key(to_tables, to_index, to_table).is_some_and(|parent| {
                    (recreated.contains(&parent) && !back.contains(&parent))
                        || deferred_new.contains(&parent)
                        || late.contains(&parent)
                });
            let root_waits = families
                .members
                .get(&key)
                .is_some_and(|root| late.contains(root));
            if parent_waits || root_waits {
                late.insert(key);
            } else if is_recreated {
                back.insert(key);
            }
        }
        late
    }

    /// The FROM foreign keys `compare_tables` drops ahead of the `DROP TABLE`
    /// of the tables TO does not have, in the order it drops them: for each
    /// such table, in drop order, every foreign key of another FROM table
    /// that references it or one of its FROM partition ancestors. `DROP TABLE`
    /// without `CASCADE` refuses to drop a referenced table, and PostgreSQL
    /// copies a foreign key referencing a partitioned table onto every one of
    /// its partitions, so dropping a partition is refused too.
    ///
    /// The referenced table is read from the definition
    /// (`fk_referenced_candidates`), so only a foreign key that references
    /// the dropped table counts — matching the definition text instead took
    /// every foreign key merely naming the table, `s.account` in `references
    /// s.accounts(id)` or `log` in `catalog_id`, and dropped it for good. An
    /// unqualified reference counts every table it may name.
    ///
    /// With `use_drop`, `enter_table_phase_view` leaves them out of the FROM
    /// tables the table phase works on: the referencing table's alter does not
    /// drop them a second time, and `compare_foreign_keys` adds back the TO
    /// definition of each one TO keeps — a foreign key to a dropped partition's
    /// ancestor, which stays, among them.
    fn plan_dropped_table_fk_predrops(&self) -> Vec<TableConstraint> {
        let to_keys: HashSet<(&str, &str)> = self
            .to
            .tables
            .iter()
            .map(|t| (t.schema.as_str(), t.name.as_str()))
            .collect();
        let dropped: Vec<&Table> = Self::ordered_tables_for_drop(&self.from.tables)
            .into_iter()
            .filter(|t| !to_keys.contains(&(t.schema.as_str(), t.name.as_str())))
            .collect();
        if dropped.is_empty() {
            return Vec::new();
        }
        let from_index = Self::relation_index(self.from.tables.iter());
        let from_by_unquoted = FromTablesUnquoted::new(&self.from.tables);
        // Every FROM foreign key, with the tables it may reference.
        let foreign_keys: Vec<(&Table, &TableConstraint, Vec<&Table>)> = self
            .from
            .tables
            .iter()
            .flat_map(|owner| {
                let from_by_unquoted = &from_by_unquoted;
                owner.constraints.iter().filter_map(move |c| {
                    if !c.constraint_type.eq_ignore_ascii_case("foreign key") {
                        return None;
                    }
                    let definition = c.definition.as_deref()?;
                    let referenced =
                        Self::fk_referenced_candidates(from_by_unquoted, definition, &owner.schema);
                    (!referenced.is_empty()).then_some((owner, c, referenced))
                })
            })
            .collect();

        let mut predrops = Vec::new();
        let mut seen: HashSet<(&str, &str, &str)> = HashSet::new();
        for table in dropped {
            // The table and its FROM partition ancestors, nearest first.
            let mut targets: Vec<&Table> = vec![table];
            targets.extend(Self::partition_ancestors(
                &self.from.tables,
                &from_index,
                table,
            ));
            for (owner, constraint, referenced) in &foreign_keys {
                if std::ptr::eq(*owner, table) {
                    continue;
                }
                let hits = referenced
                    .iter()
                    .any(|r| targets.iter().any(|t| std::ptr::eq(*r, *t)));
                if hits
                    && seen.insert((
                        constraint.schema.as_str(),
                        constraint.table_name.as_str(),
                        constraint.name.as_str(),
                    ))
                {
                    predrops.push((*constraint).clone());
                }
            }
        }
        predrops
    }

    /// The FROM foreign keys the table phase drops ahead of every table
    /// alter, because they reference a table that a recreate drops (see
    /// `recreated_families`) from a table that stays, or that another
    /// recreate drops: `DROP TABLE` without `CASCADE` refuses to drop a
    /// referenced table. A foreign key referencing a FROM partition ancestor
    /// of a root that is a partition counts too: PostgreSQL copies it onto
    /// every partition of the referenced table, the root among them. A
    /// foreign key between two tables of one family goes with that family's
    /// `DROP TABLE`; one on a FROM-only table goes with that table, which is
    /// dropped before any alter; one referencing a FROM-only table is dropped
    /// anyway (pass 1 of `compare_tables`).
    ///
    /// The referenced table is read from the definition
    /// (`fk_referenced_candidates`), which names it unqualified when the
    /// dump's `search_path` reached it; every table it may then be counts.
    ///
    /// `enter_table_phase_view` leaves these out of the FROM tables the table
    /// phase works on, so the referencing table's alter neither drops nor
    /// alters them a second time, and `compare_foreign_keys` adds the TO
    /// definition back as a new foreign key once the tables are in place —
    /// `NOT VALID`, then validated after commit, under
    /// `--output-for-production`. The referenced table comes back empty, so
    /// rows still pointing at its lost rows make that add (or its
    /// validation) fail, as they would for any foreign key to an empty table.
    ///
    /// Planned only with `use_drop`: without it the `DROP TABLE` is
    /// commented out and the foreign keys can stay.
    fn plan_recreate_fk_predrops(&self, families: &RecreatedFamilies) -> Vec<TableConstraint> {
        if families.is_empty() {
            return Vec::new();
        }
        let to_by_key: HashMap<(&str, &str), &Table> = self
            .to
            .tables
            .iter()
            .map(|t| ((t.schema.as_str(), t.name.as_str()), t))
            .collect();
        let from_by_unquoted = FromTablesUnquoted::new(&self.from.tables);
        let mut predrops = Vec::new();
        for owner in &self.from.tables {
            // A table without a hash is skipped by the table phase, and
            // `compare_foreign_keys` would not add its foreign keys back.
            let kept = owner.hash.is_some()
                && to_by_key
                    .get(&(owner.schema.as_str(), owner.name.as_str()))
                    .is_some_and(|to| to.hash.is_some());
            if !kept {
                continue;
            }
            let owner_root = families.root_of(&Self::table_key(&owner.schema, &owner.name));
            for constraint in &owner.constraints {
                if !constraint
                    .constraint_type
                    .eq_ignore_ascii_case("foreign key")
                {
                    continue;
                }
                let Some(definition) = constraint.definition.as_deref() else {
                    continue;
                };
                let blocks_a_drop =
                    Self::fk_referenced_candidates(&from_by_unquoted, definition, &owner.schema)
                        .into_iter()
                        .filter(|referenced| {
                            to_by_key.contains_key(&(
                                referenced.schema.as_str(),
                                referenced.name.as_str(),
                            ))
                        })
                        .any(|referenced| {
                            families
                                .roots_blocked_by_reference_to(&Self::table_key(
                                    &referenced.schema,
                                    &referenced.name,
                                ))
                                .any(|root| owner_root != Some(root))
                        });
                if blocks_a_drop {
                    predrops.push(constraint.clone());
                }
            }
        }
        predrops
    }

    /// The FROM tables the foreign key `definition`, on a table of
    /// `owner_schema`, may reference. `pg_get_constraintdef` qualifies the
    /// referenced table unless the dump session's `search_path` reaches it,
    /// and that path is not recorded: an unqualified name is looked up in
    /// the owner's schema and in `public` (the default path), and when
    /// neither has it, in every schema — each table found counts. Empty when
    /// the definition names no FROM table.
    fn fk_referenced_candidates<'a>(
        from: &FromTablesUnquoted<'a>,
        definition: &str,
        owner_schema: &str,
    ) -> Vec<&'a Table> {
        // With no owner schema to fall back on, an unqualified target comes
        // back with an empty schema.
        let Some((schema, name)) = Self::parse_fk_referenced_table(definition, "") else {
            return Vec::new();
        };
        if !schema.is_empty() {
            return from
                .by_key
                .get(&(schema, name))
                .copied()
                .into_iter()
                .collect();
        }
        let owner = owner_schema.trim_matches('"');
        let mut candidates: Vec<&Table> = [owner, "public"]
            .into_iter()
            .filter_map(|schema| {
                from.by_key
                    .get(&(schema.to_string(), name.clone()))
                    .copied()
            })
            .collect();
        candidates.dedup_by(|a, b| std::ptr::eq(*a, *b));
        if candidates.is_empty() {
            candidates = from.by_name.get(&name).cloned().unwrap_or_default();
        }
        candidates
    }

    /// The sequences a recreate would take along with the tables of the
    /// family rooted at `root` (see `recreated_families`), as `(FROM, TO)`
    /// pairs: the FROM sequences TO keeps whose `OWNED BY` column sits on the
    /// root or on one of its members TO keeps. `DROP TABLE` drops an owned
    /// sequence with its table, and the table's `CREATE` then names it in a
    /// `nextval` default that no longer resolves.
    ///
    /// `compare_tables` detaches each (`OWNED BY NONE`) right before the
    /// root's `DROP TABLE`, which keeps the sequence with its current value,
    /// owner, privileges and comment, and `sequence_post_script` gives it the
    /// TO `OWNED BY` once the tables are back. Identity sequences are left
    /// out: PostgreSQL does not let them be detached, and the identity column
    /// of the new table creates its own.
    fn sequences_owned_in_family<'a>(
        &'a self,
        families: &RecreatedFamilies,
        root: &str,
    ) -> Vec<(&'a Sequence, &'a Sequence)> {
        let to_tables: HashSet<String> = self
            .to
            .tables
            .iter()
            .map(|t| Self::table_key(&t.schema, &t.name))
            .collect();
        let to_sequences: HashMap<(&str, &str), &Sequence> = self
            .to
            .sequences
            .iter()
            .map(|s| ((s.schema.as_str(), s.name.as_str()), s))
            .collect();
        self.from
            .sequences
            .iter()
            .filter(|s| !s.is_identity)
            .filter_map(|s| {
                let (Some(schema), Some(table)) =
                    (s.owned_by_schema.as_deref(), s.owned_by_table.as_deref())
                else {
                    return None;
                };
                let owner = Self::table_key(schema, table);
                if families.root_of(&owner) != Some(root) || !to_tables.contains(&owner) {
                    return None;
                }
                to_sequences
                    .get(&(s.schema.as_str(), s.name.as_str()))
                    .map(|&to| (s, to))
            })
            .collect()
    }

    /// The statements that carry the sequences of `sequences_owned_in_family`
    /// across the `DROP TABLE` of `root`: `ALTER SEQUENCE ... OWNED BY NONE`
    /// for each, to run right before it, and the TO `OWNED BY` of each that
    /// has one, to run once every table is back. Only for `use_drop`: without
    /// it the `DROP TABLE` is commented out and the sequences stay owned.
    fn owned_sequence_handoff(&self, families: &RecreatedFamilies, root: &str) -> (String, String) {
        let mut detach = String::new();
        let mut reattach = String::new();
        for (from_sequence, to_sequence) in self.sequences_owned_in_family(families, root) {
            detach.push_str(
                &format!(
                    "alter sequence {}.{} owned by none;",
                    from_sequence.schema, from_sequence.name
                )
                .with_empty_lines(),
            );
            if let Some(owned_by) = to_sequence.get_owned_by_script() {
                reattach.push_str(
                    format!(
                        "/* Sequence: {}.{}*/\n",
                        block_comment_safe(&to_sequence.schema),
                        block_comment_safe(&to_sequence.name)
                    )
                    .as_str(),
                );
                reattach.push_str(&owned_by);
            }
        }
        (detach, reattach)
    }

    /// The CHECK constraints each FROM table's inheritance family drops and
    /// re-adds together, keyed by FROM table `(schema, name)` (see
    /// [`InheritedRecreates::family_checks`]).
    ///
    /// It starts from a merged copy (local, and inherited — `coninhcount >
    /// 0`) that a table's alter drops, because TO no longer has it locally
    /// or changes it in a way no in-place `ALTER CONSTRAINT` can make (the
    /// enforceability of a CHECK before PostgreSQL 19, or with a FROM dump of
    /// unknown version; its expression on any version). PostgreSQL refuses
    /// that drop while the copy is still inherited, so every `INHERITS`
    /// ancestor holding a local copy of that name joins in, up the whole
    /// chain — including one whose copy does not change, which then drops
    /// and re-adds it as it is. A `NO INHERIT` copy passes nothing down and
    /// stays out. When no such ancestor keeps the constraint in TO, each one
    /// drops it for good in its own alter, which runs first, and the table
    /// order alone works: the family is left as it is. A parent the table
    /// leaves (`NO INHERIT`, see [`Table::inheritance_moves`]) does not count
    /// either: the table's alter says `NO INHERIT` before its drops, and the
    /// copy is a plain local constraint by then.
    ///
    /// Empty with `use_drop=false`, where a drop is only commented out.
    pub(crate) fn inherited_check_recreates(
        &self,
        alter_caps: &HashMap<(String, String), TableAlterCaps>,
    ) -> HashMap<(String, String), HashSet<String>> {
        let mut plans: HashMap<(String, String), HashSet<String>> = HashMap::new();
        let from = &self.from.tables;
        if !self.use_drop || from.iter().all(|t| t.inherits_from.is_empty()) {
            return plans;
        }
        let to_by_key: HashMap<(&str, &str), &Table> = self
            .to
            .tables
            .iter()
            .map(|t| ((t.schema.as_str(), t.name.as_str()), t))
            .collect();
        let index = Self::relation_index(from.iter());
        let parents: Vec<Vec<usize>> = from
            .iter()
            .enumerate()
            .map(|(i, table)| {
                table
                    .inherits_from
                    .iter()
                    .filter_map(|reference| Self::lookup_relation(&index, reference))
                    .filter(|&j| j != i)
                    .collect()
            })
            .collect();

        for (i, table) in from.iter().enumerate() {
            if parents[i].is_empty() {
                continue;
            }
            let Some(&to_table) = to_by_key.get(&(table.schema.as_str(), table.name.as_str()))
            else {
                continue;
            };
            if !Self::hashes_differ(&table.hash, &to_table.hash) {
                continue;
            }
            let caps = Self::caps_for(alter_caps, table);
            // A parent the table leaves (`NO INHERIT`, which runs before its
            // drops) passes nothing down by then: the copy it gave is a
            // plain local constraint, which the alter drops on its own.
            let (left, _) = table.inheritance_moves(to_table);
            let kept_parents: Vec<usize> = table.inherits_from
                [..table.inherits_from.len() - left.len()]
                .iter()
                .filter_map(|reference| Self::lookup_relation(&index, reference))
                .filter(|&j| j != i)
                .collect();
            for check in table.dropped_checks(to_table, self.use_drop, caps) {
                if check.coninhcount == 0 {
                    continue;
                }
                let holds = |t: &Table| {
                    t.constraints.iter().any(|c| {
                        c.name == check.name
                            && c.constraint_type.eq_ignore_ascii_case("check")
                            && !c.no_inherit
                    })
                };
                let mut ancestors: Vec<(&Table, &Table)> = Vec::new();
                let mut seen = vec![false; from.len()];
                seen[i] = true;
                let mut queue: VecDeque<usize> = kept_parents.iter().copied().collect();
                while let Some(a) = queue.pop_front() {
                    if std::mem::replace(&mut seen[a], true) {
                        continue;
                    }
                    let ancestor = &from[a];
                    if holds(ancestor)
                        && let Some(&to_ancestor) =
                            to_by_key.get(&(ancestor.schema.as_str(), ancestor.name.as_str()))
                    {
                        ancestors.push((ancestor, to_ancestor));
                    }
                    queue.extend(parents[a].iter().copied());
                }
                // Every ancestor dropping its copy for good drops it before
                // this table's alter runs (parents are altered first), so
                // the copy is no longer inherited then: nothing to share.
                if !ancestors.iter().any(|(_, to_ancestor)| {
                    to_ancestor.constraints.iter().any(|c| c.name == check.name)
                }) {
                    continue;
                }
                plans
                    .entry((table.schema.clone(), table.name.clone()))
                    .or_default()
                    .insert(check.name.clone());
                for (ancestor, _) in ancestors {
                    plans
                        .entry((ancestor.schema.clone(), ancestor.name.clone()))
                        .or_default()
                        .insert(check.name.clone());
                }
            }
        }
        plans
    }

    /// The inherited CHECK constraints an `ALTER TABLE ONLY ... SET
    /// EXPRESSION` would corrupt, and how the migration keeps them out of
    /// its way (see [`SetExpressionHazards`]).
    ///
    /// PostgreSQL 17+ rebuilds the CHECK constraints that read the column,
    /// and gets inheritance wrong when the statement says `ONLY` (verified
    /// live on 17, 18 and 19rc1, STORED and VIRTUAL alike). On a child it
    /// drops a copy the table only inherits and never adds it back, and it
    /// turns a merged copy into a plain local one; on a parent the re-add
    /// recurses into children whose copies were never dropped, so each one
    /// ends up counting its parent twice (`coninhcount` 2), which no later
    /// statement undoes. A partitioned table's statement recurses (no
    /// `ONLY`) and is not affected.
    ///
    /// So for each FROM table whose alter writes `SET EXPRESSION` with
    /// `ONLY` (a table that is neither partitioned nor a partition, altered
    /// in place, see [`TableColumn::sets_expression_in_place`]), every
    /// inheritable CHECK that reads one of those columns has to be gone when
    /// it runs: its own when it has `INHERITS` children (in either dump: a
    /// TO-only child is created before the alters), and those of every
    /// `INHERITS` ancestor the table keeps, which the table inherits. Each
    /// such CHECK in FROM joins its holder's
    /// [`InheritedRecreates::family_checks`], so it is dropped ahead of the
    /// table alters, parents first; and the add of every CHECK of those
    /// names that TO holds on the table or a kept ancestor waits for every
    /// table alter ([`InheritedRecreates::checks_after_alters`]), as an add
    /// recurses into children whose own `SET EXPRESSION` runs later. A CHECK
    /// that reads none of the columns is left alone: PostgreSQL does not
    /// touch it.
    ///
    /// A holder the migration drops, or drops and re-creates, takes its
    /// copies with it and is left out, as is an ancestor outside the dumps.
    pub(crate) fn set_expression_check_hazards(
        &self,
        alter_caps: &HashMap<(String, String), TableAlterCaps>,
    ) -> SetExpressionHazards {
        let mut hazards = SetExpressionHazards::default();
        let from = &self.from.tables;
        let to = &self.to.tables;
        if from.iter().all(|t| t.inherits_from.is_empty())
            && to.iter().all(|t| t.inherits_from.is_empty())
        {
            return hazards;
        }
        let from_index = Self::relation_index(from.iter());
        let to_index = Self::relation_index(to.iter());
        let from_children = Self::relation_children(from);
        let to_children = Self::relation_children(to);
        let to_of = |table: &Table| {
            Self::lookup_relation(&to_index, &Self::table_key(&table.schema, &table.name))
                .map(|i| &to[i])
        };
        // `INHERITS` ancestors, nearest first, walking only the parents
        // `kept` keeps.
        let ancestors = |tables: &[Table],
                         index: &HashMap<String, usize>,
                         start: usize,
                         kept: &dyn Fn(&str) -> bool|
         -> Vec<usize> {
            let mut seen = vec![false; tables.len()];
            seen[start] = true;
            let mut out = Vec::new();
            let mut queue: VecDeque<usize> = tables[start]
                .inherits_from
                .iter()
                .filter(|r| kept(r))
                .filter_map(|r| Self::lookup_relation(index, r))
                .collect();
            while let Some(a) = queue.pop_front() {
                if std::mem::replace(&mut seen[a], true) {
                    continue;
                }
                out.push(a);
                queue.extend(
                    tables[a]
                        .inherits_from
                        .iter()
                        .filter_map(|r| Self::lookup_relation(index, r)),
                );
            }
            out
        };

        for (i, table) in from.iter().enumerate() {
            let Some(to_table) = to_of(table) else {
                continue;
            };
            if table.hash.is_none()
                || !Self::hashes_differ(&table.hash, &to_table.hash)
                || table.partition_of.is_some()
                || to_table.partition_of.is_some()
                || to_table.partition_key.is_some()
                || Self::alter_drops_table(table, to_table)
            {
                continue;
            }
            let caps = Self::caps_for(alter_caps, table).generated;
            let columns: Vec<&str> = to_table
                .columns
                .iter()
                .filter(|to_col| {
                    table
                        .columns
                        .iter()
                        .find(|c| c.name == to_col.name)
                        .is_some_and(|from_col| to_col.sets_expression_in_place(from_col, caps))
                })
                .map(|c| c.name.as_str())
                .collect();
            if columns.is_empty() {
                continue;
            }
            let reads = |c: &TableConstraint| {
                !c.no_inherit && columns.iter().any(|column| c.check_reads_column(column))
            };
            let mut names: HashSet<String> = HashSet::new();
            let mut drop_ahead = |holder: &Table, name: &str, names: &mut HashSet<String>| {
                hazards
                    .drops
                    .entry((holder.schema.clone(), holder.name.clone()))
                    .or_default()
                    .insert(name.to_string());
                names.insert(name.to_string());
            };

            // Its own CHECKs, which the parent's re-add would double up in
            // its children.
            let to_i = to_index
                .get(&Self::table_key(&to_table.schema, &to_table.name))
                .copied();
            let has_children =
                !from_children[i].is_empty() || to_i.is_some_and(|t| !to_children[t].is_empty());
            if has_children {
                for check in table.constraints.iter().filter(|c| reads(c)) {
                    drop_ahead(table, &check.name, &mut names);
                }
            }

            // The CHECKs it inherits from the ancestors it keeps.
            let kept = |r: &str| to_table.inherits_from.iter().any(|t| t == r);
            for a in ancestors(from, &from_index, i, &kept) {
                let ancestor = &from[a];
                let Some(to_ancestor) = to_of(ancestor) else {
                    continue;
                };
                if ancestor.hash.is_none() || Self::alter_drops_table(ancestor, to_ancestor) {
                    continue;
                }
                // A merged copy here stays, a plain local one once the
                // ancestors' copies are gone, which the statement leaves be.
                for check in ancestor.constraints.iter().filter(|c| reads(c)) {
                    drop_ahead(ancestor, &check.name, &mut names);
                }
            }
            // A CHECK TO's ancestors add, which the add would hand down.
            let mut to_holders: Vec<&Table> = vec![to_table];
            if let Some(to_i) = to_i {
                let kept_from = |r: &str| table.inherits_from.iter().any(|f| f == r);
                to_holders.extend(
                    ancestors(to, &to_index, to_i, &kept_from)
                        .into_iter()
                        .map(|a| &to[a]),
                );
            }
            for holder in &to_holders[1..] {
                for check in holder.constraints.iter().filter(|c| reads(c)) {
                    names.insert(check.name.clone());
                }
            }
            if names.is_empty() {
                continue;
            }
            hazards
                .tables
                .insert((table.schema.clone(), table.name.clone()));
            // Every add of those names on the table and the ancestors waits.
            for holder in to_holders {
                for check in holder.constraints.iter().filter(|c| {
                    c.constraint_type.eq_ignore_ascii_case("check") && names.contains(&c.name)
                }) {
                    hazards
                        .deferred
                        .entry((holder.schema.clone(), holder.name.clone()))
                        .or_default()
                        .insert(check.name.clone());
                }
            }
        }
        hazards
    }

    /// The columns, keyed by FROM table `(schema, name)`, that each table's
    /// alter would retype in place although an `INHERITS` ancestor's own
    /// alter retypes them first, and recurses (see
    /// [`InheritedRecreates::retyped_by_ancestor`]).
    ///
    /// A column of a table that has `INHERITS` parents qualifies when some
    /// ancestor reached through parents holding the column — a table cannot
    /// have a column of a parent's name without inheriting it — retypes it
    /// in its own in-place alter: its hash changes, its `DROP TABLE` does
    /// not run, and [`TableColumn::alters_type_in_place`] holds for it
    /// there. The comparer emits parents first, so that `ALTER COLUMN ...
    /// TYPE` has run, and recursed, by the time this table is altered.
    /// Partitions never qualify: their column changes are always left to
    /// their parent.
    pub(crate) fn inherited_retypes(
        &self,
        alter_caps: &HashMap<(String, String), TableAlterCaps>,
    ) -> HashMap<(String, String), HashSet<String>> {
        let mut plans: HashMap<(String, String), HashSet<String>> = HashMap::new();
        let from = &self.from.tables;
        if from.iter().all(|t| t.inherits_from.is_empty()) {
            return plans;
        }
        let to_by_key: HashMap<(&str, &str), &Table> = self
            .to
            .tables
            .iter()
            .map(|t| ((t.schema.as_str(), t.name.as_str()), t))
            .collect();
        let index = Self::relation_index(from.iter());
        let parents: Vec<Vec<usize>> = from
            .iter()
            .enumerate()
            .map(|(i, table)| {
                table
                    .inherits_from
                    .iter()
                    .filter_map(|reference| Self::lookup_relation(&index, reference))
                    .filter(|&j| j != i)
                    .collect()
            })
            .collect();
        // The columns each table's own alter retypes in place.
        let own: Vec<HashSet<&str>> = from
            .iter()
            .map(|table| {
                let Some(&to_table) = to_by_key.get(&(table.schema.as_str(), table.name.as_str()))
                else {
                    return HashSet::new();
                };
                if table.partition_of.is_some()
                    || table.hash.is_none()
                    || to_table.hash.is_none()
                    || !Self::hashes_differ(&table.hash, &to_table.hash)
                    || table.will_be_dropped_and_recreated(to_table)
                {
                    return HashSet::new();
                }
                let caps = Self::caps_for(alter_caps, table).generated;
                to_table
                    .columns
                    .iter()
                    .filter_map(|to_col| {
                        let from_col = table.columns.iter().find(|c| c.name == to_col.name)?;
                        to_col
                            .alters_type_in_place(from_col, caps, self.use_drop)
                            .then_some(from_col.name.as_str())
                    })
                    .collect()
            })
            .collect();
        let has_column = |i: usize, column: &str| from[i].columns.iter().any(|c| c.name == column);

        for (i, table) in from.iter().enumerate() {
            if parents[i].is_empty() || own[i].is_empty() {
                continue;
            }
            let mut columns: HashSet<String> = HashSet::new();
            for &column in &own[i] {
                let mut seen = vec![false; from.len()];
                seen[i] = true;
                let mut queue: VecDeque<usize> = parents[i].iter().copied().collect();
                while let Some(a) = queue.pop_front() {
                    if std::mem::replace(&mut seen[a], true) || !has_column(a, column) {
                        continue;
                    }
                    if own[a].contains(column) {
                        columns.insert(column.to_string());
                        break;
                    }
                    queue.extend(parents[a].iter().copied());
                }
            }
            if !columns.is_empty() {
                plans.insert((table.schema.clone(), table.name.clone()), columns);
            }
        }
        plans
    }

    /// The FROM objects that block a column drop the table phase runs to
    /// rewrite a column (see [`Table::rewrite_column_drops`]): PostgreSQL
    /// drops a column's indexes, constraints and statistics with it, but
    /// refuses the drop ("other objects depend on it") while a policy
    /// names the column, a trigger names it (`UPDATE OF`, or its `WHEN`
    /// clause), or another table's foreign key references a key on it. So
    /// the `script` drops each such object, with `IF EXISTS`, ahead of every
    /// table alter, from the FROM dump's column-dependent graph (see
    /// [`ColumnDependentKind`]).
    ///
    /// What comes back afterwards is the TO side's: policies and foreign
    /// keys with the column's other dependents, after the table's alter
    /// (`recreate_column_dependents`); triggers, whose function may be
    /// created later, after every routine with the other triggers — `triggers`
    /// lists them (`table_key` + `.` + name). A foreign key's own table
    /// may drop or change it in its alter too: `foreign_keys` lists them,
    /// by FROM table `(schema, name)`, so that alter says `IF EXISTS`.
    ///
    /// A column the table phase retypes in place (see
    /// [`Table::columns_retyped_in_place`]; a collation change alone is
    /// one), by its own table's alter or through a parent's that recurses to
    /// it, is blocked by its policies and triggers only: `ALTER COLUMN ...
    /// TYPE` rebuilds the column's indexes, constraints (foreign keys
    /// included) and statistics itself, but refuses to run while a policy
    /// ("cannot alter type of a column used in a policy definition") or a
    /// trigger ("... used in a trigger definition") depends on the column.
    /// The `retype_script` drops those, also ahead of every table alter; one
    /// the `script` drops already is not dropped again. The triggers come
    /// back like the ones above; the policies, which `policies` lists by FROM
    /// table `(schema, name)`, are created from TO after every table alter,
    /// and the table phase leaves them out of their own table's statements
    /// (see `Comparer::compare_tables`).
    ///
    /// A policy a drop above takes that also reads a column another table's
    /// alter retypes is not dropped again, but is still listed in `policies`:
    /// the restore after its own table's alter (`recreate_column_dependents`)
    /// would put it back before that other table's `ALTER COLUMN ... TYPE`,
    /// which it then blocks, so it is created once, after every table alter.
    ///
    /// Only tables altered in place count: a table whose `DROP TABLE` runs,
    /// or a partition's ancestor's, takes its objects with it and creates
    /// them from TO. Nor do the columns a typed table has from its type, in
    /// the table or in the partitions and inheritance descendants it keeps in
    /// TO (`Comparer::typed_family_columns`): no table alter retypes those, the
    /// type's `ALTER ATTRIBUTE … TYPE … CASCADE` does, and what is in its way
    /// is dropped ahead of it (`Comparer::typed_retype_pre_section`) — a
    /// difference the type does not make is not retyped at all. Empty with
    /// `use_drop=false`, and for a FROM dump without the graph (pgc 1.0.x
    /// recorded no trigger edges) — but for `ColumnDropBlockers::retyped`,
    /// the columns retyped in place, which only needs `use_drop`.
    fn column_drop_blockers(
        &self,
        alter_caps: &HashMap<(String, String), TableAlterCaps>,
    ) -> ColumnDropBlockers {
        let mut blockers = ColumnDropBlockers::default();
        if !self.use_drop {
            return blockers;
        }
        let to_by_key: HashMap<(&str, &str), &Table> = self
            .to
            .tables
            .iter()
            .map(|t| ((t.schema.as_str(), t.name.as_str()), t))
            .collect();
        let from_index = Self::relation_index(self.from.tables.iter());
        // Whether the table's own DROP TABLE runs (see
        // `tables_rewritten_by_ancestor`: a partition in FROM and TO leaves
        // a column type change to its parent), or a partition ancestor's.
        let drops = |table: &Table, to_table: &Table| {
            table.partition_key != to_table.partition_key
                || (table.will_be_dropped_and_recreated(to_table)
                    && !(table.partition_of.is_some() && to_table.partition_of.is_some()))
        };
        let recreated_wholesale = |table: &Table| -> bool {
            let mut current = table;
            for _ in 0..=self.from.tables.len() {
                match to_by_key.get(&(current.schema.as_str(), current.name.as_str())) {
                    Some(to_table) if !drops(current, to_table) => {}
                    _ => return true,
                }
                let Some(parent) = current
                    .partition_of
                    .as_deref()
                    .and_then(|r| Self::lookup_relation(&from_index, r))
                else {
                    return false;
                };
                current = &self.from.tables[parent];
            }
            false
        };
        let normalise = |s: &str| s.to_lowercase().replace('"', "");
        let mut anchors: HashSet<(String, String, String)> = HashSet::new();
        // The columns retyped in place (see `Table::columns_retyped_in_place`).
        let mut retyped: HashSet<(String, String, String)> = HashSet::new();
        // The columns only a typed table's type retypes.
        let typed_family = self.typed_family_columns();
        for table in &self.from.tables {
            let Some(&to_table) = to_by_key.get(&(table.schema.as_str(), table.name.as_str()))
            else {
                continue;
            };
            if table.hash.is_none()
                || to_table.hash.is_none()
                || !Self::hashes_differ(&table.hash, &to_table.hash)
                || recreated_wholesale(table)
            {
                continue;
            }
            let caps = Self::caps_for(alter_caps, table).generated;
            for column in table.rewrite_column_drops(to_table, self.use_drop, caps) {
                anchors.insert((
                    normalise(&table.schema),
                    normalise(&table.name),
                    normalise(&column),
                ));
            }
            for column in table.columns_retyped_in_place(to_table, self.use_drop, caps) {
                let key = (
                    normalise(&table.schema),
                    normalise(&table.name),
                    normalise(column),
                );
                if !typed_family.contains(&key) {
                    retyped.insert(key);
                }
            }
        }
        blockers.retyped.clone_from(&retyped);
        if self.from.column_dependents.is_empty() || (anchors.is_empty() && retyped.is_empty()) {
            return blockers;
        }

        let mut emitted: HashSet<String> = HashSet::new();
        let mut drop_path_tables: HashMap<String, HashSet<(String, String)>> = HashMap::new();
        self.collect_drop_blockers(
            &anchors,
            &to_by_key,
            &mut emitted,
            &mut drop_path_tables,
            &mut blockers,
        );

        // An in-place retype rebuilds the column's indexes, constraints
        // (foreign keys included) and statistics itself; only its policies
        // and triggers are in the way. One a drop above has taken is not
        // dropped again.
        self.collect_retype_blockers(&retyped, &mut emitted, &drop_path_tables, &mut blockers);
        blockers
    }

    /// The drop half of `Comparer::column_drop_blockers`: for each FROM
    /// policy, trigger and other table's foreign key that depends, by the
    /// FROM column-dependent graph, on one of the `anchors` (lowercased,
    /// quote-stripped `(schema, table, column)`, the columns dropped to be
    /// rewritten), a `drop … if exists` in `blockers.script`, with the trigger
    /// in `blockers.triggers` and the foreign key in `blockers.foreign_keys`
    /// for the table phase to put back; a foreign key only when its table is
    /// in TO (`to_by_key`). One `emitted` holds already (keyed
    /// `{kind:?}:{schema}.{table}.{name}`) is left out; the rest are added to
    /// it, and each policy with the lowercased, quote-stripped `(schema,
    /// table)` of every anchor it depends on to `drop_path_tables`.
    fn collect_drop_blockers(
        &self,
        anchors: &HashSet<(String, String, String)>,
        to_by_key: &HashMap<(&str, &str), &Table>,
        emitted: &mut HashSet<String>,
        drop_path_tables: &mut HashMap<String, HashSet<(String, String)>>,
        blockers: &mut ColumnDropBlockers,
    ) {
        let normalise = |s: &str| s.to_lowercase().replace('"', "");
        for dep in &self.from.column_dependents {
            if !anchors.contains(&(
                normalise(&dep.schema),
                normalise(&dep.table),
                normalise(&dep.column),
            )) {
                continue;
            }
            let Some(owner) = self
                .from
                .tables
                .iter()
                .find(|t| t.schema == dep.dep_schema && t.name == dep.dep_table)
            else {
                continue;
            };
            let key = format!(
                "{:?}:{}.{}.{}",
                dep.kind, owner.schema, owner.name, dep.dep_name
            );
            match dep.kind {
                ColumnDependentKind::Policy => {
                    if !owner.policies.iter().any(|p| p.name == dep.dep_name) {
                        continue;
                    }
                    drop_path_tables
                        .entry(key.clone())
                        .or_default()
                        .insert((normalise(&dep.schema), normalise(&dep.table)));
                    if emitted.insert(key) {
                        blockers.script.append_block(&format!(
                            "drop policy if exists {} on {}.{};",
                            dep.dep_name, owner.schema, owner.name
                        ));
                    }
                }
                ColumnDependentKind::Trigger => {
                    if owner.triggers.iter().any(|t| t.name == dep.dep_name) && emitted.insert(key)
                    {
                        blockers.script.append_block(&format!(
                            "drop trigger if exists {} on {}.{};",
                            dep.dep_name, owner.schema, owner.name
                        ));
                        blockers.triggers.insert(format!(
                            "{}.{}",
                            Self::table_key(&owner.schema, &owner.name),
                            dep.dep_name
                        ));
                    }
                }
                ColumnDependentKind::Constraint => {
                    // A foreign key on another table (or referencing its own)
                    // depends on the referenced column normally; the
                    // table's own UNIQUE, CHECK and the like go with it.
                    let is_fk = owner.constraints.iter().any(|c| {
                        c.name == dep.dep_name
                            && c.constraint_type.eq_ignore_ascii_case("foreign key")
                    });
                    if is_fk
                        && to_by_key.contains_key(&(owner.schema.as_str(), owner.name.as_str()))
                        && emitted.insert(key)
                    {
                        blockers.script.append_block(&format!(
                            "alter table {}.{} drop constraint if exists {};",
                            owner.schema, owner.name, dep.dep_name
                        ));
                        blockers
                            .foreign_keys
                            .entry((owner.schema.clone(), owner.name.clone()))
                            .or_default()
                            .insert(dep.dep_name.clone());
                    }
                }
                ColumnDependentKind::Index | ColumnDependentKind::Statistic => {}
            }
        }
    }

    /// The part of `Comparer::column_drop_blockers` for the columns retyped
    /// in place: for each FROM policy and trigger that depends, by the FROM
    /// column-dependent graph, on one of the `retyped` columns (lowercased,
    /// quote-stripped `(schema, table, column)`), a `drop … if exists` in
    /// `blockers.retype_script`, and the policy in `blockers.policies` or the
    /// trigger in `blockers.triggers` for the table phase to put back. One
    /// `emitted` holds already (keyed `{kind:?}:{schema}.{table}.{name}`) is
    /// not dropped again; the rest are added to it.
    ///
    /// A policy the drop half has taken (`Comparer::collect_drop_blockers`,
    /// which lists the tables of its anchors in `drop_path_tables`) is
    /// still listed in `blockers.policies` when the retyped column is on
    /// another table: that table's `ALTER COLUMN ... TYPE` may come after the
    /// drop half's restore, so the policy waits for every table alter.
    fn collect_retype_blockers(
        &self,
        retyped: &HashSet<(String, String, String)>,
        emitted: &mut HashSet<String>,
        drop_path_tables: &HashMap<String, HashSet<(String, String)>>,
        blockers: &mut ColumnDropBlockers,
    ) {
        let normalise = |s: &str| s.to_lowercase().replace('"', "");
        for dep in &self.from.column_dependents {
            if !matches!(
                dep.kind,
                ColumnDependentKind::Policy | ColumnDependentKind::Trigger
            ) || !retyped.contains(&(
                normalise(&dep.schema),
                normalise(&dep.table),
                normalise(&dep.column),
            )) {
                continue;
            }
            let Some(owner) = self
                .from
                .tables
                .iter()
                .find(|t| t.schema == dep.dep_schema && t.name == dep.dep_table)
            else {
                continue;
            };
            let key = format!(
                "{:?}:{}.{}.{}",
                dep.kind, owner.schema, owner.name, dep.dep_name
            );
            if dep.kind == ColumnDependentKind::Policy {
                if !owner.policies.iter().any(|p| p.name == dep.dep_name) {
                    continue;
                }
                let retyped_elsewhere = || {
                    drop_path_tables.get(&key).is_some_and(|tables| {
                        !tables.contains(&(normalise(&dep.schema), normalise(&dep.table)))
                    })
                };
                let fresh = emitted.insert(key.clone());
                if fresh {
                    blockers.retype_script.append_block(&format!(
                        "drop policy if exists {} on {}.{};",
                        dep.dep_name, owner.schema, owner.name
                    ));
                }
                if fresh || retyped_elsewhere() {
                    blockers
                        .policies
                        .entry((owner.schema.clone(), owner.name.clone()))
                        .or_default()
                        .insert(dep.dep_name.clone());
                }
            } else if owner.triggers.iter().any(|t| t.name == dep.dep_name) && emitted.insert(key) {
                blockers.retype_script.append_block(&format!(
                    "drop trigger if exists {} on {}.{};",
                    dep.dep_name, owner.schema, owner.name
                ));
                blockers.triggers.insert(format!(
                    "{}.{}",
                    Self::table_key(&owner.schema, &owner.name),
                    dep.dep_name
                ));
            }
        }
    }

    /// The FROM composite types whose attributes the composite section
    /// retypes with a live `ALTER ATTRIBUTE … SET DATA TYPE` (a collation
    /// change included), each with those attributes (see
    /// [`PgType::composite_retyped_attributes`]): the types `compare_types`
    /// alters in place, with what the migrated database keeps in them
    /// (`Comparer::composite_usage`).
    fn composite_retypes(&self) -> Vec<(&PgType, Vec<String>)> {
        let from_by_key: HashMap<(&str, &str), &PgType> = self
            .from
            .types
            .iter()
            .filter(|t| (t.typtype as u8 as char) == 'c')
            .map(|t| ((t.schema.as_str(), t.typname.as_str()), t))
            .collect();
        if from_by_key.is_empty() {
            return Vec::new();
        }
        self.to
            .types
            .iter()
            .filter(|t| (t.typtype as u8 as char) == 'c')
            .filter_map(|to_type| {
                let from_type =
                    *from_by_key.get(&(to_type.schema.as_str(), to_type.typname.as_str()))?;
                if !Self::type_definition_differs(from_type, to_type) {
                    return None;
                }
                let usage = self.composite_usage(from_type);
                let attributes =
                    from_type.composite_retyped_attributes(to_type, self.use_drop, &usage);
                (!attributes.is_empty()).then_some((from_type, attributes))
            })
            .collect()
    }

    /// The `(table index, column)` pairs of each seed `(table index, columns)`
    /// of `tables` and of every partition and `INHERITS` descendant of it
    /// there, transitively, that has a column of that name (each spelled as
    /// the dump stores table columns, in `quote_ident` form): `ALTER COLUMN
    /// … TYPE` (and so a composite type's `ALTER ATTRIBUTE … TYPE …
    /// CASCADE` on its typed tables) recurses to them.
    fn family_columns(
        tables: &[Table],
        seeds: Vec<(usize, Vec<String>)>,
    ) -> HashSet<(usize, String)> {
        let mut members: HashSet<(usize, String)> = HashSet::new();
        if seeds.is_empty() {
            return members;
        }
        let index = Self::relation_index(tables.iter());
        let mut children: Vec<Vec<usize>> = vec![Vec::new(); tables.len()];
        for (child, table) in tables.iter().enumerate() {
            for reference in table.partition_of.iter().chain(&table.inherits_from) {
                if let Some(parent) = Self::lookup_relation(&index, reference)
                    && parent != child
                {
                    children[parent].push(child);
                }
            }
        }
        let mut queue: VecDeque<(usize, String)> = seeds
            .into_iter()
            .flat_map(|(i, columns)| columns.into_iter().map(move |c| (i, c)))
            .collect();
        while let Some((i, column)) = queue.pop_front() {
            if !tables[i].columns.iter().any(|c| c.name == column)
                || !members.insert((i, column.clone()))
            {
                continue;
            }
            queue.extend(children[i].iter().map(|&child| (child, column.clone())));
        }
        members
    }

    /// `family_columns` as lowercased, quote-stripped `(schema, table,
    /// column)`.
    fn family_column_keys(
        tables: &[Table],
        seeds: Vec<(usize, Vec<String>)>,
    ) -> HashSet<(String, String, String)> {
        let normalise = |s: &str| s.to_lowercase().replace('"', "");
        Self::family_columns(tables, seeds)
            .into_iter()
            .map(|(i, column)| {
                let table = &tables[i];
                (
                    normalise(&table.schema),
                    normalise(&table.name),
                    normalise(&column),
                )
            })
            .collect()
    }

    /// The columns, as lowercased, quote-stripped `(schema, table, column)`,
    /// that a table typed by the same type in FROM and TO has from its type,
    /// and their copies in its partitions and `INHERITS` descendants (see
    /// `Comparer::family_columns`) — on both sides: a descendant TO takes out
    /// of the family (a partition detached, an `INHERIT` link removed), and
    /// so leaves to its own alter, does not count. The table phase retypes
    /// none of them: the typed table's alter leaves its columns to the type
    /// (see `Table::build_alter_script`), and a descendant's to that
    /// ancestor.
    fn typed_family_columns(&self) -> HashSet<(String, String, String)> {
        let from_by_key: HashMap<(&str, &str), &Table> = self
            .from
            .tables
            .iter()
            .map(|t| ((t.schema.as_str(), t.name.as_str()), t))
            .collect();
        let to_by_key: HashMap<(&str, &str), &Table> = self
            .to
            .tables
            .iter()
            .map(|t| ((t.schema.as_str(), t.name.as_str()), t))
            .collect();
        let seeds = |tables: &[Table], other: &HashMap<(&str, &str), &Table>| {
            tables
                .iter()
                .enumerate()
                .filter(|(_, t)| {
                    t.typed_table_type.is_some()
                        && other
                            .get(&(t.schema.as_str(), t.name.as_str()))
                            .is_some_and(|o| o.typed_table_type == t.typed_table_type)
                })
                .map(|(i, t)| (i, t.columns.iter().map(|c| c.name.clone()).collect()))
                .collect::<Vec<(usize, Vec<String>)>>()
        };
        let from_seeds = seeds(&self.from.tables, &to_by_key);
        if from_seeds.is_empty() {
            return HashSet::new();
        }
        let to_members =
            Self::family_column_keys(&self.to.tables, seeds(&self.to.tables, &from_by_key));
        Self::family_column_keys(&self.from.tables, from_seeds)
            .into_iter()
            .filter(|key| to_members.contains(key))
            .collect()
    }

    /// The columns of `Comparer::typed_family_columns` that the composite
    /// section's `ALTER ATTRIBUTE … TYPE … CASCADE` leaves alone (see
    /// `Comparer::typed_table_retypes`), as lowercased, quote-stripped
    /// `(schema, table, column)`: no statement of the migration retypes them,
    /// however their FROM and TO types differ — the type is not compared, a
    /// column stores it, or the difference is not the type's. So nothing
    /// reading one (a view, a rule, a routine body, a publication's column
    /// list or row filter) has to be taken out of the way of a retype.
    fn unretyped_typed_columns(&self) -> HashSet<(String, String, String)> {
        let family = self.typed_family_columns();
        if family.is_empty() {
            return family;
        }
        let retyped = self.typed_table_retypes();
        family
            .into_iter()
            .filter(|key| !retyped.contains(key))
            .collect()
    }

    /// The FROM columns, as lowercased, quote-stripped `(schema, table,
    /// column)`, whose type the composite section's `ALTER ATTRIBUTE … TYPE
    /// … CASCADE` changes (see `Comparer::composite_retypes`): the column of
    /// that name of every FROM typed table of the type — kept, recreated
    /// or dropped by the table phase, which only runs after it — and its
    /// copies in their partitions and `INHERITS` descendants, to which the
    /// statement recurses (see `Comparer::family_columns`).
    ///
    /// An attribute is named as the catalog spells it (`Amt`, `order`), a
    /// table column in `quote_ident` form (`"Amt"`, `"order"`), so each
    /// attribute is looked up among its typed table's columns by its
    /// unquoted name.
    fn typed_table_retypes(&self) -> HashSet<(String, String, String)> {
        let retypes = self.composite_retypes();
        if retypes.is_empty() {
            return HashSet::new();
        }
        let seeds: Vec<(usize, Vec<String>)> = self
            .from
            .tables
            .iter()
            .enumerate()
            .filter_map(|(i, table)| {
                let type_name = table.typed_table_type.as_deref()?;
                let columns: Vec<String> = retypes
                    .iter()
                    .filter(|(pg_type, _)| pg_type.is_named_by(type_name))
                    .flat_map(|(_, attributes)| attributes.iter())
                    .filter_map(|attribute| {
                        table
                            .columns
                            .iter()
                            .find(|c| unquote_ident(&c.name) == *attribute)
                            .map(|c| c.name.clone())
                    })
                    .collect();
                (!columns.is_empty()).then_some((i, columns))
            })
            .collect();
        Self::family_column_keys(&self.from.tables, seeds)
    }

    /// How the pass ahead of the composite section handles the generated
    /// column `column` of a FROM table the table phase drops, when a FROM
    /// partition or `INHERITS` ancestor of `table`, at any level, has a
    /// generated column of that name too: `Some(true)` when one of those
    /// ancestors is kept in TO (`to_by_key`) with a plain column of that
    /// name, whose `DROP EXPRESSION` recurses to the copy here;
    /// `Some(false)` otherwise, when an ancestor's `DROP COLUMN` recurses to
    /// it instead — taking an inherited copy along, but not one `table`
    /// defines itself as well. `None` when no ancestor has such a column.
    /// The dump does not say whether a column is inherited (`attislocal`),
    /// so both statements allow for either.
    fn ancestor_generated_handling(
        &self,
        table: &Table,
        column: &str,
        to_by_key: &HashMap<(&str, &str), &Table>,
    ) -> Option<bool> {
        let from = &self.from.tables;
        let index = Self::relation_index(from.iter());
        let parents = |t: &Table| -> Vec<usize> {
            t.partition_of
                .iter()
                .chain(&t.inherits_from)
                .filter_map(|reference| Self::lookup_relation(&index, reference))
                .collect()
        };
        let mut seen = vec![false; from.len()];
        let mut queue: VecDeque<usize> = parents(table).into();
        let mut found = None;
        while let Some(i) = queue.pop_front() {
            if std::mem::replace(&mut seen[i], true) {
                continue;
            }
            let ancestor = &from[i];
            if ancestor
                .columns
                .iter()
                .any(|c| c.name == column && c.is_generated_column())
            {
                let expression_dropped = to_by_key
                    .get(&(ancestor.schema.as_str(), ancestor.name.as_str()))
                    .and_then(|to| to.columns.iter().find(|c| c.name == column))
                    .is_some_and(|c| !c.is_generated_column());
                found = Some(found.unwrap_or(false) || expression_dropped);
            }
            queue.extend(parents(ancestor));
        }
        found
    }

    /// What has to be out of the way of the composite section's `ALTER
    /// ATTRIBUTE … TYPE … CASCADE` (see `Comparer::typed_table_retypes`),
    /// which runs after the view drops and ahead of the table phase whose
    /// own blocker drops come too late for it. PostgreSQL refuses it, as it
    /// refuses `ALTER COLUMN … TYPE`, while a policy or a trigger depends on
    /// a column it retypes — on a typed table, on a partition or `INHERITS`
    /// descendant of one, and on one the table phase drops or recreates
    /// later — or while a generated column reads one ("cannot alter type of
    /// a column used by a generated column"), which a typed table itself
    /// cannot have but its inheritance descendants can.
    ///
    /// So, in this order, right before the composite section:
    ///
    /// * the policies, triggers and foreign keys in the way of dropping those
    ///   generated columns (`Comparer::collect_drop_blockers`);
    /// * the policies and triggers on a retyped column
    ///   (`Comparer::collect_retype_blockers`), from the FROM graph;
    /// * the generated columns: a descendant the table phase alters in place
    ///   has its [`Table::ancestor_retype_pre_script`] run here instead of
    ///   there (`typed_hoisted_tables`), and adds the column back in its own
    ///   alter; one the table phase drops loses the column here, unless an
    ///   ancestor's statement there reaches its copy (see
    ///   `Comparer::ancestor_generated_handling`).
    ///
    /// The table phase puts the policies (`typed_retype_policies`) and the
    /// triggers (`typed_retype_triggers`) back from TO after every table
    /// alter, as it does its own retype blockers, and plans the generated
    /// columns' blockers again itself (its drops say `IF EXISTS`); nothing
    /// comes back on a table it drops, and a table it recreates brings its
    /// own back with its `CREATE TABLE`. Empty without `use_drop`; the
    /// policies and triggers also need the FROM dump's column-dependent
    /// graph, which a pgc 1.0.x dump does not have.
    fn typed_retype_pre_section(&mut self) -> String {
        if !self.use_drop {
            return String::new();
        }
        let retyped = self.typed_table_retypes();
        if retyped.is_empty() {
            return String::new();
        }
        let normalise = |s: &str| s.to_lowercase().replace('"', "");
        let table_retyped = |table: &Table| -> HashSet<String> {
            let (schema, name) = (normalise(&table.schema), normalise(&table.name));
            table
                .columns
                .iter()
                .filter(|c| retyped.contains(&(schema.clone(), name.clone(), normalise(&c.name))))
                .map(|c| c.name.clone())
                .collect()
        };
        let reads_any = |column: &TableColumn, columns: &HashSet<String>| {
            columns
                .iter()
                .any(|base| *base != column.name && column.generation_expression_reads(base))
        };
        let to_by_key: HashMap<(&str, &str), &Table> = self
            .to
            .tables
            .iter()
            .map(|t| ((t.schema.as_str(), t.name.as_str()), t))
            .collect();

        // The generated columns in the way, and their own blockers.
        let mut generated_script = String::new();
        let mut hoisted: HashSet<(String, String)> = HashSet::new();
        let mut kept_anchors: HashSet<(String, String, String)> = HashSet::new();
        let mut dropped_anchors: HashSet<(String, String, String)> = HashSet::new();
        let has_generated_in_the_way = self.from.tables.iter().any(|table| {
            let columns = table_retyped(table);
            !columns.is_empty()
                && table
                    .columns
                    .iter()
                    .any(|c| c.is_generated_column() && reads_any(c, &columns))
        });
        if has_generated_in_the_way {
            let alter_caps = self.table_alter_caps();
            let plan = self.plan_inherited_recreates(&alter_caps);
            let no_plan = InheritedRecreates::default();
            for table in Self::ordered_tables(&self.from.tables) {
                let columns = table_retyped(table);
                if columns.is_empty() {
                    continue;
                }
                let key = (table.schema.clone(), table.name.clone());
                let anchor = |column: &str| {
                    (
                        normalise(&table.schema),
                        normalise(&table.name),
                        normalise(column),
                    )
                };
                let Some(&to_table) = to_by_key.get(&(table.schema.as_str(), table.name.as_str()))
                else {
                    // Dropped by the table phase, after the composite section.
                    for column in table
                        .columns
                        .iter()
                        .filter(|c| c.is_generated_column() && reads_any(c, &columns))
                    {
                        dropped_anchors.insert(anchor(&column.name));
                        match self.ancestor_generated_handling(table, &column.name, &to_by_key) {
                            // No ancestor has it: the column is this table's.
                            None => generated_script.append_block(&format!(
                                "alter table {}.{} drop column {};",
                                table.schema, table.name, column.name
                            )),
                            // An ancestor's `DROP EXPRESSION` recurses to its
                            // copy here, which may not be dropped on its own
                            // ("cannot drop inherited column").
                            Some(true) => {}
                            // An ancestor's `DROP COLUMN`, ahead of this one,
                            // took an inherited copy along; one this table
                            // also defines itself is left, and goes here.
                            Some(false) => generated_script.append_block(&format!(
                                "alter table {}.{} drop column if exists {};",
                                table.schema, table.name, column.name
                            )),
                        }
                    }
                    continue;
                };
                let inherited = plan.get(&key).unwrap_or(&no_plan);
                if !inherited
                    .retyped_by_ancestor
                    .iter()
                    .any(|column| columns.contains(column))
                {
                    continue;
                }
                let caps = Self::caps_for(&alter_caps, table);
                let pre =
                    table.ancestor_retype_pre_script(to_table, self.use_drop, caps, inherited);
                if pre.is_empty() {
                    continue;
                }
                generated_script.push_str(&pre);
                for column in table.rewrite_column_drops(to_table, self.use_drop, caps.generated) {
                    if table
                        .columns
                        .iter()
                        .any(|c| c.name == column && reads_any(c, &inherited.retyped_by_ancestor))
                    {
                        kept_anchors.insert(anchor(&column));
                    }
                }
                hoisted.insert(key);
            }
        }

        let mut blockers = ColumnDropBlockers::default();
        if !self.from.column_dependents.is_empty() {
            let mut emitted: HashSet<String> = HashSet::new();
            let mut drop_path_tables: HashMap<String, HashSet<(String, String)>> = HashMap::new();
            self.collect_drop_blockers(
                &kept_anchors,
                &to_by_key,
                &mut emitted,
                &mut drop_path_tables,
                &mut blockers,
            );
            // A foreign key on a table the table phase drops is its to drop.
            self.collect_drop_blockers(
                &dropped_anchors,
                &HashMap::new(),
                &mut emitted,
                &mut drop_path_tables,
                &mut blockers,
            );
            // The composite section runs before any table alter, so a policy
            // the drops above took waits for nothing: the table phase's own
            // restore of it is late enough.
            self.collect_retype_blockers(&retyped, &mut emitted, &HashMap::new(), &mut blockers);
        }

        // What the table phase puts back: nothing on a table it drops, and
        // nothing on one whose `DROP TABLE` runs, which its `CREATE TABLE`
        // brings back.
        let families = self.recreated_families();
        let restored_on = |schema: &str, name: &str| {
            let key = Self::table_key(schema, name);
            to_by_key.contains_key(&(schema, name))
                && !families.roots.contains(&key)
                && !families.members.contains_key(&key)
        };
        let policies: HashMap<(String, String), HashSet<String>> = blockers
            .policies
            .into_iter()
            .filter(|((schema, name), _)| restored_on(schema, name))
            .collect();
        let triggers: HashSet<String> = blockers
            .triggers
            .into_iter()
            .filter(|trigger| {
                self.from.tables.iter().any(|t| {
                    restored_on(&t.schema, &t.name)
                        && trigger
                            .strip_prefix(&Self::table_key(&t.schema, &t.name))
                            .is_some_and(|rest| rest.starts_with('.'))
                })
            })
            .collect();
        self.typed_retype_policies = policies;
        self.typed_retype_triggers = triggers;
        self.typed_hoisted_tables = hoisted;

        let mut section = String::new();
        let drops = format!("{}{}", blockers.script, blockers.retype_script);
        if !drops.is_empty() {
            section.append_block(
                "\n/* ---> Dependents dropped ahead of a typed table's column retype: Start ---- */",
            );
            section.push_str(&drops);
            section.append_block(
                "/* ---> Dependents dropped ahead of a typed table's column retype: End ------ */",
            );
        }
        if !generated_script.is_empty() {
            section.append_block(
                "\n/* ---> Generated columns let go before a composite type's alter retypes a column they read: Start ---- */",
            );
            section.push_str(&generated_script);
            section.append_block(
                "/* ---> Generated columns let go before a composite type's alter retypes a column they read: End ------ */",
            );
        }
        section
    }

    /// The TO policies that read, by the TO dump's column-dependent graph, a
    /// column of another table that the migration retypes in place
    /// (`retyped`, see `ColumnDropBlockers::retyped`) — through a subquery,
    /// since a policy's own columns are its table's. PostgreSQL refuses that
    /// retype while such a policy exists ("cannot alter type of a column
    /// used in a policy definition"), so one the table phase creates before
    /// the retyping table's alter — on a new table, or new or changed on a
    /// table altered earlier — has to wait for it: `compare_tables` leaves
    /// it out of its table's statements and creates it with the policies
    /// `Comparer::column_drop_blockers` drops ahead, after every table alter.
    ///
    /// Keyed by the policy's table as `Comparer::table_key` spells it, each
    /// with the policy's name and the `table_key` of a retyping table (a
    /// policy reading columns of several has an entry for each).
    fn policies_reading_retyped_columns(
        &self,
        retyped: &HashSet<(String, String, String)>,
    ) -> HashMap<String, Vec<(String, String)>> {
        let mut waiting: HashMap<String, Vec<(String, String)>> = HashMap::new();
        if retyped.is_empty() {
            return waiting;
        }
        let normalise = |s: &str| s.to_lowercase().replace('"', "");
        let retyping: HashMap<(String, String), String> = self
            .from
            .tables
            .iter()
            .map(|t| {
                (
                    (normalise(&t.schema), normalise(&t.name)),
                    Self::table_key(&t.schema, &t.name),
                )
            })
            .collect();
        let mut seen: HashSet<(String, String, String)> = HashSet::new();
        for dep in &self.to.column_dependents {
            if dep.kind != ColumnDependentKind::Policy {
                continue;
            }
            let anchor = (
                normalise(&dep.schema),
                normalise(&dep.table),
                normalise(&dep.column),
            );
            if !retyped.contains(&anchor)
                || (normalise(&dep.dep_schema), normalise(&dep.dep_table))
                    == (anchor.0.clone(), anchor.1.clone())
            {
                continue;
            }
            let Some(owner) = self
                .to
                .tables
                .iter()
                .find(|t| t.schema == dep.dep_schema && t.name == dep.dep_table)
            else {
                continue;
            };
            let Some(retyping_key) = retyping.get(&(anchor.0, anchor.1)) else {
                continue;
            };
            let owner_key = Self::table_key(&owner.schema, &owner.name);
            if owner.policies.iter().any(|p| p.name == dep.dep_name)
                && seen.insert((
                    owner_key.clone(),
                    dep.dep_name.clone(),
                    retyping_key.clone(),
                ))
            {
                waiting
                    .entry(owner_key)
                    .or_default()
                    .push((dep.dep_name.clone(), retyping_key.clone()));
            }
        }
        waiting
    }

    /// A copy of `table` without its policies named in `names`, or `None`
    /// when `names` holds none of them.
    fn table_without_policies(table: &Table, names: Option<&HashSet<String>>) -> Option<Table> {
        let names = names?;
        if !table.policies.iter().any(|p| names.contains(&p.name)) {
            return None;
        }
        let mut view = table.clone();
        view.policies.retain(|p| !names.contains(&p.name));
        Some(view)
    }

    /// The own triggers, by name, that each FROM table's alter drops before
    /// it attaches the table to a partitioned parent, keyed by FROM table
    /// `(schema, name)` (see [`InheritedRecreates::triggers_dropped_before_attach`]):
    /// those TO no longer has that share a name with a row trigger of the TO
    /// parent or of any TO ancestor above it, each of which the attach
    /// clones onto the table, or with a clone TO records on it (every one,
    /// under a parent the dump does not have). Any other trigger drop stays
    /// with the trigger statements after every table, as before.
    fn triggers_dropped_before_attach(&self) -> HashMap<(String, String), HashSet<String>> {
        let mut plans: HashMap<(String, String), HashSet<String>> = HashMap::new();
        let to_by_key: HashMap<String, &Table> = self
            .to
            .tables
            .iter()
            .map(|t| (Self::table_key(&t.schema, &t.name), t))
            .collect();
        for table in &self.from.tables {
            let Some(to_table) = to_by_key
                .get(&Self::table_key(&table.schema, &table.name))
                .copied()
            else {
                continue;
            };
            if table.triggers.is_empty() || !table.attaches_to_another_parent(to_table) {
                continue;
            }
            // The clones TO records on the table name some too: every one,
            // under a parent the dump does not have.
            let mut cloned: HashSet<&str> = to_table
                .trigger_clones
                .iter()
                .map(|t| t.name.as_str())
                .collect();
            let mut parent = to_table.partition_of.as_deref();
            for _ in 0..self.to.tables.len() {
                let Some(ancestor) =
                    parent.and_then(|p| to_by_key.get(&Self::normalise_partition_of(p)).copied())
                else {
                    break;
                };
                cloned.extend(
                    ancestor
                        .triggers
                        .iter()
                        .filter(|t| t.is_row_level())
                        .map(|t| t.name.as_str()),
                );
                parent = ancestor.partition_of.as_deref();
            }
            let names: HashSet<String> = table
                .triggers
                .iter()
                .filter(|t| {
                    cloned.contains(t.name.as_str())
                        && !to_table.triggers.iter().any(|to| to.name == t.name)
                })
                .map(|t| t.name.clone())
                .collect();
            if !names.is_empty() {
                plans.insert((table.schema.clone(), table.name.clone()), names);
            }
        }
        plans
    }

    /// The CHECK constraints, keyed by FROM table `(schema, name)`, that each
    /// table's alter handles around its `DETACH PARTITION` / `ATTACH
    /// PARTITION` (see [`Table::partition_moves`]) and its `NO INHERIT` /
    /// `INHERIT` (see [`Table::inheritance_moves`]): the copies it inherits
    /// from the FROM parents it leaves, which PostgreSQL leaves behind as
    /// local constraints ([`InheritedRecreates::detached_checks`]); the
    /// ones it inherits from its new TO parents, which it needs before the
    /// attach ([`InheritedRecreates::attach_checks`]); and the new
    /// `INHERITS` ancestors' CHECKs that TO has it only inherit, which
    /// their holders drop and re-add around the `INHERIT`
    /// ([`InheritedRecreates::inherit_readds`]).
    fn partition_move_checks(&self) -> HashMap<(String, String), MoveChecks> {
        let mut plans = HashMap::new();
        let from_index = Self::relation_index(self.from.tables.iter());
        let to_index = Self::relation_index(self.to.tables.iter());
        for table in &self.from.tables {
            let Some(to_table) =
                Self::lookup_relation(&to_index, &Self::table_key(&table.schema, &table.name))
                    .map(|i| &self.to.tables[i])
            else {
                continue;
            };
            let mut plan = MoveChecks::default();
            let (detaches, attaches) = table.partition_moves(to_table);
            if detaches {
                plan.detached =
                    Self::inherited_check_copies(&self.from.tables, &from_index, table, to_table)
                        .into_iter()
                        .map(|mut copy| {
                            copy.coninhcount = 0;
                            copy
                        })
                        .collect();
            }
            if attaches {
                plan.attached =
                    Self::inherited_check_copies(&self.to.tables, &to_index, to_table, to_table);
            }
            let (left, joined) = table.inheritance_moves(to_table);
            if !left.is_empty() {
                // `NO INHERIT` is commented out without `use_drop`, and the
                // table keeps its copies then.
                let kept = &table.inherits_from[..table.inherits_from.len() - left.len()];
                let still: HashSet<String> =
                    Self::inheritance_check_copies(&self.from.tables, &from_index, kept, table)
                        .into_iter()
                        .map(|(copy, _)| copy.name)
                        .collect();
                for (mut copy, _) in
                    Self::inheritance_check_copies(&self.from.tables, &from_index, left, table)
                {
                    if still.contains(&copy.name)
                        || plan.detached.iter().any(|c| c.name == copy.name)
                    {
                        continue;
                    }
                    copy.coninhcount = 0;
                    plan.detached.push(copy);
                }
            }
            if !joined.is_empty() {
                let kept = &to_table.inherits_from[..to_table.inherits_from.len() - joined.len()];
                let still: HashSet<String> =
                    Self::inheritance_check_copies(&self.to.tables, &to_index, kept, to_table)
                        .into_iter()
                        .map(|(copy, _)| copy.name)
                        .collect();
                for (copy, holders) in
                    Self::inheritance_check_copies(&self.to.tables, &to_index, joined, to_table)
                {
                    if still.contains(&copy.name)
                        || plan.attached.iter().any(|c| c.name == copy.name)
                    {
                        continue;
                    }
                    let merged_in_to = to_table.constraints.iter().any(|c| {
                        c.name == copy.name && c.constraint_type.eq_ignore_ascii_case("check")
                    });
                    if merged_in_to || !self.use_drop {
                        plan.attached.push(copy);
                    } else {
                        plan.readds.extend(holders);
                    }
                }
            }
            if !plan.detached.is_empty() || !plan.attached.is_empty() || !plan.readds.is_empty() {
                plans.insert((table.schema.clone(), table.name.clone()), plan);
            }
        }
        plans
    }

    /// The copies `owner` (a table in `tables`) inherits through the
    /// `INHERITS` parents `parents` names of the CHECK constraints those
    /// parents and their `INHERITS` ancestors in `tables` hold, each with
    /// its holders' own constraints, parents first. A copy (see
    /// `TableConstraint::inherited_copy`) is ENFORCED when a holder's is, or
    /// when the dump records an enforced copy
    /// ([`Table::enforced_inherited_checks`]) on `owner` or on an ancestor.
    /// A partition cannot be an `INHERITS` parent, nor a partitioned table,
    /// so the walk follows `inherits_from` only; an ancestor outside
    /// `tables` ends it.
    fn inheritance_check_copies(
        tables: &[Table],
        index: &HashMap<String, usize>,
        parents: &[String],
        owner: &Table,
    ) -> Vec<(TableConstraint, Vec<TableConstraint>)> {
        // The ancestors, and each one's own ancestors, to order holders.
        let mut ancestors: Vec<usize> = Vec::new();
        let mut queue: VecDeque<usize> = parents
            .iter()
            .filter_map(|p| Self::lookup_relation(index, p))
            .collect();
        while let Some(a) = queue.pop_front() {
            if ancestors.contains(&a) || std::ptr::eq(&tables[a], owner) {
                continue;
            }
            ancestors.push(a);
            queue.extend(
                tables[a]
                    .inherits_from
                    .iter()
                    .filter_map(|p| Self::lookup_relation(index, p)),
            );
        }
        let above = |a: usize| -> HashSet<usize> {
            let mut seen = HashSet::new();
            let mut queue: VecDeque<usize> = tables[a]
                .inherits_from
                .iter()
                .filter_map(|p| Self::lookup_relation(index, p))
                .collect();
            while let Some(b) = queue.pop_front() {
                if b != a && seen.insert(b) {
                    queue.extend(
                        tables[b]
                            .inherits_from
                            .iter()
                            .filter_map(|p| Self::lookup_relation(index, p)),
                    );
                }
            }
            seen
        };
        // Parents first: an ancestor comes after every ancestor above it.
        let above_sets: Vec<HashSet<usize>> = ancestors.iter().map(|&a| above(a)).collect();
        let mut ordered: Vec<usize> = Vec::new();
        let mut left: Vec<usize> = (0..ancestors.len()).collect();
        while !left.is_empty() {
            let pick = left
                .iter()
                .position(|&k| {
                    above_sets[k]
                        .iter()
                        .all(|b| !left.iter().any(|&l| ancestors[l] == *b))
                })
                .unwrap_or(0);
            ordered.push(ancestors[left.remove(pick)]);
        }
        let recorded =
            |t: &Table, name: &str| t.enforced_inherited_checks.iter().any(|n| n == name);
        let mut copies: Vec<(TableConstraint, Vec<TableConstraint>)> = Vec::new();
        for &a in &ordered {
            for check in tables[a]
                .constraints
                .iter()
                .filter(|c| c.constraint_type.eq_ignore_ascii_case("check") && !c.no_inherit)
            {
                match copies.iter_mut().find(|(c, _)| c.name == check.name) {
                    Some((_, holders)) => holders.push(check.clone()),
                    None => copies.push((check.clone(), vec![check.clone()])),
                }
            }
        }
        copies
            .into_iter()
            .map(|(first, holders)| {
                let enforced = holders.iter().any(|h| h.is_enforced)
                    || recorded(owner, &first.name)
                    || ordered.iter().any(|&a| recorded(&tables[a], &first.name));
                (
                    first.inherited_copy(&owner.schema, &owner.name, enforced),
                    holders,
                )
            })
            .collect()
    }

    /// The copies `table` inherits of the CHECK constraints its
    /// `partition_of` parent and that parent's ancestors in `tables` hold,
    /// nearest first, as copies on `owner` (see
    /// `TableConstraint::inherited_copy`). Partitioned tables have no `NO
    /// INHERIT` CHECK, and a partition's inherited copies are never dumped,
    /// so the ones each ancestor holds itself are all of them. A copy is
    /// ENFORCED when the holder's is, or when the dump records an enforced
    /// copy ([`Table::enforced_inherited_checks`]) on `table` or on an
    /// ancestor below the holder: ENFORCED reaches every table beneath. An
    /// ancestor outside `tables` ends the walk.
    fn inherited_check_copies(
        tables: &[Table],
        index: &HashMap<String, usize>,
        table: &Table,
        owner: &Table,
    ) -> Vec<TableConstraint> {
        let ancestors = Self::partition_ancestors(tables, index, table);
        let mut copies: Vec<TableConstraint> = Vec::new();
        for (depth, holder) in ancestors.iter().enumerate() {
            for check in holder
                .constraints
                .iter()
                .filter(|c| c.constraint_type.eq_ignore_ascii_case("check") && !c.no_inherit)
            {
                if copies.iter().any(|c| c.name == check.name) {
                    continue;
                }
                let recorded = |t: &Table| t.enforced_inherited_checks.contains(&check.name);
                let enforced = check.is_enforced
                    || recorded(table)
                    || ancestors[..depth].iter().any(|a| recorded(a));
                copies.push(check.inherited_copy(&owner.schema, &owner.name, enforced));
            }
        }
        copies
    }

    /// The statements that give the CHECK copies TO's tables only inherit
    /// the enforceability TO records for them (see
    /// [`Table::enforced_inherited_checks`]), run after every table alter,
    /// and the warnings for those the migrated server cannot set. Both
    /// empty unless one of the dumps records such a copy.
    ///
    /// An inherited copy follows its parents' statements: an in-place
    /// `ALTER CONSTRAINT ... [NOT] ENFORCED` on an ancestor recurses and
    /// sets it, and a drop + re-add there, like creating the table, gives it
    /// the ancestor's state. So a copy TO keeps ENFORCED under `NOT ENFORCED`
    /// parents gets `ALTER TABLE t ALTER CONSTRAINT c ENFORCED` when FROM
    /// does not record it, or when the migration touches the constraint
    /// anywhere above the table. That statement recurses, and everything
    /// beneath has to be ENFORCED too. A copy FROM records and TO does not,
    /// which TO's parents leave `NOT ENFORCED`, gets `... NOT ENFORCED`,
    /// which recurses as well: a descendant TO keeps ENFORCED comes after
    /// it, as the tables are taken parents first. That recursion turns a
    /// merged local copy beneath `NOT ENFORCED` too, which the descendant's
    /// own alter — run before this pass — left as TO has it; so one TO keeps
    /// ENFORCED is set `ENFORCED` again right after, and so is any copy
    /// beneath that TO keeps ENFORCED.
    ///
    /// A table the migration attaches got its copies in TO's state before
    /// the attach, after its new parents' alters (see
    /// [`InheritedRecreates::attach_checks`]), so only a statement of this
    /// pass above it can still reset them — or, for a table joining
    /// `INHERITS` parents, the holders' re-add around its `INHERIT` (see
    /// [`InheritedRecreates::inherit_readds`]), which gives it the copy a
    /// new table would get.
    ///
    /// Only PostgreSQL 19 alters a CHECK's enforceability in place; on an
    /// older (or unknown) migrated server each such change becomes a
    /// comment in the script and a warning instead.
    fn inherited_check_enforcement(
        &self,
        inherited_recreates: &HashMap<(String, String), InheritedRecreates>,
        alter_caps: &HashMap<(String, String), TableAlterCaps>,
    ) -> (String, Vec<String>) {
        /// One CHECK a TO table inherits, as it reaches the table.
        #[derive(Clone)]
        struct Copy {
            name: String,
            /// TO's copy is ENFORCED.
            enforced: bool,
            /// The migration touches the constraint at or above the table.
            touched: bool,
            /// A statement of this pass reaches it from above.
            reset_here: bool,
            /// A `NOT ENFORCED` statement of this pass reaches it from above,
            /// with no `ENFORCED` one after it: the statement recursed, and
            /// turned every copy beneath `NOT ENFORCED` — a merged local one
            /// included — so whatever TO keeps ENFORCED here needs setting
            /// again.
            cleared: bool,
        }
        /// What the migration does with a TO table.
        enum Fate<'a> {
            /// Altered in place: its FROM side.
            Kept(&'a Table),
            /// Attached under other parents.
            Attached,
            /// Created, or created again.
            Created,
        }

        let mut script = String::new();
        let mut warnings = Vec::new();
        let records = |tables: &[Table]| {
            tables
                .iter()
                .any(|t| !t.enforced_inherited_checks.is_empty())
        };
        if !records(&self.to.tables) && !records(&self.from.tables) {
            return (script, warnings);
        }
        let to = &self.to.tables;
        let to_index = Self::relation_index(to.iter());
        let from_index = Self::relation_index(self.from.tables.iter());
        let parents: Vec<Vec<usize>> = to
            .iter()
            .enumerate()
            .map(|(i, t)| {
                t.partition_of
                    .iter()
                    .chain(t.inherits_from.iter())
                    .filter_map(|r| Self::lookup_relation(&to_index, r))
                    .filter(|&p| p != i)
                    .collect()
            })
            .collect();
        fn local_check<'t>(t: &'t Table, name: &str) -> Option<&'t TableConstraint> {
            t.constraints
                .iter()
                .find(|c| c.name == name && c.constraint_type.eq_ignore_ascii_case("check"))
        }
        let fate = |t: &Table| -> Fate<'_> {
            let Some(from) =
                Self::lookup_relation(&from_index, &Self::table_key(&t.schema, &t.name))
                    .map(|i| &self.from.tables[i])
            else {
                return Fate::Created;
            };
            if self
                .dropped_and_recreated_tables
                .contains(&Self::table_key(&t.schema, &t.name))
            {
                Fate::Created
            } else if from.partition_of != t.partition_of || from.inherits_from != t.inherits_from {
                Fate::Attached
            } else {
                Fate::Kept(from)
            }
        };
        // The CHECKs whose copies an `INHERIT` elsewhere drops and re-adds
        // around it (see `InheritedRecreates::inherit_readds`), by holder.
        let readded: HashSet<(String, String, String)> = inherited_recreates
            .values()
            .flat_map(|plan| plan.inherit_readds.iter())
            .map(|c| (c.schema.clone(), c.table_name.clone(), c.name.clone()))
            .collect();
        // Whether the migration touches TO `parent`'s own copy `check`: it
        // creates the table (again), moves it, re-creates the constraint
        // with its inheritance family or around another table's `INHERIT`,
        // re-creates a column (whose drop can take the CHECK), or the
        // constraint itself changes.
        let parent_touches = |parent: &Table, check: &TableConstraint| match fate(parent) {
            Fate::Kept(from_parent) => {
                let family = inherited_recreates
                    .get(&(from_parent.schema.clone(), from_parent.name.clone()))
                    .is_some_and(|plan| plan.family_checks.contains(&check.name));
                let caps = Self::caps_for(alter_caps, from_parent).generated;
                let rewrites_columns = !from_parent
                    .rewrite_column_drops(parent, self.use_drop, caps)
                    .is_empty();
                // A recursing in-place rewrite of a column the CHECK reads —
                // `ALTER COLUMN ... TYPE` (a collation change included), or
                // a partitioned table's `SET EXPRESSION` — rebuilds every
                // copy beneath from the parent's, and so resets a copy kept
                // ENFORCED under a `NOT ENFORCED` parent (verified live on
                // PostgreSQL 19rc1).
                let rebuilds_check = parent.columns.iter().any(|to_col| {
                    check.check_reads_column(&to_col.name)
                        && from_parent
                            .columns
                            .iter()
                            .find(|c| c.name == to_col.name)
                            .is_some_and(|from_col| {
                                to_col.alters_type_in_place(from_col, caps, self.use_drop)
                                    || (parent.partition_key.is_some()
                                        && to_col.sets_expression_in_place(from_col, caps))
                            })
                });
                family
                    || rewrites_columns
                    || rebuilds_check
                    || readded.contains(&(
                        parent.schema.clone(),
                        parent.name.clone(),
                        check.name.clone(),
                    ))
                    || local_check(from_parent, &check.name).is_none_or(|from_check| {
                        let mut from_check = from_check.clone();
                        from_check.comment.clone_from(&check.comment);
                        from_check != *check
                    })
            }
            Fate::Attached | Fate::Created => true,
        };
        // What a table's own CHECKs pass down before anything reaches them
        // from above.
        let own_copies = |table: &Table| -> Vec<Copy> {
            table
                .constraints
                .iter()
                .filter(|c| c.constraint_type.eq_ignore_ascii_case("check") && !c.no_inherit)
                .map(|c| Copy {
                    name: c.name.clone(),
                    enforced: c.is_enforced,
                    touched: parent_touches(table, c),
                    reset_here: false,
                    cleared: false,
                })
                .collect()
        };
        // Parents first: `ordered_tables` puts partitions after their parent
        // and inheritance children after theirs.
        let positions: HashMap<*const Table, usize> = to
            .iter()
            .enumerate()
            .map(|(i, t)| (t as *const Table, i))
            .collect();
        let order: Vec<usize> = Self::ordered_tables(to)
            .into_iter()
            .filter_map(|t| positions.get(&(t as *const Table)).copied())
            .collect();
        let at_19 = self.migrated_server_at_least(190000);
        // One statement of this pass, or — on a server older than 19 — the
        // comment and warning that stand for it.
        let mut emit = |table: &Table, check: &str, statement: &str| {
            if at_19 {
                script.append_block(&format!(
                    "alter table {}.{} alter constraint {} {statement};",
                    table.schema, table.name, check
                ));
            } else {
                let target = statement.to_uppercase();
                script.push_str(&format!(
                    "-- {}.{}: its inherited copy of CHECK constraint {} has to be {target}, \
                     which ALTER CONSTRAINT sets only on PostgreSQL 19; left as it is.\n",
                    line_comment_safe(&table.schema),
                    line_comment_safe(&table.name),
                    line_comment_safe(check)
                ));
                warnings.push(format!(
                    "Warning: the copy of CHECK constraint {} that table {}.{} inherits has \
                     to be {target}, which only PostgreSQL 19 can set in place (ALTER \
                     CONSTRAINT); the migration leaves it as it is.",
                    check, table.schema, table.name
                ));
            }
        };
        let mut inherited: Vec<Vec<Copy>> = vec![Vec::new(); to.len()];
        // What each table's own CHECKs pass down, with whatever reached their
        // merged copies from above; worked out when the table's turn comes,
        // or on demand for a parent the loop skips.
        let mut own_checks: Vec<Option<Vec<Copy>>> = vec![None; to.len()];
        for &i in &order {
            let table = &to[i];
            if table.hash.is_none() {
                continue;
            }
            let mut copies: Vec<Copy> = Vec::new();
            for &p in &parents[i] {
                let own = own_checks[p].get_or_insert_with(|| own_copies(&to[p]));
                for copy in own.iter().cloned().chain(inherited[p].iter().cloned()) {
                    match copies.iter_mut().find(|c| c.name == copy.name) {
                        Some(entry) => {
                            entry.enforced |= copy.enforced;
                            entry.touched |= copy.touched;
                            entry.reset_here |= copy.reset_here;
                            entry.cleared |= copy.cleared;
                        }
                        None => copies.push(copy),
                    }
                }
            }
            // A merged local copy is the table's own alter's, except that a
            // `NOT ENFORCED` statement of this pass above recursed into it:
            // one TO keeps ENFORCED is set again, which recurses too. What
            // reached it goes on to the table's children with its own copy.
            let mut own = own_copies(table);
            for copy in &copies {
                let Some(local) = local_check(table, &copy.name) else {
                    continue;
                };
                let reenforce = at_19 && copy.cleared && local.is_enforced;
                if reenforce {
                    emit(table, &copy.name, "enforced");
                }
                if let Some(entry) = own.iter_mut().find(|c| c.name == copy.name) {
                    entry.touched |= copy.touched || reenforce;
                    entry.reset_here |= copy.reset_here || reenforce;
                    entry.cleared = copy.cleared && !reenforce;
                }
            }
            own_checks[i] = Some(own);
            copies.retain(|c| local_check(table, &c.name).is_none());
            let fate = fate(table);
            for copy in &mut copies {
                let recorded_to = table.enforced_inherited_checks.contains(&copy.name);
                let mut statement = match &fate {
                    Fate::Kept(from) => {
                        let recorded_from = from.enforced_inherited_checks.contains(&copy.name);
                        if recorded_to {
                            (copy.touched || !recorded_from).then_some("enforced")
                        } else {
                            (!copy.enforced && recorded_from).then_some("not enforced")
                        }
                    }
                    // A copy the holders' re-add around this table's
                    // `INHERIT` created follows them, like a new table's.
                    Fate::Attached => (recorded_to
                        && (copy.reset_here
                            || inherited_recreates
                                .get(&(table.schema.clone(), table.name.clone()))
                                .is_some_and(|plan| {
                                    plan.inherit_readds.iter().any(|c| c.name == copy.name)
                                })))
                    .then_some("enforced"),
                    Fate::Created => recorded_to.then_some("enforced"),
                };
                // A copy TO keeps ENFORCED that a `NOT ENFORCED` statement
                // above reached is set again, whatever else holds.
                if statement.is_none() && copy.cleared && (copy.enforced || recorded_to) {
                    statement = Some("enforced");
                }
                copy.enforced |= recorded_to;
                copy.touched |= !matches!(fate, Fate::Kept(_));
                let Some(statement) = statement else {
                    continue;
                };
                // Everything beneath follows this statement, which only a
                // PostgreSQL 19 server runs.
                if at_19 {
                    copy.touched = true;
                    copy.reset_here = true;
                    copy.cleared = statement == "not enforced";
                }
                emit(table, &copy.name, statement);
            }
            inherited[i] = copies;
        }
        (script, warnings)
    }

    /// Look up a table's entry in a [`Self::table_alter_caps`] map.
    fn caps_for(caps: &HashMap<(String, String), TableAlterCaps>, table: &Table) -> TableAlterCaps {
        caps.get(&(table.schema.clone(), table.name.clone()))
            .copied()
            .unwrap_or_default()
    }

    /// Whether `table` is listed by name in a FROM-side `FOR TABLE`
    /// publication (`pg_publication_rel`), the membership PostgreSQL checks
    /// before `SET EXPRESSION` on a VIRTUAL column. Entries are
    /// `schema.table` as `quote_ident` renders them — the same form as the
    /// table's own fields; anything following the name (a column list or row
    /// filter) is tolerated.
    fn is_explicit_publication_member(&self, table: &Table) -> bool {
        let qualified = format!("{}.{}", table.schema, table.name);
        self.from
            .publications
            .iter()
            .filter(|p| !p.all_tables)
            .flat_map(|p| p.tables.iter())
            .any(|entry| {
                entry == &qualified
                    || entry
                        .strip_prefix(qualified.as_str())
                        .is_some_and(|rest| rest.starts_with(' ') || rest.starts_with('('))
            })
    }

    /// What the migration is about to drop, rebuild or retype underneath the
    /// FROM objects that depend on relations and routines, worked out once, by
    /// `drop_views`, before any of it runs. Three kinds of dependent read it,
    /// each of which has to be out of the way first: views
    /// ([`Comparer::dependent_view_keys`], which look at the tables only),
    /// routines with a SQL-standard body
    /// (`Comparer::sql_body_routine_predrop_seeds`) and rules
    /// (`Comparer::plan_rule_drops`). Every relation and column key is
    /// normalized the way `normalized_view_key` / `normalized_view_reference`
    /// produce them.
    ///
    /// A typed table's column whose type differs counts as retyped only when
    /// the type's `ALTER ATTRIBUTE … TYPE … CASCADE` retypes it (see
    /// `Comparer::unretyped_typed_columns`): no table alter retypes it.
    fn relation_changes(&self) -> RelationChanges {
        // Same capabilities the table alter loop uses, so a column counts as
        // dropped here exactly when that loop drops and re-adds it.
        let alter_caps = self.table_alter_caps();

        // Build O(1) lookup for target tables
        let to_table_map: HashMap<(&str, &str), usize> = self
            .to
            .tables
            .iter()
            .enumerate()
            .map(|(i, t)| ((t.schema.as_str(), t.name.as_str()), i))
            .collect();

        // Tables that stop existing in the form the view names them, so every
        // view over one has to go regardless of columns.
        let mut wholesale_tables: HashSet<String> = HashSet::new();
        // Columns whose drop or retype PostgreSQL would refuse while a
        // dependent view exists.
        let mut changed_columns: HashSet<String> = HashSet::new();
        // Every table touched at all — the historical, table-level test, kept
        // for views from dumps that carry no column dependencies.
        let mut altered_or_dropped: HashSet<String> = HashSet::new();
        // The historical regular-view answer, likewise kept for those dumps.
        let mut legacy_dependent: HashSet<String> = HashSet::new();
        // Partitions that go with a recreated ancestor's `DROP TABLE`, changed
        // or not.
        let families = self.recreated_families();
        // Typed-table columns no statement retypes, however they differ.
        let unretyped_typed_columns = self.unretyped_typed_columns();
        let normalise = |s: &str| s.to_lowercase().replace('"', "");

        for table in &self.from.tables {
            let matching_table = to_table_map
                .get(&(table.schema.as_str(), table.name.as_str()))
                .map(|&idx| &self.to.tables[idx]);

            let will_be_dropped = matching_table.is_none() && self.use_drop;
            let will_be_altered = matching_table
                .map(|target| Self::hashes_differ(&target.hash, &table.hash))
                .unwrap_or(false);
            // Only with `use_drop`: otherwise the ancestor's DROP TABLE is
            // commented out and the partition stays.
            let goes_with_ancestor = self.use_drop
                && families
                    .members
                    .contains_key(&Self::table_key(&table.schema, &table.name));

            if !will_be_dropped && !will_be_altered && !goes_with_ancestor {
                continue;
            }

            let table_key = Self::normalized_view_key(&table.schema, &table.name);
            altered_or_dropped.insert(table_key.clone());

            for column in &table.columns {
                if let Some(related_views) = &column.related_views {
                    for view_ref in related_views {
                        let normalized = Self::normalized_view_reference(view_ref);
                        if !normalized.is_empty() {
                            legacy_dependent.insert(normalized);
                        }
                    }
                }
            }

            match matching_table {
                // Gone, or rebuilt from scratch: nothing that reads it survives.
                None => {
                    wholesale_tables.insert(table_key);
                }
                Some(_) if goes_with_ancestor => {
                    wholesale_tables.insert(table_key);
                }
                Some(target) if table.will_be_dropped_and_recreated(target) => {
                    wholesale_tables.insert(table_key);
                }
                Some(target) => {
                    let generated_caps = Self::caps_for(&alter_caps, table).generated;
                    // Generated columns dropped out of the way of a retype
                    // (`Table::generated_columns_in_the_way_of_retype`).
                    let in_the_way = table.generated_columns_in_the_way_of_retype(
                        target,
                        self.use_drop,
                        generated_caps,
                    );
                    let to_columns: HashMap<&str, &TableColumn> = target
                        .columns
                        .iter()
                        .map(|c| (c.name.as_str(), c))
                        .collect();
                    for column in &table.columns {
                        let forbidden = match to_columns.get(column.name.as_str()) {
                            // Dropped.
                            None => true,
                            // Retyped, or dropped and re-added by the alter
                            // path (a generated-column change that SET
                            // EXPRESSION cannot make on this server). Anything
                            // else about a column — its nullability, default,
                            // comment, storage, or a SET EXPRESSION — can be
                            // altered underneath a view that reads it.
                            Some(target_column) => {
                                (column.type_differs(target_column)
                                    && !unretyped_typed_columns.contains(&(
                                        normalise(&table.schema),
                                        normalise(&table.name),
                                        normalise(&column.name),
                                    )))
                                    || target_column
                                        .would_drop_and_re_add_with_caps(column, generated_caps)
                                    || in_the_way.contains(column.name.as_str())
                            }
                        };
                        if forbidden {
                            changed_columns.insert(Self::normalized_view_reference(&format!(
                                "{}.{}.{}",
                                table.schema, table.name, column.name
                            )));
                        }
                    }
                }
            }
        }

        // Beyond the tables. A sequence the migration drops is gone as surely
        // as a dropped table.
        let to_sequences: HashSet<(&str, &str)> = self
            .to
            .sequences
            .iter()
            .map(|s| (s.schema.as_str(), s.name.as_str()))
            .collect();
        let dropped_sequences: HashSet<String> = self
            .from
            .sequences
            .iter()
            .filter(|s| !to_sequences.contains(&(s.schema.as_str(), s.name.as_str())))
            .map(|s| Self::normalized_view_key(&s.schema, &s.name))
            .collect();
        let (going_foreign_tables, changed_foreign_columns) = self.foreign_table_changes();

        // Routines whose `DROP ... CASCADE` silently takes their dependents
        // along: FROM-only ones, and ones dropped and re-created.
        let to_routines: HashMap<(&str, &str, &str), &Routine> = self
            .to
            .routines
            .iter()
            .map(|r| {
                (
                    (r.schema.as_str(), r.name.as_str(), r.arguments.as_str()),
                    r,
                )
            })
            .collect();
        let mut dropped_routines: HashSet<String> = HashSet::new();
        let mut recreated_routines: HashSet<String> = HashSet::new();
        for routine in &self.from.routines {
            match to_routines.get(&(
                routine.schema.as_str(),
                routine.name.as_str(),
                routine.arguments.as_str(),
            )) {
                None => {
                    dropped_routines.insert(routine.dependency_key());
                }
                Some(to) if Self::routine_is_recreated(routine, to) => {
                    recreated_routines.insert(routine.dependency_key());
                }
                Some(_) => {}
            }
        }

        // Operators dropped and re-created because their definition changed.
        let to_operators: HashMap<String, &crate::dump::operator::Operator> =
            self.to.operators.iter().map(|op| (op.key(), op)).collect();
        let rebuilt_operators: HashSet<String> = self
            .from
            .operators
            .iter()
            .filter(|from| {
                to_operators
                    .get(&from.key())
                    .is_some_and(|to| from.definition_differs(to))
            })
            .map(|op| op.key())
            .collect();

        RelationChanges {
            wholesale_tables,
            changed_columns,
            altered_or_dropped,
            legacy_dependent,
            dropped_sequences,
            going_foreign_tables,
            changed_foreign_columns,
            dropped_routines,
            recreated_routines,
            rebuilt_operators,
        }
    }

    /// What `compare_foreign_tables` is about to do to the FROM foreign tables,
    /// for `Comparer::relation_changes`: the foreign tables it drops — FROM-only
    /// ones, and ones whose server changes or is created again (see
    /// `Comparer::plan_server_recreates`), which are dropped and recreated;
    /// both only with `use_drop`, since otherwise those drops are commented out
    /// — and the columns it drops or retypes on the others.
    fn foreign_table_changes(&self) -> (HashSet<String>, HashSet<String>) {
        let to_foreign_tables: HashMap<(&str, &str), &crate::dump::foreign_table::ForeignTable> =
            self.to
                .foreign_tables
                .iter()
                .map(|ft| ((ft.schema.as_str(), ft.name.as_str()), ft))
                .collect();

        let mut going = HashSet::new();
        let mut changed_columns = HashSet::new();
        for ft in &self.from.foreign_tables {
            match to_foreign_tables.get(&(ft.schema.as_str(), ft.name.as_str())) {
                None => {
                    if self.use_drop {
                        going.insert(Self::normalized_view_key(&ft.schema, &ft.name));
                    }
                }
                Some(target)
                    if target.server != ft.server
                        || self.recreated_servers.contains(&ft.server) =>
                {
                    if self.use_drop {
                        going.insert(Self::normalized_view_key(&ft.schema, &ft.name));
                    }
                }
                Some(target) => {
                    for column in &ft.columns {
                        let dropped_or_retyped = target
                            .columns
                            .iter()
                            .find(|c| c.name == column.name)
                            .is_none_or(|c| c.data_type != column.data_type);
                        if dropped_or_retyped {
                            changed_columns.insert(Self::normalized_view_reference(&format!(
                                "{}.{}.{}",
                                ft.schema, ft.name, column.name
                            )));
                        }
                    }
                }
            }
        }
        (going, changed_columns)
    }

    /// `(schema, name)` of `routine`, quote-stripped and lowercased: the form
    /// `Comparer::definition_references_any` and
    /// `Comparer::aggregate_references_function` match.
    fn unquoted_routine_name(routine: &Routine) -> (String, String) {
        (
            routine.schema.to_lowercase().replace('"', ""),
            routine.name.to_lowercase().replace('"', ""),
        )
    }

    /// FROM routines with a SQL-standard body that must be dropped before the
    /// relation DDL runs, as indices into `self.from.routines`.
    ///
    /// Unlike a string body, a SQL-standard body (`BEGIN ATOMIC … END` /
    /// `RETURN expr`) is a hard `pg_depend` dependent of every relation,
    /// column, routine and operator it uses ([`Routine::sql_body_dependencies`]).
    /// PostgreSQL therefore refuses to drop a table or sequence it reads, to
    /// drop or retype a column it reads, or to rebuild the table — `cannot
    /// drop … because other objects depend on it`, `cannot alter type of a
    /// column used by a function or procedure` — and a `DROP … CASCADE` of a
    /// routine or operator it calls silently takes it along. Each of those
    /// left a migration that failed, or needed a second run, or never
    /// converged at all.
    ///
    /// A routine is a seed when its body uses, per `changes`
    /// (`Comparer::relation_changes`), a table the table phase drops or
    /// rebuilds, a column it drops, retypes or re-adds (both exactly as
    /// `Comparer::dependent_view_keys` decides them for views), a sequence or
    /// foreign table the migration drops (a foreign table whose server changes
    /// is dropped and recreated), a foreign-table column it drops or retypes,
    /// a routine it drops and re-creates (`Comparer::routine_is_recreated`),
    /// or an operator it drops and re-creates. `drop_views` adds whatever else
    /// has to go with them — a routine reading a view being dropped among them
    /// — and drops them all, dependents first; the routine phase re-creates
    /// the ones TO still has.
    ///
    /// A routine whose **argument default** calls a routine the routine phase
    /// drops with `CASCADE` is not a seed: that `CASCADE` runs after the table
    /// phase, which may create objects calling the routine, so it is left to
    /// the routine phase (`Comparer::plan_cascade_victims`). One whose default
    /// calls a routine dropped *here* goes with it, as the expansion in
    /// `Comparer::plan_view_drops` works out.
    ///
    /// Empty with `use_drop` off (the table drops are commented out then too)
    /// and for dumps without SQL-standard bodies, so their output is
    /// unchanged.
    fn sql_body_routine_predrop_seeds(&self, changes: &RelationChanges) -> Vec<usize> {
        if !self.use_drop || !self.from.routines.iter().any(|r| r.is_sql_body) {
            return Vec::new();
        }
        self.from
            .routines
            .iter()
            .enumerate()
            .filter(|(_, routine)| {
                let deps = &routine.sql_body_dependencies;
                routine.is_sql_body
                    && (deps.relations.iter().any(|rel| changes.relation_goes(rel))
                        || deps.columns.iter().any(|col| changes.column_changes(col))
                        || deps
                            .routines
                            .iter()
                            .any(|key| changes.recreated_routines.contains(key))
                        || deps
                            .operators
                            .iter()
                            .any(|key| changes.rebuilt_operators.contains(key)))
            })
            .map(|(idx, _)| idx)
            .collect()
    }

    /// The FROM routines, as `(schema, name, arguments)`, that the routine
    /// phase's `DROP … CASCADE` of a routine takes along although TO keeps
    /// them, as `Comparer::cascade_takes` finds them: an argument default
    /// calls the dropped routine, a SQL-standard body calls it, or it is an
    /// aggregate built on it — or on another routine taken the same way.
    ///
    /// The routine phase drops FROM-only routines and the ones it re-creates
    /// (`Comparer::routine_is_recreated`), unless `drop_views` drops them
    /// first (`predropping`, keyed the same way). What they take is
    /// re-created right after the routine it calls, with its privileges, and
    /// so are the objects calling it. A SQL-standard body calling a
    /// re-created routine is dropped by `drop_views` already (see
    /// `Comparer::sql_body_routine_predrop_seeds`), but one whose argument
    /// default calls it is left to the routine phase: dropping it ahead of
    /// the table phase would make every table object calling it fail to
    /// create there. Matched by name where the catalog gives no more, as
    /// every other `CASCADE` dependent is. Empty with `use_drop` off, which
    /// comments those drops out.
    fn plan_cascade_victims(
        &self,
        changes: &RelationChanges,
        predropping: &HashSet<(String, String, String)>,
    ) -> HashSet<(String, String, String)> {
        let mut victims: HashSet<(String, String, String)> = HashSet::new();
        if !self.use_drop {
            return victims;
        }
        let key_of = |r: &Routine| (r.schema.clone(), r.name.clone(), r.arguments.clone());
        let dropped: Vec<&Routine> = self
            .from
            .routines
            .iter()
            .filter(|r| {
                let key = r.dependency_key();
                (changes.dropped_routines.contains(&key)
                    || changes.recreated_routines.contains(&key))
                    && !predropping.contains(&key_of(r))
            })
            .collect();
        if dropped.is_empty() {
            return victims;
        }
        let mut names: HashSet<(String, String)> = dropped
            .iter()
            .map(|r| Self::unquoted_routine_name(r))
            .collect();
        let mut keys: HashSet<String> = dropped.iter().map(|r| r.dependency_key()).collect();
        let to_keys: HashSet<(&str, &str, &str)> = self
            .to
            .routines
            .iter()
            .map(|r| (r.schema.as_str(), r.name.as_str(), r.arguments.as_str()))
            .collect();
        loop {
            let reached: Vec<&Routine> = self
                .from
                .routines
                .iter()
                .filter(|r| {
                    let key = key_of(r);
                    !victims.contains(&key)
                        && !predropping.contains(&key)
                        && to_keys.contains(&(
                            r.schema.as_str(),
                            r.name.as_str(),
                            r.arguments.as_str(),
                        ))
                        && !changes.recreated_routines.contains(&r.dependency_key())
                        && Self::cascade_takes(r, &names, &keys)
                })
                .collect();
            if reached.is_empty() {
                return victims;
            }
            for routine in reached {
                victims.insert(key_of(routine));
                names.insert(Self::unquoted_routine_name(routine));
                keys.insert(routine.dependency_key());
            }
        }
    }

    /// Whether a `DROP … CASCADE` of the routines `names` (quote-stripped,
    /// lowercased) / `keys` ([`Routine::dependency_key`]s) takes `routine`
    /// along: an argument default of it calls one, its SQL-standard body
    /// calls one (exactly that overload, per `pg_depend`), or it is an
    /// aggregate built on one.
    fn cascade_takes(
        routine: &Routine,
        names: &HashSet<(String, String)>,
        keys: &HashSet<String>,
    ) -> bool {
        Self::defaults_call_any(routine, names)
            || routine
                .sql_body_dependencies
                .routines
                .iter()
                .any(|key| keys.contains(key))
            || (routine.aggregate_info.is_some()
                && names.iter().any(|(schema, name)| {
                    Self::aggregate_references_function(routine, schema, name)
                }))
    }

    /// `routine` and the routines a `DROP … CASCADE` of it takes along
    /// (`cascade_victim_routines`, through `Comparer::cascade_takes`),
    /// transitively.
    fn cascade_reach<'a>(&'a self, routine: &'a Routine) -> Vec<&'a Routine> {
        let mut reach = vec![routine];
        if self.cascade_victim_routines.is_empty() {
            return reach;
        }
        let mut names = HashSet::from([Self::unquoted_routine_name(routine)]);
        let mut keys = HashSet::from([routine.dependency_key()]);
        loop {
            let reached: Vec<&Routine> = self
                .from
                .routines
                .iter()
                .filter(|r| {
                    self.cascade_victim_routines.contains(&(
                        r.schema.clone(),
                        r.name.clone(),
                        r.arguments.clone(),
                    )) && !reach.iter().any(|other| std::ptr::eq(*other, *r))
                        && Self::cascade_takes(r, &names, &keys)
                })
                .collect();
            if reached.is_empty() {
                return reach;
            }
            for r in reached {
                names.insert(Self::unquoted_routine_name(r));
                keys.insert(r.dependency_key());
                reach.push(r);
            }
        }
    }

    /// Whether an argument default of `routine` calls one of `names`
    /// (quote-stripped, lowercased `(schema, name)`, the form
    /// `Comparer::definition_references_any` matches). PostgreSQL makes the
    /// routine itself depend on what its defaults call, so a `DROP … CASCADE`
    /// of one of those takes the routine along.
    fn defaults_call_any(routine: &Routine, names: &HashSet<(String, String)>) -> bool {
        !names.is_empty()
            && routine
                .arguments_defaults
                .as_deref()
                .is_some_and(|defaults| Self::definition_references_any(defaults, names))
    }

    /// Whether `reference` — a function as a catalog column spells it:
    /// `schema.name`, `schema.name(arguments)`, or a bare `name` for one on the
    /// `search_path`, quoted or not — names the routine `(schema_lc, name_lc)`
    /// (quote-stripped, lowercased).
    fn reference_names_routine(reference: &str, schema_lc: &str, name_lc: &str) -> bool {
        let lower = reference.to_lowercase().replace('"', "");
        let bare = lower.split('(').next().unwrap_or_default().trim();
        bare == name_lc || bare == format!("{schema_lc}.{name_lc}")
    }

    /// What a `DROP … CASCADE` of the FROM routine `routine` ahead of the
    /// relation DDL takes along that pgc cannot re-create afterwards, one
    /// description each, empty when there is nothing: a table whose partition
    /// key calls it (the table goes, with its data), a range type using it as
    /// its subtype difference or canonical function, or a base type using it
    /// for input, output or another support function (the type goes, with
    /// every column of it), a foreign-data wrapper using it as handler,
    /// validator or connection function (with its servers, user mappings and
    /// foreign tables), and an operator or cast built on it — which pgc could
    /// create again, but not the views, constraints, indexes and routines
    /// using the operator or cast, which go with it and are nowhere near the
    /// routine's name.
    ///
    /// Everything else a routine's `CASCADE` can reach is put back:
    /// `drop_views` plans the views, rules and routines, and
    /// `recreate_routine_dependents` the rest. That includes what goes with a
    /// generated column calling the routine (see
    /// `Comparer::cascade_readded_columns`): the views, rules and routines
    /// reading it, its privileges, the triggers naming it, a partition's own
    /// objects on it, and the publication rows whose column list or row
    /// filter uses it — a row filter cannot call a user-defined function, but
    /// it can read a generated column that does. Event triggers and trigger
    /// functions cannot be involved: neither can be a SQL-language routine.
    fn unrestorable_cascade_dependents(&self, routine: &Routine) -> Vec<String> {
        self.cascade_blockers(&[routine], CascadeMoment::AheadOfTables)
    }

    /// What the `DROP … CASCADE` of `routines` (a routine and whatever its
    /// `CASCADE` takes along with it, see `Comparer::cascade_reach`) takes at
    /// `moment` that pgc cannot re-create, as
    /// `Comparer::unrestorable_cascade_dependents` lists it.
    ///
    /// Ahead of the tables the FROM dump's objects are in place. In the
    /// routine phase `compare_tables`, `compare_types` and `compare_fdws`
    /// have made the tables, types and foreign-data wrappers the TO dump's,
    /// while operators and casts are only compared afterwards: the FROM ones
    /// TO keeps unchanged are the ones nothing would re-create (a changed
    /// one is rebuilt by `compare_operators` / `compare_casts` anyway).
    fn cascade_blockers(&self, routines: &[&Routine], moment: CascadeMoment) -> Vec<String> {
        let names: HashSet<(String, String)> = routines
            .iter()
            .map(|r| Self::unquoted_routine_name(r))
            .collect();
        let keys: HashSet<String> = routines.iter().map(|r| r.dependency_key()).collect();
        let names_it = |reference: &str| {
            names
                .iter()
                .any(|(schema, name)| Self::reference_names_routine(reference, schema, name))
        };
        let (tables, types, fdws) = match moment {
            CascadeMoment::AheadOfTables => (
                &self.from.tables,
                &self.from.types,
                &self.from.foreign_data_wrappers,
            ),
            CascadeMoment::RoutinePhase => (
                &self.to.tables,
                &self.to.types,
                &self.to.foreign_data_wrappers,
            ),
        };
        let kept_by_to = |unchanged: &dyn Fn() -> bool| match moment {
            CascadeMoment::AheadOfTables => true,
            CascadeMoment::RoutinePhase => unchanged(),
        };
        let mut found = Vec::new();
        for table in tables {
            if table
                .partition_key
                .as_deref()
                .is_some_and(|key| Self::definition_references_any(key, &names))
            {
                found.push(format!(
                    "the partition key of table {}.{}",
                    table.schema, table.name
                ));
            }
        }
        for pg_type in types {
            let functions: Vec<&str> = match pg_type.typtype as u8 as char {
                'r' => [&pg_type.range_subdiff, &pg_type.range_canonical]
                    .into_iter()
                    .filter_map(|f| f.as_deref())
                    .collect(),
                'b' => [
                    Some(&pg_type.typinput),
                    Some(&pg_type.typoutput),
                    pg_type.typreceive.as_ref(),
                    pg_type.typsend.as_ref(),
                    pg_type.typmodin.as_ref(),
                    pg_type.typmodout.as_ref(),
                    pg_type.typanalyze.as_ref(),
                    pg_type.typsubscript.as_ref(),
                ]
                .into_iter()
                .flatten()
                .map(String::as_str)
                .collect(),
                _ => Vec::new(),
            };
            if functions.into_iter().any(names_it) {
                let kind = if (pg_type.typtype as u8 as char) == 'r' {
                    "range type"
                } else {
                    "base type"
                };
                found.push(format!("{kind} {}.{}", pg_type.schema, pg_type.typname));
            }
        }
        for fdw in fdws {
            if [&fdw.handler_func, &fdw.validator_func, &fdw.connection_func]
                .into_iter()
                .filter_map(|f| f.as_deref())
                .any(names_it)
            {
                found.push(format!("foreign-data wrapper {}", fdw.name));
            }
        }
        for op in &self.from.operators {
            let unchanged = || {
                self.to
                    .operators
                    .iter()
                    .any(|to| to.key() == op.key() && !op.definition_differs(to))
            };
            if (names_it(&op.procedure)
                || op.restrict.as_deref().is_some_and(names_it)
                || op.join.as_deref().is_some_and(names_it))
                && kept_by_to(&unchanged)
            {
                found.push(format!("operator {}", op.key()));
            }
        }
        for cast in &self.from.casts {
            let unchanged = || {
                self.to.casts.iter().any(|to| {
                    to.source_type == cast.source_type
                        && to.target_type == cast.target_type
                        && to.hash == cast.hash
                })
            };
            if cast
                .function_name
                .as_deref()
                .is_some_and(|f| keys.contains(f))
                && kept_by_to(&unchanged)
            {
                found.push(format!(
                    "cast ({} as {})",
                    cast.source_type, cast.target_type
                ));
            }
        }
        found
    }

    /// What the table changes need the FROM routine `routine` for while
    /// `drop_views` would have it dropped, one description each: a generated
    /// column calling it that they add, or change to call it (on a new table
    /// too), and on a table FROM already has, a new column whose default calls
    /// it, or whose type is a domain whose default calls it — that default
    /// is detached for the drop (`Comparer::domain_default_detach`). The
    /// `ADD COLUMN` fills the existing rows with the default, so it can
    /// neither run without it nor be put off until the routine is back.
    ///
    /// Every other table object calling the routine is kept out of the table
    /// phase and created after the routines (`Comparer::enter_table_phase_view`).
    /// A partition's columns are its parent's.
    fn table_phase_needs(&self, routine: &Routine) -> Vec<String> {
        let names = HashSet::from([Self::unquoted_routine_name(routine)]);
        let calls_it =
            |text: Option<&str>| text.is_some_and(|t| Self::definition_references_any(t, &names));
        let from_tables: HashMap<(&str, &str), &Table> = self
            .from
            .tables
            .iter()
            .map(|t| ((t.schema.as_str(), t.name.as_str()), t))
            .collect();
        let unquoted = |s: &str| s.replace('"', "");
        let detached: HashSet<(String, String)> = self
            .domain_default_detach(routine)
            .1
            .iter()
            .map(|(schema, typname)| (unquoted(schema), unquoted(typname)))
            .collect();
        let mut found = Vec::new();
        for to_table in self.to.tables.iter().filter(|t| t.partition_of.is_none()) {
            let from_table = from_tables
                .get(&(to_table.schema.as_str(), to_table.name.as_str()))
                .copied();
            for column in &to_table.columns {
                let from_column =
                    from_table.and_then(|t| t.columns.iter().find(|c| c.name == column.name));
                let label = format!("{}.{}.{}", to_table.schema, to_table.name, column.name);
                if column.is_generated.eq_ignore_ascii_case("ALWAYS") {
                    let kept = from_column.is_some_and(|fc| {
                        fc.is_generated.eq_ignore_ascii_case("ALWAYS")
                            && fc.generation_expression == column.generation_expression
                    });
                    if !kept && calls_it(column.generation_expression.as_deref()) {
                        found.push(format!("generated column {label}"));
                    }
                    continue;
                }
                if from_table.is_none() || from_column.is_some() {
                    continue;
                }
                if calls_it(column.column_default.as_deref()) {
                    found.push(format!("the default of new column {label}"));
                } else if column.column_default.is_none()
                    && let (Some(domain_schema), Some(domain_name)) =
                        (&column.domain_schema, &column.domain_name)
                    && detached.contains(&(unquoted(domain_schema), unquoted(domain_name)))
                {
                    found.push(format!(
                        "the default of domain {domain_schema}.{domain_name} for new column {label}"
                    ));
                }
            }
        }
        found
    }

    /// Whether FROM routine `routine` depends on the FROM routine `callee` in
    /// a way that makes a `DROP … CASCADE` of `callee` take `routine` along:
    /// its SQL-standard body calls it (exactly that overload, per
    /// `pg_depend`), it is an aggregate built on it, or one of its argument
    /// defaults calls it (the last two matched by name).
    fn routine_depends_on_routine(routine: &Routine, callee: &Routine) -> bool {
        let (callee_schema, callee_name) = Self::unquoted_routine_name(callee);
        routine
            .sql_body_dependencies
            .routines
            .contains(&callee.dependency_key())
            || Self::aggregate_references_function(routine, &callee_schema, &callee_name)
            || Self::defaults_call_any(routine, &HashSet::from([(callee_schema, callee_name)]))
    }

    /// FROM routines, as indices into `from.routines`, that a `DROP …
    /// CASCADE` takes whether or not `drop_views` drops them first: the
    /// routine phase drops the FROM-only and re-created ones
    /// (`changes.dropped_routines`, `changes.recreated_routines`),
    /// `compare_operators` drops a rebuilt operator with `CASCADE`, taking the
    /// SQL-standard bodies using it, and everything depending on one of those
    /// (`Comparer::routine_depends_on_routine`) goes with it. Keeping such a
    /// routine back would only lose it: nothing re-creates it then.
    fn routines_cascaded_anyway(&self, changes: &RelationChanges) -> HashSet<usize> {
        let routines = &self.from.routines;
        let mut cascaded: HashSet<usize> = routines
            .iter()
            .enumerate()
            .filter(|(_, r)| {
                let key = r.dependency_key();
                changes.dropped_routines.contains(&key)
                    || changes.recreated_routines.contains(&key)
                    || (r.is_sql_body
                        && r.sql_body_dependencies
                            .operators
                            .iter()
                            .any(|op| changes.rebuilt_operators.contains(op)))
            })
            .map(|(idx, _)| idx)
            .collect();
        loop {
            let keys: HashSet<String> = cascaded
                .iter()
                .map(|&c| routines[c].dependency_key())
                .collect();
            let names: HashSet<(String, String)> = cascaded
                .iter()
                .map(|&c| Self::unquoted_routine_name(&routines[c]))
                .collect();
            let reached: Vec<usize> = routines
                .iter()
                .enumerate()
                .filter(|(idx, routine)| {
                    !cascaded.contains(idx)
                        && (routine
                            .sql_body_dependencies
                            .routines
                            .iter()
                            .any(|key| keys.contains(key))
                            || (routine.aggregate_info.is_some()
                                && names.iter().any(|(schema, name)| {
                                    Self::aggregate_references_function(routine, schema, name)
                                }))
                            || Self::defaults_call_any(routine, &names))
                })
                .map(|(idx, _)| idx)
                .collect();
            if reached.is_empty() {
                return cascaded;
            }
            cascaded.extend(reached);
        }
    }

    /// Among `routine_candidates` — indices into `from.routines` that
    /// `plan_view_drops` is about to drop with `CASCADE` — the ones it must
    /// leave alone, with the reason: each whose `CASCADE` would take along
    /// something pgc cannot re-create (`Comparer::unrestorable_cascade_dependents`),
    /// each the table changes need in place (`Comparer::table_phase_needs`),
    /// and every candidate such a routine depends on
    /// (`Comparer::routine_depends_on_routine`), since dropping that one with
    /// `CASCADE` would reach it too.
    ///
    /// Such a routine stays, and so does the change it is in the way of fail
    /// on PostgreSQL's own dependency check: the answer of pgc 1.0.x, which
    /// never dropped these routines ahead of anything — loud, rather than a
    /// migration that quietly loses the table, type or operator, or fills a
    /// new column differently. A routine a later `CASCADE` takes anyway
    /// (`Comparer::routines_cascaded_anyway`) is not kept back: that takes
    /// what it carries regardless, and dropping it here at least re-creates
    /// the routine.
    fn routines_to_keep_back(
        &self,
        routine_candidates: &[usize],
        changes: &RelationChanges,
    ) -> Vec<(usize, String)> {
        let blocked: Vec<(usize, String, KeepBackCause)> = routine_candidates
            .iter()
            .filter_map(|&idx| {
                let routine = &self.from.routines[idx];
                let dependents = self.unrestorable_cascade_dependents(routine);
                if !dependents.is_empty() {
                    return Some((idx, dependents.join(", "), KeepBackCause::Unrestorable));
                }
                let needs = self.table_phase_needs(routine);
                (!needs.is_empty()).then(|| (idx, needs.join(", "), KeepBackCause::NeededByTables))
            })
            .collect();
        if blocked.is_empty() {
            return Vec::new();
        }
        let cascaded_anyway = self.routines_cascaded_anyway(changes);
        let mut queue: VecDeque<(usize, String, KeepBackCause)> = blocked
            .into_iter()
            .filter(|(idx, _, _)| !cascaded_anyway.contains(idx))
            .collect();
        let mut kept: Vec<(usize, String)> = Vec::new();
        while let Some((idx, what, cause)) = queue.pop_front() {
            if kept.iter().any(|(k, _)| *k == idx) {
                continue;
            }
            let routine = &self.from.routines[idx];
            let signature = format!("{}.{}({})", routine.schema, routine.name, routine.arguments);
            kept.push((idx, cause.reason(&what)));
            // A routine not cascaded anyway depends on none that is, so what
            // it depends on is never in `cascaded_anyway` either.
            for &other in routine_candidates {
                if other == idx || kept.iter().any(|(k, _)| *k == other) {
                    continue;
                }
                if Self::routine_depends_on_routine(routine, &self.from.routines[other]) {
                    queue.push_back((
                        other,
                        format!("{what}, through routine {signature},"),
                        cause,
                    ));
                }
            }
        }
        kept
    }

    /// Quote-stripped, lowercased `(schema, name)` of the routines
    /// `drop_views` dropped ahead of the relation DDL (`predropped_routines`).
    fn predropped_routine_names(&self) -> HashSet<(String, String)> {
        self.from
            .routines
            .iter()
            .filter(|r| {
                self.predropped_routines.contains(&(
                    r.schema.clone(),
                    r.name.clone(),
                    r.arguments.clone(),
                ))
            })
            .map(Self::unquoted_routine_name)
            .collect()
    }

    /// A copy of `table` without what calls one of `names`: its CHECK and
    /// EXCLUDE constraints, indexes and policies calling one, and the
    /// defaults of its columns that are not generated — except, when
    /// `existing` is given, the default of a column `existing` does not have:
    /// adding such a column fills the table's rows with its default, which
    /// cannot wait (`Comparer::table_phase_needs` keeps the routine instead).
    /// `None` when nothing in `table` calls one.
    fn table_without_callers(
        table: &Table,
        existing: Option<&Table>,
        names: &HashSet<(String, String)>,
    ) -> Option<Table> {
        let calls =
            |text: Option<&str>| text.is_some_and(|t| Self::definition_references_any(t, names));
        let caller_constraint = |c: &TableConstraint| {
            (c.constraint_type.eq_ignore_ascii_case("check")
                || c.constraint_type.eq_ignore_ascii_case("exclude"))
                && calls(c.definition.as_deref())
        };
        let caller_default = |c: &TableColumn| {
            !c.is_generated.eq_ignore_ascii_case("ALWAYS")
                && calls(c.column_default.as_deref())
                && existing.is_none_or(|t| t.columns.iter().any(|fc| fc.name == c.name))
        };
        let caller_policy =
            |p: &TablePolicy| calls(p.using_clause.as_deref()) || calls(p.check_clause.as_deref());
        let any = table.constraints.iter().any(caller_constraint)
            || table.indexes.iter().any(|i| calls(Some(&i.indexdef)))
            || table.columns.iter().any(caller_default)
            || table.policies.iter().any(caller_policy);
        if !any {
            return None;
        }
        let mut view = table.clone();
        view.constraints.retain(|c| !caller_constraint(c));
        view.indexes.retain(|i| !calls(Some(&i.indexdef)));
        view.policies.retain(|p| !caller_policy(p));
        for column in &mut view.columns {
            if caller_default(column) {
                column.column_default = None;
            }
        }
        Some(view)
    }

    /// A copy of `foreign_table` without the column defaults calling one of
    /// `names`, or `None` when none does. A foreign table holds no rows of its
    /// own to fill, so a new column's default can wait too.
    fn foreign_table_without_callers(
        foreign_table: &ForeignTable,
        names: &HashSet<(String, String)>,
    ) -> Option<ForeignTable> {
        let calls = |c: &crate::dump::foreign_table::ForeignTableColumn| {
            c.column_default
                .as_deref()
                .is_some_and(|d| Self::definition_references_any(d, names))
        };
        if !foreign_table.columns.iter().any(calls) {
            return None;
        }
        let mut view = foreign_table.clone();
        for column in &mut view.columns {
            if calls(column) {
                column.column_default = None;
            }
        }
        Some(view)
    }

    /// Swap in the tables and foreign tables `compare_tables`,
    /// `compare_foreign_keys` and `compare_foreign_tables` work on, when
    /// `drop_views` dropped routines ahead of them, and hand back the dumps'
    /// own for `Comparer::leave_table_phase_view`. `None`, leaving everything
    /// as it is, when nothing there calls such a routine.
    ///
    /// The `CASCADE` of those drops took the FROM objects calling one — CHECK
    /// and EXCLUDE constraints, indexes, policies, column defaults (see
    /// `Comparer::table_without_callers`) — so the table phase finds them
    /// gone, and must neither alter nor drop them. The TO objects calling one
    /// cannot be created before the routine is back; `recreate_routine_dependents`
    /// creates every one of them after the routines instead, and the table
    /// phase leaves them out. What cannot wait — a generated column, or a new
    /// column's default filling existing rows — keeps the routine from being
    /// dropped at all (`Comparer::table_phase_needs`), so it never comes here.
    ///
    /// The FROM side also leaves out the foreign keys `compare_tables` drops
    /// ahead of a recreate (`recreate_fk_predrops`) or, with `use_drop`, ahead
    /// of the `DROP TABLE` of a table TO does not have
    /// (`dropped_table_fk_predrops`), so the table phase neither drops nor
    /// alters them again, and `compare_foreign_keys` adds the TO ones back as
    /// new foreign keys.
    ///
    /// And a FROM generated column that a routine's `DROP ... CASCADE` takes
    /// and `recreate_routine_dependents` re-adds from TO (see
    /// `Comparer::cascade_readded_columns`), with its copies in the
    /// partitions, gets its TO type here: the table phase leaves the type to
    /// that re-add. An `ALTER COLUMN ... TYPE` ahead of it would rewrite the
    /// table for nothing, and PostgreSQL 14, which records the column's use
    /// of the routine on the column itself, refuses it outright ("found
    /// unexpected dependency for column").
    fn enter_table_phase_view(&mut self) -> Option<TablePhaseOriginals> {
        let names = self.predropped_routine_names();
        // Without `use_drop` the drops ahead of a `DROP TABLE` are commented
        // out, so those foreign keys stay, and the table phase has to see them.
        let dropped_table_fks: &[TableConstraint] = if self.use_drop {
            &self.dropped_table_fk_predrops
        } else {
            &[]
        };
        let predropped_fks: HashSet<(&str, &str, &str)> = self
            .recreate_fk_predrops
            .iter()
            .chain(dropped_table_fks)
            .map(|c| (c.schema.as_str(), c.table_name.as_str(), c.name.as_str()))
            .collect();
        let retypes = self.cascade_readded_retypes();
        if names.is_empty() && predropped_fks.is_empty() && retypes.is_empty() {
            return None;
        }
        let from_by_key: HashMap<(&str, &str), &Table> = self
            .from
            .tables
            .iter()
            .map(|t| ((t.schema.as_str(), t.name.as_str()), t))
            .collect();
        let predropped = |c: &TableConstraint| {
            predropped_fks.contains(&(c.schema.as_str(), c.table_name.as_str(), c.name.as_str()))
        };
        let from_view: Vec<Option<Table>> = self
            .from
            .tables
            .iter()
            .enumerate()
            .map(|(idx, t)| {
                let mut view = Self::table_without_callers(t, None, &names);
                if t.constraints.iter().any(predropped) {
                    let mut constrained = view.unwrap_or_else(|| t.clone());
                    constrained.constraints.retain(|c| !predropped(c));
                    view = Some(constrained);
                }
                if let Some(columns) = retypes.get(&idx) {
                    let mut retyped = view.unwrap_or_else(|| t.clone());
                    for (name, to_column) in columns {
                        if let Some(column) = retyped.columns.iter_mut().find(|c| &c.name == name) {
                            *column = column.with_type_of(to_column);
                        }
                    }
                    view = Some(retyped);
                }
                view
            })
            .collect();
        let to_view: Vec<Option<Table>> = self
            .to
            .tables
            .iter()
            .map(|t| {
                let existing = from_by_key
                    .get(&(t.schema.as_str(), t.name.as_str()))
                    .copied();
                Self::table_without_callers(t, existing, &names)
            })
            .collect();
        let from_foreign_view: Vec<Option<ForeignTable>> = self
            .from
            .foreign_tables
            .iter()
            .map(|t| Self::foreign_table_without_callers(t, &names))
            .collect();
        let to_foreign_view: Vec<Option<ForeignTable>> = self
            .to
            .foreign_tables
            .iter()
            .map(|t| Self::foreign_table_without_callers(t, &names))
            .collect();
        let changed = from_view.iter().chain(to_view.iter()).any(Option::is_some)
            || from_foreign_view
                .iter()
                .chain(to_foreign_view.iter())
                .any(Option::is_some);
        if !changed {
            return None;
        }
        fn merged<T: Clone>(originals: &[T], view: Vec<Option<T>>) -> Vec<T> {
            originals
                .iter()
                .zip(view)
                .map(|(original, view)| view.unwrap_or_else(|| original.clone()))
                .collect()
        }
        let from_tables = merged(&self.from.tables, from_view);
        let to_tables = merged(&self.to.tables, to_view);
        let from_foreign_tables = merged(&self.from.foreign_tables, from_foreign_view);
        let to_foreign_tables = merged(&self.to.foreign_tables, to_foreign_view);
        Some(TablePhaseOriginals {
            from_tables: std::mem::replace(&mut self.from.tables, from_tables),
            to_tables: std::mem::replace(&mut self.to.tables, to_tables),
            from_foreign_tables: std::mem::replace(
                &mut self.from.foreign_tables,
                from_foreign_tables,
            ),
            to_foreign_tables: std::mem::replace(&mut self.to.foreign_tables, to_foreign_tables),
        })
    }

    /// The FROM columns whose type `Comparer::enter_table_phase_view` hands
    /// the table phase from TO, by FROM table position: each column a
    /// routine's `CASCADE` takes and the routine phase re-adds (see
    /// `Comparer::cascade_taken_column_keys`) whose TO type differs, with its
    /// TO column.
    fn cascade_readded_retypes(&self) -> HashMap<usize, Vec<(String, TableColumn)>> {
        let mut retypes: HashMap<usize, Vec<(String, TableColumn)>> = HashMap::new();
        if self.cascade_readded.is_empty() {
            return retypes;
        }
        let keys = self.cascade_taken_column_keys(&self.cascade_readded);
        let to_by_key: HashMap<(&str, &str), &Table> = self
            .to
            .tables
            .iter()
            .map(|t| ((t.schema.as_str(), t.name.as_str()), t))
            .collect();
        for (idx, table) in self.from.tables.iter().enumerate() {
            let Some(to_table) = to_by_key.get(&(table.schema.as_str(), table.name.as_str()))
            else {
                continue;
            };
            for column in &table.columns {
                let key = Self::normalized_view_reference(&format!(
                    "{}.{}.{}",
                    table.schema, table.name, column.name
                ));
                if !keys.contains(&key) {
                    continue;
                }
                if let Some(to_column) = to_table.columns.iter().find(|c| c.name == column.name)
                    && column.type_differs(to_column)
                {
                    retypes
                        .entry(idx)
                        .or_default()
                        .push((column.name.clone(), to_column.clone()));
                }
            }
        }
        retypes
    }

    /// Put back what `Comparer::enter_table_phase_view` swapped out.
    fn leave_table_phase_view(&mut self, originals: Option<TablePhaseOriginals>) {
        if let Some(originals) = originals {
            self.from.tables = originals.from_tables;
            self.to.tables = originals.to_tables;
            self.from.foreign_tables = originals.from_foreign_tables;
            self.to.foreign_tables = originals.to_foreign_tables;
        }
    }

    /// Views — regular and materialized — that must be dropped before the table
    /// DDL runs, because a table they read is going away or a column they read
    /// is being dropped or retyped.
    ///
    /// PostgreSQL is precise about what a dependent view forbids, and this now
    /// matches it. It refuses `DROP COLUMN` and `ALTER COLUMN ... TYPE` for a
    /// column a view depends on, and refuses nothing else: adding a column, or
    /// dropping or retyping a column the view never reads, succeeds with the
    /// view in place (verified live on PostgreSQL 16). Deciding at *table*
    /// granularity — "this view reads a table that changed at all" — turned a
    /// metadata-only `ADD COLUMN` into a `DROP` and full rebuild of every
    /// materialized view over that table, its indexes included, with the data
    /// gone until the rebuild finished (issue #242).
    ///
    /// The column dependencies come from `View::column_relation`, which
    /// `pg_depend` records for the view's `_RETURN` rule and which covers every
    /// position a column can appear in, not just the select list — `WHERE`,
    /// `JOIN ... ON` and `GROUP BY` references are all there.
    ///
    /// Two things still take a view wholesale, whatever columns it reads: the
    /// table being dropped, and the table being dropped and recreated by the
    /// alter path (a partition-key change, say), because the view's own
    /// definition cannot outlive the relation it names. The same goes for a
    /// foreign table being dropped, or dropped and created again on another
    /// server or on its own server created again.
    ///
    /// A dump written before `column_relation` existed carries no column
    /// dependencies, and every view in it keeps the historical table-level
    /// answer — over-eager, but never wrong in the direction that breaks a
    /// migration.
    fn dependent_view_keys(&self, changes: &RelationChanges) -> HashSet<String> {
        let RelationChanges {
            wholesale_tables,
            changed_columns,
            altered_or_dropped,
            legacy_dependent,
            going_foreign_tables,
            ..
        } = changes;

        let mut dependent_views = HashSet::new();
        for view in &self.from.views {
            let view_key = Self::normalized_view_key(&view.schema, &view.name);

            // A foreign table `compare_foreign_tables` drops — and creates
            // again, when its server changes or is created again — takes a
            // view over it wholesale too: PostgreSQL refuses the drop while
            // the view is there.
            let reads_a_wholesale_table = view.table_relation.iter().any(|rel| {
                let key = Self::normalized_view_reference(rel);
                wholesale_tables.contains(&key) || going_foreign_tables.contains(&key)
            });
            if reads_a_wholesale_table {
                dependent_views.insert(view_key);
                continue;
            }

            match &view.column_relation {
                Some(columns) => {
                    let reads_a_changed_column = columns
                        .iter()
                        .any(|col| changed_columns.contains(&Self::normalized_view_reference(col)));
                    if reads_a_changed_column {
                        dependent_views.insert(view_key);
                    }
                }
                // Pre-`column_relation` dump: the historical answer, which is
                // `related_views` for a regular view and `table_relation` for a
                // materialized one (materialized views never appear in
                // `information_schema.view_column_usage`, so they had no other
                // source).
                None => {
                    let touched = if view.is_materialized {
                        view.table_relation.iter().any(|rel| {
                            altered_or_dropped.contains(&Self::normalized_view_reference(rel))
                        })
                    } else {
                        legacy_dependent.contains(&view_key)
                    };
                    if touched {
                        dependent_views.insert(view_key);
                    }
                }
            }
        }

        dependent_views
    }

    /// Saves the generated script to a file
    pub async fn save_script(&self, output: &str) -> Result<(), Error> {
        let mut file = File::create(output)?;
        file.write_all(self.get_script().as_bytes())?;
        Ok(())
    }

    async fn compare_schemas(&mut self) -> Result<(), Error> {
        self.script
            .append_block("\n/* ---> Schemas: Start section --------------- */");

        let from_schema_map: HashMap<&str, usize> = self
            .from
            .schemas
            .iter()
            .enumerate()
            .map(|(i, s)| (s.name.as_str(), i))
            .collect();

        // We will find all new schemas from "to" dump that are not in "from" dump
        // and add them to the script.
        for schema in &self.to.schemas {
            if let Some(&idx) = from_schema_map.get(schema.name.as_str()) {
                let from_schema = &self.from.schemas[idx];
                if Self::hashes_differ(&from_schema.hash, &schema.hash) {
                    self.script.push_str(
                        format!("\n/* Schema: {}*/\n", block_comment_safe(&schema.name)).as_str(),
                    );
                    self.script
                        .push_str(from_schema.get_alter_script(schema).as_str());
                }
            } else {
                self.script.push_str(
                    format!("\n/* Schema: {}*/\n", block_comment_safe(&schema.name)).as_str(),
                );
                self.script.push_str(schema.get_script().as_str());
            }
        }

        // We will find all schemas that exists just in "from" dump.
        // Drop will be added just if use_drop is true
        // TODO: We need to drop schema only if it's empty (no objects inside) Maybe the best way is to just skip dropping.
        /*
        for schema in &self.from.schemas {
            if let Some(_to_schema) = self
                .to
                .schemas
                .iter()
                .find(|s| s.name == schema.name)
            {
                continue; // Schema is present in both dumps, we already processed it
            } else if self.use_drop {
                // Just if we're using DROP statements
                self.script
                    .push_str(format!("/* Schema: {}*/
\n", schema.name).as_str());
                self.script.push_str(
        " /* Schema is not present in 'to' dump and should be dropped. */
\n",
                );
                self.script.push_str(schema.get_drop_script().as_str());
            }
        }*/

        self.script
            .append_block("\n/* ---> Schemas: End section --------------- */");
        Ok(())
    }

    // Comparing extentions
    async fn compare_extensions(&mut self) -> Result<(), Error> {
        self.script
            .append_block("\n/* ---> Extensions: Start section --------------- */");

        // We will find all new extensions from "to" dump that are not in "from" dump
        // and add them to the script.
        // Also we will find only in "from" dump extensions that are not in "to" dump and drop them.
        //
        // Keyed by NAME alone, not `(schema, name)`. An extension's name is
        // unique per database — `pg_extension` has a unique index on
        // `extname` — and its schema is an attribute of where it was
        // installed, not part of its identity. Keying by the pair made a
        // moved extension look like two unrelated events: a new one in the
        // new schema and a removed one in the old. The pair replays as
        // `create extension if not exists`, a no-op because the name is
        // already taken, followed by `drop extension if exists`, which
        // removes the only copy — the extension ends up gone rather than
        // moved, and everything calling its functions breaks (issue #241).
        // Matched by name, the move reaches `Extension::get_alter_script`,
        // which has always known how to emit `alter extension ... set
        // schema ...`.
        let from_ext_map: HashMap<&str, usize> = self
            .from
            .extensions
            .iter()
            .enumerate()
            .map(|(i, e)| (e.name.as_str(), i))
            .collect();

        let to_ext_keys: HashSet<&str> =
            self.to.extensions.iter().map(|e| e.name.as_str()).collect();

        // What the migrated (FROM) server can install. Empty for dumps
        // written before pgc recorded it, and then every version is emitted
        // exactly as before. When it is known, a TO version the server cannot
        // install is never pinned: PostgreSQL 19 ships btree_gist 1.9 as the
        // only installable version, so `version '1.8'` (from an older major,
        // or from a pg_upgrade'd 19 cluster) fails, and every exclusion
        // constraint and GiST index depending on btree_gist fails with it.
        let available: HashMap<&str, &crate::dump::extension::AvailableExtension> = self
            .from
            .available_extensions
            .iter()
            .map(|a| (a.name.as_str(), a))
            .collect();

        for ext in &self.to.extensions {
            if let Some(&idx) = from_ext_map.get(ext.name.as_str()) {
                let from_ext = &self.from.extensions[idx];
                let alter = match available.get(ext.name.as_str()) {
                    Some(avail) if avail.cannot_update(&from_ext.version, &ext.version) => {
                        let mut alter = format!(
                            "-- Extension {} version '{}' is not available on the migrated server \
                             (installed: '{}'; installable: {}); version left unchanged.\n",
                            line_comment_safe(&ext.name),
                            line_comment_safe(&ext.version),
                            line_comment_safe(&from_ext.version),
                            line_comment_safe(&avail.versions_list())
                        );
                        alter.push_str(&from_ext.get_set_schema_script(ext));
                        alter
                    }
                    _ => from_ext.get_alter_script(ext),
                };
                if !alter.is_empty() {
                    self.script.push_str(
                        format!(
                            "/* Extension: {}.{}*/\n",
                            block_comment_safe(&ext.schema),
                            block_comment_safe(&ext.name)
                        )
                        .as_str(),
                    );
                    self.script.push_str(alter.as_str());
                }
                if from_ext.owner != ext.owner {
                    self.script.push_str(
                        format!(
                            "/* Extension: {}.{}*/\n",
                            block_comment_safe(&ext.schema),
                            block_comment_safe(&ext.name)
                        )
                        .as_str(),
                    );
                    self.script.push_str(
                        format!(
                            "-- Extension owner change is not supported by PostgreSQL ALTER EXTENSION syntax ({} -> {}).\n",
                            line_comment_safe(&from_ext.owner),
                            line_comment_safe(&ext.owner)
                        )
                        .as_str(),
                    );
                    eprintln!(
                        "Warning: extension {} is owned by {} in the target, but PostgreSQL has \
                         no ALTER EXTENSION ... OWNER TO; its owner stays {} (manual intervention needed).",
                        ext.name, ext.owner, from_ext.owner
                    );
                }
            } else {
                self.script.push_str(
                    format!(
                        "/* Extension: {}.{}*/\n",
                        block_comment_safe(&ext.schema),
                        block_comment_safe(&ext.name)
                    )
                    .as_str(),
                );
                match available.get(ext.name.as_str()) {
                    // Known to the server, but not at this version: install
                    // its default instead (the form pg_dump emits) rather than
                    // fail. A missing entry means the extension is not
                    // available at all, which no spelling can fix, and an
                    // empty default leaves nothing to fall back to.
                    Some(avail)
                        if !avail.can_install(&ext.version)
                            && !avail.default_version.is_empty() =>
                    {
                        self.script.push_str(&format!(
                            "-- Extension {} version '{}' is not installable on the migrated server \
                             (installable: {}); creating its default version '{}' instead.\n",
                            line_comment_safe(&ext.name),
                            line_comment_safe(&ext.version),
                            line_comment_safe(&avail.versions_list()),
                            line_comment_safe(&avail.default_version)
                        ));
                        self.script.push_str(&ext.get_script_default_version());
                    }
                    _ => self.script.push_str(ext.get_script().as_str()),
                }
            }
        }

        for ext in &self.from.extensions {
            if to_ext_keys.contains(ext.name.as_str()) {
                continue; // Extension is present in both dumps, we already processed it
            } else {
                // Extension is not present in 'to' dump and should be dropped.
                self.script.push_str(
                    format!(
                        "/* Extension: {}.{}*/\n",
                        block_comment_safe(&ext.schema),
                        block_comment_safe(&ext.name)
                    )
                    .as_str(),
                );
                self.script.push_str(
                    "/* Extension is not present in 'to' dump and should be dropped. */\n",
                );

                let drop_script = ext.get_drop_script();
                if self.use_drop {
                    self.script.push_str(drop_script.as_str());
                } else {
                    self.script.push_str(comment_out(&drop_script).as_str());
                }
            }
        }

        self.script
            .append_block("\n/* ---> Extensions: End section --------------- */");
        Ok(())
    }

    // Comparing types
    async fn compare_types(&mut self) -> Result<(), Error> {
        self.script
            .append_block("\n/* ---> User-defined types: Start --------------- */");

        let from_type_map: HashMap<(&str, &str), usize> = self
            .from
            .types
            .iter()
            .enumerate()
            .map(|(i, t)| ((t.schema.as_str(), t.typname.as_str()), i))
            .collect();

        let to_type_keys: HashSet<(&str, &str)> = self
            .to
            .types
            .iter()
            .map(|t| (t.schema.as_str(), t.typname.as_str()))
            .collect();

        // Domain `add constraint` / `validate constraint` statements. Both
        // scan every stored value of the domain, and a CHECK may call a
        // routine, operator or cast this migration creates, so they wait in
        // `domain_constraint_post_script` until those exist and the tables
        // and columns this migration drops are gone. The dropping half of a
        // changed CHECK stays here: relaxing a domain early is always safe.
        let mut domain_constraint_section = String::new();
        // Composite type alterations. They run after every type this section
        // creates, since an added or retyped attribute may be of one of them,
        // and after `drop_views`: `ALTER ATTRIBUTE … TYPE … CASCADE` retypes
        // the columns of the type's typed tables, which PostgreSQL refuses
        // while a view or rule reads one ("cannot alter type of a column used
        // by a view or rule"), and `DROP ATTRIBUTE … CASCADE` would drop such
        // a view silently. `drop_views` drops those views first, as it does
        // for an ordinary table's retyped or dropped column.
        let mut composite_section = String::new();

        for to_type in &self.to.types {
            if (to_type.typtype as u8 as char) == 'e' {
                continue;
            }
            if let Some(&idx) =
                from_type_map.get(&(to_type.schema.as_str(), to_type.typname.as_str()))
            {
                let from_type = &self.from.types[idx];
                let differs = Self::type_definition_differs(from_type, to_type);
                if !(differs && from_type.alter_recreates(to_type))
                    && let Some(move_script) = from_type.get_multirange_set_schema_script(to_type)
                {
                    // A multirange pgc before 1.1.0 created in the wrong
                    // schema; the range itself is unchanged.
                    self.script.push_str(&format!(
                        "/* Type: {}.{} (its multirange moves to the range's schema) */\n",
                        block_comment_safe(&to_type.schema),
                        block_comment_safe(&to_type.typname)
                    ));
                    self.script.push_str(&move_script);
                }
                if differs {
                    let header = format!(
                        "/* Type: {}.{} */\n",
                        block_comment_safe(&to_type.schema),
                        block_comment_safe(&to_type.typname)
                    );
                    let is_composite = (to_type.typtype as u8 as char) == 'c';
                    let usage = if is_composite {
                        self.composite_usage(from_type)
                    } else {
                        CompositeUsage::default()
                    };
                    let parts =
                        from_type.get_alter_script_parts_with(to_type, self.use_drop, &usage);
                    for warning in &parts.warnings {
                        eprintln!("Warning: {warning}");
                    }
                    // `add_values` is enum-only, and enums are compared in
                    // `compare_enums`; kept here so nothing is ever lost.
                    let in_place = format!("{}{}", parts.add_values, parts.in_place);
                    let deferred_only =
                        in_place.trim().is_empty() && !parts.deferred.trim().is_empty();
                    if !deferred_only {
                        let section = if is_composite {
                            &mut composite_section
                        } else {
                            &mut self.script
                        };
                        section.push_str(&header);
                        if in_place.trim().is_empty() {
                            section.push_str(
                                "-- No supported alterations for this type; manual review required.\n",
                            );
                            eprintln!(
                                "Warning: type {}.{} differs from the target, but pgc has no \
                                 statement to alter it (manual review required).",
                                to_type.schema, to_type.typname
                            );
                        } else {
                            section.push_str(&in_place);
                        }
                    }
                    if !parts.deferred.trim().is_empty() {
                        if domain_constraint_section.is_empty() {
                            domain_constraint_section.append_block(
                                "\n/* ---> Domain constraints: Add section (execute after the tables, routines, operators and casts they check against) --------------- */",
                            );
                        }
                        domain_constraint_section.push_str(&header);
                        domain_constraint_section.push_str(&parts.deferred);
                    }
                }
            } else {
                // New-in-`to` multirange: PostgreSQL auto-creates the
                // multirange row when its range type is created, so there is
                // nothing to emit here (and `get_script` for 'm' would only
                // return a no-op comment). Deliberately narrower than the
                // drop loop's blanket 'm' skip: multiranges that exist in
                // BOTH dumps but have metadata drift (owner / comment / ACL)
                // still reach the ALTER branch above and emit those deltas.
                if (to_type.typtype as u8 as char) == 'm' {
                    continue;
                }
                self.script.push_str(
                    format!(
                        "/* Type: {}.{} */\n",
                        block_comment_safe(&to_type.schema),
                        block_comment_safe(&to_type.typname)
                    )
                    .as_str(),
                );
                self.script.push_str(to_type.get_script().as_str());
                if let Some(warning) = to_type.create_warning() {
                    eprintln!("Warning: {warning}");
                }
            }
        }
        self.composite_alter_script.push_str(&composite_section);

        let mut drop_section = String::new();
        {
            for from_type in &self.from.types {
                if (from_type.typtype as u8 as char) == 'e' {
                    continue;
                }
                // Multirange types are auto-dropped when their range type is dropped.
                if (from_type.typtype as u8 as char) == 'm' {
                    continue;
                }
                if to_type_keys.contains(&(from_type.schema.as_str(), from_type.typname.as_str())) {
                    continue;
                }

                if drop_section.is_empty() {
                    drop_section.append_block(
                        "\n/* ---> Types: Drop section (execute after dependent objects, including routines) --------------- */",
                    );
                }

                drop_section.push_str(
                    format!(
                        "/* Type: {}.{} */\n",
                        block_comment_safe(&from_type.schema),
                        block_comment_safe(&from_type.typname)
                    )
                    .as_str(),
                );
                drop_section
                    .push_str("/* Type is not present in 'to' dump and should be dropped. */\n");

                let drop_script = from_type.get_drop_script();
                if self.use_drop {
                    drop_section.push_str(drop_script.as_str());
                } else {
                    drop_section.push_str(comment_out(&drop_script).as_str());
                }
            }
        }

        if !drop_section.is_empty() {
            drop_section.append_block("\n/* ---> Types: Drop section end --------------- */");
            self.type_post_script.push_str(&drop_section);
        }

        if !domain_constraint_section.is_empty() {
            domain_constraint_section
                .append_block("\n/* ---> Domain constraints: Add section end --------------- */");
            self.domain_constraint_post_script
                .push_str(&domain_constraint_section);
        }

        self.script
            .append_block("\n/* ---> User-defined types: End --------------- */");
        Ok(())
    }

    // Comparing enums
    async fn compare_enums(&mut self) -> Result<(), Error> {
        let mut create_alter_section = String::new();
        create_alter_section.append_block("\n/* ---> Enums: Start --------------- */");

        let mut drop_section = String::new();
        // `alter type … add value` statements moved before `begin;` (single
        // transaction only); see `enum_add_value_script`.
        let mut add_value_section = String::new();

        // Build lookup maps for O(1) access (reuses type maps since enums are stored in types)
        let from_type_map: HashMap<(&str, &str), usize> = self
            .from
            .types
            .iter()
            .enumerate()
            .map(|(i, t)| ((t.schema.as_str(), t.typname.as_str()), i))
            .collect();

        let to_type_keys: HashSet<(&str, &str)> = self
            .to
            .types
            .iter()
            .map(|t| (t.schema.as_str(), t.typname.as_str()))
            .collect();

        let to_enums = self
            .to
            .types
            .iter()
            .filter(|t| (t.typtype as u8 as char) == 'e');

        for to_enum in to_enums {
            if to_enum.hash.is_none() {
                self.script.push_str(
                    format!(
                        "/* Skipping enum {}.{} due to missing hash. */\n",
                        block_comment_safe(&to_enum.schema),
                        block_comment_safe(&to_enum.typname)
                    )
                    .as_str(),
                );
                continue;
            }
            if let Some(&idx) =
                from_type_map.get(&(to_enum.schema.as_str(), to_enum.typname.as_str()))
            {
                let from_enum = &self.from.types[idx];
                if from_enum.hash.is_none() {
                    self.script.push_str(
                        format!(
                            "/* Skipping enum {}.{} due to missing hash. */\n",
                            block_comment_safe(&from_enum.schema),
                            block_comment_safe(&from_enum.typname)
                        )
                        .as_str(),
                    );
                    continue;
                }
                if Self::type_definition_differs(from_enum, to_enum) {
                    let header = format!(
                        "/* Enum: {}.{} */\n",
                        block_comment_safe(&to_enum.schema),
                        block_comment_safe(&to_enum.typname)
                    );
                    let parts = from_enum.get_alter_script_parts(to_enum, self.use_drop);
                    for warning in &parts.warnings {
                        eprintln!("Warning: {warning}");
                    }
                    // `deferred` is domain-only; kept here so nothing is lost.
                    let rest = format!("{}{}", parts.in_place, parts.deferred);
                    if self.use_single_transaction && !parts.add_values.trim().is_empty() {
                        // A value added inside the migration's transaction
                        // cannot be used (as a default, in a CHECK, …) until
                        // it commits, so it is added before `begin;`.
                        if add_value_section.is_empty() {
                            add_value_section.append_block(
                                "\n/* ---> Enum values: Start (added before the transaction: a value added inside one cannot be used until it commits) --------------- */",
                            );
                        }
                        add_value_section.push_str(&header);
                        add_value_section.push_str(&parts.add_values);
                        if !rest.trim().is_empty() {
                            create_alter_section.push_str(&header);
                            create_alter_section.push_str(&rest);
                        }
                    } else {
                        create_alter_section.push_str(&header);
                        let alter_script = format!("{}{}", parts.add_values, rest);
                        if alter_script.trim().is_empty() {
                            create_alter_section.push_str(
                                "-- No supported alterations for this enum; manual review required.\n",
                            );
                            eprintln!(
                                "Warning: enum {}.{} differs from the target, but pgc has no \
                                 statement to alter it (manual review required).",
                                to_enum.schema, to_enum.typname
                            );
                        } else {
                            create_alter_section.push_str(alter_script.as_str());
                        }
                    }
                }
            } else {
                create_alter_section.push_str(
                    format!(
                        "/* Enum: {}.{} */\n",
                        block_comment_safe(&to_enum.schema),
                        block_comment_safe(&to_enum.typname)
                    )
                    .as_str(),
                );
                create_alter_section.push_str(to_enum.get_script().as_str());
                if let Some(warning) = to_enum.create_warning() {
                    eprintln!("Warning: {warning}");
                }
            }
        }

        {
            for from_enum in self
                .from
                .types
                .iter()
                .filter(|t| (t.typtype as u8 as char) == 'e')
            {
                if to_type_keys.contains(&(from_enum.schema.as_str(), from_enum.typname.as_str())) {
                    continue;
                }

                if drop_section.is_empty() {
                    drop_section.append_block(
                        "\n/* ---> Enums: Drop section (execute after dependent objects, including types and routines) --------------- */",
                    );
                }
                drop_section.push_str(
                    format!(
                        "/* Enum: {}.{} */\n",
                        block_comment_safe(&from_enum.schema),
                        block_comment_safe(&from_enum.typname)
                    )
                    .as_str(),
                );
                drop_section
                    .push_str("/* Enum is not present in 'to' dump and should be dropped. */\n");

                let drop_script = from_enum.get_drop_script();
                if self.use_drop {
                    drop_section.push_str(drop_script.as_str());
                } else {
                    drop_section.push_str(comment_out(&drop_script).as_str());
                }
            }
        }

        create_alter_section.append_block("\n/* ---> Enums: End --------------- */");
        if !drop_section.is_empty() {
            drop_section.append_block("\n/* ---> Enums: Drop section end --------------- */");
        }

        if !create_alter_section.trim().is_empty() {
            self.enum_pre_script.push_str(&create_alter_section);
        }
        if !add_value_section.is_empty() {
            add_value_section.append_block("\n/* ---> Enum values: End --------------- */");
            self.enum_add_value_script.push_str(&add_value_section);
        }
        if !drop_section.is_empty() {
            self.enum_post_script.push_str(&drop_section);
        }
        Ok(())
    }

    // Comparing sequences
    /// The identity sequences `compare_sequences` renames, as TO `(schema,
    /// name)` to FROM `(schema, name)`: each TO identity sequence FROM has no
    /// sequence of that name for, whose identity column FROM has as an
    /// identity column too, with its own identity sequence under a name TO
    /// has no sequence of. PostgreSQL keeps an identity sequence's name when
    /// its column or table is renamed (as it keeps a NOT NULL constraint's,
    /// see `Table::not_null_renames`), so the two are one sequence whose name
    /// drifted. Without the rename it kept the old name, and — the two never
    /// being compared — any change TO made to it (type, bounds, increment,
    /// cache, cycle, comment) was lost. `ALTER SEQUENCE ... RENAME` keeps the
    /// sequence, its position and its link to the column (checked on
    /// PostgreSQL 14 and 19).
    ///
    /// Left out when FROM already has a relation of TO's name in that schema
    /// — a table, view, sequence, foreign table or index — which the rename
    /// would collide with, and when either sequence has no hash.
    fn plan_identity_sequence_renames(&self) -> HashMap<(String, String), (String, String)> {
        let mut renames: HashMap<(String, String), (String, String)> = HashMap::new();
        let column_key = |s: &Sequence| -> Option<(String, String, String)> {
            Some((
                s.owned_by_schema.clone()?,
                s.owned_by_table.clone()?,
                s.owned_by_column.clone()?,
            ))
        };
        let is_identity_column =
            |tables: &[Table], (schema, table, column): &(String, String, String)| {
                tables
                    .iter()
                    .find(|t| &t.schema == schema && &t.name == table)
                    .and_then(|t| t.columns.iter().find(|c| &c.name == column))
                    .is_some_and(|c| c.is_identity)
            };
        let from_names: HashSet<(&str, &str)> = self
            .from
            .sequences
            .iter()
            .map(|s| (s.schema.as_str(), s.name.as_str()))
            .collect();
        let to_names: HashSet<(&str, &str)> = self
            .to
            .sequences
            .iter()
            .map(|s| (s.schema.as_str(), s.name.as_str()))
            .collect();
        // Every FROM relation name, by schema, a rename could collide with.
        let from_relations: HashSet<(&str, &str)> = self
            .from
            .tables
            .iter()
            .map(|t| (t.schema.as_str(), t.name.as_str()))
            .chain(
                self.from
                    .tables
                    .iter()
                    .flat_map(|t| t.indexes.iter())
                    .map(|i| (i.schema.as_str(), i.name.as_str())),
            )
            .chain(
                self.from
                    .views
                    .iter()
                    .map(|v| (v.schema.as_str(), v.name.as_str())),
            )
            .chain(
                self.from
                    .foreign_tables
                    .iter()
                    .map(|t| (t.schema.as_str(), t.name.as_str())),
            )
            .chain(from_names.iter().copied())
            .collect();
        let from_identity: HashMap<(String, String, String), &Sequence> = self
            .from
            .sequences
            .iter()
            .filter(|s| {
                s.is_identity
                    && s.hash.is_some()
                    && !to_names.contains(&(s.schema.as_str(), s.name.as_str()))
            })
            .filter_map(|s| Some((column_key(s)?, s)))
            .collect();
        for to_sequence in &self.to.sequences {
            if !to_sequence.is_identity
                || to_sequence.hash.is_none()
                || from_names.contains(&(to_sequence.schema.as_str(), to_sequence.name.as_str()))
                || from_relations
                    .contains(&(to_sequence.schema.as_str(), to_sequence.name.as_str()))
            {
                continue;
            }
            let Some(column) = column_key(to_sequence) else {
                continue;
            };
            let Some(from_sequence) = from_identity.get(&column) else {
                continue;
            };
            if from_sequence.schema != to_sequence.schema
                || !is_identity_column(&self.from.tables, &column)
                || !is_identity_column(&self.to.tables, &column)
            {
                continue;
            }
            renames.insert(
                (to_sequence.schema.clone(), to_sequence.name.clone()),
                (from_sequence.schema.clone(), from_sequence.name.clone()),
            );
        }
        renames
    }

    async fn compare_sequences(&mut self) -> Result<(), Error> {
        self.script
            .append_block("\n/* ---> Sequences: Start section --------------- */");

        let mut drop_section = String::new();
        // `ALTER SEQUENCE ... OWNED BY` for new sequences created here rather
        // than by their column; routed through `sequence_post_script`.
        let mut owned_by_section = String::new();

        // Build lookup maps for O(1) access
        let from_seq_map: HashMap<(&str, &str), usize> = self
            .from
            .sequences
            .iter()
            .enumerate()
            .map(|(i, s)| ((s.schema.as_str(), s.name.as_str()), i))
            .collect();

        let to_seq_keys: HashSet<(&str, &str)> = self
            .to
            .sequences
            .iter()
            .map(|s| (s.schema.as_str(), s.name.as_str()))
            .collect();

        let to_table_map: HashMap<(&str, &str), usize> = self
            .to
            .tables
            .iter()
            .enumerate()
            .map(|(i, t)| ((t.schema.as_str(), t.name.as_str()), i))
            .collect();

        let from_table_map: HashMap<(&str, &str), usize> = self
            .from
            .tables
            .iter()
            .enumerate()
            .map(|(i, t)| ((t.schema.as_str(), t.name.as_str()), i))
            .collect();

        // For the serial shortcut: which new tables name which sequences, and
        // in what order the new tables are created.
        let new_table_defaults =
            Self::new_table_regclass_defaults(&self.from.tables, &self.to.tables);
        let to_creation_rank = Self::creation_ranks(&self.to.tables);

        // Tables whose persistence is flipping in this migration. The
        // table-level `ALTER TABLE SET LOGGED|UNLOGGED` (emitted by the
        // FK-ordered phase at the end of `compare_tables`) automatically
        // propagates to every owned sequence, so re-emitting the same
        // SET on the sequence — or, when persistence is the *only*
        // diff, the entire ALTER SEQUENCE block — is redundant noise.
        // See issue #180.
        let tables_flipping_persistence: HashMap<(&str, &str), bool> = self
            .to
            .tables
            .iter()
            .filter_map(|to_table| {
                let from_table = from_table_map
                    .get(&(to_table.schema.as_str(), to_table.name.as_str()))
                    .map(|&i| &self.from.tables[i])?;
                from_table
                    .persistence_change_for(to_table)
                    .map(|new_unlogged| {
                        (
                            (to_table.schema.as_str(), to_table.name.as_str()),
                            new_unlogged,
                        )
                    })
            })
            .collect();

        // The FROM identity sequences renamed to their TO name (see
        // `plan_identity_sequence_renames`), under that name, to compare
        // with the TO sequence like any sequence both dumps have.
        self.renamed_identity_sequences = self.plan_identity_sequence_renames();
        let renamed_from: HashMap<(&str, &str), Sequence> = self
            .renamed_identity_sequences
            .iter()
            .filter_map(|((to_schema, to_name), from_key)| {
                let &idx = from_seq_map.get(&(from_key.0.as_str(), from_key.1.as_str()))?;
                let mut renamed = self.from.sequences[idx].clone();
                renamed.name = to_name.clone();
                renamed.hash();
                Some(((to_schema.as_str(), to_name.as_str()), renamed))
            })
            .collect();
        let renamed_from_keys: HashSet<(&str, &str)> = self
            .renamed_identity_sequences
            .values()
            .map(|(schema, name)| (schema.as_str(), name.as_str()))
            .collect();

        // We will find all new sequences from "to" dump that are not in "from" dump
        // and we will find all existing sequences in both dumps with different hashes
        // and add them to the script.
        for sequence in &self.to.sequences {
            if sequence.hash.is_none() {
                self.script.push_str(
                    format!(
                        "/* Skipping sequence {}.{} due to missing hash. */\n",
                        block_comment_safe(&sequence.schema),
                        block_comment_safe(&sequence.name)
                    )
                    .as_str(),
                );
                continue;
            }
            let key = (sequence.schema.as_str(), sequence.name.as_str());
            let renamed = renamed_from.get(&key);
            if let Some(from_sequence) = from_seq_map
                .get(&key)
                .map(|&idx| &self.from.sequences[idx])
                .or(renamed)
            {
                if renamed.is_some()
                    && let Some((from_schema, from_name)) = self
                        .renamed_identity_sequences
                        .get(&(sequence.schema.clone(), sequence.name.clone()))
                {
                    self.script.push_str(&format!(
                        "/* Sequence: {}.{} (identity sequence of {}.{}.{}, renamed) */\n",
                        block_comment_safe(&sequence.schema),
                        block_comment_safe(&sequence.name),
                        block_comment_safe(sequence.owned_by_schema.as_deref().unwrap_or_default()),
                        block_comment_safe(sequence.owned_by_table.as_deref().unwrap_or_default()),
                        block_comment_safe(sequence.owned_by_column.as_deref().unwrap_or_default())
                    ));
                    self.script.push_str(
                        &format!(
                            "alter sequence if exists {from_schema}.{from_name} rename to {};",
                            sequence.name
                        )
                        .with_empty_lines(),
                    );
                }
                if from_sequence.hash.is_none() {
                    self.script.push_str(
                        format!(
                            "/* Skipping sequence {}.{} due to missing hash. */\n",
                            block_comment_safe(&from_sequence.schema),
                            block_comment_safe(&from_sequence.name)
                        )
                        .as_str(),
                    );
                    continue;
                }
                if Self::hashes_differ(&from_sequence.hash, &sequence.hash) {
                    // Determine whether the owning table — if any — is
                    // also flipping persistence in this migration.
                    let owning_table_flipping = match (
                        sequence.owned_by_schema.as_deref(),
                        sequence.owned_by_table.as_deref(),
                    ) {
                        (Some(s), Some(t)) => tables_flipping_persistence.contains_key(&(s, t)),
                        _ => false,
                    };
                    if owning_table_flipping && sequence.is_only_persistence_change(from_sequence) {
                        // The whole ALTER SEQUENCE is redundant — the
                        // owning table's `ALTER TABLE SET LOGGED|
                        // UNLOGGED` propagates to this sequence on its
                        // own. Suppress the entire emit (issue #180).
                        continue;
                    }
                    self.script.push_str(
                        format!(
                            "/* Sequence: {}.{}*/\n",
                            block_comment_safe(&sequence.schema),
                            block_comment_safe(&sequence.name)
                        )
                        .as_str(),
                    );
                    let alter = if owning_table_flipping {
                        sequence.get_alter_script_excluding_persistence(from_sequence)
                    } else {
                        sequence.get_alter_script(from_sequence)
                    };
                    self.script.push_str(alter.as_str());
                    if sequence.owned_by_waits_for_tables(from_sequence)
                        && let Some(owned_by_script) = sequence.get_owned_by_script()
                    {
                        if owned_by_section.is_empty() {
                            owned_by_section.append_block(
                                "\n/* ---> Sequences: Ownership section (execute after owning tables) --------------- */",
                            );
                        }
                        owned_by_section.push_str(&format!(
                            "/* Sequence: {}.{}*/\n",
                            block_comment_safe(&sequence.schema),
                            block_comment_safe(&sequence.name)
                        ));
                        owned_by_section.push_str(&owned_by_script);
                    }
                }
            } else {
                // A new sequence owned by a column. PostgreSQL creates it by
                // itself for an identity column, and for a column of a new
                // table that `serial` reproduces exactly; anything else is
                // created here and linked to its column once the table exists.
                let mut owned_by_script = None;
                if let (Some(schema), Some(table), Some(column)) = (
                    &sequence.owned_by_schema,
                    &sequence.owned_by_table,
                    &sequence.owned_by_column,
                ) && let Some(&tidx) = to_table_map.get(&(schema.as_str(), table.as_str()))
                {
                    let to_table = &self.to.tables[tidx];
                    if let Some(to_column) = to_table.columns.iter().find(|c| c.name == *column) {
                        // The pseudo-type only exists in the column list of a
                        // plain `CREATE TABLE`. A table that is already there
                        // is altered instead, with `ADD COLUMN` or `SET
                        // DEFAULT` naming the sequence, so the sequence has to
                        // exist first; `PARTITION OF` and `OF type` give their
                        // columns no type at all.
                        let serial_type = if from_table_map
                            .contains_key(&(schema.as_str(), table.as_str()))
                            || to_table.partition_of.is_some()
                            || to_table.typed_table_type.is_some()
                        {
                            None
                        } else {
                            sequence
                                .serial_pseudo_type(
                                    to_column,
                                    self.serial_sequence_unlogged(to_table.is_unlogged),
                                )
                                .filter(|_| {
                                    !Self::sequence_named_by_earlier_new_table(
                                        &new_table_defaults,
                                        &to_creation_rank,
                                        sequence,
                                        tidx,
                                    )
                                })
                        };

                        // Only the identity sequence itself comes with its
                        // column; another sequence owned by an identity
                        // column is created like any other.
                        if sequence.is_identity || serial_type.is_some() {
                            if let Some(serial_type) = serial_type {
                                self.serial_columns.insert(
                                    (schema.clone(), table.clone(), column.clone()),
                                    serial_type.to_string(),
                                );
                            }
                            self.script.push_str(
                                        format!(
                                            "/* Skipping sequence {}.{} as it will be created by column {}.{}.{} (identity/serial) */\n",
                                            block_comment_safe(&sequence.schema),
                                            block_comment_safe(&sequence.name),
                                            block_comment_safe(schema),
                                            block_comment_safe(table),
                                            block_comment_safe(column)
                                        )
                                        .as_str(),
                                    );
                            continue;
                        }
                        owned_by_script = sequence.get_owned_by_script();
                    }
                }

                self.script.push_str(
                    format!(
                        "/* Sequence: {}.{}*/\n",
                        block_comment_safe(&sequence.schema),
                        block_comment_safe(&sequence.name)
                    )
                    .as_str(),
                );
                self.script.push_str(sequence.get_script().as_str());
                if let Some(owned_by_script) = owned_by_script {
                    if owned_by_section.is_empty() {
                        owned_by_section.append_block(
                            "\n/* ---> Sequences: Ownership section (execute after owning tables) --------------- */",
                        );
                    }
                    owned_by_section.push_str(
                        format!(
                            "/* Sequence: {}.{}*/\n",
                            block_comment_safe(&sequence.schema),
                            block_comment_safe(&sequence.name)
                        )
                        .as_str(),
                    );
                    owned_by_section.push_str(&owned_by_script);
                }
            }
        }

        {
            for sequence in &self.from.sequences {
                if to_seq_keys.contains(&(sequence.schema.as_str(), sequence.name.as_str())) {
                    continue; // Sequence is present in both dumps, we already processed it
                }
                // Renamed to its TO name above.
                if renamed_from_keys.contains(&(sequence.schema.as_str(), sequence.name.as_str())) {
                    continue;
                }

                // Check if the sequence is owned by a table/column.
                if let (Some(schema), Some(table)) =
                    (&sequence.owned_by_schema, &sequence.owned_by_table)
                {
                    let to_table_idx = to_table_map.get(&(schema.as_str(), table.as_str()));

                    if to_table_idx.is_none() {
                        self.script.push_str(
                            format!(
                                "/* Skipping drop of sequence {}.{} as it is owned by table {}.{} which will be dropped. */\n",
                                block_comment_safe(&sequence.schema),
                                block_comment_safe(&sequence.name),
                                block_comment_safe(schema),
                                block_comment_safe(table)
                            )
                            .as_str(),
                        );
                        continue;
                    }

                    // Table exists in TO. Check if column exists or if it was an identity column.
                    if let Some(column_name) = &sequence.owned_by_column {
                        let to_table = &self.to.tables[*to_table_idx.unwrap()];
                        let to_column = to_table.columns.iter().find(|c| c.name == *column_name);

                        if to_column.is_none() {
                            self.script.push_str(
                                format!(
                                    "/* Skipping drop of sequence {}.{} as it is owned by column {}.{}.{} which will be dropped. */\n",
                                    block_comment_safe(&sequence.schema),
                                    block_comment_safe(&sequence.name),
                                    block_comment_safe(schema),
                                    block_comment_safe(table),
                                    block_comment_safe(column_name)
                                )
                                .as_str(),
                            );
                            continue;
                        }

                        // Column exists in TO. The identity sequence of a FROM
                        // identity column goes with its identity (`DROP
                        // IDENTITY`); any other sequence the column owns is
                        // dropped like the rest.
                        if sequence.is_identity
                            && let Some(&fidx) =
                                from_table_map.get(&(schema.as_str(), table.as_str()))
                        {
                            let from_table = &self.from.tables[fidx];
                            if let Some(from_column) =
                                from_table.columns.iter().find(|c| c.name == *column_name)
                                && from_column.is_identity
                            {
                                self.script.push_str(
                                            format!(
                                                "/* Skipping drop of sequence {}.{} as it is owned by identity column {}.{}.{}. */\n",
                                                block_comment_safe(&sequence.schema),
                                                block_comment_safe(&sequence.name),
                                                block_comment_safe(schema),
                                                block_comment_safe(table),
                                                block_comment_safe(column_name)
                                            )
                                            .as_str(),
                                        );
                                continue;
                            }
                        }
                    }
                }

                if drop_section.is_empty() {
                    drop_section.append_block(
                        "\n/* ---> Sequences: Drop section (execute after dependent tables) --------------- */",
                    );
                }

                drop_section.push_str(
                    format!(
                        "/* Sequence: {}.{}*/\n",
                        block_comment_safe(&sequence.schema),
                        block_comment_safe(&sequence.name)
                    )
                    .as_str(),
                );
                drop_section.push_str(
                    "/* Sequence is not present in 'to' dump and should be dropped. */\n",
                );

                let drop_script = sequence.get_drop_script();
                if self.use_drop {
                    drop_section.push_str(drop_script.as_str());
                } else {
                    drop_section.push_str(comment_out(&drop_script).as_str());
                }
            }
        }

        self.script
            .append_block("\n/* ---> Sequences: End section --------------- */");

        // Flushed after the tables, so every owning table and column exists.
        if !owned_by_section.is_empty() {
            owned_by_section
                .append_block("\n/* ---> Sequences: Ownership section end --------------- */");
            self.sequence_post_script.push_str(&owned_by_section);
        }
        if !drop_section.is_empty() {
            drop_section.append_block("\n/* ---> Sequences: Drop section end --------------- */");
            self.sequence_post_script.push_str(&drop_section);
        }
        Ok(())
    }

    // Comparing routines
    #[cfg(test)]
    async fn compare_routines(&mut self) -> Result<(), Error> {
        self.script
            .append_block("\n/* ---> Routines: Start section --------------- */");

        // Build O(1) lookup maps (owned keys to avoid borrowing self)
        let to_routine_keys: HashSet<(String, String, String)> = self
            .to
            .routines
            .iter()
            .map(|r| (r.schema.clone(), r.name.clone(), r.arguments.clone()))
            .collect();

        let from_routine_map: HashMap<(String, String, String), usize> = self
            .from
            .routines
            .iter()
            .enumerate()
            .map(|(i, r)| ((r.schema.clone(), r.name.clone(), r.arguments.clone()), i))
            .collect();

        // ── Drop routines not present in TO (reverse dependency order) ──
        // Borrow rather than clone: we only need read access to each Routine
        // for dep analysis and emission, and `self.from.routines` is never
        // mutated through this function.
        let routines_to_drop: Vec<&Routine> = self
            .from
            .routines
            .iter()
            .filter(|r| {
                !to_routine_keys.contains(&(r.schema.clone(), r.name.clone(), r.arguments.clone()))
            })
            .collect();

        if !routines_to_drop.is_empty() {
            let mut drop_deps: Vec<HashSet<usize>> = vec![HashSet::new(); routines_to_drop.len()];
            let drop_names: Vec<(String, String, String)> = routines_to_drop
                .iter()
                .map(|r| {
                    (
                        r.schema.to_lowercase(),
                        r.name.to_lowercase(),
                        r.arguments.to_lowercase(),
                    )
                })
                .collect();

            // Precompute `(lower, unquoted_lower)` of each routine's source once
            // — the inner loop below is O(n²) over routine count, and without
            // this the haystack would be re-lowered on every iteration.
            let drop_sources: Vec<(String, String)> = routines_to_drop
                .iter()
                .map(|r| Self::prelower_pair_for_dependency_scan(&r.source_code))
                .collect();

            for (i, routine) in routines_to_drop.iter().enumerate() {
                let (src_lower, src_unquoted) = &drop_sources[i];
                for (j, (schema, name, _)) in drop_names.iter().enumerate() {
                    if i != j
                        && (Self::text_references_qualified_name_pre(
                            src_lower,
                            src_unquoted,
                            schema,
                            name,
                        ) || Self::aggregate_references_function(routine, schema, name))
                    {
                        drop_deps[i].insert(j);
                    }
                }
            }

            Self::warn_dependency_cycle(
                "routines being dropped",
                routines_to_drop.len(),
                &drop_deps,
                |i| {
                    let r = routines_to_drop[i];
                    format!("{}.{}({})", r.schema, r.name, r.arguments)
                },
            );
            let mut drop_order = Self::kahn_toposort(routines_to_drop.len(), &drop_deps, |i| {
                drop_names[i].clone()
            });
            drop_order.reverse();

            for idx in drop_order {
                let routine = routines_to_drop[idx];
                self.script.push_str(
                    format!(
                        "/* Routine: {}.{}({})*/\n",
                        block_comment_safe(&routine.schema),
                        block_comment_safe(&routine.name),
                        block_comment_safe(&routine.arguments)
                    )
                    .as_str(),
                );
                self.script
                    .push_str("/* Routine is not present in 'to' dump and should be dropped. */\n");
                let drop_script = routine.get_drop_script();
                if self.use_drop {
                    self.script.push_str(drop_script.as_str());
                } else {
                    self.script.push_str(comment_out(&drop_script).as_str());
                }
            }
        }

        // ── Create / update routines in dependency order ──
        // Borrow into self.to.routines instead of cloning each Routine.
        let create_routines: Vec<(usize, &Routine)> = self
            .to
            .routines
            .iter()
            .enumerate()
            .filter(|(_, r)| r.hash.is_some())
            .collect();

        if !create_routines.is_empty() {
            let n = create_routines.len();
            let names: Vec<(String, String, String)> = create_routines
                .iter()
                .map(|(_, r)| {
                    (
                        r.schema.to_lowercase(),
                        r.name.to_lowercase(),
                        r.arguments.to_lowercase(),
                    )
                })
                .collect();

            let mut depends_on: Vec<HashSet<usize>> = vec![HashSet::new(); n];
            // Same optimisation as in the drop loop above: prelower each
            // routine's source_code once so the quadratic scan doesn't.
            let create_sources: Vec<(String, String)> = create_routines
                .iter()
                .map(|(_, r)| Self::prelower_pair_for_dependency_scan(&r.source_code))
                .collect();
            for (i, (_, routine)) in create_routines.iter().enumerate() {
                let (src_lower, src_unquoted) = &create_sources[i];
                for (j, (schema, name, _)) in names.iter().enumerate() {
                    if i != j
                        && (Self::text_references_qualified_name_pre(
                            src_lower,
                            src_unquoted,
                            schema,
                            name,
                        ) || Self::aggregate_references_function(routine, schema, name))
                    {
                        depends_on[i].insert(j);
                    }
                }
            }

            Self::warn_dependency_cycle("routines", n, &depends_on, |i| {
                let r = create_routines[i].1;
                format!("{}.{}({})", r.schema, r.name, r.arguments)
            });
            let sorted = Self::kahn_toposort(n, &depends_on, |i| {
                let r = create_routines[i].1;
                let priority: u8 = if r.lang.eq_ignore_ascii_case("sql") {
                    1
                } else {
                    0
                };
                (
                    priority,
                    r.schema.to_lowercase(),
                    r.name.to_lowercase(),
                    r.arguments.to_lowercase(),
                )
            });

            // Resolve the emit plan into (to_idx, optional from_idx) pairs
            // up front. The actual emission borrows the routines directly
            // out of `self.to` / `self.from` and writes through
            // `&mut self.script`; `Self::emit_routine_diff` is a free
            // associated function precisely so these disjoint-field
            // borrows can coexist without cloning each `Routine`.
            let plan: Vec<(usize, Option<usize>)> = sorted
                .iter()
                .map(|&idx| {
                    let (to_idx, routine) = create_routines[idx];
                    let from_idx = from_routine_map
                        .get(&(
                            routine.schema.clone(),
                            routine.name.clone(),
                            routine.arguments.clone(),
                        ))
                        .copied();
                    (to_idx, from_idx)
                })
                .collect();
            drop(create_routines);

            let set_list_to_null = self.migrated_server_at_least(SET_LIST_TO_NULL_MIN_VERSION);
            let no_set_list_to_null =
                self.migrated_server_known_below(SET_LIST_TO_NULL_MIN_VERSION);
            let no_aggregate_support =
                self.migrated_server_known_below(AGGREGATE_SUPPORT_MIN_VERSION);
            for (to_idx, from_idx) in plan {
                let routine = &self.to.routines[to_idx];
                let from_routine = from_idx.map(|i| &self.from.routines[i]);
                Self::emit_routine_diff(
                    &mut self.script,
                    self.use_drop,
                    set_list_to_null,
                    no_set_list_to_null,
                    no_aggregate_support,
                    routine,
                    from_routine,
                    true,
                );
            }
        }

        self.script
            .append_block("\n/* ---> Routines: End section --------------- */");
        Ok(())
    }

    /// What each FROM table's alter has to know about the rest of its
    /// inheritance or partition family, keyed by `(schema, name)` (see
    /// [`InheritedRecreates`]): the generated-column drop + adds the family
    /// shares, the members whose rows an ancestor's alter rewrites first,
    /// the CHECK constraints the family drops and re-adds together, the
    /// columns an ancestor's alter retypes, and what attaching or detaching
    /// a partition moves. `compare_tables` adds the foreign keys dropped
    /// ahead; `Comparer::typed_retype_pre_section` reads the same plan
    /// before the composite section.
    fn plan_inherited_recreates(
        &self,
        alter_caps: &HashMap<(String, String), TableAlterCaps>,
    ) -> HashMap<(String, String), InheritedRecreates> {
        // Generated-column drop + adds that INHERITS families share, and the
        // members whose rows an ancestor's alter rewrites first.
        let mut inherited_recreates = self.inherited_recreates(alter_caps);
        for key in self.tables_rewritten_by_ancestor(alter_caps) {
            inherited_recreates
                .entry(key)
                .or_default()
                .rewritten_by_ancestor = true;
        }
        for (key, checks) in self.inherited_check_recreates(alter_caps) {
            inherited_recreates.entry(key).or_default().family_checks = checks;
        }
        // The CHECKs in the way of an `ALTER TABLE ONLY ... SET EXPRESSION`
        // (see `Comparer::set_expression_check_hazards`). Without `use_drop`
        // `table_alter_caps` has turned that form off for them instead.
        if self.use_drop {
            let hazards = self.set_expression_check_hazards(alter_caps);
            for (key, checks) in hazards.drops {
                inherited_recreates
                    .entry(key)
                    .or_default()
                    .family_checks
                    .extend(checks);
            }
            for (key, checks) in hazards.deferred {
                inherited_recreates
                    .entry(key)
                    .or_default()
                    .checks_after_alters
                    .extend(checks);
            }
        }
        for (key, columns) in self.inherited_retypes(alter_caps) {
            inherited_recreates
                .entry(key)
                .or_default()
                .retyped_by_ancestor = columns;
        }
        for (key, names) in self.triggers_dropped_before_attach() {
            inherited_recreates
                .entry(key)
                .or_default()
                .triggers_dropped_before_attach = names;
        }
        for (key, moves) in self.partition_move_checks() {
            let plan = inherited_recreates.entry(key).or_default();
            plan.detached_checks = moves.detached;
            plan.attach_checks = moves.attached;
            plan.inherit_readds = moves.readds;
        }
        inherited_recreates
    }

    // Comparing tables
    async fn compare_tables(&mut self) -> Result<(), Error> {
        self.script
            .append_block("\n/* ---> Tables: Start section --------------- */");
        let ordered_from = Self::ordered_tables(&self.from.tables);
        let ordered_to = Self::ordered_tables(&self.to.tables);

        // We will drop all tables that exists just in "from" dump (partitions first).
        let ordered_from_drop = Self::ordered_tables_for_drop(&self.from.tables);

        // Partition topology used by the production output path to build/drop
        // indexes concurrently and partition-aware. Owned maps, so they don't
        // borrow `self` and the emission loops below can still push to the
        // script buffers.
        let (partitioned_parents, partition_children, partitioned_indexes) =
            self.build_partition_context_maps();
        let partition_ctx = PartitionContext {
            partitioned_parents: &partitioned_parents,
            children: &partition_children,
            partitioned_indexes: &partitioned_indexes,
        };

        // In-place ALTER forms the FROM server accepts, per table. Shared by
        // the alter call and the Path B gate below so they cannot disagree.
        let (alter_caps, set_expression_left_out) = self.table_alter_caps_and_left_out();
        for (schema, name) in &set_expression_left_out {
            let warning = format!(
                "Warning: the generated-column expression changes of table {schema}.{name} are \
                 left out: ALTER TABLE ONLY ... SET EXPRESSION would corrupt the inherited CHECK \
                 constraints that read them (PostgreSQL 17+), and with use_drop off they cannot \
                 be dropped and re-added around it."
            );
            self.script
                .push_str(&format!("/* {} */\n", block_comment_safe(&warning)));
            eprintln!("{warning}");
        }
        let mut inherited_recreates = self.plan_inherited_recreates(&alter_caps);
        let blockers = self.column_drop_blockers(&alter_caps);
        // The policies in the way of an in-place retype, by table key: those
        // `column_drop_blockers` drops ahead of every table alter, and the TO
        // policies reading a column another table retypes that the table
        // phase would otherwise create before that retype (see
        // `policies_reading_retyped_columns`; added below, on a new table as
        // it is created ahead, on any other once the order of the alters is
        // known). The table phase leaves each one out of its own table's
        // statements, FROM and TO side alike, so no alter, `CREATE TABLE` or
        // recreate drops or creates it, and creates them all from TO once
        // every table alter has run.
        let waiting_policies = self.policies_reading_retyped_columns(&blockers.retyped);
        // With those dropped ahead of a typed table's retype, before the
        // composite section (see `typed_retype_pre_section`).
        let mut retype_policies: HashMap<String, HashSet<String>> = HashMap::new();
        for ((schema, name), names) in blockers.policies.iter().chain(&self.typed_retype_policies) {
            retype_policies
                .entry(Self::table_key(schema, name))
                .or_default()
                .extend(names.iter().cloned());
        }
        // New tables created ahead of every table alter, by key.
        let mut created_ahead: HashSet<String> = HashSet::new();
        for (key, names) in &blockers.foreign_keys {
            inherited_recreates
                .entry(key.clone())
                .or_default()
                .dropped_ahead
                .clone_from(names);
        }
        self.triggers_dropped_ahead = blockers.triggers;
        self.triggers_dropped_ahead
            .extend(self.typed_retype_triggers.iter().cloned());
        let no_inherited_recreates = InheritedRecreates::default();

        // Build lookup maps for O(1) access
        let to_table_map: HashMap<(&str, &str), usize> = self
            .to
            .tables
            .iter()
            .enumerate()
            .map(|(i, t)| ((t.schema.as_str(), t.name.as_str()), i))
            .collect();

        let from_table_map: HashMap<(&str, &str), usize> = self
            .from
            .tables
            .iter()
            .enumerate()
            .map(|(i, t)| ((t.schema.as_str(), t.name.as_str()), i))
            .collect();

        // Pass 1: drop the foreign keys in the way of the tables that will be
        // removed (see `plan_dropped_table_fk_predrops`).
        let mut fk_pre_drop = String::new();
        let mut dropped_fk_keys: HashSet<String> = HashSet::new();
        for constraint in &self.dropped_table_fk_predrops {
            let key = format!(
                "{}.{}.{}",
                constraint.schema, constraint.table_name, constraint.name
            );
            if dropped_fk_keys.insert(key) {
                let drop_cmd = constraint.get_drop_script();
                if self.use_drop {
                    fk_pre_drop.push_str(&drop_cmd);
                } else {
                    fk_pre_drop.push_str(&comment_out(&drop_cmd));
                }
            }
        }

        // Foreign keys referencing a table that a recreate below drops (see
        // `plan_recreate_fk_predrops`; only planned with `use_drop`). The
        // table phase does not see them on the FROM side, so no table alter
        // drops them again, and `compare_foreign_keys` adds the TO ones back.
        let mut recreate_fk_drop = String::new();
        for constraint in &self.recreate_fk_predrops {
            let key = format!(
                "{}.{}.{}",
                constraint.schema, constraint.table_name, constraint.name
            );
            if dropped_fk_keys.insert(key) {
                recreate_fk_drop.push_str(&constraint.get_drop_script());
            }
        }
        if !recreate_fk_drop.is_empty() {
            fk_pre_drop.push_str(
                "/* Foreign keys referencing tables that are dropped and recreated below; they are added back after the tables. */\n",
            );
            fk_pre_drop.push_str(&recreate_fk_drop);
        }

        if !fk_pre_drop.is_empty() {
            self.script.push_str(&fk_pre_drop);
        }

        // Pass 2: drop tables (and their triggers) now that FK dependencies are removed.
        for table in ordered_from_drop.iter() {
            if to_table_map.contains_key(&(table.schema.as_str(), table.name.as_str())) {
                continue; // Table is present in both dumps, we already processed it
            }

            let mut pre_drop = String::new();

            // Drop triggers on the table
            for trigger in &table.triggers {
                let drop_cmd = format!(
                    "drop trigger if exists {} on {}.{};",
                    trigger.name, table.schema, table.name
                );
                if self.use_drop {
                    pre_drop.append_block(&drop_cmd);
                } else {
                    pre_drop.append_block(&comment_out_keep_blank_lines(&drop_cmd));
                }
            }

            self.script.push_str(
                format!(
                    "/* Table: {}.{}*/\n",
                    block_comment_safe(&table.schema),
                    block_comment_safe(&table.name)
                )
                .as_str(),
            );
            self.script
                .push_str("/* Table is not present in 'to' dump and should be dropped. */\n");

            if !pre_drop.is_empty() {
                self.script.push_str(&pre_drop);
            }

            let drop_script = table.get_drop_script();
            if self.use_drop {
                self.script.push_str(drop_script.as_str());
            } else {
                self.script.push_str(comment_out(&drop_script).as_str());
            }
        }

        // Tables the alter loop below drops and creates again from TO: the
        // roots whose own alter drops them (partition-key change, a type
        // change on a partition-key column, ...) and every partition their
        // `DROP TABLE` takes along, changed or not (see `RecreatedFamilies`).
        // The partitions are created from scratch instead of altered, and a
        // new partition of any of them waits until they are back.
        //
        // With `use_drop` off the DROP TABLE is commented out, nothing goes
        // with it, and a partition is only created again (and fails, like its
        // parent) when its own definition changed, as before.
        let families = self.recreated_families();
        let mut recreated_parents: HashSet<String> = HashSet::new();
        for table in ordered_from.iter() {
            let key = Self::table_key(&table.schema, &table.name);
            let Some(&tidx) = to_table_map.get(&(table.schema.as_str(), table.name.as_str()))
            else {
                continue;
            };
            let recreated = families.roots.contains(&key)
                || (families.members.contains_key(&key)
                    && (self.use_drop
                        || Self::hashes_differ(&table.hash, &self.to.tables[tidx].hash)));
            if !recreated {
                continue;
            }
            recreated_parents.insert(key.clone());
            // Mark for grants: recreated tables auto-inherit default
            // privileges when the new row is created, so compare_grants
            // must treat the from-side ACL as "default", not the old
            // explicit ACL. See the `is_recreated` branch there.
            self.recreated_tables.insert(key.clone());
            // Its DROP TABLE, or its root's, really runs: whatever else went
            // with the table is restored by the later passes.
            self.dropped_and_recreated_tables.insert(key);
        }
        // `ALTER SEQUENCE ... OWNED BY` for the sequences detached from a
        // recreated table before its DROP TABLE (see
        // `sequences_owned_in_family`), once every table is back.
        let mut sequence_reattach = String::new();

        // TO tables by the forms a `partition_of` reference to them takes.
        let to_index = Self::relation_index(self.to.tables.iter());

        // All table keys whose creation has been deferred
        let mut deferred_new_keys: HashSet<String> = HashSet::new();

        // We will find all new tables from "to" dump that are not in "from" dump
        // and add them to the script.
        for table in ordered_to.iter() {
            if table.hash.is_none() {
                self.script.push_str(
                    format!(
                        "/* Skipping table {}.{} due to missing hash. */\n",
                        block_comment_safe(&table.schema),
                        block_comment_safe(&table.name)
                    )
                    .as_str(),
                );
                continue;
            }
            if let Some(&fidx) = from_table_map.get(&(table.schema.as_str(), table.name.as_str())) {
                let from_table = &self.from.tables[fidx];
                if from_table.hash.is_none() {
                    self.script.push_str(
                        format!(
                            "/* Skipping table {}.{} due to missing hash. */\n",
                            block_comment_safe(&from_table.schema),
                            block_comment_safe(&from_table.name)
                        )
                        .as_str(),
                    );
                    continue;
                }
                if Self::hashes_differ(&from_table.hash, &table.hash) {
                    // Just create an alter script for the table
                    // Will be added in next comparision (below)
                }
            } else {
                // Table creation must be deferred if it belongs to a parent that is being recreated
                let parent_being_recreated_or_deferred =
                    Self::to_parent_key(&self.to.tables, &to_index, table).is_some_and(
                        |parent_key| {
                            recreated_parents.contains(&parent_key)
                                || deferred_new_keys.contains(&parent_key)
                        },
                    );

                if parent_being_recreated_or_deferred {
                    // Created in the alter loop below, once its parent is
                    // there (`TableStep::New`).
                    deferred_new_keys.insert(Self::table_key(&table.schema, &table.name));

                    continue;
                }

                // Ahead of every table alter: a policy waiting for a retype
                // is left out, for after every alter.
                let key = Self::table_key(&table.schema, &table.name);
                let held: Option<HashSet<String>> = waiting_policies
                    .get(&key)
                    .map(|entries| entries.iter().map(|(name, _)| name.clone()).collect());
                let without_held = Self::table_without_policies(table, held.as_ref());
                let table: &Table = without_held.as_ref().unwrap_or(table);
                if let Some(held) = held {
                    retype_policies.entry(key.clone()).or_default().extend(held);
                }
                created_ahead.insert(key);

                self.script.push_str(
                    format!(
                        "/* Table: {}.{}*/\n",
                        block_comment_safe(&table.schema),
                        block_comment_safe(&table.name)
                    )
                    .as_str(),
                );
                if self.output_for_production {
                    Self::emit_new_table_create_prod(
                        &mut self.script,
                        &mut self.production_post_script,
                        table,
                        &partition_ctx,
                    );
                } else {
                    self.script
                        .push_str(table.get_script_without_triggers().as_str());
                }
                self.trigger_post_script
                    .push_str(table.get_trigger_script().as_str());
            }
        }
        // NOTE: `table` above is `&&Table` (from `ordered_to.iter()`); the
        // helper takes `&Table` via deref coercion at the call boundary.

        // The CHECK constraints of a table whose rows an ancestor's alter
        // rewrites (see `InheritedRecreates::rewritten_by_ancestor`): a relax
        // to NOT ENFORCED or a drop lets go of the old CHECK here, ahead of
        // every table alter, so the rewrite does not check the rows against
        // it. The table's own alter re-adds or tightens what TO has, after.
        // The CHECKs an inheritance family drops and re-adds together (see
        // `InheritedRecreates::family_checks`) are dropped here too, parents
        // first.
        if !blockers.script.is_empty() {
            self.script.append_block(
                "\n/* ---> Dependents dropped ahead of a column drop + add: Start ---- */",
            );
            self.script.push_str(&blockers.script);
            self.script.append_block(
                "/* ---> Dependents dropped ahead of a column drop + add: End ------ */",
            );
        }
        // The policies and triggers in the way of an in-place retype (see
        // `Table::columns_retyped_in_place`).
        if !blockers.retype_script.is_empty() {
            self.script.append_block(
                "\n/* ---> Dependents dropped ahead of a column retype: Start ---- */",
            );
            self.script.push_str(&blockers.retype_script);
            self.script
                .append_block("/* ---> Dependents dropped ahead of a column retype: End ------ */");
        }

        let family_drops = inherited_recreates
            .values()
            .any(|inherited| !inherited.family_checks.is_empty());
        let mut ancestor_pre_script = String::new();
        for table in ordered_from.iter() {
            let Some(inherited) = inherited_recreates
                .get(&(table.schema.clone(), table.name.clone()))
                .filter(|inherited| {
                    inherited.rewritten_by_ancestor || !inherited.family_checks.is_empty()
                })
            else {
                continue;
            };
            if let Some(&tidx) = to_table_map.get(&(table.schema.as_str(), table.name.as_str())) {
                ancestor_pre_script.push_str(&table.ancestor_rewrite_pre_script(
                    &self.to.tables[tidx],
                    self.use_drop,
                    Self::caps_for(&alter_caps, table),
                    inherited,
                ));
            }
        }
        if !ancestor_pre_script.is_empty() {
            let (start, end) = if family_drops {
                (
                    "\n/* ---> CHECK constraints dropped ahead of the table alters: Start ---- */",
                    "/* ---> CHECK constraints dropped ahead of the table alters: End ------ */",
                )
            } else {
                (
                    "\n/* ---> CHECK constraints let go before a parent's alter rewrites their table: Start ---- */",
                    "/* ---> CHECK constraints let go before a parent's alter rewrites their table: End ------ */",
                )
            };
            self.script.append_block(start);
            self.script.push_str(&ancestor_pre_script);
            self.script.append_block(end);
        }

        // The generated columns an inheritance ancestor's recursing retype
        // needs out of the way (see `InheritedRecreates::retyped_by_ancestor`),
        // after every CHECK statement above, as a table's own alter drops its
        // CHECKs before its columns.
        // Those of a typed table's descendant ran before the composite
        // section already (see `typed_retype_pre_section`).
        let mut ancestor_retype_pre_script = String::new();
        for table in ordered_from.iter() {
            let Some(inherited) = inherited_recreates
                .get(&(table.schema.clone(), table.name.clone()))
                .filter(|inherited| !inherited.retyped_by_ancestor.is_empty())
                .filter(|_| {
                    !self
                        .typed_hoisted_tables
                        .contains(&(table.schema.clone(), table.name.clone()))
                })
            else {
                continue;
            };
            if let Some(&tidx) = to_table_map.get(&(table.schema.as_str(), table.name.as_str())) {
                ancestor_retype_pre_script.push_str(&table.ancestor_retype_pre_script(
                    &self.to.tables[tidx],
                    self.use_drop,
                    Self::caps_for(&alter_caps, table),
                    inherited,
                ));
            }
        }
        if !ancestor_retype_pre_script.is_empty() {
            self.script.append_block(
                "\n/* ---> Generated columns let go before a parent's alter retypes a column they read: Start ---- */",
            );
            self.script.push_str(&ancestor_retype_pre_script);
            self.script.append_block(
                "/* ---> Generated columns let go before a parent's alter retypes a column they read: End ------ */",
            );
        }

        // The order the loop below takes the tables in: every table in both
        // dumps in FROM order, except those that have to wait for a table the
        // migration creates or creates again (see `late_table_keys`). Those
        // come last, with the new tables deferred above, in TO order, each
        // after its TO parent and a member after its root, when that one
        // waits too.
        let mut late_keys = self.late_table_keys(
            &ordered_from,
            &to_table_map,
            &to_index,
            &families,
            &recreated_parents,
            &deferred_new_keys,
        );
        // A table attached under a parent with CHECK constraints gets them
        // in the shape TO has them before the attach (see
        // `InheritedRecreates::attach_checks`), which is the shape they are
        // in only once that parent's own alter has run; so does a table that
        // joins new `INHERITS` parents, whose columns and CHECKs it has to
        // match. Its TO descendants follow it, as their alters may use what
        // its own adds.
        let mut attaching: HashSet<String> = inherited_recreates
            .iter()
            .filter(|(_, plan)| !plan.attach_checks.is_empty() || !plan.inherit_readds.is_empty())
            .map(|((schema, name), _)| Self::table_key(schema, name))
            .collect();
        for &from_table in &ordered_from {
            if let Some(&tidx) =
                to_table_map.get(&(from_table.schema.as_str(), from_table.name.as_str()))
                && !from_table
                    .inheritance_moves(&self.to.tables[tidx])
                    .1
                    .is_empty()
            {
                attaching.insert(Self::table_key(&from_table.schema, &from_table.name));
            }
        }
        if !attaching.is_empty() {
            for &to_table in &ordered_to {
                let key = Self::table_key(&to_table.schema, &to_table.name);
                if attaching.contains(&key)
                    || !from_table_map
                        .contains_key(&(to_table.schema.as_str(), to_table.name.as_str()))
                    || deferred_new_keys.contains(&key)
                {
                    continue;
                }
                if Self::to_parent_keys(&self.to.tables, &to_index, to_table)
                    .iter()
                    .any(|parent| attaching.contains(parent))
                {
                    attaching.insert(key);
                }
            }
            late_keys.extend(attaching);
        }
        let mut steps: Vec<TableStep<'_>> = ordered_from
            .iter()
            .filter(|t| !late_keys.contains(&Self::table_key(&t.schema, &t.name)))
            .map(|&t| TableStep::Existing(t))
            .collect();
        let mut waiting: Vec<(String, TableStep<'_>, Vec<String>)> = Vec::new();
        for &to_table in &ordered_to {
            let key = Self::table_key(&to_table.schema, &to_table.name);
            let step = if deferred_new_keys.contains(&key) {
                TableStep::New(to_table)
            } else if late_keys.contains(&key)
                && let Some(&fidx) =
                    from_table_map.get(&(to_table.schema.as_str(), to_table.name.as_str()))
            {
                TableStep::Existing(&self.from.tables[fidx])
            } else {
                continue;
            };
            let mut after: Vec<String> = Self::to_parent_keys(&self.to.tables, &to_index, to_table);
            after.extend(families.members.get(&key).cloned());
            waiting.push((key, step, after));
        }
        let waiting_keys: HashSet<String> = waiting.iter().map(|(k, _, _)| k.clone()).collect();
        let mut placed: HashSet<String> = HashSet::new();
        while !waiting.is_empty() {
            // The first one whose tables are all placed; TO order when none
            // is (PostgreSQL has no cycle here).
            let next = waiting
                .iter()
                .position(|(_, _, after)| {
                    after
                        .iter()
                        .all(|k| !waiting_keys.contains(k) || placed.contains(k))
                })
                .unwrap_or(0);
            let (key, step, _) = waiting.remove(next);
            placed.insert(key);
            steps.push(step);
        }

        // Which of the waiting policies of a table not created ahead wait:
        // those its own statements would create before the alter of the last
        // table they wait for (see `waiting_policies` above).
        if !waiting_policies.is_empty() {
            let positions: HashMap<String, usize> = steps
                .iter()
                .enumerate()
                .map(|(i, step)| {
                    let (TableStep::Existing(t) | TableStep::New(t)) = step;
                    (Self::table_key(&t.schema, &t.name), i)
                })
                .collect();
            let mut owners: Vec<&String> = waiting_policies
                .keys()
                .filter(|key| !created_ahead.contains(*key))
                .collect();
            owners.sort();
            for owner_key in owners {
                let entries = &waiting_policies[owner_key];
                let Some(owner) = self
                    .to
                    .tables
                    .iter()
                    .find(|t| &Self::table_key(&t.schema, &t.name) == owner_key)
                else {
                    continue;
                };
                let Some(&owner_position) = positions.get(owner_key) else {
                    continue;
                };
                let from_owner = from_table_map
                    .get(&(owner.schema.as_str(), owner.name.as_str()))
                    .map(|&i| &self.from.tables[i]);
                let mut names: Vec<&String> = entries.iter().map(|(name, _)| name).collect();
                names.sort();
                names.dedup();
                for name in names {
                    let Some(last) = entries
                        .iter()
                        .filter(|(n, _)| n == name)
                        .filter_map(|(_, t)| positions.get(t).copied())
                        .max()
                    else {
                        continue;
                    };
                    // Created by its table's statements: a new table, one
                    // created again from TO, or a new or changed policy.
                    let created = recreated_parents.contains(owner_key)
                        || from_owner.is_none_or(|from| {
                            from.policies.iter().find(|p| &p.name == name)
                                != owner.policies.iter().find(|p| &p.name == name)
                        });
                    if owner_position < last && created {
                        retype_policies
                            .entry(owner_key.clone())
                            .or_default()
                            .insert(name.clone());
                    }
                }
            }
        }

        // The policies above, as the column-dependent restore after a table's
        // alter keys them: one a column drop + add took along too is created
        // once, after every table alter, not back right after that table's.
        let held_policy_keys: HashSet<String> = self
            .to
            .tables
            .iter()
            .filter_map(|t| {
                retype_policies
                    .get(&Self::table_key(&t.schema, &t.name))
                    .map(|names| (t, names))
            })
            .flat_map(|(t, names)| {
                t.policies
                    .iter()
                    .filter(|p| names.contains(&p.name))
                    .map(|p| format!("policy:{}.{}.{}", p.schema, p.table, p.name))
            })
            .collect();

        // We will find all existing tables in both dumps with different hashes
        for step in steps {
            let table = match step {
                TableStep::Existing(table) => table,
                TableStep::New(table) => {
                    // A new partition of a table created again above, or
                    // of another such new partition.
                    let without_held = Self::table_without_policies(
                        table,
                        retype_policies.get(&Self::table_key(&table.schema, &table.name)),
                    );
                    let table: &Table = without_held.as_ref().unwrap_or(table);
                    self.script.push_str(
                        format!(
                            "/* Table: {}.{}*/\n",
                            block_comment_safe(&table.schema),
                            block_comment_safe(&table.name)
                        )
                        .as_str(),
                    );
                    if self.output_for_production {
                        Self::emit_new_table_create_prod(
                            &mut self.script,
                            &mut self.production_post_script,
                            table,
                            &partition_ctx,
                        );
                    } else {
                        self.script
                            .push_str(table.get_script_without_triggers().as_str());
                    }
                    // Its parent is created again above, or is itself new:
                    // either way it clones the parent's row triggers by
                    // itself, and only its own triggers are created (see
                    // `Table::trigger_clones`); the clones get their state
                    // from `partition_trigger_clone_script`.
                    self.trigger_post_script
                        .push_str(table.get_trigger_script().as_str());
                    continue;
                }
            };
            if table.hash.is_none() {
                self.script.push_str(
                    format!(
                        "/* Skipping table {}.{} due to missing hash. */\n",
                        block_comment_safe(&table.schema),
                        block_comment_safe(&table.name)
                    )
                    .as_str(),
                );
                continue;
            }
            if let Some(&tidx) = to_table_map.get(&(table.schema.as_str(), table.name.as_str())) {
                let to_table = &self.to.tables[tidx];
                // Without the policies in the way of a retype, created after
                // every table alter (see `retype_policies`).
                let held = retype_policies.get(&Self::table_key(&table.schema, &table.name));
                let from_without_held = Self::table_without_policies(table, held);
                let to_without_held = Self::table_without_policies(to_table, held);
                let table: &Table = from_without_held.as_ref().unwrap_or(table);
                let to_table: &Table = to_without_held.as_ref().unwrap_or(to_table);
                if to_table.hash.is_none() {
                    self.script.push_str(
                        format!(
                            "/* Skipping table {}.{} due to missing hash. */\n",
                            block_comment_safe(&to_table.schema),
                            block_comment_safe(&to_table.name)
                        )
                        .as_str(),
                    );
                    continue;
                }
                let key = Self::table_key(&table.schema, &table.name);
                if families.members.contains_key(&key) && recreated_parents.contains(&key) {
                    // A partition that its FROM ancestor's DROP TABLE took
                    // along (see `RecreatedFamilies`): created again from TO,
                    // changed or not, instead of altered. `recreated_tables`
                    // and `dropped_and_recreated_tables` list it already, so
                    // the later passes restore what went with it.
                    self.script.push_str(
                        format!(
                            "/* Table: {}.{}*/\n",
                            block_comment_safe(&table.schema),
                            block_comment_safe(&table.name)
                        )
                        .as_str(),
                    );
                    if self.output_for_production {
                        Self::emit_new_table_create_prod(
                            &mut self.script,
                            &mut self.production_post_script,
                            to_table,
                            &partition_ctx,
                        );
                    } else {
                        self.script
                            .push_str(to_table.get_script_without_triggers().as_str());
                    }
                    // The parent's row triggers clone onto it by themselves,
                    // as for a new partition above.
                    self.trigger_post_script
                        .push_str(to_table.get_trigger_script().as_str());
                    self.tables_with_late_triggers.insert(key);
                    continue;
                }
                // A family member whose CHECK re-add the family needs is
                // altered even when nothing else about it changes.
                let in_check_family = inherited_recreates
                    .get(&(table.schema.clone(), table.name.clone()))
                    .is_some_and(|inherited| !inherited.family_checks.is_empty());
                if Self::hashes_differ(&to_table.hash, &table.hash) || in_check_family {
                    self.script.push_str(
                        format!(
                            "/* Table: {}.{}*/\n",
                            block_comment_safe(&table.schema),
                            block_comment_safe(&table.name)
                        )
                        .as_str(),
                    );
                    if let Some(warning) = Self::typed_table_left_to_uncompared_type(
                        &self.from.types,
                        &self.to.types,
                        table,
                        to_table,
                    ) {
                        self.script
                            .push_str(&format!("/* {} */\n", block_comment_safe(&warning)));
                        eprintln!("Warning: {warning}");
                    }

                    // Ask the table whether the alter path will
                    // drop+recreate it wholesale (partition-key
                    // change, partition-child column type change)
                    // before emitting. This replaces the previous
                    // substring scan of `alter_script` for
                    // `"drop table if exists"`: a single
                    // authoritative predicate that
                    // `build_alter_script` itself uses, so the
                    // comparer's gate and the SQL emission cannot
                    // drift apart.
                    let table_recreated = table.will_be_dropped_and_recreated(to_table);
                    let caps = Self::caps_for(&alter_caps, table);

                    if table_recreated {
                        // Mark for grants: the dropped+recreated table
                        // inherits default privileges, so compare_grants
                        // must use the default ACL as the effective
                        // `from_acl`. The predicate also answers true for
                        // a column type change on a table that is a
                        // partition in FROM and TO, but
                        // `build_alter_script` skips the structural
                        // changes of such a child (the parent's ALTER
                        // propagates) unless its partition key changed —
                        // so no DROP TABLE runs for it, and it is not a
                        // root of `families`.
                        self.recreated_tables.insert(key.clone());
                    }

                    // The DROP TABLE below takes the sequences the
                    // family's columns own along (see
                    // `sequences_owned_in_family`): detach them first,
                    // and give them their TO owner once every table is
                    // back.
                    if self.use_drop && families.roots.contains(&key) {
                        let (detach, reattach) = self.owned_sequence_handoff(&families, &key);
                        self.script.push_str(&detach);
                        sequence_reattach.push_str(&reattach);
                    }

                    // Columns the migration drops and re-adds (see
                    // `readded_columns`). The column predicate is asked
                    // of every table whose DROP TABLE does not run, so it
                    // also covers a partition whose parent's recursing
                    // drop + add re-creates the column, including one
                    // that `recreated_tables` lists because the parent
                    // carries its type change. Generated columns re-created
                    // out of the way of a retype count too
                    // (`Table::columns_recreated_for_retype`).
                    let recreated_for_retype =
                        table.columns_recreated_for_retype(to_table, self.use_drop, caps.generated);
                    if self.use_drop
                        && !self
                            .dropped_and_recreated_tables
                            .contains(&Self::table_key(&table.schema, &table.name))
                    {
                        for to_col in &to_table.columns {
                            if let Some(from_col) =
                                table.columns.iter().find(|c| c.name == to_col.name)
                                && (to_col
                                    .would_drop_and_re_add_with_caps(from_col, caps.generated)
                                    || recreated_for_retype.contains(&to_col.name))
                            {
                                self.readded_columns.insert((
                                    to_table.schema.clone(),
                                    to_table.name.clone(),
                                    to_col.name.clone(),
                                ));
                            }
                        }
                    }

                    // In production mode (and only when the table is altered
                    // in place — a wholesale drop+recreate re-emits the full
                    // CREATE with inline indexes anyway), defer index
                    // create/drop and re-emit them concurrently / partition-
                    // aware via the index alter plan.
                    let inherited = inherited_recreates
                        .get(&(table.schema.clone(), table.name.clone()))
                        .unwrap_or(&no_inherited_recreates);
                    let defer_indexes = self.output_for_production && !table_recreated;
                    // A recreated root that is itself a partition is
                    // created with its own triggers only: its `CREATE TABLE
                    // ... PARTITION OF` clones the parent's row triggers,
                    // which `to_table` does not list among them (see
                    // `Table::trigger_clones`), and the clones get their
                    // state from `partition_trigger_clone_script`.
                    let (alter_script, replica_identity_after) = table.get_alter_script_in_family(
                        to_table,
                        self.use_drop,
                        defer_indexes,
                        caps,
                        inherited,
                    );
                    self.script.push_str(alter_script.as_str());
                    if defer_indexes {
                        let plan = table.index_alter_plan(to_table);
                        Self::emit_index_alter_plan_prod(
                            &mut self.script,
                            &mut self.production_post_script,
                            &plan,
                            self.use_drop,
                            &partition_ctx,
                        );
                    }

                    // Issue #188 / Path B: when `get_alter_script`
                    // routes a column through its
                    // `needs_full_recreate` branch (VIRTUAL expression
                    // changes and STORED expression changes the
                    // server cannot SET EXPRESSION, STORED↔VIRTUAL
                    // flips, or non-generated → GENERATED), it emits
                    // `DROP COLUMN` + `ADD COLUMN`. PostgreSQL
                    // CASCADE-drops every index / FK / CHECK /
                    // EXCLUDE constraint / RLS policy attached to
                    // that column on the way out, and the
                    // `ADD COLUMN` alone does not restore them.
                    // Walk the column-dependent graph harvested
                    // from `pg_depend` and re-emit each surviving
                    // TO-side dependent. Skipped when the table's
                    // `DROP TABLE` runs — the re-`CREATE TABLE`
                    // already includes the dependents — but not for
                    // a partition that `table_recreated` counts only
                    // because its parent carries a column type change:
                    // the parent's recursing drop + add takes the
                    // partition's own objects on the column (see
                    // `dropped_and_recreated_tables`).
                    let table_dropped = self.dropped_and_recreated_tables.contains(&key);
                    if !table_dropped {
                        let mut col_dep_emitted: HashSet<String> = held_policy_keys.clone();
                        let mut col_dep_recreate = String::new();
                        let mut col_dep_post_commit = String::new();
                        let col_dep_prod_ctx = self.output_for_production.then_some(&partition_ctx);
                        for to_col in &to_table.columns {
                            if let Some(from_col) =
                                table.columns.iter().find(|c| c.name == to_col.name)
                                && (to_col
                                    .would_drop_and_re_add_with_caps(from_col, caps.generated)
                                    || recreated_for_retype.contains(&to_col.name))
                            {
                                self.recreate_column_dependents(
                                    &to_col.schema,
                                    &to_col.table,
                                    &to_col.name,
                                    &mut col_dep_emitted,
                                    &mut RecreateBuffers::new(
                                        &mut col_dep_recreate,
                                        &mut col_dep_post_commit,
                                    ),
                                    col_dep_prod_ctx,
                                );
                            }
                        }
                        if !col_dep_recreate.is_empty() {
                            self.script.append_block(
                                "\n/* ---> Recreate dependents dropped by virtual-column rewrite: Start ---- */",
                            );
                            self.script.push_str(&col_dep_recreate);
                            self.script.append_block(
                                "/* ---> Recreate dependents dropped by virtual-column rewrite: End ------ */",
                            );
                        }
                        self.production_post_script.push_str(&col_dep_post_commit);
                    }
                    if !table_recreated {
                        // `REPLICA IDENTITY USING INDEX` the alter left out
                        // because its index is only back after the
                        // statements above: a post-commit concurrent build,
                        // or the re-creation of a column's dependents.
                        if let Some(stmt) = replica_identity_after {
                            if self.output_for_production {
                                self.production_post_script.push_str(&stmt);
                            } else {
                                self.script.push_str(&stmt);
                            }
                        }
                    }

                    // A trigger dropped ahead of a column drop + add (see
                    // `column_drop_blockers`) that the trigger alter below
                    // does not re-create, since its definition does not
                    // change, comes back here.
                    for to_trigger in &to_table.triggers {
                        let trigger_key = format!(
                            "{}.{}",
                            Self::table_key(&to_table.schema, &to_table.name),
                            to_trigger.name
                        );
                        if self.triggers_dropped_ahead.contains(&trigger_key)
                            && table.triggers.iter().any(|t| {
                                t.name == to_trigger.name && t.definition == to_trigger.definition
                            })
                        {
                            self.trigger_post_script.push_str(
                                &to_trigger.get_recreate_script(&to_table.schema, &to_table.name),
                            );
                        }
                    }
                    self.trigger_post_script.push_str(
                        table
                            .get_trigger_alter_script_in_family(to_table, self.use_drop, inherited)
                            .as_str(),
                    );
                }
            } else {
                continue; // Table is present in both dumps, we already processed it
            }
        }

        // The CHECK adds that wait for every table alter (see
        // `InheritedRecreates::checks_after_alters`), parents first.
        let mut after_alters_script = String::new();
        for table in ordered_from.iter() {
            let Some(inherited) = inherited_recreates
                .get(&(table.schema.clone(), table.name.clone()))
                .filter(|inherited| !inherited.checks_after_alters.is_empty())
            else {
                continue;
            };
            if self
                .dropped_and_recreated_tables
                .contains(&Self::table_key(&table.schema, &table.name))
            {
                continue;
            }
            if let Some(&tidx) = to_table_map.get(&(table.schema.as_str(), table.name.as_str())) {
                after_alters_script.push_str(&table.after_alters_script(
                    &self.to.tables[tidx],
                    self.use_drop,
                    Self::caps_for(&alter_caps, table),
                    inherited,
                ));
            }
        }
        if !after_alters_script.is_empty() {
            self.script.append_block(
                "\n/* ---> CHECK constraints added after the table alters: Start ---- */",
            );
            self.script.push_str(&after_alters_script);
            self.script.append_block(
                "/* ---> CHECK constraints added after the table alters: End ------ */",
            );
        }

        // The policies in the way of an in-place retype (see
        // `retype_policies`), created from TO now that every table alter has
        // run.
        let mut retype_policy_names: Vec<(&String, &String)> = retype_policies
            .iter()
            .flat_map(|(key, names)| names.iter().map(move |name| (key, name)))
            .collect();
        retype_policy_names.sort();
        let to_by_key: HashMap<String, &Table> = self
            .to
            .tables
            .iter()
            .map(|t| (Self::table_key(&t.schema, &t.name), t))
            .collect();
        let mut retype_policy_script = String::new();
        for (key, policy) in retype_policy_names {
            if let Some(to_policy) = to_by_key
                .get(key)
                .and_then(|t| t.policies.iter().find(|p| &p.name == policy))
            {
                retype_policy_script.push_str(&policy_recreate_block(to_policy));
            }
        }
        if !retype_policy_script.is_empty() {
            self.script.append_block(
                "\n/* ---> Recreate policies dropped ahead of a column retype: Start ---- */",
            );
            self.script.push_str(&retype_policy_script);
            self.script.append_block(
                "/* ---> Recreate policies dropped ahead of a column retype: End ------ */",
            );
        }

        // A partition's `REPLICA IDENTITY USING INDEX` on an index PostgreSQL
        // creates from its parent, when the partition does not have it yet:
        // it comes from the parent's statements above, or from creating the
        // partition, so it runs once every table is done — after commit under
        // `--output-for-production`, where a new parent index is built for
        // each partition concurrently.
        for to_table in &self.to.tables {
            if to_table.hash.is_none() {
                continue;
            }
            let from_table = from_table_map
                .get(&(to_table.schema.as_str(), to_table.name.as_str()))
                .map(|&fidx| &self.from.tables[fidx]);
            if from_table.is_some_and(|from| from.hash.is_none()) {
                continue;
            }
            let recreated = self
                .recreated_tables
                .contains(&Self::table_key(&to_table.schema, &to_table.name));
            let from_table = if recreated { None } else { from_table };
            if let Some(stmt) = to_table.replica_identity_script_after_tables(from_table) {
                if self.output_for_production {
                    self.production_post_script.push_str(&stmt);
                } else {
                    self.script.push_str(&stmt);
                }
            }
        }

        // The CHECK copies the tables only inherit, once every parent's
        // statements have run (see `inherited_check_enforcement`).
        let (enforcement, warnings) =
            self.inherited_check_enforcement(&inherited_recreates, &alter_caps);
        for warning in warnings {
            eprintln!("{warning}");
        }
        if !enforcement.is_empty() {
            self.script.append_block(
                "\n/* ---> Enforceability of inherited CHECK constraint copies: Start ---- */",
            );
            self.script.push_str(&enforcement);
            self.script.append_block(
                "/* ---> Enforceability of inherited CHECK constraint copies: End ------ */",
            );
        }

        self.emit_persistence_changes();

        if !sequence_reattach.is_empty() {
            self.sequence_post_script.append_block(
                "\n/* ---> Sequences: Ownership of sequences kept across a table recreate --------------- */",
            );
            self.sequence_post_script.push_str(&sequence_reattach);
            self.sequence_post_script.append_block(
                "\n/* ---> Sequences: Ownership of sequences kept across a table recreate end --------------- */",
            );
        }

        // Once every table's triggers are in place (all of them wait in
        // `trigger_post_script`), the partition clones get their own state.
        let clone_states = self.partition_trigger_clone_script();
        self.trigger_post_script.push_str(&clone_states);

        self.script
            .append_block("\n/* ---> Tables: End section --------------- */");
        Ok(())
    }

    /// Emit `ALTER TABLE ... SET LOGGED|UNLOGGED` statements in FK
    /// topological order. PostgreSQL rejects the conversion when a
    /// related table is in the wrong persistence state at the moment
    /// the ALTER runs:
    ///
    ///   * `SET UNLOGGED` fails if any LOGGED table references the
    ///     target via FK — every FK-referencing table must already be
    ///     UNLOGGED.  Ordering: leaves before roots (reverse topo).
    ///   * `SET LOGGED`   fails if the target references any UNLOGGED
    ///     table via FK — every FK-referenced table must already be
    ///     LOGGED.  Ordering: roots before leaves (forward topo).
    ///
    /// `Table::build_alter_script` deliberately omits the persistence
    /// line so this single phase owns the order. See issue #180.
    ///
    /// **FK timing analysis** (issue #184 review thread): this phase
    /// runs at the end of `compare_tables`, before `compare_foreign_
    /// keys` has had a chance to add new FKs. The migration order is:
    ///
    ///   1. `compare_tables` per-table loop drops FKs whose
    ///      definition changed (and FROM-only FKs alongside their
    ///      table). Unchanged FKs are left in place.
    ///   2. `emit_persistence_changes` (this method) runs the SETs.
    ///   3. `compare_foreign_keys` re-adds modified FKs and creates
    ///      new ones.
    ///
    /// So the live FK graph at the SET point is exactly the
    /// **intersection** of FROM and TO by `(table, fk-name,
    /// definition)` — neither the soon-to-be-added new FKs nor the
    /// already-dropped modified FKs are active. The adjacency below
    /// is built from that intersection (rather than the TO-side
    /// constraints alone) so we don't impose ordering for FKs that
    /// won't exist at the SET point and so we don't miss ordering
    /// for FKs that DO exist but will later be dropped+re-added.
    ///
    /// **FK-cycle handling** (issue #191): a mutual FK cycle
    /// (`A → B` and `B → A`, valid in PostgreSQL when both FKs are
    /// deferrable) flipping persistence in the same direction has no
    /// valid SET order — `SET UNLOGGED A` requires B to already be
    /// UNLOGGED and vice versa. Pre-fix, `kahn_toposort` silently
    /// appended cyclic nodes alphabetically and the migration failed
    /// at apply time with the same `could not change table …` error
    /// issue #180 was meant to eliminate. The fix detects cycles via
    /// `kahn_toposort_detect_cycle`, drops every FK whose endpoints
    /// are both inside the cyclic set BEFORE the SET statements, and
    /// re-emits those FKs from their TO-side definitions AFTER the
    /// SETs — the same drop-and-recreate pattern PostgreSQL itself
    /// recommends for migrations involving deferrable cycles. Unchanged
    /// FKs that survive the cycle break are dropped+re-added inline by
    /// this phase so `compare_foreign_keys` (which only emits modified
    /// or new FKs) does not need to know about the cycle.
    fn emit_persistence_changes(&mut self) {
        let from_table_map: HashMap<(&str, &str), &Table> = self
            .from
            .tables
            .iter()
            .map(|t| ((t.schema.as_str(), t.name.as_str()), t))
            .collect();

        // Index every TO table once; the FK adjacency below and the
        // subset orderings refer to tables by their position in
        // `self.to.tables` to avoid borrow-tied pointer identity.
        // Quote-stripped lookup so FK targets parsed by
        // `parse_fk_referenced_table` (which strips the surrounding
        // quotes from a quoted identifier like `"MyTable"`) match the
        // table-side keys without requiring callers to know the
        // dump-time quoting form. PR #187 review (C2): without this
        // normalisation, mixed-case FK chains stored as `"Schema"."T"`
        // would fall back to alphabetical SET ordering and fail.
        let strip_quotes = |s: &str| -> String { s.replace('"', "") };
        let to_index_by_key: HashMap<(String, String), usize> = self
            .to
            .tables
            .iter()
            .enumerate()
            .map(|(i, t)| ((strip_quotes(&t.schema), strip_quotes(&t.name)), i))
            .collect();

        // Partition into the two flip directions; tables newly created
        // (no FROM counterpart) do not need a SET — they're created
        // with their TO-side persistence inline.
        let mut to_unlogged: Vec<usize> = Vec::new();
        let mut to_logged: Vec<usize> = Vec::new();
        for (idx, to_table) in self.to.tables.iter().enumerate() {
            let Some(from_table) = from_table_map
                .get(&(to_table.schema.as_str(), to_table.name.as_str()))
                .copied()
            else {
                continue;
            };
            match from_table.persistence_change_for(to_table) {
                Some(true) => to_unlogged.push(idx),
                Some(false) => to_logged.push(idx),
                None => {}
            }
        }

        if to_unlogged.is_empty() && to_logged.is_empty() {
            return;
        }

        // FK adjacency restricted to the **live** FK set — FKs that
        // exist in BOTH FROM and TO with the same definition. See the
        // doc comment above this method for the timing rationale.
        // `fk_targets[i]` holds the indices of tables that table `i`
        // references via FK — i.e. `i` *depends on* those.
        //
        // `edge_fks` records, for each `(referrer_idx, target_idx)`
        // edge, the underlying TO-side constraint(s) that produced it.
        // The persistence-flip cycle break (issue #191) reaches in
        // here to find which FKs to drop+re-add when the SET ordering
        // is a deadlock. Stored as cloned `TableConstraint`s so the
        // data outlives the immutable borrow of `self.to.tables` (the
        // `self.script` mutations later in the function would
        // otherwise conflict) AND so the re-emit path can call
        // `TableConstraint::get_script()` — preserving the comment
        // and any other metadata that the raw `definition` string
        // doesn't carry (PR #198 review).
        let mut fk_targets: Vec<HashSet<usize>> = vec![HashSet::new(); self.to.tables.len()];
        let mut edge_fks: HashMap<(usize, usize), Vec<TableConstraint>> = HashMap::new();
        for (i, to_table) in self.to.tables.iter().enumerate() {
            // Lift the matching FROM table once so the FK lookup
            // doesn't pay a per-constraint hashmap miss.
            let from_table = from_table_map
                .get(&(to_table.schema.as_str(), to_table.name.as_str()))
                .copied();
            for c in &to_table.constraints {
                if !c.constraint_type.eq_ignore_ascii_case("foreign key") {
                    continue;
                }
                let Some(def) = &c.definition else { continue };
                // Live edge requires the same FK to exist in FROM with
                // the same definition. A new FK (TO-only) won't be
                // present at the SET point — `compare_foreign_keys`
                // adds it later. A modified FK was dropped in the
                // per-table ALTER loop and is also absent here. Both
                // get filtered out.
                // PR #187 review (C13): an FK is also live during the
                // SET phase when its sole change between FROM and TO
                // is one of the in-place-alterable knobs
                // (deferrability / enforced / no-inherit / comment).
                // `compare_tables` does NOT drop those — they get
                // ALTERed in `compare_foreign_keys` later — so the
                // FK constraint is still active when the persistence
                // flip runs. Filtering only on byte-identical
                // definitions would miss them. Use the definitive
                // alterability check from `TableConstraint`.
                let live = from_table
                    .map(|ft| {
                        ft.constraints.iter().any(|fc| {
                            fc.name == c.name
                                && fc.constraint_type.eq_ignore_ascii_case("foreign key")
                                && (fc.definition.as_deref() == c.definition.as_deref()
                                    || fc.can_be_altered_to(c))
                        })
                    })
                    .unwrap_or(false);
                if !live {
                    continue;
                }
                // Issue #190: pass the constraint owner's schema so
                // `pg_get_constraintdef` output that omitted the
                // schema qualifier (typical for `public` reachable via
                // search_path) still resolves to a valid TO-side table
                // index. Without this, the FK edge is missing from the
                // adjacency and a persistence flip in `public` falls
                // back to alphabetical SET order — exactly the order
                // PostgreSQL rejects.
                if let Some((schema, name)) = Self::parse_fk_referenced_table(def, &to_table.schema)
                    && let Some(&j) = to_index_by_key.get(&(schema, name))
                    && j != i
                {
                    fk_targets[i].insert(j);
                    edge_fks.entry((i, j)).or_default().push(c.clone());
                }
            }
        }

        // Issue #191: collect FKs whose endpoints both sit inside a
        // SET subset's true cyclic set (Tarjan SCC of size >= 2;
        // see `topo_order_within_subset_detect_cycle`). Dropping
        // these breaks the ordering deadlock so the SET statements
        // can run in any order; we re-add the same FKs from their
        // TO `TableConstraint` clones afterwards. Returns each FK
        // exactly once even when the same edge appears in both
        // directions of a 2-node cycle.
        let collect_cycle_fks = |fk_targets: &[HashSet<usize>],
                                 cyclic_abs: &HashSet<usize>|
         -> Vec<TableConstraint> {
            let mut out: Vec<TableConstraint> = Vec::new();
            let mut seen: HashSet<(String, String, String)> = HashSet::new();
            // Iterate in a deterministic order so the emitted script
            // is stable from run to run.
            let mut cyclic_sorted: Vec<usize> = cyclic_abs.iter().copied().collect();
            cyclic_sorted.sort_unstable();
            for i in cyclic_sorted {
                let mut targets: Vec<usize> = fk_targets[i].iter().copied().collect();
                targets.sort_unstable();
                for j in targets {
                    if !cyclic_abs.contains(&j) {
                        continue;
                    }
                    if let Some(fks) = edge_fks.get(&(i, j)) {
                        for fk in fks {
                            let key = (fk.schema.clone(), fk.table_name.clone(), fk.name.clone());
                            if seen.insert(key) {
                                out.push(fk.clone());
                            }
                        }
                    }
                }
            }
            out
        };

        // Issue #191 + PR #198 review: emit the cycle-break sandwich
        // around `set_kind` SETs. `use_drop=true` runs the drops and
        // re-adds live (the normal cycle-break path). `use_drop=false`
        // honours the user's destructive-op review mode by commenting
        // out both the drops and the re-adds — the SET statements
        // themselves stay live (matching every other SET in this
        // phase), but a loud banner warns that without the drops the
        // SETs will FAIL at apply time. The re-emit uses
        // `TableConstraint::get_script()` so a FK comment
        // (`COMMENT ON CONSTRAINT ...`) survives the round-trip.
        let emit_cycle_sandwich = |out: &mut String,
                                   use_drop: bool,
                                   tables: &[Table],
                                   ordered: Vec<usize>,
                                   cycle_fks: &[TableConstraint],
                                   set_kind: &str| {
            if !cycle_fks.is_empty() {
                let banner = if use_drop {
                    "/* Persistence-flip FK cycle (issue #191): dropping cycle FKs to break the SET-ordering deadlock; re-added after the SET block below. */"
                } else {
                    "/* Persistence-flip FK cycle (issue #191): use_drop=false — cycle-break DROP/ADD statements commented out below. The SET statements that follow WILL FAIL at apply time until the cycle FKs are dropped (re-run with use_drop=true, or run the commented statements by hand). */"
                };
                out.append_block(banner);
                for fk in cycle_fks {
                    Self::emit_drop(out, use_drop, &fk.get_drop_script());
                }
            }
            for table_idx in ordered {
                let table = &tables[table_idx];
                out.append_block(&format!(
                    "alter table {}.{} set {};",
                    table.schema, table.name, set_kind
                ));
            }
            for fk in cycle_fks {
                let script = fk.get_script();
                if use_drop {
                    out.push_str(&script);
                } else {
                    // Pair the add-side with the commented drop above
                    // so use_drop=false produces an inert block.
                    out.push_str(&comment_out(&script));
                }
            }
        };

        // For SET UNLOGGED we want leaves (referencers) first. The
        // generic toposort returns `dependencies before dependents`
        // when fed `depends_on[i] = nodes i references`, so we reverse
        // its output to get the dependent-first order this direction
        // requires.
        if !to_unlogged.is_empty() {
            let (ordered, cyclic_abs) = Self::topo_order_within_subset_detect_cycle(
                &to_unlogged,
                &fk_targets,
                &self.to.tables,
                true,
            );
            let cycle_fks = collect_cycle_fks(&fk_targets, &cyclic_abs);
            emit_cycle_sandwich(
                &mut self.script,
                self.use_drop,
                &self.to.tables,
                ordered,
                &cycle_fks,
                "unlogged",
            );
        }

        // For SET LOGGED we want roots (referenced tables) first —
        // forward topo order is exactly what we need.
        if !to_logged.is_empty() {
            let (ordered, cyclic_abs) = Self::topo_order_within_subset_detect_cycle(
                &to_logged,
                &fk_targets,
                &self.to.tables,
                false,
            );
            let cycle_fks = collect_cycle_fks(&fk_targets, &cyclic_abs);
            emit_cycle_sandwich(
                &mut self.script,
                self.use_drop,
                &self.to.tables,
                ordered,
                &cycle_fks,
                "logged",
            );
        }
    }

    /// Order `subset` (absolute indices into `tables`) by FK
    /// dependencies restricted to the subset itself, and also return
    /// the set of absolute table indices that participate in a cycle.
    /// Edges to tables outside the subset are ignored: a table the
    /// migration isn't flipping doesn't constrain the SET ordering —
    /// only PostgreSQL's runtime check against the live state does,
    /// and that's the user's problem (they would have to add the
    /// missing flip to the migration). With `dependents_first = true`
    /// returns reverse-topo (leaves first — `SET UNLOGGED`); else
    /// forward-topo (roots first — `SET LOGGED`). The ordered result
    /// is a permutation of `subset`. The cyclic set lets callers
    /// break the SET-ordering deadlock (issue #191): drop the FKs
    /// whose endpoints both sit in the cyclic set, run the SETs, and
    /// re-add the FKs from their TO definitions. When the FK graph
    /// restricted to `subset` is acyclic the cyclic set is empty.
    ///
    /// PR #198 review: the cyclic set is computed via
    /// [`Comparer::strongly_connected_components`] (Tarjan), not the raw Kahn
    /// remainder. The remainder would include nodes merely *blocked
    /// by* a cycle (e.g. `C` in `A↔B + A→C`), and treating them as
    /// cycle participants would drop FKs that are not actually in
    /// any cycle — making migrations more destructive than needed.
    /// Filtering to SCCs of size >= 2 keeps the drop surgical to
    /// the true cycle edges.
    fn topo_order_within_subset_detect_cycle(
        subset: &[usize],
        fk_targets: &[HashSet<usize>],
        tables: &[Table],
        dependents_first: bool,
    ) -> (Vec<usize>, HashSet<usize>) {
        let n = subset.len();
        let pos_in_subset: HashMap<usize, usize> = subset
            .iter()
            .enumerate()
            .map(|(pos, &abs)| (abs, pos))
            .collect();

        let mut depends_on: Vec<HashSet<usize>> = vec![HashSet::new(); n];
        for (pos, &abs_idx) in subset.iter().enumerate() {
            for &target_abs in &fk_targets[abs_idx] {
                if let Some(&target_pos) = pos_in_subset.get(&target_abs)
                    && target_pos != pos
                {
                    depends_on[pos].insert(target_pos);
                }
            }
        }

        let (mut sorted, _kahn_remainder) = Self::kahn_toposort_detect_cycle(n, &depends_on, |i| {
            let t = &tables[subset[i]];
            (t.schema.to_lowercase(), t.name.to_lowercase())
        });
        if dependents_first {
            sorted.reverse();
        }

        // Narrow the cycle set to *true* SCC members (size >= 2).
        // Self-loops are impossible here — `fk_targets` is built
        // with the `j != i` guard in `emit_persistence_changes` — so
        // a singleton SCC always means an acyclic node.
        let sccs = Self::strongly_connected_components(n, &depends_on);
        let mut true_cyclic_pos: HashSet<usize> = HashSet::new();
        for scc in sccs {
            if scc.len() >= 2 {
                true_cyclic_pos.extend(scc);
            }
        }

        let ordered_abs: Vec<usize> = sorted.into_iter().map(|pos| subset[pos]).collect();
        let cyclic_abs: HashSet<usize> =
            true_cyclic_pos.into_iter().map(|pos| subset[pos]).collect();
        (ordered_abs, cyclic_abs)
    }

    /// Pull the `schema.name` of the table targeted by an FK constraint
    /// definition (`pg_get_constraintdef` output, e.g.
    /// `FOREIGN KEY (col) REFERENCES schema.table(col)`). Tolerates
    /// quoted identifiers; returns `None` when the definition isn't
    /// recognisably an FK or the referenced identifier is unqualified.
    ///
    /// The keyword scan is anchored to a whole-word match — a naive
    /// `find("references ")` would false-match a column name like
    /// `"references "` that happened to appear inside the FK column
    /// list (or any earlier substring containing the literal text).
    /// The schema/name split is quote-aware so a quoted identifier
    /// that itself contains a literal `.`
    /// (e.g. `REFERENCES "weird.schema"."t"(id)`) is split at the
    /// dot OUTSIDE the quotes, not the first dot in byte order.
    ///
    /// The case-insensitive haystack is built with [`str::to_ascii_
    /// lowercase`] rather than [`str::to_lowercase`]: the latter can
    /// change byte length for some non-ASCII characters (e.g. capital
    /// Turkish dotted I, which lowercases into a multi-character
    /// sequence with a different UTF-8 length), and we slice back
    /// into the original `def` using offsets that came from the
    /// haystack — a length-changing lowercasing would leave those
    /// offsets pointing inside a UTF-8 codepoint and panic.
    /// `to_ascii_lowercase` is byte-length-preserving (only ASCII
    /// letters change, every other byte is left intact) and the
    /// keyword `references` is pure ASCII, so it's the right tool.
    /// Parse the referenced `(schema, table)` pair out of a FOREIGN KEY
    /// constraint's deparsed definition. `owner_schema` is the schema of
    /// the table that *owns* the constraint and is used to resolve
    /// unqualified targets (issue #190): `pg_get_constraintdef` may omit
    /// the schema qualifier when the target is reachable via
    /// `search_path` — most commonly the `public` schema — and without
    /// this fallback the FK edge is missed by
    /// `emit_persistence_changes`, leaving FK-chained persistence flips
    /// in `public` ordered alphabetically (which PostgreSQL rejects with
    /// the `could not change table … to logged/unlogged` error).
    ///
    /// The fallback resolves to `(owner_schema, target)`, matching
    /// PostgreSQL's own search_path resolution for the simplest and
    /// commonest case where the FK target lives in the same schema as
    /// the FK. Per-role / per-database `search_path` settings beyond
    /// the owner schema are not currently captured at dump time, so an
    /// FK referencing a `public` table from a non-`public` schema (with
    /// `public` on the search path) will still escape this fallback;
    /// that path is documented as a known limitation.
    fn parse_fk_referenced_table(def: &str, owner_schema: &str) -> Option<(String, String)> {
        let lower = def.to_ascii_lowercase();
        let bytes = lower.as_bytes();
        let kw = b"references";
        let kw_len = kw.len();

        // Walk every occurrence of "references" until we find one that
        // sits at an identifier word boundary AND is followed by
        // whitespace — `pg_get_constraintdef` always renders the
        // keyword that way. Substring matches inside quoted column
        // names (the FK column list precedes the keyword) fail one of
        // these checks: a quoted `"references"` is followed by `"`
        // (not whitespace), `references_table` is followed by `_`
        // (still an identifier char), and `xxx_references` is
        // preceded by `_`.
        // Track double-quoted regions across the whole scan so a
        // column literally named `"my references col"` (the keyword
        // appears mid-quote but is preceded and followed by spaces)
        // doesn't pass the boundary check. PR #187 review.
        let mut quote_open_at: Vec<usize> = Vec::new();
        for (i, &b) in bytes.iter().enumerate() {
            if b == b'"' {
                if quote_open_at.last().copied() == Some(usize::MAX) {
                    quote_open_at.pop();
                } else {
                    quote_open_at.push(usize::MAX);
                }
                quote_open_at.push(i);
            }
        }
        // Reduce to a flat list of (open, close) byte ranges.
        let mut quoted_ranges: Vec<(usize, usize)> = Vec::new();
        let mut iter = quote_open_at.into_iter();
        while let (Some(_), Some(open)) = (iter.next(), iter.next()) {
            if let (Some(_), Some(close)) = (iter.next(), iter.next()) {
                quoted_ranges.push((open, close));
            }
        }
        let in_quoted_range = |idx: usize| -> bool {
            quoted_ranges
                .iter()
                .any(|&(open, close)| idx > open && idx < close)
        };

        let mut search_from = 0;
        let kw_pos = loop {
            if search_from + kw_len > bytes.len() {
                return None;
            }
            let rel = lower[search_from..].find("references")?;
            let pos = search_from + rel;
            let left_ok = pos == 0
                || !{
                    let b = bytes[pos - 1];
                    b.is_ascii_alphanumeric() || b == b'_' || b == b'"' || b == b'$'
                };
            let after_kw = pos + kw_len;
            let right_ok = after_kw < bytes.len() && bytes[after_kw].is_ascii_whitespace();
            if left_ok && right_ok && !in_quoted_range(pos) {
                break pos;
            }
            // Advance one byte to keep ASCII-byte indices valid; the
            // haystack is `lower` which is purely ASCII at byte 0 of
            // each match candidate (because `find` lands on the
            // ASCII keyword `references`).
            search_from = pos + 1;
        };

        // Skip the keyword and the run of whitespace immediately after.
        let after = (def[kw_pos + kw_len..]).trim_start();

        // Scan a single qualified identifier: identifier chars, dot,
        // or quoted segments. Stop at the first non-identifier byte
        // outside quotes (typically `(` for the column list). PG
        // identifiers may include `$`, so include it here — PR #187
        // review.
        let mut end = 0;
        let mut in_quotes = false;
        for (i, ch) in after.char_indices() {
            if ch == '"' {
                in_quotes = !in_quotes;
                end = i + ch.len_utf8();
                continue;
            }
            if in_quotes {
                end = i + ch.len_utf8();
                continue;
            }
            if ch.is_alphanumeric() || ch == '_' || ch == '.' || ch == '$' {
                end = i + ch.len_utf8();
            } else {
                break;
            }
        }
        let target = &after[..end];
        match Self::split_qualified_name_quote_aware(target) {
            Some(pair) => Some(pair),
            None => {
                // No dot outside quotes — unqualified target. Resolve
                // against the constraint's owning schema (issue #190).
                // `trim_matches('"')` on both halves mirrors what
                // `split_qualified_name_quote_aware` already does for
                // the schema-qualified path: callers (and the existing
                // tests) expect pre-stripped strings, and dump-time
                // identifiers are stored as `quote_ident`-wrapped
                // forms (`MySchema` → `"MySchema"`) which would
                // otherwise miss the comparer's normalised lookup
                // keys.
                let name = target.trim_matches('"').to_string();
                if name.is_empty() {
                    None
                } else {
                    Some((owner_schema.trim_matches('"').to_string(), name))
                }
            }
        }
    }

    /// Split a `schema.name` identifier at the first dot that lives
    /// OUTSIDE any double-quoted segment. PostgreSQL allows quoted
    /// identifiers to contain literal dots (`"weird.schema"`), so the
    /// naive `target.find('.')` would split inside the quoted segment
    /// and produce an invalid pair.
    fn split_qualified_name_quote_aware(target: &str) -> Option<(String, String)> {
        let bytes = target.as_bytes();
        let mut in_quotes = false;
        let mut split_at = None;
        for (i, &b) in bytes.iter().enumerate() {
            if b == b'"' {
                in_quotes = !in_quotes;
            } else if b == b'.' && !in_quotes {
                split_at = Some(i);
                break;
            }
        }
        let dot = split_at?;
        let schema = target[..dot].trim_matches('"').to_string();
        let name = target[dot + 1..].trim_matches('"').to_string();
        if schema.is_empty() || name.is_empty() {
            None
        } else {
            Some((schema, name))
        }
    }

    // Comparing foreign keys
    async fn compare_foreign_keys(&mut self) -> Result<(), Error> {
        self.script
            .append_block("\n/* ---> Foreign Keys: Start section --------------- */");

        let from_table_map: HashMap<(&str, &str), usize> = self
            .from
            .tables
            .iter()
            .enumerate()
            .map(|(i, t)| ((t.schema.as_str(), t.name.as_str()), i))
            .collect();

        for table in &self.to.tables {
            if table.hash.is_none() {
                continue;
            }
            let from_table = from_table_map
                .get(&(table.schema.as_str(), table.name.as_str()))
                .map(|&fidx| &self.from.tables[fidx]);
            if from_table.is_some_and(|from| from.hash.is_none()) {
                continue;
            }
            // A table `compare_tables` dropped and created again from TO has
            // none of its foreign keys: `CREATE TABLE` leaves them to this
            // pass, so they are added like a new table's. With `use_drop` off
            // the DROP TABLE is commented out and they are still there.
            let recreated = self.use_drop
                && self
                    .dropped_and_recreated_tables
                    .contains(&Self::table_key(&table.schema, &table.name));
            let from_table = if recreated { None } else { from_table };
            // PostgreSQL adds a foreign key `NOT VALID` to a partitioned
            // table only from 18 on; an older server adds and validates it in
            // one statement.
            let not_valid_supported = table.partition_key.is_none()
                || self.migrated_server_at_least(production::NOT_VALID_PARTITIONED_FK_MIN_VERSION);

            if let Some(from_table) = from_table {
                // Table exists in both. Check for FK changes.
                if self.output_for_production {
                    // In-place FK modifications stay as-is; brand-new foreign
                    // keys are added NOT VALID and validated after commit.
                    let (alters, new_fks) = from_table.foreign_key_alter_split(table);
                    self.script.push_str(&alters);
                    for constraint in new_fks {
                        Self::emit_production_foreign_key(
                            &mut self.script,
                            &mut self.production_post_script,
                            constraint,
                            not_valid_supported,
                        );
                    }
                } else {
                    self.script
                        .push_str(&from_table.get_foreign_key_alter_script(table));
                }
            } else {
                // New or recreated table. Add its FKs.
                if self.output_for_production {
                    for constraint in &table.constraints {
                        if constraint.constraint_type.to_lowercase() == "foreign key" {
                            Self::emit_production_foreign_key(
                                &mut self.script,
                                &mut self.production_post_script,
                                constraint,
                                not_valid_supported,
                            );
                        }
                    }
                } else {
                    self.script.push_str(&table.get_foreign_key_script());
                }
            }
        }

        self.script
            .append_block("\n/* ---> Foreign Keys: End section --------------- */");
        Ok(())
    }

    /// Add the foreign key `constraint` under `--output-for-production`:
    /// `NOT VALID` in the transaction and validated after commit
    /// ([`production::foreign_key_split`]), or in one statement when the
    /// migrated server cannot add it `NOT VALID` (`not_valid_supported` is
    /// false: a partitioned table before PostgreSQL 18).
    fn emit_production_foreign_key(
        script: &mut String,
        post_commit: &mut String,
        constraint: &TableConstraint,
        not_valid_supported: bool,
    ) {
        if not_valid_supported {
            let split = production::foreign_key_split(constraint);
            script.push_str(&split.in_txn);
            post_commit.push_str(&split.post_commit);
        } else {
            script.push_str(&constraint.get_script());
        }
    }

    /// Drop the FROM views that are going away or have to be rebuilt, and the
    /// routines with a SQL-standard body in the way of a change (see
    /// `Comparer::sql_body_routine_predrop_seeds`), in reverse dependency
    /// order, and record each in `dropped_views` / `predropped_routines`.
    ///
    /// The rules that have to be out of the way first — FROM-only rules, and
    /// rules reading a view dropped here or a table or column the table
    /// section drops or retypes, or calling a routine dropped here or later —
    /// are dropped just before the views (see `Comparer::drop_rules`). The
    /// view, routine and rule plans all work from one
    /// `Comparer::relation_changes`.
    ///
    /// A generated column that a routine's `DROP ... CASCADE` takes, here or
    /// in the routine phase, counts among the changed columns (see
    /// `Comparer::cascade_readded_columns`): what reads it goes with it, so
    /// it is dropped here first and re-created once the column is back.
    /// Which columns those are depends on which routines are dropped here,
    /// which in turn depends on the changed columns, so the plan is redone
    /// until they agree; the set only grows. The final set is recorded in
    /// `cascade_readded` for the later passes.
    async fn drop_views(&mut self) -> Result<(), Error> {
        // Planned below, once the routines dropped here are known; nothing in
        // the planning may read a stale set.
        self.cascade_victim_routines.clear();
        self.predropped_routines.clear();
        let mut changes = self.relation_changes();
        let drops = loop {
            let drops = self.plan_view_drops(&changes);
            if !self.use_drop {
                break drops;
            }
            // The routines these drops and the routine phase drop with
            // `CASCADE`, provisionally, for `cascade_readded_columns`.
            let predropping: HashSet<(String, String, String)> = drops
                .iter()
                .filter_map(|drop| match drop {
                    ViewPhaseDrop::Routine(idx) => {
                        let routine = &self.from.routines[*idx];
                        Some((
                            routine.schema.clone(),
                            routine.name.clone(),
                            routine.arguments.clone(),
                        ))
                    }
                    _ => None,
                })
                .collect();
            self.cascade_victim_routines = self.plan_cascade_victims(&changes, &predropping);
            self.predropped_routines = predropping;
            let taken = self.cascade_taken_column_keys(&self.cascade_readded_columns());
            self.cascade_victim_routines.clear();
            self.predropped_routines.clear();
            let known = changes.changed_columns.len();
            changes.changed_columns.extend(taken);
            if changes.changed_columns.len() == known {
                break drops;
            }
        };

        let mut dropping_views: HashSet<String> = HashSet::new();
        let mut dropping_routines: HashSet<String> = HashSet::new();
        let mut predropping: HashSet<(String, String, String)> = HashSet::new();
        for drop in &drops {
            match drop {
                ViewPhaseDrop::View { key, .. } => {
                    dropping_views.insert(key.clone());
                }
                ViewPhaseDrop::Routine(idx) => {
                    let routine = &self.from.routines[*idx];
                    dropping_routines.insert(routine.dependency_key());
                    predropping.insert((
                        routine.schema.clone(),
                        routine.name.clone(),
                        routine.arguments.clone(),
                    ));
                }
                ViewPhaseDrop::KeptRoutine { .. } => {}
            }
        }
        // What the routine phase's CASCADE takes along besides the routines
        // it drops; a rule calling one of those goes with them too.
        self.cascade_victim_routines = self.plan_cascade_victims(&changes, &predropping);
        let mut rule_routines = dropping_routines;
        rule_routines.extend(
            self.from
                .routines
                .iter()
                .filter(|r| {
                    self.cascade_victim_routines.contains(&(
                        r.schema.clone(),
                        r.name.clone(),
                        r.arguments.clone(),
                    ))
                })
                .map(Routine::dependency_key),
        );
        self.drop_rules(&changes, &dropping_views, &rule_routines);

        self.script
            .append_block("\n/* ---> Views DROP: Start section --------------- */");

        self.dropped_views.clear();
        self.predropped_routines.clear();
        self.detached_domain_defaults.clear();
        for drop in drops {
            match drop {
                ViewPhaseDrop::Routine(idx) => {
                    let routine = &self.from.routines[idx];
                    self.script.push_str(
                        format!(
                            "/* Routine: {}.{}({})*/\n",
                            block_comment_safe(&routine.schema),
                            block_comment_safe(&routine.name),
                            block_comment_safe(&routine.arguments)
                        )
                        .as_str(),
                    );
                    self.script.push_str(
                        "/* Routine uses a relation, routine or operator this migration drops or rebuilds: dropped before it, re-created after it. */\n",
                    );
                    self.emit_domain_default_detach(idx);
                    let routine = &self.from.routines[idx];
                    self.script.push_str(&routine.get_drop_script());
                    self.predropped_routines.insert((
                        routine.schema.clone(),
                        routine.name.clone(),
                        routine.arguments.clone(),
                    ));
                }
                ViewPhaseDrop::KeptRoutine { index, reason } => {
                    let routine = &self.from.routines[index];
                    let signature =
                        format!("{}.{}({})", routine.schema, routine.name, routine.arguments);
                    eprintln!(
                        "Warning: routine {signature} is in the way of this migration but is not \
                         dropped ahead of it: {reason}. The statement that needs the routine gone \
                         will fail; resolve it by hand."
                    );
                    // One `--` line: a name may hold a line break.
                    let note = format!(
                        "Routine {signature} is in the way of this migration but is not dropped ahead of it: {reason} (manual intervention needed)."
                    )
                    .replace(['\n', '\r'], " ");
                    self.script
                        .push_str(&format!("-- {note}").with_empty_lines());
                }
                ViewPhaseDrop::View { index, key, active } => {
                    let from_view = &self.from.views[index];

                    self.script.push_str(
                        format!(
                            "/* View: {}.{}*/\n",
                            block_comment_safe(&from_view.schema),
                            block_comment_safe(&from_view.name)
                        )
                        .as_str(),
                    );

                    let drop_script = from_view.get_drop_script();
                    if active {
                        self.script.push_str(drop_script.as_str());
                    } else {
                        self.script.push_str(comment_out(&drop_script).as_str());
                    }
                    self.dropped_views.insert(key, active);
                }
            }
        }

        self.script
            .append_block("\n/* ---> Views DROP: End section --------------- */");
        self.cascade_readded = self.cascade_readded_columns();
        Ok(())
    }

    /// What `drop_views` drops, in drop order (dependents first): the FROM
    /// views going away or being rebuilt, the views the routine phase's
    /// `CASCADE` would take along with what reads them
    /// (`Comparer::views_cascaded_with_readers`), and the routines with a
    /// SQL-standard body that go with them (see
    /// `Comparer::sql_body_routine_predrop_seeds`).
    /// A view's drop is active only with `use_drop`; otherwise it is emitted
    /// commented out. Routines are only ever planned with `use_drop`.
    fn plan_view_drops(&self, changes: &RelationChanges) -> Vec<ViewPhaseDrop> {
        let dependent_views = self.dependent_view_keys(changes);
        let cascaded_with_readers = self.views_cascaded_with_readers(changes);

        // Build O(1) lookup for target views
        let to_view_map: HashMap<(&str, &str), usize> = self
            .to
            .views
            .iter()
            .enumerate()
            .map(|(i, v)| ((v.schema.as_str(), v.name.as_str()), i))
            .collect();

        // Collect all views that should be dropped: (from_view index, normalized_key, force_drop)
        let mut candidates: Vec<(usize, String, bool)> = Vec::new();

        for (idx, from_view) in self.from.views.iter().enumerate() {
            let normalized_view = Self::normalized_view_key(&from_view.schema, &from_view.name);

            let is_dependent = dependent_views.contains(&normalized_view);

            let to_view = to_view_map
                .get(&(from_view.schema.as_str(), from_view.name.as_str()))
                .map(|&i| &self.to.views[i]);

            let is_from_only = to_view.is_none();

            let is_changed_mat_view = from_view.is_materialized
                && to_view
                    .map(|tv| Self::hashes_differ(&from_view.hash, &tv.hash))
                    .unwrap_or(false);

            // Kind transition (regular <-> materialized) requires drop+recreate
            // because neither supports an in-place ALTER to the other kind.
            let is_kind_transition = to_view
                .map(|tv| from_view.is_materialized != tv.is_materialized)
                .unwrap_or(false);

            // A changed regular view whose column list is not an exact prefix of the
            // target's (column inserted, reordered, renamed, retyped, or dropped)
            // cannot be updated with CREATE OR REPLACE VIEW — PostgreSQL rejects it
            // with "cannot change name of view column" (issue #227). Route it through
            // drop+recreate; when either dump predates column capture the check
            // returns compatible and the historical OR REPLACE path is kept.
            let is_incompatible_regular_change = !from_view.is_materialized
                && to_view
                    .map(|tv| {
                        !tv.is_materialized
                            && Self::hashes_differ(&from_view.hash, &tv.hash)
                            && !from_view.or_replace_compatible(tv)
                    })
                    .unwrap_or(false);

            // A view the routine phase's CASCADE would take along with what
            // reads it (see `Comparer::views_cascaded_with_readers`).
            let is_cascaded_with_readers = cascaded_with_readers.contains(&idx);

            let should_drop = is_dependent
                || is_from_only
                || is_changed_mat_view
                || is_kind_transition
                || is_incompatible_regular_change
                || is_cascaded_with_readers;

            if should_drop {
                // The drop is emitted as active SQL only when use_drop is true.
                // Changed materialized views still *need* a drop+recreate, but when
                // use_drop is false the DROP (and its dependent CREATE) are commented
                // out so the user can review them manually.
                let drop_is_active = self.use_drop;
                candidates.push((idx, normalized_view, drop_is_active));
            }
        }

        // Routines with a SQL-standard body are hard dependents of what they
        // use (see `Comparer::sql_body_routine_predrop_seeds`). The ones in the
        // way of a drop or rebuild ride along with the views: dropped here,
        // before any table DDL, and re-created from the TO dump by
        // `compare_routines_and_views`, after the objects they use. Indices
        // into `self.from.routines`.
        let seeds: Vec<usize> = self.sql_body_routine_predrop_seeds(changes);
        let base_candidates = candidates;
        let expanding_at_all = !base_candidates.is_empty() || !seeds.is_empty();

        // Prelowered definition and lowered (schema, name) for every FROM view,
        // computed once and shared by the dependency-expansion fixpoint below and the
        // drop-ordering pass after it. Indexed by FROM-view position, so it stays
        // valid while the candidate set grows. Skipped entirely when nothing drops.
        let prelowered_defs: Vec<(String, String)> = if !expanding_at_all {
            Vec::new()
        } else {
            self.from
                .views
                .iter()
                .map(|v| Self::prelower_pair_for_dependency_scan(&v.definition))
                .collect()
        };
        let names_lc: Vec<(String, String)> = if !expanding_at_all {
            Vec::new()
        } else {
            self.from
                .views
                .iter()
                .map(|v| (v.schema.to_lowercase(), v.name.to_lowercase()))
                .collect()
        };

        // Routines left in place although they are in the way: dropping one
        // with CASCADE would take along something pgc cannot put back (see
        // `Comparer::unrestorable_cascade_dependents`), and so would dropping
        // anything it depends on. With the reason, for `drop_views` to say so.
        let mut kept_back: Vec<(usize, String)> = Vec::new();
        let (candidates, routine_candidates) = loop {
            let mut candidates = base_candidates.clone();
            let mut routine_candidates: Vec<usize> = seeds
                .iter()
                .copied()
                .filter(|i| !kept_back.iter().any(|(k, _)| k == i))
                .collect();
            let mut routine_candidate_set: HashSet<usize> =
                routine_candidates.iter().copied().collect();
            let mut expanding = expanding_at_all;

            // A view that reads another view being dropped must itself be dropped first
            // and recreated after: DROP VIEW runs without CASCADE, so it fails while any
            // dependent view still exists (issue #227). Expand the candidate set to a
            // fixpoint so whole dependency chains ride along; every addition is recreated
            // from the TO dump afterwards because dropped_views membership marks it for
            // action in compare_routines_and_views.
            //
            // The same holds across routines with a SQL-standard body, both ways: such
            // a routine reading a view being dropped goes first, and a view calling a
            // routine being dropped (its CASCADE would take the view) goes too, as does
            // a routine calling one, an aggregate built on one, or a routine whose
            // argument defaults call one.
            let mut candidate_keys: HashSet<String> =
                candidates.iter().map(|(_, key, _)| key.clone()).collect();
            while expanding {
                let mut added = false;
                // Phase 7's matcher form: quote-stripped, lowercased.
                let routine_names: HashSet<(String, String)> = routine_candidates
                    .iter()
                    .map(|&i| Self::unquoted_routine_name(&self.from.routines[i]))
                    .collect();
                for (idx, from_view) in self.from.views.iter().enumerate() {
                    let normalized_view =
                        Self::normalized_view_key(&from_view.schema, &from_view.name);
                    if candidate_keys.contains(&normalized_view) {
                        continue;
                    }
                    // FROM-only views are already candidates via is_from_only, so only
                    // views that still exist in TO can be reached here.
                    let (def_lower, def_unquoted) = &prelowered_defs[idx];
                    let references_candidate =
                        from_view.table_relation.iter().any(|rel| {
                            candidate_keys.contains(&Self::normalized_view_reference(rel))
                        }) || candidates.iter().any(|(cidx, _, _)| {
                            let (schema_lc, name_lc) = &names_lc[*cidx];
                            Self::text_references_qualified_name_pre(
                                def_lower,
                                def_unquoted,
                                schema_lc,
                                name_lc,
                            )
                        }) || (!routine_names.is_empty()
                            && Self::definition_references_any(
                                &from_view.definition,
                                &routine_names,
                            ));
                    if references_candidate {
                        candidate_keys.insert(normalized_view.clone());
                        candidates.push((idx, normalized_view, self.use_drop));
                        added = true;
                    }
                }
                if self.use_drop {
                    let routine_keys: HashSet<String> = routine_candidates
                        .iter()
                        .map(|&i| self.from.routines[i].dependency_key())
                        .collect();
                    for (idx, routine) in self.from.routines.iter().enumerate() {
                        if routine_candidate_set.contains(&idx)
                            || kept_back.iter().any(|(k, _)| *k == idx)
                        {
                            continue;
                        }
                        let deps = &routine.sql_body_dependencies;
                        let uses_candidate = routine.is_sql_body
                            && (deps.relations.iter().any(|rel| {
                                candidate_keys.contains(&Self::normalized_view_reference(rel))
                            }) || deps.routines.iter().any(|key| routine_keys.contains(key)));
                        let aggregates_candidate = routine.aggregate_info.is_some()
                            && routine_names.iter().any(|(schema, name)| {
                                Self::aggregate_references_function(routine, schema, name)
                            });
                        let defaults_call_candidate =
                            Self::defaults_call_any(routine, &routine_names);
                        if uses_candidate || aggregates_candidate || defaults_call_candidate {
                            routine_candidate_set.insert(idx);
                            routine_candidates.push(idx);
                            added = true;
                        }
                    }
                }
                expanding = added;
            }

            let newly_kept = self.routines_to_keep_back(&routine_candidates, changes);
            if newly_kept.is_empty() {
                break (candidates, routine_candidates);
            }
            kept_back.extend(newly_kept);
        };

        // Sort drops in reverse dependency order (dependents first). Nodes are the
        // view candidates, then the routine candidates.
        let view_count = candidates.len();
        let node_count = view_count + routine_candidates.len();
        let drop_order = if node_count > 1 {
            let key_to_pos: HashMap<String, usize> = candidates
                .iter()
                .enumerate()
                .map(|(i, (_, key, _))| (key.clone(), i))
                .collect();
            let routine_key_to_pos: HashMap<String, usize> = routine_candidates
                .iter()
                .enumerate()
                .map(|(k, &ridx)| (self.from.routines[ridx].dependency_key(), view_count + k))
                .collect();
            let routine_names: Vec<(String, String)> = routine_candidates
                .iter()
                .map(|&ridx| Self::unquoted_routine_name(&self.from.routines[ridx]))
                .collect();

            let mut depends_on: Vec<HashSet<usize>> = vec![HashSet::new(); node_count];

            for (i, (view_idx, _, _)) in candidates.iter().enumerate() {
                let view = &self.from.views[*view_idx];
                // Check table_relation
                for rel in &view.table_relation {
                    let normalized = Self::normalized_view_reference(rel);
                    if let Some(&j) = key_to_pos.get(&normalized)
                        && i != j
                    {
                        depends_on[i].insert(j);
                    }
                }
                // Check definition text for references to other dropping views
                let (def_lower, def_unquoted) = &prelowered_defs[*view_idx];
                for (j, (cand_idx, _, _)) in candidates.iter().enumerate() {
                    let (schema_lc, name_lc) = &names_lc[*cand_idx];
                    if i != j
                        && Self::text_references_qualified_name_pre(
                            def_lower,
                            def_unquoted,
                            schema_lc,
                            name_lc,
                        )
                    {
                        depends_on[i].insert(j);
                    }
                }
                // A view calling a routine being dropped goes before it.
                for (k, name) in routine_names.iter().enumerate() {
                    if Self::definition_references_any(
                        &view.definition,
                        &HashSet::from([name.clone()]),
                    ) {
                        depends_on[i].insert(view_count + k);
                    }
                }
            }
            for (k, &ridx) in routine_candidates.iter().enumerate() {
                let i = view_count + k;
                let routine = &self.from.routines[ridx];
                let deps = &routine.sql_body_dependencies;
                for rel in &deps.relations {
                    if let Some(&j) = key_to_pos.get(&Self::normalized_view_reference(rel)) {
                        depends_on[i].insert(j);
                    }
                }
                for key in &deps.routines {
                    if let Some(&j) = routine_key_to_pos.get(key)
                        && i != j
                    {
                        depends_on[i].insert(j);
                    }
                }
                for (k2, (schema, name)) in routine_names.iter().enumerate() {
                    if k2 != k
                        && (Self::aggregate_references_function(routine, schema, name)
                            || Self::defaults_call_any(
                                routine,
                                &HashSet::from([(schema.clone(), name.clone())]),
                            ))
                    {
                        depends_on[i].insert(view_count + k2);
                    }
                }
            }

            // Topological sort in creation order, then reverse for drops.
            Self::warn_dependency_cycle("views being dropped", node_count, &depends_on, |i| {
                if i < view_count {
                    let v = &self.from.views[candidates[i].0];
                    format!("{}.{}", v.schema, v.name)
                } else {
                    self.from.routines[routine_candidates[i - view_count]].dependency_key()
                }
            });
            let mut sorted = Self::kahn_toposort(node_count, &depends_on, |i| {
                if i < view_count {
                    let v = &self.from.views[candidates[i].0];
                    (
                        0u8,
                        v.schema.to_lowercase(),
                        v.name.to_lowercase(),
                        String::new(),
                    )
                } else {
                    let r = &self.from.routines[routine_candidates[i - view_count]];
                    (
                        1u8,
                        r.schema.to_lowercase(),
                        r.name.to_lowercase(),
                        r.arguments.to_lowercase(),
                    )
                }
            });
            sorted.reverse();
            sorted
        } else {
            (0..node_count).collect()
        };

        kept_back
            .into_iter()
            .map(|(index, reason)| ViewPhaseDrop::KeptRoutine { index, reason })
            .chain(drop_order.into_iter().map(|i| {
                if i < view_count {
                    let (index, ref key, active) = candidates[i];
                    ViewPhaseDrop::View {
                        index,
                        key: key.clone(),
                        active,
                    }
                } else {
                    ViewPhaseDrop::Routine(routine_candidates[i - view_count])
                }
            }))
            .collect()
    }

    /// FROM views, as indices into `from.views`, that `drop_views` drops
    /// ahead of the relation DDL because the routine phase's `DROP …
    /// CASCADE` would otherwise take them together with something that reads
    /// them.
    ///
    /// A view calling a routine the routine phase drops with `CASCADE` — a
    /// FROM-only one, one `Comparer::routine_is_recreated`, or one that
    /// `CASCADE` takes along (`Comparer::plan_cascade_victims`) — goes with
    /// it, and so does everything reading that view: other views and
    /// materialized views (with their indexes and statistics), routines with
    /// a SQL-standard body, and the rules on or reading any of them.
    /// `recreate_routine_dependents` puts back only the views whose own text
    /// calls the routine, so it is left the ones nothing else reads. A view
    /// something reads is dropped here instead, which makes `plan_view_drops`
    /// take its readers along, dependents first, and `compare_routines_and_views`
    /// re-create all of them from TO, each after what it reads — the view
    /// itself after the routine it calls.
    ///
    /// Whether a view calls such a routine is read from the routines its
    /// `_RETURN` rule depends on (`View::routine_dependencies`), matched by
    /// signature: a name match would also take a view calling an unchanged
    /// overload, or a built-in of the same name, and rebuild it — a
    /// materialized view's data included — for nothing, losing what pgc does
    /// not model on it, such as an `INSTEAD OF` trigger. A dump without them
    /// (pgc 1.0.x) leaves every view to `recreate_routine_dependents`, as
    /// before.
    ///
    /// The readers counted are the views of either dump whose
    /// `View::table_relation` names the view (a TO one the routine phase
    /// creates would be created against a view the `CASCADE` has taken, or
    /// be taken with it), and the routines of either dump with a
    /// SQL-standard body whose `Routine::sql_body_dependencies` do; FROM
    /// ones only when TO keeps them, since the rest are dropped anyway. A
    /// routine the routine phase drops without `CASCADE` because it would
    /// take something pgc cannot re-create (`Comparer::cascade_blockers`)
    /// takes nothing, and does not count.
    ///
    /// `drop_views` drops without `CASCADE`, so a view is left to the
    /// routine's `CASCADE` when it, or a FROM view reading it, has a
    /// dependent the drop cannot take along
    /// (`Comparer::view_drop_blockers`).
    ///
    /// Empty with `use_drop` off, which comments the routine drops out.
    fn views_cascaded_with_readers(&self, changes: &RelationChanges) -> HashSet<usize> {
        if !self.use_drop || self.from.views.is_empty() {
            return HashSet::new();
        }
        let key_of = |r: &Routine| (r.schema.clone(), r.name.clone(), r.arguments.clone());
        let dropped: Vec<&Routine> = self
            .from
            .routines
            .iter()
            .filter(|r| {
                let key = r.dependency_key();
                changes.dropped_routines.contains(&key) || changes.recreated_routines.contains(&key)
            })
            .collect();
        if dropped.is_empty() {
            return HashSet::new();
        }
        let (blocked, cascading): (Vec<&Routine>, Vec<&Routine>) =
            dropped.into_iter().partition(|r| {
                !self
                    .cascade_blockers(&[*r], CascadeMoment::RoutinePhase)
                    .is_empty()
            });
        let blocked_keys: HashSet<(String, String, String)> =
            blocked.iter().map(|r| key_of(r)).collect();
        // `Routine::dependency_key` form, as `View::routine_dependencies`
        // records them.
        let mut keys: HashSet<String> = cascading.iter().map(|r| r.dependency_key()).collect();
        keys.extend(
            self.plan_cascade_victims(changes, &blocked_keys)
                .into_iter()
                .map(|(schema, name, arguments)| format!("{schema}.{name}({arguments})")),
        );
        if keys.is_empty() {
            return HashSet::new();
        }

        let to_views: HashSet<(&str, &str)> = self
            .to
            .views
            .iter()
            .map(|v| (v.schema.as_str(), v.name.as_str()))
            .collect();
        // The views the `CASCADE` takes because they call such a routine
        // (FROM-only ones are dropped here anyway), by normalized key.
        let calling: HashMap<String, usize> = self
            .from
            .views
            .iter()
            .enumerate()
            .filter(|(_, view)| {
                to_views.contains(&(view.schema.as_str(), view.name.as_str()))
                    && view
                        .routine_dependencies
                        .as_ref()
                        .is_some_and(|deps| deps.iter().any(|key| keys.contains(key)))
            })
            .map(|(idx, view)| (Self::normalized_view_key(&view.schema, &view.name), idx))
            .collect();
        if calling.is_empty() {
            return HashSet::new();
        }

        let to_routines: HashSet<(&str, &str, &str)> = self
            .to
            .routines
            .iter()
            .map(|r| (r.schema.as_str(), r.name.as_str(), r.arguments.as_str()))
            .collect();
        // What reads each of them, other than itself: a view of TO, or of
        // FROM that TO keeps, and a SQL-standard routine of either.
        let mut picked: HashSet<usize> = HashSet::new();
        let mut note_reader = |relation: &str, reader: Option<String>| {
            let key = Self::normalized_view_reference(relation);
            if let Some(&idx) = calling.get(&key)
                && reader.is_none_or(|reader| reader != key)
            {
                picked.insert(idx);
            }
        };
        let from_views = self
            .from
            .views
            .iter()
            .filter(|v| to_views.contains(&(v.schema.as_str(), v.name.as_str())));
        for view in from_views.chain(self.to.views.iter()) {
            for rel in &view.table_relation {
                note_reader(
                    rel,
                    Some(Self::normalized_view_key(&view.schema, &view.name)),
                );
            }
        }
        let from_routines = self.from.routines.iter().filter(|r| {
            to_routines.contains(&(r.schema.as_str(), r.name.as_str(), r.arguments.as_str()))
        });
        for routine in from_routines.chain(self.to.routines.iter()) {
            if routine.is_sql_body {
                for rel in &routine.sql_body_dependencies.relations {
                    note_reader(rel, None);
                }
            }
        }
        if picked.is_empty() {
            return picked;
        }

        // Each pick drops along the FROM views reading it, transitively;
        // one of them that cannot be dropped keeps the pick back.
        let mut readers_of: HashMap<String, Vec<usize>> = HashMap::new();
        for (idx, view) in self.from.views.iter().enumerate() {
            let own = Self::normalized_view_key(&view.schema, &view.name);
            for rel in &view.table_relation {
                let key = Self::normalized_view_reference(rel);
                if key != own {
                    readers_of.entry(key).or_default().push(idx);
                }
            }
        }
        picked.retain(|&idx| {
            let mut seen: HashSet<usize> = HashSet::from([idx]);
            let mut queue = vec![idx];
            while let Some(current) = queue.pop() {
                if !self
                    .view_drop_blockers(&self.from.views[current])
                    .is_empty()
                {
                    return false;
                }
                let view = &self.from.views[current];
                let key = Self::normalized_view_key(&view.schema, &view.name);
                for &reader in readers_of.get(&key).into_iter().flatten() {
                    if seen.insert(reader) {
                        queue.push(reader);
                    }
                }
            }
            true
        });
        picked
    }

    /// What depends on the FROM view `view` that its plain `DROP VIEW` (as
    /// `drop_views` issues it) could neither take along nor get past, one
    /// description each: a routine whose result or argument types use the
    /// view's row type — one with a SQL-standard body reading the view
    /// excepted, since `drop_views` drops it first — a policy expression
    /// reading it, and a table column of its row type. Matched by name,
    /// qualified or not, so a lookalike errs on the side of keeping the view.
    fn view_drop_blockers(&self, view: &View) -> Vec<String> {
        let own = Self::normalized_view_key(&view.schema, &view.name);
        let mentions = |text: &str| Self::text_mentions_relation(text, &view.schema, &view.name);
        let mut found = Vec::new();
        for routine in &self.from.routines {
            let reads_it = routine.is_sql_body
                && routine
                    .sql_body_dependencies
                    .relations
                    .iter()
                    .any(|rel| Self::normalized_view_reference(rel) == own);
            if !reads_it && (mentions(&routine.return_type) || mentions(&routine.arguments)) {
                found.push(format!(
                    "{} {}",
                    routine.kind.to_lowercase(),
                    routine.dependency_key()
                ));
            }
        }
        for table in &self.from.tables {
            for policy in &table.policies {
                if [&policy.using_clause, &policy.check_clause]
                    .into_iter()
                    .flatten()
                    .any(|expr| mentions(expr))
                {
                    found.push(format!(
                        "policy {} on {}.{}",
                        policy.name, table.schema, table.name
                    ));
                }
            }
            for column in &table.columns {
                let of_row_type = column
                    .udt_schema
                    .as_deref()
                    .zip(column.udt_name.as_deref())
                    .is_some_and(|(schema, name)| {
                        let name = name.strip_prefix('_').unwrap_or(name);
                        Self::normalized_view_key(schema, name) == own
                    });
                if of_row_type {
                    found.push(format!(
                        "column {}.{}.{}",
                        table.schema, table.name, column.name
                    ));
                }
            }
        }
        found
    }

    /// Whether `text` names the relation `schema.name` (as the dump spells
    /// them, possibly quoted), qualified or not, as a whole identifier.
    fn text_mentions_relation(text: &str, schema: &str, name: &str) -> bool {
        let (lower, unquoted) = Self::prelower_pair(text);
        let schema_lc = schema.to_lowercase();
        let name_lc = name.to_lowercase();
        if Self::text_references_qualified_name_pre(&lower, &unquoted, &schema_lc, &name_lc) {
            return true;
        }
        let bare: String = name_lc.chars().filter(|c| *c != '"').collect();
        if bare.is_empty() {
            return false;
        }
        let bytes = unquoted.as_bytes();
        let is_ident = |b: u8| b.is_ascii_alphanumeric() || b == b'_' || b == b'$' || b == b'.';
        unquoted.match_indices(&bare).any(|(idx, _)| {
            let end = idx + bare.len();
            (idx == 0 || !is_ident(bytes[idx - 1])) && (end >= bytes.len() || !is_ident(bytes[end]))
        })
    }

    // Create views from TO dump
    #[cfg(test)]
    async fn create_views(&mut self) -> Result<(), Error> {
        self.script
            .append_block("\n/* ---> Views CREATE: Start section --------------- */");

        for to_view in &self.to.views {
            let normalized_view = Self::normalized_view_key(&to_view.schema, &to_view.name);

            let from_view = self
                .from
                .views
                .iter()
                .find(|view| view.schema == to_view.schema && view.name == to_view.name);

            let existed_in_from = from_view.is_some();
            let was_dropped = self.dropped_views.contains_key(&normalized_view);
            let drop_was_active = self
                .dropped_views
                .get(&normalized_view)
                .copied()
                .unwrap_or(false);
            let definition_changed = from_view
                .map(|fv| Self::hashes_differ(&fv.hash, &to_view.hash))
                .unwrap_or(false);

            if existed_in_from && !was_dropped && !definition_changed {
                // Unchanged view — only emit owner change if needed.
                if let Some(fv) = from_view
                    && fv.owner != to_view.owner
                {
                    self.script.push_str(
                        format!(
                            "/* View: {}.{}*/\n",
                            block_comment_safe(&to_view.schema),
                            block_comment_safe(&to_view.name)
                        )
                        .as_str(),
                    );
                    self.script.push_str(&to_view.get_owner_script());
                }
                continue;
            }

            self.script.push_str(
                format!(
                    "/* View: {}.{}*/\n",
                    block_comment_safe(&to_view.schema),
                    block_comment_safe(&to_view.name)
                )
                .as_str(),
            );

            if to_view.is_materialized {
                // Materialized views do not support CREATE OR REPLACE — get_script()
                // already emits the correct CREATE MATERIALIZED VIEW syntax.
                if was_dropped && !drop_was_active {
                    // DROP was commented out, so CREATE would fail; comment it out too.
                    let create_script = to_view.get_script();
                    self.script.push_str(&format!(
                        "-- use_drop=false: materialized view {}.{} requires drop+recreate; create commented out (manual intervention needed)\n",
                        line_comment_safe(&to_view.schema),
                        line_comment_safe(&to_view.name)
                    ));
                    self.script.push_str(&comment_out(&create_script));
                } else {
                    self.script.push_str(&to_view.get_script());
                }
            } else {
                let mut view_script = to_view.get_script();
                if !view_script
                    .to_uppercase()
                    .contains("CREATE OR REPLACE VIEW")
                {
                    const TARGET: &str = "create view";
                    const REPLACEMENT: &str = "CREATE OR REPLACE VIEW";

                    if let Some(pos) = view_script.to_ascii_lowercase().find(TARGET) {
                        view_script.replace_range(pos..pos + TARGET.len(), REPLACEMENT);
                    }
                }
                if was_dropped && !drop_was_active {
                    // DROP was commented out (for example, because use_drop=false),
                    // so CREATE OR REPLACE VIEW would fail; comment it out too.
                    self.script.push_str(&format!(
                        "-- use_drop=false: DROP for view {}.{} was commented out, so create is also commented out because it would fail (manual intervention needed)\n",
                        line_comment_safe(&to_view.schema),
                        line_comment_safe(&to_view.name)
                    ));
                    self.script.push_str(&comment_out(&view_script));
                } else {
                    self.script.push_str(&view_script);
                }
            }
        }

        self.script
            .append_block("\n/* ---> Views CREATE: End section --------------- */");
        Ok(())
    }

    /// Emit the CREATE (or CREATE OR REPLACE) script for a single target view.
    ///
    /// `prod_ctx` is `Some` only in production mode, where a materialized view's
    /// indexes are split out of the CREATE and built concurrently after the
    /// transaction commits, exactly as a new table's are.
    fn emit_view_create(&mut self, to_view: &View, prod_ctx: Option<&PartitionContext>) {
        self.script.push_str(
            format!(
                "/* View: {}.{}*/\n",
                block_comment_safe(&to_view.schema),
                block_comment_safe(&to_view.name)
            )
            .as_str(),
        );

        if to_view.is_materialized {
            let normalized_view = Self::normalized_view_key(&to_view.schema, &to_view.name);
            let drop_was_active = self
                .dropped_views
                .get(&normalized_view)
                .copied()
                .unwrap_or(true);
            if !drop_was_active {
                // DROP was commented out, so CREATE would fail; comment it out too.
                // The indexes ride along: without the view they have nothing to
                // build on.
                let create_script = to_view.get_script();
                self.script.push_str(&format!(
                    "-- use_drop=false: materialized view {}.{} requires drop+recreate; create commented out (manual intervention needed)\n",
                    line_comment_safe(&to_view.schema),
                    line_comment_safe(&to_view.name)
                ));
                self.script.push_str(&comment_out(&create_script));
            } else if let Some(ctx) = prod_ctx {
                self.script.push_str(&to_view.get_script_without_indexes());
                for index in &to_view.indexes {
                    let split = production::index_create_split(index, ctx, true);
                    self.script.push_str(&split.in_txn);
                    self.production_post_script.push_str(&split.post_commit);
                }
            } else {
                self.script.push_str(&to_view.get_script());
            }
        } else {
            let view_script = rewrite_create_view_to_create_or_replace(&to_view.get_script());
            let normalized_view = Self::normalized_view_key(&to_view.schema, &to_view.name);
            let drop_was_active = self
                .dropped_views
                .get(&normalized_view)
                .copied()
                .unwrap_or(true);
            let was_dropped = self.dropped_views.contains_key(&normalized_view);
            if was_dropped && !drop_was_active {
                // DROP was commented out, so CREATE OR REPLACE VIEW would fail;
                // comment it out too.
                self.script.push_str(&format!(
                    "-- use_drop=false: view {}.{} requires drop+recreate; create commented out (manual intervention needed)\n",
                    line_comment_safe(&to_view.schema),
                    line_comment_safe(&to_view.name)
                ));
                self.script.push_str(&comment_out(&view_script));
            } else {
                self.script.push_str(&view_script);
            }
        }
    }

    /// Compare foreign tables between dumps.
    async fn compare_foreign_tables(&mut self) -> Result<(), Error> {
        self.script
            .append_block("/* ---> Compare Foreign Tables --------------- */");

        let from_map: std::collections::HashMap<
            (&str, &str),
            &crate::dump::foreign_table::ForeignTable,
        > = self
            .from
            .foreign_tables
            .iter()
            .map(|ft| ((ft.schema.as_str(), ft.name.as_str()), ft))
            .collect();

        let to_map: std::collections::HashMap<
            (&str, &str),
            &crate::dump::foreign_table::ForeignTable,
        > = self
            .to
            .foreign_tables
            .iter()
            .map(|ft| ((ft.schema.as_str(), ft.name.as_str()), ft))
            .collect();

        // Drop foreign tables that exist only in FROM
        for (key, ft) in &from_map {
            if !to_map.contains_key(key) {
                if self.use_drop {
                    self.script.push_str(&ft.get_drop_script());
                } else {
                    let drop_script = ft.get_drop_script();
                    self.script.push_str(&comment_out(&drop_script));
                }
            }
        }

        // Create or alter foreign tables in TO. One on a server the
        // migration creates again (see `plan_server_recreates`) is dropped
        // here and created by `compare_servers`, after its server.
        let on_recreated_server = |server: &str| self.recreated_servers.contains(server);
        for ft in &self.to.foreign_tables {
            let key = (ft.schema.as_str(), ft.name.as_str());
            if let Some(existing) = from_map.get(&key) {
                if on_recreated_server(&existing.server) || on_recreated_server(&ft.server) {
                    self.script.push_str(&existing.get_drop_script());
                    if on_recreated_server(&ft.server) {
                        self.foreign_tables_after_servers
                            .entry(ft.server.clone())
                            .or_default()
                            .push_str(&ft.get_script());
                    } else {
                        self.script.push_str(&ft.get_script());
                    }
                    self.recreated_foreign_tables
                        .insert((ft.schema.clone(), ft.name.clone()));
                    continue;
                }
                // Alter if hash differs
                if existing.hash != ft.hash {
                    let alter = existing.get_alter_script(ft, self.use_drop);
                    if !alter.is_empty() {
                        self.script.push_str(&alter);
                    }
                    // A server change drops and creates the table again.
                    if self.use_drop && existing.server != ft.server {
                        self.recreated_foreign_tables
                            .insert((ft.schema.clone(), ft.name.clone()));
                    }
                }
            } else if on_recreated_server(&ft.server) {
                self.foreign_tables_after_servers
                    .entry(ft.server.clone())
                    .or_default()
                    .push_str(&ft.get_script());
            } else {
                // Create new foreign table
                self.script.push_str(&ft.get_script());
            }
        }

        Ok(())
    }

    /// Compare extended statistics between dumps.
    async fn compare_statistics(&mut self) -> Result<(), Error> {
        self.script
            .append_block("/* ---> Compare Statistics --------------- */");

        let from_map: HashMap<(&str, &str), &Statistic> = self
            .from
            .statistics
            .iter()
            .map(|s| ((s.schema.as_str(), s.name.as_str()), s))
            .collect();

        let to_map: HashMap<(&str, &str), &Statistic> = self
            .to
            .statistics
            .iter()
            .map(|s| ((s.schema.as_str(), s.name.as_str()), s))
            .collect();

        // Drop statistics that exist only in FROM. Walks the dump's own list
        // (ordered by schema and name) rather than the map, so the drops come
        // out in the same order on every run.
        for stat in &self.from.statistics {
            if !to_map.contains_key(&(stat.schema.as_str(), stat.name.as_str())) {
                if self.use_drop {
                    self.script.push_str(&stat.get_drop_script());
                } else {
                    let drop_script = stat.get_drop_script();
                    self.script.push_str(&comment_out(&drop_script));
                }
            }
        }

        // Materialized views that `compare_routines_and_views` creates, or
        // recreates after `drop_views` dropped them (a changed definition, a
        // dependency on a changing table). Neither exists yet at this point.
        let created_matviews = self.created_matview_keys();
        let targets_unknown = self.to_statistics_targets_unknown();
        let rebuilt_in_place = self.statistics_rebuilt_in_place();
        // Objects calling a routine the migration drops with `CASCADE`:
        // `recreate_routine_dependents` creates them from TO once the
        // routines are back, whatever this pass would do to them — before
        // the drop (a routine the routine phase drops) the work would be
        // undone, and after it (one `drop_views` drops) an `alter statistics`
        // would find nothing to alter. With `use_drop` off nothing is
        // dropped, and the ordinary diff applies.
        let restored_after_cascade: HashSet<(String, String)> = if self.use_drop {
            self.statistics_restored_after_cascade(&self.cascade_affected_routines())
                .into_iter()
                .collect()
        } else {
            HashSet::new()
        };

        // Create or alter statistics in TO
        for stat in &self.to.statistics {
            let key = (stat.schema.as_str(), stat.name.as_str());
            let existing = from_map.get(&key).copied();
            let target = Self::statistic_for_emission(targets_unknown, existing, stat);
            if let (Some(from_target), None) =
                (existing.and_then(|e| e.stxstattarget), stat.stxstattarget)
                && target.stxstattarget.is_some()
            {
                eprintln!(
                    "Warning: the 'to' dump was written by a pgc older than 1.1.0, which \
                     did not read statistics targets on PostgreSQL 17 and later; keeping \
                     the statistics target {from_target} of {}.{} instead of resetting it.",
                    stat.schema, stat.name
                );
            }
            let stat: &Statistic = &target;

            if restored_after_cascade.contains(&(stat.schema.clone(), stat.name.clone())) {
                continue;
            }

            // Statistics on a materialized view that is created or recreated
            // later in the migration: the view does not exist yet (or no
            // longer does, with the statistics that were on it), so the
            // object is created after the view, whether or not its hash
            // changed. A FROM object of that name may still sit elsewhere; it
            // is dropped first, and with `use_drop` off that drop and the
            // create that depends on it are left commented out.
            if created_matviews.contains(&Self::normalized_view_key(
                &stat.table_schema,
                &stat.table_name,
            )) {
                let mut block = existing.map(Statistic::get_drop_script).unwrap_or_default();
                block.push_str(&stat.get_script());
                Self::emit_dependent_recreate(
                    &mut self.view_statistics_post_script,
                    self.use_drop || existing.is_none(),
                    &block,
                );
                continue;
            }

            // `DROP TABLE` took every statistics object on a table that
            // `compare_tables` dropped and recreated (partition-key change,
            // column type change that forces it, recreated parent), and the
            // re-`CREATE TABLE` does not bring them back, so each one on that
            // table is created afresh — whether or not its hash changed. A
            // FROM object of that name is dropped first, guarded: it is still
            // there when it sat on another table, and the guard costs nothing
            // when it went with this one. With `use_drop` off the `DROP
            // TABLE` is commented out, the statistics survive, and the
            // ordinary diff below applies.
            if self.use_drop
                && self
                    .dropped_and_recreated_tables
                    .contains(&Self::table_key(&stat.table_schema, &stat.table_name))
            {
                if let Some(existing) = existing {
                    self.script.push_str(&existing.get_drop_script());
                }
                self.script.push_str(&stat.get_script());
                continue;
            }
            if let Some(existing) = existing {
                if existing.hash != stat.hash {
                    let alter = existing.get_alter_script(stat, self.use_drop);
                    if !alter.is_empty() {
                        self.script.push_str(&alter);
                    }
                }
                // `compare_tables` changed a column the object reads in place,
                // and PostgreSQL rebuilt the object without its statistics
                // target; set it again, unless the alter above already does —
                // a live recreate, or a target change of its own.
                let alter_sets_target = if existing.requires_recreate(stat) {
                    self.use_drop
                } else {
                    existing.stxstattarget != stat.stxstattarget
                };
                if !alter_sets_target
                    && rebuilt_in_place.contains(&(stat.schema.clone(), stat.name.clone()))
                    && let Some(target_script) = stat.get_target_script()
                {
                    self.script.push_str(&target_script);
                }
            } else {
                self.script.push_str(&stat.get_script());
            }
        }

        Ok(())
    }

    /// The `(schema, name)` of every FROM statistics object that
    /// `compare_tables` makes PostgreSQL rebuild in place: one that reads a
    /// column altered with `ALTER COLUMN ... TYPE` or `SET EXPRESSION` (see
    /// `TableColumn::rebuilds_statistics_in_place`), on its own table or
    /// through a parent's alter that recurses to it. The rebuild keeps the
    /// object's owner and comment but resets an explicit statistics target.
    ///
    /// Which object reads which column comes from the `Statistic` column
    /// dependents of either dump. An object neither dump has an edge for —
    /// both were written before pgc 1.1.0 — counts when its table has such a
    /// column at all: setting a target that survived is a no-op. Tables that
    /// are dropped and recreated are left out; their statistics are created
    /// afresh. Empty when no TO object has an explicit target, the only case
    /// the caller acts on.
    fn statistics_rebuilt_in_place(&self) -> HashSet<(String, String)> {
        if !self
            .to
            .statistics
            .iter()
            .any(|s| s.stxstattarget.is_some_and(|t| t >= 0))
        {
            return HashSet::new();
        }
        let normalise = |s: &str| s.to_lowercase().replace('"', "");
        let alter_caps = self.table_alter_caps();
        let to_by_key: HashMap<(&str, &str), &Table> = self
            .to
            .tables
            .iter()
            .map(|t| ((t.schema.as_str(), t.name.as_str()), t))
            .collect();

        // (schema, table, column), normalised as `recreate_column_dependents`
        // matches anchors.
        let mut columns: HashSet<(String, String, String)> = HashSet::new();
        for table in &self.from.tables {
            if self
                .dropped_and_recreated_tables
                .contains(&Self::table_key(&table.schema, &table.name))
            {
                continue;
            }
            let Some(to_table) = to_by_key.get(&(table.schema.as_str(), table.name.as_str()))
            else {
                continue;
            };
            let caps = Self::caps_for(&alter_caps, table).generated;
            for to_col in &to_table.columns {
                if let Some(from_col) = table.columns.iter().find(|c| c.name == to_col.name)
                    && to_col.rebuilds_statistics_in_place(from_col, caps, self.use_drop)
                {
                    columns.insert((
                        normalise(&table.schema),
                        normalise(&table.name),
                        normalise(&to_col.name),
                    ));
                }
            }
        }
        if columns.is_empty() {
            return HashSet::new();
        }

        let mut with_edges: HashSet<(&str, &str)> = HashSet::new();
        let mut rebuilt: HashSet<(String, String)> = HashSet::new();
        for dep in self
            .from
            .column_dependents
            .iter()
            .chain(self.to.column_dependents.iter())
            .filter(|dep| dep.kind == ColumnDependentKind::Statistic)
        {
            with_edges.insert((dep.dep_schema.as_str(), dep.dep_name.as_str()));
            if columns.contains(&(
                normalise(&dep.schema),
                normalise(&dep.table),
                normalise(&dep.column),
            )) {
                rebuilt.insert((dep.dep_schema.clone(), dep.dep_name.clone()));
            }
        }
        let tables: HashSet<(&str, &str)> = columns
            .iter()
            .map(|(schema, table, _)| (schema.as_str(), table.as_str()))
            .collect();
        for stat in &self.from.statistics {
            if !with_edges.contains(&(stat.schema.as_str(), stat.name.as_str()))
                && tables.contains(&(
                    normalise(&stat.table_schema).as_str(),
                    normalise(&stat.table_name).as_str(),
                ))
            {
                rebuilt.insert((stat.schema.clone(), stat.name.clone()));
            }
        }
        rebuilt
    }

    /// Whether a TO statistics object without a target means "unknown"
    /// rather than "the default".
    ///
    /// pgc before 1.1.0 read `stxstattarget` uncast, and from PostgreSQL 17
    /// on — where it became a nullable `smallint` — that failed to decode,
    /// so its dumps of those servers carry no target at all. Compared with
    /// such a TO dump, every explicit target a 1.1.0 FROM dump holds would
    /// read as a change back to the default, and `SET STATISTICS -1` would
    /// wipe it. That is the case this answers `true` for:
    ///
    /// * the TO dump predates pgc 1.1.0 (it has no `server_version_num`),
    /// * the FROM dump is known to come from PostgreSQL 17 or later — only
    ///   there did pgc 1.1.0 start reading targets that 1.0.x missed, so a
    ///   FROM dump of an older server compares exactly as it always did,
    /// * and no TO object has a target, which it would if that pgc could
    ///   read them (a TO server of 16 or older).
    ///
    /// Two dumps written by pgc 1.0.x keep comparing as they always did.
    /// `pgc` itself never gets here with such a pair: it refuses a pgc 1.0.x
    /// TO dump against a pgc 1.1+ FROM dump outright
    /// ([`Comparer::check_dump_versions`]), so this guards callers of the
    /// library that compare the two in memory.
    fn to_statistics_targets_unknown(&self) -> bool {
        self.to.server_version_num.is_none()
            && self.migrated_server_at_least(170000)
            && self.to.statistics.iter().all(|s| s.stxstattarget.is_none())
    }

    /// The TO statistics object as the migration should create it.
    ///
    /// When the TO dump cannot tell a default target from a missing one (see
    /// `to_statistics_targets_unknown`), the FROM object's target is kept
    /// instead of being reset — in an in-place diff and in a re-create alike.
    fn statistic_for_emission<'a>(
        targets_unknown: bool,
        from: Option<&Statistic>,
        to: &'a Statistic,
    ) -> Cow<'a, Statistic> {
        match from.and_then(|f| f.stxstattarget) {
            Some(target) if targets_unknown && to.stxstattarget.is_none() => {
                let mut kept = to.clone();
                kept.stxstattarget = Some(target);
                kept.hash();
                Cow::Owned(kept)
            }
            _ => Cow::Borrowed(to),
        }
    }

    /// Drop, ahead of every view and table change, the FROM rules that cannot
    /// wait for the rules pass at the end of the migration, and record in
    /// `rules_to_recreate` the rules TO still has that the migration takes away
    /// on the way, for `Comparer::compare_rules` to recreate (see
    /// `Comparer::plan_rule_drops` for both).
    ///
    /// Emits nothing, not even the section markers, when no rule qualifies, so
    /// a migration without such rules is laid out exactly as before.
    fn drop_rules(
        &mut self,
        changes: &RelationChanges,
        dropping_views: &HashSet<String>,
        dropping_routines: &HashSet<String>,
    ) {
        let (drops, recreate) = self.plan_rule_drops(changes, dropping_views, dropping_routines);
        self.rules_to_recreate = recreate;
        if drops.is_empty() {
            return;
        }

        self.script
            .append_block("\n/* ---> Rules DROP: Start section --------------- */");
        for idx in drops {
            let drop_script = self.from.rules[idx].get_drop_script();
            Self::emit_drop(&mut self.script, self.use_drop, &drop_script);
        }
        self.script
            .append_block("\n/* ---> Rules DROP: End section --------------- */");
    }

    /// Which FROM rules `drop_rules` drops, as indexes into `from.rules` in dump
    /// order, and which rules TO still has must be recreated at the end:
    ///
    /// * every rule TO no longer has is dropped. Its relation may outlive the
    ///   migration, and a rule that writes to a table the table section drops
    ///   blocks that `DROP TABLE`, which runs without `CASCADE`.
    /// * every rule TO still has whose `Rule::depends_on` names something the
    ///   migration drops or changes underneath it is dropped and recreated,
    ///   whether or not its definition changed: a view in `dropping_views`; a
    ///   table or foreign table dropped, or dropped and recreated; a column of
    ///   either dropped or retyped (`ALTER COLUMN ... TYPE` is refused while a
    ///   rule reads the column); a FROM-only sequence; or a routine that is
    ///   dropped or recreated, whose `DROP ... CASCADE` would take the rule
    ///   along silently — including a routine with a SQL-standard body that
    ///   `drop_views` drops ahead of the relation DDL (`dropping_routines`).
    ///   `DROP TABLE`, `DROP VIEW`, `DROP FOREIGN TABLE` and `DROP SEQUENCE`
    ///   all run without `CASCADE` and are refused while the rule reads the
    ///   relation.
    /// * every rule TO still has whose own table is dropped or recreated — a
    ///   partition-key change, or a table that becomes a view of the same name
    ///   — is recreated. `DROP TABLE` takes a table's own rules with it, so
    ///   there is nothing to drop first, and this needs no recorded
    ///   dependencies: a rule whose actions read nothing (`NOTIFY`) has none.
    ///
    /// What goes and what changes comes from `changes`
    /// (`Comparer::relation_changes`), the same answer the view and SQL-body
    /// routine plans work from.
    ///
    /// A rule from a dump written before pgc recorded dependencies is never in
    /// the second group; the third, and the relation checks in
    /// `compare_rules`, are as much as those dumps ever got.
    fn plan_rule_drops(
        &self,
        changes: &RelationChanges,
        dropping_views: &HashSet<String>,
        dropping_routines: &HashSet<String>,
    ) -> (Vec<usize>, HashSet<RuleKey>) {
        let to_rule_keys: HashSet<(&str, &str, &str)> = self
            .to
            .rules
            .iter()
            .map(|r| {
                (
                    r.schema.as_str(),
                    r.table_name.as_str(),
                    r.rule_name.as_str(),
                )
            })
            .collect();

        let mut drops = Vec::new();
        let mut recreate = HashSet::new();
        for (idx, rule) in self.from.rules.iter().enumerate() {
            let key = (
                rule.schema.as_str(),
                rule.table_name.as_str(),
                rule.rule_name.as_str(),
            );
            if !to_rule_keys.contains(&key) {
                drops.push(idx);
                continue;
            }
            let at_risk = rule.depends_on.as_ref().is_some_and(|deps| {
                deps.relations.iter().any(|rel| {
                    changes.relation_goes(rel)
                        || dropping_views.contains(&Self::normalized_view_reference(rel))
                }) || deps.columns.iter().any(|col| changes.column_changes(col))
                    || deps.routines.iter().any(|r| {
                        changes.dropped_routines.contains(r)
                            || changes.recreated_routines.contains(r)
                            || dropping_routines.contains(r)
                    })
            });
            if at_risk {
                drops.push(idx);
            }
            let own_table_goes = changes
                .wholesale_tables
                .contains(&Self::normalized_view_key(&rule.schema, &rule.table_name));
            if at_risk || own_table_goes {
                recreate.insert((
                    rule.schema.clone(),
                    rule.table_name.clone(),
                    rule.rule_name.clone(),
                ));
            }
        }
        (drops, recreate)
    }

    /// Create and change rules. Runs after routines, views, operators and
    /// casts, because a rule can sit on a view the migration creates and its
    /// actions can call routines and write to relations the migration creates.
    /// FROM-only rules were dropped earlier, by `drop_rules`.
    ///
    /// A rule both dumps have is emitted with `CREATE OR REPLACE RULE`, changed
    /// or not, when the migration has dropped it on the way: it is in
    /// `rules_to_recreate` (see `Comparer::plan_rule_drops`), its own relation
    /// was dropped and recreated — a table in `recreated_tables`, a view in
    /// `dropped_views` or in `views_recreated_by_cascade` — or the FROM rule
    /// reads a view in `views_recreated_by_cascade`, which the routine's
    /// `CASCADE` took along with the rule. `OR REPLACE` also covers the cases
    /// where the rule is in fact still there: the drop was commented out
    /// (`use_drop` off), or the view was flagged by a textual match only; see
    /// `Rule::get_recreate_script` for its comment. Otherwise a changed rule
    /// gets `Rule::get_alter_script`. A new rule gets `CREATE RULE`, or in
    /// production mode `CREATE OR REPLACE RULE`, so a partially applied
    /// migration can be run again.
    async fn compare_rules(&mut self) -> Result<(), Error> {
        self.script
            .append_block("/* ---> Compare Rules --------------- */");

        let from_map: HashMap<(&str, &str, &str), &crate::dump::rule::Rule> = self
            .from
            .rules
            .iter()
            .map(|r| {
                (
                    (
                        r.schema.as_str(),
                        r.table_name.as_str(),
                        r.rule_name.as_str(),
                    ),
                    r,
                )
            })
            .collect();

        for rule in &self.to.rules {
            let key = (
                rule.schema.as_str(),
                rule.table_name.as_str(),
                rule.rule_name.as_str(),
            );
            if let Some(existing) = from_map.get(&key) {
                let relation_view_key = Self::normalized_view_key(&rule.schema, &rule.table_name);
                let reads_a_cascaded_view = existing.depends_on.as_ref().is_some_and(|deps| {
                    deps.relations.iter().any(|rel| {
                        self.views_recreated_by_cascade
                            .contains(&Self::normalized_view_reference(rel))
                    })
                });
                let dropped_on_the_way = self.rules_to_recreate.contains(&(
                    rule.schema.clone(),
                    rule.table_name.clone(),
                    rule.rule_name.clone(),
                )) || self
                    .recreated_tables
                    .contains(&Self::table_key(&rule.schema, &rule.table_name))
                    || self.dropped_views.contains_key(&relation_view_key)
                    || self.views_recreated_by_cascade.contains(&relation_view_key)
                    || reads_a_cascaded_view;
                if dropped_on_the_way {
                    self.script
                        .push_str(&rule.get_recreate_script(existing, self.use_drop));
                } else if existing.hash != rule.hash {
                    let alter = existing.get_alter_script(rule, self.use_drop);
                    if !alter.is_empty() {
                        self.script.push_str(&alter);
                    }
                }
            } else if self.output_for_production {
                self.script.push_str(&rule.get_replace_script());
            } else {
                self.script.push_str(&rule.get_script());
            }
        }

        Ok(())
    }

    async fn compare_event_triggers(&mut self) -> Result<(), Error> {
        self.script
            .append_block("/* ---> Compare Event Triggers --------------- */");

        let from_map: HashMap<&str, &crate::dump::event_trigger::EventTrigger> = self
            .from
            .event_triggers
            .iter()
            .map(|e| (e.name.as_str(), e))
            .collect();

        let to_map: HashMap<&str, &crate::dump::event_trigger::EventTrigger> = self
            .to
            .event_triggers
            .iter()
            .map(|e| (e.name.as_str(), e))
            .collect();

        // Drop event triggers that only exist in FROM
        for (name, et) in &from_map {
            if !to_map.contains_key(name) {
                if self.use_drop {
                    self.script.push_str(&et.get_drop_script());
                } else {
                    let drop = et.get_drop_script();
                    self.script.push_str(&comment_out(&drop));
                }
            }
        }

        // Create or alter event triggers in TO
        for et in &self.to.event_triggers {
            if let Some(existing) = from_map.get(et.name.as_str()) {
                if existing.hash != et.hash {
                    let alter = existing.get_alter_script(et, self.use_drop);
                    if !alter.is_empty() {
                        self.script.push_str(&alter);
                    }
                }
            } else {
                self.script.push_str(&et.get_script());
            }
        }

        Ok(())
    }

    async fn compare_collations(&mut self) -> Result<(), Error> {
        self.script
            .append_block("/* ---> Compare Collations ------------------- */");

        let from_map: HashMap<String, &crate::dump::collation::Collation> = self
            .from
            .collations
            .iter()
            .map(|c| (format!("{}.{}", c.schema, c.name), c))
            .collect();

        let to_map: HashMap<String, &crate::dump::collation::Collation> = self
            .to
            .collations
            .iter()
            .map(|c| (format!("{}.{}", c.schema, c.name), c))
            .collect();

        for (key, coll) in &from_map {
            if !to_map.contains_key(key) {
                Self::emit_drop(&mut self.script, self.use_drop, &coll.get_drop_script());
            }
        }

        // The collations TO adds were created by `create_new_collations`,
        // ahead of the types and tables that may use them.
        for coll in &self.to.collations {
            let key = format!("{}.{}", coll.schema, coll.name);
            if let Some(existing) = from_map.get(&key)
                && existing.hash != coll.hash
            {
                let alter = existing.get_alter_script(coll, self.use_drop);
                if !alter.is_empty() {
                    self.script.push_str(&alter);
                }
            }
        }

        Ok(())
    }

    /// Creates, with their owner and comment, the collations TO has and FROM
    /// does not, before any type or table is touched.
    ///
    /// A column (`CREATE TABLE`, `ADD COLUMN`, or `ALTER COLUMN ... TYPE ...
    /// COLLATE` when a column moves to the new collation), a domain, a range
    /// type, an index or a constraint can name a new collation, and each of
    /// them fails with "collation ... does not exist" while it is missing. A
    /// collation depends on nothing but its schema, which `compare_schemas`
    /// has created by now. `compare_collations` keeps the drops of the
    /// collations TO no longer has, after the tables have moved their
    /// columns off them, and the changes of the ones both sides have.
    ///
    /// The section header is written only when there is something in it, so
    /// a migration without a new collation is unchanged byte for byte.
    fn create_new_collations(&mut self) {
        let from_keys: HashSet<String> = self
            .from
            .collations
            .iter()
            .map(|c| format!("{}.{}", c.schema, c.name))
            .collect();
        let mut section = String::new();
        for coll in &self.to.collations {
            if !from_keys.contains(&format!("{}.{}", coll.schema, coll.name)) {
                section.push_str(&coll.get_script());
            }
        }
        if !section.is_empty() {
            self.script
                .append_block("/* ---> New Collations (before types and tables) ---------- */");
            self.script.push_str(&section);
        }
    }

    async fn compare_ts_configs(&mut self) -> Result<(), Error> {
        self.script
            .append_block("/* ---> Compare Text Search Configurations --- */");

        let from_map: HashMap<String, &crate::dump::text_search::TextSearchConfig> = self
            .from
            .ts_configs
            .iter()
            .map(|c| (format!("{}.{}", c.schema, c.name), c))
            .collect();

        for cfg in &self.from.ts_configs {
            let key = format!("{}.{}", cfg.schema, cfg.name);
            if !self
                .to
                .ts_configs
                .iter()
                .any(|c| format!("{}.{}", c.schema, c.name) == key)
            {
                Self::emit_drop(&mut self.script, self.use_drop, &cfg.get_drop_script());
            }
        }

        for cfg in &self.to.ts_configs {
            let key = format!("{}.{}", cfg.schema, cfg.name);
            if let Some(existing) = from_map.get(&key) {
                if existing.hash != cfg.hash {
                    let alter = existing.get_alter_script(cfg, self.use_drop);
                    if !alter.is_empty() {
                        self.script.push_str(&alter);
                    }
                }
            } else {
                self.script.push_str(&cfg.get_script());
            }
        }

        Ok(())
    }

    /// Applies, before any table is touched, the in-place option changes of
    /// text search dictionaries that exist on both sides
    /// (`ALTER TEXT SEARCH DICTIONARY ... (...)`; see
    /// [`TextSearchDict::get_alter_script`](crate::dump::text_search::TextSearchDict::get_alter_script)).
    ///
    /// A generated `tsvector` column or an expression index that the tables
    /// phase creates is computed through the dictionary as it is at that
    /// moment, and the ALTER never re-processes it. Emitted after the tables
    /// (with the rest of the dictionaries), a column added in the same
    /// migration would be filled with the old options' output, and no later
    /// compare could see it. Such an ALTER references nothing a table creates,
    /// so it can go first. Drops, creates, template changes (drop and
    /// recreate) and owner- or comment-only changes stay in
    /// `compare_ts_dicts`, where they always were.
    ///
    /// A dictionary that has to be created anew — its template changes, or
    /// its options change beyond what the in-place ALTER can make (see
    /// [`TextSearchDict::is_recreated_as`](crate::dump::text_search::TextSearchDict::is_recreated_as))
    /// — is replaced here too, with `use_drop`, and without `CASCADE`: the
    /// FROM configurations mapping it move to the new one (see
    /// [`TextSearchDict::get_recreate_script`](crate::dump::text_search::TextSearchDict::get_recreate_script)),
    /// so the generated columns and indexes built through them stay. Without
    /// `use_drop` it stays a commented-out drop + create in
    /// `compare_ts_dicts`.
    ///
    /// Each section header is written only when there is something in it, so
    /// a migration without such a change is unchanged byte for byte.
    async fn compare_ts_dict_options(&mut self) -> Result<(), Error> {
        let from_map: HashMap<String, &crate::dump::text_search::TextSearchDict> = self
            .from
            .ts_dicts
            .iter()
            .map(|d| (format!("{}.{}", d.schema, d.name), d))
            .collect();

        let mut section = String::new();
        let mut recreate_section = String::new();
        for d in &self.to.ts_dicts {
            let Some(existing) = from_map.get(&format!("{}.{}", d.schema, d.name)) else {
                continue;
            };
            if existing.hash == d.hash {
                continue;
            }
            if self.use_drop && existing.is_recreated_as(d) {
                let configurations: Vec<&crate::dump::text_search::TextSearchConfig> = self
                    .from
                    .ts_configs
                    .iter()
                    .filter(|c| c.maps_dictionary(&existing.schema, &existing.name))
                    .collect();
                recreate_section.push_str(&existing.get_recreate_script(d, &configurations));
                eprintln!(
                    "Warning: text search dictionary {}.{} is created anew, as its template or \
                     options cannot be altered in place, and the configurations mapping it move \
                     to the new one. tsvector values already stored through it, and the indexes \
                     built on them, are not re-processed: rebuild them if its output changed.",
                    d.schema, d.name
                );
                continue;
            }
            if !existing.alters_options_in_place(d) {
                continue;
            }
            section.push_str(&existing.get_alter_script(d, self.use_drop));
            // The script's own note is a comment, gone with USE_COMMENTS off.
            eprintln!(
                "Warning: text search dictionary {}.{} changes its options in place. \
                 tsvector values already stored through it, and the indexes built on \
                 them, are not re-processed: rebuild them if its output changed.",
                d.schema, d.name
            );
        }

        if !section.is_empty() {
            self.script
                .append_block("/* ---> Text Search Dictionary Options (before tables) ----- */");
            self.script.push_str(&section);
        }
        if !recreate_section.is_empty() {
            self.script.append_block(
                "/* ---> Text Search Dictionaries created anew (before tables) ----- */",
            );
            self.script.push_str(&recreate_section);
        }

        Ok(())
    }

    async fn compare_ts_dicts(&mut self) -> Result<(), Error> {
        self.script
            .append_block("/* ---> Compare Text Search Dictionaries ----- */");

        let from_map: HashMap<String, &crate::dump::text_search::TextSearchDict> = self
            .from
            .ts_dicts
            .iter()
            .map(|d| (format!("{}.{}", d.schema, d.name), d))
            .collect();

        for d in &self.from.ts_dicts {
            let key = format!("{}.{}", d.schema, d.name);
            if !self
                .to
                .ts_dicts
                .iter()
                .any(|td| format!("{}.{}", td.schema, td.name) == key)
            {
                Self::emit_drop(&mut self.script, self.use_drop, &d.get_drop_script());
            }
        }

        for d in &self.to.ts_dicts {
            let key = format!("{}.{}", d.schema, d.name);
            if let Some(existing) = from_map.get(&key) {
                // In-place option changes, and with `use_drop` the
                // dictionaries created anew, went out before the tables
                // (`compare_ts_dict_options`).
                if existing.hash != d.hash
                    && !existing.alters_options_in_place(d)
                    && !(self.use_drop && existing.is_recreated_as(d))
                {
                    let alter = existing.get_alter_script(d, self.use_drop);
                    if !alter.is_empty() {
                        self.script.push_str(&alter);
                    }
                }
            } else {
                self.script.push_str(&d.get_script());
            }
        }

        Ok(())
    }

    async fn compare_casts(&mut self) -> Result<(), Error> {
        self.script
            .append_block("/* ---> Compare Casts ------------------------ */");

        let from_map: HashMap<String, &crate::dump::cast::Cast> = self
            .from
            .casts
            .iter()
            .map(|c| (format!("{} AS {}", c.source_type, c.target_type), c))
            .collect();

        // Build sets of user-defined type names present in each dump.
        // format_type() returns "schema.typname" for user types, matching how
        // PgType stores schema (nspname, unquoted) and typname.
        let to_type_names: HashSet<String> = self
            .to
            .types
            .iter()
            .map(|t| format!("{}.{}", t.schema, t.typname))
            .collect();
        let from_type_names: HashSet<String> = self
            .from
            .types
            .iter()
            .map(|t| format!("{}.{}", t.schema, t.typname))
            .collect();

        for cast in &self.from.casts {
            let key = format!("{} AS {}", cast.source_type, cast.target_type);
            if !self
                .to
                .casts
                .iter()
                .any(|c| format!("{} AS {}", c.source_type, c.target_type) == key)
            {
                // If either the source or target type is a user-defined type being
                // removed in this migration, skip the explicit DROP CAST — it will
                // be handled automatically by DROP TYPE … CASCADE.
                let src_dropped = from_type_names.contains(&cast.source_type)
                    && !to_type_names.contains(&cast.source_type);
                let tgt_dropped = from_type_names.contains(&cast.target_type)
                    && !to_type_names.contains(&cast.target_type);
                if src_dropped || tgt_dropped {
                    continue;
                }
                Self::emit_drop(&mut self.script, self.use_drop, &cast.get_drop_script());
            }
        }

        for cast in &self.to.casts {
            let key = format!("{} AS {}", cast.source_type, cast.target_type);
            if let Some(existing) = from_map.get(&key) {
                if existing.hash != cast.hash {
                    let alter = existing.get_alter_script(cast, self.use_drop);
                    if !alter.is_empty() {
                        self.script.push_str(&alter);
                    }
                }
            } else {
                self.script.push_str(&cast.get_script());
            }
        }

        Ok(())
    }

    async fn compare_operators(&mut self) -> Result<(), Error> {
        self.script
            .append_block("/* ---> Compare Operators -------------------- */");

        let op_key = |op: &crate::dump::operator::Operator| op.key();

        let from_map: HashMap<String, &crate::dump::operator::Operator> = self
            .from
            .operators
            .iter()
            .map(|op| (op_key(op), op))
            .collect();

        for op in &self.from.operators {
            let key = op_key(op);
            if !self.to.operators.iter().any(|o| op_key(o) == key) {
                Self::emit_drop(&mut self.script, self.use_drop, &op.get_drop_script());
            }
        }

        for op in &self.to.operators {
            let key = op_key(op);
            if let Some(existing) = from_map.get(&key) {
                if existing.hash != op.hash {
                    let alter = existing.get_alter_script(op, self.use_drop);
                    if !alter.is_empty() {
                        self.script.push_str(&alter);
                    }
                }
            } else {
                self.script.push_str(&op.get_script());
            }
        }

        Ok(())
    }

    async fn compare_default_privileges(&mut self) -> Result<(), Error> {
        self.script
            .append_block("/* ---> Compare Default Privileges ----------- */");

        let dp_key = |dp: &crate::dump::default_privilege::DefaultPrivilege| {
            format!("{}|{}|{}", dp.role_name, dp.schema_name, dp.object_type)
        };

        let from_map: HashMap<String, &crate::dump::default_privilege::DefaultPrivilege> = self
            .from
            .default_privileges
            .iter()
            .map(|dp| (dp_key(dp), dp))
            .collect();

        for dp in &self.from.default_privileges {
            let key = dp_key(dp);
            if !self.to.default_privileges.iter().any(|d| dp_key(d) == key) {
                Self::emit_drop(&mut self.script, self.use_drop, &dp.get_revoke_script());
            }
        }

        for dp in &self.to.default_privileges {
            let key = dp_key(dp);
            if let Some(existing) = from_map.get(&key) {
                if existing.hash != dp.hash {
                    // Revoke old, grant new
                    self.script.push_str(&existing.get_revoke_script());
                    self.script.push_str(&dp.get_script());
                }
            } else {
                self.script.push_str(&dp.get_script());
            }
        }

        Ok(())
    }

    /// What the earlier sections of the migration do, and what the migrated
    /// database looks like, that publication statements depend on: whether
    /// the migrated server takes PostgreSQL 19's `SET ALL ...`; which tables
    /// have lost their `pg_publication_rel` rows by the time publications run
    /// (the ones `compare_tables` drops, or drops and recreates via
    /// `dropped_and_recreated_tables` — both only with `use_drop`, since
    /// otherwise the `DROP TABLE` is commented out and the table keeps its
    /// rows); and which tables are classical-inheritance parents, whose
    /// members must be written `ONLY`.
    ///
    /// Not `recreated_tables`: that set also holds partitions whose column
    /// type changes through their parent, which are never dropped and keep
    /// their rows. Counting them as gone left them out of the `DROP TABLE`
    /// that empties a publication, so a following `SET ALL TABLES` failed
    /// and a list emptied by the migration kept them as members.
    fn publication_alter_context(&self) -> PublicationAlterContext {
        let to_tables: HashSet<(&str, &str)> = self
            .to
            .tables
            .iter()
            .map(|t| (t.schema.as_str(), t.name.as_str()))
            .collect();
        let gone_tables = if self.use_drop {
            self.from
                .tables
                .iter()
                .filter(|t| {
                    !to_tables.contains(&(t.schema.as_str(), t.name.as_str()))
                        || self
                            .dropped_and_recreated_tables
                            .contains(&Self::table_key(&t.schema, &t.name))
                })
                // `Table::schema` / `name` are quote_ident'ed, as publication
                // members are.
                .map(|t| format!("{}.{}", t.schema, t.name))
                .collect()
        } else {
            HashSet::new()
        };
        // `inherits_from` is spelled like publication members. Both dumps
        // count: `ONLY` is a no-op on a table without children, and a child
        // that only FROM has may still be there (without `use_drop`).
        let inheritance_parents = self
            .from
            .tables
            .iter()
            .chain(&self.to.tables)
            .flat_map(|t| t.inherits_from.iter().cloned())
            .collect();
        PublicationAlterContext {
            set_all_supported: self.migrated_server_at_least(190000),
            gone_tables,
            inheritance_parents,
            released_tables: self.released_publication_tables.clone(),
        }
    }

    /// Take out of the FROM publications, ahead of the table changes, the
    /// `pg_publication_rel` rows that would make one of those changes fail,
    /// and record them in `released_publication_tables` so that
    /// `compare_publications` puts back whatever the TO publication keeps:
    ///
    /// - a `FOR TABLE` member whose column list or row filter (PostgreSQL 15+)
    ///   uses a column the migration drops (with `use_drop`), retypes, or
    ///   drops and re-adds — itself, a generated column taken out of the way
    ///   of a retype ([`Table::rewrite_column_drops`]), or a partition's copy
    ///   of one its parent's statements drop — PostgreSQL refuses the `DROP
    ///   COLUMN` ("other objects depend on it") and the `ALTER COLUMN ...
    ///   TYPE` ("cannot alter type of a column used by a publication WHERE
    ///   clause", column lists included). A generated column a routine's
    ///   `CASCADE` takes is not retyped by the table phase (see
    ///   `Comparer::enter_table_phase_view`), and its rows go with that
    ///   `CASCADE` rather than blocking it: `recreate_routine_dependents`
    ///   puts them back. Nor is a typed table's column that its type does not
    ///   retype (see `Comparer::unretyped_typed_columns`);
    /// - a member the migration makes `UNLOGGED`;
    /// - on PostgreSQL 19, an EXCEPT entry the migration makes `UNLOGGED` or
    ///   attaches as a partition.
    ///
    /// A table the migration drops loses its rows with its `DROP TABLE` and
    /// needs nothing here (`gone_tables` covers it). FROM-only publications
    /// are released too: they are dropped only after the tables, and not at
    /// all without `use_drop` — then, like a publication whose drop+recreate
    /// is commented out, `compare_publications` sets its own list again after
    /// the tables (`Publication::get_restore_script`), as nothing else would.
    /// The section is emitted only when something is released, so other
    /// migrations are unchanged.
    fn release_publication_blockers(&mut self) {
        let to_tables: HashMap<(&str, &str), &Table> = self
            .to
            .tables
            .iter()
            .map(|t| ((t.schema.as_str(), t.name.as_str()), t))
            .collect();
        let alter_caps = self.table_alter_caps();
        let cascade_taken = self.cascade_taken_column_keys(&self.cascade_readded);
        // Typed-table columns no statement retypes, however they differ.
        let unretyped_typed_columns = self.unretyped_typed_columns();
        let normalise = |s: &str| s.to_lowercase().replace('"', "");

        // Per FROM table (`schema.table`, as publication entries spell it):
        // the columns the migration drops, retypes or re-adds, whether it
        // becomes UNLOGGED, and whether it is attached as a partition.
        let mut blocked_columns: HashMap<String, HashSet<String>> = HashMap::new();
        let mut goes_unlogged: HashSet<String> = HashSet::new();
        let mut gets_attached: HashSet<String> = HashSet::new();
        for table in &self.from.tables {
            let Some(target) = to_tables.get(&(table.schema.as_str(), table.name.as_str())) else {
                continue;
            };
            // A table the migration drops and recreates needs nothing, but
            // `will_be_dropped_and_recreated` also answers true for a
            // partition retyped through its parent, which keeps its rows (and
            // their column dependencies). Releasing a member that goes anyway
            // costs one statement, so no such table is skipped.
            if !Self::hashes_differ(&table.hash, &target.hash) {
                continue;
            }
            let key = format!("{}.{}", table.schema, table.name);
            let generated_caps = Self::caps_for(&alter_caps, table).generated;
            let retyped_by_readd = |column: &TableColumn| {
                cascade_taken.contains(&Self::normalized_view_reference(&format!(
                    "{}.{}.{}",
                    table.schema, table.name, column.name
                )))
            };
            let mut columns: HashSet<String> = table
                .columns
                .iter()
                .filter(
                    |column| match target.columns.iter().find(|c| c.name == column.name) {
                        None => self.use_drop,
                        Some(target_column) => {
                            (column.type_differs(target_column)
                                && !retyped_by_readd(column)
                                && !unretyped_typed_columns.contains(&(
                                    normalise(&table.schema),
                                    normalise(&table.name),
                                    normalise(&column.name),
                                )))
                                || (self.use_drop
                                    && target_column
                                        .would_drop_and_re_add_with_caps(column, generated_caps))
                        }
                    },
                )
                .map(|column| column.name.clone())
                .collect();
            columns.extend(table.rewrite_column_drops(target, self.use_drop, generated_caps));
            if !columns.is_empty() {
                blocked_columns.insert(key.clone(), columns);
            }
            if table.persistence_change_for(target) == Some(true) {
                goes_unlogged.insert(key.clone());
            }
            if target.partition_of.is_some() && table.partition_of != target.partition_of {
                gets_attached.insert(key);
            }
        }
        if blocked_columns.is_empty() && goes_unlogged.is_empty() && gets_attached.is_empty() {
            return;
        }

        let mut script = String::new();
        for publication in &self.from.publications {
            let members: Vec<String> = publication
                .tables
                .iter()
                .filter(|t| {
                    goes_unlogged.contains(*t)
                        || blocked_columns.get(*t).is_some_and(|columns| {
                            publication
                                .table_specs
                                .get(*t)
                                .is_some_and(|spec| spec.uses_any_column(columns))
                        })
                })
                .cloned()
                .collect();
            let except: Vec<String> = if publication.all_tables {
                publication
                    .except_tables
                    .iter()
                    .filter(|t| goes_unlogged.contains(*t) || gets_attached.contains(*t))
                    .cloned()
                    .collect()
            } else {
                Vec::new()
            };
            if members.is_empty() && except.is_empty() {
                continue;
            }
            script.push_str(&publication.get_release_script(&members, &except));
            self.released_publication_tables
                .entry(publication.name.clone())
                .or_default()
                .extend(members.into_iter().chain(except));
        }
        if script.is_empty() {
            return;
        }
        self.unlogged_publication_tables = goes_unlogged;
        self.script.append_block(
            "\n/* ---> Publications: release members that block table changes --------------- */",
        );
        self.script.push_str(&script);
    }

    async fn compare_publications(&mut self) -> Result<(), Error> {
        self.script
            .append_block("/* ---> Compare Publications ----------------- */");

        let from_map: HashMap<&str, &crate::dump::publication::Publication> = self
            .from
            .publications
            .iter()
            .map(|p| (p.name.as_str(), p))
            .collect();

        let ctx = self.publication_alter_context();
        // The rows `release_publication_blockers` took out of a publication
        // that stays as it is: nothing below restores them.
        // `--use-comments false` strips the notes on what cannot come back,
        // hence a warning as well.
        let restore = |publication: &crate::dump::publication::Publication| -> String {
            match ctx.released_tables.get(&publication.name) {
                Some(released) if !self.use_drop => {
                    for note in
                        publication.released_for_good(released, &self.unlogged_publication_tables)
                    {
                        eprintln!("Warning: {note}");
                    }
                    publication.get_restore_script(
                        released,
                        &self.unlogged_publication_tables,
                        &ctx,
                    )
                }
                _ => String::new(),
            }
        };
        let mut script = String::new();
        for pub_ in &self.from.publications {
            if !self.to.publications.iter().any(|p| p.name == pub_.name) {
                Self::emit_drop(&mut script, self.use_drop, &pub_.get_drop_script());
                script.push_str(&restore(pub_));
            }
        }

        for pub_ in &self.to.publications {
            if let Some(existing) = from_map.get(pub_.name.as_str()) {
                // Equal hashes are not enough when the migration dropped and
                // recreated a member or EXCEPT table: its row went with it.
                if existing.hash != pub_.hash || ctx.touches(existing) {
                    let alter = existing.get_alter_script_with(pub_, self.use_drop, &ctx);
                    if !alter.is_empty() {
                        script.push_str(&alter);
                    }
                    if existing.alter_recreates(pub_, &ctx) {
                        script.push_str(&restore(existing));
                    }
                }
            } else {
                script.push_str(&pub_.get_script_with(&ctx));
            }
        }
        self.script.push_str(&script);

        Ok(())
    }

    /// Drop the FROM-only subscriptions. Called before every other section.
    ///
    /// A subscription has no local dependencies of its own, but on PG19 a
    /// `SERVER` subscription blocks `DROP SERVER`, `DROP FOREIGN DATA WRAPPER`
    /// and `DROP EXTENSION` of the wrapper's extension — even with `CASCADE`
    /// ("Drop subscription S first"), which also rules out a cascading
    /// `DROP SCHEMA`. One that still owns a replication slot also needs its
    /// server and user mapping to reach the publisher and drop the slot,
    /// which is why the server and wrapper drops are deferred to
    /// `drop_foreign_servers_and_fdws`. The section header is only emitted
    /// when something is dropped, so an unchanged schema renders as before.
    ///
    /// A pgc 1.0.x FROM dump against a pgc 1.1+ TO dump
    /// (`legacy_from_against_current_to`) holds the subscriptions of every
    /// database in the cluster, where two databases can use one name: each
    /// name is dropped once there.
    async fn drop_subscriptions(&mut self) -> Result<(), Error> {
        let mut seen: HashSet<&str> = HashSet::new();
        let dedup = self.legacy_from_against_current_to();
        let from_only: Vec<String> = self
            .from
            .subscriptions
            .iter()
            .filter(|sub| !self.to.subscriptions.iter().any(|s| s.name == sub.name))
            .filter(|sub| !dedup || seen.insert(sub.name.as_str()))
            .map(|sub| sub.get_drop_script())
            .collect();
        if from_only.is_empty() {
            return Ok(());
        }

        self.script
            .append_block("\n/* ---> Subscriptions DROP: Start section --------------- */");
        for drop_script in &from_only {
            Self::emit_drop(&mut self.script, self.use_drop, drop_script);
        }

        Ok(())
    }

    /// Alter or create the subscriptions present in TO. FROM-only ones were
    /// already dropped by `drop_subscriptions`. Runs after servers and user
    /// mappings, which a PG19 `SERVER` subscription needs at `CREATE` /
    /// `ALTER ... SERVER` time. Against a pgc 1.0.x FROM dump every TO
    /// subscription is created (see `from_subscriptions_by_name`).
    async fn compare_subscriptions(&mut self) -> Result<(), Error> {
        self.script
            .append_block("/* ---> Compare Subscriptions ---------------- */");

        let from_map = Self::from_subscriptions_by_name(
            &self.from.subscriptions,
            self.legacy_from_against_current_to(),
        );
        let rules = self.slot_name_rules();

        for sub in &self.to.subscriptions {
            if let Some(existing) = from_map.get(sub.name.as_str()) {
                if existing.hash != sub.hash {
                    let alter = existing.get_alter_script_for(
                        sub,
                        self.use_drop,
                        self.use_single_transaction,
                        rules,
                    );
                    self.script.push_str(&alter.script);
                    self.subscription_post_commit_script
                        .push_str(&alter.post_commit);
                }
            } else {
                self.script.push_str(&sub.get_script_for(rules));
            }
        }

        if let Some(warning) = self.worker_restart_warning() {
            eprintln!("{warning}");
        }
        if let Some(warning) = self.unsettable_slot_warning() {
            eprintln!("{warning}");
        }

        Ok(())
    }

    /// The slot names the migrated server accepts as a subscription's
    /// `slot_name`: PostgreSQL 19 reserves `pg_conflict_detection`. A FROM
    /// dump written by pgc 1.0.x does not record its server version, so it
    /// gets the rules of PostgreSQL 14 to 18.
    fn slot_name_rules(&self) -> SlotNameRules {
        SlotNameRules {
            conflict_slot_reserved: self
                .migrated_server_at_least(SlotNameRules::CONFLICT_SLOT_RESERVED_MIN_VERSION),
        }
    }

    /// The warning printed when the migration cannot give TO subscriptions
    /// their replication slot, because PostgreSQL does not accept the slot's
    /// name as a `slot_name` (see
    /// [`Subscription::slot_out_of_reach`](crate::dump::publication::Subscription::slot_out_of_reach)).
    ///
    /// The script leaves such a slot out, with a `--` note. That note is lost
    /// under `--use-comments false`, hence a warning at generation time.
    /// Returns `None` when every slot can be set.
    fn unsettable_slot_warning(&self) -> Option<String> {
        let rules = self.slot_name_rules();
        let from_map = Self::from_subscriptions_by_name(
            &self.from.subscriptions,
            self.legacy_from_against_current_to(),
        );
        let slots: Vec<String> = self
            .to
            .subscriptions
            .iter()
            .filter(|sub| {
                let from = from_map.get(sub.name.as_str()).copied();
                from.is_none_or(|existing| existing.hash != sub.hash)
                    && sub.slot_out_of_reach(from, rules)
            })
            .map(|sub| {
                format!(
                    "{} (slot '{}')",
                    sub.name,
                    sub.slot().unwrap_or_default().replace('\'', "''")
                )
            })
            .collect();
        if slots.is_empty() {
            return None;
        }
        Some(format!(
            "Warning: PostgreSQL does not accept the replication slot of subscription(s) {} as \
             a slot_name ({SLOT_NAME_RULE}). The migration leaves that slot out, so those \
             subscriptions keep a slot other than the target's, and the next compare warns \
             again. Give them a slot PostgreSQL accepts by hand.",
            slots.join(", ")
        ))
    }

    /// The warning printed when a `--use-single-transaction` migration has to
    /// bounce an enabled subscription to change `retain_dead_tuples` (see
    /// [`Subscription::restarts_apply_worker`](crate::dump::publication::Subscription::restarts_apply_worker)).
    ///
    /// The apply worker only sees the `DISABLE` once it commits, so inside
    /// `begin` / `commit` the `SET` fails while the worker runs and the whole
    /// migration rolls back. The script's own `--` note on the bounce is lost
    /// under `--use-comments false`, hence a warning at generation time.
    /// `None` without a single transaction, where each statement commits on
    /// its own and the migration waits after the `DISABLE` until the
    /// subscription's workers have exited.
    fn worker_restart_warning(&self) -> Option<String> {
        if !self.use_single_transaction {
            return None;
        }
        let from_map = Self::from_subscriptions_by_name(
            &self.from.subscriptions,
            self.legacy_from_against_current_to(),
        );
        let names: Vec<&str> = self
            .to
            .subscriptions
            .iter()
            .filter(|sub| {
                from_map.get(sub.name.as_str()).is_some_and(|existing| {
                    existing.hash != sub.hash && existing.restarts_apply_worker(sub)
                })
            })
            .map(|sub| sub.name.as_str())
            .collect();
        if names.is_empty() {
            return None;
        }
        Some(format!(
            "Warning: retain_dead_tuples changes on enabled subscription(s) {}. PostgreSQL \
             accepts that only once the apply worker has exited, which the DISABLE emitted \
             before it cannot achieve inside a single transaction: while the worker runs the \
             migration fails with \"logical replication worker is still running\" and rolls \
             back. Run those ALTER SUBSCRIPTION ... DISABLE / SET / ENABLE statements on their \
             own, or generate the migration without --use-single-transaction.",
            names.join(", ")
        ))
    }

    /// Alter or create the foreign-data wrappers present in TO. FROM-only
    /// wrappers are dropped later, by `drop_foreign_servers_and_fdws`.
    async fn compare_fdws(&mut self) -> Result<(), Error> {
        self.script
            .append_block("/* ---> Compare Foreign Data Wrappers -------- */");

        let from_map: HashMap<&str, &crate::dump::fdw::ForeignDataWrapper> = self
            .from
            .foreign_data_wrappers
            .iter()
            .map(|f| (f.name.as_str(), f))
            .collect();

        for fdw in &self.to.foreign_data_wrappers {
            if let Some(existing) = from_map.get(fdw.name.as_str()) {
                if existing.hash != fdw.hash {
                    let alter = existing.get_alter_script(fdw, self.use_drop);
                    if !alter.is_empty() {
                        self.script.push_str(&alter);
                    }
                }
            } else {
                self.script.push_str(&fdw.get_script());
            }
        }

        Ok(())
    }

    /// Alter or create the foreign servers present in TO. FROM-only servers
    /// are dropped later, by `drop_foreign_servers_and_fdws`.
    async fn compare_servers(&mut self) -> Result<(), Error> {
        self.script
            .append_block("/* ---> Compare Foreign Servers -------------- */");

        let from_map: HashMap<&str, &crate::dump::fdw::ForeignServer> = self
            .from
            .foreign_servers
            .iter()
            .map(|s| (s.name.as_str(), s))
            .collect();

        for srv in &self.to.foreign_servers {
            if let Some(existing) = from_map.get(srv.name.as_str()) {
                if existing.hash != srv.hash {
                    if let Some(subscriptions) = self.refused_servers.get(&srv.name) {
                        self.script.push_str(&format!(
                            "-- Foreign server {} changes its foreign-data wrapper, which takes \
                             DROP SERVER and CREATE SERVER, and PostgreSQL refuses that drop \
                             while subscription {} uses it: left as it is.\n",
                            line_comment_safe(&srv.name),
                            line_comment_safe(&subscriptions.join(", "))
                        ));
                        eprintln!(
                            "Warning: foreign server {} changes its foreign-data wrapper ({} to \
                             {}), which takes DROP SERVER and CREATE SERVER, but subscription {} \
                             uses it, and PostgreSQL refuses the drop while it does. The \
                             migration leaves the server as it is: move the subscription off it \
                             (or drop it) first.",
                            srv.name,
                            existing.fdw_name,
                            srv.fdw_name,
                            subscriptions.join(", ")
                        );
                        continue;
                    }
                    if self.recreated_servers.contains(&srv.name) {
                        let recreate = self.server_recreate_script(existing, srv);
                        self.script.push_str(&recreate);
                        continue;
                    }
                    let alter = existing.get_alter_script(srv, self.use_drop);
                    if !alter.is_empty() {
                        self.script.push_str(&alter);
                    }
                }
            } else {
                self.script.push_str(&srv.get_script());
            }
        }

        Ok(())
    }

    /// The FROM foreign servers whose foreign-data wrapper changes, which
    /// `ALTER SERVER` cannot do, sorted into `recreated_servers` and
    /// `refused_servers`. Nothing without `use_drop`, where the server's
    /// alter only comments its drop out.
    ///
    /// `DROP SERVER ... CASCADE` would silently take the server's user
    /// mappings and every foreign table on it — with their privileges,
    /// comments, and the views over them — and nothing in the migration
    /// brought them back. So the migration drops the foreign tables on it
    /// with the other foreign tables (the views over them go and come back
    /// as for any foreign table that is dropped and created again), drops
    /// its user mappings and then the server without `CASCADE`, and creates
    /// the server, its TO user mappings and the TO foreign tables on it
    /// again (see `Comparer::server_recreate_script`) — a foreign partition
    /// attached to its partitioned table again (see
    /// [`ForeignTable::get_script`](crate::dump::foreign_table::ForeignTable::get_script)) — and
    /// `compare_grants` gives the server and those foreign tables TO's
    /// privileges, column privileges included. Whatever else still uses the
    /// server — a foreign table in a schema the dump does not cover — makes
    /// the drop fail instead of going with it.
    ///
    /// On PostgreSQL 19 a subscription created with `SERVER` blocks the
    /// drop even with `CASCADE`. A FROM one the migration keeps is still
    /// there when the server would go, so such a server is refused: left as
    /// it is, with a warning.
    fn plan_server_recreates(&mut self) {
        if !self.use_drop {
            return;
        }
        for srv in &self.from.foreign_servers {
            let Some(target) = self.to.foreign_servers.iter().find(|s| s.name == srv.name) else {
                continue;
            };
            if !srv.changes_wrapper(target) {
                continue;
            }
            let subscriptions: Vec<String> = self
                .from
                .subscriptions
                .iter()
                .filter(|sub| {
                    sub.server.as_deref() == Some(srv.name.as_str())
                        && self.to.subscriptions.iter().any(|s| s.name == sub.name)
                })
                .map(|sub| sub.name.clone())
                .collect();
            if subscriptions.is_empty() {
                // `compare_grants` gives the server and the foreign tables
                // on it TO's privileges back; with grants ignored nothing
                // does, and they start from the defaults.
                let privileged = !target.acl.is_empty()
                    || self.to.foreign_tables.iter().any(|ft| {
                        ft.server == target.name
                            && (!ft.acl.is_empty() || ft.columns.iter().any(|c| !c.acl.is_empty()))
                    });
                if privileged && self.grants_mode == GrantsMode::Ignore {
                    eprintln!(
                        "Warning: foreign server {} changes its foreign-data wrapper and is \
                         created again, with the foreign tables on it; with --grants-mode ignore \
                         their privileges are not restored.",
                        srv.name
                    );
                }
                self.recreated_servers.insert(srv.name.clone());
            } else {
                self.refused_servers.insert(srv.name.clone(), subscriptions);
            }
        }
    }

    /// The statements that replace FROM server `from` with TO server `to`
    /// (see `Comparer::plan_server_recreates`): its FROM user mappings and
    /// the server itself dropped without `CASCADE` — the foreign tables on
    /// it are gone already — then the server, its TO user mappings and the
    /// TO foreign tables on it created again.
    fn server_recreate_script(
        &self,
        from: &crate::dump::fdw::ForeignServer,
        to: &crate::dump::fdw::ForeignServer,
    ) -> String {
        let mut script = String::from(
            "-- The foreign-data wrapper changes, which ALTER SERVER cannot do: the server is \
             dropped without CASCADE and created again, with its user mappings and the foreign \
             tables on it.\n",
        );
        for mapping in self
            .from
            .user_mappings
            .iter()
            .filter(|um| um.server_name == from.name)
        {
            script.push_str(&mapping.get_drop_script());
        }
        script.push_str(&from.get_restrict_drop_script());
        script.push_str(&to.get_script());
        for mapping in self
            .to
            .user_mappings
            .iter()
            .filter(|um| um.server_name == to.name)
        {
            script.push_str(&mapping.get_script());
        }
        if let Some(foreign_tables) = self.foreign_tables_after_servers.get(&to.name) {
            script.push_str(foreign_tables);
        }
        script
    }

    /// Drop the FROM-only foreign servers, then the FROM-only foreign-data
    /// wrappers. Called after `compare_subscriptions`.
    ///
    /// On PG19 a subscription created with `SERVER` depends on its server,
    /// and neither `DROP SERVER ... CASCADE` nor `DROP FOREIGN DATA WRAPPER
    /// ... CASCADE` removes it — both fail. Deferring the drops until the
    /// FROM-only subscriptions are gone (`drop_subscriptions`) and the kept
    /// ones have been moved to their TO source (`compare_subscriptions`)
    /// lets them succeed. Servers go before wrappers, so a wrapper's
    /// `CASCADE` never has a server left to remove. The section header is
    /// only emitted when something is dropped.
    async fn drop_foreign_servers_and_fdws(&mut self) -> Result<(), Error> {
        let mut drops: Vec<String> = self
            .from
            .foreign_servers
            .iter()
            .filter(|srv| !self.to.foreign_servers.iter().any(|s| s.name == srv.name))
            .map(|srv| srv.get_drop_script())
            .collect();
        drops.extend(
            self.from
                .foreign_data_wrappers
                .iter()
                .filter(|fdw| {
                    !self
                        .to
                        .foreign_data_wrappers
                        .iter()
                        .any(|f| f.name == fdw.name)
                })
                .map(|fdw| fdw.get_drop_script()),
        );
        if drops.is_empty() {
            return Ok(());
        }

        self.script.append_block(
            "\n/* ---> Foreign Servers and Data Wrappers DROP: Start section --------------- */",
        );
        for drop_script in &drops {
            Self::emit_drop(&mut self.script, self.use_drop, drop_script);
        }

        Ok(())
    }

    async fn compare_user_mappings(&mut self) -> Result<(), Error> {
        self.script
            .append_block("/* ---> Compare User Mappings ---------------- */");

        let um_key =
            |um: &crate::dump::fdw::UserMapping| format!("{}@{}", um.username, um.server_name);

        let from_map: HashMap<String, &crate::dump::fdw::UserMapping> = self
            .from
            .user_mappings
            .iter()
            .map(|um| (um_key(um), um))
            .collect();

        // The mappings of a server created again went with it (see
        // `server_recreate_script`).
        for um in &self.from.user_mappings {
            let key = um_key(um);
            if !self.to.user_mappings.iter().any(|u| um_key(u) == key)
                && !self.recreated_servers.contains(&um.server_name)
            {
                Self::emit_drop(&mut self.script, self.use_drop, &um.get_drop_script());
            }
        }

        for um in &self.to.user_mappings {
            if self.recreated_servers.contains(&um.server_name) {
                continue;
            }
            let key = um_key(um);
            if let Some(existing) = from_map.get(&key) {
                if existing.hash != um.hash {
                    let alter = existing.get_alter_script(um);
                    if !alter.is_empty() {
                        self.script.push_str(&alter);
                    }
                }
            } else {
                self.script.push_str(&um.get_script());
            }
        }

        Ok(())
    }

    /// Compare routines and views together, respecting cross-dependencies.
    ///
    /// Instead of processing all routines before all views (which breaks when
    /// a view depends on a function, or a function depends on a view), this
    /// method builds a dependency graph across both object types and emits
    /// CREATE / UPDATE statements in topological order.
    ///
    /// Drop statements for routines that exist only in the FROM dump are
    /// emitted first, also in reverse dependency order.
    async fn compare_routines_and_views(&mut self) -> Result<(), Error> {
        self.script
            .append_block("\n/* ---> Routines & Views: Start section --------------- */");

        // Build O(1) lookup maps (owned keys to avoid borrowing self)
        let to_routine_keys: HashSet<(String, String, String)> = self
            .to
            .routines
            .iter()
            .map(|r| (r.schema.clone(), r.name.clone(), r.arguments.clone()))
            .collect();

        let from_routine_map: HashMap<(String, String, String), usize> = self
            .from
            .routines
            .iter()
            .enumerate()
            .map(|(i, r)| ((r.schema.clone(), r.name.clone(), r.arguments.clone()), i))
            .collect();

        let from_view_map: HashMap<(String, String), usize> = self
            .from
            .views
            .iter()
            .enumerate()
            .map(|(i, v)| ((v.schema.clone(), v.name.clone()), i))
            .collect();

        // ──────────────────────────────────────────────────────────
        // Phase 1 – Drop routines that are not present in TO
        // ──────────────────────────────────────────────────────────
        // Borrow rather than clone: dependency analysis and emission
        // only need read access; `self.from.routines` is not mutated
        // during this function, and emission only mutates `self.script`
        // (a disjoint field).
        // A routine `drop_views` already dropped is not dropped again.
        let routines_to_drop: Vec<&Routine> = self
            .from
            .routines
            .iter()
            .filter(|r| {
                let key = (r.schema.clone(), r.name.clone(), r.arguments.clone());
                !to_routine_keys.contains(&key) && !self.predropped_routines.contains(&key)
            })
            .collect();

        let mut detached_by_drops: Vec<(String, String)> = Vec::new();
        if !routines_to_drop.is_empty() {
            // Build inter-routine dependency graph and sort drops in
            // reverse dependency order (dependents first).
            let mut drop_deps: Vec<HashSet<usize>> = vec![HashSet::new(); routines_to_drop.len()];

            let drop_names: Vec<(String, String, String)> = routines_to_drop
                .iter()
                .map(|r| {
                    (
                        r.schema.to_lowercase(),
                        r.name.to_lowercase(),
                        r.arguments.to_lowercase(),
                    )
                })
                .collect();

            // Prelower each source_code once; the scan below is O(n²) in routine
            // count and previously re-lowered the full source on every call.
            let drop_sources: Vec<(String, String)> = routines_to_drop
                .iter()
                .map(|r| Self::prelower_pair_for_dependency_scan(&r.source_code))
                .collect();

            for (i, routine) in routines_to_drop.iter().enumerate() {
                let (src_lower, src_unquoted) = &drop_sources[i];
                for (j, (schema, name, _)) in drop_names.iter().enumerate() {
                    if i != j
                        && (Self::text_references_qualified_name_pre(
                            src_lower,
                            src_unquoted,
                            schema,
                            name,
                        ) || Self::aggregate_references_function(routine, schema, name))
                    {
                        drop_deps[i].insert(j);
                    }
                }
            }

            Self::warn_dependency_cycle(
                "routines being dropped",
                routines_to_drop.len(),
                &drop_deps,
                |i| {
                    let r = routines_to_drop[i];
                    format!("{}.{}({})", r.schema, r.name, r.arguments)
                },
            );
            let mut drop_order = Self::kahn_toposort(routines_to_drop.len(), &drop_deps, |i| {
                drop_names[i].clone()
            });
            drop_order.reverse();

            for idx in drop_order {
                let routine = routines_to_drop[idx];
                self.script.push_str(
                    format!(
                        "/* Routine: {}.{}({})*/\n",
                        block_comment_safe(&routine.schema),
                        block_comment_safe(&routine.name),
                        block_comment_safe(&routine.arguments)
                    )
                    .as_str(),
                );
                self.script
                    .push_str("/* Routine is not present in 'to' dump and should be dropped. */\n");
                let restrict_note = self.routine_phase_cascade_note(routine);
                let drop_script = match &restrict_note {
                    Some(note) => {
                        self.script.push_str(note);
                        Self::without_cascade(&routine.get_drop_script())
                    }
                    None => {
                        let (detach, detached) = self.domain_default_detach(routine);
                        Self::emit_drop(&mut self.script, self.use_drop, &detach);
                        detached_by_drops.extend(detached);
                        routine.get_drop_script()
                    }
                };
                if self.use_drop {
                    self.script.push_str(drop_script.as_str());
                } else {
                    self.script.push_str(comment_out(&drop_script).as_str());
                }
            }
        }

        self.record_detached_domain_defaults(detached_by_drops);

        // ──────────────────────────────────────────────────────────
        // Phase 2 – Determine which objects need create / update
        // ──────────────────────────────────────────────────────────
        // Each item is (is_view, index_in_to_vec).
        let mut action_items: Vec<(bool, usize)> = Vec::new();
        let mut no_action_view_indices: Vec<usize> = Vec::new();

        for (idx, routine) in self.to.routines.iter().enumerate() {
            if routine.hash.is_none() {
                continue;
            }
            let from_routine = from_routine_map
                .get(&(
                    routine.schema.clone(),
                    routine.name.clone(),
                    routine.arguments.clone(),
                ))
                .map(|&i| &self.from.routines[i]);
            // A routine `drop_views` dropped ahead of the relation DDL is
            // re-created whether or not it changed, like a view in
            // `dropped_views`.
            let routine_key = (
                routine.schema.clone(),
                routine.name.clone(),
                routine.arguments.clone(),
            );
            let predropped = self.predropped_routines.contains(&routine_key)
                || self.cascade_victim_routines.contains(&routine_key);
            let needs_action = predropped
                || match from_routine {
                    None => true,
                    Some(fr) => fr.hash.is_some() && Self::hashes_differ(&fr.hash, &routine.hash),
                };
            if needs_action {
                action_items.push((false, idx));
            }
        }

        for (idx, view) in self.to.views.iter().enumerate() {
            let normalized_view = Self::normalized_view_key(&view.schema, &view.name);
            let from_view = from_view_map
                .get(&(view.schema.clone(), view.name.clone()))
                .map(|&i| &self.from.views[i]);
            let was_dropped = self.dropped_views.contains_key(&normalized_view);
            let definition_changed = from_view
                .map(|fv| Self::hashes_differ(&fv.hash, &view.hash))
                .unwrap_or(false);
            let needs_action = from_view.is_none() || was_dropped || definition_changed;
            if needs_action {
                action_items.push((true, idx));
            } else {
                no_action_view_indices.push(idx);
            }
        }

        // ──────────────────────────────────────────────────────────
        // Phase 3 – Build dependency graph among action items
        // ──────────────────────────────────────────────────────────
        // Precompute info we need for each action item so we don't
        // hold any reference to `self` while building the graph.
        // `text_lower` / `text_unquoted_lower` are prelowered once and reused
        // in the O(n²) reference scan below.
        struct ItemInfo {
            text_lower: String,
            text_unquoted_lower: String,
            schema_lower: String,
            name_lower: String,
            is_view: bool,
            orig_idx: usize,
            sort_key: (u8, String, String),
            table_relation_keys: Vec<String>,
            // A materialized view's index definitions, prelowered: the view
            // is created with them, so after the routines they call.
            index_text: Option<(String, String)>,
        }

        let items: Vec<ItemInfo> = action_items
            .iter()
            .map(|&(is_view, orig_idx)| {
                if is_view {
                    let v = &self.to.views[orig_idx];
                    let table_relation_keys: Vec<String> = v
                        .table_relation
                        .iter()
                        .map(|rel| Self::normalized_view_reference(rel))
                        .collect();
                    let (text_lower, text_unquoted_lower) =
                        Self::prelower_pair_for_dependency_scan(&v.definition);
                    let index_text = (v.is_materialized && !v.indexes.is_empty()).then(|| {
                        let defs: Vec<&str> =
                            v.indexes.iter().map(|i| i.indexdef.as_str()).collect();
                        Self::prelower_pair_for_dependency_scan(&defs.join("\n"))
                    });
                    ItemInfo {
                        text_lower,
                        text_unquoted_lower,
                        schema_lower: v.schema.to_lowercase(),
                        name_lower: v.name.to_lowercase(),
                        is_view: true,
                        orig_idx,
                        sort_key: (2, v.schema.to_lowercase(), v.name.to_lowercase()),
                        table_relation_keys,
                        index_text,
                    }
                } else {
                    let r = &self.to.routines[orig_idx];
                    let priority = if r.lang.eq_ignore_ascii_case("sql") {
                        1
                    } else {
                        0
                    };
                    let (mut text_lower, mut text_unquoted_lower) =
                        Self::prelower_pair_for_dependency_scan(&r.source_code);
                    // Argument defaults are parsed at CREATE time, whatever
                    // the body is, so a routine they call goes first. Scanned
                    // on their own: an unterminated quote in a body must not
                    // hide them.
                    if let Some(defaults) = &r.arguments_defaults {
                        let (lower, unquoted) = Self::prelower_pair_for_dependency_scan(defaults);
                        text_lower.push('\n');
                        text_lower.push_str(&lower);
                        text_unquoted_lower.push('\n');
                        text_unquoted_lower.push_str(&unquoted);
                    }
                    // A SQL-standard body is resolved at CREATE time, so the
                    // relations `pg_depend` records for it order it like a
                    // view's table_relation does.
                    let table_relation_keys: Vec<String> = r
                        .sql_body_dependencies
                        .relations
                        .iter()
                        .map(|rel| Self::normalized_view_reference(rel))
                        .collect();
                    ItemInfo {
                        text_lower,
                        text_unquoted_lower,
                        schema_lower: r.schema.to_lowercase(),
                        name_lower: r.name.to_lowercase(),
                        is_view: false,
                        orig_idx,
                        sort_key: (priority, r.schema.to_lowercase(), r.name.to_lowercase()),
                        table_relation_keys,
                        index_text: None,
                    }
                }
            })
            .collect();

        // Reverse lookup: (schema, name) -> list of item indices
        let mut name_to_items: HashMap<(String, String), Vec<usize>> = HashMap::new();
        for (i, item) in items.iter().enumerate() {
            name_to_items
                .entry((item.schema_lower.clone(), item.name_lower.clone()))
                .or_default()
                .push(i);
        }

        // Routine items by `Routine::dependency_key`, for the routines a
        // SQL-standard body calls — including a cast's function, which the
        // body text never names.
        let routine_key_to_item: HashMap<String, usize> = items
            .iter()
            .enumerate()
            .filter(|(_, item)| !item.is_view)
            .map(|(i, item)| (self.to.routines[item.orig_idx].dependency_key(), i))
            .collect();

        // Build edges: i depends on j  =>  j must be emitted before i.
        let mut depends_on: Vec<HashSet<usize>> = vec![HashSet::new(); items.len()];

        for (i, item) in items.iter().enumerate() {
            // Scan the object body for qualified references to other action items.
            for ((schema, name), targets) in &name_to_items {
                if Self::text_references_qualified_name_pre(
                    &item.text_lower,
                    &item.text_unquoted_lower,
                    schema,
                    name,
                ) {
                    for &j in targets {
                        if j != i {
                            depends_on[i].insert(j);
                        }
                    }
                }
            }

            // For aggregate routines, check if their sfunc/finalfunc/etc.
            // reference any other action item.
            if !item.is_view {
                let routine = &self.to.routines[item.orig_idx];
                if routine.aggregate_info.is_some() {
                    for ((schema, name), targets) in &name_to_items {
                        if Self::aggregate_references_function(routine, schema, name) {
                            for &j in targets {
                                if j != i {
                                    depends_on[i].insert(j);
                                }
                            }
                        }
                    }
                }
            }

            // For a SQL-standard body, the routines pg_depend records.
            if !item.is_view {
                let routine = &self.to.routines[item.orig_idx];
                for key in &routine.sql_body_dependencies.routines {
                    if let Some(&j) = routine_key_to_item.get(key)
                        && j != i
                    {
                        depends_on[i].insert(j);
                    }
                }
            }

            // For views, also honour the explicit table_relation metadata
            // (which captures referenced tables / views but not functions),
            // and likewise the relations a SQL-standard body reads.
            {
                for dep_key in &item.table_relation_keys {
                    if let Some(dot_pos) = dep_key.find('.') {
                        let schema = dep_key[..dot_pos].to_string();
                        let name = dep_key[dot_pos + 1..].to_string();
                        if let Some(targets) = name_to_items.get(&(schema, name)) {
                            for &j in targets {
                                if j != i {
                                    depends_on[i].insert(j);
                                }
                            }
                        }
                    }
                }
            }
        }

        // A materialized view's indexes are created with it, so it follows the
        // routines they call: a new routine has to exist by then, and one
        // dropped with `CASCADE` and re-created here would take the index
        // along if it came later. Not where the routine already follows the
        // view — its body reads it — which would only make a cycle.
        for (i, item) in items.iter().enumerate() {
            let Some((index_lower, index_unquoted)) = &item.index_text else {
                continue;
            };
            for ((schema, name), targets) in &name_to_items {
                if !Self::text_references_qualified_name_pre(
                    index_lower,
                    index_unquoted,
                    schema,
                    name,
                ) {
                    continue;
                }
                for &j in targets {
                    if j != i && !items[j].is_view && !depends_on[j].contains(&i) {
                        depends_on[i].insert(j);
                    }
                }
            }
        }

        // ──────────────────────────────────────────────────────────
        // Phase 4 – Topological sort
        // ──────────────────────────────────────────────────────────
        Self::warn_dependency_cycle("routines and views", items.len(), &depends_on, |i| {
            let item = &items[i];
            let kind = if item.is_view { "view" } else { "routine" };
            format!("{kind} {}.{}", item.schema_lower, item.name_lower)
        });
        let sorted = Self::kahn_toposort(items.len(), &depends_on, |i| items[i].sort_key.clone());

        // Operators and casts are created after this section (they need their
        // functions), but a SQL-standard body using one is resolved when the
        // routine is created, whatever `check_function_bodies` says. Such a
        // routine — and everything here that depends on it — is held back
        // until after them (`deferred_routine_script`).
        let held_back = self.items_needing_later_operators_or_casts(
            items
                .iter()
                .map(|item| (!item.is_view).then_some(item.orig_idx)),
            &depends_on,
        );

        // What reads a generated column the routines' `CASCADE` takes and
        // Phase 7 re-adds waits for it (`after_readd_script`).
        let after_readd = self.items_reading_cascade_readded_columns(
            items.iter().map(|item| (item.is_view, item.orig_idx)),
            &depends_on,
        );

        // Prepare the emit plan (is_view, orig_idx, held back, after the
        // re-added columns) so we can drop `items`.
        let emit_order: Vec<(bool, usize, bool, bool)> = sorted
            .iter()
            .map(|&i| {
                (
                    items[i].is_view,
                    items[i].orig_idx,
                    held_back.contains(&i),
                    after_readd.contains(&i),
                )
            })
            .collect();
        // Whether an item waiting for the re-added columns also needs the
        // operators or casts created after this section.
        let after_readd_needs_later = emit_order
            .iter()
            .any(|&(_, _, held_back, after_readd)| held_back && after_readd);
        let mut after_readd_script = String::new();

        // ──────────────────────────────────────────────────────────
        // Phase 5 – Emit creates / updates in dependency order
        // ──────────────────────────────────────────────────────────
        // A materialized view's indexes are emitted with it (Phase 5) or
        // reconciled in place (Phase 6); production mode routes both through the
        // concurrent-build split, which needs the partition topology. A
        // materialized view is never a partitioned parent, so the maps only ever
        // steer these calls down the plain-relation branch — they are built here
        // for the same reason the table path builds them: `index_create_split`
        // and `index_drop_statement` take the context unconditionally.
        let view_partition_maps = if self.output_for_production {
            Some(self.build_partition_context_maps())
        } else {
            None
        };
        let view_prod_ctx =
            view_partition_maps
                .as_ref()
                .map(
                    |(parents, children, partitioned_indexes)| PartitionContext {
                        partitioned_parents: parents,
                        children,
                        partitioned_indexes,
                    },
                );

        let set_list_to_null = self.migrated_server_at_least(SET_LIST_TO_NULL_MIN_VERSION);
        let no_set_list_to_null = self.migrated_server_known_below(SET_LIST_TO_NULL_MIN_VERSION);
        let no_aggregate_support = self.migrated_server_known_below(AGGREGATE_SUPPORT_MIN_VERSION);
        for &(is_view, orig_idx, held_back, after_readd) in &emit_order {
            // A held-back item is emitted into its own buffer, in the same
            // dependency order, by swapping it in as `script`; so is one
            // waiting for the re-added columns, into another.
            let main_script = if after_readd {
                Some(std::mem::replace(
                    &mut self.script,
                    std::mem::take(&mut after_readd_script),
                ))
            } else {
                held_back.then(|| {
                    std::mem::replace(
                        &mut self.script,
                        std::mem::take(&mut self.deferred_routine_script),
                    )
                })
            };
            if is_view {
                let view = self.to.views[orig_idx].clone();
                self.emit_view_create(&view, view_prod_ctx.as_ref());
            } else {
                // Resolve the matching FROM-side routine by index so
                // `emit_routine_diff` can borrow the `Routine` out of
                // `self.from` / `self.to` directly — no clones of the
                // potentially-large `source_code` string.
                // A routine `drop_views` dropped is created afresh.
                let from_idx = {
                    let routine = &self.to.routines[orig_idx];
                    let key = (
                        routine.schema.clone(),
                        routine.name.clone(),
                        routine.arguments.clone(),
                    );
                    if self.predropped_routines.contains(&key)
                        || self.cascade_victim_routines.contains(&key)
                    {
                        None
                    } else {
                        from_routine_map.get(&key).copied()
                    }
                };
                // Its drop (`CASCADE`) goes right into the script: domain
                // defaults calling it are detached first — unless the
                // `CASCADE` would take what pgc cannot re-create, and the
                // routine is dropped without it.
                let mut cascade = true;
                if let Some(fi) = from_idx
                    && Self::routine_is_recreated(
                        &self.from.routines[fi],
                        &self.to.routines[orig_idx],
                    )
                {
                    match self.routine_phase_cascade_note(&self.from.routines[fi]) {
                        Some(note) => {
                            self.script.push_str(&note);
                            cascade = false;
                        }
                        None => self.emit_domain_default_detach(fi),
                    }
                }
                let routine = &self.to.routines[orig_idx];
                let from_routine = from_idx.map(|i| &self.from.routines[i]);
                Self::emit_routine_diff(
                    &mut self.script,
                    self.use_drop,
                    set_list_to_null,
                    no_set_list_to_null,
                    no_aggregate_support,
                    routine,
                    from_routine,
                    cascade,
                );
            }
            if let Some(main_script) = main_script {
                let buffer = std::mem::replace(&mut self.script, main_script);
                if after_readd {
                    after_readd_script = buffer;
                } else {
                    self.deferred_routine_script = buffer;
                }
            }
        }

        // ──────────────────────────────────────────────────────────
        // Phase 6 – Unchanged views: owner changes and index diffs
        // ──────────────────────────────────────────────────────────
        // A materialized view that is not being recreated keeps its indexes, so
        // added / removed / redefined ones have to be reconciled in place —
        // index changes deliberately stay out of `View::hash` so that adding an
        // index does not force a full rebuild of the view's contents (#235).
        // What a routine dropped with `CASCADE` above took along is gone by
        // now, and Phase 7 creates it again from TO: a view calling one, with
        // its owner and indexes, and an index calling one, with its comment.
        // Neither is changed here.
        let cascaded = if self.use_drop && !no_action_view_indices.is_empty() {
            self.cascade_affected_routines()
        } else {
            HashSet::new()
        };
        for &idx in &no_action_view_indices {
            let to_view = &self.to.views[idx];
            let Some(&fidx) = from_view_map.get(&(to_view.schema.clone(), to_view.name.clone()))
            else {
                continue;
            };
            let from_view = &self.from.views[fidx];
            if Self::definition_references_any(&from_view.definition, &cascaded) {
                continue;
            }
            let owner_changed = from_view.owner != to_view.owner;
            let mut plan = from_view.index_alter_plan(to_view);
            plan.comment_changes
                .retain(|index| !Self::definition_references_any(&index.indexdef, &cascaded));
            let has_index_changes = !plan.create.is_empty()
                || !plan.drop.is_empty()
                || !plan.comment_changes.is_empty();
            if !owner_changed && !has_index_changes {
                continue;
            }

            self.script.push_str(
                format!(
                    "/* View: {}.{}*/\n",
                    block_comment_safe(&to_view.schema),
                    block_comment_safe(&to_view.name)
                )
                .as_str(),
            );
            if owner_changed {
                self.script.push_str(&to_view.get_owner_script());
            }
            if has_index_changes {
                if let Some(ctx) = view_prod_ctx.as_ref() {
                    Self::emit_index_alter_plan_prod(
                        &mut self.script,
                        &mut self.production_post_script,
                        &plan,
                        self.use_drop,
                        ctx,
                    );
                } else {
                    Self::emit_index_alter_plan(&mut self.script, &plan, self.use_drop);
                }
            }
        }

        // ──────────────────────────────────────────────────────────
        // Phase 7 – Recreate dependents silently dropped by CASCADE
        // ──────────────────────────────────────────────────────────
        // When a routine is fully dropped, or its return type / argument
        // list / argument defaults change (each forces DROP+CREATE
        // because PostgreSQL has no in-place ALTER for those), PGC emits
        // `DROP ... CASCADE` and PostgreSQL removes every object whose
        // `pg_depend` entry points at that function: functional indexes,
        // CHECK constraints, generated columns, column DEFAULT
        // expressions, RLS policies and views (regular + materialized).
        // The rest of the comparer never re-emits those dependents
        // because they were unchanged between FROM and TO. See issues
        // #179 (non-view dependents) and #189 (views).
        //
        // A routine waiting for the re-added columns is only created below,
        // after this phase, so what calls one of those comes back after it:
        // Phase 7 hands it back (`recreate_late`) instead of emitting it.
        let late_routines: HashSet<(String, String)> = emit_order
            .iter()
            .filter(|&&(is_view, _, _, after_readd)| after_readd && !is_view)
            .map(|&(_, orig_idx, _, _)| Self::unquoted_routine_name(&self.to.routines[orig_idx]))
            .collect();
        let recreate_late = self.recreate_routine_dependents(&late_routines);

        // The views and routines reading a generated column Phase 7 has just
        // re-added, after it — in the same buffer as its restore, or with the
        // held-back routines when one of them needs an operator or cast —
        // followed by what Phase 7 left for once those routines exist.
        let mut after_readd_deferred = false;
        if !after_readd_script.is_empty() || !recreate_late.is_empty() {
            after_readd_deferred =
                after_readd_needs_later || !self.deferred_routine_script.is_empty();
            let out = if after_readd_deferred {
                &mut self.deferred_routine_script
            } else {
                &mut self.script
            };
            if !after_readd_script.is_empty() {
                out.append_block(
                    "\n/* ---> Views and routines reading generated columns re-added above: Start ---- */",
                );
                out.push_str(&after_readd_script);
                out.append_block(
                    "/* ---> Views and routines reading generated columns re-added above: End ------ */",
                );
            }
            if !recreate_late.is_empty() {
                out.append_block(
                    "\n/* ---> Recreate dependents dropped by CASCADE that call the routines created above: Start ---- */",
                );
                out.push_str(&recreate_late);
                out.append_block(
                    "/* ---> Recreate dependents dropped by CASCADE that call the routines created above: End ------ */",
                );
            }
        }

        // Extended statistics on a materialized view created in the migration
        // wait in `view_statistics_post_script`, which runs right after this
        // section. When such a view is emitted with the held-back routines
        // instead — held back itself, or in the block above when that went
        // there — they follow it to the end of that buffer.
        let deferred_matviews: HashSet<String> = emit_order
            .iter()
            .filter(|&&(is_view, orig_idx, held_back, after_readd)| {
                is_view
                    && self.to.views[orig_idx].is_materialized
                    && if after_readd {
                        after_readd_deferred
                    } else {
                        held_back
                    }
            })
            .map(|&(_, orig_idx, _, _)| {
                let view = &self.to.views[orig_idx];
                Self::normalized_view_key(&view.schema, &view.name)
            })
            .collect();
        if !self.view_statistics_post_script.is_empty()
            && self.to.statistics.iter().any(|stat| {
                deferred_matviews.contains(&Self::normalized_view_key(
                    &stat.table_schema,
                    &stat.table_name,
                ))
            })
        {
            let statistics = std::mem::take(&mut self.view_statistics_post_script);
            self.deferred_routine_script.push_str(&statistics);
        }

        self.script
            .append_block("\n/* ---> Routines & Views: End section --------------- */");
        Ok(())
    }

    /// The `compare_routines_and_views` items, as positions in its item
    /// list, that have to wait until Phase 7 has re-added the generated
    /// columns a routine's `DROP ... CASCADE` took (`cascade_readded`), since
    /// PostgreSQL resolves the columns they read at `CREATE`: a view whose TO
    /// definition reads one (`View::column_relation`; any column of its
    /// table, from a dump without it), a routine whose SQL-standard body
    /// reads one, and a SQL-language routine with a string body naming the
    /// table and the column (checked at `CREATE` with `check_function_bodies`
    /// on; matched by name) — and, transitively, every item depending on one
    /// of those (`depends_on[i]` holds the items `i` must follow).
    ///
    /// A routine that a re-added column's expression calls never waits:
    /// Phase 7 re-adds the column calling it. `items` gives each item's kind
    /// and its index into the TO views or routines. Empty when nothing is
    /// re-added, so the order is otherwise unchanged.
    fn items_reading_cascade_readded_columns(
        &self,
        items: impl Iterator<Item = (bool, usize)>,
        depends_on: &[HashSet<usize>],
    ) -> HashSet<usize> {
        let mut waiting: HashSet<usize> = HashSet::new();
        if self.cascade_readded.is_empty() {
            return waiting;
        }
        let columns: HashSet<String> = self.cascade_readded.iter().map(CascadeReadd::key).collect();
        let tables: HashSet<String> = self
            .cascade_readded
            .iter()
            .map(|c| Self::normalized_view_key(&c.schema, &c.table))
            .collect();
        let to_columns: HashMap<(&str, &str, &str), &TableColumn> = self
            .to
            .tables
            .iter()
            .flat_map(|t| t.columns.iter())
            .map(|c| ((c.schema.as_str(), c.table.as_str(), c.name.as_str()), c))
            .collect();
        // The routines the re-added columns call, as the matcher wants them.
        let called: Vec<&str> = self
            .cascade_readded
            .iter()
            .filter_map(|c| {
                to_columns
                    .get(&(c.schema.as_str(), c.table.as_str(), c.column.as_str()))
                    .and_then(|column| column.generation_expression.as_deref())
            })
            .collect();
        let calls_it = |routine: &Routine| {
            let name = HashSet::from([Self::unquoted_routine_name(routine)]);
            called
                .iter()
                .any(|expr| Self::definition_references_any(expr, &name))
        };
        let names_column = |text: &str| {
            self.cascade_readded.iter().any(|c| {
                Self::text_mentions_relation(text, &c.schema, &c.table)
                    && Self::text_mentions_relation(text, &c.schema, &c.column)
            })
        };
        let mut exempt: HashSet<usize> = HashSet::new();
        for (position, (is_view, idx)) in items.enumerate() {
            let reads = if is_view {
                let view = &self.to.views[idx];
                match &view.column_relation {
                    Some(read) => read
                        .iter()
                        .any(|col| columns.contains(&Self::normalized_view_reference(col))),
                    None => view
                        .table_relation
                        .iter()
                        .any(|rel| tables.contains(&Self::normalized_view_reference(rel))),
                }
            } else {
                let routine = &self.to.routines[idx];
                if calls_it(routine) {
                    exempt.insert(position);
                    continue;
                }
                if routine.is_sql_body {
                    routine
                        .sql_body_dependencies
                        .columns
                        .iter()
                        .any(|col| columns.contains(&Self::normalized_view_reference(col)))
                } else {
                    routine.lang.eq_ignore_ascii_case("sql") && names_column(&routine.source_code)
                }
            };
            if reads {
                waiting.insert(position);
            }
        }
        loop {
            let reached: Vec<usize> = (0..depends_on.len())
                .filter(|i| {
                    !waiting.contains(i)
                        && !exempt.contains(i)
                        && depends_on[*i].iter().any(|j| waiting.contains(j))
                })
                .collect();
            if reached.is_empty() {
                return waiting;
            }
            waiting.extend(reached);
        }
    }

    /// The `compare_routines_and_views` items to hold back until operators and
    /// casts exist, as positions in its item list.
    ///
    /// `routine_of[i]` is the TO routine index of item `i`, `None` for a view;
    /// `depends_on[i]` holds the items `i` must follow. The seeds are routines
    /// with a SQL-standard body whose `pg_depend` rows name an operator
    /// `compare_operators` creates (new, or dropped and re-created) or the
    /// function of a cast `compare_casts` creates (new or changed): PostgreSQL
    /// resolves such a body at CREATE time, even with `check_function_bodies`
    /// off, so creating it in the routine phase fails. Everything that depends
    /// on a seed, transitively, is held back with it, so the held-back items
    /// can be emitted later in the same order without breaking any edge.
    ///
    /// A cast without a function (`WITHOUT FUNCTION`, `WITH INOUT`) leaves no
    /// `pg_depend` row, so a body relying on one is not seen here.
    fn items_needing_later_operators_or_casts(
        &self,
        routine_of: impl Iterator<Item = Option<usize>>,
        depends_on: &[HashSet<usize>],
    ) -> HashSet<usize> {
        let from_operators: HashMap<String, &crate::dump::operator::Operator> = self
            .from
            .operators
            .iter()
            .map(|op| (op.key(), op))
            .collect();
        let created_operators: HashSet<String> = self
            .to
            .operators
            .iter()
            .filter(|op| {
                from_operators
                    .get(&op.key())
                    .is_none_or(|from| from.definition_differs(op))
            })
            .map(|op| op.key())
            .collect();
        let cast_key =
            |c: &crate::dump::cast::Cast| format!("{} AS {}", c.source_type, c.target_type);
        let from_casts: HashMap<String, &crate::dump::cast::Cast> =
            self.from.casts.iter().map(|c| (cast_key(c), c)).collect();
        let created_cast_functions: HashSet<&str> = self
            .to
            .casts
            .iter()
            .filter(|c| {
                from_casts
                    .get(&cast_key(c))
                    .is_none_or(|from| from.hash != c.hash)
            })
            .filter_map(|c| c.function_name.as_deref())
            .collect();
        if created_operators.is_empty() && created_cast_functions.is_empty() {
            return HashSet::new();
        }

        let mut held_back: HashSet<usize> = HashSet::new();
        let mut queue: VecDeque<usize> = VecDeque::new();
        for (i, routine_idx) in routine_of.enumerate() {
            let Some(routine) = routine_idx.map(|r| &self.to.routines[r]) else {
                continue;
            };
            let deps = &routine.sql_body_dependencies;
            if routine.is_sql_body
                && (deps
                    .operators
                    .iter()
                    .any(|key| created_operators.contains(key))
                    || deps
                        .routines
                        .iter()
                        .any(|key| created_cast_functions.contains(key.as_str())))
                && held_back.insert(i)
            {
                queue.push_back(i);
            }
        }
        // Everything depending on a held-back item follows it.
        while let Some(j) = queue.pop_front() {
            for (i, deps) in depends_on.iter().enumerate() {
                if deps.contains(&j) && held_back.insert(i) {
                    queue.push_back(i);
                }
            }
        }
        held_back
    }

    /// Emit what `compare_routines_and_views` held back for after operators
    /// and casts (see `Comparer::items_needing_later_operators_or_casts`).
    fn emit_deferred_routines(&mut self) {
        if self.deferred_routine_script.is_empty() {
            return;
        }
        let deferred = std::mem::take(&mut self.deferred_routine_script);
        self.script.append_block(
            "\n/* ---> Routines & Views using operators or casts created above: Start section --------------- */",
        );
        self.script.push_str(&deferred);
        self.script.append_block(
            "\n/* ---> Routines & Views using operators or casts created above: End section --------------- */",
        );
    }

    /// The FROM routines whose `DROP … CASCADE` this migration runs: the
    /// ones `drop_views` dropped ahead of the relation DDL
    /// (`predropped_routines`), the FROM-only ones, the ones
    /// `Comparer::routine_is_recreated`, and the ones those take along
    /// (`cascade_victim_routines`).
    fn cascade_dropped_routines(&self) -> Vec<&Routine> {
        // Routines absent from TO, plus routines whose change `CREATE OR
        // REPLACE` cannot apply (`routine_requires_drop`: return type, args,
        // routine or aggregate kind) — the same condition that drives the
        // DROP+CREATE branch in `emit_routine_diff`.
        let to_routines_by_full_id: HashMap<(&str, &str, &str), &Routine> = self
            .to
            .routines
            .iter()
            .map(|r| {
                (
                    (r.schema.as_str(), r.name.as_str(), r.arguments.as_str()),
                    r,
                )
            })
            .collect();
        self.from
            .routines
            .iter()
            .filter(|from_routine| {
                // `drop_views` dropped it (CASCADE) ahead of the relation
                // DDL, or the routine phase's CASCADE of a routine it calls
                // takes it (`cascade_victim_routines`).
                let routine_key = (
                    from_routine.schema.clone(),
                    from_routine.name.clone(),
                    from_routine.arguments.clone(),
                );
                let predropped = self.predropped_routines.contains(&routine_key)
                    || self.cascade_victim_routines.contains(&routine_key);
                predropped
                    || match to_routines_by_full_id.get(&(
                        from_routine.schema.as_str(),
                        from_routine.name.as_str(),
                        from_routine.arguments.as_str(),
                    )) {
                        None => true,
                        // PR #187 review (C10/C11): defaults-only changes
                        // are emitted via `CREATE OR REPLACE FUNCTION` —
                        // they do NOT cause CASCADE drops, so Phase 7 must
                        // also exclude them from `affected` or it would
                        // emit pointless dependent recreates for routines
                        // that PostgreSQL never dropped.
                        Some(to_routine) => Self::routine_is_recreated(from_routine, to_routine),
                    }
            })
            .collect()
    }

    /// The routines whose `DROP … CASCADE` this migration runs, as
    /// quote-stripped, lowercased `(schema, name)` — the form
    /// `Comparer::definition_references_any` matches dependents' text
    /// against: the ones `drop_views` dropped ahead of the relation DDL
    /// (`predropped_routines`), the FROM-only ones, the ones
    /// `Comparer::routine_is_recreated`, and the ones those take along
    /// (`cascade_victim_routines`) — restricted to names TO still has a
    /// routine of (PR #187 review, C16).
    ///
    /// Read once `drop_views` has run: `recreate_routine_dependents` restores
    /// what these drops take along, and `compare_statistics` leaves the
    /// statistics objects that restore covers to it.
    fn cascade_affected_routines(&self) -> HashSet<(String, String)> {
        let mut affected: HashSet<(String, String)> = HashSet::new();
        for from_routine in self.cascade_dropped_routines() {
            // The dump query wraps `nspname` / `proname` with
            // `quote_ident` (see `dump/core.rs`), so a routine named
            // `MyFunc` lands here as `"MyFunc"`. Strip the quotes
            // before storing — both matchers below operate on
            // quote-stripped haystacks (`unquoted` half of
            // `prelower_pair`), and the qualified matcher already
            // re-strips its pattern internally, so a quote-free
            // needle works for both forms.
            affected.insert((
                from_routine.schema.to_lowercase().replace('"', ""),
                from_routine.name.to_lowercase().replace('"', ""),
            ));
        }

        if affected.is_empty() {
            return affected;
        }

        // PR #187 review (C16): only recreate dependents when at
        // least one routine sharing the affected `(schema, name)`
        // survives in TO. If the routine name is gone entirely from
        // TO, dependents that text-match it would resolve to nothing
        // — emitting a recreate would generate invalid SQL. The
        // surviving routine may be a different overload (different
        // argument types) but PostgreSQL's name-based binding will
        // pick it up.
        let surviving_routine_names: HashSet<(String, String)> = self
            .to
            .routines
            .iter()
            .map(|r| {
                (
                    r.schema.to_lowercase().replace('"', ""),
                    r.name.to_lowercase().replace('"', ""),
                )
            })
            .collect();
        affected.retain(|key| surviving_routine_names.contains(key));
        affected
    }

    /// The generated columns a routine's `DROP ... CASCADE` takes and
    /// `recreate_routine_dependents` re-adds from TO, exactly as its two
    /// scans find them: a FROM column calling a routine of
    /// `Comparer::cascade_affected_routines` that TO still has calling one,
    /// and a TO column calling a routine the routine phase drops with
    /// `CASCADE` after the table phase created or changed it, or one
    /// `drop_views` dropped ahead of it (see
    /// `Comparer::recreate_table_dependents_created_before_cascade`). A
    /// partition in FROM and TO is left to its parent there, and each
    /// partition below a re-added column's table follows as a copy
    /// ([`CascadeReadd::in_partition`]): the `CASCADE` takes its column too,
    /// and the parent's `ADD COLUMN` brings it back.
    ///
    /// The `CASCADE` takes along everything else that depends on such a
    /// column, which `pg_depend` column edges do not all cover: the views,
    /// rules and SQL-standard routines reading it (`drop_views` drops them
    /// ahead and `compare_routines_and_views` re-creates them once the column
    /// is back), its privileges, the triggers naming it in `UPDATE OF` or
    /// reading it in `WHEN`, the publication rows whose column list or row
    /// filter uses it, and a partition's own objects on its copy — all put
    /// back by `Comparer::restore_cascade_readded_dependents`.
    ///
    /// Reads `predropped_routines` and `cascade_victim_routines`, so it is
    /// only final once `drop_views` has planned them. Empty with `use_drop`
    /// off, which comments every routine drop out.
    fn cascade_readded_columns(&self) -> Vec<CascadeReadd> {
        if !self.use_drop {
            return Vec::new();
        }
        let affected = self.cascade_affected_routines();
        if affected.is_empty() {
            return Vec::new();
        }
        let mut late = self.routine_phase_cascaded_routines(&affected);
        late.extend(
            self.predropped_routine_names()
                .into_iter()
                .filter(|name| affected.contains(name)),
        );
        let generated = |column: &TableColumn| column.is_generated.eq_ignore_ascii_case("ALWAYS");
        let calls = |column: &TableColumn, names: &HashSet<(String, String)>| {
            generated(column)
                && column
                    .generation_expression
                    .as_deref()
                    .is_some_and(|expr| Self::definition_references_any(expr, names))
        };
        let to_by_key: HashMap<(&str, &str), &Table> = self
            .to
            .tables
            .iter()
            .map(|t| ((t.schema.as_str(), t.name.as_str()), t))
            .collect();
        let from_by_key: HashMap<(&str, &str), &Table> = self
            .from
            .tables
            .iter()
            .map(|t| ((t.schema.as_str(), t.name.as_str()), t))
            .collect();

        let mut anchors: Vec<&TableColumn> = Vec::new();
        for from_table in &self.from.tables {
            let Some(to_table) =
                to_by_key.get(&(from_table.schema.as_str(), from_table.name.as_str()))
            else {
                continue;
            };
            if from_table.partition_of.is_some() && to_table.partition_of.is_some() {
                continue;
            }
            for column in &from_table.columns {
                if !calls(column, &affected) {
                    continue;
                }
                if let Some(to_column) = to_table.columns.iter().find(|c| c.name == column.name)
                    && calls(to_column, &affected)
                {
                    anchors.push(to_column);
                }
            }
        }
        if !late.is_empty() {
            for to_table in &self.to.tables {
                let from_table = from_by_key
                    .get(&(to_table.schema.as_str(), to_table.name.as_str()))
                    .copied();
                if to_table.partition_of.is_some()
                    && from_table.is_none_or(|t| t.partition_of.is_some())
                {
                    continue;
                }
                anchors.extend(to_table.columns.iter().filter(|c| calls(c, &late)));
            }
        }

        let mut readded: Vec<CascadeReadd> = Vec::new();
        let mut seen: HashSet<(String, String, String)> = HashSet::new();
        let mut push = |readded: &mut Vec<CascadeReadd>,
                        schema: &str,
                        table: &str,
                        column: &str,
                        in_partition: bool| {
            if seen.insert((schema.to_string(), table.to_string(), column.to_string())) {
                readded.push(CascadeReadd {
                    schema: schema.to_string(),
                    table: table.to_string(),
                    column: column.to_string(),
                    in_partition,
                });
            }
        };
        for column in &anchors {
            push(
                &mut readded,
                &column.schema,
                &column.table,
                &column.name,
                false,
            );
        }
        for column in &anchors {
            for partition in
                Self::partition_descendants(&self.to.tables, &column.schema, &column.table)
            {
                if partition.columns.iter().any(|c| c.name == column.name) {
                    push(
                        &mut readded,
                        &partition.schema,
                        &partition.name,
                        &column.name,
                        true,
                    );
                }
            }
        }
        readded
    }

    /// The `schema.table.column` keys (see [`CascadeReadd::key`]) of the
    /// FROM columns the `CASCADE` of `Comparer::cascade_readded_columns`
    /// takes: those columns, and their copies in the FROM partitions below
    /// their tables. What `drop_views` adds to `RelationChanges::changed_columns`.
    fn cascade_taken_column_keys(&self, readded: &[CascadeReadd]) -> HashSet<String> {
        let mut keys: HashSet<String> = HashSet::new();
        for column in readded {
            keys.insert(column.key());
            if column.in_partition {
                continue;
            }
            for partition in
                Self::partition_descendants(&self.from.tables, &column.schema, &column.table)
            {
                if partition.columns.iter().any(|c| c.name == column.column) {
                    keys.insert(
                        CascadeReadd {
                            schema: partition.schema.clone(),
                            table: partition.name.clone(),
                            column: column.column.clone(),
                            in_partition: true,
                        }
                        .key(),
                    );
                }
            }
        }
        keys
    }

    /// The `partition_of` ancestors of `table` among `tables`, nearest first,
    /// resolved against `index` (a [`Self::relation_index`] of `tables`). The
    /// walk ends at an ancestor `tables` does not hold, and at one it has
    /// met already (or `table` itself), so a malformed dump cannot loop it.
    /// Empty for a table that is no partition.
    fn partition_ancestors<'a>(
        tables: &'a [Table],
        index: &HashMap<String, usize>,
        table: &Table,
    ) -> Vec<&'a Table> {
        let mut ancestors: Vec<&Table> = Vec::new();
        let mut parent = table.partition_of.as_deref();
        while let Some(i) = parent.and_then(|p| Self::lookup_relation(index, p)) {
            let ancestor = &tables[i];
            if std::ptr::eq(ancestor, table) || ancestors.iter().any(|a| std::ptr::eq(*a, ancestor))
            {
                break;
            }
            ancestors.push(ancestor);
            parent = ancestor.partition_of.as_deref();
        }
        ancestors
    }

    /// The partitions of the table `schema.name` among `tables`, at every
    /// depth, parents before their own partitions. Empty for a table that is
    /// not there or has none.
    fn partition_descendants<'a>(tables: &'a [Table], schema: &str, name: &str) -> Vec<&'a Table> {
        let index = Self::relation_index(tables.iter());
        let Some(root) = Self::lookup_relation(&index, &format!("{schema}.{name}")) else {
            return Vec::new();
        };
        let mut found: Vec<&Table> = Vec::new();
        let mut seen: HashSet<usize> = HashSet::from([root]);
        let mut frontier = vec![root];
        while let Some(parent) = frontier.pop() {
            for (i, table) in tables.iter().enumerate() {
                let is_child = table
                    .partition_of
                    .as_deref()
                    .and_then(|reference| Self::lookup_relation(&index, reference))
                    == Some(parent);
                if is_child && seen.insert(i) {
                    found.push(table);
                    frontier.push(i);
                }
            }
        }
        found
    }

    /// Re-emit objects that PostgreSQL silently dropped via
    /// `DROP FUNCTION ... CASCADE` so they are present in the database
    /// after the migration applies. Targets functional indexes, CHECK
    /// constraints, generated columns, column DEFAULT expressions, RLS
    /// policies and views (regular + materialized) that reference any
    /// routine being dropped+recreated or fully dropped. Detection is
    /// text-based against the FROM-side definition; the recreated form
    /// comes from TO so any reshape of the dependent (e.g. a column
    /// type change) is preserved. Issued statements are idempotent
    /// (`drop ... if exists` followed by the CREATE, or `CREATE OR
    /// REPLACE` / `IF NOT EXISTS` for views) so they are safe even
    /// when other diff phases also emitted the object.
    ///
    /// Extended statistics go with the generated columns and materialized
    /// views they sit on, and are restored after them — including those on
    /// the partitions of a re-added column's table and those the TO side
    /// alone has (see [`Comparer::recreate_statistics_on_readded_column`]).
    ///
    /// The `CASCADE` takes more than table-level objects, and those are put
    /// back here too:
    ///
    /// * **domain defaults** — PostgreSQL makes the domain itself depend on
    ///   its default, so the `CASCADE` would drop the domain and every column
    ///   of that type with its data. The default is detached right before the
    ///   drop instead (`Comparer::domain_default_detach`) and set again here;
    /// * **domain CHECKs** the domain alter leaves in place, re-added through
    ///   `domain_constraint_post_script` so they check the data once the
    ///   tables are done (`Comparer::domain_constraint_restores`);
    /// * **triggers** whose function is the routine or whose `WHEN` clause
    ///   calls it, when their definition does not change (a changed one is
    ///   re-created at the end of the migration anyway);
    /// * **extended statistics** whose expressions call the routine
    ///   (`Comparer::statistics_restored_after_cascade`), which
    ///   `compare_statistics` leaves to this phase;
    /// * the partitions' clones of such a trigger get their own enabled state
    ///   and comment back (`Comparer::partition_trigger_clone_states`);
    /// * **indexes on a materialized view** the migration keeps, calling the
    ///   routine while the view does not;
    /// * **foreign-table column defaults** calling the routine;
    /// * the table-level kinds above when only the TO side calls the routine
    ///   — new, or changed to call it: `compare_tables` and
    ///   `compare_foreign_tables` created them before the routine phase's
    ///   `CASCADE`, or left them out because `drop_views` had dropped the
    ///   routine (`Comparer::recreate_table_dependents_created_before_cascade`);
    /// * what went with a re-added generated column beyond its own `pg_depend`
    ///   edges — the triggers naming it, the publication rows using it, a
    ///   partition's own objects on its copy, and its privileges
    ///   (`Comparer::restore_cascade_readded_dependents`). The views, rules
    ///   and SQL-standard routines reading it are dropped by `drop_views` and
    ///   re-created by `compare_routines_and_views` right after this phase.
    ///
    /// Views, rules, aggregates and other routines that go with the routine
    /// are handled where they are planned: `drop_views` drops and
    /// `compare_routines_and_views` re-creates the views calling it, the
    /// SQL-standard routines using it, the aggregates built on it and the
    /// routines whose argument defaults call it (those the routine phase's
    /// `CASCADE` takes are `cascade_victim_routines`), and `compare_rules`
    /// re-creates the rules.
    ///
    /// The views calling a routine the routine phase itself drops are the
    /// exception: they are re-created here, from TO, with their owner and
    /// comments (and a materialized view's indexes and statistics), and
    /// recorded in `views_recreated_by_cascade`, from which `compare_grants`
    /// plans their privileges and `compare_rules` their rules. Only views
    /// nothing else reads come here: one that another view or a SQL-standard
    /// routine reads is dropped by `drop_views` ahead of everything, with its
    /// readers (`Comparer::views_cascaded_with_readers`), and all of them are
    /// re-created in dependency order — unless the dump cannot say which
    /// routines the view calls, or something its drop cannot take along
    /// depends on it or on a reader, and the view is then left here.
    ///
    /// `late_routines` are the routines `compare_routines_and_views` creates
    /// only after this phase, because they read a generated column it
    /// re-adds (see `Comparer::items_reading_cascade_readded_columns`). What
    /// calls one of them — a column default, a CHECK, a policy, an index, a
    /// view — cannot come back before it, and one that routine's own `DROP
    /// ... CASCADE` would take again: those restores are returned instead of
    /// emitted, for the caller to emit once the routines exist (see
    /// [`LateRecreate`]). Empty when `late_routines` is.
    fn recreate_routine_dependents(&mut self, late_routines: &HashSet<(String, String)>) -> String {
        let affected = self.cascade_affected_routines();
        if affected.is_empty() && self.detached_domain_defaults.is_empty() {
            return String::new();
        }
        // The generated columns re-added below start without privileges, like
        // any column the table phase drops and re-adds.
        for readd in &self.cascade_readded {
            self.readded_columns.insert((
                readd.schema.clone(),
                readd.table.clone(),
                readd.column.clone(),
            ));
        }

        let to_table_by_id: HashMap<(&str, &str), &Table> = self
            .to
            .tables
            .iter()
            .map(|t| ((t.schema.as_str(), t.name.as_str()), t))
            .collect();

        let mut emitted_keys: HashSet<String> = HashSet::new();
        let mut recreate: String = String::new();
        // What calls a routine of `late_routines`, for the caller to emit once
        // it has created them.
        let mut recreate_late: String = String::new();
        // Post-commit statements produced by the production-mode index split
        // (per-partition CONCURRENTLY builds and ATTACH PARTITION); flushed into
        // production_post_script at the end. Empty in default mode.
        let mut recreate_post_commit: String = String::new();
        let mut buffers = RecreateBuffers {
            out: &mut recreate,
            post_commit: &mut recreate_post_commit,
            late: (!late_routines.is_empty()).then_some(LateRecreate {
                out: &mut recreate_late,
                routines: late_routines,
            }),
        };

        // Domain defaults first, so a domain gets its default back as soon as
        // the routine it calls exists again.
        for (schema, typname) in &self.detached_domain_defaults {
            let Some(set_default) = self
                .to
                .types
                .iter()
                .find(|t| {
                    t.schema == *schema && t.typname == *typname && (t.typtype as u8 as char) == 'd'
                })
                .and_then(PgType::get_set_default_script)
            else {
                continue;
            };
            Self::emit_dependent_recreate(
                buffers.out_for(&set_default),
                self.use_drop,
                &set_default,
            );
        }
        let partition_maps = if self.output_for_production {
            Some(self.build_partition_context_maps())
        } else {
            None
        };
        let prod_ctx = partition_maps
            .as_ref()
            .map(
                |(parents, children, partitioned_indexes)| PartitionContext {
                    partitioned_parents: parents,
                    children,
                    partitioned_indexes,
                },
            );

        for from_table in &self.from.tables {
            // If the table is gone in TO it is being dropped — there is
            // no post-migration object to restore the dependents onto.
            let Some(to_table) =
                to_table_by_id.get(&(from_table.schema.as_str(), from_table.name.as_str()))
            else {
                continue;
            };

            // Mirror Table::diff's partition-child safeguards. When both
            // FROM and TO are partition children, structural changes to
            // inherited columns/constraints/indexes belong to the
            // parent's recreate path: PostgreSQL forbids `ALTER TABLE
            // child DROP COLUMN` for inherited columns, refuses direct
            // ALTER on inherited constraints (`coninhcount > 0`), and
            // partition-inherited indexes are managed by attaching to
            // the parent's index. Re-emitting them on the child would
            // produce a script PostgreSQL rejects.
            let is_target_partition =
                from_table.partition_of.is_some() && to_table.partition_of.is_some();

            // Each scan below requires BOTH FROM and TO to reference an
            // affected routine before re-emitting:
            //
            // * FROM-reference is necessary to know the dependent
            //   *had* a `pg_depend` link to the function before the
            //   migration began — otherwise CASCADE would never have
            //   touched it in the first place.
            //
            // * TO-reference is necessary because `compare_tables()`
            //   runs *before* `compare_routines_and_views()` and may
            //   already have rewritten the dependent to its TO-side
            //   form, breaking the dependency. Once the link is gone,
            //   `DROP FUNCTION ... CASCADE` leaves the object alone —
            //   re-emitting a recreate would be redundant for
            //   constraints/indexes/policies and *destructive* for
            //   generated columns: `DROP COLUMN IF EXISTS` cascades to
            //   indexes, constraints, and FKs that reference the
            //   column, none of which Phase 7 restores.
            //
            // CHECK and EXCLUDE constraints can reference functions;
            // FKs and PKs cannot — skip them to avoid spurious matches
            // in their textual `definition` (e.g. a referenced table
            // named similarly to a routine).
            for constraint in &from_table.constraints {
                let is_check = constraint.constraint_type.eq_ignore_ascii_case("check")
                    || constraint.constraint_type.eq_ignore_ascii_case("exclude");
                if !is_check {
                    continue;
                }
                let Some(def) = &constraint.definition else {
                    continue;
                };
                if !Self::definition_references_any(def, &affected) {
                    continue;
                }
                let Some(to_constraint) = to_table
                    .constraints
                    .iter()
                    .find(|tc| tc.name == constraint.name)
                else {
                    continue;
                };
                // On partition children, inherited constraints
                // (coninhcount > 0) propagate from the parent and must
                // not be re-emitted locally.
                if is_target_partition && to_constraint.coninhcount > 0 {
                    continue;
                }
                // TO-side gate: skip when the TO definition no longer
                // references the affected routine — `compare_tables`
                // has rewritten the constraint, the new form has no
                // pg_depend link to the function, CASCADE will leave
                // it intact, and re-emitting would be wasted work.
                let to_def_refs = to_constraint
                    .definition
                    .as_deref()
                    .map(|d| Self::definition_references_any(d, &affected))
                    .unwrap_or(false);
                if !to_def_refs {
                    continue;
                }
                let key = format!(
                    "constraint:{}.{}.{}",
                    to_constraint.schema, to_constraint.table_name, to_constraint.name
                );
                if !emitted_keys.insert(key) {
                    continue;
                }
                let block = constraint_recreate_block(to_constraint);
                Self::emit_dependent_recreate(buffers.out_for(&block), self.use_drop, &block);
            }

            for index in &from_table.indexes {
                // Partition-inherited indexes are recreated via the
                // parent — a redundant CREATE INDEX on the child would
                // collide with the inherited index name.
                if index.is_partition_index {
                    continue;
                }
                if !Self::definition_references_any(&index.indexdef, &affected) {
                    continue;
                }
                let Some(to_index) = to_table.indexes.iter().find(|ti| ti.name == index.name)
                else {
                    continue;
                };
                if to_index.is_partition_index {
                    continue;
                }
                // TO-side gate: see the constraint scan above.
                if !Self::definition_references_any(&to_index.indexdef, &affected) {
                    continue;
                }
                let key = format!("index:{}.{}", to_index.schema, to_index.name);
                if !emitted_keys.insert(key) {
                    continue;
                }
                let (out, post_commit) = buffers.targets(&to_index.indexdef);
                Self::emit_index_dependent_recreate(
                    out,
                    post_commit,
                    self.use_drop,
                    to_index,
                    prod_ctx.as_ref(),
                );
            }

            // Structural column changes (ADD / DROP / type) are forbidden
            // on partition children — the parent owns the column shape.
            // Skip the whole column scan in that case; the parent's
            // recreate (handled when we iterate the parent table) will
            // re-add the dependent.
            if !is_target_partition {
                for column in &from_table.columns {
                    let default_referenced = column
                        .column_default
                        .as_deref()
                        .map(|d| Self::definition_references_any(d, &affected))
                        .unwrap_or(false);
                    let generation_referenced = column.is_generated.eq_ignore_ascii_case("ALWAYS")
                        && column
                            .generation_expression
                            .as_deref()
                            .map(|e| Self::definition_references_any(e, &affected))
                            .unwrap_or(false);

                    if !default_referenced && !generation_referenced {
                        continue;
                    }
                    let Some(to_column) = to_table.columns.iter().find(|tc| tc.name == column.name)
                    else {
                        continue;
                    };

                    if generation_referenced {
                        // TO-side gate: only `DROP COLUMN IF EXISTS` +
                        // re-add the column when TO is *still* a
                        // generated column whose expression references
                        // the affected routine. If TO has dropped the
                        // generation expression or rewritten it to no
                        // longer reference the function, the column
                        // survives CASCADE intact and dropping it here
                        // would cascade-destroy any indexes / FKs /
                        // constraints attached to it — none of which
                        // Phase 7 knows how to restore.
                        let to_generation_refs =
                            to_column.is_generated.eq_ignore_ascii_case("ALWAYS")
                                && to_column
                                    .generation_expression
                                    .as_deref()
                                    .map(|e| Self::definition_references_any(e, &affected))
                                    .unwrap_or(false);
                        if !to_generation_refs {
                            continue;
                        }
                        let key = format!(
                            "column-generated:{}.{}.{}",
                            to_column.schema, to_column.table, to_column.name
                        );
                        if emitted_keys.insert(key) {
                            let block = column_recreate_block(to_column);
                            Self::emit_dependent_recreate(
                                buffers.out_for(&block),
                                self.use_drop,
                                &block,
                            );
                            // Issue #188: the `ADD COLUMN IF NOT EXISTS`
                            // restores the generated column itself, but
                            // PostgreSQL had already CASCADE-dropped
                            // every index / FK / CHECK / EXCLUDE
                            // constraint / RLS policy attached to it
                            // *because they referenced the column, not
                            // the routine*. Walk the column-dependent
                            // graph harvested from `pg_depend` at dump
                            // time and re-emit each surviving TO-side
                            // dependent. `emitted_keys` is shared so
                            // anything Phase 7 already restored via its
                            // routine-text scan is not double-emitted.
                            // The TO dump's edges count too: what only TO
                            // has on the column — new, or changed to use
                            // it — `compare_tables` created before the
                            // `CASCADE`, which took it along.
                            self.recreate_column_dependents_from(
                                self.from
                                    .column_dependents
                                    .iter()
                                    .chain(self.to.column_dependents.iter()),
                                (&to_column.schema, &to_column.table, &to_column.name),
                                &mut emitted_keys,
                                &mut buffers,
                                prod_ctx.as_ref(),
                            );
                            self.recreate_statistics_on_readded_column(
                                &to_column.schema,
                                &to_column.table,
                                &to_column.name,
                                &mut emitted_keys,
                                &mut buffers,
                            );
                        }
                    } else if default_referenced && let Some(default) = &to_column.column_default {
                        // TO-side gate: only re-set the DEFAULT when
                        // TO's default itself still references the
                        // affected routine. If TO has switched to a
                        // function-free default (or no default), the
                        // CASCADE leaves the column alone and
                        // `compare_tables` already emitted the right
                        // ALTER COLUMN SET DEFAULT.
                        if !Self::definition_references_any(default, &affected) {
                            continue;
                        }
                        let key = format!(
                            "column-default:{}.{}.{}",
                            to_column.schema, to_column.table, to_column.name
                        );
                        if emitted_keys.insert(key) {
                            let stmt = format!(
                                "alter table {}.{} alter column {} set default {};",
                                to_column.schema, to_column.table, to_column.name, default
                            )
                            .with_empty_lines();
                            Self::emit_dependent_recreate(
                                buffers.out_for(&stmt),
                                self.use_drop,
                                &stmt,
                            );
                        }
                    }
                }
            }

            // RLS policies are not inherited via PARTITION OF — each
            // partition has its own — so no partition-child skip applies.
            for policy in &from_table.policies {
                let referenced = policy
                    .using_clause
                    .as_deref()
                    .map(|c| Self::definition_references_any(c, &affected))
                    .unwrap_or(false)
                    || policy
                        .check_clause
                        .as_deref()
                        .map(|c| Self::definition_references_any(c, &affected))
                        .unwrap_or(false);
                if !referenced {
                    continue;
                }
                let Some(to_policy) = to_table.policies.iter().find(|tp| tp.name == policy.name)
                else {
                    continue;
                };
                // TO-side gate: same rationale as constraints/indexes.
                let to_policy_refs = to_policy
                    .using_clause
                    .as_deref()
                    .map(|c| Self::definition_references_any(c, &affected))
                    .unwrap_or(false)
                    || to_policy
                        .check_clause
                        .as_deref()
                        .map(|c| Self::definition_references_any(c, &affected))
                        .unwrap_or(false);
                if !to_policy_refs {
                    continue;
                }
                let key = format!(
                    "policy:{}.{}.{}",
                    to_policy.schema, to_policy.table, to_policy.name
                );
                if !emitted_keys.insert(key) {
                    continue;
                }
                let block = policy_recreate_block(to_policy);
                Self::emit_dependent_recreate(buffers.out_for(&block), self.use_drop, &block);
            }

            // Triggers: the routine is the trigger's function, or its `WHEN`
            // clause calls it — `pg_get_triggerdef` spells both as calls.
            // Only an unchanged trigger is put back here: a changed one is
            // dropped (`if exists`) and re-created from TO by the trigger
            // diff, which runs after every routine, and so are the triggers
            // of a partition re-created with its parent
            // (`tables_with_late_triggers`).
            if !self
                .tables_with_late_triggers
                .contains(&Self::table_key(&from_table.schema, &from_table.name))
            {
                for trigger in &from_table.triggers {
                    if !Self::definition_references_any(&trigger.definition, &affected) {
                        continue;
                    }
                    let Some(to_trigger) =
                        to_table.triggers.iter().find(|tt| tt.name == trigger.name)
                    else {
                        continue;
                    };
                    if to_trigger.definition != trigger.definition {
                        continue;
                    }
                    let key = format!(
                        "trigger:{}.{}.{}",
                        to_table.schema, to_table.name, to_trigger.name
                    );
                    if !emitted_keys.insert(key) {
                        continue;
                    }
                    let block = to_trigger.get_recreate_script(&to_table.schema, &to_table.name);
                    let out = buffers.out_for(&block);
                    Self::emit_dependent_recreate(out, self.use_drop, &block);
                    // Its partitions' clones come back in its state, with it.
                    Self::emit_dependent_recreate(
                        out,
                        self.use_drop,
                        &self.partition_trigger_clone_states(to_table, to_trigger),
                    );
                }
            }
        }

        // Foreign-table column defaults: the same gates as a table column's.
        let to_foreign_by_id: HashMap<(&str, &str), &ForeignTable> = self
            .to
            .foreign_tables
            .iter()
            .map(|t| ((t.schema.as_str(), t.name.as_str()), t))
            .collect();
        for from_foreign in &self.from.foreign_tables {
            let Some(to_foreign) =
                to_foreign_by_id.get(&(from_foreign.schema.as_str(), from_foreign.name.as_str()))
            else {
                continue;
            };
            for column in &from_foreign.columns {
                if !column
                    .column_default
                    .as_deref()
                    .is_some_and(|d| Self::definition_references_any(d, &affected))
                {
                    continue;
                }
                let Some(to_column) = to_foreign.columns.iter().find(|c| c.name == column.name)
                else {
                    continue;
                };
                Self::emit_foreign_default_restore(
                    &mut buffers,
                    self.use_drop,
                    &mut emitted_keys,
                    to_foreign,
                    to_column,
                    &affected,
                );
            }
        }

        // What the table phase created calling a routine dropped with
        // `CASCADE` after it — new objects, and ones changed to call it — and
        // what it left for this phase because it calls a routine `drop_views`
        // dropped before it (`Comparer::enter_table_phase_view`).
        let mut late = self.routine_phase_cascaded_routines(&affected);
        late.extend(
            self.predropped_routine_names()
                .into_iter()
                .filter(|name| affected.contains(name)),
        );
        if !late.is_empty() {
            self.recreate_table_dependents_created_before_cascade(
                &late,
                &mut emitted_keys,
                &mut buffers,
                prod_ctx.as_ref(),
            );
        }

        // What else the `CASCADE` took with the generated columns re-added
        // above.
        self.restore_cascade_readded_dependents(&mut emitted_keys, &mut buffers, prod_ctx.as_ref());

        // Views (regular and materialized) whose definition references an
        // affected routine are also CASCADE-dropped by PostgreSQL — issue
        // #189 / PR #187 review. The hash check in
        // `compare_routines_and_views` skips byte-identical views, so this
        // is the only place where an unchanged view referencing a
        // signature-changed function gets re-emitted.
        let to_view_by_id: HashMap<(&str, &str), &View> = self
            .to
            .views
            .iter()
            .map(|v| ((v.schema.as_str(), v.name.as_str()), v))
            .collect();

        for from_view in &self.from.views {
            if !Self::definition_references_any(&from_view.definition, &affected) {
                continue;
            }
            let Some(to_view) =
                to_view_by_id.get(&(from_view.schema.as_str(), from_view.name.as_str()))
            else {
                continue;
            };
            // TO-side gate: if the TO definition no longer references the
            // affected routine, `compare_tables`/Phase 5 has already
            // rewritten the view or its function dependency is gone — the
            // CASCADE leaves it intact (or another phase re-emits it).
            if !Self::definition_references_any(&to_view.definition, &affected) {
                continue;
            }
            // Phase 5 already emits the view when `dropped_views`
            // contains it (drop-and-recreate path triggered by a
            // referenced table changing) — don't duplicate that work.
            let normalized_view = Self::normalized_view_key(&to_view.schema, &to_view.name);
            if self.dropped_views.contains_key(&normalized_view) {
                continue;
            }
            let key = format!("view:{}.{}", to_view.schema, to_view.name);
            if !emitted_keys.insert(key) {
                continue;
            }
            let block = view_recreate_block(to_view);
            Self::emit_dependent_recreate(buffers.out_for(&block), self.use_drop, &block);
            // Extended statistics on a materialized view went with it.
            if to_view.is_materialized {
                let view_key = Self::normalized_view_key(&to_view.schema, &to_view.name);
                for stat in &self.to.statistics {
                    if Self::normalized_view_key(&stat.table_schema, &stat.table_name) == view_key {
                        self.emit_statistic_restore(stat, &mut emitted_keys, &mut buffers);
                    }
                }
            }
            self.views_recreated_by_cascade.insert(normalized_view);
        }

        // Indexes on a materialized view the migration keeps: the view does
        // not call the routine, but an index expression or predicate does.
        // One the migration re-creates — `drop_views` dropped it, or it was
        // just re-created above — comes back with all its indexes, and one
        // that only TO has is created in the routine phase, after the
        // routines its indexes call.
        for from_view in self.from.views.iter().filter(|v| v.is_materialized) {
            let normalized_view = Self::normalized_view_key(&from_view.schema, &from_view.name);
            if self.dropped_views.contains_key(&normalized_view)
                || self.views_recreated_by_cascade.contains(&normalized_view)
            {
                continue;
            }
            let Some(to_view) =
                to_view_by_id.get(&(from_view.schema.as_str(), from_view.name.as_str()))
            else {
                continue;
            };
            for index in &from_view.indexes {
                if !Self::definition_references_any(&index.indexdef, &affected) {
                    continue;
                }
                let Some(to_index) = to_view.indexes.iter().find(|ti| ti.name == index.name) else {
                    continue;
                };
                // TO-side gate: see the table index scan above.
                if !Self::definition_references_any(&to_index.indexdef, &affected) {
                    continue;
                }
                let key = format!("index:{}.{}", to_index.schema, to_index.name);
                if !emitted_keys.insert(key) {
                    continue;
                }
                let (out, post_commit) = buffers.targets(&to_index.indexdef);
                Self::emit_index_dependent_recreate(
                    out,
                    post_commit,
                    self.use_drop,
                    to_index,
                    prod_ctx.as_ref(),
                );
            }
        }

        // Extended statistics whose expressions call an affected routine
        // (the ones on a restored materialized view went with it above).
        for stat_key in self.statistics_restored_after_cascade(&affected) {
            if let Some(stat) = self
                .to
                .statistics
                .iter()
                .find(|s| s.schema == stat_key.0 && s.name == stat_key.1)
            {
                self.emit_statistic_restore(stat, &mut emitted_keys, &mut buffers);
            }
        }

        // Domain CHECKs: with the other domain constraint statements, which
        // run once the tables are done, but ahead of them — a `validate
        // constraint` there may name a CHECK restored here.
        let domain_checks = self.domain_constraint_restores(&affected);
        if !domain_checks.is_empty() {
            let mut block = String::new();
            block.append_block(
                "\n/* ---> Recreate domain constraints dropped by CASCADE: Start ---- */",
            );
            block.push_str(&domain_checks);
            block.append_block(
                "/* ---> Recreate domain constraints dropped by CASCADE: End ------ */",
            );
            self.domain_constraint_post_script.insert_str(0, &block);
        }

        if !recreate.is_empty() {
            // After the held-back routines when there are any: a dependent may
            // call one of them, and none of Phase 5 depends on a dependent.
            let out = if self.deferred_routine_script.is_empty() {
                &mut self.script
            } else {
                &mut self.deferred_routine_script
            };
            out.append_block("\n/* ---> Recreate dependents dropped by CASCADE: Start ---- */");
            out.push_str(&recreate);
            out.append_block("/* ---> Recreate dependents dropped by CASCADE: End ------ */");
        }
        self.production_post_script.push_str(&recreate_post_commit);
        recreate_late
    }

    /// Put back, once `recreate_routine_dependents` has re-added the
    /// generated columns a routine's `DROP ... CASCADE` took
    /// (`cascade_readded`), what that `CASCADE` took with them beyond the
    /// column-dependent edges restored with each column:
    ///
    /// * a partition's own indexes, constraints, policies and statistics on
    ///   its copy of a column its parent re-adds, from the `pg_depend` edges
    ///   of either dump (the parent's `ADD COLUMN` re-creates the copy, not
    ///   them);
    /// * the triggers naming such a column in `UPDATE OF` or reading it in
    ///   `WHEN` ([`ColumnDependentKind::Trigger`]), with their partitions'
    ///   clones — only an unchanged one, as for the triggers calling the
    ///   routine: the trigger diff after every routine drops (`if exists`)
    ///   and re-creates a changed one, and the triggers of a table created
    ///   late (`tables_with_late_triggers`) and those `compare_tables` drops
    ///   ahead of a column drop + add (`triggers_dropped_ahead`) come after
    ///   this anyway;
    /// * the publication rows whose column list or row filter uses such a
    ///   column: each TO publication listing one is set to its TO list
    ///   again ([`Publication::get_restore_script`](crate::dump::publication::Publication::get_restore_script)),
    ///   which a replayed migration can run twice.
    ///
    /// Their privileges are planned from nothing by `compare_grants` (see
    /// `readded_columns`), and the views, rules and SQL-standard routines
    /// reading them are re-created after this (see
    /// `Comparer::cascade_readded_columns`).
    fn restore_cascade_readded_dependents(
        &self,
        emitted_keys: &mut HashSet<String>,
        buffers: &mut RecreateBuffers<'_>,
        prod_ctx: Option<&PartitionContext>,
    ) {
        if self.cascade_readded.is_empty() {
            return;
        }
        for readd in self.cascade_readded.iter().filter(|c| c.in_partition) {
            self.recreate_column_dependents_from(
                self.from
                    .column_dependents
                    .iter()
                    .chain(self.to.column_dependents.iter()),
                (&readd.schema, &readd.table, &readd.column),
                emitted_keys,
                buffers,
                prod_ctx,
            );
        }

        let normalise = |s: &str| s.to_lowercase().replace('"', "");
        let anchors: HashSet<(String, String, String)> = self
            .cascade_readded
            .iter()
            .map(|c| {
                (
                    normalise(&c.schema),
                    normalise(&c.table),
                    normalise(&c.column),
                )
            })
            .collect();
        for dep in &self.from.column_dependents {
            if dep.kind != ColumnDependentKind::Trigger
                || !anchors.contains(&(
                    normalise(&dep.schema),
                    normalise(&dep.table),
                    normalise(&dep.column),
                ))
            {
                continue;
            }
            let find = |tables: &'_ [Table]| {
                tables
                    .iter()
                    .position(|t| t.schema == dep.dep_schema && t.name == dep.dep_table)
            };
            let (Some(from_idx), Some(to_idx)) = (find(&self.from.tables), find(&self.to.tables))
            else {
                continue;
            };
            let (from_owner, to_owner) = (&self.from.tables[from_idx], &self.to.tables[to_idx]);
            let owner_key = Self::table_key(&to_owner.schema, &to_owner.name);
            // Created later anyway: with a table created late, or after
            // `compare_tables` dropped it ahead of a column drop + add.
            if self.tables_with_late_triggers.contains(&owner_key)
                || self
                    .triggers_dropped_ahead
                    .contains(&format!("{owner_key}.{}", dep.dep_name))
            {
                continue;
            }
            let (Some(from_trigger), Some(to_trigger)) = (
                from_owner.triggers.iter().find(|t| t.name == dep.dep_name),
                to_owner.triggers.iter().find(|t| t.name == dep.dep_name),
            ) else {
                continue;
            };
            if from_trigger.definition != to_trigger.definition {
                continue;
            }
            let key = format!(
                "trigger:{}.{}.{}",
                to_owner.schema, to_owner.name, to_trigger.name
            );
            if !emitted_keys.insert(key) {
                continue;
            }
            let block = to_trigger.get_recreate_script(&to_owner.schema, &to_owner.name);
            let out = buffers.out_for(&block);
            Self::emit_dependent_recreate(out, self.use_drop, &block);
            Self::emit_dependent_recreate(
                out,
                self.use_drop,
                &self.partition_trigger_clone_states(to_owner, to_trigger),
            );
        }

        // Publication members spell tables and columns as the dump does.
        let mut columns_by_member: HashMap<String, HashSet<String>> = HashMap::new();
        for readd in &self.cascade_readded {
            columns_by_member
                .entry(format!("{}.{}", readd.schema, readd.table))
                .or_default()
                .insert(readd.column.clone());
        }
        let ctx = self.publication_alter_context();
        for publication in &self.to.publications {
            let released: HashSet<String> = publication
                .tables
                .iter()
                .filter(|member| {
                    columns_by_member.get(*member).is_some_and(|columns| {
                        publication
                            .table_specs
                            .get(*member)
                            .is_some_and(|spec| spec.uses_any_column(columns))
                    })
                })
                .cloned()
                .collect();
            if released.is_empty()
                || !emitted_keys.insert(format!("publication:{}", publication.name))
            {
                continue;
            }
            let block = publication.get_restore_script(&released, &HashSet::new(), &ctx);
            Self::emit_dependent_recreate(buffers.out_for(&block), self.use_drop, &block);
        }
    }

    /// The part of `affected` (see `Comparer::cascade_affected_routines`)
    /// that the routine phase itself drops with `CASCADE` — FROM-only
    /// routines and ones `Comparer::routine_is_recreated`, unless `drop_views`
    /// already dropped them — as quote-stripped, lowercased `(schema, name)`.
    /// Unlike `drop_views`, which runs before them, that `CASCADE` comes after
    /// `compare_tables` and `compare_types`, and so takes what they created.
    fn routine_phase_cascaded_routines(
        &self,
        affected: &HashSet<(String, String)>,
    ) -> HashSet<(String, String)> {
        let to_routines: HashMap<(&str, &str, &str), &Routine> = self
            .to
            .routines
            .iter()
            .map(|r| {
                (
                    (r.schema.as_str(), r.name.as_str(), r.arguments.as_str()),
                    r,
                )
            })
            .collect();
        self.from
            .routines
            .iter()
            .filter(|r| {
                let key = (r.schema.clone(), r.name.clone(), r.arguments.clone());
                !self.predropped_routines.contains(&key)
                    && (self.cascade_victim_routines.contains(&key)
                        || to_routines
                            .get(&(r.schema.as_str(), r.name.as_str(), r.arguments.as_str()))
                            .is_none_or(|to| Self::routine_is_recreated(r, to)))
            })
            .map(Self::unquoted_routine_name)
            .filter(|name| affected.contains(name))
            .collect()
    }

    /// Put back the table-level dependents — CHECK and EXCLUDE constraints,
    /// indexes, column defaults, generated columns, policies, and foreign-table
    /// column defaults — that call a routine in `routine_phase` on the TO side
    /// but that the FROM-side scan of `recreate_routine_dependents` does not
    /// cover: new ones, on new tables too, and ones changed to call it.
    ///
    /// `routine_phase` holds two kinds of routine. One the routine phase drops
    /// with `CASCADE` (`Comparer::routine_phase_cascaded_routines`):
    /// `compare_tables` and `compare_foreign_tables` create these objects
    /// before that drop, which takes them again. And one `drop_views` dropped
    /// ahead of the tables: the table phase leaves them out altogether
    /// (`Comparer::enter_table_phase_view`), for this pass to create once the
    /// routine is back. `emitted_keys` is the scan's, so nothing is put back
    /// twice, and the partition safeguards are the scan's too.
    ///
    /// A re-added generated column brings back what depends on the column
    /// itself, from both dumps' `pg_depend` column edges: on the TO side, a
    /// new index or constraint on a new column went with it.
    fn recreate_table_dependents_created_before_cascade(
        &self,
        routine_phase: &HashSet<(String, String)>,
        emitted_keys: &mut HashSet<String>,
        buffers: &mut RecreateBuffers<'_>,
        prod_ctx: Option<&PartitionContext>,
    ) {
        let from_tables: HashMap<(&str, &str), &Table> = self
            .from
            .tables
            .iter()
            .map(|t| ((t.schema.as_str(), t.name.as_str()), t))
            .collect();
        let calls_it = |text: Option<&str>| {
            text.is_some_and(|t| Self::definition_references_any(t, routine_phase))
        };
        for to_table in &self.to.tables {
            let from_table = from_tables
                .get(&(to_table.schema.as_str(), to_table.name.as_str()))
                .copied();
            // As in the FROM-side scan: a partition's inherited structure
            // belongs to its parent.
            let is_partition = to_table.partition_of.is_some();

            for constraint in &to_table.constraints {
                let is_check = constraint.constraint_type.eq_ignore_ascii_case("check")
                    || constraint.constraint_type.eq_ignore_ascii_case("exclude");
                if !is_check
                    || (is_partition && constraint.coninhcount > 0)
                    || !calls_it(constraint.definition.as_deref())
                {
                    continue;
                }
                let key = format!(
                    "constraint:{}.{}.{}",
                    constraint.schema, constraint.table_name, constraint.name
                );
                if !emitted_keys.insert(key) {
                    continue;
                }
                let block = constraint_recreate_block(constraint);
                Self::emit_dependent_recreate(buffers.out_for(&block), self.use_drop, &block);
            }

            for index in &to_table.indexes {
                if index.is_partition_index || !calls_it(Some(&index.indexdef)) {
                    continue;
                }
                let key = format!("index:{}.{}", index.schema, index.name);
                if !emitted_keys.insert(key) {
                    continue;
                }
                let (out, post_commit) = buffers.targets(&index.indexdef);
                Self::emit_index_dependent_recreate(
                    out,
                    post_commit,
                    self.use_drop,
                    index,
                    prod_ctx,
                );
            }

            if !(is_partition && from_table.is_none_or(|t| t.partition_of.is_some())) {
                for column in &to_table.columns {
                    let generated = column.is_generated.eq_ignore_ascii_case("ALWAYS");
                    if generated && calls_it(column.generation_expression.as_deref()) {
                        let key = format!(
                            "column-generated:{}.{}.{}",
                            column.schema, column.table, column.name
                        );
                        if !emitted_keys.insert(key) {
                            continue;
                        }
                        let block = column_recreate_block(column);
                        Self::emit_dependent_recreate(
                            buffers.out_for(&block),
                            self.use_drop,
                            &block,
                        );
                        self.recreate_column_dependents_from(
                            self.from
                                .column_dependents
                                .iter()
                                .chain(self.to.column_dependents.iter()),
                            (&column.schema, &column.table, &column.name),
                            emitted_keys,
                            buffers,
                            prod_ctx,
                        );
                        self.recreate_statistics_on_readded_column(
                            &column.schema,
                            &column.table,
                            &column.name,
                            emitted_keys,
                            buffers,
                        );
                    } else if !generated
                        && let Some(default) = column.column_default.as_deref()
                        && calls_it(Some(default))
                    {
                        let key = format!(
                            "column-default:{}.{}.{}",
                            column.schema, column.table, column.name
                        );
                        if !emitted_keys.insert(key) {
                            continue;
                        }
                        let stmt = format!(
                            "alter table {}.{} alter column {} set default {};",
                            column.schema, column.table, column.name, default
                        )
                        .with_empty_lines();
                        Self::emit_dependent_recreate(buffers.out_for(&stmt), self.use_drop, &stmt);
                    }
                }
            }

            for policy in &to_table.policies {
                if !calls_it(policy.using_clause.as_deref())
                    && !calls_it(policy.check_clause.as_deref())
                {
                    continue;
                }
                let key = format!("policy:{}.{}.{}", policy.schema, policy.table, policy.name);
                if !emitted_keys.insert(key) {
                    continue;
                }
                let block = policy_recreate_block(policy);
                Self::emit_dependent_recreate(buffers.out_for(&block), self.use_drop, &block);
            }
        }

        for to_foreign in &self.to.foreign_tables {
            for column in &to_foreign.columns {
                Self::emit_foreign_default_restore(
                    buffers,
                    self.use_drop,
                    emitted_keys,
                    to_foreign,
                    column,
                    routine_phase,
                );
            }
        }
    }

    /// Emit, into `buffers`, `alter foreign table … alter column … set default …`
    /// for the TO foreign-table column `column` when its default calls one of
    /// `names` and `emitted_keys` has not seen it yet — commented out with
    /// `use_drop` off.
    fn emit_foreign_default_restore(
        buffers: &mut RecreateBuffers<'_>,
        use_drop: bool,
        emitted_keys: &mut HashSet<String>,
        foreign_table: &ForeignTable,
        column: &crate::dump::foreign_table::ForeignTableColumn,
        names: &HashSet<(String, String)>,
    ) {
        let Some(default) = column
            .column_default
            .as_deref()
            .filter(|d| Self::definition_references_any(d, names))
        else {
            return;
        };
        let key = format!(
            "foreign-column-default:{}.{}.{}",
            foreign_table.schema, foreign_table.name, column.name
        );
        if !emitted_keys.insert(key) {
            return;
        }
        let stmt = format!(
            "alter foreign table {}.{} alter column {} set default {};",
            foreign_table.schema, foreign_table.name, column.name, default
        )
        .with_empty_lines();
        Self::emit_dependent_recreate(buffers.out_for(&stmt), use_drop, &stmt);
    }

    /// The statements that give the clones of the TO trigger `trigger` of the
    /// partitioned TO table `parent`, on every level of its partitions, their
    /// own enabled state and comment once `recreate_routine_dependents` has
    /// created the parent's trigger again (see
    /// [`TableTrigger::get_clone_state_script`]): PostgreSQL clones it in the
    /// parent's state and without a comment. Only a clone TO records (see
    /// [`Table::trigger_clones`]) needs anything; parents come first, since
    /// setting a partition's state recurses to its own partitions.
    fn partition_trigger_clone_states(&self, parent: &Table, trigger: &TableTrigger) -> String {
        let mut script = String::new();
        let mut queue: VecDeque<(String, String)> = VecDeque::from([(
            Self::table_key(&parent.schema, &parent.name),
            trigger.enabled.clone(),
        )]);
        let mut seen: HashSet<String> = HashSet::new();
        while let Some((parent_key, parent_enabled)) = queue.pop_front() {
            if !seen.insert(parent_key.clone()) {
                continue;
            }
            for partition in &self.to.tables {
                if partition
                    .partition_of
                    .as_deref()
                    .is_none_or(|p| Self::normalise_partition_of(p) != parent_key)
                {
                    continue;
                }
                let enabled = match partition
                    .trigger_clones
                    .iter()
                    .find(|t| t.name == trigger.name)
                {
                    Some(clone) => {
                        script.push_str(&clone.get_clone_state_script(
                            &partition.schema,
                            &partition.name,
                            &parent_enabled,
                        ));
                        clone.enabled.clone()
                    }
                    None => parent_enabled.clone(),
                };
                queue.push_back((Self::table_key(&partition.schema, &partition.name), enabled));
            }
        }
        script
    }

    /// The statements that bring the partition clones of every TO row
    /// trigger to the enabled state and comment TO has for them (see
    /// [`Table::trigger_clones`]), for the end of `trigger_post_script`.
    ///
    /// A clone is never created or dropped: PostgreSQL creates it with its
    /// parent's trigger, or with the partition, in the parent's enabled
    /// state and without a comment. Setting a table's enabled state (no
    /// `ONLY`) sets every clone below it too. So each clone is followed from
    /// its trigger's table down, parents first, and is in one of these
    /// states once the statements before this pass have run:
    ///
    /// * **fresh** — the parent's trigger is new or re-created (a changed
    ///   definition, a table the migration creates or drops and re-creates),
    ///   or the partition is new, re-created or attached to its parent by
    ///   this migration: the parent's final enabled state, no comment;
    /// * **overridden** — an ancestor's enabled state was set, by the
    ///   trigger alter or by this pass: the parent's final enabled state,
    ///   and the comment FROM gives it;
    /// * otherwise as FROM has it.
    ///
    /// A partition whose parent is not in the TO dump (it lives in a schema
    /// the dump does not cover) starts a walk of its own, from each clone it
    /// records — the dump records every clone of such a partition. The
    /// parent's state is unknown there, so a fresh clone gets its TO enabled
    /// state set whatever it is, and an existing one is compared with the
    /// clone FROM records; one FROM does not record does not exist there
    /// (its parent's trigger is not in FROM's copy of that parent), and is
    /// left alone with everything below it.
    ///
    /// A statement goes out only where that differs from TO, so an
    /// unchanged schema gets none. Tables whose hash is missing are left
    /// alone, as `compare_tables` leaves them.
    fn partition_trigger_clone_script(&self) -> String {
        let mut script = String::new();
        let mut children: HashMap<String, Vec<&Table>> = HashMap::new();
        for table in &self.to.tables {
            if let Some(parent) = table.partition_of.as_deref() {
                children
                    .entry(Self::normalise_partition_of(parent))
                    .or_default()
                    .push(table);
            }
        }
        if children.is_empty() {
            return script;
        }
        let from_by_key: HashMap<String, &Table> = self
            .from
            .tables
            .iter()
            .map(|t| (Self::table_key(&t.schema, &t.name), t))
            .collect();
        let to_keys: HashSet<String> = self
            .to
            .tables
            .iter()
            .map(|t| Self::table_key(&t.schema, &t.name))
            .collect();
        let rebuilt = |key: &str| self.dropped_and_recreated_tables.contains(key);

        for owner in &self.to.tables {
            let owner_key = Self::table_key(&owner.schema, &owner.name);
            // A partition of a parent the dump does not have: a walk from
            // each clone it records.
            if let Some(parent) = owner.partition_of.as_deref()
                && !to_keys.contains(&Self::normalise_partition_of(parent))
            {
                for clone in &owner.trigger_clones {
                    self.walk_partition_trigger_clones(
                        &mut script,
                        (&clone.name, clone.oid),
                        VecDeque::from([(owner, None, false, false)]),
                        &children,
                        &from_by_key,
                    );
                }
            }
            let Some(partitions) = children.get(&owner_key) else {
                continue;
            };
            let from_owner = from_by_key.get(&owner_key).copied();
            if owner.hash.is_none() || from_owner.is_some_and(|t| t.hash.is_none()) {
                continue;
            }
            for trigger in owner.triggers.iter().filter(|t| t.is_row_level()) {
                let from_trigger = from_owner
                    .filter(|_| !rebuilt(&owner_key))
                    .and_then(|t| t.triggers.iter().find(|ft| ft.name == trigger.name));
                let fresh = from_trigger.is_none_or(|ft| ft.definition != trigger.definition)
                    || self
                        .triggers_dropped_ahead
                        .contains(&format!("{owner_key}.{}", trigger.name));
                let overridden = from_trigger.is_some_and(|ft| ft.enabled != trigger.enabled);
                self.walk_partition_trigger_clones(
                    &mut script,
                    (&trigger.name, trigger.oid),
                    partitions
                        .iter()
                        .map(|p| (*p, Some(trigger.enabled.clone()), fresh, overridden))
                        .collect(),
                    &children,
                    &from_by_key,
                );
            }
        }
        script
    }

    /// One walk of `partition_trigger_clone_script`: the clones named
    /// `trigger` (name, OID) on the TO partitions in `queue` and on every
    /// level below them, parents first. Each queue entry is `(partition, the
    /// enabled state its parent's copy ends up in — None when unknown,
    /// whether that copy is fresh, whether its state was set by the
    /// migration)`.
    fn walk_partition_trigger_clones<'a>(
        &self,
        script: &mut String,
        (name, oid): (&str, Oid),
        mut queue: VecDeque<(&'a Table, Option<String>, bool, bool)>,
        children: &HashMap<String, Vec<&'a Table>>,
        from_by_key: &HashMap<String, &Table>,
    ) {
        let rebuilt = |key: &str| self.dropped_and_recreated_tables.contains(key);
        let mut seen: HashSet<String> = HashSet::new();
        while let Some((partition, parent_enabled, parent_fresh, parent_touched)) =
            queue.pop_front()
        {
            let key = Self::table_key(&partition.schema, &partition.name);
            if !seen.insert(key.clone()) {
                continue;
            }
            let from_partition = from_by_key.get(&key).copied();
            if partition.hash.is_none() || from_partition.is_some_and(|t| t.hash.is_none()) {
                continue;
            }
            let to_clone = partition.trigger_clones.iter().find(|c| c.name == name);
            let Some(to_enabled) = to_clone
                .map(|c| c.enabled.clone())
                .or_else(|| parent_enabled.clone())
            else {
                continue;
            };
            let to_comment = to_clone.and_then(|c| c.comment.as_deref());

            let fresh = parent_fresh
                || rebuilt(&key)
                || from_partition.is_none_or(|fp| {
                    fp.partition_of.as_deref().map(Self::normalise_partition_of)
                        != partition
                            .partition_of
                            .as_deref()
                            .map(Self::normalise_partition_of)
                });
            let from_state = if fresh {
                None
            } else {
                from_partition.and_then(|fp| self.trigger_clone_state_in_from(fp, name))
            };
            if !fresh && from_state.is_none() && parent_enabled.is_none() {
                // Below a parent the dump does not have, and FROM has no
                // such clone: that parent has no such trigger in FROM.
                continue;
            }
            let (after_enabled, after_comment) = match &from_state {
                None => (parent_enabled.as_deref(), None),
                Some((enabled, comment)) => (
                    if parent_touched {
                        parent_enabled.as_deref()
                    } else {
                        Some(enabled.as_str())
                    },
                    comment.as_deref(),
                ),
            };

            let mut set_here = false;
            let state = TableTrigger {
                oid,
                name: name.to_string(),
                definition: String::new(),
                enabled: to_enabled.clone(),
                comment: to_comment.map(str::to_string),
            };
            if after_enabled != Some(to_enabled.as_str()) {
                script.append_block(
                    &state.enabled_state_statement(&partition.schema, &partition.name),
                );
                set_here = true;
            }
            if after_comment != to_comment {
                script.append_block(&match to_comment {
                    Some(comment) => format!(
                        "comment on trigger {} on {}.{} is '{}';",
                        name,
                        partition.schema,
                        partition.name,
                        comment.replace('\'', "''")
                    ),
                    None => format!(
                        "comment on trigger {} on {}.{} is null;",
                        name, partition.schema, partition.name
                    ),
                });
            }
            for child in children.get(&key).into_iter().flatten() {
                queue.push_back((
                    *child,
                    Some(to_enabled.clone()),
                    fresh,
                    parent_touched || set_here,
                ));
            }
        }
    }

    /// The enabled state and comment the FROM partition `partition` has for
    /// its clone of the row trigger `name`: its own, when FROM records the
    /// clone (see [`Table::trigger_clones`]); otherwise its parent's state,
    /// looked up the chain, and no comment. `None` when no FROM ancestor has
    /// the trigger, so the partition has no such clone.
    fn trigger_clone_state_in_from(
        &self,
        partition: &Table,
        name: &str,
    ) -> Option<(String, Option<String>)> {
        if let Some(clone) = partition.trigger_clones.iter().find(|c| c.name == name) {
            return Some((clone.enabled.clone(), clone.comment.clone()));
        }
        let mut current = partition;
        for _ in 0..self.from.tables.len() {
            let parent_key = Self::normalise_partition_of(current.partition_of.as_deref()?);
            current = self
                .from
                .tables
                .iter()
                .find(|t| Self::table_key(&t.schema, &t.name) == parent_key)?;
            if let Some(clone) = current.trigger_clones.iter().find(|c| c.name == name) {
                return Some((clone.enabled.clone(), None));
            }
            if let Some(trigger) = current
                .triggers
                .iter()
                .find(|t| t.name == name && t.is_row_level())
            {
                return Some((trigger.enabled.clone(), None));
            }
        }
        None
    }

    /// Materialized views `compare_routines_and_views` creates — new, or
    /// recreated after `drop_views` dropped them (a changed definition, a
    /// dependency on a changing table) — as `Comparer::normalized_view_key`s.
    /// Statistics on them are created after the view
    /// (`view_statistics_post_script`).
    fn created_matview_keys(&self) -> HashSet<String> {
        let from_views: HashSet<String> = self
            .from
            .views
            .iter()
            .map(|v| Self::normalized_view_key(&v.schema, &v.name))
            .collect();
        self.to
            .views
            .iter()
            .filter(|v| v.is_materialized)
            .map(|v| Self::normalized_view_key(&v.schema, &v.name))
            .filter(|key| !from_views.contains(key) || self.dropped_views.get(key) == Some(&true))
            .collect()
    }

    /// The TO extended statistics objects `recreate_routine_dependents` puts
    /// back after the routines, as `(schema, name)` in TO dump order: every
    /// one whose definition calls a routine in `affected` (see
    /// `Comparer::cascade_affected_routines`), except those on a materialized
    /// view the migration creates, which are created after that view anyway
    /// (`Comparer::created_matview_keys`).
    ///
    /// Such an object is gone once the routine's `CASCADE` has run, or will
    /// be: `drop_views` drops a routine before `compare_statistics` runs, the
    /// routine phase after it. Either way it is created from TO after the
    /// routines, which is why `compare_statistics` skips these (with
    /// `use_drop` on; with it off nothing is dropped, and the restore is only
    /// emitted commented out). A TO-only object calling the routine is in the
    /// set too: created before the routine phase, it would go with the drop.
    fn statistics_restored_after_cascade(
        &self,
        affected: &HashSet<(String, String)>,
    ) -> Vec<(String, String)> {
        if affected.is_empty() {
            return Vec::new();
        }
        let created_matviews = self.created_matview_keys();
        self.to
            .statistics
            .iter()
            .filter(|stat| {
                !created_matviews.contains(&Self::normalized_view_key(
                    &stat.table_schema,
                    &stat.table_name,
                ))
            })
            .filter(|stat| Self::definition_references_any(&stat.definition, affected))
            .map(|stat| (stat.schema.clone(), stat.name.clone()))
            .collect()
    }

    /// `drop constraint if exists` + `add constraint` blocks for the domain
    /// CHECKs a routine's `CASCADE` takes along and nothing else in the
    /// migration puts back, commented out with `use_drop` off.
    ///
    /// A TO CHECK calling a routine in `affected` is in place when that
    /// routine is dropped if the domain alter leaves it there
    /// ([`PgType::keeps_domain_constraint`]: the same text, or only
    /// validated), or if the domain is new — `compare_types` creates a new
    /// domain with its CHECKs before any routine is dropped. A changed or
    /// added CHECK on an existing domain is not: the alter drops the old text
    /// early and adds the new one late, after the routines.
    fn domain_constraint_restores(&self, affected: &HashSet<(String, String)>) -> String {
        let mut out = String::new();
        if affected.is_empty() {
            return out;
        }
        let from_types: HashMap<(&str, &str), &PgType> = self
            .from
            .types
            .iter()
            .map(|t| ((t.schema.as_str(), t.typname.as_str()), t))
            .collect();
        for to_type in &self.to.types {
            if (to_type.typtype as u8 as char) != 'd' {
                continue;
            }
            let from_type = from_types
                .get(&(to_type.schema.as_str(), to_type.typname.as_str()))
                .copied();
            for constraint in &to_type.domain_constraints {
                if !Self::definition_references_any(&constraint.definition, affected) {
                    continue;
                }
                let in_place = match from_type {
                    None => true,
                    Some(from_type) => {
                        (from_type.typtype as u8 as char) == 'd'
                            && from_type.keeps_domain_constraint(to_type, &constraint.name)
                    }
                };
                if in_place {
                    Self::emit_dependent_recreate(
                        &mut out,
                        self.use_drop,
                        &to_type.get_domain_constraint_recreate_script(constraint),
                    );
                }
            }
        }
        out
    }

    /// The `--` note for a routine the routine phase would drop with
    /// `CASCADE` — a FROM-only one, or one `Comparer::routine_is_recreated` —
    /// when that `CASCADE` would take along something pgc cannot re-create
    /// (`Comparer::cascade_blockers`, over the routine and the routines its
    /// `CASCADE` takes along, `Comparer::cascade_reach`): a partitioned table with its rows, a
    /// range or base type with every column of it, a foreign-data wrapper,
    /// or an operator or cast TO keeps. `None` when there is nothing such,
    /// and with `use_drop` off, which comments the drop out.
    ///
    /// The caller then drops the routine without `CASCADE`, so the migration
    /// stops on PostgreSQL's dependency check instead of losing them; the
    /// same warning goes to stderr.
    fn routine_phase_cascade_note(&self, routine: &Routine) -> Option<String> {
        if !self.use_drop {
            return None;
        }
        let blockers =
            self.cascade_blockers(&self.cascade_reach(routine), CascadeMoment::RoutinePhase);
        if blockers.is_empty() {
            return None;
        }
        let signature = format!("{}.{}({})", routine.schema, routine.name, routine.arguments);
        let what = blockers.join(", ");
        eprintln!(
            "Warning: routine {signature} has to be dropped by this migration, but {what} \
             depends on it, which a DROP ... CASCADE would take along and pgc cannot \
             re-create. It is dropped without CASCADE, so that statement fails; resolve it by \
             hand."
        );
        // One `--` line: a name may hold a line break.
        let note = format!(
            "Routine {signature} is dropped without CASCADE: {what} depends on it, which a DROP ... CASCADE would take along and pgc cannot re-create (manual intervention needed)."
        )
        .replace(['\n', '\r'], " ");
        Some(format!("-- {note}").with_empty_lines())
    }

    /// `alter domain … drop default;` for every domain whose default calls
    /// `routine` at the point the migration drops that routine with
    /// `CASCADE`, together with those domains as `(schema, typname)` for
    /// `detached_domain_defaults`, which `recreate_routine_dependents` reads
    /// to set the TO default again.
    ///
    /// PostgreSQL records the dependencies of a domain's default on the domain
    /// itself, so the `CASCADE` would otherwise drop the domain and every
    /// column of that type, with its data. The default in place at that point
    /// is the TO one when `compare_types` changed it (it runs before any
    /// routine is dropped) and the FROM one otherwise; a domain only TO has
    /// was created by `compare_types` with its TO default. Matched by the
    /// routine's name, as every other `CASCADE` dependent is, so an overload
    /// the default does not call detaches it too — harmlessly, since it is
    /// set again.
    fn domain_default_detach(&self, routine: &Routine) -> (String, Vec<(String, String)>) {
        // The routines its argument defaults take along go with it too.
        let name: HashSet<(String, String)> = self
            .cascade_reach(routine)
            .into_iter()
            .map(Self::unquoted_routine_name)
            .collect();
        let calls_routine = |default: Option<&str>| {
            default.is_some_and(|d| Self::definition_references_any(d, &name))
        };
        let is_domain = |t: &PgType| (t.typtype as u8 as char) == 'd';
        let to_domains: HashMap<(&str, &str), &PgType> = self
            .to
            .types
            .iter()
            .filter(|t| is_domain(t))
            .map(|t| ((t.schema.as_str(), t.typname.as_str()), t))
            .collect();
        let from_types: HashSet<(&str, &str)> = self
            .from
            .types
            .iter()
            .map(|t| (t.schema.as_str(), t.typname.as_str()))
            .collect();

        let mut script = String::new();
        let mut detached = Vec::new();
        for from_type in self.from.types.iter().filter(|t| is_domain(t)) {
            let default =
                match to_domains.get(&(from_type.schema.as_str(), from_type.typname.as_str())) {
                    // `compare_types` set the TO default, or dropped the FROM one
                    // (with `use_drop`; without it the drop is commented out).
                    Some(to_type)
                        if to_type.typdefault != from_type.typdefault
                            && (to_type.typdefault.is_some() || self.use_drop) =>
                    {
                        to_type.typdefault.as_deref()
                    }
                    _ => from_type.typdefault.as_deref(),
                };
            if calls_routine(default) {
                script.push_str(&from_type.get_drop_default_script());
                detached.push((from_type.schema.clone(), from_type.typname.clone()));
            }
        }
        for to_type in self.to.types.iter().filter(|t| is_domain(t)) {
            if !from_types.contains(&(to_type.schema.as_str(), to_type.typname.as_str()))
                && calls_routine(to_type.typdefault.as_deref())
            {
                script.push_str(&to_type.get_drop_default_script());
                detached.push((to_type.schema.clone(), to_type.typname.clone()));
            }
        }
        (script, detached)
    }

    /// Record domains `Comparer::domain_default_detach` detached, once each.
    fn record_detached_domain_defaults(&mut self, detached: Vec<(String, String)>) {
        for key in detached {
            if !self.detached_domain_defaults.contains(&key) {
                self.detached_domain_defaults.push(key);
            }
        }
    }

    /// Emit, into `script`, the `Comparer::domain_default_detach` statements
    /// for the FROM routine `routine_idx`, which the statement emitted next
    /// drops with `CASCADE` — commented out, like that drop, with `use_drop`
    /// off.
    fn emit_domain_default_detach(&mut self, routine_idx: usize) {
        let (script, detached) = self.domain_default_detach(&self.from.routines[routine_idx]);
        if script.is_empty() {
            return;
        }
        Self::emit_drop(&mut self.script, self.use_drop, &script);
        self.record_detached_domain_defaults(detached);
    }

    /// True when `definition` textually references any `(schema, name)` in
    /// `routines`. Tries the qualified `schema.name` form first, then
    /// falls back to an *unqualified* function-call match (`name(`):
    /// PostgreSQL's deparsers (`pg_get_constraintdef`, `pg_get_indexdef`,
    /// `pg_get_expr`) drop the schema qualifier whenever the function is
    /// reachable via the active `search_path` — typical for `public` —
    /// so dependents like `CHECK (compute(value) > 0)` would otherwise
    /// be missed and the CASCADE drift left unfixed.
    ///
    /// The unqualified pass requires `(` (after optional whitespace) so
    /// it only matches function-call positions: identifiers used as
    /// column references, type names, or string-literal contents do not
    /// trigger a false positive.
    pub(crate) fn definition_references_any(
        definition: &str,
        routines: &HashSet<(String, String)>,
    ) -> bool {
        // PR #187 review: blank out single-quoted string-literal
        // contents before matching so a literal like
        // `CHECK (msg <> 'compute(')` doesn't trip the matcher.
        // `prelower_pair` only handles double quotes (identifier
        // quoting); single quotes are SQL string literals and any
        // routine-name lookalikes inside them must not count.
        let scrubbed = Self::blank_single_quoted_literals(definition);
        let (lower, unquoted) = Self::prelower_pair(&scrubbed);
        routines.iter().any(|(schema, name)| {
            // Both passes require a function-call context (`name(` after
            // optional whitespace). The qualified pass inherits this gate
            // so a routine name that collides with another object kind
            // in the same schema (table, sequence, view, …) does not
            // trip Phase 7 — `pg_get_indexdef` emits `ON schema.table`,
            // `pg_get_expr` emits `nextval('schema.seq'::regclass)`,
            // etc., and those non-call references must not look like
            // CASCADE-affected dependents.
            Self::text_references_qualified_call(&lower, &unquoted, schema, name)
                || Self::text_references_unqualified_call(&unquoted, name)
        })
    }

    /// Replace every character inside single-quoted string literals
    /// with a space (length-preserved so byte indices in callers stay
    /// stable). Doubled-single-quote escapes (`''`) are treated as a
    /// literal `'` inside the string and stay scrubbed. Used by the
    /// Phase-7 dependency matcher (PR #187 review): a SQL string
    /// literal like `'compute('` must not be mistaken for a function
    /// call to `compute`.
    fn blank_single_quoted_literals(text: &str) -> String {
        let mut out = String::with_capacity(text.len());
        let mut chars = text.chars();
        while let Some(c) = chars.next() {
            if c != '\'' {
                out.push(c);
                continue;
            }
            // Opening quote stays as-is so token boundaries are
            // preserved; only the literal's body is scrubbed.
            out.push('\'');
            loop {
                match chars.next() {
                    Some('\'') => {
                        if chars.as_str().starts_with('\'') {
                            // doubled-quote escape — still inside the
                            // literal, blank both single quotes' worth
                            // of whitespace (one for the escape pair).
                            out.push(' ');
                            out.push(' ');
                            chars.next();
                        } else {
                            out.push('\''); // closing quote
                            break;
                        }
                    }
                    Some(ch) => {
                        // Replace each char with a space-equivalent
                        // sequence of the same byte length so indices
                        // in callers stay aligned with the original.
                        for _ in 0..ch.len_utf8() {
                            out.push(' ');
                        }
                    }
                    None => break, // unterminated literal
                }
            }
        }
        out
    }

    /// Like [`Self::text_references_qualified_name_pre`] but additionally
    /// requires the `schema.name` reference to be a function call:
    /// followed (after optional ASCII whitespace) by `(`. Phase 7 uses
    /// this stricter form so a routine sharing its name with another
    /// object kind in the same schema (table, sequence, view) does not
    /// false-positive on the deparsed `ON schema.table` of a CREATE
    /// INDEX or the `'schema.seq'::regclass` of a `nextval` default.
    /// The looser, call-context-free
    /// [`Self::text_references_qualified_name_pre`] is kept for the
    /// inter-routine dependency scan (which filters non-routine matches
    /// by identity afterwards) and other call-sites where a non-call
    /// reference is still meaningful.
    fn text_references_qualified_call(
        lowered: &str,
        unquoted_lowered: &str,
        schema_lc: &str,
        name_lc: &str,
    ) -> bool {
        let mut pattern = String::with_capacity(schema_lc.len() + 1 + name_lc.len());
        pattern.push_str(schema_lc);
        pattern.push('.');
        pattern.push_str(name_lc);
        if Self::has_whole_qualified_call(lowered, &pattern) {
            return true;
        }
        let unquoted_pattern: String = if pattern.contains('"') {
            pattern.chars().filter(|c| *c != '"').collect()
        } else {
            pattern
        };
        Self::has_whole_qualified_call(unquoted_lowered, &unquoted_pattern)
    }

    /// Identifier-boundary delimited match for `qualified` plus a call
    /// gate (optional whitespace then `(`). Mirrors
    /// [`Self::has_whole_qualified_name`] for boundary semantics; the
    /// extra gate makes it safe to feed deparsed DDL that may contain
    /// `schema.name` references in non-call positions.
    fn has_whole_qualified_call(text: &str, qualified: &str) -> bool {
        let bytes = text.as_bytes();
        for (idx, _) in text.match_indices(qualified) {
            if idx > 0 {
                let before = bytes[idx - 1];
                if before.is_ascii_alphanumeric() || before == b'_' {
                    continue;
                }
            }
            let end = idx + qualified.len();
            if end < bytes.len() {
                let after = bytes[end];
                if after.is_ascii_alphanumeric() || after == b'_' || after == b'.' {
                    continue;
                }
            }
            let mut j = end;
            while j < bytes.len() && bytes[j].is_ascii_whitespace() {
                j += 1;
            }
            if j < bytes.len() && bytes[j] == b'(' {
                return true;
            }
        }
        false
    }

    /// True when `text` contains `name` as a function call: a whole
    /// identifier (delimited on the left by a non-identifier, non-dot
    /// character, on the right by a non-identifier character) followed
    /// — possibly across whitespace — by `(`. The left-side dot
    /// exclusion ensures `schema.name(` is left to the qualified
    /// matcher; the right-side identifier check stops `compute_v2(`
    /// from matching when `name = "compute"`. `text` and `name` must
    /// already be lowercased and quote-stripped (use the `unquoted`
    /// half of [`Self::prelower_pair`]).
    fn text_references_unqualified_call(text: &str, name: &str) -> bool {
        if name.is_empty() {
            return false;
        }
        // `match_indices` yields byte offsets that are guaranteed UTF-8
        // char boundaries — safe to slice around. PR #187 review: the
        // boundary check now uses CHARACTER classification rather than
        // raw byte tests so identifier-class Unicode neighbours
        // (e.g. another Cyrillic letter next to a Cyrillic name) are
        // correctly rejected. ASCII byte tests treated those bytes
        // as non-identifier and would let `функция` match inside the
        // longer identifier `мояфункция`.
        let bytes = text.as_bytes();
        let name_len = name.len();
        let is_ident_continuation = |c: char| c.is_alphanumeric() || c == '_' || c == '$';
        for (i, _) in text.match_indices(name) {
            if i > 0
                && let Some(prev_ch) = text[..i].chars().next_back()
                && (is_ident_continuation(prev_ch) || prev_ch == '.')
            {
                continue;
            }
            let end = i + name_len;
            if end < bytes.len()
                && let Some(next_ch) = text[end..].chars().next()
                && is_ident_continuation(next_ch)
            {
                continue;
            }
            let mut j = end;
            while j < bytes.len() && bytes[j].is_ascii_whitespace() {
                j += 1;
            }
            if j < bytes.len() && bytes[j] == b'(' {
                return true;
            }
        }
        false
    }

    /// Append a recreate block (drop-if-exists + create) to `out`. When
    /// `use_drop` is false every line is line-prefixed with `-- ` so the
    /// SQL is left in for review but won't execute, mirroring
    /// [`Comparer::emit_drop`].
    fn emit_dependent_recreate(out: &mut String, use_drop: bool, block: &str) {
        if use_drop {
            out.push_str(block);
        } else {
            out.push_str(&comment_out(block));
        }
    }

    /// Emit the recreate block for a CASCADE-dropped index.
    ///
    /// In production mode (`prod_ctx` is `Some`) the index goes through the same
    /// partition-aware concurrent split as every other index creation: `ON ONLY`
    /// in-transaction plus per-partition `CONCURRENTLY`/`ATTACH PARTITION`
    /// post-commit for a partitioned parent, and a concurrent post-commit build for
    /// a plain table. A plain in-transaction `CREATE INDEX` here would take a long
    /// write lock — and before issue #223 the verbatim `ON ONLY` rendering left the
    /// recreated parent index permanently invalid instead. The default mode keeps
    /// the plain `CREATE INDEX IF NOT EXISTS` block, and so does commented review
    /// output (`use_drop = false`), where post-commit fragments would be
    /// meaningless.
    fn emit_index_dependent_recreate(
        out: &mut String,
        post_commit: &mut String,
        use_drop: bool,
        index: &TableIndex,
        prod_ctx: Option<&PartitionContext>,
    ) {
        match prod_ctx {
            Some(ctx) if use_drop => {
                let split = production::index_create_split(index, ctx, false);
                out.push_str(&split.in_txn);
                post_commit.push_str(&split.post_commit);
            }
            _ => Self::emit_dependent_recreate(out, use_drop, &index_recreate_block(index)),
        }
    }

    /// Re-emit every TO-side dependent of `(table_schema, table_name,
    /// column_name)` that PostgreSQL would have CASCADE-dropped when the
    /// column was dropped. Uses the `column_dependents` graph harvested
    /// from `pg_depend` at dump time (issue #188); fixes the gap where
    /// the text-based Phase 7 scanner misses dependents that reference
    /// the column rather than the routine driving the CASCADE chain.
    ///
    /// `emitted_keys` is shared with the caller so dependents already
    /// emitted by Phase 7's routine-text scan (e.g., a functional index
    /// that names both the routine *and* the column) are not double-
    /// emitted.
    ///
    /// Anchor identifiers are matched against `Dump::column_dependents`
    /// after case-insensitive comparison with quote-stripping; that is
    /// because the dump query wraps identifiers via `quote_ident` (so
    /// case-sensitive names land here as `"MixedCase"`) but Phase 7 and
    /// `compare_tables` operate on the raw fields, which may be either
    /// form depending on origin.
    fn recreate_column_dependents(
        &self,
        table_schema: &str,
        table_name: &str,
        column_name: &str,
        emitted_keys: &mut HashSet<String>,
        buffers: &mut RecreateBuffers<'_>,
        prod_ctx: Option<&PartitionContext>,
    ) {
        self.recreate_column_dependents_from(
            self.from.column_dependents.iter(),
            (table_schema, table_name, column_name),
            emitted_keys,
            buffers,
            prod_ctx,
        );
    }

    /// [`Comparer::recreate_column_dependents`] over the column edges `edges`
    /// rather than the FROM dump's alone, for the anchor `(table_schema,
    /// table_name, column_name)`.
    fn recreate_column_dependents_from<'a>(
        &'a self,
        edges: impl Iterator<Item = &'a ColumnDependent>,
        (table_schema, table_name, column_name): (&str, &str, &str),
        emitted_keys: &mut HashSet<String>,
        buffers: &mut RecreateBuffers<'_>,
        prod_ctx: Option<&PartitionContext>,
    ) {
        let want_schema = table_schema.to_lowercase().replace('"', "");
        let want_table = table_name.to_lowercase().replace('"', "");
        let want_column = column_name.to_lowercase().replace('"', "");

        // Two passes so FK constraints emit *after* the UNIQUE/PK
        // target they reference. A single linear pass would let the
        // pg_depend query's undefined output order produce a script
        // where `ADD CONSTRAINT fk … FOREIGN KEY … REFERENCES …` runs
        // before the matching `ADD CONSTRAINT … UNIQUE` is back, and
        // PostgreSQL rejects FKs whose target lacks a unique
        // constraint. Pass 1 emits indexes, non-FK constraints, and
        // policies; pass 2 emits FKs against the now-restored unique
        // targets.
        let mut deferred_fks: Vec<&TableConstraint> = Vec::new();

        for dep in edges {
            if dep.schema.to_lowercase().replace('"', "") != want_schema
                || dep.table.to_lowercase().replace('"', "") != want_table
                || dep.column.to_lowercase().replace('"', "") != want_column
            {
                continue;
            }

            // Statistics are looked up before the owning-table lookup below:
            // that one keys on `(dep_schema, dep_table)`, but a statistics
            // object's `dep_schema` is its own namespace, not the table's.
            if dep.kind == ColumnDependentKind::Statistic {
                self.recreate_column_dependent_statistic(dep, emitted_keys, buffers);
                continue;
            }

            let Some(owner) = self
                .to
                .tables
                .iter()
                .find(|t| t.schema == dep.dep_schema && t.name == dep.dep_table)
            else {
                continue;
            };

            match dep.kind {
                ColumnDependentKind::Index => {
                    let Some(to_index) = owner.indexes.iter().find(|i| i.name == dep.dep_name)
                    else {
                        continue;
                    };
                    if to_index.is_partition_index {
                        continue;
                    }
                    // UNIQUE/PK/EXCLUDE constraints share their backing
                    // index name. When such a constraint exists in TO,
                    // the constraint branch below will recreate the
                    // index — emitting both would race and the
                    // `ADD CONSTRAINT … PRIMARY KEY` form rejects a
                    // pre-existing index of the same name. Skip the
                    // index emit and let the constraint do it.
                    if owner.constraints.iter().any(|c| c.name == to_index.name) {
                        continue;
                    }
                    let key = format!("index:{}.{}", to_index.schema, to_index.name);
                    if !emitted_keys.insert(key) {
                        continue;
                    }
                    let (out, post_commit) = buffers.targets(&to_index.indexdef);
                    Self::emit_index_dependent_recreate(
                        out,
                        post_commit,
                        self.use_drop,
                        to_index,
                        prod_ctx,
                    );
                }
                ColumnDependentKind::Constraint => {
                    let Some(to_constraint) =
                        owner.constraints.iter().find(|c| c.name == dep.dep_name)
                    else {
                        continue;
                    };
                    // Partition-inherited constraints (coninhcount > 0)
                    // propagate from the parent — re-emitting on the
                    // child is the same rejection Phase 7 already
                    // guards against in its routine-text scan.
                    let is_target_partition_child = owner.partition_of.is_some();
                    if is_target_partition_child && to_constraint.coninhcount > 0 {
                        continue;
                    }
                    let key = format!(
                        "constraint:{}.{}.{}",
                        to_constraint.schema, to_constraint.table_name, to_constraint.name
                    );
                    if !emitted_keys.insert(key) {
                        continue;
                    }
                    if to_constraint
                        .constraint_type
                        .eq_ignore_ascii_case("foreign key")
                    {
                        // Defer FK emission until pass 2 (after every
                        // UNIQUE/PK in the same call has been restored).
                        deferred_fks.push(to_constraint);
                        continue;
                    }
                    let block = constraint_recreate_block(to_constraint);
                    Self::emit_dependent_recreate(buffers.out_for(&block), self.use_drop, &block);
                }
                // Handled (and `continue`d) before the owner lookup above.
                ColumnDependentKind::Statistic => {}
                // Not taken along by the column's drop but blocking it: the
                // table phase drops such a trigger ahead of the table alters
                // and creates it again after every routine, with the other
                // triggers (see `Comparer::column_drop_blockers`).
                ColumnDependentKind::Trigger => {}
                ColumnDependentKind::Policy => {
                    let Some(to_policy) = owner.policies.iter().find(|p| p.name == dep.dep_name)
                    else {
                        continue;
                    };
                    let key = format!(
                        "policy:{}.{}.{}",
                        to_policy.schema, to_policy.table, to_policy.name
                    );
                    if !emitted_keys.insert(key) {
                        continue;
                    }
                    let block = policy_recreate_block(to_policy);
                    Self::emit_dependent_recreate(buffers.out_for(&block), self.use_drop, &block);
                }
            }
        }

        // Pass 2: FK constraints, now that any UNIQUE/PK target they
        // reference (on the anchor column's table or any other table)
        // has already been restored above.
        for to_constraint in deferred_fks {
            let block = constraint_recreate_block(to_constraint);
            Self::emit_dependent_recreate(buffers.out_for(&block), self.use_drop, &block);
        }
    }

    /// Restore one extended statistics object that PostgreSQL CASCADE-dropped
    /// with a column it names — the `ColumnDependentKind::Statistic` arm of
    /// [`Comparer::recreate_column_dependents`], and all of
    /// [`Comparer::recreate_statistics_on_readded_column`].
    ///
    /// The TO-side object is found by its own `(schema, name)` and must still
    /// be defined on the anchor column's table; anything else (gone from TO,
    /// or moved to another table) is left to `compare_statistics`, whose
    /// hash-driven drop + create covers it. The block is
    /// `drop statistics if exists` + the TO `CREATE STATISTICS`, so it is
    /// harmless if the object survived after all, and `compare_statistics`
    /// may still emit its own diff for the same object afterwards.
    fn recreate_column_dependent_statistic(
        &self,
        dep: &ColumnDependent,
        emitted_keys: &mut HashSet<String>,
        buffers: &mut RecreateBuffers<'_>,
    ) {
        let Some(to_stat) = self
            .to
            .statistics
            .iter()
            .find(|s| s.schema == dep.dep_schema && s.name == dep.dep_name)
        else {
            return;
        };
        if to_stat.table_schema != dep.schema || to_stat.table_name != dep.table {
            return;
        }
        self.emit_statistic_restore(to_stat, emitted_keys, buffers);
    }

    /// Append the restore block of one TO statistics object to `buffers`:
    /// `drop statistics if exists` + its `CREATE STATISTICS` (owner, target
    /// and comment included), commented out when `use_drop` is off. The key
    /// `statistic:<schema>.<name>` in `emitted_keys` keeps an object that
    /// several dropped columns or relations name from being restored twice.
    fn emit_statistic_restore(
        &self,
        to_stat: &Statistic,
        emitted_keys: &mut HashSet<String>,
        buffers: &mut RecreateBuffers<'_>,
    ) {
        let key = format!("statistic:{}.{}", to_stat.schema, to_stat.name);
        if !emitted_keys.insert(key) {
            return;
        }
        let from_stat = self
            .from
            .statistics
            .iter()
            .find(|s| s.schema == to_stat.schema && s.name == to_stat.name);
        let to_stat =
            Self::statistic_for_emission(self.to_statistics_targets_unknown(), from_stat, to_stat);
        let mut block = to_stat.get_drop_script();
        block.push_str(&to_stat.get_script());
        Self::emit_dependent_recreate(buffers.out_for(&block), self.use_drop, &block);
    }

    /// Phase 7: restore the extended statistics that `DROP FUNCTION ...
    /// CASCADE` took with a generated column Phase 7 re-adds, beyond the
    /// FROM-side edges on the column's own table that
    /// [`Comparer::recreate_column_dependents`] walks:
    ///
    /// * statistics on the **partitions** of that table. Phase 7 re-adds the
    ///   column on the partitioned parent only (the `ADD COLUMN` propagates),
    ///   but extended statistics are not inherited: one defined on a
    ///   partition names the partition's own column, and PostgreSQL drops it
    ///   together with that column.
    /// * statistics only the **TO** side has on the column — new, or
    ///   redefined to name it. `compare_statistics` runs before this phase,
    ///   so it created them before the `CASCADE`, which then dropped them.
    ///   The TO dump's own column-dependent edges find those.
    ///
    /// Anchors are matched the way `recreate_column_dependents` matches them
    /// (case-insensitive, quotes stripped).
    fn recreate_statistics_on_readded_column(
        &self,
        table_schema: &str,
        table_name: &str,
        column_name: &str,
        emitted_keys: &mut HashSet<String>,
        buffers: &mut RecreateBuffers<'_>,
    ) {
        let normalise = |s: &str| s.to_lowercase().replace('"', "");
        let want_column = normalise(column_name);

        // The table and every partition below it, at any depth.
        let mut anchors: HashSet<(String, String)> = HashSet::new();
        anchors.insert((normalise(table_schema), normalise(table_name)));
        loop {
            let mut added = false;
            for table in &self.to.tables {
                let Some((parent_schema, parent_name)) = table
                    .partition_of
                    .as_deref()
                    .and_then(Self::split_qualified_name_quote_aware)
                else {
                    continue;
                };
                if anchors.contains(&(normalise(&parent_schema), normalise(&parent_name)))
                    && anchors.insert((normalise(&table.schema), normalise(&table.name)))
                {
                    added = true;
                }
            }
            if !added {
                break;
            }
        }

        for dep in self
            .from
            .column_dependents
            .iter()
            .chain(self.to.column_dependents.iter())
        {
            if dep.kind == ColumnDependentKind::Statistic
                && normalise(&dep.column) == want_column
                && anchors.contains(&(normalise(&dep.schema), normalise(&dep.table)))
            {
                self.recreate_column_dependent_statistic(dep, emitted_keys, buffers);
            }
        }
    }

    // Compare grants (privileges) for all objects
    async fn compare_grants(&mut self) -> Result<(), Error> {
        if self.grants_mode == GrantsMode::Ignore {
            return Ok(());
        }

        let full = self.grants_mode == GrantsMode::Full;

        // Build O(1) lookup maps from the "from" dump
        let from_schema_map: HashMap<&str, (&[String], &str)> = self
            .from
            .schemas
            .iter()
            .map(|s| (s.name.as_str(), (s.acl.as_slice(), s.owner.as_str())))
            .collect();

        let from_table_map: HashMap<(&str, &str), (&[String], &str)> = self
            .from
            .tables
            .iter()
            .map(|t| {
                (
                    (t.schema.as_str(), t.name.as_str()),
                    (t.acl.as_slice(), t.owner.as_str()),
                )
            })
            .collect();

        let mut from_seq_map: HashMap<(&str, &str), (&[String], &str)> = self
            .from
            .sequences
            .iter()
            .map(|s| {
                (
                    (s.schema.as_str(), s.name.as_str()),
                    (s.acl.as_slice(), s.owner.as_str()),
                )
            })
            .collect();
        // An identity sequence `compare_sequences` renames keeps its FROM
        // privileges under its TO name.
        for ((to_schema, to_name), (from_schema, from_name)) in &self.renamed_identity_sequences {
            if let Some(&from) = from_seq_map.get(&(from_schema.as_str(), from_name.as_str())) {
                from_seq_map.insert((to_schema.as_str(), to_name.as_str()), from);
            }
        }

        let from_view_map: HashMap<(&str, &str), (&[String], &str)> = self
            .from
            .views
            .iter()
            .map(|v| {
                (
                    (v.schema.as_str(), v.name.as_str()),
                    (v.acl.as_slice(), v.owner.as_str()),
                )
            })
            .collect();

        let from_routine_map: HashMap<(&str, &str, &str), &Routine> = self
            .from
            .routines
            .iter()
            .map(|r| {
                (
                    (r.schema.as_str(), r.name.as_str(), r.arguments.as_str()),
                    r,
                )
            })
            .collect();

        self.script
            .append_block("\n/* ---> Grants: Start section --------------- */");

        // Entries granted by a role other than the owner are revoked by naming
        // that grantor (`acl::generate_grants_scripts`). Those revokes are
        // collected here and emitted at the top of the section, ahead of every
        // owner-level statement: a grantor can revoke only while it still
        // holds the grant option, and a `SET ROLE` grantor still needs `USAGE`
        // on the object's schema. Columns go first because a table-level grant
        // option also covers the columns, and schemas last.
        let revokes_at = self.script.len();
        let mut column_revokes = String::new();
        let mut object_revokes = String::new();
        let mut schema_revokes = String::new();
        let revoke_style = if self.migrated_server_at_least(acl::GRANTED_BY_MIN_VERSION) {
            acl::RevokeStyle::GrantedBy
        } else {
            acl::RevokeStyle::SetRole
        };
        // A replayed grantor-named revoke can fail once its entry is gone, so
        // production output (whose contract is a replayable migration) guards
        // each one.
        let replay_guard = self.output_for_production;
        // What each table's statements revoke: PostgreSQL applies a
        // table-level REVOKE to every column as well, so the column diffs
        // below start from what is left.
        let mut table_revoked: HashMap<(&str, &str), Vec<acl::RevokedPrivilege>> = HashMap::new();

        // --- Schemas ---
        for schema in &self.to.schemas {
            let (from_acl, from_owner) = from_schema_map
                .get(schema.name.as_str())
                .copied()
                .unwrap_or((&[], ""));
            // Asymmetric owner filters: ALTER ... OWNER TO removes the FROM
            // owner's implicit-owner ACL entry from the FROM relation and
            // creates one for the TO owner. Pre-fix the comparer used a
            // single symmetric filter and treated the FROM owner's
            // implicit privileges as if they would persist post-migration,
            // producing non-idempotent REVOKE/GRANT pairs whenever
            // ownership changed.
            let from_owners: Vec<&str> =
                [from_owner].into_iter().filter(|o| !o.is_empty()).collect();
            let to_owners: Vec<&str> = [schema.owner.as_str()]
                .into_iter()
                .filter(|o| !o.is_empty())
                .collect();
            let owners = [from_owner, schema.owner.as_str()];
            let scripts = acl::generate_grants_scripts(
                from_acl,
                &schema.acl,
                full,
                acl::AclTarget::Object {
                    kind: "SCHEMA",
                    name: &schema.name,
                },
                &from_owners,
                &to_owners,
                Self::grantor_revokes(true, &owners, revoke_style, replay_guard),
            );
            Self::push_grants(
                &mut self.script,
                &mut schema_revokes,
                self.use_comments,
                &format!("schema: {}", schema.name),
                scripts,
            );
        }

        // --- Tables ---
        for table in &self.to.tables {
            // A new table, and one the migration drops and recreates (e.g. a
            // partition key change), start from what the table's creation
            // gives it (`created_object_acl`): in full mode, where PostgreSQL
            // may apply the FROM default privileges, what they grant and TO
            // lacks is revoked in this run rather than on the next compare.
            // A partition whose column type changes through its parent is
            // never dropped (it is in `recreated_tables`, not
            // `dropped_and_recreated_tables`) and keeps its FROM ACL, and so
            // does every table without `use_drop`, which only comments the
            // DROP TABLE out. Planning a recreated table from its old ACL took
            // TO's grants for present and never re-issued them.
            let table_key = Self::table_key(&table.schema, &table.name);
            let is_recreated =
                self.use_drop && self.dropped_and_recreated_tables.contains(&table_key);
            let object_name = format!("{}.{}", table.schema, table.name);
            let target = acl::AclTarget::Object {
                kind: "TABLE",
                name: &object_name,
            };
            let to_owners: Vec<&str> = [table.owner.as_str()]
                .into_iter()
                .filter(|o| !o.is_empty())
                .collect();
            let mut scripts = match from_table_map
                .get(&(table.schema.as_str(), table.name.as_str()))
                .copied()
            {
                Some((from_acl, from_owner)) if !is_recreated => {
                    // See the schema branch above for why `from_owners` and
                    // `to_owners` are kept separate.
                    let from_owners: Vec<&str> =
                        [from_owner].into_iter().filter(|o| !o.is_empty()).collect();
                    let owners = [from_owner, table.owner.as_str()];
                    acl::generate_grants_scripts(
                        from_acl,
                        &table.acl,
                        full,
                        target,
                        &from_owners,
                        &to_owners,
                        Self::grantor_revokes(true, &owners, revoke_style, replay_guard),
                    )
                }
                // New, or dropped and recreated.
                _ => acl::generate_created_object_grants(
                    &self.created_object_acl("r", &table.schema, "TABLE", &table.owner),
                    &table.acl,
                    full,
                    target,
                    &to_owners,
                ),
            };
            if !scripts.revoked.is_empty() {
                table_revoked.insert(
                    (table.schema.as_str(), table.name.as_str()),
                    std::mem::take(&mut scripts.revoked),
                );
            }
            Self::push_grants(
                &mut self.script,
                &mut object_revokes,
                self.use_comments,
                &format!("table: {object_name}"),
                scripts,
            );
        }

        // --- Sequences ---
        for seq in &self.to.sequences {
            // A new sequence starts from what its creation gives it, like a
            // new table.
            let object_name = format!("{}.{}", seq.schema, seq.name);
            let target = acl::AclTarget::Object {
                kind: "SEQUENCE",
                name: &object_name,
            };
            let to_owners: Vec<&str> = [seq.owner.as_str()]
                .into_iter()
                .filter(|o| !o.is_empty())
                .collect();
            let scripts = match from_seq_map
                .get(&(seq.schema.as_str(), seq.name.as_str()))
                .copied()
            {
                Some((from_acl, from_owner)) => {
                    let from_owners: Vec<&str> =
                        [from_owner].into_iter().filter(|o| !o.is_empty()).collect();
                    let owners = [from_owner, seq.owner.as_str()];
                    acl::generate_grants_scripts(
                        from_acl,
                        &seq.acl,
                        full,
                        target,
                        &from_owners,
                        &to_owners,
                        Self::grantor_revokes(true, &owners, revoke_style, replay_guard),
                    )
                }
                None => acl::generate_created_object_grants(
                    &self.created_object_acl("S", &seq.schema, "SEQUENCE", &seq.owner),
                    &seq.acl,
                    full,
                    target,
                    &to_owners,
                ),
            };
            Self::push_grants(
                &mut self.script,
                &mut object_revokes,
                self.use_comments,
                &format!("sequence: {object_name}"),
                scripts,
            );
        }

        // --- Views ---
        let from_views_by_id: HashMap<(&str, &str), &View> = self
            .from
            .views
            .iter()
            .map(|v| ((v.schema.as_str(), v.name.as_str()), v))
            .collect();
        // What the views `recreate_routine_dependents` re-created may have
        // been taken by, as `View::routine_dependencies` records it.
        let cascade_dropped_keys: HashSet<String> = if self.views_recreated_by_cascade.is_empty() {
            HashSet::new()
        } else {
            self.cascade_dropped_routines()
                .into_iter()
                .map(Routine::dependency_key)
                .collect()
        };
        for view in &self.to.views {
            let normalized_key = Self::normalized_view_key(&view.schema, &view.name);
            // A view actively dropped earlier in the script (e.g. as a
            // dependency of an altered table) loses its ACL with the DROP and
            // is created again, like a new view: both start from what the
            // creation gives them (`created_object_acl`; views share the
            // tables' `pg_default_acl.defaclobjtype = 'r'`). When the drop was
            // only commented out (use_drop=false), the view still exists and
            // keeps its FROM ACL.
            let from_view = from_view_map
                .get(&(view.schema.as_str(), view.name.as_str()))
                .copied();
            let is_new_view = from_view.is_none();
            // A view `recreate_routine_dependents` re-created after a
            // routine's `DROP … CASCADE` took it has lost its privileges and
            // starts afresh, like a dropped view. It is matched by the
            // routine's name, though, so an overload the view does not call
            // can stand in for the routine, and then the view is still there
            // with its FROM privileges: the routines its `_RETURN` rule calls
            // tell which (`View::routine_dependencies`).
            let cascade_taken = if self.use_drop
                && !is_new_view
                && self.dropped_views.get(&normalized_key) != Some(&true)
                && self.views_recreated_by_cascade.contains(&normalized_key)
            {
                from_views_by_id
                    .get(&(view.schema.as_str(), view.name.as_str()))
                    .map_or(Some(false), |from_view| {
                        from_view
                            .routine_dependencies
                            .as_ref()
                            .map(|deps| deps.iter().any(|key| cascade_dropped_keys.contains(key)))
                    })
            } else {
                Some(false)
            };
            let is_dropped = self.dropped_views.get(&normalized_key) == Some(&true)
                || cascade_taken == Some(true);
            let object_name = format!("{}.{}", view.schema, view.name);
            let target = acl::AclTarget::Object {
                kind: "TABLE",
                name: &object_name,
            };
            let to_owners: Vec<&str> = [view.owner.as_str()]
                .into_iter()
                .filter(|o| !o.is_empty())
                .collect();
            let scripts = match from_view {
                // A dump that predates them cannot tell: the statements reach
                // TO from either state, created or kept, and the revokes
                // naming a grantor only run while the entry they remove is
                // still there.
                Some((from_acl, from_owner)) if cascade_taken.is_none() => {
                    let from_owners: Vec<&str> =
                        [from_owner].into_iter().filter(|o| !o.is_empty()).collect();
                    let owners = [from_owner, view.owner.as_str()];
                    let grantor_revokes = acl::generate_grants_scripts(
                        from_acl,
                        &view.acl,
                        full,
                        target,
                        &from_owners,
                        &to_owners,
                        Self::grantor_revokes(true, &owners, revoke_style, true),
                    )
                    .grantor_revokes;
                    let created = self.created_object_acl("r", &view.schema, "TABLE", &view.owner);
                    let mut states = created.states(&to_owners).to_vec();
                    states.push((from_acl, from_owners.as_slice()));
                    acl::GrantsScripts {
                        grantor_revokes,
                        owner_level: acl::generate_grants_script_from_any(
                            &states, &view.acl, full, target, &to_owners,
                        ),
                        ..acl::GrantsScripts::default()
                    }
                }
                Some((from_acl, from_owner)) if !is_dropped => {
                    let from_owners: Vec<&str> =
                        [from_owner].into_iter().filter(|o| !o.is_empty()).collect();
                    let owners = [from_owner, view.owner.as_str()];
                    acl::generate_grants_scripts(
                        from_acl,
                        &view.acl,
                        full,
                        target,
                        &from_owners,
                        &to_owners,
                        Self::grantor_revokes(true, &owners, revoke_style, replay_guard),
                    )
                }
                // New, dropped, or taken by a routine's CASCADE.
                _ => acl::generate_created_object_grants(
                    &self.created_object_acl("r", &view.schema, "TABLE", &view.owner),
                    &view.acl,
                    full,
                    target,
                    &to_owners,
                ),
            };
            Self::push_grants(
                &mut self.script,
                &mut object_revokes,
                self.use_comments,
                &format!("view: {object_name}"),
                scripts,
            );
        }

        // --- Foreign Tables ---
        let from_ft_map: HashMap<(&str, &str), (&[String], &str)> = self
            .from
            .foreign_tables
            .iter()
            .map(|ft| {
                (
                    (ft.schema.as_str(), ft.name.as_str()),
                    (ft.acl.as_slice(), ft.owner.as_str()),
                )
            })
            .collect();

        for ft in &self.to.foreign_tables {
            // Same treatment as regular tables: foreign tables share
            // `pg_default_acl.defaclobjtype = 'r'`, so a new foreign table
            // starts from what its creation gives it, and in full mode what
            // the FROM default privileges may grant and TO lacks is revoked
            // in this run rather than on the next compare (non-idempotent
            // diff).
            //
            // PostgreSQL `GRANT` only accepts `ON TABLE`; there is no
            // `ON FOREIGN TABLE` form. Foreign tables share the privilege
            // set with regular tables (handled in `valid_privileges` via
            // the `"TABLE" | "FOREIGN TABLE"` arm), so passing "TABLE"
            // here yields valid SQL while preserving the full privilege
            // vocabulary including PG17's MAINTAIN.
            let object_name = format!("{}.{}", ft.schema, ft.name);
            let target = acl::AclTarget::Object {
                kind: "TABLE",
                name: &object_name,
            };
            let to_owners: Vec<&str> = [ft.owner.as_str()]
                .into_iter()
                .filter(|o| !o.is_empty())
                .collect();
            // A foreign table dropped and created again starts from the
            // default privileges, like a new one.
            let recreated = self
                .recreated_foreign_tables
                .contains(&(ft.schema.clone(), ft.name.clone()));
            let scripts = match from_ft_map
                .get(&(ft.schema.as_str(), ft.name.as_str()))
                .copied()
            {
                Some((from_acl, from_owner)) if !recreated => {
                    let from_owners: Vec<&str> =
                        [from_owner].into_iter().filter(|o| !o.is_empty()).collect();
                    let owners = [from_owner, ft.owner.as_str()];
                    acl::generate_grants_scripts(
                        from_acl,
                        &ft.acl,
                        full,
                        target,
                        &from_owners,
                        &to_owners,
                        Self::grantor_revokes(true, &owners, revoke_style, replay_guard),
                    )
                }
                _ => acl::generate_created_object_grants(
                    &self.created_object_acl("r", &ft.schema, "TABLE", &ft.owner),
                    &ft.acl,
                    full,
                    target,
                    &to_owners,
                ),
            };
            Self::push_grants(
                &mut self.script,
                &mut object_revokes,
                self.use_comments,
                &format!("foreign table: {object_name}"),
                scripts,
            );
            // Column privileges are not compared for a foreign table, but
            // one the migration creates — new, or dropped and created again
            // — starts without any, so it gets TO's.
            if recreated || !from_ft_map.contains_key(&(ft.schema.as_str(), ft.name.as_str())) {
                for column in ft.columns.iter().filter(|c| !c.acl.is_empty()) {
                    let owner_level = acl::generate_column_grants_script(
                        &[],
                        &column.acl,
                        full,
                        &object_name,
                        &column.name,
                        &[],
                        &to_owners,
                    );
                    Self::push_grants(
                        &mut self.script,
                        &mut object_revokes,
                        self.use_comments,
                        &format!("foreign table column: {object_name}.{}", column.name),
                        acl::GrantsScripts {
                            owner_level,
                            ..acl::GrantsScripts::default()
                        },
                    );
                }
            }
        }

        // --- Foreign servers ---
        // Their privileges are not compared, but a server the migration
        // creates — new, or dropped and created again because its wrapper
        // changes (see `plan_server_recreates`) — starts with the owner's
        // alone, so it gets TO's.
        let from_servers: HashSet<&str> = self
            .from
            .foreign_servers
            .iter()
            .map(|srv| srv.name.as_str())
            .collect();
        for srv in &self.to.foreign_servers {
            if srv.acl.is_empty()
                || (from_servers.contains(srv.name.as_str())
                    && !self.recreated_servers.contains(&srv.name))
            {
                continue;
            }
            let to_owners: Vec<&str> = [srv.owner.as_str()]
                .into_iter()
                .filter(|o| !o.is_empty())
                .collect();
            let scripts = acl::generate_created_object_grants(
                &acl::CreatedAcl::default(),
                &srv.acl,
                full,
                acl::AclTarget::Object {
                    kind: "FOREIGN SERVER",
                    name: &srv.name,
                },
                &to_owners,
            );
            Self::push_grants(
                &mut self.script,
                &mut object_revokes,
                self.use_comments,
                &format!("foreign server: {}", srv.name),
                scripts,
            );
        }

        // --- Routines ---
        for routine in &self.to.routines {
            let from_routine = from_routine_map
                .get(&(
                    routine.schema.as_str(),
                    routine.name.as_str(),
                    routine.arguments.as_str(),
                ))
                .copied();
            // A routine the migration drops and recreates (a return type or
            // kind change, a pre-drop, or another routine's CASCADE) has lost
            // its FROM privileges by the time these statements run, and starts
            // over like a new one (`created_object_acl`). When the drop is
            // only commented out (use_drop = false), the routine keeps them.
            let routine_key = (
                routine.schema.clone(),
                routine.name.clone(),
                routine.arguments.clone(),
            );
            let predropped = self.predropped_routines.contains(&routine_key)
                || self.cascade_victim_routines.contains(&routine_key);
            let object_kind = match routine.kind.as_str() {
                "procedure" => "PROCEDURE",
                _ => "FUNCTION",
            };
            // A NULL ACL is compared as the default it stands for (PUBLIC's
            // EXECUTE), so a REVOKE ... FROM PUBLIC on either side shows.
            let to_acl = acl::effective_acl(
                &routine.acl,
                routine.acl_explicitly_empty,
                object_kind,
                &routine.owner,
            );
            let object_name = format!("{}.{}({})", routine.schema, routine.name, routine.arguments);
            let target = acl::AclTarget::Object {
                kind: object_kind,
                name: &object_name,
            };
            let to_owners: Vec<&str> = [routine.owner.as_str()]
                .into_iter()
                .filter(|o| !o.is_empty())
                .collect();
            let scripts = match from_routine {
                Some(from_routine)
                    if !(self.use_drop
                        && (predropped || Self::routine_is_recreated(from_routine, routine))) =>
                {
                    let from_acl = acl::effective_acl(
                        &from_routine.acl,
                        from_routine.acl_explicitly_empty,
                        object_kind,
                        &from_routine.owner,
                    );
                    let from_owners: Vec<&str> = [from_routine.owner.as_str()]
                        .into_iter()
                        .filter(|o| !o.is_empty())
                        .collect();
                    let owners = [from_routine.owner.as_str(), routine.owner.as_str()];
                    acl::generate_grants_scripts(
                        &from_acl,
                        &to_acl,
                        full,
                        target,
                        &from_owners,
                        &to_owners,
                        Self::grantor_revokes(true, &owners, revoke_style, replay_guard),
                    )
                }
                // New, or dropped and recreated.
                _ => acl::generate_created_object_grants(
                    &self.created_object_acl("f", &routine.schema, object_kind, &routine.owner),
                    &to_acl,
                    full,
                    target,
                    &to_owners,
                ),
            };
            Self::push_grants(
                &mut self.script,
                &mut object_revokes,
                self.use_comments,
                &format!("{}: {}", routine.kind, object_name),
                scripts,
            );
        }

        // --- Types ---
        let from_type_map: HashMap<(&str, &str), &crate::dump::pg_type::PgType> = self
            .from
            .types
            .iter()
            .map(|t| ((t.schema.as_str(), t.typname.as_str()), t))
            .collect();

        for pg_type in &self.to.types {
            // A multirange has no privileges of its own — PostgreSQL checks
            // its range type's — and refuses any GRANT or REVOKE on it
            // ("cannot set privileges of multirange types").
            if (pg_type.typtype as u8 as char) == 'm' {
                continue;
            }
            // A NULL ACL is compared as the default it stands for (PUBLIC's
            // USAGE), as for routines.
            let to_acl = acl::effective_acl(
                &pg_type.acl,
                pg_type.acl_explicitly_empty,
                "TYPE",
                &pg_type.owner,
            );
            let object_name = format!("{}.{}", pg_type.schema, pg_type.typname);
            let target = acl::AclTarget::Object {
                kind: "TYPE",
                name: &object_name,
            };
            let to_owners: Vec<&str> = [pg_type.owner.as_str()]
                .into_iter()
                .filter(|o| !o.is_empty())
                .collect();
            let from_type = from_type_map
                .get(&(pg_type.schema.as_str(), pg_type.typname.as_str()))
                .copied();
            let scripts = match from_type {
                // `compare_types` drops and recreates a changed range type,
                // which then starts over like a new one; without `use_drop`
                // the drop is commented out and the type keeps its ACL.
                Some(from_type)
                    if !(self.use_drop
                        && Self::type_definition_differs(from_type, pg_type)
                        && from_type.alter_recreates(pg_type)) =>
                {
                    let from_acl = acl::effective_acl(
                        &from_type.acl,
                        from_type.acl_explicitly_empty,
                        "TYPE",
                        &from_type.owner,
                    );
                    let from_owners: Vec<&str> = [from_type.owner.as_str()]
                        .into_iter()
                        .filter(|o| !o.is_empty())
                        .collect();
                    let owners = [from_type.owner.as_str(), pg_type.owner.as_str()];
                    acl::generate_grants_scripts(
                        &from_acl,
                        &to_acl,
                        full,
                        target,
                        &from_owners,
                        &to_owners,
                        Self::grantor_revokes(true, &owners, revoke_style, replay_guard),
                    )
                }
                // New, or dropped and recreated.
                _ => acl::generate_created_object_grants(
                    &self.created_object_acl("T", &pg_type.schema, "TYPE", &pg_type.owner),
                    &to_acl,
                    full,
                    target,
                    &to_owners,
                ),
            };
            Self::push_grants(
                &mut self.script,
                &mut object_revokes,
                self.use_comments,
                &format!("type: {object_name}"),
                scripts,
            );
        }

        // --- Column-level ACLs ---
        // Build from-table column map for O(1) lookup
        let from_table_cols: std::collections::HashMap<
            (&str, &str),
            &[crate::dump::table_column::TableColumn],
        > = self
            .from
            .tables
            .iter()
            .map(|t| ((t.schema.as_str(), t.name.as_str()), t.columns.as_slice()))
            .collect();

        for table in &self.to.tables {
            // Column ACLs are stored in `pg_attribute.attacl`, which —
            // unlike `pg_class.relacl` — is NOT touched by `ALTER TABLE
            // OWNER TO`. Explicit column grants to the former owner
            // persist after the owner change and must be revoked under
            // full mode. So we pass an empty `from_owners` to the column
            // helper (no implicit-owner entry to strip) but keep the
            // `to_owners` filter so a redundant explicit column grant to
            // the new owner is not emitted (the new owner already has
            // implicit access via table ownership).
            let to_table_owners: Vec<&str> = [table.owner.as_str()]
                .into_iter()
                .filter(|o| !o.is_empty())
                .collect();
            let object_table_name = format!("{}.{}", table.schema, table.name);
            let from_cols = from_table_cols
                .get(&(table.schema.as_str(), table.name.as_str()))
                .copied()
                .unwrap_or(&[]);

            // Index from-columns by name once per table so the per-column
            // lookup below is O(1) instead of an O(C²) linear scan.
            let from_col_acls: HashMap<&str, &[String]> = from_cols
                .iter()
                .map(|c| (c.name.as_str(), c.acl.as_slice()))
                .collect();

            // Grantors, unlike grantees, are classified against both owners:
            // `ALTER TABLE ... OWNER TO` rewrites the old owner to the new
            // one in `attacl` too (verified on 14 and 19), so a plain REVOKE
            // reaches the column grants either owner made. A table-level
            // grant option also lets a role grant a column privilege, hence
            // the parent ACL, and a table-level REVOKE also strips the
            // columns, hence the table's revokes.
            let (from_table_acl, from_table_owner) = from_table_map
                .get(&(table.schema.as_str(), table.name.as_str()))
                .copied()
                .unwrap_or((&[], ""));
            let table_owners = [from_table_owner, table.owner.as_str()];
            let parent_revokes = table_revoked
                .get(&(table.schema.as_str(), table.name.as_str()))
                .map_or(&[][..], Vec::as_slice);
            let column_grantors =
                Self::grantor_revokes(true, &table_owners, revoke_style, replay_guard).map(|g| {
                    acl::GrantorRevokes {
                        parent_acl: from_table_acl,
                        parent_revokes,
                        ..g
                    }
                });
            // A column the migration creates anew — every column of a table
            // whose DROP TABLE runs, and each one the table's alter drops and
            // re-adds — starts with an empty `attacl` (default privileges
            // never reach columns). Its privileges are planned from nothing:
            // what TO has is granted again, and no FROM entry, gone with the
            // old column, is revoked (in a grantor's name that would fail).
            // Without `use_drop` the drops are commented out and the FROM
            // column grants stay.
            let table_dropped = self.use_drop
                && self
                    .dropped_and_recreated_tables
                    .contains(&Self::table_key(&table.schema, &table.name));

            for col in &table.columns {
                let column_created = table_dropped
                    || self.readded_columns.contains(&(
                        table.schema.clone(),
                        table.name.clone(),
                        col.name.clone(),
                    ));
                let from_col_acl: &[String] = if column_created {
                    &[]
                } else {
                    from_col_acls.get(col.name.as_str()).copied().unwrap_or(&[])
                };
                if col.acl.is_empty() && from_col_acl.is_empty() {
                    continue;
                }
                let scripts = acl::generate_grants_scripts(
                    from_col_acl,
                    &col.acl,
                    full,
                    acl::AclTarget::Column {
                        table: &object_table_name,
                        column: &col.name,
                    },
                    &[],
                    &to_table_owners,
                    column_grantors.filter(|_| !column_created),
                );
                if !scripts.grantor_revokes.is_empty() {
                    if self.use_comments {
                        column_revokes.push_str(&format!(
                            "/* Grantor revokes for column: {}.{} */\n",
                            block_comment_safe(&object_table_name),
                            block_comment_safe(&col.name)
                        ));
                    }
                    column_revokes.push_str(&scripts.grantor_revokes);
                }
                if !scripts.owner_level.is_empty() {
                    if self.use_comments {
                        self.script.push_str(
                            format!(
                                "/* Column grants for {}.{} */\n",
                                block_comment_safe(&object_table_name),
                                block_comment_safe(&col.name)
                            )
                            .as_str(),
                        );
                    }
                    self.script.push_str(&scripts.owner_level);
                }
            }
        }

        let mut revokes = column_revokes;
        revokes.push_str(&object_revokes);
        revokes.push_str(&schema_revokes);
        self.script.insert_str(revokes_at, &revokes);

        self.script
            .append_block("\n/* ---> Grants: End section --------------- */");
        Ok(())
    }

    /// The grantor-aware revoke settings for one object whose FROM and TO
    /// owners are `owners`, or `None` — the plain grantor-insensitive script —
    /// unless the FROM ACL being diffed is the object's catalog ACL
    /// (`catalog_acl`; not the default privileges standing in for a new or
    /// recreated object's) and its FROM owner is known. `replay_guard` is set
    /// for production output (see [`acl::GrantorRevokes::replay_guard`]).
    fn grantor_revokes<'a>(
        catalog_acl: bool,
        owners: &'a [&'a str; 2],
        style: acl::RevokeStyle,
        replay_guard: bool,
    ) -> Option<acl::GrantorRevokes<'a>> {
        (catalog_acl && !owners[0].is_empty()).then_some(acl::GrantorRevokes {
            owners: owners.as_slice(),
            style,
            parent_acl: &[],
            parent_revokes: &[],
            replay_guard,
        })
    }

    /// Route one object's [`acl::GrantsScripts`]: the owner-level statements
    /// to `script`, where the object's grants have always gone, and the
    /// grantor-qualified revokes to `revokes`, which `compare_grants` emits at
    /// the top of the grants section. `what` names the object in the optional
    /// comments (`table: s.t`).
    fn push_grants(
        script: &mut String,
        revokes: &mut String,
        use_comments: bool,
        what: &str,
        scripts: acl::GrantsScripts,
    ) {
        if !scripts.grantor_revokes.is_empty() {
            if use_comments {
                let what = block_comment_safe(what);
                revokes.push_str(&format!("/* Grantor revokes for {what} */\n"));
            }
            revokes.push_str(&scripts.grantor_revokes);
        }
        if !scripts.owner_level.is_empty() {
            if use_comments {
                let what = block_comment_safe(what);
                script.push_str(&format!("/* Grants for {what} */\n"));
            }
            script.push_str(&scripts.owner_level);
        }
    }
}

/// `drop constraint if exists` + `add constraint` block from the TO-side
/// constraint, used to restore a CHECK/EXCLUDE constraint that PostgreSQL
/// removed when CASCADE-dropping a referenced function.
fn constraint_recreate_block(constraint: &TableConstraint) -> String {
    let mut block = format!(
        "alter table {}.{} drop constraint if exists {};",
        constraint.schema, constraint.table_name, constraint.name
    )
    .with_empty_lines();
    block.push_str(&constraint.get_script());
    block
}

/// What `Comparer::column_drop_blockers` drops ahead of the table alters.
#[derive(Debug, Default)]
struct ColumnDropBlockers {
    /// The `drop … if exists` statements for the columns dropped and re-added,
    /// in the FROM graph's order.
    script: String,
    /// The `drop … if exists` statements of the policies and triggers in the
    /// way of an in-place retype, in the FROM graph's order.
    retype_script: String,
    /// Foreign keys dropped, by name, keyed by their FROM table `(schema, name)`.
    foreign_keys: HashMap<(String, String), HashSet<String>>,
    /// Policies dropped ahead of a retype, by name, keyed by their FROM table
    /// `(schema, name)`, which `compare_tables` creates from TO after every
    /// table alter (see `Comparer::column_drop_blockers`).
    policies: HashMap<(String, String), HashSet<String>>,
    /// Triggers dropped, as `table_key` + `.` + trigger name.
    triggers: HashSet<String>,
    /// The columns retyped in place, as lowercased, quote-stripped
    /// `(schema, table, column)` (see `Table::columns_retyped_in_place`).
    retyped: HashSet<(String, String, String)>,
}

/// A generated column that a routine's `DROP ... CASCADE` takes and
/// `Comparer::recreate_routine_dependents` re-adds from TO (see
/// `Comparer::cascade_readded_columns`), spelled as the TO dump spells it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct CascadeReadd {
    schema: String,
    table: String,
    column: String,
    /// A partition's copy of a column re-added on a partitioned ancestor:
    /// the ancestor's `ADD COLUMN` re-creates it, and nothing else restores
    /// what the partition had on it.
    in_partition: bool,
}

impl CascadeReadd {
    /// `schema.table.column`, normalized like `View::column_relation` and
    /// `RelationChanges::changed_columns` entries.
    fn key(&self) -> String {
        Comparer::normalized_view_reference(&format!(
            "{}.{}.{}",
            self.schema, self.table, self.column
        ))
    }
}

/// `CREATE INDEX IF NOT EXISTS` block built from the TO-side index.
/// We deliberately *do not* `DROP INDEX` first: Phase 7's text-based
/// matching cannot tell which overload of an `(schema, name)` routine
/// a dependent actually called, and the TO-side gate cannot detect
/// that case either, so the matcher may false-positive on an index
/// that PostgreSQL never actually CASCADE-dropped. An unconditional
/// drop in that case would silently invalidate a perfectly good index.
/// `IF NOT EXISTS` makes the recreate a no-op when the index survived
/// CASCADE and a real create when it did not.
/// Output buffers for dependent-recreation emission: the in-transaction recreate
/// block and the production post-commit statements (per-partition concurrent index
/// builds and ATTACH PARTITION) that belong with it.
struct RecreateBuffers<'a> {
    out: &'a mut String,
    post_commit: &'a mut String,
    /// Where a block calling one of some routines goes instead of `out`,
    /// when those routines are only created after `out` runs (see
    /// [`LateRecreate`]). `None` sends everything to `out`.
    late: Option<LateRecreate<'a>>,
}

/// The second in-transaction buffer of a [`RecreateBuffers`]: the blocks that
/// call one of `routines` — as `Comparer::definition_references_any` reads
/// their SQL — go to `out`, which the caller emits once those routines exist.
/// `recreate_routine_dependents` uses it for the routines that read a
/// generated column it re-adds, which `compare_routines_and_views` creates
/// only after that re-add.
struct LateRecreate<'a> {
    out: &'a mut String,
    routines: &'a HashSet<(String, String)>,
}

impl<'a> RecreateBuffers<'a> {
    /// Buffers without a late part: every block goes to `out`.
    fn new(out: &'a mut String, post_commit: &'a mut String) -> Self {
        Self {
            out,
            post_commit,
            late: None,
        }
    }

    /// The in-transaction buffer and the post-commit buffer for a block
    /// whose SQL (or, for an index, whose definition) is `text`: the late
    /// buffer when `text` calls one of its routines, `out` otherwise.
    fn targets(&mut self, text: &str) -> (&mut String, &mut String) {
        match self.late.as_mut() {
            Some(late) if Comparer::definition_references_any(text, late.routines) => {
                (&mut *late.out, &mut *self.post_commit)
            }
            _ => (&mut *self.out, &mut *self.post_commit),
        }
    }

    /// The in-transaction buffer for the block `text` (see
    /// [`RecreateBuffers::targets`]).
    fn out_for(&mut self, text: &str) -> &mut String {
        self.targets(text).0
    }
}

/// What the migration drops, rebuilds or retypes underneath the FROM objects
/// that depend on relations and routines, as `Comparer::relation_changes`
/// works it out before any of it runs. Relation and column keys are in
/// `Comparer::normalized_view_reference` form (lowercased, quotes stripped:
/// `schema.relation` / `schema.relation.column`), routine keys are
/// [`Routine::dependency_key`]s and operator keys `Operator::key`s.
///
/// Views read the four table fields only. Routines with a SQL-standard body
/// and rules — both hard `pg_depend` dependents, refused a `DROP` or retype
/// underneath them and taken along by a `DROP ... CASCADE` — read the rest
/// too, mostly through `RelationChanges::relation_goes` and
/// `RelationChanges::column_changes`.
struct RelationChanges {
    /// Tables that stop existing in the form a reader names them: dropped
    /// (with `use_drop`), or dropped and recreated by the alter path, along
    /// with every partition such a recreate takes (see `RecreatedFamilies`).
    wholesale_tables: HashSet<String>,
    /// `schema.table.column` of every table column dropped, retyped, or
    /// dropped and re-added — what PostgreSQL refuses while a dependent
    /// exists.
    changed_columns: HashSet<String>,
    /// Every table touched at all: the table-level answer kept for views
    /// from dumps without `View::column_relation`.
    altered_or_dropped: HashSet<String>,
    /// The historical regular-view answer (`related_views` of a touched
    /// table's columns), kept for the same dumps.
    legacy_dependent: HashSet<String>,
    /// Sequences only FROM has.
    dropped_sequences: HashSet<String>,
    /// Foreign tables `compare_foreign_tables` drops: FROM-only ones, and ones
    /// whose server changes, which it drops and recreates — both only with
    /// `use_drop`, since otherwise those drops are commented out.
    going_foreign_tables: HashSet<String>,
    /// `schema.table.column` of every foreign-table column dropped or retyped.
    changed_foreign_columns: HashSet<String>,
    /// Routines only FROM has; the routine phase drops them with `CASCADE`.
    dropped_routines: HashSet<String>,
    /// Routines the routine phase drops (`CASCADE`) and re-creates, see
    /// `Comparer::routine_is_recreated`.
    recreated_routines: HashSet<String>,
    /// Operators dropped and re-created because their definition changed.
    rebuilt_operators: HashSet<String>,
}

impl RelationChanges {
    /// Whether the relation `relation` (`schema.name` as a `pg_depend`
    /// dependency list spells it) is dropped, or dropped and recreated: a
    /// table, a sequence or a foreign table.
    fn relation_goes(&self, relation: &str) -> bool {
        let key = Comparer::normalized_view_reference(relation);
        self.wholesale_tables.contains(&key)
            || self.dropped_sequences.contains(&key)
            || self.going_foreign_tables.contains(&key)
    }

    /// Whether the column `column` (`schema.relation.column`) of a table or a
    /// foreign table is dropped or retyped, or dropped and re-added.
    fn column_changes(&self, column: &str) -> bool {
        let key = Comparer::normalized_view_reference(column);
        self.changed_columns.contains(&key) || self.changed_foreign_columns.contains(&key)
    }
}

/// The FROM tables the table phase drops with `DROP TABLE` and creates again
/// from TO, as `Comparer::recreated_families` works them out before any of it
/// runs. Every key is a `Comparer::table_key` of a FROM table.
///
/// A root's own alter drops and recreates it: a partition-key change, or a
/// column type change it cannot make in place. The `DROP TABLE` of a
/// partitioned root takes every partition along, at every depth: those are
/// its members. `compare_tables` creates each member TO keeps again from TO,
/// whether or not its own definition changed (FROM-only ones are dropped by
/// its first pass anyway), and records roots and those members in
/// `recreated_tables` and `dropped_and_recreated_tables`, from which the
/// later passes restore what went with them.
#[derive(Debug, Default)]
struct RecreatedFamilies {
    /// Tables whose own alter runs the `DROP TABLE`.
    roots: HashSet<String>,
    /// Partitions that go with a root's `DROP TABLE`, each mapped to that
    /// root.
    members: HashMap<String, String>,
    /// The FROM partition ancestors of the roots that are partitions, each
    /// mapped to the roots below it. The root's `DROP TABLE` leaves them in
    /// place, but a foreign key referencing one references each of its
    /// partitions too (PostgreSQL keeps a derived `pg_constraint` row per
    /// referenced partition), and so blocks the drop.
    root_ancestors: HashMap<String, HashSet<String>>,
}

impl RecreatedFamilies {
    /// The roots whose `DROP TABLE` a foreign key referencing `key` blocks:
    /// the one `key` goes with, and those below it when it is a partition
    /// ancestor of roots.
    fn roots_blocked_by_reference_to<'a>(&'a self, key: &str) -> impl Iterator<Item = &'a str> {
        self.root_of(key).into_iter().chain(
            self.root_ancestors
                .get(key)
                .into_iter()
                .flatten()
                .map(String::as_str),
        )
    }

    /// The root whose `DROP TABLE` takes `key` along — `key` itself for a
    /// root — or `None` when no recreate drops it.
    fn root_of(&self, key: &str) -> Option<&str> {
        self.roots
            .get(key)
            .or_else(|| self.members.get(key))
            .map(String::as_str)
    }

    /// Whether no table is recreated at all.
    fn is_empty(&self) -> bool {
        self.roots.is_empty()
    }
}

/// FROM tables by the unquoted names `Comparer::parse_fk_referenced_table`
/// resolves a foreign key's reference to, for
/// `Comparer::fk_referenced_candidates`.
struct FromTablesUnquoted<'a> {
    /// By `(schema, name)`.
    by_key: HashMap<(String, String), &'a Table>,
    /// By name, in every schema.
    by_name: HashMap<String, Vec<&'a Table>>,
}

impl<'a> FromTablesUnquoted<'a> {
    fn new(tables: &'a [Table]) -> Self {
        let unquoted = |s: &str| s.trim_matches('"').to_string();
        let mut by_key = HashMap::new();
        let mut by_name: HashMap<String, Vec<&'a Table>> = HashMap::new();
        for table in tables {
            by_key.insert((unquoted(&table.schema), unquoted(&table.name)), table);
            by_name
                .entry(unquoted(&table.name))
                .or_default()
                .push(table);
        }
        FromTablesUnquoted { by_key, by_name }
    }
}

/// One step of the table alter loop of `Comparer::compare_tables`, which
/// takes them in the order `Comparer::late_table_keys` makes safe.
enum TableStep<'a> {
    /// A FROM table TO keeps: altered, or created again from TO when a
    /// recreate drops it.
    Existing(&'a Table),
    /// A TO-only partition whose parent is created again (or is itself such
    /// a partition), created once that parent is there.
    New(&'a Table),
}

/// One object `Comparer::drop_views` drops, as `Comparer::plan_view_drops`
/// lays them out.
enum ViewPhaseDrop {
    /// A FROM view: its index into `from.views`, its normalized key, and
    /// whether the drop is active (`use_drop`) rather than commented out.
    View {
        index: usize,
        key: String,
        active: bool,
    },
    /// A FROM routine with a SQL-standard body, as an index into
    /// `from.routines`.
    Routine(usize),
    /// A FROM routine, as an index into `from.routines`, that is in the way
    /// of the migration but is *not* dropped ahead of it: its `DROP …
    /// CASCADE` would take along `reason` (or what a routine depending on it
    /// is used by), which pgc cannot put back. `drop_views` only says so; the
    /// statement that needs the routine gone then fails on PostgreSQL's
    /// dependency check, as it did before pgc dropped such routines at all.
    KeptRoutine { index: usize, reason: String },
}

/// When a routine's `DROP … CASCADE` runs, which decides what is in place
/// for it to take (see `Comparer::cascade_blockers`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CascadeMoment {
    /// In `drop_views`, ahead of the relation DDL: the FROM dump's objects.
    AheadOfTables,
    /// In the routine phase, after the tables, types and foreign-data
    /// wrappers are the TO dump's.
    RoutinePhase,
}

/// Why `Comparer::routines_to_keep_back` leaves a routine where it is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum KeepBackCause {
    /// Its `CASCADE` would take something pgc cannot re-create.
    Unrestorable,
    /// The table changes need it in place (`Comparer::table_phase_needs`).
    NeededByTables,
}

impl KeepBackCause {
    /// The reason `drop_views` gives for keeping a routine back, `what`
    /// being what needs it.
    fn reason(self, what: &str) -> String {
        match self {
            KeepBackCause::Unrestorable => format!(
                "{what} depends on it, which a DROP ... CASCADE would take along and pgc cannot re-create"
            ),
            KeepBackCause::NeededByTables => {
                format!("{what} needs it in place while the tables change")
            }
        }
    }
}

/// The dumps' own tables and foreign tables, while
/// `Comparer::enter_table_phase_view` has the table phase's in their place.
struct TablePhaseOriginals {
    from_tables: Vec<Table>,
    to_tables: Vec<Table>,
    from_foreign_tables: Vec<ForeignTable>,
    to_foreign_tables: Vec<ForeignTable>,
}

/// A rule's identity: `(schema, table, rule)`, `quote_ident`-ed as dumped.
type RuleKey = (String, String, String);

fn index_recreate_block(index: &TableIndex) -> String {
    inject_if_not_exists_into_create_index(&index.get_script())
}

/// `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` block — used to restore a
/// generated column that PostgreSQL removed when CASCADE-dropping a
/// referenced function. Crucially we do NOT emit `DROP COLUMN IF
/// EXISTS` first: that would cascade-destroy every index, FK, and
/// CHECK constraint attached to the column, and Phase 7 has no way to
/// restore those secondary dependents. Because Phase 7's affected-set
/// keys on `(schema, name)` only and overloaded routines collapse
/// together, the matcher can false-positive on a column referencing a
/// completely unrelated overload that survived CASCADE — using
/// `IF NOT EXISTS` makes that case a no-op rather than a destructive
/// drop, and still does the right thing when the column was actually
/// removed.
fn column_recreate_block(column: &TableColumn) -> String {
    inject_if_not_exists_into_add_column(&column.get_add_script())
}

/// Rewrites `CREATE [UNIQUE] INDEX <name>` to
/// `CREATE [UNIQUE] INDEX IF NOT EXISTS <name>`. PostgreSQL's
/// `pg_get_indexdef` always uses uppercase keywords, so a literal
/// prefix match suffices; if neither prefix is present the script is
/// returned unchanged rather than corrupted.
fn inject_if_not_exists_into_create_index(script: &str) -> String {
    if let Some(rest) = script.strip_prefix("CREATE UNIQUE INDEX ") {
        return format!("CREATE UNIQUE INDEX IF NOT EXISTS {}", rest);
    }
    if let Some(rest) = script.strip_prefix("CREATE INDEX ") {
        return format!("CREATE INDEX IF NOT EXISTS {}", rest);
    }
    script.to_string()
}

/// Rewrites the `add column ` clause produced by `TableColumn::get_add_script`
/// into `add column if not exists `. The clause is generated by us in
/// known lowercase form, so a single `replacen` against the first hit
/// is safe and unambiguous.
fn inject_if_not_exists_into_add_column(script: &str) -> String {
    script.replacen("add column ", "add column if not exists ", 1)
}

/// Idempotent recreate block for a view that may have been silently
/// dropped by `DROP FUNCTION ... CASCADE`. Regular views use
/// `CREATE OR REPLACE VIEW` (a no-op when the view still exists with the
/// same definition; a create when CASCADE removed it). Materialized
/// views use `CREATE MATERIALIZED VIEW IF NOT EXISTS` for the same
/// reason — Phase 7's text-based matching can false-positive on a view
/// that PostgreSQL never actually dropped, and an unconditional CREATE
/// would fail against a surviving view.
fn view_recreate_block(view: &View) -> String {
    if !view.is_materialized {
        return rewrite_create_view_to_create_or_replace(&view.get_script());
    }
    // The view's indexes went with it if CASCADE really did drop it, so they are
    // recreated too — each guarded the same way and for the same reason as the
    // view itself, since an unconditional CREATE INDEX would fail against the
    // indexes of a view PostgreSQL never actually dropped.
    let mut block =
        inject_if_not_exists_into_create_materialized_view(&view.get_script_without_indexes());
    for index in &view.indexes {
        block.push_str(&inject_if_not_exists_into_create_index(&index.get_script()));
    }
    block
}

/// Rewrites the lowercase `create view ` prefix produced by
/// `View::get_script` to `CREATE OR REPLACE VIEW `. The check is
/// anchored to the start of the script via `strip_prefix` rather than a
/// whole-script `contains` / `find` scan: the view definition body
/// `View::get_script` appends after the prefix can legitimately contain
/// the literal text `create view` or `CREATE OR REPLACE VIEW` (e.g. a
/// string literal in the select list), and a non-anchored matcher would
/// either false-positive on the early-return branch (leaving the
/// leading `create view` unrewritten) or rewrite a non-prefix
/// occurrence. If the script already starts with `CREATE OR REPLACE
/// VIEW` (upstream rewrite) or some other unanticipated leading form,
/// it is returned unchanged.
fn rewrite_create_view_to_create_or_replace(script: &str) -> String {
    if let Some(rest) = script.strip_prefix("create view ") {
        return format!("CREATE OR REPLACE VIEW {}", rest);
    }
    script.to_string()
}

/// Rewrites the lowercase `create materialized view ` prefix produced by
/// `View::get_script` to `create materialized view if not exists `. The
/// prefix is generated by us in known lowercase form so a single
/// `replacen` against the first hit is safe and unambiguous.
fn inject_if_not_exists_into_create_materialized_view(script: &str) -> String {
    script.replacen(
        "create materialized view ",
        "create materialized view if not exists ",
        1,
    )
}

/// `drop policy if exists` + `create policy` block from the TO-side policy.
fn policy_recreate_block(policy: &TablePolicy) -> String {
    let mut block = format!(
        "drop policy if exists {} on {}.{};",
        policy.name, policy.schema, policy.table
    )
    .with_empty_lines();
    block.push_str(&policy.get_script());
    block
}

#[cfg(test)]
#[path = "tests/core.rs"]
mod tests;
