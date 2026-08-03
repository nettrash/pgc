//! Issue #180 — `SET UNLOGGED` / `SET LOGGED` statements must respect FK
//! dependencies (PostgreSQL rejects an out-of-order conversion), and owned
//! sequences should not redundantly re-emit the persistence flip the table
//! cascade already propagates. Also covers the FK-parsing fixes from
//! PR #187 and the cycle-breaking work in issues #190 and #191.

use crate::comparer::core::*;
use super::helpers::*;
use crate::config::dump_config::DumpConfig;
use crate::config::grants_mode::GrantsMode;
use crate::dump::sequence::Sequence;
use crate::dump::table::Table;
use crate::dump::table_constraint::TableConstraint;

/// Build a logged/unlogged-controlled table with a single FK to another
/// table in the same schema, named with a numeric `id` PK column. Used
/// by the issue-#180 ordering tests.
fn issue180_logged_table(
    schema: &str,
    name: &str,
    is_unlogged: bool,
    fk_target: Option<(&str, &str, &str)>,
) -> Table {
    let mut id_col = int_column(schema, name, "id", 1);
    id_col.is_nullable = false;

    let mut constraints: Vec<TableConstraint> = vec![TableConstraint {
        catalog: "postgres".to_string(),
        schema: schema.to_string(),
        name: format!("{name}_pkey"),
        table_name: name.to_string(),
        constraint_type: "PRIMARY KEY".to_string(),
        is_deferrable: false,
        initially_deferred: false,
        definition: Some("PRIMARY KEY (id)".to_string()),
        coninhcount: 0,
        is_enforced: true,
        no_inherit: false,
        nulls_not_distinct: false,
        comment: None,
    }];

    let mut columns = vec![id_col];
    if let Some((fk_col, fk_schema, fk_table)) = fk_target {
        let mut ref_col = int_column(schema, name, fk_col, 2);
        ref_col.is_nullable = true;
        columns.push(ref_col);
        constraints.push(TableConstraint {
            catalog: "postgres".to_string(),
            schema: schema.to_string(),
            name: format!("{name}_{fk_col}_fkey"),
            table_name: name.to_string(),
            constraint_type: "FOREIGN KEY".to_string(),
            is_deferrable: false,
            initially_deferred: false,
            definition: Some(format!(
                "FOREIGN KEY ({fk_col}) REFERENCES {fk_schema}.{fk_table}(id)"
            )),
            coninhcount: 0,
            is_enforced: true,
            no_inherit: false,
            nulls_not_distinct: false,
            comment: None,
        });
    }

    let mut table = Table::new(
        schema.to_string(),
        name.to_string(),
        schema.to_string(),
        name.to_string(),
        "postgres".to_string(),
        None,
        columns,
        constraints,
        vec![],
        vec![],
        None,
    );
    table.is_unlogged = is_unlogged;
    table.hash();
    table
}

#[tokio::test]
async fn issue180_set_unlogged_orders_dependents_before_referenced() {
    // FROM: three logged tables with FK chain
    //   child -> parent -> grandparent.
    // TO:   the same three tables, all UNLOGGED.
    // PostgreSQL refuses `SET UNLOGGED` on a table while a LOGGED table
    // still references it, so the conversion order must be leaves
    // first: child, then parent, then grandparent.
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());
    from_dump.tables.push(issue180_logged_table(
        "test_order",
        "grandparent",
        false,
        None,
    ));
    from_dump.tables.push(issue180_logged_table(
        "test_order",
        "parent",
        false,
        Some(("grandparent_id", "test_order", "grandparent")),
    ));
    from_dump.tables.push(issue180_logged_table(
        "test_order",
        "child",
        false,
        Some(("parent_id", "test_order", "parent")),
    ));
    to_dump.tables.push(issue180_logged_table(
        "test_order",
        "grandparent",
        true,
        None,
    ));
    to_dump.tables.push(issue180_logged_table(
        "test_order",
        "parent",
        true,
        Some(("grandparent_id", "test_order", "grandparent")),
    ));
    to_dump.tables.push(issue180_logged_table(
        "test_order",
        "child",
        true,
        Some(("parent_id", "test_order", "parent")),
    ));

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Ignore);
    comparer.compare_tables().await.unwrap();
    let script = comparer.get_script();

    let pos_child = script
        .find("alter table test_order.child set unlogged;")
        .expect("child SET UNLOGGED must be emitted");
    let pos_parent = script
        .find("alter table test_order.parent set unlogged;")
        .expect("parent SET UNLOGGED must be emitted");
    let pos_grand = script
        .find("alter table test_order.grandparent set unlogged;")
        .expect("grandparent SET UNLOGGED must be emitted");

    assert!(
        pos_child < pos_parent && pos_parent < pos_grand,
        "SET UNLOGGED must be ordered child -> parent -> grandparent (FK leaves first); got\n{}",
        script
    );
}

#[tokio::test]
async fn issue180_set_logged_orders_referenced_before_dependents() {
    // Reverse direction: all UNLOGGED -> all LOGGED.
    // PostgreSQL refuses `SET LOGGED` while the table still references
    // an UNLOGGED one, so order must be roots first: grandparent, then
    // parent, then child.
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());
    from_dump.tables.push(issue180_logged_table(
        "test_order",
        "grandparent",
        true,
        None,
    ));
    from_dump.tables.push(issue180_logged_table(
        "test_order",
        "parent",
        true,
        Some(("grandparent_id", "test_order", "grandparent")),
    ));
    from_dump.tables.push(issue180_logged_table(
        "test_order",
        "child",
        true,
        Some(("parent_id", "test_order", "parent")),
    ));
    to_dump.tables.push(issue180_logged_table(
        "test_order",
        "grandparent",
        false,
        None,
    ));
    to_dump.tables.push(issue180_logged_table(
        "test_order",
        "parent",
        false,
        Some(("grandparent_id", "test_order", "grandparent")),
    ));
    to_dump.tables.push(issue180_logged_table(
        "test_order",
        "child",
        false,
        Some(("parent_id", "test_order", "parent")),
    ));

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Ignore);
    comparer.compare_tables().await.unwrap();
    let script = comparer.get_script();

    let pos_grand = script
        .find("alter table test_order.grandparent set logged;")
        .expect("grandparent SET LOGGED must be emitted");
    let pos_parent = script
        .find("alter table test_order.parent set logged;")
        .expect("parent SET LOGGED must be emitted");
    let pos_child = script
        .find("alter table test_order.child set logged;")
        .expect("child SET LOGGED must be emitted");

    assert!(
        pos_grand < pos_parent && pos_parent < pos_child,
        "SET LOGGED must be ordered grandparent -> parent -> child (FK roots first); got\n{}",
        script
    );
}

#[tokio::test]
async fn issue180_persistence_change_does_not_emit_inline_inside_alter_table() {
    // A table-level ALTER (e.g. add column) MUST NOT carry a SET
    // UNLOGGED line — that would re-introduce the alphabetical ordering
    // bug. The persistence flip is owned by the dedicated phase.
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_table = Table::new(
        "test_order".to_string(),
        "items".to_string(),
        "test_order".to_string(),
        "items".to_string(),
        "postgres".to_string(),
        None,
        vec![int_column("test_order", "items", "id", 1)],
        vec![],
        vec![],
        vec![],
        None,
    );
    from_table.is_unlogged = false;
    from_table.hash();

    let mut to_table = Table::new(
        "test_order".to_string(),
        "items".to_string(),
        "test_order".to_string(),
        "items".to_string(),
        "postgres".to_string(),
        None,
        vec![
            int_column("test_order", "items", "id", 1),
            int_column("test_order", "items", "name", 2),
        ],
        vec![],
        vec![],
        vec![],
        None,
    );
    to_table.is_unlogged = true;
    to_table.hash();

    from_dump.tables.push(from_table);
    to_dump.tables.push(to_table);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Ignore);
    comparer.compare_tables().await.unwrap();
    let script = comparer.get_script();

    // SET UNLOGGED is still emitted, but only once and after the ADD
    // COLUMN — not interleaved inside the per-table ALTER block.
    let add_pos = script
        .find("alter table test_order.items add column name")
        .expect("add column must be emitted");
    let set_pos = script
        .find("alter table test_order.items set unlogged;")
        .expect("set unlogged must be emitted by the dedicated phase");
    assert!(
        add_pos < set_pos,
        "SET UNLOGGED must come from the dedicated phase, after the per-table ALTER: {}",
        script
    );
    assert_eq!(
        script.matches("set unlogged").count(),
        1,
        "SET UNLOGGED must be emitted exactly once (no inline + dedicated double-up): {}",
        script
    );
}

#[tokio::test]
async fn issue180_owned_sequence_persistence_only_diff_is_skipped() {
    // A sequence whose owning table flips persistence — and which has
    // no other diff — produces a redundant `ALTER SEQUENCE ... SET
    // UNLOGGED` followed by the full clause list. Both are noise: the
    // table's `ALTER TABLE ... SET UNLOGGED` already cascades to all
    // owned sequences. Suppress the entire ALTER SEQUENCE.
    use crate::dump::sequence::Sequence;

    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_table = Table::new(
        "test_order".to_string(),
        "items".to_string(),
        "test_order".to_string(),
        "items".to_string(),
        "postgres".to_string(),
        None,
        vec![int_column("test_order", "items", "id", 1)],
        vec![],
        vec![],
        vec![],
        None,
    );
    from_table.is_unlogged = false;
    from_table.hash();

    let mut to_table = from_table.clone();
    to_table.is_unlogged = true;
    to_table.hash();

    from_dump.tables.push(from_table);
    to_dump.tables.push(to_table);

    let make_seq = |is_unlogged: bool| {
        let mut s = Sequence::new(
            "test_order".to_string(),
            "items_id_seq".to_string(),
            "postgres".to_string(),
            "integer".to_string(),
            Some(1),
            Some(1),
            Some(2147483647),
            Some(1),
            false,
            Some(1),
            Some(1),
            Some("test_order".to_string()),
            Some("items".to_string()),
            Some("id".to_string()),
        );
        s.is_unlogged = is_unlogged;
        s.hash();
        s
    };
    from_dump.sequences.push(make_seq(false));
    to_dump.sequences.push(make_seq(true));

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Ignore);
    comparer.compare_sequences().await.unwrap();
    let script = comparer.get_script();

    assert!(
        !script.contains("alter sequence test_order.items_id_seq"),
        "owned-sequence persistence-only flip must be suppressed (table cascade handles it); got:\n{}",
        script
    );
}

#[tokio::test]
async fn issue180_owned_sequence_other_diff_skips_only_persistence_line() {
    // When the sequence has a real change (e.g. cache_size) AND the
    // owning table is also flipping persistence, we still need the
    // ALTER SEQUENCE — but not the `SET UNLOGGED|LOGGED` line, because
    // the table cascade handles that.
    use crate::dump::sequence::Sequence;

    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let mut from_table = Table::new(
        "test_order".to_string(),
        "items".to_string(),
        "test_order".to_string(),
        "items".to_string(),
        "postgres".to_string(),
        None,
        vec![int_column("test_order", "items", "id", 1)],
        vec![],
        vec![],
        vec![],
        None,
    );
    from_table.is_unlogged = false;
    from_table.hash();
    let mut to_table = from_table.clone();
    to_table.is_unlogged = true;
    to_table.hash();
    from_dump.tables.push(from_table);
    to_dump.tables.push(to_table);

    let make_seq = |is_unlogged: bool, cache: i64| {
        let mut s = Sequence::new(
            "test_order".to_string(),
            "items_id_seq".to_string(),
            "postgres".to_string(),
            "integer".to_string(),
            Some(1),
            Some(1),
            Some(2147483647),
            Some(1),
            false,
            Some(cache),
            Some(1),
            Some("test_order".to_string()),
            Some("items".to_string()),
            Some("id".to_string()),
        );
        s.is_unlogged = is_unlogged;
        s.hash();
        s
    };
    from_dump.sequences.push(make_seq(false, 1));
    to_dump.sequences.push(make_seq(true, 5));

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Ignore);
    comparer.compare_sequences().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("alter sequence test_order.items_id_seq"),
        "ALTER SEQUENCE must be emitted when non-persistence params changed: {}",
        script
    );
    assert!(
        script.contains("cache 5"),
        "the changed cache value must be in the script: {}",
        script
    );
    assert!(
        !script.contains("alter sequence test_order.items_id_seq set unlogged"),
        "SET UNLOGGED on owned sequence is redundant when the owning table is flipping persistence: {}",
        script
    );
}

#[tokio::test]
async fn issue180_standalone_sequence_persistence_change_still_emits_set() {
    // A sequence not owned by any table (or owned by a table whose
    // persistence is unchanged) must still get its own SET because no
    // table cascade applies.
    use crate::dump::sequence::Sequence;

    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    let make_seq = |is_unlogged: bool| {
        let mut s = Sequence::new(
            "test_order".to_string(),
            "global_seq".to_string(),
            "postgres".to_string(),
            "integer".to_string(),
            Some(1),
            Some(1),
            Some(2147483647),
            Some(1),
            false,
            Some(1),
            Some(1),
            None,
            None,
            None,
        );
        s.is_unlogged = is_unlogged;
        s.hash();
        s
    };
    from_dump.sequences.push(make_seq(false));
    to_dump.sequences.push(make_seq(true));

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Ignore);
    comparer.compare_sequences().await.unwrap();
    let script = comparer.get_script();

    assert!(
        script.contains("alter sequence test_order.global_seq set unlogged"),
        "standalone sequence must still emit SET UNLOGGED: {}",
        script
    );
}

#[test]
fn issue180_parse_fk_referenced_table_word_boundary() {
    // PR #184 review: a naive `find("references ")` substring match
    // can pick up the literal text inside a quoted column name in the
    // FK column list. The matcher must be anchored to a word boundary
    // and the keyword must be followed by whitespace.
    assert_eq!(
        Comparer::parse_fk_referenced_table(
            "FOREIGN KEY (col_a) REFERENCES public.target(id)",
            "public",
        ),
        Some(("public".to_string(), "target".to_string())),
        "happy-path FK definition must parse"
    );
    // Column literally named `"references "` (with trailing space) in
    // the FK column list. Naive substring search would lock onto it
    // before the real keyword and parse garbage.
    assert_eq!(
        Comparer::parse_fk_referenced_table(
            "FOREIGN KEY (\"references \", col_b) REFERENCES public.target(id)",
            "public",
        ),
        Some(("public".to_string(), "target".to_string()))
    );
    // Column named `references_count` — substring match on
    // "references" without the right-side word-boundary check would
    // see this column first and try to parse what follows.
    assert_eq!(
        Comparer::parse_fk_referenced_table(
            "FOREIGN KEY (references_count) REFERENCES public.target(id)",
            "public",
        ),
        Some(("public".to_string(), "target".to_string()))
    );
}

#[test]
fn issue180_parse_fk_referenced_table_quoted_identifier_with_dot() {
    // PR #184 review: a quoted identifier may contain a literal `.`,
    // and the schema/name split must respect quotes — otherwise the
    // first dot inside the quoted segment is taken as the boundary
    // and the parsed pair is nonsensical.
    assert_eq!(
        Comparer::parse_fk_referenced_table(
            "FOREIGN KEY (col) REFERENCES \"weird.schema\".\"t\"(id)",
            "public",
        ),
        Some(("weird.schema".to_string(), "t".to_string()))
    );
    // Both halves quoted with embedded dots — the split must still
    // land on the dot OUTSIDE every quoted segment.
    assert_eq!(
        Comparer::parse_fk_referenced_table(
            "FOREIGN KEY (col) REFERENCES \"a.b\".\"c.d\"(id)",
            "public",
        ),
        Some(("a.b".to_string(), "c.d".to_string()))
    );
}

#[test]
fn pr187_parse_fk_skips_keyword_inside_quoted_column_name() {
    // PR #187 review (C7): a column literally named
    // `"my references col"` puts the bytes `references` between two
    // spaces, passing the naive boundary check, then returns `None`
    // from the false match without ever reaching the real keyword.
    // The scanner must skip matches that fall inside a double-quoted
    // identifier.
    assert_eq!(
        Comparer::parse_fk_referenced_table(
            "FOREIGN KEY (\"my references col\") REFERENCES public.target(id)",
            "public",
        ),
        Some(("public".to_string(), "target".to_string())),
        "FK keyword must still be located even with `references` inside a quoted column name"
    );
}

#[test]
fn pr187_parse_fk_handles_dollar_in_identifier() {
    // PR #187 review (C8): PostgreSQL identifiers may contain `$`,
    // so the unquoted-identifier scan must include it. Otherwise a
    // target like `public.parent$table` is truncated to
    // `public.parent`.
    assert_eq!(
        Comparer::parse_fk_referenced_table(
            "FOREIGN KEY (col) REFERENCES public.parent$table(id)",
            "public",
        ),
        Some(("public".to_string(), "parent$table".to_string()))
    );
}

#[test]
fn pr187_definition_references_any_skips_string_literals() {
    // PR #187 review (C4): a SQL string literal containing routine
    // text — `CHECK (msg <> 'compute(value)')` — must not trigger
    // the unqualified-call matcher. The `definition_references_any`
    // pre-pass must blank out single-quoted literals before scanning.
    let mut affected: HashSet<(String, String)> = HashSet::new();
    affected.insert(("public".to_string(), "compute".to_string()));
    assert!(
        !Comparer::definition_references_any("CHECK (msg <> 'compute(value)')", &affected),
        "literal text must not be treated as a function call"
    );
    // Sanity check: a real call outside a literal still matches.
    assert!(
        Comparer::definition_references_any(
            "CHECK (compute(value) > 0 AND msg <> 'compute(value)')",
            &affected
        ),
        "real call outside the literal must still match"
    );
}

#[tokio::test]
async fn pr187_persistence_ordering_works_with_quoted_identifiers() {
    // PR #187 review (C2): mixed-case table names round-trip into
    // `Table.schema` / `Table.name` with surrounding quotes
    // (`quote_ident` in the dump query). The FK parser strips quotes
    // from its returned `(schema, name)`. Without normalising the
    // lookup map to the same quote-stripped form, FK edges between
    // quoted-identifier tables go missing and persistence flips fall
    // back to alphabetical order, which PostgreSQL rejects for FK
    // chains. Build a parent→child chain whose names are quoted and
    // assert the SET UNLOGGED order is leaves-first.
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());
    let mk = |name: &str, is_unlogged: bool, fk: Option<(&str, &str, &str)>| {
        // Wrap the schema/name in quotes the way `quote_ident` would.
        let mut t = issue180_logged_table("\"TestOrder\"", name, is_unlogged, fk);
        t.schema = "\"TestOrder\"".to_string();
        t.raw_schema = "\"TestOrder\"".to_string();
        t
    };
    from_dump.tables.push(mk("\"Grand\"", false, None));
    from_dump.tables.push(mk(
        "\"Parent\"",
        false,
        Some(("grand_id", "\"TestOrder\"", "\"Grand\"")),
    ));
    from_dump.tables.push(mk(
        "\"Child\"",
        false,
        Some(("parent_id", "\"TestOrder\"", "\"Parent\"")),
    ));
    to_dump.tables.push(mk("\"Grand\"", true, None));
    to_dump.tables.push(mk(
        "\"Parent\"",
        true,
        Some(("grand_id", "\"TestOrder\"", "\"Grand\"")),
    ));
    to_dump.tables.push(mk(
        "\"Child\"",
        true,
        Some(("parent_id", "\"TestOrder\"", "\"Parent\"")),
    ));

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Ignore);
    comparer.compare_tables().await.unwrap();
    let script = comparer.get_script();

    let pos_child = script
        .find("alter table \"TestOrder\".\"Child\" set unlogged;")
        .expect("child SET UNLOGGED missing");
    let pos_parent = script
        .find("alter table \"TestOrder\".\"Parent\" set unlogged;")
        .expect("parent SET UNLOGGED missing");
    let pos_grand = script
        .find("alter table \"TestOrder\".\"Grand\" set unlogged;")
        .expect("grand SET UNLOGGED missing");
    assert!(
        pos_child < pos_parent && pos_parent < pos_grand,
        "FK-leaf-first order must hold for quoted identifiers too: {script}"
    );
}

#[tokio::test]
async fn pr187_persistence_ordering_includes_in_place_alterable_fks() {
    // PR #187 review (C13): an FK whose definition differs only in
    // an in-place-alterable property (deferrability, enforced,
    // no_inherit, comment) is NOT dropped by `compare_tables` — it
    // stays live until `compare_foreign_keys` ALTERs it. The live
    // FK adjacency for the SET phase must include it, otherwise
    // chains where one FK is being toggled deferrable/enforced fall
    // back to alphabetical SET order.
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());
    from_dump
        .tables
        .push(issue180_logged_table("test_order", "parent", false, None));
    from_dump.tables.push(issue180_logged_table(
        "test_order",
        "child",
        false,
        Some(("parent_id", "test_order", "parent")),
    ));
    to_dump
        .tables
        .push(issue180_logged_table("test_order", "parent", true, None));
    let mut to_child = issue180_logged_table(
        "test_order",
        "child",
        true,
        Some(("parent_id", "test_order", "parent")),
    );
    // Toggle the FK's deferrability — `can_be_altered_to` accepts
    // this, so the FK survives `compare_tables` and is still live at
    // the SET point.
    if let Some(fk) = to_child
        .constraints
        .iter_mut()
        .find(|c| c.constraint_type.eq_ignore_ascii_case("foreign key"))
    {
        fk.is_deferrable = true;
        fk.initially_deferred = true;
    }
    to_child.hash();
    to_dump.tables.push(to_child);

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Ignore);
    comparer.compare_tables().await.unwrap();
    let script = comparer.get_script();

    let pos_child = script
        .find("alter table test_order.child set unlogged;")
        .expect("child SET UNLOGGED missing");
    let pos_parent = script
        .find("alter table test_order.parent set unlogged;")
        .expect("parent SET UNLOGGED missing");
    assert!(
        pos_child < pos_parent,
        "child must come before parent even when the FK between them is being in-place ALTERed: {script}"
    );
}

#[test]
fn pr187_unqualified_matcher_unicode_boundary_rejects_longer_identifier() {
    // PR #187 review (C17): the boundary check used raw ASCII byte
    // tests, which treated Cyrillic neighbours as non-identifier and
    // let `функция` match inside `мояфункция(`. The check now uses
    // character-class identifier rules, so Cyrillic-letter neighbours
    // correctly extend the identifier and reject the match.
    let mut affected: HashSet<(String, String)> = HashSet::new();
    affected.insert(("public".to_string(), "функция".to_string()));
    assert!(
        !Comparer::definition_references_any("CHECK (мояфункция(x) > 0)", &affected),
        "unicode letter to the left must extend the identifier"
    );
    assert!(
        !Comparer::definition_references_any("CHECK (функцияд(x) > 0)", &affected),
        "unicode letter to the right must extend the identifier"
    );
    // Sanity: a clean Cyrillic call still matches.
    assert!(
        Comparer::definition_references_any("CHECK (функция(x) > 0)", &affected),
        "standalone unicode call must still match"
    );
}

#[test]
fn issue180_parse_fk_referenced_table_handles_non_ascii_column_names() {
    // PR #184 follow-up review: `parse_fk_referenced_table` previously
    // built the case-insensitive haystack via `to_lowercase()`, which
    // can change byte length for some non-ASCII characters
    // (e.g. capital Turkish dotted I, `İ`, lowercases to a multi-char
    // sequence with a different UTF-8 length). The keyword position
    // came from the lowercased haystack but the slice that produces
    // the parsed identifier reaches back into `def`, so a
    // length-changing lowercasing would land mid-codepoint and panic.
    // `to_ascii_lowercase()` is byte-length-preserving — pin that
    // contract by parsing FK definitions whose column list contains
    // identifiers that trip every byte-length-changing lowercase
    // conversion in common locales.
    //
    // Quoted column with capital `İ` (U+0130). With `to_lowercase()`
    // this produces `i\u{0307}` (3 bytes total); `to_ascii_lowercase`
    // leaves the 2-byte `İ` alone, so byte offsets line up.
    assert_eq!(
        Comparer::parse_fk_referenced_table(
            "FOREIGN KEY (\"\u{0130}d\") REFERENCES public.target(id)",
            "public",
        ),
        Some(("public".to_string(), "target".to_string()))
    );
    // German sharp S (`ß`, U+00DF). `to_lowercase()` keeps it as `ß`,
    // but the inverse — uppercase `ẞ` (U+1E9E) lowercasing to `ß` —
    // is length-preserving in UTF-8 too. Use a Cyrillic lowercase
    // identifier here just to round out coverage of identifiers whose
    // bytes lie outside the ASCII range.
    assert_eq!(
        Comparer::parse_fk_referenced_table(
            "FOREIGN KEY (\"русское_имя\") REFERENCES public.target(id)",
            "public",
        ),
        Some(("public".to_string(), "target".to_string()))
    );
    // Same case in the qualified target identifier.
    assert_eq!(
        Comparer::parse_fk_referenced_table(
            "FOREIGN KEY (col) REFERENCES \"тест\".\"target\"(id)",
            "public",
        ),
        Some(("тест".to_string(), "target".to_string()))
    );
}

#[test]
fn issue190_parse_fk_unqualified_target_falls_back_to_owner_schema() {
    // Issue #190: `pg_get_constraintdef` omits the schema qualifier
    // when the target is reachable via `search_path` — typical for
    // tables in `public`. Pre-fix the parser returned `None` for these
    // and the FK edge was silently dropped from the persistence-flip
    // adjacency, leaving FK chains in `public` ordered alphabetically
    // (the order PostgreSQL rejects).
    //
    // Plain unqualified target — same schema as the FK owner.
    assert_eq!(
        Comparer::parse_fk_referenced_table(
            "FOREIGN KEY (parent_id) REFERENCES parent(id)",
            "public",
        ),
        Some(("public".to_string(), "parent".to_string())),
        "unqualified target must resolve to (owner_schema, target)"
    );
    // Quoted unqualified target — the quotes must be stripped to
    // match the comparer's normalised `to_index_by_key` keys (which
    // strip quotes on the index side too).
    assert_eq!(
        Comparer::parse_fk_referenced_table(
            "FOREIGN KEY (col) REFERENCES \"MixedCase\"(id)",
            "public",
        ),
        Some(("public".to_string(), "MixedCase".to_string()))
    );
    // Quoted owner schema (e.g. mixed-case schema names land here as
    // `"MySchema"` via `quote_ident`) — the fallback must strip the
    // surrounding quotes from the owner schema too, otherwise the
    // produced pair misses the index-side lookup keys.
    assert_eq!(
        Comparer::parse_fk_referenced_table(
            "FOREIGN KEY (col) REFERENCES parent(id)",
            "\"MySchema\"",
        ),
        Some(("MySchema".to_string(), "parent".to_string()))
    );
    // ON UPDATE / ON DELETE clauses follow the target — make sure
    // they don't confuse the boundary scan.
    assert_eq!(
        Comparer::parse_fk_referenced_table(
            "FOREIGN KEY (col) REFERENCES parent(id) ON DELETE CASCADE",
            "public",
        ),
        Some(("public".to_string(), "parent".to_string()))
    );
}

/// Issue #190 end-to-end: a FK chain in `public` whose deparsed
/// definition uses unqualified target names must still be ordered
/// leaves-first by `emit_persistence_changes`. Pre-fix the unqualified
/// targets returned `None` from the parser, the adjacency went empty,
/// and the SET UNLOGGED order fell back to alphabetical (`child`
/// emitted *after* `parent` — exactly the order PostgreSQL rejects).
#[tokio::test]
async fn issue190_set_unlogged_orders_unqualified_public_fk_chain() {
    // Builder that matches `issue180_logged_table` but emits FK
    // definitions WITHOUT the schema qualifier — the
    // `pg_get_constraintdef` output shape that exposes the issue.
    fn make_table(name: &str, is_unlogged: bool, fk_target: Option<(&str, &str)>) -> Table {
        let mut id_col = int_column("public", name, "id", 1);
        id_col.is_nullable = false;
        let mut constraints: Vec<TableConstraint> = vec![TableConstraint {
            catalog: "postgres".to_string(),
            schema: "public".to_string(),
            name: format!("{name}_pkey"),
            table_name: name.to_string(),
            constraint_type: "PRIMARY KEY".to_string(),
            is_deferrable: false,
            initially_deferred: false,
            definition: Some("PRIMARY KEY (id)".to_string()),
            coninhcount: 0,
            is_enforced: true,
            no_inherit: false,
            nulls_not_distinct: false,
            comment: None,
        }];

        let mut columns = vec![id_col];
        if let Some((fk_col, fk_table)) = fk_target {
            let mut ref_col = int_column("public", name, fk_col, 2);
            ref_col.is_nullable = true;
            columns.push(ref_col);
            // Unqualified `REFERENCES parent(id)` — no `public.`
            // qualifier. This is what `pg_get_constraintdef` returns
            // when the target is reachable via `search_path`.
            constraints.push(TableConstraint {
                catalog: "postgres".to_string(),
                schema: "public".to_string(),
                name: format!("{name}_{fk_col}_fkey"),
                table_name: name.to_string(),
                constraint_type: "FOREIGN KEY".to_string(),
                is_deferrable: false,
                initially_deferred: false,
                definition: Some(format!("FOREIGN KEY ({fk_col}) REFERENCES {fk_table}(id)")),
                coninhcount: 0,
                is_enforced: true,
                no_inherit: false,
                nulls_not_distinct: false,
                comment: None,
            });
        }

        let mut table = Table::new(
            "public".to_string(),
            name.to_string(),
            "public".to_string(),
            name.to_string(),
            "postgres".to_string(),
            None,
            columns,
            constraints,
            vec![],
            vec![],
            None,
        );
        table.is_unlogged = is_unlogged;
        table.hash();
        table
    }

    // FROM: child → parent in public, both LOGGED.
    // TO:   same chain, both UNLOGGED. The FK must survive in TO
    // unchanged (live edge) so the adjacency considers it.
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());
    from_dump.tables.push(make_table("parent", false, None));
    from_dump
        .tables
        .push(make_table("child", false, Some(("parent_id", "parent"))));
    to_dump.tables.push(make_table("parent", true, None));
    to_dump
        .tables
        .push(make_table("child", true, Some(("parent_id", "parent"))));

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Ignore);
    comparer.compare_tables().await.unwrap();
    let script = comparer.get_script();

    let child_pos = script
        .find("alter table public.child set unlogged;")
        .expect("child SET UNLOGGED must be emitted");
    let parent_pos = script
        .find("alter table public.parent set unlogged;")
        .expect("parent SET UNLOGGED must be emitted");
    assert!(
        child_pos < parent_pos,
        "child (referrer) must SET UNLOGGED BEFORE parent (referenced); \
         got child@{} parent@{} — alphabetical order would put `child` \
         after `parent` and PostgreSQL would reject the SET on `parent` \
         while `child` is still LOGGED:\n{}",
        child_pos,
        parent_pos,
        script
    );
}

#[tokio::test]
async fn issue180_set_unlogged_skips_ordering_for_new_fks_added_later() {
    // PR #184 review (FK-timing): when an FK is brand-new in TO it is
    // not yet active at the moment `emit_persistence_changes` runs —
    // `compare_foreign_keys` adds it strictly after. The adjacency
    // must therefore filter to FKs that exist UNCHANGED in both
    // FROM and TO. Without that filter, an alphabetical pair would
    // be over-ordered as if the new FK were already live.
    //
    // Setup: child references parent in TO (new FK). FROM has no FK.
    // Both flip from LOGGED to UNLOGGED.
    //
    // With the live-FK-set tightening, the adjacency is empty, so
    // ordering falls back to alphabetical (deterministic via the
    // sort_key in the topo sort). This is safe because PG won't see
    // the FK link until after the SET phase.
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());
    from_dump
        .tables
        .push(issue180_logged_table("test_order", "parent", false, None));
    // FROM child has no FK to parent — the FK is new in TO.
    from_dump
        .tables
        .push(issue180_logged_table("test_order", "child", false, None));
    to_dump
        .tables
        .push(issue180_logged_table("test_order", "parent", true, None));
    to_dump.tables.push(issue180_logged_table(
        "test_order",
        "child",
        true,
        Some(("parent_id", "test_order", "parent")),
    ));

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Ignore);
    comparer.compare_tables().await.unwrap();
    let script = comparer.get_script();

    // Both SET UNLOGGED statements must be present; ordering between
    // them is not constrained by the (not-yet-live) new FK.
    assert!(script.contains("alter table test_order.child set unlogged;"));
    assert!(script.contains("alter table test_order.parent set unlogged;"));
}

#[test]
fn issue180_sequence_only_persistence_change_uses_hash_diff() {
    // PR #184 review: `is_only_persistence_change` clones, equalises
    // `is_unlogged` to FROM, recomputes the hash, and compares.
    // That keeps the check honest if `Sequence::hash` later starts
    // covering a new field. This test pins the contract by exercising
    // both directions: identical-except-persistence returns true; a
    // hashed field different (here `cache_size`) returns false.
    use crate::dump::sequence::Sequence;

    let make = |is_unlogged: bool, cache: i64| {
        let mut s = Sequence::new(
            "public".to_string(),
            "s".to_string(),
            "postgres".to_string(),
            "integer".to_string(),
            Some(1),
            Some(1),
            Some(2147483647),
            Some(1),
            false,
            Some(cache),
            Some(1),
            None,
            None,
            None,
        );
        s.is_unlogged = is_unlogged;
        s.hash();
        s
    };
    let from = make(false, 1);
    let to_only_persistence = make(true, 1);
    let to_persistence_and_cache = make(true, 5);
    assert!(
        to_only_persistence.is_only_persistence_change(&from),
        "identical except is_unlogged must be detected as persistence-only"
    );
    assert!(
        !to_persistence_and_cache.is_only_persistence_change(&from),
        "a hashed field difference must block the persistence-only suppression"
    );
}

/// Issue #191: a mutual FK cycle (`A → B` and `B → A`) flipping
/// persistence in the same direction has NO valid SET LOGGED|UNLOGGED
/// order — `SET UNLOGGED A` requires B to already be UNLOGGED and
/// vice versa. Pre-fix `kahn_toposort`'s fallback appended cyclic
/// nodes alphabetically, the migration emitted the SETs in that
/// order, and PostgreSQL rejected the second SET at apply time with
/// the same `could not change table … to logged/unlogged` error
/// issue #180 was meant to eliminate. Fix: detect the cycle, drop
/// every FK whose endpoints both sit in the cyclic set BEFORE the
/// SETs, then re-add them from their TO definitions AFTER. The
/// post-fix migration is therefore: DROP cycle FKs → SET both
/// (any order) → ADD cycle FKs.
#[tokio::test]
async fn issue191_persistence_flip_breaks_mutual_fk_cycle() {
    fn make_cycle_table(name: &str, is_unlogged: bool, fk: (&str, &str, &str)) -> Table {
        let (fk_col, target_schema, target_table) = fk;

        let mut id_col = int_column("test_cycle", name, "id", 1);
        id_col.is_nullable = false;

        let mut ref_col = int_column("test_cycle", name, fk_col, 2);
        ref_col.is_nullable = true;

        let pk = TableConstraint {
            catalog: "postgres".to_string(),
            schema: "test_cycle".to_string(),
            name: format!("{name}_pkey"),
            table_name: name.to_string(),
            constraint_type: "PRIMARY KEY".to_string(),
            is_deferrable: false,
            initially_deferred: false,
            definition: Some("PRIMARY KEY (id)".to_string()),
            coninhcount: 0,
            is_enforced: true,
            no_inherit: false,
            nulls_not_distinct: false,
            comment: None,
        };

        // Deferrable FK — PostgreSQL only permits mutual FK cycles
        // when both FKs are deferrable; without DEFERRABLE the cycle
        // can't be inserted/seeded in the first place.
        let fk_constraint = TableConstraint {
            catalog: "postgres".to_string(),
            schema: "test_cycle".to_string(),
            name: format!("{name}_{fk_col}_fkey"),
            table_name: name.to_string(),
            constraint_type: "FOREIGN KEY".to_string(),
            is_deferrable: true,
            initially_deferred: true,
            definition: Some(format!(
                "FOREIGN KEY ({fk_col}) REFERENCES {target_schema}.{target_table}(id) DEFERRABLE INITIALLY DEFERRED"
            )),
            coninhcount: 0,
            is_enforced: true,
            no_inherit: false,
            nulls_not_distinct: false,
            comment: None,
        };

        let mut table = Table::new(
            "test_cycle".to_string(),
            name.to_string(),
            "test_cycle".to_string(),
            name.to_string(),
            "postgres".to_string(),
            None,
            vec![id_col, ref_col],
            vec![pk, fk_constraint],
            vec![],
            vec![],
            None,
        );
        table.is_unlogged = is_unlogged;
        table.hash();
        table
    }

    // FROM: both LOGGED, mutual deferrable FKs.
    // TO:   both UNLOGGED, same FKs (live edges — unchanged).
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());
    from_dump
        .tables
        .push(make_cycle_table("a", false, ("b_id", "test_cycle", "b")));
    from_dump
        .tables
        .push(make_cycle_table("b", false, ("a_id", "test_cycle", "a")));
    to_dump
        .tables
        .push(make_cycle_table("a", true, ("b_id", "test_cycle", "b")));
    to_dump
        .tables
        .push(make_cycle_table("b", true, ("a_id", "test_cycle", "a")));

    // `use_drop=true` — this test validates the *active* cycle-break
    // path (drops and re-adds emitted live). The `use_drop=false`
    // semantics are covered separately by
    // `issue191_pr198_use_drop_false_comments_out_cycle_break`.
    // PR #198 review: an earlier revision used `use_drop=false` here
    // and the substring-based `script.find` assertions were
    // false-positives — they matched the commented-out `-- alter
    // table … drop constraint …` lines and never actually verified
    // the live path.
    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_tables().await.unwrap();
    let script = comparer.get_script();

    // Cycle-break banner must appear so the choice is loud.
    assert!(
        script.contains("Persistence-flip FK cycle (issue #191)"),
        "cycle banner must mark the drop+SET+add block: {}",
        script
    );

    // Both cycle FKs must be dropped BEFORE either SET UNLOGGED.
    // Anchor each `find` to a leading newline so the assertion
    // distinguishes the live statement from a commented `-- alter
    // table …` prefix — this is the PR #198 review fix.
    let drop_a_pos = script
        .find("\nalter table test_cycle.a drop constraint a_b_id_fkey;")
        .expect("FK a_b_id_fkey must be dropped before SET (live, not commented)");
    let drop_b_pos = script
        .find("\nalter table test_cycle.b drop constraint b_a_id_fkey;")
        .expect("FK b_a_id_fkey must be dropped before SET (live, not commented)");
    assert!(
        !script.contains("-- alter table test_cycle.a drop constraint a_b_id_fkey;"),
        "DROP CONSTRAINT must be emitted LIVE under use_drop=true, not commented: {}",
        script
    );
    assert!(
        !script.contains("-- alter table test_cycle.b drop constraint b_a_id_fkey;"),
        "DROP CONSTRAINT must be emitted LIVE under use_drop=true, not commented: {}",
        script
    );
    let set_a_pos = script
        .find("\nalter table test_cycle.a set unlogged;")
        .expect("a SET UNLOGGED must be emitted");
    let set_b_pos = script
        .find("\nalter table test_cycle.b set unlogged;")
        .expect("b SET UNLOGGED must be emitted");
    assert!(
        drop_a_pos < set_a_pos && drop_a_pos < set_b_pos,
        "FK a_b_id_fkey drop must precede every SET: drop@{} a@{} b@{}\n{}",
        drop_a_pos,
        set_a_pos,
        set_b_pos,
        script
    );
    assert!(
        drop_b_pos < set_a_pos && drop_b_pos < set_b_pos,
        "FK b_a_id_fkey drop must precede every SET: drop@{} a@{} b@{}\n{}",
        drop_b_pos,
        set_a_pos,
        set_b_pos,
        script
    );

    // Both cycle FKs must be re-added AFTER every SET so the post-
    // migration state matches TO. PR #198 review: the re-emit path
    // now goes through `TableConstraint::get_script()`, which
    // lowercases SQL keywords outside literals — match on the
    // lowercase form. The newline anchor again separates live ADDs
    // from any `-- alter table … add constraint …` form.
    let add_a_pos = script
        .find("\nalter table test_cycle.a add constraint a_b_id_fkey foreign key")
        .expect("FK a_b_id_fkey must be re-added after SET (live, not commented)");
    let add_b_pos = script
        .find("\nalter table test_cycle.b add constraint b_a_id_fkey foreign key")
        .expect("FK b_a_id_fkey must be re-added after SET (live, not commented)");
    assert!(
        !script.contains("-- alter table test_cycle.a add constraint a_b_id_fkey"),
        "ADD CONSTRAINT must be emitted LIVE under use_drop=true, not commented: {}",
        script
    );
    assert!(
        !script.contains("-- alter table test_cycle.b add constraint b_a_id_fkey"),
        "ADD CONSTRAINT must be emitted LIVE under use_drop=true, not commented: {}",
        script
    );
    assert!(
        add_a_pos > set_a_pos && add_a_pos > set_b_pos,
        "FK a_b_id_fkey re-add must follow every SET: add@{} a@{} b@{}\n{}",
        add_a_pos,
        set_a_pos,
        set_b_pos,
        script
    );
    assert!(
        add_b_pos > set_a_pos && add_b_pos > set_b_pos,
        "FK b_a_id_fkey re-add must follow every SET: add@{} a@{} b@{}\n{}",
        add_b_pos,
        set_a_pos,
        set_b_pos,
        script
    );
}

/// Issue #191 counter-test: an acyclic FK chain (no cycle) must NOT
/// emit cycle-break drops/re-adds. Locks the cycle path to only the
/// cycle case so we don't regress and start dropping FKs on every
/// persistence flip.
#[tokio::test]
async fn issue191_persistence_flip_acyclic_chain_does_not_drop_fks() {
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());
    from_dump
        .tables
        .push(issue180_logged_table("test_order", "parent", false, None));
    from_dump.tables.push(issue180_logged_table(
        "test_order",
        "child",
        false,
        Some(("parent_id", "test_order", "parent")),
    ));
    to_dump
        .tables
        .push(issue180_logged_table("test_order", "parent", true, None));
    to_dump.tables.push(issue180_logged_table(
        "test_order",
        "child",
        true,
        Some(("parent_id", "test_order", "parent")),
    ));

    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Ignore);
    comparer.compare_tables().await.unwrap();
    let script = comparer.get_script();

    assert!(
        !script.contains("Persistence-flip FK cycle"),
        "acyclic chain must not trip the cycle-break path: {}",
        script
    );
    assert!(
        !script.contains("drop constraint child_parent_id_fkey"),
        "live FK on an acyclic chain must not be dropped: {}",
        script
    );
}

/// Issue #191 / PR #198 review: when a cycle exists alongside an
/// edge that's *blocked by* the cycle but not in it (e.g. `A ↔ B`
/// plus `A → C` from outside the cycle, with all three flipping
/// persistence in the same direction), Kahn's "couldn't-be-ordered"
/// remainder includes C — even though C is not part of any directed
/// cycle. Pre-refinement the comparer treated the entire remainder
/// as cycle participants and dropped the `A → C` FK alongside the
/// true cycle edges, making the migration more destructive than
/// needed. Tarjan's SCC narrows the cycle set to nodes in
/// strongly-connected components of size >= 2, so only the true
/// cycle edges get dropped.
#[tokio::test]
async fn issue191_pr198_cycle_detection_excludes_blocked_non_cycle_nodes() {
    fn build(name: &str, is_unlogged: bool, fk: Option<(&str, &str, &str)>) -> Table {
        let mut id_col = int_column("test_cycle", name, "id", 1);
        id_col.is_nullable = false;

        let pk = TableConstraint {
            catalog: "postgres".to_string(),
            schema: "test_cycle".to_string(),
            name: format!("{name}_pkey"),
            table_name: name.to_string(),
            constraint_type: "PRIMARY KEY".to_string(),
            is_deferrable: false,
            initially_deferred: false,
            definition: Some("PRIMARY KEY (id)".to_string()),
            coninhcount: 0,
            is_enforced: true,
            no_inherit: false,
            nulls_not_distinct: false,
            comment: None,
        };

        let mut constraints = vec![pk];
        let mut columns = vec![id_col];

        if let Some((fk_col, target_schema, target_table)) = fk {
            let mut ref_col = int_column("test_cycle", name, fk_col, 2);
            ref_col.is_nullable = true;
            columns.push(ref_col);
            constraints.push(TableConstraint {
                catalog: "postgres".to_string(),
                schema: "test_cycle".to_string(),
                name: format!("{name}_{fk_col}_fkey"),
                table_name: name.to_string(),
                constraint_type: "FOREIGN KEY".to_string(),
                is_deferrable: true,
                initially_deferred: true,
                definition: Some(format!(
                    "FOREIGN KEY ({fk_col}) REFERENCES {target_schema}.{target_table}(id) DEFERRABLE INITIALLY DEFERRED"
                )),
                coninhcount: 0,
                is_enforced: true,
                no_inherit: false,
                nulls_not_distinct: false,
                comment: None,
            });
        }

        let mut t = Table::new(
            "test_cycle".to_string(),
            name.to_string(),
            "test_cycle".to_string(),
            name.to_string(),
            "postgres".to_string(),
            None,
            columns,
            constraints,
            vec![],
            vec![],
            None,
        );
        t.is_unlogged = is_unlogged;
        t.hash();
        t
    }

    // Build three tables:
    //   a ↔ b   (cycle: a → b and b → a)
    //   a → c   (non-cycle: a depends on c, but c does not depend on a)
    // All three flip LOGGED → UNLOGGED. The cycle set is {a, b}; the
    // FK `a_c_id_fkey` is NOT in any cycle and must survive the
    // cycle break.
    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());

    // FROM: all LOGGED. `a` has TWO FKs: a → b (cycle), a → c (not).
    let mut a_from = build("a", false, Some(("b_id", "test_cycle", "b")));
    a_from.columns.push({
        let mut c_id = int_column("test_cycle", "a", "c_id", 3);
        c_id.is_nullable = true;
        c_id
    });
    a_from.constraints.push(TableConstraint {
        catalog: "postgres".to_string(),
        schema: "test_cycle".to_string(),
        name: "a_c_id_fkey".to_string(),
        table_name: "a".to_string(),
        constraint_type: "FOREIGN KEY".to_string(),
        is_deferrable: false,
        initially_deferred: false,
        definition: Some("FOREIGN KEY (c_id) REFERENCES test_cycle.c(id)".to_string()),
        coninhcount: 0,
        is_enforced: true,
        no_inherit: false,
        nulls_not_distinct: false,
        comment: None,
    });
    a_from.hash();
    from_dump.tables.push(a_from);
    from_dump
        .tables
        .push(build("b", false, Some(("a_id", "test_cycle", "a"))));
    from_dump.tables.push(build("c", false, None));

    // TO: all UNLOGGED. Same constraint shapes.
    let mut a_to = build("a", true, Some(("b_id", "test_cycle", "b")));
    a_to.columns.push({
        let mut c_id = int_column("test_cycle", "a", "c_id", 3);
        c_id.is_nullable = true;
        c_id
    });
    a_to.constraints.push(TableConstraint {
        catalog: "postgres".to_string(),
        schema: "test_cycle".to_string(),
        name: "a_c_id_fkey".to_string(),
        table_name: "a".to_string(),
        constraint_type: "FOREIGN KEY".to_string(),
        is_deferrable: false,
        initially_deferred: false,
        definition: Some("FOREIGN KEY (c_id) REFERENCES test_cycle.c(id)".to_string()),
        coninhcount: 0,
        is_enforced: true,
        no_inherit: false,
        nulls_not_distinct: false,
        comment: None,
    });
    a_to.hash();
    to_dump.tables.push(a_to);
    to_dump
        .tables
        .push(build("b", true, Some(("a_id", "test_cycle", "a"))));
    to_dump.tables.push(build("c", true, None));

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_tables().await.unwrap();
    let script = comparer.get_script();

    // Cycle banner must appear (a↔b cycle is present).
    assert!(
        script.contains("Persistence-flip FK cycle (issue #191)"),
        "cycle banner must appear for the a↔b cycle: {}",
        script
    );
    // True cycle FKs must be dropped.
    assert!(
        script.contains("alter table test_cycle.a drop constraint a_b_id_fkey;"),
        "true cycle FK a_b_id_fkey must be dropped: {}",
        script
    );
    assert!(
        script.contains("alter table test_cycle.b drop constraint b_a_id_fkey;"),
        "true cycle FK b_a_id_fkey must be dropped: {}",
        script
    );
    // The non-cycle FK (a → c) is merely *blocked by* the cycle in
    // Kahn's remainder but is not part of any directed cycle. With
    // SCC-based detection it must NOT be dropped.
    assert!(
        !script.contains("drop constraint a_c_id_fkey"),
        "non-cycle FK a_c_id_fkey (a → c) must NOT be dropped: {}",
        script
    );
}

/// Issue #191 / PR #198 review: when `use_drop=false`, the cycle-
/// break drops and re-adds must be commented out, with a loud banner
/// explaining that the SETs will fail without manual intervention.
/// The user has explicitly asked the comparer to surface destructive
/// statements for review rather than emit them live.
#[tokio::test]
async fn issue191_pr198_use_drop_false_comments_out_cycle_break() {
    fn make_cycle_table(name: &str, is_unlogged: bool, fk: (&str, &str, &str)) -> Table {
        let (fk_col, target_schema, target_table) = fk;
        let mut id_col = int_column("test_cycle", name, "id", 1);
        id_col.is_nullable = false;
        let mut ref_col = int_column("test_cycle", name, fk_col, 2);
        ref_col.is_nullable = true;

        let pk = TableConstraint {
            catalog: "postgres".to_string(),
            schema: "test_cycle".to_string(),
            name: format!("{name}_pkey"),
            table_name: name.to_string(),
            constraint_type: "PRIMARY KEY".to_string(),
            is_deferrable: false,
            initially_deferred: false,
            definition: Some("PRIMARY KEY (id)".to_string()),
            coninhcount: 0,
            is_enforced: true,
            no_inherit: false,
            nulls_not_distinct: false,
            comment: None,
        };
        let fk_constraint = TableConstraint {
            catalog: "postgres".to_string(),
            schema: "test_cycle".to_string(),
            name: format!("{name}_{fk_col}_fkey"),
            table_name: name.to_string(),
            constraint_type: "FOREIGN KEY".to_string(),
            is_deferrable: true,
            initially_deferred: true,
            definition: Some(format!(
                "FOREIGN KEY ({fk_col}) REFERENCES {target_schema}.{target_table}(id) DEFERRABLE INITIALLY DEFERRED"
            )),
            coninhcount: 0,
            is_enforced: true,
            no_inherit: false,
            nulls_not_distinct: false,
            comment: None,
        };

        let mut table = Table::new(
            "test_cycle".to_string(),
            name.to_string(),
            "test_cycle".to_string(),
            name.to_string(),
            "postgres".to_string(),
            None,
            vec![id_col, ref_col],
            vec![pk, fk_constraint],
            vec![],
            vec![],
            None,
        );
        table.is_unlogged = is_unlogged;
        table.hash();
        table
    }

    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());
    from_dump
        .tables
        .push(make_cycle_table("a", false, ("b_id", "test_cycle", "b")));
    from_dump
        .tables
        .push(make_cycle_table("b", false, ("a_id", "test_cycle", "a")));
    to_dump
        .tables
        .push(make_cycle_table("a", true, ("b_id", "test_cycle", "b")));
    to_dump
        .tables
        .push(make_cycle_table("b", true, ("a_id", "test_cycle", "a")));

    // use_drop=false — drops and re-adds must be commented out.
    // `use_comments=true` so the banner and commented-out lines
    // survive `get_script`'s output (which strips comments under
    // `use_comments=false`).
    let mut comparer = Comparer::new(from_dump, to_dump, false, false, true, GrantsMode::Ignore);
    comparer.compare_tables().await.unwrap();
    let script = comparer.get_script();

    // Loud banner specifically calls out use_drop=false semantics.
    assert!(
        script.contains("use_drop=false"),
        "banner must mention use_drop=false: {}",
        script
    );
    // DROP CONSTRAINT lines must be commented out (i.e. they appear
    // only as `-- alter table ... drop constraint ...;`).
    assert!(
        !script.contains("\nalter table test_cycle.a drop constraint a_b_id_fkey;"),
        "live DROP CONSTRAINT must NOT be emitted under use_drop=false: {}",
        script
    );
    assert!(
        script.contains("-- alter table test_cycle.a drop constraint a_b_id_fkey;"),
        "commented DROP CONSTRAINT must be emitted under use_drop=false: {}",
        script
    );
    // ADD CONSTRAINT lines must also be commented out so re-running
    // with use_drop=true after manual review produces a clean diff.
    assert!(
        !script.contains("\nalter table test_cycle.a add constraint a_b_id_fkey foreign key"),
        "live ADD CONSTRAINT must NOT be emitted under use_drop=false: {}",
        script
    );
    assert!(
        script.contains("-- alter table test_cycle.a add constraint a_b_id_fkey foreign key"),
        "commented ADD CONSTRAINT must be emitted under use_drop=false: {}",
        script
    );
    // SET statements are NOT destructive and stay live, matching how
    // SETs are handled elsewhere when use_drop=false (the cycle case
    // is highlighted by the banner above).
    assert!(
        script.contains("alter table test_cycle.a set unlogged;"),
        "SET UNLOGGED must remain live under use_drop=false: {}",
        script
    );
}

/// Issue #191 / PR #198 review: the cycle-break re-emit path goes
/// through `TableConstraint::get_script()`, which appends
/// `COMMENT ON CONSTRAINT ...` when the FK has a comment. Verify
/// the comment survives the drop+SET+re-add round-trip — i.e. the
/// emitted re-add carries the `comment on constraint` clause from
/// the TO-side metadata so the post-migration schema matches TO.
#[tokio::test]
async fn issue191_pr198_cycle_fk_comment_survives_round_trip() {
    fn cycle_table_with_comment(name: &str, is_unlogged: bool, fk: (&str, &str, &str)) -> Table {
        let (fk_col, target_schema, target_table) = fk;
        let mut id_col = int_column("test_cycle", name, "id", 1);
        id_col.is_nullable = false;
        let mut ref_col = int_column("test_cycle", name, fk_col, 2);
        ref_col.is_nullable = true;

        let pk = TableConstraint {
            catalog: "postgres".to_string(),
            schema: "test_cycle".to_string(),
            name: format!("{name}_pkey"),
            table_name: name.to_string(),
            constraint_type: "PRIMARY KEY".to_string(),
            is_deferrable: false,
            initially_deferred: false,
            definition: Some("PRIMARY KEY (id)".to_string()),
            coninhcount: 0,
            is_enforced: true,
            no_inherit: false,
            nulls_not_distinct: false,
            comment: None,
        };
        let fk_constraint = TableConstraint {
            catalog: "postgres".to_string(),
            schema: "test_cycle".to_string(),
            name: format!("{name}_{fk_col}_fkey"),
            table_name: name.to_string(),
            constraint_type: "FOREIGN KEY".to_string(),
            is_deferrable: true,
            initially_deferred: true,
            definition: Some(format!(
                "FOREIGN KEY ({fk_col}) REFERENCES {target_schema}.{target_table}(id) DEFERRABLE INITIALLY DEFERRED"
            )),
            coninhcount: 0,
            is_enforced: true,
            no_inherit: false,
            nulls_not_distinct: false,
            comment: Some(format!("FK {name} → {target_table} (cycle annotated)")),
        };

        let mut table = Table::new(
            "test_cycle".to_string(),
            name.to_string(),
            "test_cycle".to_string(),
            name.to_string(),
            "postgres".to_string(),
            None,
            vec![id_col, ref_col],
            vec![pk, fk_constraint],
            vec![],
            vec![],
            None,
        );
        table.is_unlogged = is_unlogged;
        table.hash();
        table
    }

    let mut from_dump = Dump::new(DumpConfig::default());
    let mut to_dump = Dump::new(DumpConfig::default());
    from_dump.tables.push(cycle_table_with_comment(
        "a",
        false,
        ("b_id", "test_cycle", "b"),
    ));
    from_dump.tables.push(cycle_table_with_comment(
        "b",
        false,
        ("a_id", "test_cycle", "a"),
    ));
    to_dump.tables.push(cycle_table_with_comment(
        "a",
        true,
        ("b_id", "test_cycle", "b"),
    ));
    to_dump.tables.push(cycle_table_with_comment(
        "b",
        true,
        ("a_id", "test_cycle", "a"),
    ));

    let mut comparer = Comparer::new(from_dump, to_dump, true, false, true, GrantsMode::Ignore);
    comparer.compare_tables().await.unwrap();
    let script = comparer.get_script();

    // The re-add must include the `comment on constraint` clause —
    // proof that the cycle-break path round-trips full
    // `TableConstraint` metadata via `get_script()`, not just the
    // raw `(schema, table, name, definition)` tuple.
    assert!(
        script.contains(
            "comment on constraint a_b_id_fkey on test_cycle.a is 'FK a → b (cycle annotated)';"
        ),
        "FK comment on a_b_id_fkey must be re-emitted after the SET: {}",
        script
    );
    assert!(
        script.contains(
            "comment on constraint b_a_id_fkey on test_cycle.b is 'FK b → a (cycle annotated)';"
        ),
        "FK comment on b_a_id_fkey must be re-emitted after the SET: {}",
        script
    );
}
