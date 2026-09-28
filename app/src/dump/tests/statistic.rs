use super::*;

fn make_statistic() -> Statistic {
    Statistic::new(
        "public".to_string(),
        "my_stat".to_string(),
        "postgres".to_string(),
        "public".to_string(),
        "my_table".to_string(),
        vec!["ndistinct".to_string(), "dependencies".to_string()],
        vec!["col1".to_string(), "col2".to_string()],
        "CREATE STATISTICS public.my_stat (ndistinct, dependencies) ON col1, col2 FROM public.my_table".to_string(),
    )
}

#[test]
fn hash_populates_hash_field() {
    let stat = make_statistic();
    assert!(stat.hash.is_some());
}

#[test]
fn hash_is_consistent() {
    let s1 = make_statistic();
    let s2 = make_statistic();
    assert_eq!(s1.hash, s2.hash);
}

#[test]
fn hash_differs_with_different_name() {
    let s1 = make_statistic();
    let mut s2 = make_statistic();
    s2.name = "other_stat".to_string();
    s2.hash();
    assert_ne!(s1.hash, s2.hash);
}

#[test]
fn get_script_creates_statistics() {
    let stat = make_statistic();
    let script = stat.get_script();
    assert!(script.contains("create statistics public.my_stat"));
    assert!(script.contains("(ndistinct, dependencies)"));
    assert!(script.contains("on col1, col2"));
    assert!(script.contains("from public.my_table"));
}

#[test]
fn get_script_includes_owner() {
    let stat = make_statistic();
    let script = stat.get_script();
    assert!(script.contains("alter statistics public.my_stat owner to postgres;"));
}

#[test]
fn get_script_includes_comment() {
    let mut stat = make_statistic();
    stat.comment = Some("test comment".to_string());
    let script = stat.get_script();
    assert!(script.contains("comment on statistics public.my_stat is 'test comment';"));
}

#[test]
fn get_drop_script() {
    let stat = make_statistic();
    let script = stat.get_drop_script();
    assert!(script.contains("drop statistics if exists public.my_stat;"));
}

#[test]
fn get_alter_script_owner_change() {
    let s1 = make_statistic();
    let mut s2 = make_statistic();
    s2.owner = "new_owner".to_string();
    let script = s1.get_alter_script(&s2, true);
    assert!(script.contains("alter statistics public.my_stat owner to new_owner;"));
}

#[test]
fn get_alter_script_definition_change_drops_recreates() {
    let s1 = make_statistic();
    let mut s2 = make_statistic();
    s2.definition =
        "CREATE STATISTICS public.my_stat (mcv) ON col1, col2, col3 FROM public.my_table"
            .to_string();
    s2.kinds = vec!["mcv".to_string()];
    s2.columns = vec!["col1".to_string(), "col2".to_string(), "col3".to_string()];
    let script = s1.get_alter_script(&s2, true);
    assert!(script.contains("drop statistics if exists public.my_stat;"));
    assert!(script.contains("create statistics public.my_stat"));
}

#[test]
fn get_alter_script_comment_change() {
    let s1 = make_statistic();
    let mut s2 = make_statistic();
    s2.comment = Some("new comment".to_string());
    let script = s1.get_alter_script(&s2, true);
    assert!(script.contains("comment on statistics public.my_stat is 'new comment';"));
}

#[test]
fn get_alter_script_no_changes() {
    let s1 = make_statistic();
    let s2 = make_statistic();
    let script = s1.get_alter_script(&s2, true);
    assert!(script.is_empty());
}

#[test]
fn get_alter_script_definition_change_use_drop_false_comments_out() {
    let s1 = make_statistic();
    let mut s2 = make_statistic();
    s2.kinds = vec!["mcv".to_string()];
    s2.columns = vec!["col1".to_string(), "col2".to_string(), "col3".to_string()];
    let script = s1.get_alter_script(&s2, false);
    assert!(script.contains("-- use_drop=false"));
    assert!(script.contains("-- drop statistics"));
    assert!(script.contains("-- create statistics"));
    assert!(!script.contains("\ndrop statistics"));
}

// ---- Expression statistics and the ON list (pgc 1.1.0) ----

/// A statistics object on expressions as `fetch_statistics_standalone`
/// builds it: `kinds` straight from `stxkind` (so `'e'` is `"expressions"`),
/// `columns` from the lossy `,` split of `definition` (reproduced here, since
/// the dump's splitter is private to `dump::core`), and `columns_def` from
/// `pg_get_statisticsobjdef_columns`.
fn make_expression_statistic(kinds: &[&str], definition: &str, columns_def: &str) -> Statistic {
    let upper = definition.to_uppercase();
    let columns = upper
        .find(" ON ")
        .and_then(|on| {
            let after = &definition[on + 4..];
            after.to_uppercase().find(" FROM ").map(|from| {
                after[..from]
                    .split(',')
                    .map(|c| c.trim().to_string())
                    .filter(|c| !c.is_empty())
                    .collect()
            })
        })
        .unwrap_or_default();
    let mut stat = Statistic::new(
        "s".to_string(),
        "st".to_string(),
        String::new(),
        "s".to_string(),
        "t".to_string(),
        kinds.iter().map(|k| k.to_string()).collect(),
        columns,
        definition.to_string(),
    );
    stat.columns_def = Some(columns_def.to_string());
    stat
}

#[test]
fn get_script_output_for_a_plain_statistic_is_unchanged() {
    // Pinned byte-for-byte: plain-column statistics dumped from PostgreSQL
    // 14-18 must render exactly as pgc 1.0.x rendered them.
    let stat = make_statistic();
    assert_eq!(
        stat.get_script(),
        "create statistics public.my_stat (ndistinct, dependencies) on col1, col2 from public.my_table;\n\n\
         alter statistics public.my_stat owner to postgres;\n\n"
    );
    let mut with_columns_def = make_statistic();
    with_columns_def.columns_def = Some("col1, col2".to_string());
    assert_eq!(with_columns_def.get_script(), stat.get_script());
}

#[test]
fn hash_is_pinned_and_ignores_columns_def() {
    // The hash must not move across pgc versions, or every statistics object
    // compared against an older dump would be dropped and recreated.
    let stat = make_statistic();
    assert_eq!(
        stat.hash.as_deref(),
        Some("10f6c43fd79f4d90de8d41433493f288d6a028b681879d53ae34b84bb131d23a")
    );
    let mut with_columns_def = make_statistic();
    with_columns_def.columns_def = Some("col1, col2".to_string());
    with_columns_def.hash();
    assert_eq!(with_columns_def.hash, stat.hash);
}

#[test]
fn kinds_clause_drops_expressions_but_keeps_the_rest() {
    let stat = make_expression_statistic(
        &["mcv", "expressions"],
        "CREATE STATISTICS s.st (mcv) ON a, (b * 3) FROM s.t",
        "a, (b * 3)",
    );
    assert_eq!(stat.kinds_clause(), " (mcv)");
    let script = stat.get_script();
    assert!(
        script.starts_with("create statistics s.st (mcv) on a, (b * 3) from s.t;"),
        "{script}"
    );
    assert!(!script.contains("expressions"), "{script}");
}

#[test]
fn kinds_clause_keeps_the_full_list_for_multivariate_expression_statistics() {
    let stat = make_expression_statistic(
        &["ndistinct", "dependencies", "mcv", "expressions"],
        "CREATE STATISTICS s.st ON a, b, (v) FROM s.t",
        "a, b, (v)",
    );
    assert!(
        stat.get_script().starts_with(
            "create statistics s.st (ndistinct, dependencies, mcv) on a, b, (v) from s.t;"
        ),
        "{}",
        stat.get_script()
    );
}

#[test]
fn univariate_statistics_carry_no_kind_list() {
    // PG19: a single virtual generated column is stored as an expression;
    // PG14+: a single expression. Both reject any kind list.
    let on_virtual_column = make_expression_statistic(
        &["expressions"],
        "CREATE STATISTICS s.st ON (v) FROM s.t",
        "(v)",
    );
    assert_eq!(on_virtual_column.kinds_clause(), "");
    assert!(
        on_virtual_column
            .get_script()
            .starts_with("create statistics s.st on (v) from s.t;"),
        "{}",
        on_virtual_column.get_script()
    );

    let on_expression = make_expression_statistic(
        &["expressions"],
        "CREATE STATISTICS s.st ON lower(code) FROM s.t",
        "lower(code)",
    );
    assert!(
        on_expression
            .get_script()
            .starts_with("create statistics s.st on lower(code) from s.t;"),
        "{}",
        on_expression.get_script()
    );
}

#[test]
fn get_script_emits_columns_def_rather_than_the_split() {
    let definition = "CREATE STATISTICS s.st ON qty, COALESCE(code, ','::text), EXTRACT(year FROM created_on) FROM s.t";
    let stat = make_expression_statistic(
        &["ndistinct", "dependencies", "mcv", "expressions"],
        definition,
        "qty, COALESCE(code, ','::text), EXTRACT(year FROM created_on)",
    );
    // The split mangles both the literal and the EXTRACT.
    assert_eq!(
        stat.columns,
        vec!["qty", "COALESCE(code", "'", "'::text)", "EXTRACT(year"]
    );
    assert!(
        stat.get_script().starts_with(
            "create statistics s.st (ndistinct, dependencies, mcv) on qty, COALESCE(code, ','::text), EXTRACT(year FROM created_on) from s.t;"
        ),
        "{}",
        stat.get_script()
    );
}

#[test]
fn get_script_for_an_old_dump_cuts_the_on_list_out_of_the_definition() {
    // A pgc 1.0.x dump has no columns_def; the ON list must still come out
    // parenthesis-balanced, or psql swallows the rest of the migration.
    let mut stat = make_expression_statistic(
        &["ndistinct", "dependencies", "mcv", "expressions"],
        "CREATE STATISTICS s.st ON qty, COALESCE(code, ','::text), EXTRACT(year FROM created_on) FROM s.t",
        "unused",
    );
    stat.columns_def = None;
    assert!(
        stat.get_script().starts_with(
            "create statistics s.st (ndistinct, dependencies, mcv) on qty, COALESCE(code, ','::text), EXTRACT(year FROM created_on) from s.t;"
        ),
        "{}",
        stat.get_script()
    );
}

#[test]
fn get_script_falls_back_to_the_split_when_the_definition_has_no_on_list() {
    let mut stat = make_statistic();
    stat.definition = "not a statistics definition".to_string();
    assert!(
        stat.get_script().starts_with(
            "create statistics public.my_stat (ndistinct, dependencies) on col1, col2 from"
        ),
        "{}",
        stat.get_script()
    );
}

#[test]
fn on_list_from_definition_respects_quotes_and_parentheses() {
    let cases = [
        (
            "CREATE STATISTICS public.my_stat (ndistinct, dependencies) ON col1, col2 FROM public.my_table",
            Some("col1, col2"),
        ),
        (
            "CREATE STATISTICS s.st ON EXTRACT(year FROM d) FROM s.t",
            Some("EXTRACT(year FROM d)"),
        ),
        (
            "CREATE STATISTICS s.st ON n, (c || ' FROM , '::text) FROM s.t",
            Some("n, (c || ' FROM , '::text)"),
        ),
        (
            "CREATE STATISTICS s.\"x ON y\" ON \"a FROM b\", \"it\"\" ON c\" FROM s.\"t FROM\"",
            Some("\"a FROM b\", \"it\"\" ON c\""),
        ),
        (
            "CREATE STATISTICS s.st (mcv) ON a, (b * 3) FROM s.t",
            Some("a, (b * 3)"),
        ),
        ("CREATE STATISTICS s.st FROM s.t", None),
        ("CREATE STATISTICS s.st ON  FROM s.t", None),
        ("", None),
    ];
    for (definition, expected) in cases {
        assert_eq!(
            Statistic::on_list_from_definition(definition).as_deref(),
            expected,
            "definition: {definition}"
        );
    }
}

#[test]
fn get_alter_script_sees_an_on_list_change_the_split_cannot() {
    let from = make_expression_statistic(
        &["ndistinct", "dependencies", "mcv", "expressions"],
        "CREATE STATISTICS s.st ON qty, EXTRACT(year FROM d1) FROM s.t",
        "qty, EXTRACT(year FROM d1)",
    );
    let to = make_expression_statistic(
        &["ndistinct", "dependencies", "mcv", "expressions"],
        "CREATE STATISTICS s.st ON qty, EXTRACT(year FROM d2) FROM s.t",
        "qty, EXTRACT(year FROM d2)",
    );
    assert_eq!(from.columns, to.columns, "the split cannot tell them apart");
    assert_ne!(from.hash, to.hash);

    let script = from.get_alter_script(&to, true);
    assert!(
        script.starts_with("drop statistics if exists s.st;"),
        "{script}"
    );
    assert!(
        script.contains("on qty, EXTRACT(year FROM d2) from s.t;"),
        "{script}"
    );

    // The same holds for two pgc 1.0.x dumps, through the definition.
    let (mut old_from, mut old_to) = (from.clone(), to.clone());
    old_from.columns_def = None;
    old_to.columns_def = None;
    assert!(
        old_from
            .get_alter_script(&old_to, true)
            .contains("on qty, EXTRACT(year FROM d2) from s.t;")
    );
}

#[test]
fn get_alter_script_does_not_recreate_across_old_and_new_dumps() {
    // FROM read from a pgc 1.0.x dump, TO from 1.1.0: the same object must
    // not look structurally changed just because only one side has
    // columns_def. Only the owner differs here.
    let mut old = make_expression_statistic(
        &["mcv", "expressions"],
        "CREATE STATISTICS s.st (mcv) ON a, COALESCE(b, 0) FROM s.t",
        "unused",
    );
    old.columns_def = None;
    old.hash();
    let mut new = make_expression_statistic(
        &["mcv", "expressions"],
        "CREATE STATISTICS s.st (mcv) ON a, COALESCE(b, 0) FROM s.t",
        "a, COALESCE(b, 0)",
    );
    new.owner = "someone".to_string();
    new.hash();

    let script = old.get_alter_script(&new, true);
    assert_eq!(
        script, "alter statistics s.st owner to someone;\n\n",
        "only the owner changed: {script}"
    );
}

#[test]
fn columns_def_is_optional_in_serialized_form() {
    let stat = make_statistic();
    let json = serde_json::to_value(&stat).expect("serialize");
    assert!(
        json.get("columns_def").is_none(),
        "None must not be written, so the dump stays byte-identical"
    );

    let mut with_columns_def = make_statistic();
    with_columns_def.columns_def = Some("col1, col2".to_string());
    let json = serde_json::to_string(&with_columns_def).expect("serialize");
    let back: Statistic = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(back.columns_def.as_deref(), Some("col1, col2"));

    // A pgc 1.0.x statistic has no columns_def key at all.
    let legacy = r#"{
        "schema": "s", "name": "st", "owner": "", "table_schema": "s",
        "table_name": "t", "kinds": ["expressions"], "columns": ["(a + b)"],
        "definition": "CREATE STATISTICS s.st ON (a + b) FROM s.t",
        "hash": "h"
    }"#;
    let legacy: Statistic = serde_json::from_str(legacy).expect("legacy statistic loads");
    assert_eq!(legacy.columns_def, None);
    assert_eq!(legacy.hash.as_deref(), Some("h"));
    assert!(
        legacy
            .get_script()
            .starts_with("create statistics s.st on (a + b) from s.t;")
    );
}

#[test]
fn get_target_script_only_for_an_explicit_target() {
    let mut stat = make_statistic();
    assert_eq!(stat.get_target_script(), None);
    // The default: -1 before PostgreSQL 17, NULL from 17 on.
    stat.stxstattarget = Some(-1);
    assert_eq!(stat.get_target_script(), None);
    stat.stxstattarget = Some(0);
    assert_eq!(
        stat.get_target_script().as_deref(),
        Some("alter statistics public.my_stat set statistics 0;\n\n")
    );
    stat.stxstattarget = Some(250);
    assert!(
        stat.get_script()
            .contains("alter statistics public.my_stat set statistics 250;"),
    );
}

#[test]
fn requires_recreate_only_for_structural_changes() {
    let stat = make_statistic();
    let mut other = make_statistic();
    other.owner = "someone".to_string();
    other.comment = Some("c".to_string());
    other.stxstattarget = Some(10);
    assert!(!stat.requires_recreate(&other));

    let mut other = make_statistic();
    other.kinds = vec!["mcv".to_string()];
    assert!(stat.requires_recreate(&other));
    let mut other = make_statistic();
    other.table_name = "elsewhere".to_string();
    assert!(stat.requires_recreate(&other));
    // A change the `,` split cannot see.
    let mut a = make_statistic();
    let mut b = make_statistic();
    a.columns_def = Some("col1, EXTRACT(year FROM d1)".to_string());
    b.columns_def = Some("col1, EXTRACT(year FROM d2)".to_string());
    assert!(a.requires_recreate(&b));
}

/// `make_statistic` with the `ON` list `on`, as a dump of a table whose
/// columns sit in that physical order prints it.
fn statistic_on(on: &str) -> Statistic {
    let mut stat = Statistic::new(
        "public".to_string(),
        "my_stat".to_string(),
        "postgres".to_string(),
        "public".to_string(),
        "my_table".to_string(),
        vec!["ndistinct".to_string(), "dependencies".to_string()],
        on.split(',').map(|c| c.trim().to_string()).collect(),
        format!(
            "CREATE STATISTICS public.my_stat (ndistinct, dependencies) ON {on} FROM public.my_table"
        ),
    );
    stat.columns_def = Some(on.to_string());
    stat
}

#[test]
fn pre_columns_plain_column_order_is_not_a_change() {
    // PostgreSQL keeps the plain columns sorted by attnum and prints them
    // so: a table whose `col1` was dropped and re-added prints `col2, col1`.
    let from = statistic_on("col2, col1");
    let to = statistic_on("col1, col2");
    // The stored hash still covers the text, so it differs and pins the
    // hash of every object whose text does not change...
    assert_ne!(from.hash, to.hash);
    assert_eq!(to.hash, make_statistic().hash);
    // ...and the alter finds nothing to do.
    assert!(!from.requires_recreate(&to));
    assert_eq!(from.get_alter_script(&to, true), "");
    assert_eq!(from.get_alter_script(&to, false), "");
    // An old dump (no columns_def) compares the same way.
    let mut old_from = from.clone();
    old_from.columns_def = None;
    assert!(!old_from.requires_recreate(&to));
    // Quoted names too.
    assert!(!statistic_on("\"B c\", a").requires_recreate(&statistic_on("a, \"B c\"")));
}

#[test]
fn pre_columns_column_set_and_expressions_still_count() {
    // Another set of columns.
    assert!(statistic_on("col1, col2").requires_recreate(&statistic_on("col1, col3")));
    assert!(statistic_on("col1, col2").requires_recreate(&statistic_on("col1, col2, col3")));
    // Expressions keep their order, and follow the plain columns.
    assert!(
        !statistic_on("col2, col1, lower(col3), (col4 + 1)")
            .requires_recreate(&statistic_on("col1, col2, lower(col3), (col4 + 1)"))
    );
    assert!(
        statistic_on("col1, lower(col3), (col4 + 1)")
            .requires_recreate(&statistic_on("col1, (col4 + 1), lower(col3)"))
    );
    // A function call the server prints without parentheses is an
    // expression, not a column; so is a PG19 virtual column, `(v)`.
    assert!(statistic_on("col1, lower(col3)").requires_recreate(&statistic_on("col1, col3")));
    assert!(statistic_on("col1, (v)").requires_recreate(&statistic_on("col1, v")));
    // A comma inside an expression or a literal does not split it.
    assert!(
        !statistic_on("b, a, COALESCE(c, 'x,y')")
            .requires_recreate(&statistic_on("a, b, COALESCE(c, 'x,y')"))
    );
    assert!(
        statistic_on("a, COALESCE(c, 'x,y')")
            .requires_recreate(&statistic_on("a, COALESCE(c, 'x,z')"))
    );
}

#[test]
fn pre_columns_on_list_key_splits_at_top_level_commas() {
    assert_eq!(
        Statistic::split_top_level("a, lower(b, c), 'x,y', \"q,r\" , (d)"),
        vec!["a", "lower(b, c)", "'x,y'", "\"q,r\"", "(d)"]
    );
    assert!(Statistic::split_top_level("").is_empty());
    assert!(Statistic::is_identifier("col_1"));
    assert!(Statistic::is_identifier("\"Col \"\"1\"\"\""));
    assert!(!Statistic::is_identifier("lower(c)"));
    assert!(!Statistic::is_identifier("(c)"));
    assert!(!Statistic::is_identifier("\"a\" || \"b\""));
    assert_eq!(
        statistic_on("c, b, lower(a)").on_list_key(),
        (
            vec!["b".to_string(), "c".to_string()],
            vec!["lower(a)".to_string()]
        )
    );
}
