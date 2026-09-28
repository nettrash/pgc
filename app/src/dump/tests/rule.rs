use super::*;

#[test]
fn test_rule_hash() {
    let mut r = Rule::new(
        "public".into(),
        "orders".into(),
        "protect_delete".into(),
        "CREATE OR REPLACE RULE protect_delete AS ON DELETE TO public.orders DO INSTEAD NOTHING"
            .into(),
        None,
    );
    assert!(r.hash.is_some());
    let h1 = r.hash.clone();
    r.definition =
        "CREATE OR REPLACE RULE protect_delete AS ON DELETE TO public.orders DO ALSO NOTHING"
            .into();
    r.hash();
    assert_ne!(h1, r.hash);
}

#[test]
fn test_get_script() {
    let r = Rule::new(
        "public".into(),
        "orders".into(),
        "protect_delete".into(),
        "CREATE OR REPLACE RULE protect_delete AS ON DELETE TO public.orders DO INSTEAD NOTHING"
            .into(),
        Some("Prevent deletes".into()),
    );
    let script = r.get_script();
    assert!(script.contains("CREATE OR REPLACE RULE"));
    assert!(script.contains("comment on rule"));
    assert!(script.contains("Prevent deletes"));
}

#[test]
fn test_get_drop_script() {
    let r = Rule::new(
        "public".into(),
        "orders".into(),
        "protect_delete".into(),
        "CREATE OR REPLACE RULE protect_delete AS ON DELETE TO public.orders DO INSTEAD NOTHING"
            .into(),
        None,
    );
    let script = r.get_drop_script();
    assert!(script.contains("drop rule if exists"));
    assert!(script.contains("cascade"));
}

#[test]
fn test_get_alter_script_definition_change() {
    let from = Rule::new(
        "public".into(),
        "orders".into(),
        "protect_delete".into(),
        "CREATE OR REPLACE RULE protect_delete AS ON DELETE TO public.orders DO INSTEAD NOTHING"
            .into(),
        None,
    );
    let to = Rule::new(
        "public".into(),
        "orders".into(),
        "protect_delete".into(),
        "CREATE OR REPLACE RULE protect_delete AS ON DELETE TO public.orders DO ALSO NOTHING"
            .into(),
        None,
    );
    let script = from.get_alter_script(&to, true);
    assert!(script.contains("DO ALSO NOTHING"));
}

// ---- CREATE OR REPLACE RULE, ';;', quoting, dependencies ----

/// `pg_get_ruledef(oid, true)` as PostgreSQL prints it: `CREATE RULE`, never
/// `CREATE OR REPLACE`, and a trailing semicolon.
fn ruledef(name: &str, body: &str) -> String {
    format!("CREATE RULE {name} AS\n    {body};")
}

fn rule_with(schema: &str, table: &str, name: &str, body: &str, comment: Option<&str>) -> Rule {
    Rule::new(
        schema.into(),
        table.into(),
        name.into(),
        ruledef(name, body),
        comment.map(str::to_string),
    )
}

#[test]
fn test_hash_is_pinned_and_ignores_dependencies() {
    // The hash is what an older dump stored: it must not move, and the
    // dependency arrays (which follow from the definition) must not feed it.
    let mut r = rule_with(
        "public",
        "orders",
        "protect_delete",
        "ON DELETE TO public.orders DO INSTEAD NOTHING",
        None,
    );
    assert_eq!(
        r.hash.as_deref(),
        Some("3054af4232aa7827b665d205599e80926c2543d2da746d53d4138d3c9df43829")
    );
    r.depends_on = Some(RuleDependencies {
        relations: vec!["public.orders".into()],
        columns: vec!["public.orders.id".into()],
        routines: vec!["public.f(p integer)".into()],
    });
    r.hash();
    assert_eq!(
        r.hash.as_deref(),
        Some("3054af4232aa7827b665d205599e80926c2543d2da746d53d4138d3c9df43829")
    );

    let commented = rule_with(
        "public",
        "orders",
        "protect_delete",
        "ON DELETE TO public.orders DO INSTEAD NOTHING",
        Some("Prevent deletes"),
    );
    assert_eq!(
        commented.hash.as_deref(),
        Some("ce2cdc49e296eb48d8c367d8696f97044f50fc62a235bb4a3226a0206966dfe4")
    );
}

#[test]
fn test_get_script_ends_in_one_semicolon() {
    let r = rule_with(
        "public",
        "orders",
        "protect_delete",
        "ON DELETE TO public.orders DO INSTEAD NOTHING",
        None,
    );
    assert_eq!(
        r.get_script(),
        "CREATE RULE protect_delete AS\n    ON DELETE TO public.orders DO INSTEAD NOTHING;\n\n"
    );
}

#[test]
fn test_get_script_multi_action_rule_ends_in_one_semicolon() {
    // A multi-action rule's definition ends in ");" — the ';' inside the
    // parentheses belongs to the actions and must stay.
    let definition = "CREATE RULE r_multi AS\n    ON UPDATE TO public.t DO INSTEAD ( INSERT INTO public.log (id)\n  VALUES (new.id);\n INSERT INTO public.log (id)\n  VALUES (old.id);\n);";
    let r = Rule::new(
        "public".into(),
        "t".into(),
        "r_multi".into(),
        definition.into(),
        None,
    );
    let script = r.get_script();
    assert!(script.ends_with("  VALUES (old.id);\n);\n\n"), "{script}");
    assert!(!script.contains(";;"), "{script}");
    assert_eq!(script.matches(';').count(), 3, "{script}");
}

#[test]
fn test_get_script_with_comment() {
    let r = rule_with(
        "public",
        "orders",
        "protect_delete",
        "ON DELETE TO public.orders DO INSTEAD NOTHING",
        Some("It's protected"),
    );
    assert_eq!(
        r.get_script(),
        "CREATE RULE protect_delete AS\n    ON DELETE TO public.orders DO INSTEAD NOTHING;\n\n\
         comment on rule \"protect_delete\" on public.\"orders\" is 'It''s protected';\n\n"
    );
}

#[test]
fn test_get_replace_script_turns_create_rule_into_create_or_replace() {
    let r = rule_with(
        "public",
        "orders",
        "protect_delete",
        "ON DELETE TO public.orders DO INSTEAD NOTHING",
        Some("c"),
    );
    assert_eq!(
        r.get_replace_script(),
        "CREATE OR REPLACE RULE protect_delete AS\n    ON DELETE TO public.orders DO INSTEAD NOTHING;\n\n\
         comment on rule \"protect_delete\" on public.\"orders\" is 'c';\n\n"
    );
}

#[test]
fn test_get_replace_script_is_case_insensitive_and_idempotent() {
    let lower = Rule::new(
        "public".into(),
        "t".into(),
        "r".into(),
        "create rule r AS ON DELETE TO public.t DO INSTEAD NOTHING;".into(),
        None,
    );
    assert_eq!(
        lower.get_replace_script(),
        "CREATE OR REPLACE RULE r AS ON DELETE TO public.t DO INSTEAD NOTHING;\n\n"
    );
    // Already OR REPLACE (hand-built definitions in older tests): untouched.
    let already = Rule::new(
        "public".into(),
        "t".into(),
        "r".into(),
        "CREATE OR REPLACE RULE r AS ON DELETE TO public.t DO INSTEAD NOTHING".into(),
        None,
    );
    assert_eq!(
        already.get_replace_script(),
        "CREATE OR REPLACE RULE r AS ON DELETE TO public.t DO INSTEAD NOTHING;\n\n"
    );
}

#[test]
fn test_get_alter_script_uses_create_or_replace() {
    // Applying `CREATE RULE` to a rule that exists fails with
    // `rule "..." for relation "..." already exists`.
    let from = rule_with(
        "public",
        "orders",
        "protect_delete",
        "ON DELETE TO public.orders DO INSTEAD NOTHING",
        None,
    );
    let to = rule_with(
        "public",
        "orders",
        "protect_delete",
        "ON DELETE TO public.orders DO NOTHING",
        None,
    );
    assert_eq!(
        from.get_alter_script(&to, true),
        "CREATE OR REPLACE RULE protect_delete AS\n    ON DELETE TO public.orders DO NOTHING;\n\n"
    );
}

#[test]
fn test_get_alter_script_pg19_on_conflict_do_select() {
    // PG19's ON CONFLICT DO SELECT, as pg_get_ruledef prints it.
    let from = rule_with(
        "public",
        "front",
        "r_upsert",
        "ON INSERT TO public.front DO INSTEAD  INSERT INTO public.target (id)\n  VALUES (new.id) ON CONFLICT(id) DO NOTHING",
        None,
    );
    let to = rule_with(
        "public",
        "front",
        "r_upsert",
        "ON INSERT TO public.front DO INSTEAD  INSERT INTO public.target (id)\n  VALUES (new.id) ON CONFLICT(id) DO SELECT FOR UPDATE\n  RETURNING target.id",
        None,
    );
    let script = from.get_alter_script(&to, true);
    assert!(
        script.starts_with("CREATE OR REPLACE RULE r_upsert AS\n"),
        "{script}"
    );
    assert!(
        script.ends_with("DO SELECT FOR UPDATE\n  RETURNING target.id;\n\n"),
        "{script}"
    );
}

#[test]
fn test_get_alter_script_comment_only_change() {
    let from = rule_with(
        "public",
        "orders",
        "protect_delete",
        "ON DELETE TO public.orders DO INSTEAD NOTHING",
        Some("old"),
    );
    let mut to = from.clone();
    to.comment = Some("new".into());
    to.hash();
    assert_eq!(
        from.get_alter_script(&to, true),
        "comment on rule \"protect_delete\" on public.\"orders\" is 'new';\n\n"
    );

    to.comment = None;
    to.hash();
    assert_eq!(
        from.get_alter_script(&to, true),
        "comment on rule \"protect_delete\" on public.\"orders\" is null;\n\n"
    );
    assert_eq!(from.get_alter_script(&to, false), "");
}

#[test]
fn test_get_drop_script_plain_names_unchanged() {
    // Byte-identical to what every earlier version emitted.
    let r = rule_with(
        "public",
        "orders",
        "protect_delete",
        "ON DELETE TO public.orders DO INSTEAD NOTHING",
        None,
    );
    assert_eq!(
        r.get_drop_script(),
        "drop rule if exists \"protect_delete\" on public.\"orders\" cascade;\n\n"
    );
}

#[test]
fn test_drop_and_comment_scripts_do_not_requote_quoted_names() {
    // The dump stores names quote_ident-ed. Wrapping `"Rule X"` in quotes
    // again produced `"""Rule X"""` — another name — and the drop was a
    // silent no-op (`relation ... does not exist, skipping`).
    let mut r = Rule::new(
        "test_schema".into(),
        "\"Mixed Tab\"".into(),
        "\"Rule X\"".into(),
        "CREATE RULE \"Rule X\" AS\n    ON DELETE TO test_schema.\"Mixed Tab\" DO INSTEAD NOTHING;"
            .into(),
        Some("c".into()),
    );
    assert_eq!(
        r.get_drop_script(),
        "drop rule if exists \"Rule X\" on test_schema.\"Mixed Tab\" cascade;\n\n"
    );
    assert!(
        r.get_script()
            .contains("comment on rule \"Rule X\" on test_schema.\"Mixed Tab\" is 'c';"),
        "{}",
        r.get_script()
    );

    // A name with an embedded quote arrives doubled inside its quotes.
    r.rule_name = "\"say \"\"hi\"\"\"".into();
    assert_eq!(
        r.get_drop_script(),
        "drop rule if exists \"say \"\"hi\"\"\" on test_schema.\"Mixed Tab\" cascade;\n\n"
    );
}

#[test]
fn test_new_rule_has_no_dependencies_recorded() {
    let r = rule_with(
        "public",
        "t",
        "r",
        "ON DELETE TO public.t DO INSTEAD NOTHING",
        None,
    );
    assert_eq!(r.depends_on, None);
}

#[test]
fn test_get_recreate_script_clears_a_removed_comment() {
    // The rule may still exist with its FROM comment (a view flagged as taken
    // by a routine's CASCADE on a textual match only), so a comment TO dropped
    // is cleared after the OR REPLACE. Harmless on a recreated rule.
    let from = rule_with(
        "public",
        "orders",
        "protect_delete",
        "ON DELETE TO public.orders DO INSTEAD NOTHING",
        Some("old"),
    );
    let to = rule_with(
        "public",
        "orders",
        "protect_delete",
        "ON DELETE TO public.orders DO INSTEAD NOTHING",
        None,
    );
    assert_eq!(
        to.get_recreate_script(&from, true),
        "CREATE OR REPLACE RULE protect_delete AS\n    ON DELETE TO public.orders DO INSTEAD NOTHING;\n\n\
         comment on rule \"protect_delete\" on public.\"orders\" is null;\n\n"
    );
    // use_drop off leaves a removed comment alone, as get_alter_script does.
    assert_eq!(
        to.get_recreate_script(&from, false),
        to.get_replace_script()
    );
}

#[test]
fn test_get_recreate_script_matches_replace_script_otherwise() {
    let with_old = rule_with(
        "public",
        "orders",
        "protect_delete",
        "ON DELETE TO public.orders DO INSTEAD NOTHING",
        Some("old"),
    );
    let with_new = rule_with(
        "public",
        "orders",
        "protect_delete",
        "ON DELETE TO public.orders DO INSTEAD NOTHING",
        Some("new"),
    );
    let without = rule_with(
        "public",
        "orders",
        "protect_delete",
        "ON DELETE TO public.orders DO INSTEAD NOTHING",
        None,
    );
    // TO has a comment: it is written, whatever FROM had.
    assert_eq!(
        with_new.get_recreate_script(&with_old, true),
        with_new.get_replace_script()
    );
    assert_eq!(
        with_new.get_recreate_script(&without, true),
        with_new.get_replace_script()
    );
    // Neither has one: nothing to clear.
    assert_eq!(
        without.get_recreate_script(&without, true),
        without.get_replace_script()
    );
}
