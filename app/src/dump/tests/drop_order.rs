use super::*;

// ---- keys ----

#[test]
fn keys_carry_their_kind_and_the_dumped_identity() {
    assert_eq!(relation_key("app", "\"Orders\""), "rel:app.\"Orders\"");
    assert_eq!(foreign_key_key("app", "orders", "fk"), "fk:app.orders.fk");
    assert_eq!(routine_key("app.f(p integer)"), "proc:app.f(p integer)");
    assert_eq!(type_key("app", "mood"), "type:app.mood");
    assert_eq!(statistic_key("app", "st"), "stx:app.st");
    assert_eq!(
        operator_key("app.===(integer, integer)"),
        "op:app.===(integer, integer)"
    );
    assert_eq!(collation_key("app", "c"), "coll:app.c");
    assert_eq!(ts_config_key("app", "cfg"), "tscfg:app.cfg");
    assert_eq!(ts_dict_key("app", "d"), "tsdict:app.d");
    assert_eq!(cast_key("app.mood", "text"), "cast:(app.mood AS text)");
    assert_eq!(event_trigger_key("et"), "evt:et");
    assert_eq!(fdw_key("w"), "fdw:w");
    assert_eq!(server_key("s"), "srv:s");
    assert_eq!(user_mapping_key("PUBLIC", "s"), "um:PUBLIC SERVER s");
    assert_eq!(subscription_key("sub"), "sub:sub");
    assert_eq!(extension_key("\"uuid-ossp\""), "ext:\"uuid-ossp\"");
    assert_eq!(schema_key("app"), "nsp:app");
}

#[test]
fn a_dependency_has_no_breaker_unless_given_one() {
    let plain = DropDependency::new("rel:app.t", "proc:app.f()");
    assert_eq!(plain.breaker, None);
    let breakable = plain
        .clone()
        .with_breaker("alter table if exists app.t drop constraint if exists c");
    assert_eq!(breakable.before, "rel:app.t");
    assert_eq!(breakable.after, "proc:app.f()");
    assert_eq!(
        breakable.breaker.as_deref(),
        Some("alter table if exists app.t drop constraint if exists c")
    );
}

#[test]
fn a_dependency_without_a_breaker_serializes_without_the_field() {
    let json = serde_json::to_string(&DropDependency::new("a", "b")).unwrap();
    assert_eq!(json, r#"{"before":"a","after":"b"}"#);
    let back: DropDependency = serde_json::from_str(&json).unwrap();
    assert_eq!(back, DropDependency::new("a", "b"));
}

// ---- query ----

#[test]
fn drop_dependencies_query_scopes_schema_objects_to_the_filter() {
    let query = build_drop_dependencies_query("('app', 'public')");
    // Every schema-scoped key is limited to the inspected schemas…
    for kind in [
        "'rel:'",
        "'proc:'",
        "'type:'",
        "'fk:'",
        "'op:'",
        "'coll:'",
        "'tscfg:'",
        "'tsdict:'",
        "'stx:'",
        "'nsp:'",
    ] {
        let at = query.find(kind).unwrap_or_else(|| panic!("{kind} missing"));
        let branch_end = query[at..]
            .find("\n            when")
            .unwrap_or(query.len() - at);
        assert!(
            query[at..at + branch_end].contains("nspname in ('app', 'public')"),
            "{kind} is not scoped:\n{}",
            &query[at..at + branch_end]
        );
    }
    // …while database-wide objects are keyed wherever they are.
    for kind in [
        "'cast:('", "'evt:'", "'fdw:'", "'srv:'", "'um:'", "'sub:'", "'ext:'",
    ] {
        assert!(query.contains(kind), "{kind} missing");
    }
}

#[test]
fn drop_dependencies_query_reads_only_hard_dependencies() {
    let query = build_drop_dependencies_query("('app')");
    assert!(query.contains("where d.deptype in ('n', 'i')"), "{query}");
    // An internal dependency is reversed: the owner goes first.
    assert!(
        query.contains(
            "case when d.deptype = 'i' then rk.key else coalesce(dk.key, 'outside:') end as before_key"
        ),
        "{query}"
    );
    assert!(
        query.contains("case when d.deptype = 'i' then dk.key else rk.key end as after_key"),
        "{query}"
    );
    // An extension member is keyed as its extension, and the walk up to the
    // owning object stops at one.
    assert!(query.contains("m.deptype = 'e'"), "{query}");
    assert!(
        query.contains("'ext:' || quote_ident(x.extname)"),
        "{query}"
    );
}

#[test]
fn drop_dependencies_query_reads_user_mappings_through_the_public_view() {
    let query = build_drop_dependencies_query("('app')");
    assert!(
        query.contains("from pg_catalog.pg_user_mappings u"),
        "{query}"
    );
    assert!(
        !query.contains("from pg_catalog.pg_user_mapping "),
        "{query}"
    );
    // Only the publicly readable columns of pg_subscription.
    assert!(
        query.contains("select 'sub:' || quote_ident(s.subname)"),
        "{query}"
    );
    assert!(!query.contains("subconninfo"), "{query}");
}

#[test]
fn drop_dependencies_query_keys_routines_and_operators_like_the_dump() {
    let query = build_drop_dependencies_query("('app')");
    assert!(
        query.contains(
            "select 'proc:' || quote_ident(n.nspname) || '.' || quote_ident(p.proname)\n                    || '(' || pg_catalog.pg_get_function_identity_arguments(p.oid) || ')'"
        ),
        "{query}"
    );
    assert!(
        query.contains("then pg_catalog.format_type(o.oprleft, null) end, 'NONE')"),
        "{query}"
    );
    assert!(
        query.contains("select 'cast:(' || pg_catalog.format_type(c.castsource, null) || ' AS '"),
        "{query}"
    );
}

#[test]
fn drop_dependencies_query_folds_sub_objects_into_their_owner() {
    let query = build_drop_dependencies_query("('app')");
    for (catalog, owner) in [
        ("pg_catalog.pg_index i", "i.indrelid"),
        ("pg_catalog.pg_attrdef a", "a.adrelid"),
        ("pg_catalog.pg_trigger t", "t.tgrelid"),
        ("pg_catalog.pg_rewrite r", "r.ev_class"),
        ("pg_catalog.pg_policy p", "p.polrelid"),
    ] {
        let at = query.find(&format!("from {catalog}\n")).unwrap_or_else(|| {
            panic!("{catalog} not walked:\n{query}");
        });
        assert!(
            query[..at].ends_with(&format!("{owner}\n        ")),
            "{catalog}"
        );
    }
    // A foreign key keeps its own key; every other constraint belongs to its
    // table or domain.
    assert!(
        query.contains("and not (c.contype = 'f' and c.conrelid <> 0)"),
        "{query}"
    );
    // Array types go to their element type, row types to their relation.
    assert!(query.contains("e.typarray = t.oid"), "{query}");
    assert!(query.contains("and c.relkind <> 'c'"), "{query}");
}

#[test]
fn drop_dependencies_query_offers_breakers_for_droppable_table_parts() {
    let query = build_drop_dependencies_query("('app')");
    for breaker in [
        "format('alter table if exists %I.%I drop constraint if exists %I'",
        "then format('alter table if exists %I.%I alter column %I drop expression if exists'",
        "then format('alter table if exists %I.%I drop column if exists %I'",
        "else format('alter table if exists %I.%I alter column %I drop default'",
        "format('drop trigger if exists %I on %I.%I'",
        "format('drop rule if exists %I on %I.%I'",
        "format('drop policy if exists %I on %I.%I'",
        "format('drop index if exists %I.%I'",
    ] {
        assert!(query.contains(breaker), "{breaker} missing:\n{query}");
    }
    // Only for ordinary dependencies, and never for a part that cannot be
    // dropped on its own: an inherited constraint or column, an internal or
    // cloned trigger, a view's own rule, a constraint's or a partition's index.
    assert!(
        query.contains("case when d.deptype = 'n' and dk.key is not null then ("),
        "{query}"
    );
    assert!(query.contains("k.contype = 'c' and k.conislocal and k.coninhcount = 0"));
    assert!(query.contains("a.attislocal and a.attinhcount = 0"));
    assert!(query.contains("not tg.tgisinternal and tg.tgparentid = 0"));
    assert!(query.contains("r.rulename <> '_RETURN'"));
    assert!(query.contains("where k.conindid = c.oid"));
    assert!(query.contains("where h.inhrelid = c.oid"));
}

#[test]
fn drop_dependencies_query_needs_no_version_gate() {
    // The same text for every server: nothing in it is newer than
    // PostgreSQL 14 (`tgparentid` is 13+, multiranges and `pg_user_mappings`
    // older still).
    let query = build_drop_dependencies_query("('app')");
    for newer in ["subserver", "conenforced", "prosqlbody", "attcompression"] {
        assert!(!query.contains(newer), "{newer}");
    }
}

// ---- ordering ----

#[test]
fn an_order_that_already_satisfies_every_edge_is_kept() {
    let sorted = stable_topological_order(4, &[(0, 1), (1, 3), (0, 2)]);
    assert_eq!(sorted.order, vec![0, 1, 2, 3]);
    assert!(sorted.forced.is_empty());
}

#[test]
fn a_node_moves_only_as_far_as_an_edge_forces_it() {
    // 3 must go before 1: 1 waits for it, nothing else moves.
    let sorted = stable_topological_order(5, &[(3, 1)]);
    assert_eq!(sorted.order, vec![0, 2, 3, 1, 4]);
    assert!(sorted.forced.is_empty());
}

#[test]
fn a_chain_of_edges_is_followed() {
    let sorted = stable_topological_order(4, &[(3, 2), (2, 1), (1, 0)]);
    assert_eq!(sorted.order, vec![3, 2, 1, 0]);
}

#[test]
fn self_edges_duplicates_and_out_of_range_nodes_are_ignored() {
    let sorted = stable_topological_order(3, &[(1, 1), (2, 0), (2, 0), (7, 0), (0, 9)]);
    assert_eq!(sorted.order, vec![1, 2, 0]);
    assert!(sorted.forced.is_empty());
}

#[test]
fn a_cycle_is_forced_at_its_first_node_and_everything_else_still_holds() {
    // 1 <-> 2 is a cycle; 0 waits for 1 and 3 for 2.
    let sorted = stable_topological_order(4, &[(1, 2), (2, 1), (1, 0), (2, 3)]);
    assert_eq!(sorted.forced, vec![1]);
    assert_eq!(sorted.order, vec![1, 0, 2, 3]);
}

#[test]
fn a_node_queued_behind_a_cycle_is_not_forced_ahead_of_it() {
    // 0 waits for the cycle 2 <-> 3; the lowest-numbered waiting node is 0,
    // but it is not on the cycle, so 2 is forced instead.
    let sorted = stable_topological_order(4, &[(2, 0), (2, 3), (3, 2)]);
    assert_eq!(sorted.forced, vec![2]);
    assert_eq!(sorted.order, vec![1, 2, 0, 3]);
}

#[test]
fn edges_on_cycles_marks_only_edges_inside_a_strong_component() {
    let edges = [(0, 1), (1, 2), (2, 1), (2, 3), (3, 3), (4, 0)];
    assert_eq!(
        edges_on_cycles(5, &edges),
        vec![false, true, true, false, false, false]
    );
    // A longer cycle, reached from outside.
    let edges = [(0, 1), (1, 2), (2, 3), (3, 1)];
    assert_eq!(edges_on_cycles(4, &edges), vec![false, true, true, true]);
}

#[test]
fn a_long_chain_does_not_overflow_the_stack() {
    let n = 200_000;
    let mut edges: Vec<(usize, usize)> = (1..n).map(|i| (i, i - 1)).collect();
    edges.push((0, n - 1));
    assert!(edges_on_cycles(n, &edges).iter().all(|&c| c));
    let sorted = stable_topological_order(n, &edges);
    assert_eq!(sorted.order.len(), n);
    assert_eq!(sorted.forced, vec![0]);
}

#[test]
fn drop_dependencies_query_breaks_a_stored_generated_column_by_its_expression() {
    // `drop column` failed under a view reading the column ("cannot drop
    // column g of table ge.t because other objects depend on it"), and the
    // cycle stayed. DROP EXPRESSION keeps the column, so views on it do not
    // matter; only a virtual column, which cannot drop its expression (and on
    // PostgreSQL 18 and 19 cannot call a user-defined function at all), is
    // still dropped whole.
    let query = build_drop_dependencies_query("('app')");
    let stored = query
        .find("when a.attgenerated = 's'\n                then format('alter table if exists %I.%I alter column %I drop expression if exists'")
        .unwrap_or_else(|| panic!("no DROP EXPRESSION breaker:\n{query}"));
    let virtual_ = query
        .find("when a.attgenerated <> ''\n                then format('alter table if exists %I.%I drop column if exists %I'")
        .unwrap_or_else(|| panic!("no DROP COLUMN fallback:\n{query}"));
    assert!(stored < virtual_, "{query}");
}

#[test]
fn drop_dependencies_query_reads_a_pg14_generated_column_dependency() {
    // PostgreSQL 14 records a generated column's use of a function as an
    // automatic dependency of the column: `drop function` drops the column
    // too, and fails while a view reads the column. Each normal dependent of
    // such a column counts as a dependent of the function.
    let query = build_drop_dependencies_query("('app')");
    for part in [
        "select u.classid, u.objid, g.refclassid, g.refobjid, 'n'::\"char\"",
        "join pg_catalog.pg_attribute a on a.attrelid = g.objid and a.attnum = g.objsubid",
        "and u.refobjsubid = g.objsubid and u.deptype = 'n'",
        "where g.classid = 'pg_catalog.pg_class'::regclass and g.objsubid > 0",
        "and g.deptype = 'a' and a.attgenerated <> ''",
        // …but not the column's use of its own table's other columns.
        "and not (g.refclassid = 'pg_catalog.pg_class'::regclass and g.refobjid = g.objid)",
    ] {
        assert!(query.contains(part), "{part} missing:\n{query}");
    }
    // Still only ordinary and internal dependencies otherwise.
    assert!(
        query.contains("where d.deptype in ('n', 'i')\n  union\n"),
        "{query}"
    );
}

#[test]
fn drop_dependencies_query_reports_outside_dependents_of_database_wide_objects() {
    // A foreign table in an unselected schema has no key; on a server (or a
    // wrapper, mapping, subscription or event trigger) it still has to show,
    // or the clear command would drop the server's user mappings and then
    // fail on the server.
    let query = build_drop_dependencies_query("('app')");
    assert!(
        query.contains(
            "or (dk.key is null and d.deptype = 'n'\n        and split_part(rk.key, ':', 1) in ('fdw', 'srv', 'um', 'sub', 'evt')))"
        ),
        "{query}"
    );
    assert!(query.contains("where rk.key is not null\n"), "{query}");
    // Every other key names a kind and an identity, so none can be mistaken
    // for the marker.
    assert_eq!(OUTSIDE_KEY, "outside:");
    for key in [
        relation_key("app", "t"),
        server_key("s"),
        extension_key("e"),
        schema_key("app"),
    ] {
        assert_ne!(key, OUTSIDE_KEY);
        assert!(!key.starts_with("outside:"), "{key}");
    }
}
