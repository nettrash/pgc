//! Subscriptions, foreign-data wrappers and servers: PG19 SERVER subscriptions, subscription options, FDW CONNECTION, drop ordering.

use crate::comparer::core::*;
use crate::config::dump_config::DumpConfig;
use crate::config::grants_mode::GrantsMode;
use crate::dump::fdw::{ForeignDataWrapper, ForeignServer, UserMapping};
use crate::dump::publication::{Subscription, SubscriptionSlot};

fn fdw(name: &str) -> ForeignDataWrapper {
    let mut f = ForeignDataWrapper {
        name: name.into(),
        owner: "postgres".into(),
        handler_func: Some("public.postgres_fdw_handler".into()),
        validator_func: Some("public.postgres_fdw_validator".into()),
        connection_func: Some("public.postgres_fdw_connection".into()),
        options: vec![],
        comment: None,
        hash: None,
    };
    f.hash();
    f
}

fn server(name: &str, fdw_name: &str) -> ForeignServer {
    let mut s = ForeignServer {
        name: name.into(),
        owner: "postgres".into(),
        fdw_name: fdw_name.into(),
        server_type: None,
        server_version: None,
        options: vec![format!("dbname={name}")],
        comment: None,
        hash: None,
        acl: Vec::new(),
    };
    s.hash();
    s
}

fn mapping(server_name: &str) -> UserMapping {
    let mut m = UserMapping {
        server_name: server_name.into(),
        username: "PUBLIC".into(),
        options: vec!["user=repl".into()],
        hash: None,
    };
    m.hash();
    m
}

fn subscription(name: &str, server: Option<&str>) -> Subscription {
    let mut s = Subscription {
        name: name.into(),
        owner: "postgres".into(),
        connection: if server.is_some() {
            String::new()
        } else {
            "host=127.0.0.1 port=1 dbname=nowhere".into()
        },
        server: server.map(Into::into),
        publications: vec!["p".into()],
        enabled: false,
        slot_name: Default::default(),
        retain_dead_tuples: false,
        max_retention_duration: 0,
        wal_receiver_timeout: None,
        comment: None,
        hash: None,
    };
    s.hash();
    s
}

/// FROM: wrapper `w_old` (FROM-only) carrying server `srv_old` (FROM-only),
/// with `sub_gone` (FROM-only) and `sub_moved` on it. TO: `sub_moved` moves
/// to the TO-only server `srv_new` on the TO-only wrapper `w_new`.
fn retarget_dumps() -> (Dump, Dump) {
    let mut from = Dump::new(DumpConfig::default());
    from.foreign_data_wrappers.push(fdw("w_old"));
    from.foreign_servers.push(server("srv_old", "w_old"));
    from.user_mappings.push(mapping("srv_old"));
    from.subscriptions
        .push(subscription("sub_gone", Some("srv_old")));
    from.subscriptions
        .push(subscription("sub_moved", Some("srv_old")));

    let mut to = Dump::new(DumpConfig::default());
    to.foreign_data_wrappers.push(fdw("w_new"));
    to.foreign_servers.push(server("srv_new", "w_new"));
    to.user_mappings.push(mapping("srv_new"));
    to.subscriptions
        .push(subscription("sub_moved", Some("srv_new")));
    (from, to)
}

fn position(script: &str, needle: &str) -> usize {
    script
        .find(needle)
        .unwrap_or_else(|| panic!("`{needle}` missing from:\n{script}"))
}

#[tokio::test]
async fn subscriptions_are_dropped_or_moved_before_their_server_and_wrapper() {
    let (from, to) = retarget_dumps();
    let mut comparer = Comparer::new(from, to, true, false, false, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();

    let drop_sub = position(&script, "DROP SUBSCRIPTION IF EXISTS sub_gone;");
    let create_fdw = position(&script, "CREATE FOREIGN DATA WRAPPER w_new");
    let create_srv = position(&script, "CREATE SERVER srv_new");
    let create_um = position(&script, "CREATE USER MAPPING FOR PUBLIC SERVER srv_new");
    let drop_um = position(
        &script,
        "DROP USER MAPPING IF EXISTS FOR PUBLIC SERVER srv_old;",
    );
    let alter_sub = position(&script, "ALTER SUBSCRIPTION sub_moved SERVER srv_new;");
    let drop_srv = position(&script, "DROP SERVER IF EXISTS srv_old CASCADE;");
    let drop_fdw = position(
        &script,
        "DROP FOREIGN DATA WRAPPER IF EXISTS w_old CASCADE;",
    );

    // A subscription blocks DROP SERVER / DROP FOREIGN DATA WRAPPER even
    // with CASCADE, so it must be gone (or moved) first.
    assert!(drop_sub < drop_srv && drop_sub < drop_fdw, "{script}");
    // ALTER ... SERVER needs the server and a user mapping to exist.
    assert!(
        create_fdw < create_srv && create_srv < create_um,
        "{script}"
    );
    assert!(create_um < alter_sub, "{script}");
    assert!(alter_sub < drop_srv, "{script}");
    // Servers before wrappers; the old mapping may go before its server.
    assert!(drop_um < drop_srv && drop_srv < drop_fdw, "{script}");
}

#[tokio::test]
async fn from_only_subscriptions_are_dropped_before_every_other_section() {
    let (from, to) = retarget_dumps();
    let mut comparer = Comparer::new(from, to, true, true, true, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();

    let begin = position(&script, "begin;");
    let drop_sub = position(&script, "DROP SUBSCRIPTION IF EXISTS sub_gone;");
    let schemas = position(&script, "Schemas: Start section");
    // A cascading schema or extension drop could otherwise reach a server a
    // subscription depends on.
    assert!(begin < drop_sub && drop_sub < schemas, "{script}");
    assert!(script.contains("/* ---> Subscriptions DROP: Start section"));
    assert!(script.contains("/* ---> Foreign Servers and Data Wrappers DROP: Start section"));
}

#[tokio::test]
async fn deferred_drops_stay_commented_out_without_use_drop() {
    let (from, to) = retarget_dumps();
    let mut comparer = Comparer::new(from, to, false, false, true, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();

    for drop in [
        "DROP SUBSCRIPTION IF EXISTS sub_gone;",
        "DROP SERVER IF EXISTS srv_old CASCADE;",
        "DROP FOREIGN DATA WRAPPER IF EXISTS w_old CASCADE;",
    ] {
        assert!(
            script.contains(&format!("-- {drop}")),
            "{drop} must be commented:\n{script}"
        );
        assert!(
            !script.lines().any(|l| l.trim_start().starts_with(drop)),
            "{drop} must not be active:\n{script}"
        );
    }
    // The retarget itself is not a drop.
    assert!(script.contains("\nALTER SUBSCRIPTION sub_moved SERVER srv_new;"));
}

#[tokio::test]
async fn no_drop_sections_are_emitted_when_nothing_is_dropped() {
    // Byte-identical output for the common case: the new sections exist only
    // when there is something to drop.
    let mut from = Dump::new(DumpConfig::default());
    from.foreign_data_wrappers.push(fdw("w"));
    from.foreign_servers.push(server("srv", "w"));
    from.subscriptions.push(subscription("s", None));
    let to = from.clone();
    let mut comparer = Comparer::new(from, to, true, false, true, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();

    assert!(!script.contains("Subscriptions DROP"), "{script}");
    assert!(!script.contains("Data Wrappers DROP"), "{script}");
    assert!(!script.contains("DROP "), "{script}");
}

#[tokio::test]
async fn fdw_connection_change_is_altered_in_place() {
    let mut from = Dump::new(DumpConfig::default());
    let mut without = fdw("w");
    without.connection_func = None;
    without.hash();
    from.foreign_data_wrappers.push(without);
    let mut to = Dump::new(DumpConfig::default());
    to.foreign_data_wrappers.push(fdw("w"));

    let mut comparer = Comparer::new(from, to, true, false, false, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    assert_eq!(
        comparer.get_script(),
        "ALTER FOREIGN DATA WRAPPER w CONNECTION public.postgres_fdw_connection;\n"
    );
}

/// FROM and TO each hold `s_rdt` (enabled; retain_dead_tuples flips on),
/// `s_off` (disabled; flips on too, no worker to wait for), `s_mrd` (enabled;
/// only max_retention_duration changes) and `s_same`.
fn worker_restart_dumps() -> (Dump, Dump) {
    let mut from = Dump::new(DumpConfig::default());
    let mut to = Dump::new(DumpConfig::default());
    for (name, enabled, rdt_to, mrd_to) in [
        ("s_rdt", true, true, 0),
        ("s_off", false, true, 0),
        ("s_mrd", true, false, 5000),
        ("s_same", true, false, 0),
    ] {
        let mut before = subscription(name, None);
        before.enabled = enabled;
        before.hash();
        let mut after = before.clone();
        after.retain_dead_tuples = rdt_to;
        after.max_retention_duration = mrd_to;
        after.hash();
        from.subscriptions.push(before);
        to.subscriptions.push(after);
    }
    (from, to)
}

#[test]
fn worker_restart_warning_names_bounced_subscriptions_in_a_single_transaction() {
    let (from, to) = worker_restart_dumps();
    let single = Comparer::new(
        from.clone(),
        to.clone(),
        true,
        true,
        false,
        GrantsMode::Ignore,
    );
    let warning = single
        .worker_restart_warning()
        .expect("a bounce inside begin/commit must be reported");
    assert!(warning.starts_with("Warning: "), "{warning}");
    assert!(warning.contains("s_rdt"), "{warning}");
    for quiet in ["s_off", "s_mrd", "s_same"] {
        assert!(
            !warning.contains(quiet),
            "{quiet} needs no bounce: {warning}"
        );
    }
    assert!(
        warning.contains("logical replication worker is still running"),
        "{warning}"
    );

    // Without --use-single-transaction every statement commits on its own,
    // so the DISABLE reaches the worker before the SET runs.
    let autocommit = Comparer::new(from, to, true, false, false, GrantsMode::Ignore);
    assert!(autocommit.worker_restart_warning().is_none());
}

#[tokio::test]
async fn worker_restart_warning_leaves_the_script_unchanged() {
    // The warning goes to stderr only: with or without comments the script
    // is the same bounce it always was.
    let (from, to) = worker_restart_dumps();
    let mut comparer = Comparer::new(from, to, true, true, false, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    assert_eq!(
        comparer.get_script(),
        "begin;\n\nALTER SUBSCRIPTION s_rdt DISABLE;\n\n\
         ALTER SUBSCRIPTION s_rdt SET (retain_dead_tuples = true);\n\n\
         ALTER SUBSCRIPTION s_rdt ENABLE;\n\n\
         ALTER SUBSCRIPTION s_off SET (retain_dead_tuples = true);\n\n\
         ALTER SUBSCRIPTION s_mrd SET (max_retention_duration = 5000);\n\ncommit;\n"
    );
}

// ==== fix:pre-grants tests ====

/// `s` on publication `p` (enabled as given) in FROM, on `p` and `MyPub` in TO.
fn publication_change_dumps(from_enabled: bool, to_enabled: bool) -> (Dump, Dump) {
    let mut from_sub = subscription("s", None);
    from_sub.enabled = from_enabled;
    from_sub.hash();
    let mut to_sub = subscription("s", None);
    to_sub.enabled = to_enabled;
    to_sub.publications = vec!["p".into(), "MyPub".into()];
    to_sub.hash();
    let mut from = Dump::new(DumpConfig::default());
    from.subscriptions.push(from_sub);
    let mut to = Dump::new(DumpConfig::default());
    to.subscriptions.push(to_sub);
    (from, to)
}

async fn compare_subscription_change(
    from_enabled: bool,
    to_enabled: bool,
    single_transaction: bool,
) -> String {
    let (from, to) = publication_change_dumps(from_enabled, to_enabled);
    let mut comparer = Comparer::new(from, to, true, single_transaction, true, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    comparer.get_script()
}

#[tokio::test]
async fn set_publication_of_a_disabled_subscription_does_not_refresh() {
    let script = compare_subscription_change(false, false, false).await;
    assert!(
        script
            .contains("ALTER SUBSCRIPTION s SET PUBLICATION p, \"MyPub\" WITH (refresh = false);"),
        "{script}"
    );
    assert!(!script.contains("REFRESH PUBLICATION"), "{script}");
    assert!(!script.contains("Subscriptions post-commit"), "{script}");
}

#[tokio::test]
async fn subscription_enabled_by_the_migration_is_refreshed_after_its_enable() {
    let script = compare_subscription_change(false, true, false).await;
    let set = position(
        &script,
        "SET PUBLICATION p, \"MyPub\" WITH (refresh = false);",
    );
    let enable = position(&script, "ALTER SUBSCRIPTION s ENABLE;");
    let refresh = position(&script, "ALTER SUBSCRIPTION s REFRESH PUBLICATION;");
    assert!(set < enable && enable < refresh, "{script}");
}

#[tokio::test]
async fn single_transaction_refreshes_after_commit() {
    // Refreshing cannot run inside a transaction block, even enabled.
    let script = compare_subscription_change(true, true, true).await;
    let set = position(
        &script,
        "SET PUBLICATION p, \"MyPub\" WITH (refresh = false);",
    );
    let commit = position(&script, "commit;");
    let header = position(
        &script,
        "/* ---> Subscriptions post-commit (run outside a transaction): Start",
    );
    let refresh = position(&script, "ALTER SUBSCRIPTION s REFRESH PUBLICATION;");
    assert!(
        set < commit && commit < header && header < refresh,
        "{script}"
    );
    // Without a single transaction the enabled subscription refreshes in place.
    let script = compare_subscription_change(true, true, false).await;
    assert!(
        script.contains("ALTER SUBSCRIPTION s SET PUBLICATION p, \"MyPub\";"),
        "{script}"
    );
    assert!(!script.contains("post-commit"), "{script}");
}

// ==== r3-constraints tests ====

/// Wrappers `w1` and `w2`; server `srv` on `wrapper` with a PUBLIC user
/// mapping; foreign table `public.ft` on it — commented, with a grant — and
/// the view `public.v_ft` over it.
fn wrapper_change_dump(wrapper: &str) -> Dump {
    use crate::dump::foreign_table::{ForeignTable, ForeignTableColumn};
    use crate::dump::view::View;
    let mut dump = Dump::new(DumpConfig::default());
    dump.foreign_data_wrappers.push(fdw("w1"));
    dump.foreign_data_wrappers.push(fdw("w2"));
    dump.foreign_servers.push(server("srv", wrapper));
    dump.user_mappings.push(mapping("srv"));
    let mut ft = ForeignTable::new(
        "public".into(),
        "ft".into(),
        "srv".into(),
        "postgres".into(),
        vec!["table_name 'remote'".into()],
        vec![ForeignTableColumn {
            name: "id".into(),
            data_type: "integer".into(),
            is_nullable: true,
            column_default: None,
            options: vec![],
            acl: Vec::new(),
        }],
    );
    ft.comment = Some("remote rows".into());
    ft.acl = vec![
        "postgres=arwdDxtm/postgres".into(),
        "reader=r/postgres".into(),
    ];
    ft.hash();
    dump.foreign_tables.push(ft);
    let mut view = View::new(
        "v_ft".into(),
        " SELECT id\n   FROM public.ft;".into(),
        "public".into(),
        vec!["public.ft".into()],
    );
    view.owner = "postgres".into();
    view.hash();
    dump.views.push(view);
    dump
}

#[tokio::test]
async fn r3c_server_changing_its_wrapper_comes_back_with_everything_on_it() {
    // Finding 4: DROP SERVER ... CASCADE silently took the user mapping, the
    // foreign table (grant and comment included) and the view over it.
    let mut comparer = Comparer::new(
        wrapper_change_dump("w1"),
        wrapper_change_dump("w2"),
        true,
        false,
        false,
        GrantsMode::Full,
    );
    comparer.compare().await.unwrap();
    let script = comparer.get_script();

    let drop_view = position(&script, "drop view if exists public.v_ft;");
    let drop_ft = position(&script, "drop foreign table if exists public.ft;");
    let drop_um = position(
        &script,
        "DROP USER MAPPING IF EXISTS FOR PUBLIC SERVER srv;",
    );
    let drop_srv = position(&script, "DROP SERVER IF EXISTS srv;");
    let create_srv = position(&script, "CREATE SERVER srv FOREIGN DATA WRAPPER w2");
    let create_um = position(&script, "CREATE USER MAPPING FOR PUBLIC SERVER srv");
    let create_ft = position(&script, "create foreign table public.ft (");
    let comment = position(
        &script,
        "comment on foreign table public.ft is 'remote rows';",
    );
    let create_view = position(&script, "CREATE OR REPLACE VIEW public.v_ft");
    let grant = position(&script, "GRANT SELECT ON TABLE public.ft TO reader;");
    assert!(
        drop_view < drop_ft
            && drop_ft < drop_um
            && drop_um < drop_srv
            && drop_srv < create_srv
            && create_srv < create_um
            && create_um < create_ft
            && create_ft < comment
            && comment < create_view
            && create_view < grant,
        "{script}"
    );
    assert!(!script.contains("CASCADE"), "{script}");
    assert_eq!(script.matches("CREATE USER MAPPING").count(), 1, "{script}");
    assert_eq!(
        script.matches("create foreign table").count(),
        1,
        "{script}"
    );
}

#[tokio::test]
async fn r3c_server_changing_its_wrapper_is_refused_while_a_subscription_uses_it() {
    // PostgreSQL 19 refuses DROP SERVER, even with CASCADE, while a SERVER
    // subscription uses it; the migration keeps this one.
    let with_subscription = |wrapper: &str| {
        let mut dump = wrapper_change_dump(wrapper);
        dump.subscriptions.push(subscription("sub", Some("srv")));
        dump
    };
    let mut comparer = Comparer::new(
        with_subscription("w1"),
        with_subscription("w2"),
        true,
        false,
        true,
        GrantsMode::Ignore,
    );
    comparer.compare().await.unwrap();
    let script = comparer.get_script();
    assert!(
        script.contains(
            "-- Foreign server srv changes its foreign-data wrapper, which takes DROP SERVER and \
             CREATE SERVER, and PostgreSQL refuses that drop while subscription sub uses it: left \
             as it is.\n"
        ),
        "{script}"
    );
    for statement in [
        "DROP SERVER",
        "CREATE SERVER",
        "drop foreign table",
        "DROP USER MAPPING",
    ] {
        assert!(
            !script.lines().any(|l| l.starts_with(statement)),
            "{statement}: {script}"
        );
    }
}

#[tokio::test]
async fn r3c_server_changing_its_wrapper_without_use_drop_is_unchanged() {
    let mut comparer = Comparer::new(
        wrapper_change_dump("w1"),
        wrapper_change_dump("w2"),
        false,
        false,
        true,
        GrantsMode::Ignore,
    );
    comparer.compare().await.unwrap();
    let script = comparer.get_script();
    let drop = position(&script, "-- DROP SERVER IF EXISTS srv CASCADE;");
    let create = position(&script, "CREATE SERVER srv FOREIGN DATA WRAPPER w2");
    assert!(drop < create, "{script}");
    assert!(!script.contains("foreign table public.ft"), "{script}");
    assert!(!script.contains("USER MAPPING"), "{script}");
}

#[tokio::test]
async fn r3c_foreign_table_moving_server_drops_its_view_first_and_is_granted_anew() {
    use crate::dump::foreign_table::ForeignTable;
    let from = wrapper_change_dump("w1");
    let mut to = wrapper_change_dump("w1");
    to.foreign_servers.push(server("srv2", "w1"));
    let ft: &mut ForeignTable = &mut to.foreign_tables[0];
    ft.server = "srv2".into();
    ft.hash();
    let mut from = from;
    from.foreign_servers.push(server("srv2", "w1"));
    let mut comparer = Comparer::new(from, to, true, false, false, GrantsMode::Full);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();
    let drop_view = position(&script, "drop view if exists public.v_ft;");
    let drop_ft = position(&script, "drop foreign table if exists public.ft;");
    let create_ft = position(&script, "create foreign table public.ft (");
    let grant = position(&script, "GRANT SELECT ON TABLE public.ft TO reader;");
    assert!(
        drop_view < drop_ft && drop_ft < create_ft && create_ft < grant,
        "{script}"
    );
    assert!(script[create_ft..].contains("server srv2"), "{script}");
}

/// [`wrapper_change_dump`] with what else the server's re-create has to
/// bring back: the server's USAGE grant, a column grant on `public.ft`, and
/// the foreign table `public.ft_part` on the server, a partition of the local
/// `public.lp`.
fn wrapper_change_dump_with_privileges(wrapper: &str) -> Dump {
    use crate::dump::foreign_table::{ForeignTable, ForeignTableColumn};
    let mut dump = wrapper_change_dump(wrapper);
    dump.foreign_servers[0].acl = vec!["postgres=U/postgres".into(), "reader=U/postgres".into()];
    dump.foreign_tables[0].columns[0].acl = vec!["reader=w/postgres".into()];
    let mut part = ForeignTable::new(
        "public".into(),
        "ft_part".into(),
        "srv".into(),
        "postgres".into(),
        vec![],
        vec![ForeignTableColumn {
            name: "id".into(),
            data_type: "integer".into(),
            is_nullable: true,
            column_default: None,
            options: vec![],
            acl: Vec::new(),
        }],
    );
    part.partition_of = Some("public.lp".into());
    part.partition_bound = Some("FOR VALUES FROM (0) TO (10)".into());
    dump.foreign_tables.push(part);
    dump
}

#[tokio::test]
async fn r3c_server_re_created_gets_its_grants_column_grants_and_partitions_back() {
    // Review finding: the re-create silently lost the server's own USAGE
    // grant, a column grant on a foreign table on it, and a foreign table's
    // place as a partition of a local partitioned table.
    let mut comparer = Comparer::new(
        wrapper_change_dump_with_privileges("w1"),
        wrapper_change_dump_with_privileges("w2"),
        true,
        false,
        false,
        GrantsMode::Full,
    );
    comparer.compare().await.unwrap();
    let script = comparer.get_script();
    let create_srv = position(&script, "CREATE SERVER srv FOREIGN DATA WRAPPER w2");
    let create_part = position(&script, "create foreign table public.ft_part (");
    let attach = position(
        &script,
        "alter table public.lp attach partition public.ft_part FOR VALUES FROM (0) TO (10);",
    );
    let usage = position(&script, "GRANT USAGE ON FOREIGN SERVER srv TO reader;");
    let column = position(&script, "GRANT UPDATE (id) ON TABLE public.ft TO reader;");
    assert!(
        create_srv < create_part && create_part < attach && attach < usage,
        "{script}"
    );
    assert!(create_srv < column, "{script}");

    // A server that stays gets no grants: its privileges are not compared.
    let mut comparer = Comparer::new(
        wrapper_change_dump_with_privileges("w1"),
        wrapper_change_dump_with_privileges("w1"),
        true,
        false,
        false,
        GrantsMode::Full,
    );
    comparer.compare().await.unwrap();
    let script = comparer.get_script();
    assert!(!script.contains("GRANT"), "{script}");
}

// ==== r3-compat tests ====

/// The wait for `s_rdt`'s workers, as the bounce emits it.
const R3_COMPAT_WAIT_S_RDT: &str = "DO $pgc$
DECLARE
    deadline timestamptz := clock_timestamp() + interval '60 seconds';
BEGIN
    WHILE clock_timestamp() < deadline AND EXISTS (
        SELECT 1 FROM pg_catalog.pg_stat_subscription st
        JOIN pg_catalog.pg_subscription s ON s.oid = st.subid
        WHERE s.subname = 's_rdt'
          AND s.subdbid = (SELECT oid FROM pg_catalog.pg_database WHERE datname = current_database())
          AND st.pid IS NOT NULL
    ) LOOP
        PERFORM pg_catalog.pg_sleep(0.1);
    END LOOP;
END
$pgc$;";

#[tokio::test]
async fn autocommit_bounce_waits_for_the_worker_even_without_comments() {
    // Stripping comments must leave the DO block whole: it is what makes the
    // SET safe, where the note above it only explains it. Nothing is printed
    // on stderr for autocommit (see the single-transaction warning above).
    let (from, to) = worker_restart_dumps();
    let mut comparer = Comparer::new(from, to, true, false, false, GrantsMode::Ignore);
    assert!(comparer.worker_restart_warning().is_none());
    comparer.compare().await.unwrap();
    assert_eq!(
        comparer.get_script(),
        format!(
            "ALTER SUBSCRIPTION s_rdt DISABLE;\n\n{R3_COMPAT_WAIT_S_RDT}\n\n\
             ALTER SUBSCRIPTION s_rdt SET (retain_dead_tuples = true);\n\n\
             ALTER SUBSCRIPTION s_rdt ENABLE;\n\n\
             ALTER SUBSCRIPTION s_off SET (retain_dead_tuples = true);\n\n\
             ALTER SUBSCRIPTION s_mrd SET (max_retention_duration = 5000);\n"
        )
    );
}

#[tokio::test]
async fn autocommit_bounce_with_comments_explains_the_wait() {
    let (from, to) = worker_restart_dumps();
    let mut comparer = Comparer::new(from, to, true, false, true, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();
    let note = position(&script, "-- retain_dead_tuples can only change");
    let disable = position(&script, "ALTER SUBSCRIPTION s_rdt DISABLE;");
    let wait = position(&script, R3_COMPAT_WAIT_S_RDT);
    let set = position(
        &script,
        "ALTER SUBSCRIPTION s_rdt SET (retain_dead_tuples = true);",
    );
    assert!(note < disable && disable < wait && wait < set, "{script}");
    assert_eq!(script.matches("DO $pgc$").count(), 1, "{script}");
}

// ==== r4-sub tests ====

/// The disabled CONNECTION subscription `name` on `slot`, hashed.
fn r4s_subscription(name: &str, slot: SubscriptionSlot) -> Subscription {
    let mut s = subscription(name, None);
    s.slot_name = slot;
    s.hash();
    s
}

/// FROM: `s_same` and `s_none_to_named` without a slot, `s_own_to_none` on
/// its own slot. TO: `s_same` unchanged, `s_own_to_none` without a slot,
/// `s_none_to_named` on `slot_x`, and the TO-only `t_none` (no slot),
/// `t_own` (its own slot) and `t_named` (on `pre_made`). Both dumps are
/// pgc 1.1 dumps of a PG18 server.
fn r4s_dumps() -> (Dump, Dump) {
    let mut from = Dump::new(DumpConfig::default());
    from.server_version_num = Some(180000);
    from.subscriptions = vec![
        r4s_subscription("s_same", SubscriptionSlot::NoSlot),
        r4s_subscription("s_own_to_none", SubscriptionSlot::Own),
        r4s_subscription("s_none_to_named", SubscriptionSlot::NoSlot),
    ];
    let mut to = Dump::new(DumpConfig::default());
    to.server_version_num = Some(180000);
    to.subscriptions = vec![
        r4s_subscription("s_same", SubscriptionSlot::NoSlot),
        r4s_subscription("s_own_to_none", SubscriptionSlot::NoSlot),
        r4s_subscription("s_none_to_named", SubscriptionSlot::Named("slot_x".into())),
        r4s_subscription("t_none", SubscriptionSlot::NoSlot),
        r4s_subscription("t_own", SubscriptionSlot::Own),
        r4s_subscription("t_named", SubscriptionSlot::Named("pre_made".into())),
    ];
    (from, to)
}

#[tokio::test]
async fn r4s_subscriptions_mirror_the_slot_without_contacting_the_publisher() {
    let (from, to) = r4s_dumps();
    let mut comparer = Comparer::new(from, to, true, false, false, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let conn = "CONNECTION 'host=127.0.0.1 port=1 dbname=nowhere' PUBLICATION p";
    assert_eq!(
        comparer.get_script(),
        format!(
            "ALTER SUBSCRIPTION s_own_to_none SET (slot_name = NONE);\n\n\
             ALTER SUBSCRIPTION s_none_to_named SET (slot_name = 'slot_x');\n\n\
             CREATE SUBSCRIPTION t_none {conn} WITH (connect = false, slot_name = NONE, enabled = false);\n\n\
             ALTER SUBSCRIPTION t_none OWNER TO postgres;\n\n\
             CREATE SUBSCRIPTION t_own {conn} WITH (connect = false, enabled = false);\n\n\
             ALTER SUBSCRIPTION t_own OWNER TO postgres;\n\n\
             CREATE SUBSCRIPTION t_named {conn} WITH (connect = false, slot_name = 'pre_made', enabled = false);\n\n\
             ALTER SUBSCRIPTION t_named OWNER TO postgres;\n"
        )
    );
}

#[tokio::test]
async fn r4s_subscription_notes_precede_their_statements() {
    let (from, to) = r4s_dumps();
    let mut comparer = Comparer::new(from, to, true, false, true, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();
    let kept = position(
        &script,
        "-- replication slot s_own_to_none stays on the publisher",
    );
    let set_none = position(
        &script,
        "ALTER SUBSCRIPTION s_own_to_none SET (slot_name = NONE);",
    );
    let not_created = position(
        &script,
        "-- ALTER SUBSCRIPTION does not create replication slot slot_x",
    );
    let set_named = position(&script, "SET (slot_name = 'slot_x');");
    let own_note = position(&script, "-- subscription t_own is created disabled");
    let create_own = position(&script, "CREATE SUBSCRIPTION t_own ");
    assert!(
        kept < set_none && set_none < not_created && not_created < set_named,
        "{script}"
    );
    assert!(own_note < create_own, "{script}");
    assert!(
        !script.contains("-- subscription t_none"),
        "a subscription without a slot is created as it is: {script}"
    );
}

#[tokio::test]
async fn r4s_pgc_1_0_dumps_create_disabled_subscriptions_unconnected() {
    // Neither dump knows the slot (pgc 1.0.x did not read it): both load on
    // their own slot, so nothing is altered, and a TO-only disabled
    // subscription is still created without contacting its publisher.
    let (mut from, mut to) = r4s_dumps();
    for dump in [&mut from, &mut to] {
        dump.server_version_num = None;
        for sub in &mut dump.subscriptions {
            sub.slot_name = SubscriptionSlot::Own;
            sub.hash();
        }
    }
    let mut comparer = Comparer::new(from, to, true, false, false, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();
    assert!(!script.contains("slot_name"), "{script}");
    assert!(!script.contains("ALTER SUBSCRIPTION s_"), "{script}");
    assert_eq!(
        script.matches("CREATE SUBSCRIPTION ").count(),
        3,
        "{script}"
    );
    assert_eq!(
        script
            .matches(" WITH (connect = false, enabled = false);")
            .count(),
        3,
        "{script}"
    );
}

#[tokio::test]
async fn r4s_legacy_from_creates_every_to_subscription_with_its_slot() {
    // A pgc 1.0.x FROM dump against a current TO: every TO subscription is
    // created (see from_subscriptions_by_name), each as TO has it.
    let (mut from, to) = r4s_dumps();
    from.server_version_num = None;
    let mut comparer = Comparer::new(from, to, true, false, false, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();
    for create in [
        "CREATE SUBSCRIPTION s_same CONNECTION 'host=127.0.0.1 port=1 dbname=nowhere' PUBLICATION p WITH (connect = false, slot_name = NONE, enabled = false);",
        "CREATE SUBSCRIPTION s_none_to_named CONNECTION 'host=127.0.0.1 port=1 dbname=nowhere' PUBLICATION p WITH (connect = false, slot_name = 'slot_x', enabled = false);",
        "CREATE SUBSCRIPTION t_own CONNECTION 'host=127.0.0.1 port=1 dbname=nowhere' PUBLICATION p WITH (connect = false, enabled = false);",
    ] {
        assert!(script.contains(create), "{create}\n{script}");
    }
    assert!(!script.contains("SET (slot_name"), "{script}");
}

// ---- slot names PostgreSQL refuses as slot_name (r4-sub review) ----

/// FROM: `"x""y"` on slot `abc`, `s_alias` without a slot, `s_ok` without a
/// slot. TO: `"x""y"` on its own slot `x"y`, `s_alias` on a slot named
/// `none` (which `slot_name = 'none'` would turn into NONE), `s_ok` on
/// `slot_ok`, and the TO-only `t_bad` (on `Bad`) and `"T Own"` (on its own
/// slot `T Own`). Both dumps are pgc 1.1 dumps of a PG18 server.
fn r4s_refused_dumps() -> (Dump, Dump) {
    let mut from = Dump::new(DumpConfig::default());
    from.server_version_num = Some(180000);
    from.subscriptions = vec![
        r4s_subscription("\"x\"\"y\"", SubscriptionSlot::Named("abc".into())),
        r4s_subscription("s_alias", SubscriptionSlot::NoSlot),
        r4s_subscription("s_ok", SubscriptionSlot::NoSlot),
    ];
    let mut to = Dump::new(DumpConfig::default());
    to.server_version_num = Some(180000);
    to.subscriptions = vec![
        r4s_subscription("\"x\"\"y\"", SubscriptionSlot::Own),
        r4s_subscription("s_alias", SubscriptionSlot::Named("none".into())),
        r4s_subscription("s_ok", SubscriptionSlot::Named("slot_ok".into())),
        r4s_subscription("t_bad", SubscriptionSlot::Named("Bad".into())),
        r4s_subscription("\"T Own\"", SubscriptionSlot::Own),
    ];
    (from, to)
}

#[tokio::test]
async fn r4s_refused_slot_names_are_never_written() {
    let (from, to) = r4s_refused_dumps();
    let mut comparer = Comparer::new(from, to, true, false, false, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let conn = "CONNECTION 'host=127.0.0.1 port=1 dbname=nowhere' PUBLICATION p";
    assert_eq!(
        comparer.get_script(),
        format!(
            "ALTER SUBSCRIPTION s_ok SET (slot_name = 'slot_ok');\n\n\
             CREATE SUBSCRIPTION t_bad {conn} WITH (connect = false, enabled = false);\n\n\
             ALTER SUBSCRIPTION t_bad OWNER TO postgres;\n\n\
             CREATE SUBSCRIPTION \"T Own\" {conn} WITH (connect = false, enabled = false);\n\n\
             ALTER SUBSCRIPTION \"T Own\" OWNER TO postgres;\n"
        ),
        "no ALTER of \"x\"\"y\" or s_alias, whose TO slots PostgreSQL refuses"
    );
}

#[tokio::test]
async fn r4s_refused_slot_names_get_a_note_and_a_warning() {
    let (from, to) = r4s_refused_dumps();
    let mut comparer = Comparer::new(from, to, true, false, true, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    let script = comparer.get_script();
    for note in [
        "-- replication slot x\"y of subscription \"x\"\"y\" is left out: PostgreSQL does not \
         accept it as a slot_name",
        "-- replication slot none of subscription s_alias is left out",
        "-- replication slot Bad of subscription t_bad is left out",
        "-- subscription \"T Own\" is created disabled without contacting its publisher \
         (connect = false): PostgreSQL does not accept its replication slot T Own",
    ] {
        position(&script, note);
    }

    let warning = comparer
        .unsettable_slot_warning()
        .expect("slots the migration cannot set must be reported");
    assert!(warning.starts_with("Warning: "), "{warning}");
    for named in [
        "\"x\"\"y\" (slot 'x\"y')",
        "s_alias (slot 'none')",
        "t_bad (slot 'Bad')",
    ] {
        assert!(warning.contains(named), "{named}: {warning}");
    }
    // "T Own" is created on its own slot, exactly as TO has it; s_ok's
    // slot is set.
    assert!(!warning.contains("T Own"), "{warning}");
    assert!(!warning.contains("s_ok"), "{warning}");
}

#[tokio::test]
async fn r4s_settable_slots_raise_no_warning() {
    let (from, to) = r4s_dumps();
    let mut comparer = Comparer::new(from, to, true, false, false, GrantsMode::Ignore);
    comparer.compare().await.unwrap();
    assert!(comparer.unsettable_slot_warning().is_none());
    // An unchanged subscription on a refused slot is not reported either.
    let (from, _) = r4s_refused_dumps();
    let comparer = Comparer::new(from.clone(), from, true, false, false, GrantsMode::Ignore);
    assert!(comparer.unsettable_slot_warning().is_none());
}

#[tokio::test]
async fn r4s_pg19_reserves_the_conflict_detection_slot() {
    let mut from = Dump::new(DumpConfig::default());
    from.subscriptions = vec![r4s_subscription("s", SubscriptionSlot::Named("abc".into()))];
    let mut to = Dump::new(DumpConfig::default());
    to.server_version_num = Some(190000);
    to.subscriptions = vec![r4s_subscription(
        "s",
        SubscriptionSlot::Named("pg_conflict_detection".into()),
    )];
    // The migrated server is FROM's: PostgreSQL 18 accepts the name.
    for (version, set) in [(Some(180000), true), (Some(190000), false)] {
        let mut from = from.clone();
        from.server_version_num = version;
        let mut comparer = Comparer::new(from, to.clone(), true, false, false, GrantsMode::Ignore);
        comparer.compare().await.unwrap();
        let script = comparer.get_script();
        assert_eq!(
            script.contains("ALTER SUBSCRIPTION s SET (slot_name = 'pg_conflict_detection');"),
            set,
            "{version:?}: {script}"
        );
        assert_eq!(
            comparer.unsettable_slot_warning().is_some(),
            !set,
            "{version:?}"
        );
    }
}
